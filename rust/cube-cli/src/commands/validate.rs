use anyhow::{bail, Result};
use owo_colors::OwoColorize;
use serde_json::Value;

use crate::client::Query;
use crate::{output, util, Ctx};

/// Validate a deployment's data model in Cube Cloud.
///
/// The compile happens where the model actually runs: the API asks the
/// branch's own Cube runtime for `/meta`, exactly as the console does in dev
/// mode. So this validates the model against the deployment's real environment
/// variables, drivers and dependencies — what a local compile cannot do — and
/// `--branch` / `--dev-mode` pick which of those runtimes answers.
#[derive(clap::Args)]
pub struct Args {
    /// Deployment id
    deployment: i64,
    /// Branch to validate (defaults to the deployment's deploy branch)
    #[arg(long, conflicts_with = "dev_mode")]
    branch: Option<String>,
    /// Validate your active dev-mode branch — the uncommitted working copy
    /// `cube data-model dev-mode` put you on
    #[arg(long)]
    dev_mode: bool,
}

/// Render one compilation error as `<file>: <message>`, or just the message
/// when the compiler didn't attribute it to a file.
///
/// The endpoint reports each error as `{ fileName?, message }`, but the CLI and
/// the server ship from separate repos on separate cadences, so an entry that
/// doesn't match that shape still has to print as something: a blank line is
/// the one output this command must never produce — printing the errors IS the
/// command. So a bare string renders as itself, a half-filled object renders as
/// whichever half it has, and anything else falls back to its own JSON.
fn format_error(error: &Value) -> String {
    if let Value::String(message) = error {
        return message.clone();
    }

    let message = output::field(error, "message");
    let file = output::field(error, "fileName");

    match (file.is_empty(), message.is_empty()) {
        (false, false) => format!("{file}: {message}"),
        (true, false) => message,
        (false, true) => file,
        (true, true) => error.to_string(),
    }
}

/// What to call the validated branch in user-facing output.
///
/// The server echoes the branch it resolved, which is the only source for the
/// `--dev-mode` case (the personal `dev-…` name is server-side). A
/// differently-versioned one that doesn't echo it must not turn every message
/// into "on  is valid" — fall back to what the caller asked for, and to a
/// generic label when the caller named nothing either.
fn branch_label(res: &Value, args: &Args) -> String {
    let echoed = output::field(res, "branchName");
    if !echoed.is_empty() {
        return echoed;
    }

    match (&args.branch, args.dev_mode) {
        (Some(branch), _) => branch.clone(),
        (None, true) => "the dev-mode branch".to_string(),
        (None, false) => "the deploy branch".to_string(),
    }
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let mut query: Query = Vec::new();
    util::push(&mut query, "branchName", &args.branch);
    if args.dev_mode {
        query.push(("devMode".into(), "true".into()));
    }

    let res = ctx
        .api()?
        .get(
            &format!(
                "/build/api/v1/deployments/{}/data-model/validate",
                args.deployment
            ),
            &query,
        )
        .await?;

    let branch = branch_label(&res, &args);
    // Absent `valid` fails closed: a report this command can't read is not
    // evidence the model compiles, and the whole point is gating CI on it.
    let valid = res.get("valid").and_then(Value::as_bool).unwrap_or(false);
    let errors = res
        .get("errors")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    if ctx.json {
        output::print_json(&res);
    } else if valid {
        let cubes = res.get("cubesCount").and_then(Value::as_u64);
        match cubes {
            Some(n) => output::success(&format!("Data model on {branch} is valid ({n} cubes)")),
            None => output::success(&format!("Data model on {branch} is valid")),
        }
    } else if errors.is_empty() {
        // A failure the API couldn't itemize. Saying "failed to compile:" here
        // would promise a list and then print nothing; point at the runtime
        // instead, which is where the answer actually is.
        eprintln!(
            "{} Data model on {branch} could not be validated.",
            "✗".red()
        );
    } else {
        // Compilation errors go to stderr so `cube validate --json` stays
        // machine-readable on stdout and a human run stays readable when
        // stdout is piped.
        eprintln!("{} Data model on {branch} failed to compile:", "✗".red());
        for error in &errors {
            eprintln!("  {}", format_error(error));
        }
    }

    if !valid {
        // Non-zero exit is the point of the command in CI, so it holds in
        // --json mode too, where the report above was printed as JSON.
        if errors.is_empty() {
            bail!(
                "data model on {branch} could not be validated \
                 (the API reported a failure without any compilation errors)"
            );
        }
        bail!(
            "data model on {branch} has {} compilation error(s)",
            errors.len()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_error_is_prefixed_with_the_file_only_when_the_compiler_named_one() {
        assert_eq!(
            format_error(&json!({"fileName": "model/cubes/orders.yml", "message": "no sql"})),
            "model/cubes/orders.yml: no sql"
        );
        // Errors the compiler couldn't attribute carry no file; a bare `": "`
        // in front of them would read as an empty filename.
        assert_eq!(format_error(&json!({"message": "no sql"})), "no sql");
        assert_eq!(
            format_error(&json!({"fileName": null, "message": "no sql"})),
            "no sql"
        );
    }

    #[test]
    fn an_entry_that_is_not_the_expected_object_still_prints_something() {
        // A blank line is the one output this command must never produce, so
        // every shape a differently-versioned server could send has to render.
        assert_eq!(
            format_error(&json!("Orders cube: no sql")),
            "Orders cube: no sql"
        );
        assert_eq!(
            format_error(&json!({"fileName": "model/cubes/orders.yml"})),
            "model/cubes/orders.yml"
        );
        assert_eq!(format_error(&json!({"code": 7})), r#"{"code":7}"#);
        assert_eq!(format_error(&Value::Null), "null");
    }

    fn args(branch: Option<&str>, dev_mode: bool) -> Args {
        Args {
            deployment: 1,
            branch: branch.map(str::to_string),
            dev_mode,
        }
    }

    #[test]
    fn the_branch_the_server_echoes_wins() {
        // The `--dev-mode` name only exists server-side, so the echo is the
        // only way to report which branch was actually validated.
        let res = json!({"branchName": "dev-pavel-feature", "valid": true});
        assert_eq!(branch_label(&res, &args(None, true)), "dev-pavel-feature");
        assert_eq!(
            branch_label(&res, &args(Some("feature"), false)),
            "dev-pavel-feature"
        );
    }

    #[test]
    fn a_response_without_a_branch_never_leaves_a_hole_in_the_message() {
        // "Data model on  is valid" is the failure this guards against.
        let res = json!({"valid": true});
        assert_eq!(branch_label(&res, &args(Some("feature"), false)), "feature");
        assert_eq!(branch_label(&res, &args(None, true)), "the dev-mode branch");
        assert_eq!(branch_label(&res, &args(None, false)), "the deploy branch");
    }
}
