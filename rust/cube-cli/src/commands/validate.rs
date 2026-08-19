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
fn format_error(error: &Value) -> String {
    let message = output::field(error, "message");
    let file = output::field(error, "fileName");
    if file.is_empty() {
        message
    } else {
        format!("{file}: {message}")
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

    let branch = output::field(&res, "branchName");
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
}
