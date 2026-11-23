use std::time::Duration;

use anyhow::{anyhow, bail, Context as _, Result};
use clap::Subcommand;
use reqwest::Method;
use serde_json::{json, Map, Value};

use crate::client::{Client, Query};
use crate::wait::{self, Progress, Wait};
use crate::{output, util, Ctx};

/// Publish Cube views as Databricks Metric Views: manage a data source's publication
/// targets, start or promote runs, and follow them
#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List a data source's publication targets, each with its newest run
    Targets {
        #[command(flatten)]
        integration: Integration,
    },
    /// Read, save or delete one publication target
    Target {
        #[command(subcommand)]
        cmd: TargetCmd,
    },
    /// Start a publication run, or promote a completed run of another target
    Sync {
        #[command(flatten)]
        integration: Integration,
        /// Publication target to publish to, such as `production`. Defaults to `default`
        #[arg(long, value_parser = util::nonempty_filter)]
        target: Option<String>,
        /// Publish the build that this completed run of another target published, such as
        /// a staging run you have tested
        #[arg(long, value_name = "RUN_ID", value_parser = util::nonempty_filter)]
        promote_from: Option<String>,
        /// A UUID unique to this publication attempt: repeating the start with it returns
        /// the first run instead of publishing twice
        #[arg(long, value_name = "UUID", value_parser = util::nonempty_filter)]
        idempotency_key: Option<String>,
        /// Wait for the run, and exit non-zero unless it completes with no measures withheld
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long
        #[arg(long, default_value = "30m", value_parser = util::parse_duration, requires = "wait")]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration, requires = "wait")]
        poll: Duration,
    },
    /// List a publication target's runs, newest first
    Runs {
        #[command(flatten)]
        integration: Integration,
        /// Publication target whose runs to list. Defaults to `default`
        #[arg(long, value_parser = util::nonempty_filter)]
        target: Option<String>,
        /// Only runs with this status: QUEUED, RUNNING, CANCELLING, COMPLETED, PARTIAL,
        /// FAILED, CANCELLED
        #[arg(long, value_parser = util::nonempty_filter)]
        status: Option<String>,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Show a publication run and what happened to each view
    Run {
        #[command(flatten)]
        integration: Integration,
        /// Run id, as returned by `sync`
        #[arg(value_parser = util::path_name)]
        run_id: String,
        /// Wait for the run, and exit non-zero unless it completes with no measures withheld
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long
        #[arg(long, default_value = "30m", value_parser = util::parse_duration, requires = "wait")]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration, requires = "wait")]
        poll: Duration,
    },
    /// Cancel an active publication run. Metric Views it already wrote stay
    Cancel {
        #[command(flatten)]
        integration: Integration,
        /// Run id, as returned by `sync`
        #[arg(value_parser = util::path_name)]
        run_id: String,
    },
}

#[derive(Subcommand)]
enum TargetCmd {
    /// Show a publication target's settings and the builds deployed and last published
    Get {
        #[command(flatten)]
        integration: Integration,
        /// Publication target, such as `staging`. Defaults to `default`
        #[arg(long, value_parser = util::nonempty_filter)]
        target: Option<String>,
    },
    /// Create or update a publication target's settings
    ///
    /// Reads the target first and sends the configuration version it read, so a save that
    /// races another one fails. Without --data, the flags change only what they name.
    Set {
        #[command(flatten)]
        integration: Integration,
        /// Publication target, such as `staging`; created when it doesn't exist. Defaults
        /// to `default`
        #[arg(long, value_parser = util::nonempty_filter)]
        target: Option<String>,
        #[command(flatten)]
        settings: Settings,
        /// The whole settings as JSON (inline, @file, or - for stdin), replacing the saved
        /// ones. The flags above win over the same key here
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Delete a publication target. Its Metric Views stay in Databricks
    ///
    /// Refused for `default`, for an enabled target (disable it first) and for one with an
    /// active run.
    Delete {
        #[command(flatten)]
        integration: Integration,
        /// Publication target to delete
        #[arg(long, value_parser = util::nonempty_filter)]
        target: String,
    },
}

/// The deployment and data source every command addresses.
#[derive(clap::Args)]
struct Integration {
    /// Deployment id
    deployment: i64,
    /// Data source name, such as `default`
    #[arg(value_parser = util::path_name)]
    data_source: String,
}

impl Integration {
    fn path(&self) -> String {
        format!(
            "/api/v1/deployments/{}/databricks-metric-view-integrations/{}",
            self.deployment,
            util::path_segment(&self.data_source)
        )
    }

    fn run_path(&self, run_id: &str) -> String {
        format!("{}/syncs/{}", self.path(), util::path_segment(run_id))
    }

    /// The command that waits for a run, for messages to suggest.
    fn follow_command(&self, run_id: &str) -> String {
        format!(
            "cube dbx-mv run {} {} {} --wait",
            self.deployment,
            util::shell_quote(&self.data_source),
            util::shell_quote(run_id)
        )
    }
}

#[derive(clap::Args)]
struct Settings {
    /// Publish to this target: `--enabled` or `--enabled=false`
    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    enabled: Option<bool>,
    /// Start a run after each successful build of the deployment: `--sync-on-deploy` or
    /// `--sync-on-deploy=false`
    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    sync_on_deploy: Option<bool>,
    /// Databricks catalog to publish into
    #[arg(long, value_parser = util::nonempty_filter)]
    catalog: Option<String>,
    /// Databricks schema to publish into
    #[arg(long, value_parser = util::nonempty_filter)]
    schema: Option<String>,
    /// Which Cube views to publish: all, selected (--views) or pattern (--view-name-regex)
    #[arg(long, value_parser = util::nonempty_filter)]
    selection_mode: Option<String>,
    /// Cube views to publish with `--selection-mode selected`, comma-separated
    #[arg(long, value_delimiter = ',', value_parser = util::nonempty_filter)]
    views: Option<Vec<String>>,
    /// Name pattern of the Cube views to publish with `--selection-mode pattern`
    #[arg(long, value_parser = util::nonempty_filter)]
    view_name_regex: Option<String>,
    /// What a run does with a Metric View whose Cube view left the scope: retain or
    /// delete-managed
    #[arg(long, value_parser = util::nonempty_filter)]
    deletion_policy: Option<String>,
}

impl Settings {
    /// The settings fields the flags name, keyed as the API saves them.
    fn fields(&self) -> Map<String, Value> {
        let mut fields = Map::new();
        util::set(&mut fields, "enabled", &self.enabled);
        util::set(&mut fields, "syncOnDeploy", &self.sync_on_deploy);
        util::set(&mut fields, "targetCatalog", &self.catalog);
        util::set(&mut fields, "targetSchema", &self.schema);
        util::set(&mut fields, "selectionMode", &self.selection_mode);
        util::set(&mut fields, "viewNames", &self.views);
        util::set(&mut fields, "viewNameRegex", &self.view_name_regex);
        util::set(&mut fields, "deletionPolicy", &self.deletion_policy);
        fields
    }
}

// Everything else, including a status this build has never heard of, means still running.
const COMPLETED: &str = "COMPLETED";
const TERMINAL: [&str; 4] = [COMPLETED, "PARTIAL", "FAILED", "CANCELLED"];
const DEFAULT_TARGET: &str = "default";

fn target_query(target: &Option<String>) -> Query {
    let mut query = Query::new();
    util::push(&mut query, "target", target);
    query
}

/// Fail unless `settings` belong to the requested target. A tenant from before named
/// targets ignores `?target=` and answers for `default`, without a `targetName`.
fn check_target(settings: &Value, target: &Option<String>) -> Result<()> {
    let requested = target.as_deref().unwrap_or(DEFAULT_TARGET);
    let returned = output::field(settings, "targetName");
    let returned = if util::is_blank(&returned) {
        DEFAULT_TARGET
    } else {
        returned.as_str()
    };
    // Cube lower-cases target names.
    if returned.eq_ignore_ascii_case(requested) {
        return Ok(());
    }

    bail!(
        "asked for publication target `{requested}`, but Cube answered for `{}`; this tenant \
         may not support named publication targets yet",
        util::one_cell(returned)
    )
}

/// Whether `target` names a publication target other than `default`.
fn is_named(target: &Option<String>) -> bool {
    target
        .as_deref()
        .is_some_and(|name| !name.eq_ignore_ascii_case(DEFAULT_TARGET))
}

/// The save request: `data` when given, otherwise the saved settings, with the flags on top.
/// It carries the configuration version read from `saved`, or 0 to create the target.
fn save_body(saved: Option<&Value>, data: Option<Map<String, Value>>, flags: Settings) -> Value {
    let flags = flags.fields();
    let mut body = data.unwrap_or_else(|| {
        let mut current = saved
            .and_then(|saved| saved.get("configuration"))
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        // `version` is the response's own; an omitted `connection` keeps the stored
        // credential, whose secret a read never returns.
        current.remove("version");
        current.remove("connection");
        // A new selection mode takes its views or pattern from the flags: the API refuses
        // either one beside a mode that doesn't use it.
        if flags.contains_key("selectionMode") {
            current.remove("viewNames");
            current.remove("viewNameRegex");
        }
        current
    });
    body.extend(flags);
    body.entry("expectedConfigurationVersion")
        .or_insert_with(|| {
            saved
                .and_then(|saved| saved.get("configurationVersion"))
                .cloned()
                .unwrap_or(json!(0))
        });

    util::body(body)
}

fn sync_body(
    configuration_version: Value,
    promote_from: &Option<String>,
    idempotency_key: &Option<String>,
) -> Value {
    let mut body = Map::new();
    body.insert("configurationVersion".to_string(), configuration_version);
    util::set(&mut body, "promoteFromRunId", promote_from);
    util::set(&mut body, "idempotencyKey", idempotency_key);
    util::body(body)
}

/// The settings' configuration version, which a run start and a delete must send back.
fn configuration_version(settings: &Value) -> Result<Value> {
    match settings.get("configurationVersion") {
        Some(version @ Value::Number(_)) => Ok(version.clone()),
        _ => {
            bail!("Cube returned the publication target's settings without a configurationVersion")
        }
    }
}

fn started_run_id(started: &Value) -> Result<String> {
    let id = output::field(started, "runId");
    // Every message names the id, and one with control characters could drive the terminal.
    if util::is_blank(&id) || id.chars().any(char::is_control) {
        bail!(
            "Cube started the publication run but returned no usable run id: {}",
            id.escape_debug()
        );
    }

    Ok(id)
}

fn is_terminal(status: &str) -> bool {
    TERMINAL.contains(&status)
}

/// Why a finished run fails `--wait`, or `None` when it passes: it completed and published
/// every view in scope with all its measures, which is also what promoting it requires.
fn gate_failure(run: &Value) -> Option<String> {
    let id = util::one_cell(&output::field(run, "runId"));
    let status = util::status_of(run, "status");
    if status != COMPLETED {
        return Some(format!(
            "publication run {id} ended {}, not {COMPLETED}",
            util::one_cell(&status)
        ));
    }

    // Fail closed: without the count, nothing says every view kept all its measures.
    let Some(incomplete) = run.get("incompleteViewCount").and_then(Value::as_u64) else {
        return Some(format!(
            "publication run {id} completed, but Cube returned no incompleteViewCount to \
             check its views against"
        ));
    };
    if incomplete > 0 {
        return Some(format!(
            "publication run {id} completed, but {incomplete} view(s) published without \
             some measures; see their withheldMembers"
        ));
    }

    None
}

/// The wait's progress label: the status, then the stage and how many views are published.
fn progress_label(run: &Value) -> String {
    let mut parts = vec![
        util::status_of(run, "status"),
        output::field(run, "progress.stage"),
    ];
    // Planning sets the total; before it, `0/0` would say nothing.
    let total = output::field(run, "progress.total");
    if !util::is_blank(&total) && total != "0" {
        parts.push(format!(
            "{}/{total}",
            output::field(run, "progress.published")
        ));
    }
    let label = parts
        .iter()
        .filter(|part| !util::is_blank(part))
        .map(|part| util::one_cell(part))
        .collect::<Vec<_>>()
        .join(" ");
    if label.is_empty() {
        "waiting".to_string()
    } else {
        label
    }
}

/// The human summary of a run: its status, the build it published, then one row per view.
fn run_lines(run: &Value) -> Vec<String> {
    let field = |path: &str| util::one_cell(&output::field(run, path));
    let mut lines = vec![format!(
        "Publication run {} of target {}: {}",
        field("runId"),
        field("targetName"),
        field("status")
    )];

    let build = field("buildJobId");
    if !util::is_blank(&build) {
        let mut line = format!("Build {build}");
        let commit = field("commit");
        if !util::is_blank(&commit) {
            line.push_str(&format!(" (commit {commit})"));
        }
        let promoted = field("promotedFromRunId");
        if !util::is_blank(&promoted) {
            line.push_str(&format!(", promoted from run {promoted}"));
        }
        lines.push(line);
    }

    lines
}

fn view_rows(run: &Value) -> Vec<Vec<String>> {
    output::items(run.get("views").unwrap_or(&Value::Null))
        .iter()
        .map(|view| {
            let withheld = output::items(view.get("withheldMembers").unwrap_or(&Value::Null))
                .iter()
                .filter_map(|member| member.as_str().map(str::to_string))
                .collect::<Vec<_>>()
                .join(", ");
            vec![
                util::one_cell(&output::field(view, "cubeViewName")),
                util::one_cell(&output::field(view, "outcome")),
                util::one_cell(&output::field(view, "target")),
                util::one_cell(&output::field(view, "detail")),
                util::one_cell(&withheld),
            ]
        })
        .collect()
}

fn print_run(run: &Value, json: bool) {
    if json {
        output::print_json(run);
        return;
    }

    for line in run_lines(run) {
        println!("{line}");
    }
    output::table(
        &["VIEW", "OUTCOME", "DESTINATION", "DETAIL", "WITHHELD"],
        view_rows(run),
    );
}

/// Print a finished run, failing unless it passes the gate (see [`gate_failure`]).
fn finish(run: &Value, json: bool) -> Result<()> {
    print_run(run, json);
    match gate_failure(run) {
        Some(reason) => Err(anyhow!(reason)),
        None => Ok(()),
    }
}

async fn wait_for_run(
    api: &Client,
    integration: &Integration,
    run_id: &str,
    timeout: Duration,
    interval: Duration,
) -> Result<Value> {
    let path = integration.run_path(run_id);
    let advice = format!(
        "It may still be running — follow it with `{}`, or raise --timeout",
        integration.follow_command(run_id)
    );

    wait::poll(
        Wait::new(format!("publication run {run_id}"), timeout, interval).advising(advice),
        || async {
            let run = api.get(&path, &Query::new()).await?;
            if is_terminal(&util::status_of(&run, "status")) {
                return Ok(Progress::Done(run));
            }

            Ok(Progress::Waiting(progress_label(&run)))
        },
    )
    .await
}

async fn target_command(cmd: TargetCmd, api: &Client, json: bool) -> Result<()> {
    match cmd {
        TargetCmd::Get {
            integration,
            target,
        } => {
            let res = api.get(&integration.path(), &target_query(&target)).await?;
            check_target(&res, &target)?;
            output::print_json(&res);
        }
        TargetCmd::Set {
            integration,
            target,
            settings,
            data,
        } => {
            let data = data
                .map(|data| util::parse_json_object("--data", &data))
                .transpose()?;
            let path = integration.path();
            let query = target_query(&target);
            // Not there yet means this save creates it.
            let saved = api.get_optional(&path, &query).await?;
            match &saved {
                Some(saved) => check_target(saved, &target)?,
                // An old tenant would create `default` instead. It answers `/targets` with its
                // web app, which `request` refuses; a current one 404s while there's no target.
                None if is_named(&target) => {
                    api.get_optional(&format!("{path}/targets"), &Query::new())
                        .await
                        .context(
                            "this tenant could not confirm it supports named publication \
                             targets, so saving could create `default` instead",
                        )?;
                }
                None => {}
            }
            let body = save_body(saved.as_ref(), data, settings);
            let res = api.request(Method::PUT, &path, &query, Some(&body)).await?;
            check_target(&res, &target)?;
            if json {
                output::print_json(&res);
            } else {
                output::success(&format!(
                    "Saved publication target {} of data source {} at configuration version {}",
                    util::one_cell(&output::field(&res, "targetName")),
                    integration.data_source,
                    util::one_cell(&output::field(&res, "configurationVersion"))
                ));
            }
        }
        TargetCmd::Delete {
            integration,
            target,
        } => {
            let path = integration.path();
            let target = Some(target);
            let mut query = target_query(&target);
            let saved = api.get(&path, &query).await?;
            check_target(&saved, &target)?;
            query.push((
                "expectedConfigurationVersion".to_string(),
                configuration_version(&saved)?.to_string(),
            ));
            let res = api.request(Method::DELETE, &path, &query, None).await?;
            if json {
                output::print_json(&res);
            } else {
                output::success(&format!(
                    "Deleted publication target {} of data source {}",
                    util::one_cell(&output::field(&res, "targetName")),
                    integration.data_source
                ));
                let retained =
                    output::items(res.get("retainedMetricViews").unwrap_or(&Value::Null));
                if !retained.is_empty() {
                    println!(
                        "These Metric Views stay in Databricks, managed by no target. Drop them, \
                         or take them over from another target:"
                    );
                    for view in retained.iter().filter_map(Value::as_str) {
                        println!("  {}", util::one_cell(view));
                    }
                }
            }
        }
    }

    Ok(())
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::Targets { integration } => {
            let path = format!("{}/targets", integration.path());
            let res = api.get(&path, &Query::new()).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("TARGET", "targetName"),
                    ("ENABLED", "enabled"),
                    ("SYNC ON DEPLOY", "syncOnDeploy"),
                    ("CATALOG", "targetCatalog"),
                    ("SCHEMA", "targetSchema"),
                    ("VERSION", "configurationVersion"),
                    ("LAST RUN", "lastRun.runId"),
                    ("LAST STATUS", "lastRun.status"),
                ],
            );
        }
        Cmd::Target { cmd } => target_command(cmd, &api, ctx.json).await?,
        Cmd::Sync {
            integration,
            target,
            promote_from,
            idempotency_key,
            wait,
            timeout,
            poll,
        } => {
            let path = integration.path();
            let query = target_query(&target);
            let settings = api.get(&path, &query).await?;
            check_target(&settings, &target)?;
            let body = sync_body(
                configuration_version(&settings)?,
                &promote_from,
                &idempotency_key,
            );
            let started = api
                .request(Method::POST, &format!("{path}/syncs"), &query, Some(&body))
                .await?;
            let run_id = started_run_id(&started)?;

            if !wait {
                if ctx.json {
                    output::print_json(&started);
                } else {
                    let verb = if started.get("reused") == Some(&Value::Bool(true)) {
                        "Found the run this idempotency key already started:"
                    } else {
                        "Started publication run"
                    };
                    output::success(&format!(
                        "{verb} {run_id} of target {} (build {})",
                        util::one_cell(&output::field(&started, "targetName")),
                        util::one_cell(&output::field(&started, "buildJobId"))
                    ));
                    println!("Follow it with `{}`.", integration.follow_command(&run_id));
                }

                return Ok(());
            }

            eprintln!("Publication run {run_id} started");
            let run = wait_for_run(&api, &integration, &run_id, timeout, poll).await?;
            finish(&run, ctx.json)?;
        }
        Cmd::Runs {
            integration,
            target,
            status,
            first,
            after,
        } => {
            let mut query = target_query(&target);
            util::push(&mut query, "status", &status);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(&format!("{}/syncs", integration.path()), &query)
                .await?;
            for run in output::items(&res) {
                check_target(&run, &target)?;
            }
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("RUN ID", "runId"),
                    ("STATUS", "status"),
                    ("TRIGGER", "trigger"),
                    ("BUILD", "buildJobId"),
                    ("PROMOTED FROM", "promotedFromRunId"),
                    ("INCOMPLETE", "incompleteViewCount"),
                    ("CREATED", "createdAt"),
                ],
            );
        }
        Cmd::Run {
            integration,
            run_id,
            wait,
            timeout,
            poll,
        } => {
            if wait {
                let run = wait_for_run(&api, &integration, &run_id, timeout, poll).await?;
                return finish(&run, ctx.json);
            }

            let run = api
                .get(&integration.run_path(&run_id), &Query::new())
                .await?;
            print_run(&run, ctx.json);
        }
        Cmd::Cancel {
            integration,
            run_id,
        } => {
            let run = api.delete(&integration.run_path(&run_id), None).await?;
            if ctx.json {
                output::print_json(&run);
            } else {
                output::success(&format!(
                    "Requested cancellation of publication run {run_id}; it is {}",
                    util::one_cell(&util::status_of(&run, "status"))
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;

    fn parse(args: &[&str]) -> Result<crate::Cli, clap::Error> {
        crate::Cli::try_parse_from(["cube", "dbx-mv"].iter().chain(args))
    }

    fn settings(args: &[&str]) -> Settings {
        let args = ["target", "set", "1", "default"].iter().chain(args);
        match parse(&args.copied().collect::<Vec<_>>()).unwrap().command {
            Some(crate::Command::DbxMv(Args {
                cmd:
                    Cmd::Target {
                        cmd: TargetCmd::Set { settings, .. },
                    },
            })) => settings,
            _ => panic!("not a target set"),
        }
    }

    #[test]
    fn arguments_parse_as_documented() {
        assert!(parse(&["targets", "1", "default"]).is_ok());
        assert!(parse(&["target", "get", "1", "default", "--target", "staging"]).is_ok());
        assert!(parse(&["target", "set", "1", "default", "--data", "@t.json"]).is_ok());
        assert!(parse(&["target", "delete", "1", "default", "--target", "staging"]).is_ok());
        assert!(parse(&[
            "sync",
            "1",
            "default",
            "--target",
            "production",
            "--promote-from",
            "run-1",
            "--idempotency-key",
            "k",
            "--wait",
            "--timeout",
            "1h",
            "--poll",
            "10s"
        ])
        .is_ok());
        assert!(parse(&["runs", "1", "default", "--target", "staging", "--first", "5"]).is_ok());
        assert!(parse(&["run", "1", "default", "run-1", "--wait"]).is_ok());
        assert!(parse(&["cancel", "1", "default", "run-1"]).is_ok());

        // A delete has to name the target it removes.
        assert!(parse(&["target", "delete", "1", "default"]).is_err());
        // A wait budget needs a wait.
        let err = parse(&["sync", "1", "default", "--timeout", "1m"])
            .err()
            .unwrap();
        assert!(err.to_string().contains("--wait"), "{err}");
        // An empty value (an unset CI variable) is refused rather than sent as one.
        assert!(parse(&["sync", "1", "default", "--target", ""]).is_err());
        assert!(parse(&["sync", "1", "default", "--promote-from", " "]).is_err());
        assert!(parse(&["targets", "1", ""]).is_err());
        // A dot name would be read as a relative path, which encoding can't prevent.
        assert!(parse(&["targets", "1", ".."]).is_err());
        assert!(parse(&["run", "1", "default", "."]).is_err());
    }

    #[test]
    fn settings_flags_name_only_what_they_set() {
        assert_eq!(settings(&[]).fields(), Map::new());
        assert_eq!(
            Value::Object(
                settings(&[
                    "--enabled",
                    "--sync-on-deploy=false",
                    "--catalog",
                    "main",
                    "--schema",
                    "mv_staging",
                    "--selection-mode",
                    "selected",
                    "--views",
                    "orders,customers",
                    "--deletion-policy",
                    "delete-managed",
                ])
                .fields()
            ),
            json!({
                "enabled": true,
                "syncOnDeploy": false,
                "targetCatalog": "main",
                "targetSchema": "mv_staging",
                "selectionMode": "selected",
                "viewNames": ["orders", "customers"],
                "deletionPolicy": "delete-managed"
            })
        );
        // The value is attached with `=`, so a flag before the positionals cannot eat one.
        assert_eq!(
            settings(&["--enabled=false"]).fields()["enabled"],
            json!(false)
        );
    }

    fn saved() -> Value {
        json!({
            "targetName": "staging",
            "configurationVersion": 3,
            "configuration": {
                "version": 1,
                "enabled": true,
                "syncOnDeploy": true,
                "targetCatalog": "main",
                "targetSchema": "mv_staging",
                "selectionMode": "all",
                "deletionPolicy": "delete-managed",
                "connection": {"host": "h", "httpPath": "/sql", "clientId": "c"}
            }
        })
    }

    #[test]
    fn a_save_keeps_the_saved_settings_the_flags_do_not_change() {
        assert_eq!(
            save_body(Some(&saved()), None, settings(&["--enabled=false"])),
            json!({
                "enabled": false,
                "syncOnDeploy": true,
                "targetCatalog": "main",
                "targetSchema": "mv_staging",
                "selectionMode": "all",
                "deletionPolicy": "delete-managed",
                "expectedConfigurationVersion": 3
            })
        );
    }

    #[test]
    fn a_new_selection_mode_drops_the_saved_views_and_pattern() {
        let mut selected = saved();
        selected["configuration"]["selectionMode"] = json!("selected");
        selected["configuration"]["viewNames"] = json!(["orders"]);

        let all = save_body(
            Some(&selected),
            None,
            settings(&["--selection-mode", "all"]),
        );
        assert_eq!(all["selectionMode"], json!("all"));
        assert_eq!(all.get("viewNames"), None);

        let pattern = save_body(
            Some(&selected),
            None,
            settings(&["--selection-mode", "pattern", "--view-name-regex", "^s"]),
        );
        assert_eq!(pattern.get("viewNames"), None);
        assert_eq!(pattern["viewNameRegex"], json!("^s"));

        // Without a new mode, the saved views stay.
        let kept = save_body(Some(&selected), None, settings(&["--enabled=false"]));
        assert_eq!(kept["viewNames"], json!(["orders"]));
    }

    #[test]
    fn a_save_of_a_new_target_creates_it_at_version_zero() {
        assert_eq!(
            save_body(
                None,
                None,
                settings(&["--enabled", "--catalog", "main", "--schema", "s"])
            ),
            json!({
                "enabled": true,
                "targetCatalog": "main",
                "targetSchema": "s",
                "expectedConfigurationVersion": 0
            })
        );
    }

    #[test]
    fn data_replaces_the_saved_settings_and_may_pin_the_version() {
        let data = json!({"enabled": true, "targetCatalog": "c", "targetSchema": "s", "selectionMode": "all"});
        assert_eq!(
            save_body(
                Some(&saved()),
                Some(data.as_object().unwrap().clone()),
                settings(&["--schema", "flag"])
            ),
            json!({
                "enabled": true,
                "targetCatalog": "c",
                "targetSchema": "flag",
                "selectionMode": "all",
                "expectedConfigurationVersion": 3
            })
        );

        // A version the caller read earlier is sent as given, so a stale save fails.
        let pinned = json!({"enabled": true, "expectedConfigurationVersion": 2});
        assert_eq!(
            save_body(
                Some(&saved()),
                Some(pinned.as_object().unwrap().clone()),
                settings(&[])
            ),
            pinned
        );
    }

    #[test]
    fn a_run_starts_at_the_version_read_with_only_the_options_given() {
        assert_eq!(
            sync_body(json!(3), &None, &None),
            json!({"configurationVersion": 3})
        );
        assert_eq!(
            sync_body(json!(3), &Some("run-1".to_string()), &Some("k".to_string())),
            json!({"configurationVersion": 3, "promoteFromRunId": "run-1", "idempotencyKey": "k"})
        );
        assert_eq!(configuration_version(&saved()).unwrap(), json!(3));
        assert!(configuration_version(&json!({})).is_err());
    }

    #[test]
    fn names_cannot_address_another_route() {
        let integration = |data_source: &str| Integration {
            deployment: 1,
            data_source: data_source.to_string(),
        };
        assert_eq!(
            integration("default").path(),
            "/api/v1/deployments/1/databricks-metric-view-integrations/default"
        );
        assert_eq!(
            integration("my ds/x").run_path("a?b#c"),
            "/api/v1/deployments/1/databricks-metric-view-integrations/my%20ds%2Fx/syncs/a%3Fb%23c"
        );
        assert_eq!(
            integration("my ds").follow_command("r-1"),
            "cube dbx-mv run 1 'my ds' 'r-1' --wait"
        );
        assert!(target_query(&None).is_empty());
        assert_eq!(
            target_query(&Some("staging".to_string())),
            vec![("target".to_string(), "staging".to_string())]
        );
    }

    #[test]
    fn settings_must_belong_to_the_requested_target() {
        let staging = Some("staging".to_string());
        assert!(check_target(&json!({"targetName": "staging"}), &staging).is_ok());
        assert!(check_target(
            &json!({"targetName": "staging"}),
            &Some("Staging".to_string())
        )
        .is_ok());
        assert!(check_target(&json!({"targetName": "default"}), &None).is_ok());
        // A tenant from before named targets answers for `default` with no targetName.
        assert!(check_target(&json!({}), &None).is_ok());
        let err = check_target(&json!({}), &staging).unwrap_err().to_string();
        assert!(err.contains("answered for `default`"), "{err}");
        assert!(check_target(&json!({"targetName": "production"}), &staging).is_err());

        assert!(is_named(&staging));
        assert!(!is_named(&None));
        assert!(!is_named(&Some("Default".to_string())));
    }

    #[test]
    fn a_started_run_needs_a_printable_id() {
        assert_eq!(started_run_id(&json!({"runId": "r-1"})).unwrap(), "r-1");
        assert!(started_run_id(&json!({"runId": " "})).is_err());
        let err = started_run_id(&json!({"runId": "r\u{1b}]0;x\u{7}"}))
            .unwrap_err()
            .to_string();
        assert!(!err.contains('\u{1b}'), "{err:?}");
    }

    #[test]
    fn only_completed_partial_failed_and_cancelled_end_a_wait() {
        for status in ["COMPLETED", "PARTIAL", "FAILED", "CANCELLED"] {
            assert!(is_terminal(status), "{status}");
        }
        for status in ["QUEUED", "RUNNING", "CANCELLING", "", "SOMETHING_NEW"] {
            assert!(!is_terminal(status), "{status}");
        }
    }

    #[test]
    fn only_a_complete_run_passes_the_wait() {
        assert_eq!(
            gate_failure(&json!({"runId": "r-1", "status": "COMPLETED", "incompleteViewCount": 0})),
            None
        );
        for status in ["PARTIAL", "FAILED", "CANCELLED"] {
            assert_eq!(
                gate_failure(&json!({"runId": "r-1", "status": status})).unwrap(),
                format!("publication run r-1 ended {status}, not COMPLETED")
            );
        }
        // Promotion refuses a run that withheld measures, so the gate does too.
        let incomplete =
            gate_failure(&json!({"runId": "r-1", "status": "COMPLETED", "incompleteViewCount": 2}))
                .unwrap();
        assert!(incomplete.contains("2 view(s)"), "{incomplete}");
        // Without the count, the gate can't tell, so it fails rather than passing.
        let unknown = gate_failure(&json!({"runId": "r-1", "status": "COMPLETED"})).unwrap();
        assert!(unknown.contains("no incompleteViewCount"), "{unknown}");
        assert!(finish(&json!({"runId": "r-1", "status": "FAILED"}), true).is_err());
    }

    #[test]
    fn progress_reports_the_stage_and_published_views() {
        assert_eq!(progress_label(&json!({"status": "QUEUED"})), "QUEUED");
        assert_eq!(
            progress_label(&json!({
                "status": "RUNNING",
                "progress": {"stage": "publishing", "published": 3, "total": 10}
            })),
            "RUNNING publishing 3/10"
        );
        assert_eq!(
            progress_label(
                &json!({"progress": {"stage": "publishing", "published": 1, "total": 2}})
            ),
            "publishing 1/2"
        );
        assert_eq!(
            progress_label(&json!({
                "status": "RUNNING",
                "progress": {"stage": "CONVERTING", "published": 0, "total": 0}
            })),
            "RUNNING CONVERTING"
        );
        assert_eq!(progress_label(&json!({})), "waiting");
    }

    #[test]
    fn a_run_summary_names_the_build_and_each_view() {
        let run = json!({
            "runId": "r-2",
            "targetName": "production",
            "status": "COMPLETED",
            "buildJobId": 42,
            "commit": "abc123",
            "promotedFromRunId": "r-1",
            "views": [
                {"cubeViewName": "orders", "outcome": "created", "target": "main.prod.orders",
                 "detail": null, "withheldMembers": []},
                {"cubeViewName": "sales", "outcome": "updated", "target": "main.prod.sales",
                 "detail": null, "withheldMembers": ["orders.a", "orders.b"]}
            ]
        });
        assert_eq!(
            run_lines(&run),
            vec![
                "Publication run r-2 of target production: COMPLETED".to_string(),
                "Build 42 (commit abc123), promoted from run r-1".to_string(),
            ]
        );
        assert_eq!(
            view_rows(&run),
            vec![
                vec!["orders", "created", "main.prod.orders", "", ""],
                vec![
                    "sales",
                    "updated",
                    "main.prod.sales",
                    "",
                    "orders.a, orders.b"
                ],
            ]
        );
    }
}
