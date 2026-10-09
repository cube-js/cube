use std::io::Write as _;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context as _, Result};
use clap::Subcommand;
use serde_json::{json, Map, Value};

use crate::client::{self, Client, Query};
use crate::commands::deploy;
use crate::wait::{self, Progress, Wait};
use crate::{output, util, Ctx};

/// Convert between a Cube data model and Apache Ossie (Open Semantic Interchange) in Cube
/// Cloud. Neither direction changes the deployment.
#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Convert a Cube data model to an Ossie model
    ///
    /// Converts the YAML data model of the deployment's latest successful build, or the
    /// Cube model files under --from-dir.
    Export {
        /// Deployment id
        deployment: i64,
        /// Convert only this view's public members
        #[arg(long, value_parser = util::nonempty_filter)]
        view: Option<String>,
        /// Fail on a measure whose value a join fan-out could change, instead of converting
        /// it with a warning. On by default with --view; `--strict-fanout=false` turns it off
        #[arg(
            long,
            value_name = "BOOL",
            num_args = 0..=1,
            require_equals = true,
            default_missing_value = "true"
        )]
        strict_fanout: Option<bool>,
        /// Convert the Cube YAML model files under this directory instead of the deployed
        /// model. Pass the project root, so the paths match the deployed ones (`model/...`)
        #[arg(long, value_name = "DIR", value_parser = util::nonempty_path)]
        from_dir: Option<String>,
        /// Wait for the conversion, then write the Ossie model
        #[arg(long)]
        wait: bool,
        /// File to write the Ossie model to (`-` for stdout)
        #[arg(
            long,
            value_name = "FILE",
            default_value = "ossie.yaml",
            value_parser = util::nonempty_path,
            requires = "wait"
        )]
        out: String,
        /// Give up waiting after this long
        #[arg(long, default_value = "10m", value_parser = util::parse_duration, requires = "wait")]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "2s", value_parser = util::parse_duration, requires = "wait")]
        poll: Duration,
    },
    /// Convert an Ossie model to Cube data model files and write them locally
    Import {
        /// Deployment id
        deployment: i64,
        /// Read the Ossie model from this file (`-` for stdin)
        #[arg(long, value_name = "FILE", value_parser = util::nonempty_path)]
        file: String,
        /// Warehouse dialect whose expressions to prefer: ANSI_SQL, SNOWFLAKE, DATABRICKS
        /// or BIGQUERY. Defaults to the dialect of the deployment's data source
        #[arg(long, value_parser = util::nonempty_filter)]
        dialect: Option<String>,
        /// Cube to root the generated view at, when the Ossie model's relationships do not
        /// determine one
        #[arg(long, value_parser = util::nonempty_filter)]
        base_cube: Option<String>,
        /// Directory to write into — the root of your data model project, since the
        /// generated paths are project-relative (`model/cubes/...`)
        #[arg(long, value_name = "DIR", default_value = ".", value_parser = util::nonempty_path)]
        out: String,
        /// Compare with what is on disk instead of writing, and exit non-zero if any file
        /// is missing or differs
        #[arg(long)]
        check: bool,
        /// Give up waiting after this long
        #[arg(long, default_value = "10m", value_parser = util::parse_duration)]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "2s", value_parser = util::parse_duration)]
        poll: Duration,
    },
    /// Show a conversion's status
    Status {
        /// Deployment id
        deployment: i64,
        /// Conversion id, as returned by `export` or `import`
        conversion_id: String,
        /// Wait for the conversion to finish, then show its result
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long
        #[arg(long, default_value = "10m", value_parser = util::parse_duration, requires = "wait")]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "2s", value_parser = util::parse_duration, requires = "wait")]
        poll: Duration,
    },
    /// Show a finished conversion's counts and issues
    Result {
        /// Deployment id
        deployment: i64,
        /// Conversion id, as returned by `export` or `import`
        conversion_id: String,
        /// Also write the converted files: an export's Ossie model to this file (`-` for
        /// stdout), or an import's Cube files under this directory
        #[arg(long, value_name = "PATH", value_parser = util::nonempty_path)]
        out: Option<String>,
    },
    /// Cancel a running conversion
    Cancel {
        /// Deployment id
        deployment: i64,
        /// Conversion id, as returned by `export` or `import`
        conversion_id: String,
    },
}

// Everything else, including a status this build has never heard of, means still running.
const COMPLETED: &str = "COMPLETED";
const FAILED: &str = "FAILED";
const CANCELLED: &str = "CANCELLED";

/// A conversion is briefly invisible right after it starts, so a 404 is tolerated for
/// this long; a mistyped id still fails well before the timeout.
const MISSING_GRACE: Duration = Duration::from_secs(60);

/// Reading a finished conversion's result only has to ride out a transient failure.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const FETCH_POLL: Duration = Duration::from_secs(2);

fn base(deployment: i64) -> String {
    format!("/api/v1/deployments/{deployment}/ossie-conversions")
}

fn conversion_path(deployment: i64, id: &str) -> String {
    format!("{}/{id}", base(deployment))
}

fn export_body(
    view: &Option<String>,
    strict_fanout: Option<bool>,
    files: Option<Map<String, Value>>,
) -> Value {
    let mut body = Map::new();
    body.insert("direction".to_string(), json!("export"));
    // Without files, the server's default source converts the deployed model.
    if let Some(files) = files {
        body.insert("source".to_string(), json!("files"));
        body.insert("files".to_string(), Value::Object(files));
    }
    util::set(&mut body, "view", view);
    util::set(&mut body, "strictFanout", &strict_fanout);

    util::body(body)
}

fn import_body(ossie_yaml: String, dialect: &Option<String>, base_cube: &Option<String>) -> Value {
    let mut body = Map::new();
    body.insert("direction".to_string(), json!("import"));
    body.insert("ossieYaml".to_string(), Value::String(ossie_yaml));
    util::set(&mut body, "dialect", dialect);
    util::set(&mut body, "baseCube", base_cube);

    util::body(body)
}

/// Whether a YAML file declares `cubes:` or `views:` at its root, a lexical form of the test
/// the server applies to a deployed model.
fn is_cube_model_yaml(content: &str) -> bool {
    // Some Windows editors start a file with a byte-order mark, which YAML ignores.
    let content = content.strip_prefix('\u{feff}').unwrap_or(content);
    const ROOT_KEYS: [&str; 6] = [
        "cubes",
        "views",
        "\"cubes\"",
        "\"views\"",
        "'cubes'",
        "'views'",
    ];

    content.lines().any(|line| {
        ROOT_KEYS.iter().any(|key| {
            line.strip_prefix(key)
                .is_some_and(|rest| rest.trim_start().starts_with(':'))
        })
    })
}

/// The Cube model files under `dir`, keyed by their path relative to it. Only model YAML is
/// sent, so other YAML in a project (docker-compose.yml, CI config) never leaves the machine.
fn model_files(dir: &Path) -> Result<Map<String, Value>> {
    if !dir.is_dir() {
        bail!("{} is not a directory", dir.display());
    }

    let mut found = Vec::new();
    deploy::collect_files(dir, dir, &mut found)?;

    let mut files = Map::new();
    for (rel, path) in found {
        // Case-sensitive, like the schema compiler: Cube does not compile `orders.YAML`.
        if !rel.ends_with(".yml") && !rel.ends_with(".yaml") {
            continue;
        }

        let content = match std::fs::read_to_string(&path) {
            Ok(content) => content,
            // Not UTF-8, so not a model the server would accept: skip it like other YAML.
            Err(err) if err.kind() == std::io::ErrorKind::InvalidData => continue,
            Err(err) => {
                return Err(
                    anyhow::Error::new(err).context(format!("failed to read {}", path.display()))
                )
            }
        };
        if is_cube_model_yaml(&content) {
            files.insert(rel, Value::String(content));
        }
    }

    if files.is_empty() {
        bail!(
            "no Cube YAML model files (with a top-level `cubes:` or `views:`) under {}",
            dir.display()
        );
    }

    Ok(files)
}

fn started_id(started: &Value) -> Result<String> {
    let id = output::field(started, "conversionId");
    if util::is_blank(&id) {
        bail!("Cube started the Ossie conversion but returned no conversion id");
    }
    // Every message names the id, and one with control characters could drive the terminal.
    if id.chars().any(char::is_control) {
        bail!(
            "Cube started the Ossie conversion but returned the unusable conversion id {}",
            id.escape_debug()
        );
    }

    Ok(id)
}

fn not_found(deployment: i64, id: &str) -> anyhow::Error {
    anyhow!(
        "no Ossie conversion {id} on deployment {deployment}. It may not be visible yet, \
         belong to another deployment, or have expired: conversions are kept for one day"
    )
}

fn is_terminal(state: &str) -> bool {
    matches!(state, COMPLETED | FAILED | CANCELLED)
}

fn status_label(status: &Value) -> String {
    let state = util::one_cell(&util::status_of(status, "status"));
    let error = util::one_line(
        &util::printable(&output::field(status, "error")),
        util::REASON_LIMIT,
    );

    if util::is_blank(&error) {
        state
    } else {
        format!("{state} — {error}")
    }
}

/// The error for a conversion that ended without a result, if it did.
fn ended_without_result(id: &str, status: &Value) -> Option<anyhow::Error> {
    match util::status_of(status, "status").as_str() {
        FAILED => {
            let error = util::one_line(
                &util::printable(&output::field(status, "error")),
                util::REASON_LIMIT,
            );
            let reason = if util::is_blank(&error) {
                "(no reason reported)"
            } else {
                &error
            };
            Some(anyhow!("Ossie conversion {id} failed: {reason}"))
        }
        CANCELLED => Some(anyhow!("Ossie conversion {id} was cancelled")),
        _ => None,
    }
}

async fn get_status(api: &Client, deployment: i64, id: &str) -> Result<Value> {
    api.get_optional(&conversion_path(deployment, id), &Query::new())
        .await?
        .ok_or_else(|| not_found(deployment, id))
}

/// Poll until the conversion reaches a terminal status, and return that status.
async fn wait_for_conversion(
    api: &Client,
    deployment: i64,
    id: &str,
    timeout: Duration,
    interval: Duration,
) -> Result<Value> {
    let path = conversion_path(deployment, id);
    // A Cell because the poll closure's futures borrow what it captures.
    let missing_since = std::cell::Cell::new(None::<Instant>);

    wait::poll(Wait::new("Ossie conversion", timeout, interval), || async {
        let Some(status) = api.get_optional(&path, &Query::new()).await? else {
            let since = missing_since.get().unwrap_or_else(Instant::now);
            missing_since.set(Some(since));
            if since.elapsed() > MISSING_GRACE {
                return Err(not_found(deployment, id));
            }

            return Ok(Progress::Waiting("starting".to_string()));
        };
        missing_since.set(None);

        let state = util::status_of(&status, "status");
        if is_terminal(&state) {
            return Ok(Progress::Done(status));
        }

        Ok(Progress::Waiting(if state.is_empty() {
            "waiting".to_string()
        } else {
            util::one_cell(&state)
        }))
    })
    .await
}

/// A finished conversion's result answers 409 for a moment: the job reports COMPLETED
/// before it has closed, and only a closed job serves its result.
fn result_progress(response: Result<Value>) -> Result<Progress<Value>> {
    match response {
        Ok(value) => Ok(Progress::Done(value)),
        Err(err)
            if err
                .downcast_ref::<client::ApiError>()
                .is_some_and(|api| api.status == reqwest::StatusCode::CONFLICT) =>
        {
            Ok(Progress::Waiting("closing".to_string()))
        }
        Err(err) => Err(err),
    }
}

/// GET a finished conversion's document, riding out transient failures and the 409 above.
async fn fetch(api: &Client, path: &str, what: &str) -> Result<Value> {
    wait::poll(
        Wait::new(what, FETCH_TIMEOUT, FETCH_POLL).advising_nothing(),
        || async { result_progress(api.get(path, &Query::new()).await) },
    )
    .await
}

async fn fetch_files(api: &Client, deployment: i64, id: &str) -> Result<Vec<(String, String)>> {
    let path = format!("{}/files", conversion_path(deployment, id));
    let payload = fetch(api, &path, "Ossie conversion files").await?;
    // Without `first` the server returns every file; a gate must not pass on a partial list.
    if payload.pointer("/pageInfo/hasNextPage") == Some(&Value::Bool(true)) {
        bail!("Ossie conversion {id} returned an incomplete file list");
    }

    util::read_generated_files(&payload, &format!("Ossie conversion {id}"))
}

/// Where a finished conversion's files go.
enum Out {
    /// Nowhere: only report the result.
    Nothing,
    /// An export's Ossie model, to this file or `-` for stdout.
    Ossie(String),
    /// An import's Cube files, beneath this directory, or compared with it for `check`.
    Cube { dir: String, check: bool },
}

/// The Ossie model on stdout would collide with the `--json` report there.
fn ossie_out(path: String, json: bool) -> Result<Out> {
    if json && path == "-" {
        bail!("--json prints its report on stdout, so write the Ossie model with --out FILE");
    }

    Ok(Out::Ossie(path))
}

/// The `--json` document's common fields: the job's status and the conversion's result,
/// which are null when the job ended without one. `finish` adds where the files went.
fn report(id: &str, status: &Value, result: Option<&Value>) -> Value {
    let result_field = |key: &str| {
        result
            .and_then(|result| result.get(key))
            .cloned()
            .unwrap_or(Value::Null)
    };

    json!({
        "conversionId": id,
        "status": util::status_of(status, "status"),
        "error": status.get("error").cloned().unwrap_or(Value::Null),
        "outcome": result_field("outcome"),
        "counts": result_field("counts"),
        "issues": result_field("issues"),
    })
}

/// The human summary of a result: what converted, issue counts by severity, then each issue.
fn summary_lines(result: &Value) -> Vec<String> {
    let count = |key: &str| util::one_cell(&output::field(result, &format!("counts.{key}")));

    let mut lines = vec![
        format!(
            "Cube: {} cube(s), {} view(s), {} file(s). Ossie: {} dataset(s), {} \
             relationship(s), {} field(s), {} metric(s).",
            count("cubes"),
            count("views"),
            count("files"),
            count("datasets"),
            count("relationships"),
            count("fields"),
            count("metrics"),
        ),
        format!(
            "Issues: {} error(s), {} warning(s), {} info.",
            count("issues.error"),
            count("issues.warning"),
            count("issues.info"),
        ),
    ];
    for issue in output::items(result.get("issues").unwrap_or(&Value::Null)) {
        let element = util::one_cell(&output::field(&issue, "element"));
        let message = util::one_line(
            &util::printable(&output::field(&issue, "message")),
            util::REASON_LIMIT,
        );
        lines.push(format!(
            "  {} {}{}: {message}",
            util::one_cell(&output::field(&issue, "severity")),
            util::one_cell(&output::field(&issue, "code")),
            if util::is_blank(&element) {
                String::new()
            } else {
                format!(" {element}")
            },
        ));
    }

    lines
}

/// Report a conversion that reached a terminal status, and write or check its files per
/// `out`. Fails when it ended without a result, its outcome is not `success`, or `--check`
/// found a difference.
async fn finish(
    api: &Client,
    deployment: i64,
    id: &str,
    status: &Value,
    out: Out,
    json: bool,
) -> Result<()> {
    if let Some(err) = ended_without_result(id, status) {
        if json {
            output::print_json(&report(id, status, None));
        }

        return Err(err);
    }

    let result_path = format!("{}/result", conversion_path(deployment, id));
    let result = fetch(api, &result_path, "Ossie conversion result")
        .await
        .with_context(|| {
            format!(
                "Ossie conversion {id} completed, but its result could not be read. Read it \
                 with `cube ossie result {deployment} {}`",
                util::shell_quote(id)
            )
        })?;

    // Stdout may be carrying the Ossie model.
    let to_stderr = matches!(&out, Out::Ossie(path) if path == "-");
    if !json {
        for line in summary_lines(&result) {
            if to_stderr {
                eprintln!("{line}");
            } else {
                println!("{line}");
            }
        }
    }

    let mut doc = report(id, status, Some(&result));
    if util::status_of(&result, "outcome") != "success" {
        if json {
            output::print_json(&doc);
        }

        bail!(
            "Ossie conversion {id} ended with outcome `{}`, not `success`; see its issues",
            util::one_cell(&util::status_of(&result, "outcome"))
        );
    }

    // The document goes out on failure too: its `conversionId` is how a pipeline recovers.
    let delivered = deliver(api, deployment, id, out, json, &mut doc).await;
    if json {
        output::print_json(&doc);
    }

    delivered
}

fn paths_json(files: &[(String, String)]) -> Value {
    files.iter().map(|(path, _)| json!(path)).collect()
}

fn differing_json(differing: &[(String, &str)]) -> Value {
    differing
        .iter()
        .map(|(path, status)| json!({"path": path, "status": status}))
        .collect()
}

/// Write or check a successful conversion's files per `out`, recording where they went in
/// `doc`.
async fn deliver(
    api: &Client,
    deployment: i64,
    id: &str,
    out: Out,
    json: bool,
    doc: &mut Value,
) -> Result<()> {
    match out {
        Out::Nothing => {}
        Out::Ossie(path) => {
            let files = fetch_files(api, deployment, id).await?;
            let [(_, content)] = files.as_slice() else {
                bail!(
                    "Ossie conversion {id} returned {} files instead of one Ossie model",
                    files.len()
                );
            };

            if path == "-" {
                std::io::stdout().write_all(content.as_bytes())?;
            } else {
                std::fs::write(&path, content)
                    .with_context(|| format!("could not write {path}"))?;
                if !json {
                    output::success(&format!("Wrote the Ossie model to {path}"));
                }
            }
            doc["outputPath"] = json!(path);
        }
        Out::Cube { dir, check } => {
            let files = fetch_files(api, deployment, id).await?;
            if files.is_empty() {
                bail!("Ossie conversion {id} returned no files");
            }
            doc["files"] = paths_json(&files);

            let root = Path::new(&dir);
            if !check {
                let targets = util::write_files(root, &files)?;
                if !json {
                    output::success(&format!("Wrote {} file(s) under {dir}", targets.len()));
                    for target in &targets {
                        println!("  {}", target.display());
                    }
                }

                return Ok(());
            }

            let differing = util::differing_files(root, &files)?;
            doc["differing"] = differing_json(&differing);
            if !differing.is_empty() {
                if !json {
                    for (path, status) in &differing {
                        eprintln!("  {} {status}", root.join(path).display());
                    }
                }

                bail!(
                    "{} of {} converted file(s) do not match what is on disk. Re-run without \
                     --check to write them",
                    differing.len(),
                    files.len()
                );
            }

            if !json {
                output::success(&format!(
                    "all {} converted file(s) match what is on disk",
                    files.len()
                ));
            }
        }
    }

    Ok(())
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::Export {
            deployment,
            view,
            strict_fanout,
            from_dir,
            wait,
            out,
            timeout,
            poll,
        } => {
            let out = ossie_out(out, ctx.json && wait)?;
            let files = match &from_dir {
                Some(dir) => {
                    let files = model_files(Path::new(dir))?;
                    eprintln!("Converting {} model file(s) from {dir}", files.len());
                    Some(files)
                }
                None => None,
            };

            let body = export_body(&view, strict_fanout, files);
            let started = api.post(&base(deployment), Some(&body)).await?;
            let id = started_id(&started)?;

            if !wait {
                if ctx.json {
                    output::print_json(&started);
                } else {
                    output::success(&format!("Started Ossie export {id}"));
                    println!(
                        "Once `cube ossie status {deployment} {0}` reports COMPLETED, write the \
                         Ossie model with `cube ossie result {deployment} {0} --out ossie.yaml`, \
                         or re-run export with --wait.",
                        util::shell_quote(&id)
                    );
                }

                return Ok(());
            }

            eprintln!("Ossie export {id} started");
            let status = wait_for_conversion(&api, deployment, &id, timeout, poll).await?;
            finish(&api, deployment, &id, &status, out, ctx.json).await
        }
        Cmd::Import {
            deployment,
            file,
            dialect,
            base_cube,
            out,
            check,
            timeout,
            poll,
        } => {
            let (ossie_yaml, _) = util::read_text(&file, "Ossie model")?;
            let body = import_body(ossie_yaml, &dialect, &base_cube);
            let started = api.post(&base(deployment), Some(&body)).await?;
            let id = started_id(&started)?;

            eprintln!("Ossie import {id} started");
            let status = wait_for_conversion(&api, deployment, &id, timeout, poll).await?;
            finish(
                &api,
                deployment,
                &id,
                &status,
                Out::Cube { dir: out, check },
                ctx.json,
            )
            .await
        }
        Cmd::Status {
            deployment,
            conversion_id,
            wait,
            timeout,
            poll,
        } => {
            if wait {
                let status =
                    wait_for_conversion(&api, deployment, &conversion_id, timeout, poll).await?;
                return finish(
                    &api,
                    deployment,
                    &conversion_id,
                    &status,
                    Out::Nothing,
                    ctx.json,
                )
                .await;
            }

            let status = get_status(&api, deployment, &conversion_id).await?;
            if ctx.json {
                output::print_json(&status);
            } else {
                println!("{}", status_label(&status));
            }

            Ok(())
        }
        Cmd::Result {
            deployment,
            conversion_id,
            out,
        } => {
            let status = get_status(&api, deployment, &conversion_id).await?;
            let state = util::status_of(&status, "status");
            if !is_terminal(&state) {
                bail!(
                    "Ossie conversion {conversion_id} is still {} — wait for it with \
                     `cube ossie status {deployment} {} --wait`",
                    util::one_cell(&state),
                    util::shell_quote(&conversion_id)
                );
            }

            let out = match out {
                None => Out::Nothing,
                Some(path) => match util::status_of(&status, "direction").as_str() {
                    "export" => ossie_out(path, ctx.json)?,
                    "import" => Out::Cube {
                        dir: path,
                        check: false,
                    },
                    other => bail!(
                        "Ossie conversion {conversion_id} has the unknown direction `{}`; \
                         update the CLI with `cube update`",
                        util::one_cell(other)
                    ),
                },
            };

            finish(&api, deployment, &conversion_id, &status, out, ctx.json).await
        }
        Cmd::Cancel {
            deployment,
            conversion_id,
        } => {
            let res = api
                .delete(&conversion_path(deployment, &conversion_id), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!(
                    "Cancelled Ossie conversion {conversion_id}, unless it had already finished"
                ));
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;
    use std::path::PathBuf;

    fn parse(args: &[&str]) -> Result<crate::Cli, clap::Error> {
        crate::Cli::try_parse_from(["cube", "ossie"].iter().chain(args))
    }

    /// A fresh directory for one test, removed first in case an earlier run left it.
    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("cube-cli-ossie-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(root: &Path, path: &str, content: &str) {
        let target = root.join(path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(target, content).unwrap();
    }

    #[test]
    fn arguments_parse_as_documented() {
        assert!(parse(&["export", "1"]).is_ok());
        assert!(parse(&["export", "1", "--wait", "--out", "-", "--view", "sales"]).is_ok());
        assert!(parse(&["import", "1", "--file", "ossie.yaml", "--check"]).is_ok());
        assert!(parse(&["status", "1", "abc", "--wait", "--timeout", "1m"]).is_ok());
        assert!(parse(&["result", "1", "abc", "--out", "ossie.yaml"]).is_ok());
        assert!(parse(&["cancel", "1", "abc"]).is_ok());

        // Writing the Ossie model needs a result to write.
        let err = parse(&["export", "1", "--out", "x.yaml"]).err().unwrap();
        assert!(err.to_string().contains("--wait"), "{err}");
        // An import has to name the document it converts.
        assert!(parse(&["import", "1"]).is_err());
        // An empty value is refused rather than sent as one.
        assert!(parse(&["import", "1", "--file", ""]).is_err());
        assert!(parse(&["export", "1", "--view", ""]).is_err());
    }

    #[test]
    fn strict_fanout_is_a_flag_that_can_also_be_turned_off() {
        let strict_fanout = |args: &[&str]| match parse(args).unwrap().command {
            Some(crate::Command::Ossie(Args {
                cmd: Cmd::Export { strict_fanout, .. },
            })) => strict_fanout,
            _ => panic!("not an export"),
        };

        assert_eq!(strict_fanout(&["export", "1"]), None);
        assert_eq!(
            strict_fanout(&["export", "1", "--strict-fanout"]),
            Some(true)
        );
        assert_eq!(
            strict_fanout(&["export", "1", "--strict-fanout=false"]),
            Some(false)
        );
        // The value is attached with `=`, so a flag before the deployment id cannot eat it.
        assert_eq!(
            strict_fanout(&["export", "--strict-fanout", "1"]),
            Some(true)
        );
    }

    #[test]
    fn an_export_converts_the_deployed_model_unless_files_are_sent() {
        assert_eq!(
            export_body(&None, None, None),
            json!({"direction": "export"})
        );

        let mut files = Map::new();
        files.insert("model/cubes/orders.yml".to_string(), json!("cubes: []\n"));
        assert_eq!(
            export_body(&Some("sales".to_string()), Some(false), Some(files)),
            json!({
                "direction": "export",
                "source": "files",
                "files": {"model/cubes/orders.yml": "cubes: []\n"},
                "view": "sales",
                "strictFanout": false
            })
        );
    }

    #[test]
    fn an_import_sends_the_document_and_only_the_options_given() {
        assert_eq!(
            import_body("version: 0.1\n".to_string(), &None, &None),
            json!({"direction": "import", "ossieYaml": "version: 0.1\n"})
        );
        assert_eq!(
            import_body(
                "x".to_string(),
                &Some("SNOWFLAKE".to_string()),
                &Some("orders".to_string())
            ),
            json!({
                "direction": "import",
                "ossieYaml": "x",
                "dialect": "SNOWFLAKE",
                "baseCube": "orders"
            })
        );
    }

    #[test]
    fn only_cube_model_yaml_is_recognised() {
        assert!(is_cube_model_yaml("cubes:\n  - name: orders\n"));
        assert!(is_cube_model_yaml("# orders\nviews :\n  - name: sales\n"));
        assert!(is_cube_model_yaml("\"cubes\": []\n"));
        // Nested keys and other YAML are not a Cube model.
        assert!(!is_cube_model_yaml(
            "services:\n  cubes:\n    image: cube\n"
        ));
        assert!(!is_cube_model_yaml("cubes_extra: 1\n"));
        assert!(!is_cube_model_yaml(""));
        // A byte-order mark is not part of the key.
        assert!(is_cube_model_yaml("\u{feff}cubes:\n  - name: orders\n"));
    }

    #[test]
    fn from_dir_sends_only_model_yaml_keyed_from_the_directory() {
        let root = scratch("from-dir");
        write(
            &root,
            "model/cubes/orders.yml",
            "cubes:\n  - name: orders\n",
        );
        write(&root, "model/views/sales.yaml", "views:\n  - name: sales\n");
        write(
            &root,
            "model/views/legacy.YAML",
            "views:\n  - name: legacy\n",
        );
        write(&root, "docker-compose.yml", "services:\n  cube: {}\n");
        write(&root, "cube.js", "module.exports = {};\n");
        write(&root, ".github/workflows/ci.yml", "cubes: []\n");
        write(&root, "node_modules/pkg/cubes.yml", "cubes: []\n");
        // A Latin-1 file elsewhere in the project is skipped rather than failing the export.
        let latin1 = root.join("config/legacy.yml");
        std::fs::create_dir_all(latin1.parent().unwrap()).unwrap();
        std::fs::write(&latin1, b"name: caf\xe9\n").unwrap();

        let files = model_files(&root).unwrap();
        assert_eq!(
            files.keys().collect::<Vec<_>>(),
            vec!["model/cubes/orders.yml", "model/views/sales.yaml"]
        );

        let empty = scratch("from-dir-empty");
        write(&empty, "docker-compose.yml", "services: {}\n");
        let err = model_files(&empty).unwrap_err().to_string();
        assert!(err.contains("no Cube YAML model files"), "{err}");
        assert!(model_files(&empty.join("missing")).is_err());

        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(empty);
    }

    #[test]
    fn a_started_conversion_needs_a_printable_id() {
        assert_eq!(started_id(&json!({"conversionId": "c-1"})).unwrap(), "c-1");
        assert!(started_id(&json!({"conversionId": " "})).is_err());
        let err = started_id(&json!({"conversionId": "c\u{1b}]0;pwned\u{7}"}))
            .unwrap_err()
            .to_string();
        assert!(!err.contains('\u{1b}'), "{err:?}");
    }

    #[test]
    fn only_completed_failed_and_cancelled_end_a_wait() {
        for state in [COMPLETED, FAILED, CANCELLED] {
            assert!(is_terminal(state), "{state}");
        }
        for state in ["QUEUED", "RUNNING", "", "SOMETHING_NEW"] {
            assert!(!is_terminal(state), "{state}");
        }
    }

    #[test]
    fn a_job_that_ended_without_a_result_says_why() {
        let failed = ended_without_result(
            "c-1",
            &json!({"status": "FAILED", "error": "sandbox\n timed out"}),
        )
        .unwrap();
        assert_eq!(
            failed.to_string(),
            "Ossie conversion c-1 failed: sandbox timed out"
        );
        // Server text cannot drive the terminal, here or in `status_label`.
        let escaped = ended_without_result(
            "c-1",
            &json!({"status": "FAILED", "error": "\u{1b}]0;pwned\u{7}boom"}),
        )
        .unwrap();
        assert_eq!(
            escaped.to_string(),
            "Ossie conversion c-1 failed: ]0;pwnedboom"
        );
        assert!(ended_without_result("c-1", &json!({"status": "FAILED"}))
            .unwrap()
            .to_string()
            .contains("(no reason reported)"));
        assert_eq!(
            ended_without_result("c-1", &json!({"status": "CANCELLED"}))
                .unwrap()
                .to_string(),
            "Ossie conversion c-1 was cancelled"
        );
        assert!(ended_without_result("c-1", &json!({"status": "COMPLETED"})).is_none());

        assert_eq!(status_label(&json!({"status": "RUNNING"})), "RUNNING");
        assert_eq!(
            status_label(&json!({"status": "FAILED", "error": "boom"})),
            "FAILED — boom"
        );
    }

    fn result() -> Value {
        json!({
            "conversionId": "c-1",
            "direction": "import",
            "outcome": "success",
            "counts": {
                "datasets": 4, "relationships": 3, "fields": 20, "metrics": 12,
                "cubes": 4, "views": 2, "files": 6,
                "issues": {"error": 0, "warning": 1, "info": 1}
            },
            "issues": [
                {"code": "DROPPED_MEASURE", "severity": "WARNING", "element": "orders.x", "message": "dropped"},
                {"code": "GEO_SPLIT", "severity": "INFO", "element": null, "message": "split\nin two"}
            ]
        })
    }

    #[test]
    fn the_json_report_has_one_shape_for_every_ending() {
        let completed = report(
            "c-1",
            &json!({"status": "COMPLETED", "error": null}),
            Some(&result()),
        );
        assert_eq!(completed["conversionId"], json!("c-1"));
        assert_eq!(completed["status"], json!("COMPLETED"));
        assert_eq!(completed["error"], Value::Null);
        assert_eq!(completed["outcome"], json!("success"));
        assert_eq!(completed["counts"]["cubes"], json!(4));
        assert_eq!(completed["issues"].as_array().unwrap().len(), 2);

        // A job that ended without a result still names itself and says why.
        let failed = report(
            "c-1",
            &json!({"status": " FAILED ", "error": "timed out"}),
            None,
        );
        assert_eq!(
            failed,
            json!({
                "conversionId": "c-1",
                "status": "FAILED",
                "error": "timed out",
                "outcome": null,
                "counts": null,
                "issues": null
            })
        );

        // An import adds the converted paths, and `--check` what differs from disk, keyed
        // the same way so a gate can join the two.
        let files = vec![("model/cubes/orders.yml".to_string(), "x".to_string())];
        assert_eq!(paths_json(&files), json!(["model/cubes/orders.yml"]));
        assert_eq!(
            differing_json(&[("model/cubes/orders.yml".to_string(), "differs")]),
            json!([{"path": "model/cubes/orders.yml", "status": "differs"}])
        );
        assert_eq!(differing_json(&[]), json!([]));
    }

    #[test]
    fn a_result_that_is_not_served_yet_is_waited_for() {
        let api_error = |status| {
            Err(anyhow::Error::new(client::ApiError {
                status,
                method: reqwest::Method::GET,
                path: "/result".to_string(),
                detail: String::new(),
            }))
        };

        // A COMPLETED job serves its result only once it has closed, a moment later.
        assert!(matches!(
            result_progress(api_error(reqwest::StatusCode::CONFLICT)),
            Ok(Progress::Waiting(_))
        ));
        assert!(matches!(
            result_progress(Ok(json!({"outcome": "success"}))),
            Ok(Progress::Done(_))
        ));
        // An expired result is an answer, not something to wait out.
        assert!(result_progress(api_error(reqwest::StatusCode::NOT_FOUND)).is_err());
    }

    #[test]
    fn the_summary_counts_issues_by_severity_then_lists_them() {
        assert_eq!(
            summary_lines(&result()),
            vec![
                "Cube: 4 cube(s), 2 view(s), 6 file(s). Ossie: 4 dataset(s), 3 relationship(s), \
                 20 field(s), 12 metric(s)."
                    .to_string(),
                "Issues: 0 error(s), 1 warning(s), 1 info.".to_string(),
                "  WARNING DROPPED_MEASURE orders.x: dropped".to_string(),
                "  INFO GEO_SPLIT: split in two".to_string(),
            ]
        );
    }

    #[test]
    fn the_ossie_model_and_the_json_report_cannot_share_stdout() {
        assert!(ossie_out("-".to_string(), true).is_err());
        assert!(matches!(
            ossie_out("-".to_string(), false),
            Ok(Out::Ossie(_))
        ));
        assert!(matches!(
            ossie_out("ossie.yaml".to_string(), true),
            Ok(Out::Ossie(_))
        ));
    }
}
