use std::io::{self, Write as _};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _, Result};
use clap::Subcommand;
use serde_json::{json, Value};

use crate::client::{self, Client, Query};
use crate::wait::{self, Progress, Wait};
use crate::{output, util, Ctx};

// The eval runner persists exactly these two terminal statuses.
const COMPLETED: &str = "completed";
const FAILED: &str = "failed";
// Fetching the terminal result page is a separate request after the user-facing
// wait. Give transient proxy/network errors a small, bounded recovery window so
// a completed run does not hang the CLI or fail on a single blip.
const RESULTS_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
// A run that becomes terminal at the status deadline still needs one bounded
// chance to fetch its verdict. This may extend --timeout by at most five seconds.
const RESULTS_FETCH_FLOOR: Duration = Duration::from_secs(5);
const RESULTS_FETCH_POLL_MAX: Duration = Duration::from_secs(5);

fn results_fetch_timeout(timeout: Duration, elapsed: Duration) -> Duration {
    timeout
        .saturating_sub(elapsed)
        .clamp(RESULTS_FETCH_FLOOR, RESULTS_FETCH_TIMEOUT)
}

/// Run and inspect AI agent evals.
#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start an eval run
    Run {
        /// Deployment id
        deployment: i64,
        /// Cube branch to evaluate
        #[arg(long, value_parser = util::nonempty)]
        branch: String,
        /// Agent to evaluate (defaults to the auto agent)
        #[arg(long, value_parser = util::nonempty_filter)]
        agent: Option<String>,
        /// Run one file under agents/, e.g. eval_questions/revenue.yml
        #[arg(long, value_name = "PATH", value_parser = util::nonempty_repo_path)]
        file: Option<String>,
        /// Security context to evaluate under, as JSON (inline, @file, or - for stdin); merged over
        /// yours, and not applied to the auto agent's own queries
        #[arg(long)]
        security_context: Option<String>,
        /// Wait for the run and exit non-zero unless every question passes
        #[arg(long)]
        wait: bool,
        /// Wait up to this long for the run; terminal results may take 5s more
        #[arg(long, default_value = "30m", value_parser = util::parse_duration, requires = "wait")]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration, requires = "wait")]
        poll: Duration,
    },
    /// List a deployment's eval runs, newest first
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Maximum number of runs to return (default 100, at most 200)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Show an eval run's status
    Status {
        /// Deployment id
        deployment: i64,
        /// Eval run id, as returned by `run` or `list`
        evaluation: i64,
        /// Wait for the run and exit non-zero unless every question passes
        #[arg(long)]
        wait: bool,
        /// Wait up to this long for the run; terminal results may take 5s more
        #[arg(long, default_value = "30m", value_parser = util::parse_duration, requires = "wait")]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration, requires = "wait")]
        poll: Duration,
    },
    /// Show the per-question results for an eval run
    Results {
        /// Deployment id
        deployment: i64,
        /// Eval run id, as returned by `run` or `list`
        evaluation: i64,
        /// Maximum number of results to return
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
}

fn base(deployment: i64) -> String {
    format!("/api/v1/deployments/{deployment}/evaluations")
}

fn run_path(deployment: i64, evaluation: i64) -> String {
    format!("{}/{evaluation}", base(deployment))
}

fn results_path(deployment: i64, evaluation: i64) -> String {
    format!("{}/results", run_path(deployment, evaluation))
}

fn page_query(first: Option<u64>, after: &Option<String>) -> Query {
    let mut query = Query::new();
    util::push(&mut query, "first", &first);
    util::push(&mut query, "after", after);
    query
}

/// A run 404 is a wrong run or deployment id: keep the API's message, name `cube evals list`, and
/// return a plain (non-`ApiError`) error so the "update the CLI" hint doesn't fire.
fn with_list_hint(err: anyhow::Error, deployment: i64) -> anyhow::Error {
    // Only a bare 404: one a later step wrapped in context, like the results fetch after
    // `--wait`, is about an id the run endpoint already answered for.
    if !client::is_not_found(&err) || err.chain().count() > 1 {
        return err;
    }
    anyhow::anyhow!("{err}; find eval run ids with `cube evals list {deployment}`")
}

fn start_body(
    branch: &str,
    agent: &Option<String>,
    file: &Option<String>,
    security_context: Option<Value>,
) -> Value {
    let mut body = json!({ "branchName": branch });
    if let Some(agent) = agent {
        body["agentName"] = json!(agent);
    }
    if let Some(file) = file {
        body["questionFile"] = json!(file);
    }
    if let Some(security_context) = security_context {
        body["securityContext"] = security_context;
    }
    body
}

/// The auto agent queries under the caller's own context, so an override would reach only the
/// ground truth and grade the two under different contexts.
fn override_skips_agent(agent: &Option<String>, security_context: &Option<Value>) -> bool {
    security_context.is_some() && matches!(agent.as_deref(), None | Some("auto"))
}

async fn wait_for_run(
    api: &Client,
    deployment: i64,
    evaluation: i64,
    timeout: Duration,
    interval: Duration,
) -> Result<Value> {
    let path = run_path(deployment, evaluation);
    wait::poll(Wait::new("eval run", timeout, interval), || async {
        let run = api.get(&path, &Query::new()).await?;
        let status = util::status_of(&run, "status").to_ascii_lowercase();
        if status == COMPLETED || status == FAILED {
            return Ok(Progress::Done(run));
        }

        Ok(Progress::Waiting(if status.is_empty() {
            "waiting".to_string()
        } else {
            util::one_cell(&status)
        }))
    })
    .await
}

fn passing_results(results: &Value) -> (usize, usize) {
    let items = output::items(results);
    let passed = items
        .iter()
        .filter(|item| output::field(item, "verdict").eq_ignore_ascii_case("pass"))
        .count();
    (passed, items.len())
}

fn print_results(json_output: bool, results: &Value) {
    if json_output {
        output::print_json(results);
        return;
    }

    let rows = output::items(results)
        .into_iter()
        .map(|item| {
            vec![
                util::one_cell(&output::field(&item, "verdict")),
                util::one_cell(&output::field(&item, "questionTextSnapshot")),
                util::one_cell(&output::field(&item, "score")),
            ]
        })
        .collect();
    output::table(&["VERDICT", "QUESTION", "SCORE"], rows);
}

/// `command` is the CLI invocation that fetched `page`, without its paging flags, and `noun`
/// names what the page holds.
fn next_page_hint(command: &str, noun: &str, first: Option<u64>, page: &Value) -> Option<String> {
    if page.pointer("/pageInfo/hasNextPage") != Some(&Value::Bool(true)) {
        return None;
    }

    let Some(cursor) = page
        .pointer("/pageInfo/endCursor")
        .and_then(Value::as_str)
        .filter(|cursor| !cursor.is_empty())
    else {
        return Some(format!(
            "More {noun} are available; use --json to read pageInfo.endCursor"
        ));
    };

    let first = first.map_or_else(String::new, |count| format!(" --first {count}"));
    Some(format!(
        "More {noun}: `{command}{first} --after {}`",
        util::shell_quote(cursor)
    ))
}

fn ensure_complete_results(evaluation: i64, results: &Value) -> Result<()> {
    if results.get("items").and_then(Value::as_array).is_none() {
        bail!("eval run {evaluation} returned a malformed results page; refusing to grade it");
    }

    match results.pointer("/pageInfo/hasNextPage") {
        Some(Value::Bool(false)) => Ok(()),
        Some(Value::Bool(true)) => {
            bail!("eval run {evaluation} returned an incomplete results page; refusing to grade it")
        }
        None => {
            bail!("eval run {evaluation} returned results without pageInfo.hasNextPage; refusing to grade because completeness is unknown")
        }
        Some(_) => {
            bail!("eval run {evaluation} returned a non-boolean pageInfo.hasNextPage; refusing to grade it")
        }
    }
}

async fn fetch_complete_results(
    api: &Client,
    deployment: i64,
    evaluation: i64,
    timeout: Duration,
    poll: Duration,
) -> Result<Value> {
    let path = results_path(deployment, evaluation);
    let results = wait::poll(
        Wait::new("eval results", timeout, poll.min(RESULTS_FETCH_POLL_MAX)).advising_nothing(),
        || async { api.get(&path, &Query::new()).await.map(Progress::Done) },
    )
    .await
    .with_context(|| format!("failed to fetch results for eval run {evaluation}"))?;

    ensure_complete_results(evaluation, &results)?;
    Ok(results)
}

fn ensure_passed(evaluation: i64, run: &Value, results: &Value) -> Result<()> {
    let status = util::status_of(run, "status").to_ascii_lowercase();
    let (passed, total) = passing_results(results);

    if status == FAILED {
        bail!("eval run {evaluation} failed ({passed}/{total} questions passed)");
    }
    if total == 0 {
        bail!("eval run {evaluation} completed without any question results");
    }
    if passed != total {
        bail!("eval run {evaluation} did not pass ({passed}/{total} questions passed)");
    }

    Ok(())
}

fn print_completed(json_output: bool, evaluation: i64, run: &Value, results: &Value) {
    if json_output {
        output::print_json(&json!({ "evalRun": run, "results": results }));
        return;
    }

    let (passed, total) = passing_results(results);
    println!("Eval run {evaluation}: {passed}/{total} passed");
    print_results(false, results);
}

async fn finish_wait(
    api: &Client,
    deployment: i64,
    evaluation: i64,
    timeout: Duration,
    poll: Duration,
    json_output: bool,
) -> Result<()> {
    let started = Instant::now();
    let run = wait_for_run(api, deployment, evaluation, timeout, poll).await?;
    let results_timeout = results_fetch_timeout(timeout, started.elapsed());
    let results = fetch_complete_results(api, deployment, evaluation, results_timeout, poll)
        .await
        .with_context(|| {
            format!(
                "could not verify eval run {evaluation}; inspect available results with \
                 `cube evals results {deployment} {evaluation}`"
            )
        })?;
    print_completed(json_output, evaluation, &run, &results);
    ensure_passed(evaluation, &run, &results)
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::Run {
            deployment,
            branch,
            agent,
            file,
            security_context,
            wait,
            timeout,
            poll,
        } => {
            let security_context = security_context
                .map(|raw| util::parse_json_object("--security-context", &raw).map(Value::Object))
                .transpose()?;
            if override_skips_agent(&agent, &security_context) {
                eprintln!(
                    "warning: the auto agent ignores --security-context, so only the ground truth \
                     runs under it; pass --agent with an agent from agents/config.yml to apply it \
                     to both"
                );
            }
            let body = start_body(&branch, &agent, &file, security_context);
            let started = api.post(&base(deployment), Some(&body)).await?;
            let evaluation = output::field(&started, "id")
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("Cube started the eval run but returned no run id"))?;

            if wait {
                eprintln!("Eval run {evaluation} started on {branch}");
                return finish_wait(&api, deployment, evaluation, timeout, poll, ctx.json).await;
            }

            if ctx.json {
                output::print_json(&started);
            } else {
                output::success(&format!("Started eval run {evaluation} on {branch}"));
                println!(
                    "Watch it with `cube evals status {deployment} {evaluation} --wait`, or re-run with --wait."
                );
            }
            Ok(())
        }
        Cmd::List {
            deployment,
            first,
            after,
        } => {
            let runs = api
                .get(&base(deployment), &page_query(first, &after))
                .await?;
            if ctx.json {
                output::print_json(&runs);
                return Ok(());
            }

            let rows = output::items(&runs)
                .into_iter()
                .map(|run| {
                    let cell = |field: &str| util::one_cell(&output::field(&run, field));
                    vec![
                        cell("id"),
                        cell("status"),
                        cell("branchName"),
                        cell("agentName"),
                        cell("startedAt"),
                    ]
                })
                .collect();
            output::table(&["ID", "STATUS", "BRANCH", "AGENT", "STARTED"], rows);
            let command = format!("cube evals list {deployment}");
            if let Some(hint) = next_page_hint(&command, "runs", first, &runs) {
                io::stdout().flush()?;
                eprintln!("{hint}");
            }
            Ok(())
        }
        Cmd::Status {
            deployment,
            evaluation,
            wait,
            timeout,
            poll,
        } => {
            if wait {
                // Here, unlike `run --wait`, the id came from the user.
                return finish_wait(&api, deployment, evaluation, timeout, poll, ctx.json)
                    .await
                    .map_err(|err| with_list_hint(err, deployment));
            }

            let run = api
                .get(&run_path(deployment, evaluation), &Query::new())
                .await
                .map_err(|err| with_list_hint(err, deployment))?;
            if ctx.json {
                output::print_json(&run);
            } else {
                output::table(
                    &["ID", "STATUS", "BRANCH", "AGENT"],
                    vec![vec![
                        util::one_cell(&output::field(&run, "id")),
                        util::one_cell(&output::field(&run, "status")),
                        util::one_cell(&output::field(&run, "branchName")),
                        util::one_cell(&output::field(&run, "agentName")),
                    ]],
                );
            }
            Ok(())
        }
        Cmd::Results {
            deployment,
            evaluation,
            first,
            after,
        } => {
            let results = api
                .get(
                    &results_path(deployment, evaluation),
                    &page_query(first, &after),
                )
                .await
                .map_err(|err| with_list_hint(err, deployment))?;
            print_results(ctx.json, &results);
            if !ctx.json {
                let command = format!("cube evals results {deployment} {evaluation}");
                if let Some(hint) = next_page_hint(&command, "results", first, &results) {
                    io::stdout().flush()?;
                    eprintln!("{hint}");
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_body_omits_unspecified_options() {
        assert_eq!(
            start_body("feature", &None, &None, None),
            json!({ "branchName": "feature" })
        );
        assert_eq!(
            start_body(
                "feature",
                &Some("sales-agent".to_string()),
                &Some("eval_questions/revenue.yml".to_string()),
                Some(json!({ "tenant_id": 7 }))
            ),
            json!({
                "branchName": "feature",
                "agentName": "sales-agent",
                "questionFile": "eval_questions/revenue.yml",
                "securityContext": { "tenant_id": 7 }
            })
        );
    }

    #[test]
    fn warns_when_the_override_skips_the_agent() {
        let ctx = Some(json!({ "tenant_id": 7 }));
        assert!(override_skips_agent(&None, &ctx));
        assert!(override_skips_agent(&Some("auto".to_string()), &ctx));
        assert!(!override_skips_agent(
            &Some("sales-agent".to_string()),
            &ctx
        ));
        assert!(!override_skips_agent(&None, &None));
    }

    #[test]
    fn every_result_must_pass() {
        let all_pass = json!({ "items": [
            { "verdict": "pass" },
            { "verdict": "PASS" }
        ] });
        assert_eq!(passing_results(&all_pass), (2, 2));

        let mixed = json!({ "items": [
            { "verdict": "pass" },
            { "verdict": "review" },
            { "verdict": null }
        ] });
        assert_eq!(passing_results(&mixed), (1, 3));
    }

    #[test]
    fn empty_or_non_passing_suites_fail_closed() {
        let run = json!({ "id": 42, "status": "completed" });
        assert!(ensure_passed(42, &run, &json!({ "items": [] }))
            .unwrap_err()
            .to_string()
            .contains("without any question results"));
        assert!(ensure_passed(
            42,
            &run,
            &json!({ "items": [{ "verdict": "pass" }, { "verdict": "fail" }] })
        )
        .unwrap_err()
        .to_string()
        .contains("1/2 questions passed"));

        assert!(ensure_complete_results(
            42,
            &json!({
                "items": [{ "verdict": "pass" }],
                "pageInfo": { "hasNextPage": true }
            })
        )
        .unwrap_err()
        .to_string()
        .contains("incomplete results page"));

        assert!(
            ensure_complete_results(42, &json!({ "items": [{ "verdict": "pass" }] }))
                .unwrap_err()
                .to_string()
                .contains("without pageInfo.hasNextPage")
        );

        assert!(ensure_complete_results(
            42,
            &json!({
                "items": [{ "verdict": "pass" }],
                "pageInfo": { "hasNextPage": "false" }
            })
        )
        .unwrap_err()
        .to_string()
        .contains("non-boolean pageInfo.hasNextPage"));

        assert!(ensure_complete_results(
            42,
            &json!({ "items": null, "pageInfo": { "hasNextPage": false } })
        )
        .unwrap_err()
        .to_string()
        .contains("malformed results page"));

        ensure_complete_results(
            42,
            &json!({ "items": [{ "verdict": "pass" }], "pageInfo": { "hasNextPage": false } }),
        )
        .unwrap();
        ensure_passed(
            42,
            &run,
            &json!({ "items": [{ "verdict": "pass" }, { "verdict": "PASS" }] }),
        )
        .unwrap();

        let failed = json!({ "id": 42, "status": "FAILED" });
        let error = ensure_passed(42, &failed, &json!({ "items": [{ "verdict": "pass" }] }))
            .unwrap_err()
            .to_string();
        assert!(error.contains("eval run 42 failed"), "got: {error}");
        assert!(error.contains("1/1"), "got: {error}");
    }

    #[test]
    fn paginated_results_show_a_runnable_next_page_command() {
        let results = json!({
            "items": [{ "verdict": "pass" }],
            "pageInfo": { "hasNextPage": true, "endCursor": "a;b" }
        });
        let command = "cube evals results 1 42";
        assert_eq!(
            next_page_hint(command, "results", Some(5), &results),
            Some("More results: `cube evals results 1 42 --first 5 --after 'a;b'`".into())
        );
        assert_eq!(
            next_page_hint(
                command,
                "results",
                None,
                &json!({ "pageInfo": { "hasNextPage": true, "endCursor": "" } })
            ),
            Some("More results are available; use --json to read pageInfo.endCursor".into())
        );
        assert_eq!(
            next_page_hint(
                command,
                "results",
                None,
                &json!({ "pageInfo": { "hasNextPage": true } })
            ),
            Some("More results are available; use --json to read pageInfo.endCursor".into())
        );
        assert_eq!(
            next_page_hint(
                command,
                "results",
                None,
                &json!({ "pageInfo": { "hasNextPage": false } })
            ),
            None
        );
    }

    #[test]
    fn a_run_404_keeps_the_api_message_and_points_at_list() {
        let api_error = |status, detail: &str| {
            anyhow::Error::new(client::ApiError {
                status,
                method: reqwest::Method::GET,
                path: "/api/v1/deployments/7/evaluations/150".to_string(),
                detail: detail.to_string(),
            })
        };

        let err = with_list_hint(
            api_error(reqwest::StatusCode::NOT_FOUND, "Deployment 7 not found"),
            7,
        );
        assert_eq!(
            err.to_string(),
            "not found (404): GET /api/v1/deployments/7/evaluations/150. Deployment 7 not found; \
             find eval run ids with `cube evals list 7`"
        );
        assert!(!crate::error::is_api_error(&err));

        let err = with_list_hint(
            api_error(reqwest::StatusCode::INTERNAL_SERVER_ERROR, "boom"),
            7,
        );
        assert!(crate::error::is_api_error(&err));
        assert!(!err.to_string().contains("cube evals list"), "got: {err}");

        let err = with_list_hint(
            api_error(reqwest::StatusCode::NOT_FOUND, "Evaluation 150 not found")
                .context("failed to fetch results for eval run 150"),
            7,
        );
        assert!(crate::error::is_api_error(&err));
        assert!(
            !format!("{err:#}").contains("cube evals list"),
            "got: {err:#}"
        );
    }

    #[test]
    fn paging_sends_only_the_flags_given() {
        assert!(page_query(None, &None).is_empty());
        assert_eq!(
            page_query(Some(20), &Some("c1".to_string())),
            vec![
                ("first".to_string(), "20".to_string()),
                ("after".to_string(), "c1".to_string())
            ]
        );
    }

    #[test]
    fn paginated_runs_show_a_runnable_next_page_command() {
        let runs = json!({
            "items": [{ "id": 42 }],
            "pageInfo": { "hasNextPage": true, "endCursor": "c1" }
        });
        assert_eq!(
            next_page_hint("cube evals list 7", "runs", Some(20), &runs),
            Some("More runs: `cube evals list 7 --first 20 --after 'c1'`".into())
        );
        assert_eq!(
            next_page_hint("cube evals list 7", "runs", None, &runs),
            Some("More runs: `cube evals list 7 --after 'c1'`".into())
        );
        assert_eq!(
            next_page_hint(
                "cube evals list 7",
                "runs",
                None,
                &json!({ "pageInfo": { "hasNextPage": true } })
            ),
            Some("More runs are available; use --json to read pageInfo.endCursor".into())
        );
        assert_eq!(
            next_page_hint(
                "cube evals list 7",
                "runs",
                None,
                &json!({ "items": [], "pageInfo": { "hasNextPage": false, "endCursor": "c1" } })
            ),
            None
        );
    }

    #[test]
    fn results_fetch_stays_within_the_user_timeout() {
        assert_eq!(
            results_fetch_timeout(Duration::from_secs(60), Duration::from_secs(50)),
            Duration::from_secs(10)
        );
        assert_eq!(
            results_fetch_timeout(Duration::from_secs(600), Duration::from_secs(50)),
            RESULTS_FETCH_TIMEOUT
        );
        assert_eq!(
            results_fetch_timeout(Duration::from_secs(60), Duration::from_secs(60)),
            RESULTS_FETCH_FLOOR
        );
        assert_eq!(
            results_fetch_timeout(Duration::from_secs(60), Duration::from_secs(59)),
            RESULTS_FETCH_FLOOR
        );
    }
}
