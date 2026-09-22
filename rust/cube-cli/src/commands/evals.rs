use std::io::{self, Write as _};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _, Result};
use clap::Subcommand;
use serde_json::{json, Value};

use crate::client::{Client, Query};
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
        /// Run one eval-question YAML file within the agents directory
        #[arg(long, value_name = "PATH", value_parser = util::nonempty_repo_path)]
        file: Option<String>,
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
    /// Show an eval run's status
    Status {
        /// Deployment id
        deployment: i64,
        /// Eval run id, as returned by `run`
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
        /// Eval run id, as returned by `run`
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

fn start_body(branch: &str, agent: &Option<String>, file: &Option<String>) -> Value {
    let mut body = json!({ "branchName": branch });
    if let Some(agent) = agent {
        body["agentName"] = json!(agent);
    }
    if let Some(file) = file {
        body["questionFile"] = json!(file);
    }
    body
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

fn next_page_hint(
    deployment: i64,
    evaluation: i64,
    first: Option<u64>,
    results: &Value,
) -> Option<String> {
    if results.pointer("/pageInfo/hasNextPage") != Some(&Value::Bool(true)) {
        return None;
    }

    let Some(cursor) = results
        .pointer("/pageInfo/endCursor")
        .and_then(Value::as_str)
        .filter(|cursor| !cursor.is_empty())
    else {
        return Some("More results are available; use --json to read pageInfo.endCursor".into());
    };

    let first = first.map_or_else(String::new, |count| format!(" --first {count}"));
    Some(format!(
        "More results: `cube evals results {deployment} {evaluation}{first} --after {}`",
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
            wait,
            timeout,
            poll,
        } => {
            let body = start_body(&branch, &agent, &file);
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
        Cmd::Status {
            deployment,
            evaluation,
            wait,
            timeout,
            poll,
        } => {
            if wait {
                return finish_wait(&api, deployment, evaluation, timeout, poll, ctx.json).await;
            }

            let run = api
                .get(&run_path(deployment, evaluation), &Query::new())
                .await?;
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
            let mut query = Query::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let results = api
                .get(&results_path(deployment, evaluation), &query)
                .await?;
            print_results(ctx.json, &results);
            if !ctx.json {
                if let Some(hint) = next_page_hint(deployment, evaluation, first, &results) {
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
            start_body("feature", &None, &None),
            json!({ "branchName": "feature" })
        );
        assert_eq!(
            start_body(
                "feature",
                &Some("sales-agent".to_string()),
                &Some("eval_questions/revenue.yml".to_string())
            ),
            json!({
                "branchName": "feature",
                "agentName": "sales-agent",
                "questionFile": "eval_questions/revenue.yml"
            })
        );
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
        assert_eq!(
            next_page_hint(1, 42, Some(5), &results),
            Some("More results: `cube evals results 1 42 --first 5 --after 'a;b'`".into())
        );
        assert_eq!(
            next_page_hint(
                1,
                42,
                None,
                &json!({ "pageInfo": { "hasNextPage": true, "endCursor": "" } })
            ),
            Some("More results are available; use --json to read pageInfo.endCursor".into())
        );
        assert_eq!(
            next_page_hint(1, 42, None, &json!({ "pageInfo": { "hasNextPage": true } })),
            Some("More results are available; use --json to read pageInfo.endCursor".into())
        );
        assert_eq!(
            next_page_hint(
                1,
                42,
                None,
                &json!({ "pageInfo": { "hasNextPage": false } })
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
