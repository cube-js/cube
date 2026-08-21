use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use clap::Subcommand;
use serde_json::Value;

use crate::client::{Client, Query};
use crate::wait::{self, Progress, Wait};
use crate::{output, util, Ctx};

/// Run a deployment's dbt sync: pull the dbt project, convert its models into
/// cubes, and commit them to a fresh branch.
#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start a dbt sync
    Sync {
        /// Deployment id
        deployment: i64,
        /// Git ref in the dbt repository to sync — a branch or tag, not a commit
        /// SHA. Overrides the branch saved on the dbt integration for this sync
        /// only, so CI can test the ref under review.
        #[arg(long, value_parser = util::nonempty_ref)]
        r#ref: Option<String>,
        /// Name for the Cube branch the generated cubes land on (defaults to a
        /// generated `dbt-sync/…` name)
        #[arg(long, value_parser = util::nonempty)]
        branch: Option<String>,
        /// Wait for the sync to finish, then print its result
        #[arg(long)]
        wait: bool,
        /// Give up waiting for the sync after this long; reading its result may take 30s more
        #[arg(long, default_value = "30m", value_parser = util::parse_duration, requires = "wait")]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration, requires = "wait")]
        poll: Duration,
    },
    /// Show the status of a dbt sync
    Status {
        /// Deployment id
        deployment: i64,
        /// Sync job id, as returned by `sync`
        sync_job_id: String,
        /// Wait for the sync to finish
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long (30s, 15m, 1h)
        #[arg(long, default_value = "30m", value_parser = util::parse_duration, requires = "wait")]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration, requires = "wait")]
        poll: Duration,
    },
    /// Show what a completed dbt sync generated
    Result {
        /// Deployment id
        deployment: i64,
        /// Sync job id, as returned by `sync`
        sync_job_id: String,
    },
    /// Cancel a running dbt sync
    Cancel {
        /// Deployment id
        deployment: i64,
        /// Sync job id, as returned by `sync`
        sync_job_id: String,
    },
}

fn base(deployment: i64) -> String {
    format!("/api/v1/deployments/{deployment}/dbt-sync")
}

/// The status values that end a sync. Everything else — including a value this
/// build has never heard of — means "still working", so a new stage server-side
/// cannot make an older CLI declare a sync finished.
const COMPLETED: &str = "COMPLETED";
const FAILED: &str = "FAILED";

/// How long to tolerate "no such sync" before giving up on it.
///
/// A sync is briefly invisible right after it starts, so a poll loop has to accept
/// 404 for a moment. It must not accept it forever, or a mistyped id would wait out
/// the whole timeout instead of saying what was wrong.
///
/// Measured in TIME, not polls: `--poll` is the user's dial for responsiveness, and
/// counting attempts would silently shrink this window to six seconds for anyone
/// who tightened it to `--poll 1s`.
const MISSING_GRACE: Duration = Duration::from_secs(60);

/// Budget for reading the result of a sync that has already reported COMPLETED.
/// Short on purpose: nothing is being waited for here, it only buys enough room to
/// ride out a transient failure on the gate's final request.
const RESULT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const RESULT_FETCH_POLL_MAX: Duration = Duration::from_secs(5);

/// One line summarising a status payload, for progress output.
///
/// Every field is normalised on the way in, not just compared as if it were. This label
/// is `poll`'s dedupe key, so a value that alternates between two spellings of one thing
/// — `"BUILDING"` and `"BUILDING "` — makes two keys, and a fifteen-minute sync prints a
/// line per poll instead of the handful the loop exists to produce. Nothing here is ever
/// acted on, only shown and compared, so normalising costs nothing: the opposite of the
/// branch names, which print untrimmed precisely because a command addresses them.
fn status_label(status: &Value) -> String {
    let state = util::status_of(status, "status");
    // `one_line`, not `trim`: `trim` only takes whitespace off the ENDS, so an interior
    // newline still breaks the one-line promise and an unbounded message is reproduced in
    // full on every change. `deployments`' label has been capped for both reasons since it
    // was written; this is the same loop's other waiter. `COMPLAINT_LIMIT`, because a
    // label repeats and dedupes — a terminal reason is the one that earns the long budget.
    let stage = util::one_line(
        &output::field(status, "progress.stage"),
        util::COMPLAINT_LIMIT,
    );
    // Through the same rule, despite being a number: `output::field` stringifies whatever
    // arrived, so a server sending it as a STRING can put whitespace in it — which is the
    // shape the test fixture below uses. Exempting one field would make the label's
    // one-line guarantee depend on another field's type.
    let percent = util::one_line(
        &output::field(status, "progress.percentComplete"),
        util::COMPLAINT_LIMIT,
    );
    let message = util::one_line(
        &output::field(status, "progress.message"),
        util::COMPLAINT_LIMIT,
    );

    // `is_blank`, not `is_empty`: blanks would add empty brackets, a bare `%`, or a dash
    // with nothing after it.
    let mut label = if util::is_blank(&stage) || stage == state {
        state
    } else {
        format!("{state} ({stage})")
    };
    if !util::is_blank(&percent) {
        label.push_str(&format!(" {percent}%"));
    }
    if !util::is_blank(&message) {
        label.push_str(&format!(" — {message}"));
    }

    label
}

fn print_status(json: bool, status: &Value) {
    if json {
        output::print_json(status);
    } else {
        println!("{}", status_label(status));
    }
}

fn print_result(json: bool, result: &Value) {
    if json {
        output::print_json(result);
        return;
    }
    let files = output::items(result.get("generatedFiles").unwrap_or(&Value::Null));
    output::success(&format!(
        "Generated {} cube(s) in {} file(s) in {}ms",
        output::field(result, "cubeCount"),
        files.len(),
        output::field(result, "durationMs")
    ));
    for file in files {
        println!("{}", output::field(&file, "path"));
    }
}

/// Each sync deliberately creates a FRESH branch which outlives the command, so a job
/// running per pull request accumulates them — say so, and name the command that removes
/// one, rather than let them pile up unnoticed. Printed from every path that ends well and
/// speaks to a human — never into `--json`, where a document is no place for advice —
/// through one function so they can't drift into saying different things.
///
/// `sync_created_it` is false when the caller passed `--branch`: the branch is then
/// theirs, may be long-lived, and this sync didn't create it, so a filled-in
/// `delete-branch` would be pointing at their own branch.
fn print_prune_hint(sync_created_it: bool, deployment: i64, branch_name: &str) {
    if sync_created_it {
        let branch_name = util::branch_or_placeholder(branch_name);
        println!(
            "The branch is not removed automatically — prune it with \
             `cube data-model delete-branch {deployment} {}` \
             when you're done.",
            util::shell_quote(&branch_name)
        );
    }
}

/// A result is available only when it is a non-empty object. Some deployments return
/// `200 null` or `{}` while the completed workflow is still publishing its result.
fn available_result(result: Option<Value>) -> Option<Value> {
    result.filter(|value| value.as_object().is_some_and(|object| !object.is_empty()))
}

/// The `--wait --json` document: one object carrying BOTH halves a caller needs —
/// which branch to compile next, and how the sync ended. Waiting otherwise forced a
/// choice between the start payload (which names the branch) and the result (which
/// does not), and a CI job needs both.
fn wait_json(started: &Value, status: &Value, branch_name: &str, result: Option<&Value>) -> Value {
    serde_json::json!({
        "syncJobId": output::field(started, "syncJobId"),
        "workflowId": output::field(started, "workflowId"),
        // Raw, unlike the human-facing messages: a gate reads this field and acts on it,
        // so an empty string it can test for beats a placeholder it would try to use.
        "branchName": branch_name,
        // Normalised, unlike `branchName` above, and for the reason that rule gives: a
        // gate ACTS on a branch name, so an empty string it can test for beats a
        // placeholder it would use — while a status is only ever compared. Raw here would
        // mean a gate copying the CLI's own check (`.status == "COMPLETED"`) failing on a
        // value the CLI had just accepted as terminal.
        "status": util::status_of(status, "status"),
        "error": status.get("error").cloned().unwrap_or(Value::Null),
        "result": result.cloned().unwrap_or(Value::Null),
    })
}

/// Poll until the sync reaches a terminal state, reporting movement as it goes.
/// Returns the terminal status payload; a FAILED sync is returned, not raised, so
/// the caller decides what a failure means for its own exit.
async fn wait_for_sync(
    api: &Client,
    deployment: i64,
    sync_job_id: &str,
    timeout: Duration,
    interval: Duration,
) -> Result<Value> {
    let path = format!("{}/{}", base(deployment), sync_job_id);
    // A Cell, not a plain variable: the poll closure hands back a future that
    // borrows what it captures, and a mutable capture would make each call borrow
    // the closure itself — which the borrow checker rejects even though the calls
    // are strictly sequential.
    let missing_since = std::cell::Cell::new(None::<Instant>);

    wait::poll(Wait::new("dbt sync", timeout, interval), || async {
        let Some(status) = api.get_optional(&path, &Vec::new()).await? else {
            let since = missing_since.get().unwrap_or_else(Instant::now);
            missing_since.set(Some(since));
            if since.elapsed() > MISSING_GRACE {
                bail!(
                    "no dbt sync {sync_job_id} on deployment {deployment}. It may belong \
                     to another deployment, have aged out, or this tenant may not serve \
                     the dbt-sync endpoints yet"
                );
            }

            return Ok(Progress::Waiting("starting".to_string()));
        };
        missing_since.set(None);

        let state = util::status_of(&status, "status");
        if state == COMPLETED || state == FAILED {
            return Ok(Progress::Done(status));
        }

        Ok(Progress::Waiting(status_label(&status)))
    })
    .await
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::Sync {
            deployment,
            r#ref,
            branch,
            wait,
            timeout,
            poll,
        } => {
            let mut body = serde_json::Map::new();
            util::set(&mut body, "branchName", &branch);
            util::set(&mut body, "ref", &r#ref);
            let started = api.post(&base(deployment), Some(&util::body(body))).await?;

            let sync_job_id = output::field(&started, "syncJobId");
            let branch_name = output::field(&started, "branchName");
            // Every message below says which branch, and a payload that named none would
            // otherwise leave a hole mid-sentence. The raw value stays for the two things
            // that must act on it rather than print it: the ref comparison, and the
            // `--wait` document a gate reads.
            let shown = util::branch_or_placeholder(&branch_name);

            if !wait {
                if ctx.json {
                    output::print_json(&started);
                }

                if !ctx.json {
                    output::success(&format!("Started dbt sync {sync_job_id} on {shown}"));
                    println!(
                        "Watch it with `cube dbt status {deployment} {} --wait`, \
                         or re-run sync with --wait.",
                        util::shell_quote(&sync_job_id)
                    );
                    print_prune_hint(branch.is_none(), deployment, &branch_name);
                }

                return Ok(());
            }

            eprintln!("dbt sync {sync_job_id} started on {shown}");
            let status = wait_for_sync(&api, deployment, &sync_job_id, timeout, poll).await?;

            if util::status_of(&status, "status") == FAILED {
                if ctx.json {
                    output::print_json(&wait_json(&started, &status, &branch_name, None));
                }

                // The only signal a CI gate needs is the non-zero exit. The message
                // carries the workflow's own reason, which is what a human reading
                // the failed job actually wants — and `is_blank` rather than `is_empty`
                // because a reason of blanks would fill the slot without answering it,
                // on the one line somebody reads when the gate goes red.
                // Collapsed like the build failure in `deployments`: a dbt reason is a
                // compile or warehouse error that arrives multi-line, and this is a
                // `bail!` whose chain `main` renders on one line with `{err:#}`.
                let error = util::one_line(&output::field(&status, "error"), util::REASON_LIMIT);
                bail!(
                    "dbt sync {sync_job_id} failed: {}",
                    if util::is_blank(&error) {
                        "(no reason reported)".to_string()
                    } else {
                        error
                    }
                );
            }

            // COMPLETED from here on, so the result is a value rather than a
            // possibility: a sync that produced none leaves through the `Err` branch
            // below. Handling the failure above rather than carrying an `Option` this
            // far is what makes that structural instead of merely true.
            //
            // Fold the result into the same command rather than making the caller ask
            // again.
            //
            // Through `wait::poll` for two reasons. Its transient tolerance covers
            // the LAST request of the gate, where a single 502 would otherwise fail a
            // job whose sync already succeeded. And an absent result is treated as
            // "not yet" rather than as an answer: a result is normally readable the
            // instant the status says COMPLETED, but not guaranteed to be — the run
            // is still closing, and a worker that can't answer for a moment yields a
            // 404. Returning `null` with exit 0 there would hand a gate a document
            // whose `result.generatedFiles` is missing, which is the one shape of
            // failure `--wait` exists to make loud.
            let result = {
                let path = format!("{}/{sync_job_id}/result", base(deployment));
                // No tail of its own. The default counsel to raise `--timeout` would be
                // wrong twice over — this budget is a const, and the sync has already
                // reported COMPLETED — but the recovery can't live here either: the
                // tail only reaches the reader on the TIMEOUT branch, while exhausted
                // retries and an immediate 401/500 are the likelier failures. So the
                // context below carries it for every outcome, and this stays silent so
                // the timeout branch doesn't print it twice (`main` renders the chain
                // with `{err:#}`, joining links with ": ").
                let fetched = wait::poll(
                    Wait::new(
                        "dbt sync result",
                        RESULT_FETCH_TIMEOUT,
                        poll.min(RESULT_FETCH_POLL_MAX),
                    )
                    .advising_nothing(),
                    || async {
                        match available_result(api.get_optional(&path, &Vec::new()).await?) {
                            Some(result) => Ok(Progress::Done(result)),
                            _ => Ok(Progress::Waiting("result not available yet".to_string())),
                        }
                    },
                )
                .await;

                match fetched {
                    Ok(result) => result,
                    Err(err) => {
                        // Still emit the document: the sync itself succeeded, and the
                        // branch name in it is what a caller needs to carry on with.
                        if ctx.json {
                            output::print_json(&wait_json(&started, &status, &branch_name, None));
                        }

                        // Carries the recovery for EVERY way the read can fail, not
                        // just the timeout: the branch exists and the cubes are
                        // committed, so a caller can read the result separately and
                        // carry on.
                        return Err(err.context(format!(
                            "dbt sync {sync_job_id} completed on {shown}, but its result \
                             could not be read. The sync itself succeeded — read the result with \
                             `cube dbt result {deployment} {}`",
                            util::shell_quote(&sync_job_id)
                        )));
                    }
                }
            };

            if ctx.json {
                output::print_json(&wait_json(&started, &status, &branch_name, Some(&result)));
            } else {
                output::success(&format!("dbt sync {sync_job_id} completed on {shown}"));
                print_result(false, &result);
                // Waiting doesn't clean up either, and this is the form the docs teach —
                // so the interactive user who waits hears what the one who doesn't
                // already heard. Not on the FAILED path above: a branch that failed is
                // kept deliberately, as the evidence for why.
                print_prune_hint(branch.is_none(), deployment, &branch_name);
            }
        }
        Cmd::Status {
            deployment,
            sync_job_id,
            wait,
            timeout,
            poll,
        } => {
            if wait {
                let status = wait_for_sync(&api, deployment, &sync_job_id, timeout, poll).await?;
                print_status(ctx.json, &status);
                if util::status_of(&status, "status") == FAILED {
                    // Collapsed like the build failure in `deployments`: a dbt reason is a
                    // compile or warehouse error that arrives multi-line, and this is a
                    // `bail!` whose chain `main` renders on one line with `{err:#}`.
                    let error =
                        util::one_line(&output::field(&status, "error"), util::REASON_LIMIT);
                    bail!(
                        "dbt sync {sync_job_id} failed: {}",
                        if util::is_blank(&error) {
                            "(no reason reported)".to_string()
                        } else {
                            error
                        }
                    );
                }

                return Ok(());
            }

            let query: Query = Vec::new();
            let path = format!("{}/{sync_job_id}", base(deployment));
            match api.get_optional(&path, &query).await? {
                Some(status) => print_status(ctx.json, &status),
                None => bail!(
                    "no dbt sync {sync_job_id} on deployment {deployment} \
                     (it may not be visible yet, belong to another deployment, or have aged out)"
                ),
            }
        }
        Cmd::Result {
            deployment,
            sync_job_id,
        } => {
            let path = format!("{}/{sync_job_id}/result", base(deployment));
            match available_result(api.get_optional(&path, &Vec::new()).await?) {
                Some(result) => print_result(ctx.json, &result),
                // A running sync, `200 null`, and `{}` all mean "not available yet".
                None => bail!(
                    "no result for dbt sync {sync_job_id} yet — check \
                     `cube dbt status {deployment} {}`",
                    util::shell_quote(&sync_job_id)
                ),
            }
        }
        Cmd::Cancel {
            deployment,
            sync_job_id,
        } => {
            let res = api
                .delete(&format!("{}/{sync_job_id}", base(deployment)), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Cancelled dbt sync {sync_job_id}"));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_nonempty_objects_are_results() {
        assert!(available_result(None).is_none());
        assert!(available_result(Some(Value::Null)).is_none());
        assert!(available_result(Some(json!({}))).is_none());
        assert!(available_result(Some(json!([]))).is_none());
        assert_eq!(
            available_result(Some(json!({"generatedFiles": []}))),
            Some(json!({"generatedFiles": []}))
        );
    }

    #[test]
    fn a_blank_stage_does_not_become_empty_brackets() {
        // Also `poll`'s dedupe key: blanks that vary in width would read as movement and
        // print a line per poll, which is the thing `poll` exists to avoid.
        assert_eq!(
            status_label(&json!({"status": "BUILDING", "progress": {"stage": "  "}})),
            "BUILDING"
        );
        assert_eq!(
            status_label(&json!({"status": "BUILDING", "progress": {"message": " "}})),
            "BUILDING"
        );
        assert_eq!(
            status_label(&json!({"status": "BUILDING", "progress": {"stage": "compile"}})),
            "BUILDING (compile)"
        );
        // Padded but equal: naming the stage again would say the payload reported two
        // things when it reported one — and a stage alternating between the two
        // spellings would flip the rendering and read as movement to `poll`.
        assert_eq!(
            status_label(&json!({"status": "BUILDING", "progress": {"stage": "BUILDING "}})),
            "BUILDING"
        );
        // Padding anywhere makes a second key for one state, and `poll` reports a CHANGED
        // key — so the label a padded payload produces has to equal the one its clean
        // spelling produces, or a fifteen-minute sync prints a line per poll.
        let padded = json!({
            "status": "BUILDING ",
            "progress": {"stage": " compile", "percentComplete": " 42 ", "message": "x "}
        });
        let clean = json!({
            "status": "BUILDING",
            "progress": {"stage": "compile", "percentComplete": "42", "message": "x"}
        });
        assert_eq!(status_label(&padded), status_label(&clean));
        assert_eq!(status_label(&padded), "BUILDING (compile) 42% — x");
        // Interior newlines are the half `trim` can't reach, and the label promises one
        // line — to stderr on every change, and inside `poll`'s "(last seen: …)".
        assert_eq!(
            status_label(&json!({
                "status": "BUILDING",
                "progress": {"message": "Parsing dbt project\n  models/fct_orders.sql"}
            })),
            "BUILDING — Parsing dbt project models/fct_orders.sql"
        );
        // And length: an unbounded message would be reproduced in full on every change.
        let long = status_label(&json!({
            "status": "BUILDING",
            "progress": {"message": "x".repeat(500)}
        }));
        assert!(long.ends_with('…'), "a long message is capped: {long}");
    }

    #[test]
    fn the_document_reports_the_state_the_cli_decided_on() {
        // A gate copying the CLI's own check must not fail on a value the CLI just
        // accepted as terminal — unlike `branchName`, which stays raw because a gate
        // acts on it rather than only comparing it.
        let doc = wait_json(
            &json!({}),
            &json!({"status": " COMPLETED\n", "branchName": "  x  "}),
            "  x  ",
            None,
        );
        assert_eq!(doc["status"], json!("COMPLETED"));
        assert_eq!(doc["branchName"], json!("  x  "));
    }

    #[test]
    fn status_label_reads_like_progress() {
        let status = json!({
            "status": "COMPILING_DBT",
            "progress": { "stage": "COMPILING_DBT", "percentComplete": 50, "message": "Parsing dbt project" }
        });
        // Stage repeated in `status` is not printed twice.
        assert_eq!(
            status_label(&status),
            "COMPILING_DBT 50% — Parsing dbt project"
        );
    }

    #[test]
    fn status_label_keeps_a_stage_that_differs_from_the_status() {
        let status = json!({
            "status": "FINALIZING",
            "progress": { "stage": "COMPLETED", "percentComplete": 100 }
        });
        assert_eq!(status_label(&status), "FINALIZING (COMPLETED) 100%");
    }

    #[test]
    fn status_label_survives_a_payload_with_no_progress() {
        assert_eq!(
            status_label(&json!({ "status": "INITIALIZING" })),
            "INITIALIZING"
        );
    }
}
