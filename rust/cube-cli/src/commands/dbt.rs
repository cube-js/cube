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
        #[arg(long)]
        r#ref: Option<String>,
        /// Name for the Cube branch the generated cubes land on (defaults to a
        /// generated `dbt-sync/…` name)
        #[arg(long)]
        branch: Option<String>,
        /// Wait for the sync to finish, then print its result
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long (30s, 15m, 1h)
        #[arg(long, default_value = "30m", value_parser = util::parse_duration)]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration)]
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
        #[arg(long, default_value = "30m", value_parser = util::parse_duration)]
        timeout: Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration)]
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

/// One line summarising a status payload, for progress output.
fn status_label(status: &Value) -> String {
    let state = output::field(status, "status");
    let stage = output::field(status, "progress.stage");
    let percent = output::field(status, "progress.percentComplete");
    let message = output::field(status, "progress.message");

    let mut label = if stage.is_empty() || stage == state {
        state
    } else {
        format!("{state} ({stage})")
    };
    if !percent.is_empty() {
        label.push_str(&format!(" {percent}%"));
    }
    if !message.is_empty() {
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

/// The `--wait --json` document: one object carrying BOTH halves a caller needs —
/// which branch to compile next, and how the sync ended. Waiting otherwise forced a
/// choice between the start payload (which names the branch) and the result (which
/// does not), and a CI job needs both.
fn wait_json(started: &Value, status: &Value, branch_name: &str, result: Option<&Value>) -> Value {
    serde_json::json!({
        "syncJobId": output::field(started, "syncJobId"),
        "workflowId": output::field(started, "workflowId"),
        "branchName": branch_name,
        "status": output::field(status, "status"),
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

        let state = output::field(&status, "status");
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

            if !wait {
                if ctx.json {
                    output::print_json(&started);
                } else {
                    output::success(&format!("Started dbt sync {sync_job_id} on {branch_name}"));
                    println!(
                        "Watch it with `cube dbt status {deployment} {sync_job_id} --wait`, \
                         or re-run sync with --wait."
                    );
                    // Each sync deliberately creates a FRESH branch, and no endpoint
                    // deletes one yet — a job running per pull request accumulates
                    // them, so say so rather than let them pile up unnoticed. Worth
                    // revisiting if branch deletion becomes available.
                    println!(
                        "The branch is not removed automatically — prune it when you're done."
                    );
                }

                return Ok(());
            }

            eprintln!("dbt sync {sync_job_id} started on {branch_name}");
            let status = wait_for_sync(&api, deployment, &sync_job_id, timeout, poll).await?;
            let failed = output::field(&status, "status") == FAILED;

            // COMPLETED: fold the result into the same command rather than making
            // the caller ask again.
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
            let result = if failed {
                None
            } else {
                let path = format!("{}/{sync_job_id}/result", base(deployment));
                // Its own advice: this budget is a const, so the default counsel to
                // raise `--timeout` would send someone after a flag that cannot move
                // this deadline, and "it may still be running" describes a sync that
                // has already reported COMPLETED.
                let fetched = wait::poll(
                    Wait::new("dbt sync result", RESULT_FETCH_TIMEOUT, poll).advising(format!(
                        "The sync finished, so this is the result read failing, not the sync — \
                         try `cube dbt result {deployment} {sync_job_id}`"
                    )),
                    || async {
                        match api.get_optional(&path, &Vec::new()).await? {
                            Some(result) => Ok(Progress::Done(result)),
                            None => Ok(Progress::Waiting("result not available yet".to_string())),
                        }
                    },
                )
                .await;

                match fetched {
                    Ok(result) => Some(result),
                    Err(err) => {
                        // Still emit the document: the sync itself succeeded, and the
                        // branch name in it is what a caller needs to carry on with.
                        if ctx.json {
                            output::print_json(&wait_json(&started, &status, &branch_name, None));
                        }

                        return Err(err.context(format!(
                            "dbt sync {sync_job_id} completed on {branch_name}, but its result \
                             could not be read. The sync itself succeeded — read it with \
                             `cube dbt result {deployment} {sync_job_id}`"
                        )));
                    }
                }
            };

            if ctx.json {
                output::print_json(&wait_json(&started, &status, &branch_name, result.as_ref()));
            }

            if failed {
                // The only signal a CI gate needs is the non-zero exit. The message
                // carries the workflow's own reason, which is what a human reading
                // the failed job actually wants.
                let error = output::field(&status, "error");
                bail!(
                    "dbt sync {sync_job_id} failed: {}",
                    if error.is_empty() {
                        "(no reason reported)".to_string()
                    } else {
                        error
                    }
                );
            }

            if !ctx.json {
                output::success(&format!(
                    "dbt sync {sync_job_id} completed on {branch_name}"
                ));
                match &result {
                    Some(result) => print_result(false, result),
                    None => println!("(no result reported)"),
                }
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
                if output::field(&status, "status") == FAILED {
                    let error = output::field(&status, "error");
                    bail!(
                        "dbt sync {sync_job_id} failed: {}",
                        if error.is_empty() {
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
            match api.get_optional(&path, &Vec::new()).await? {
                Some(result) => print_result(ctx.json, &result),
                // A running sync has no result yet, which is not the same as a
                // sync that does not exist — point at the status either way.
                None => bail!(
                    "no result for dbt sync {sync_job_id} yet — check \
                     `cube dbt status {deployment} {sync_job_id}`"
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
