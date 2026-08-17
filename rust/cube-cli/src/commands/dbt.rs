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
        #[arg(long, value_parser = util::nonempty_target)]
        r#ref: Option<String>,
        /// Name for the Cube branch the generated cubes land on (defaults to a
        /// generated `dbt-sync/…` name)
        #[arg(long, value_parser = util::nonempty)]
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

/// How much of a ref to expect in a branch name.
///
/// Measured against a live deployment rather than inferred, because the first version of
/// this check guessed and rejected two of three legitimate refs. What the server does
/// with `--ref X`, in `dbt-sync/<X>-<timestamp>-<hash>`:
///
/// | requested                                          | branch segment                             |
/// |----------------------------------------------------|--------------------------------------------|
/// | `main`                                             | `main`                                     |
/// | `refs/heads/main`                                  | `refs-heads-main`                          |
/// | `release/2026.08`                                  | `release-2026.08`  (dot kept)              |
/// | `feat_x_orders`                                    | `feat_x_orders`    (underscore kept)       |
/// | `UPPER_lower`                                      | `UPPER_lower`      (case kept)             |
/// | `feat x orders`                                    | `feat-x-orders`    (space replaced)        |
/// | `feat~x`                                           | `feat-x`           (tilde replaced)        |
/// | `feature/CUB-1234-add-orders-fact-table-and-more`  | `feature-CUB-1234-add-orders-fact-table-a` |
///
/// (`git check-ref-format` rejects a space, so that row measures the server rather than
/// anything `--ref` can carry; `~` earns its place — `main~1` is a legal revision.)
///
/// One observation deliberately *not* encoded: `#1-fix` produced `1-fix`, so a leading `#`
/// is dropped rather than replaced. One probe of one character is how the first two
/// versions of this check went wrong, so `#` stays in the unmeasured set and reports the
/// sync unverified instead. Extending the mapping wants the same treatment the table got:
/// measure the class, then encode it.
///
/// So alphanumerics, `_`, `.` and `-` survive with their case; `/`, space and `~` become
/// `-`; and the segment is cut at 40 characters. Comparing whole refs therefore fails
/// healthy syncs — a prefix is what survives every shape, and it keeps the property that
/// matters, since a sync that ignored the ref contains *nothing* from it.
const REF_PREFIX: usize = 12;

/// The leading part of a ref as it should appear in a server-generated branch name.
fn ref_fingerprint(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            // Measured as replaced by the server.
            '/' | ' ' | '~' => '-',
            // Measured as kept; compared case-insensitively.
            ch => ch.to_ascii_lowercase(),
        })
        // Stop at the first character nobody measured — `:`, `#`, `^` and friends could
        // be replaced or kept, and guessing either way risks failing a healthy sync. A
        // shorter fingerprint is the safe direction: it can only under-check.
        .take_while(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        .take(REF_PREFIX)
        .collect()
}

/// Fail if the branch the server created shows no sign of the ref we asked for.
///
/// A deployment that predates `ref` support drops the field silently and syncs the
/// integration's tracked branch instead — the sync succeeds, the gate passes, and the
/// pull request's dbt models were never compiled. Nothing in the response says which ref
/// was used, so the branch name is the only evidence available (see [`REF_PREFIX`]).
///
/// Only checked when the caller let the server name the branch: with `--branch` the name
/// is the caller's own and carries no ref to look for.
///
/// `Ok(true)` when the branch name confirms the ref, `Ok(false)` when the ref left nothing
/// comparable — the caller warns and reports it as `refVerified: false` — and `Err` when
/// the name contradicts the ref.
fn verify_ref_applied(requested: &str, branch_name: &str, sync_job_id: &str) -> Result<bool> {
    let fingerprint = ref_fingerprint(requested);
    // A ref beginning with a character nobody measured leaves nothing to compare. That's
    // a legal ref (`#1234-fix`, `@release`), and a silent Ok would mean the gate ran
    // unverified with no way to tell.
    if fingerprint.is_empty() {
        return Ok(false);
    }

    let lowered = branch_name.to_ascii_lowercase();
    let matched = match lowered.split_once("dbt-sync/") {
        // Anchored on the marker rather than the start of the name, so a nested
        // `myorg/dbt-sync/…` still gets the strict check: the ref segment begins right
        // after it, and requiring `starts_with` there is what stops a short fingerprint
        // matching the trailing timestamp-hash.
        Some((_, segment)) => segment.starts_with(&fingerprint),
        // No marker to anchor on — a renamed prefix. Weaker on purpose, and knowingly:
        // the trailing `-<timestamp>-<hash>` outlives any rename, so a short fingerprint
        // (`1`, `2026`, a hex character) can still be satisfied by it. Preferred over
        // failing every healthy sync after the POST, which is what anchoring on a
        // literal did.
        None => lowered.contains(&fingerprint),
    };
    if matched {
        return Ok(true);
    }

    anyhow::bail!(
        "the sync started on {branch_name}, which doesn't begin with the ref you asked \
         for ({requested}). Either this deployment predates `ref` support — in which case \
         it is syncing the branch saved on the dbt integration, compiling the wrong code \
         and still passing — or it names branches differently than this check expects. \
         Stopping rather than reporting a result for a ref that may not have been used. \
         The sync is still running: cancel it with `cube dbt cancel <deployment> \
         {sync_job_id}`, and delete the branch with `cube data-model delete-branch \
         <deployment> {branch_name}`. Pass --branch to name the branch yourself, which \
         skips this check."
    )
}

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
/// `ref_verified` is false when `--ref` was given but nothing in it could be checked
/// against the branch name (see [`verify_ref_applied`]) — a warning on stderr is
/// invisible to the `jq` consumer this document exists for, which would otherwise read
/// identically to a verified run. `null` when no `--ref` was given.
fn wait_json(
    started: &Value,
    status: &Value,
    branch_name: &str,
    result: Option<&Value>,
    ref_verified: Option<bool>,
) -> Value {
    serde_json::json!({
        "syncJobId": output::field(started, "syncJobId"),
        "workflowId": output::field(started, "workflowId"),
        "branchName": branch_name,
        "status": output::field(status, "status"),
        "error": status.get("error").cloned().unwrap_or(Value::Null),
        "result": result.cloned().unwrap_or(Value::Null),
        "refVerified": ref_verified.map(Value::Bool).unwrap_or(Value::Null),
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

            // Fail closed if the server didn't honour --ref (see `verify_ref_applied`).
            // `None` when there was nothing to verify: no --ref, or the caller named the
            // branch themselves.
            let ref_verified = match (&r#ref, &branch) {
                (Some(requested), None) => {
                    let verified = verify_ref_applied(requested, &branch_name, &sync_job_id)?;
                    if !verified {
                        eprintln!(
                            "warning: no part of `{requested}` can be checked against the \
                             branch name, so this sync is not verified to have used it"
                        );
                    }
                    Some(verified)
                }
                _ => None,
            };

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

            if output::field(&status, "status") == FAILED {
                if ctx.json {
                    output::print_json(&wait_json(
                        &started,
                        &status,
                        &branch_name,
                        None,
                        ref_verified,
                    ));
                }

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
                    Wait::new("dbt sync result", RESULT_FETCH_TIMEOUT, poll).advising_nothing(),
                    || async {
                        match api.get_optional(&path, &Vec::new()).await? {
                            // Only an object is a result. A 200 carrying `null` — or
                            // an empty object — is the workflow saying "nothing yet",
                            // and treating it as done reported success with
                            // `result: null` in the document.
                            Some(result)
                                if result.as_object().is_some_and(|map| !map.is_empty()) =>
                            {
                                Ok(Progress::Done(result))
                            }
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
                            output::print_json(&wait_json(
                                &started,
                                &status,
                                &branch_name,
                                None,
                                ref_verified,
                            ));
                        }

                        // Carries the recovery for EVERY way the read can fail, not
                        // just the timeout: the branch exists and the cubes are
                        // committed, so a caller can read the result separately and
                        // carry on.
                        return Err(err.context(format!(
                            "dbt sync {sync_job_id} completed on {branch_name}, but its result \
                             could not be read. The sync itself succeeded — read the result with \
                             `cube dbt result {deployment} {sync_job_id}`"
                        )));
                    }
                }
            };

            if ctx.json {
                output::print_json(&wait_json(
                    &started,
                    &status,
                    &branch_name,
                    Some(&result),
                    ref_verified,
                ));
            } else {
                output::success(&format!(
                    "dbt sync {sync_job_id} completed on {branch_name}"
                ));
                print_result(false, &result);
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

    /// Every row is a branch name a live deployment actually returned.
    #[test]
    fn verify_ref_applied_accepts_what_the_server_really_produces() {
        for (requested, branch) in [
            ("main", "dbt-sync/main-20260817140209-9351635f"),
            (
                "refs/heads/main",
                "dbt-sync/refs-heads-main-20260817141045-ca062f89",
            ),
            (
                "release/2026.08",
                "dbt-sync/release-2026.08-20260817141042-5800f094",
            ),
            (
                "feature/CUB-1234-add-orders-fact-table-and-more",
                "dbt-sync/feature-CUB-1234-add-orders-fact-table-a-20260817141039-facd2053",
            ),
            (
                "feat_x_orders",
                "dbt-sync/feat_x_orders-20260817142925-8e26c377",
            ),
            (
                "feat x orders",
                "dbt-sync/feat-x-orders-20260817142930-1ca54ad4",
            ),
            ("feat~x", "dbt-sync/feat-x-20260817142936-a4e47002"),
            (
                "UPPER_lower",
                "dbt-sync/UPPER_lower-20260817142942-eb6730ca",
            ),
        ] {
            assert!(
                verify_ref_applied(requested, branch, "job").unwrap_or(false),
                "rejected or failed to verify a healthy sync: {requested} → {branch}"
            );
        }
    }

    #[test]
    fn verify_ref_applied_rejects_a_branch_that_ignored_it() {
        // What an older deployment produces: the tracked branch, or no ref segment.
        let err = verify_ref_applied("feature/x", "dbt-sync/main-20260817-abcd1234", "job-1")
            .expect_err("a branch naming a different ref must not pass");
        assert!(err.to_string().contains("predates `ref` support"), "{err}");
        assert!(err.to_string().contains("job-1"), "{err}");
        assert!(verify_ref_applied("feature/x", "dbt-sync/20260817-abcd1234", "j").is_err());
    }

    #[test]
    fn ref_fingerprint_matches_the_server_convention() {
        assert_eq!(ref_fingerprint("main"), "main");
        assert_eq!(ref_fingerprint("release/2026.08"), "release-2026");
        assert_eq!(
            ref_fingerprint("feature/CUB-1234-add-orders"),
            "feature-cub-"
        );
        assert_eq!(ref_fingerprint("feat_x_orders"), "feat_x_order");
        assert_eq!(ref_fingerprint("feat x orders"), "feat-x-order");
        assert_eq!(ref_fingerprint("UPPER_lower"), "upper_lower");
        // Unmeasured characters cut the fingerprint short rather than guessing.
        assert_eq!(ref_fingerprint("feat:x"), "feat");
        assert_eq!(ref_fingerprint("#1"), "");
    }

    #[test]
    fn an_uncheckable_ref_passes_but_says_so() {
        // `#1` fingerprints to nothing, so the sync is allowed but reported unverified —
        // asserting `is_ok()` alone would pass against the silent version this replaced.
        assert!(
            !verify_ref_applied("#1", "dbt-sync/main-20260817-abcd1234", "job").unwrap(),
            "an uncheckable ref must be reported as unverified, not as verified"
        );
    }

    #[test]
    fn an_unrecognised_branch_shape_falls_back_to_a_contains_check() {
        // If the server ever renames or nests the prefix, a healthy sync must not fail.
        // A nested prefix still contains the marker, so it keeps the STRICT check — which
        // is what stops a short ref matching the trailing timestamp-hash.
        assert!(verify_ref_applied("main", "myorg/dbt-sync/main-20260817-abcd1234", "j").unwrap());
        assert!(verify_ref_applied("1", "myorg/dbt-sync/main-20260817-abcd1234", "j").is_err());
        // A renamed prefix has no marker: permissive, and knowingly weaker.
        assert!(verify_ref_applied("main", "sync-main-20260817-abcd1234", "j").unwrap());
        // And it still catches a name with no trace of the ref at all.
        assert!(verify_ref_applied("feature/x", "some-other-shape-2026", "j").is_err());
    }

    /// A short ref must still be checked: searching the whole branch name matched the
    /// constant `dbt-sync/` prefix or the trailing hash, so these used to pass against a
    /// sync that ignored the ref entirely.
    #[test]
    fn a_short_ref_is_not_satisfied_by_the_branch_name_boilerplate() {
        for requested in ["sync", "dbt", "b", "1", "2026", "f"] {
            assert!(
                verify_ref_applied(requested, "dbt-sync/main-20260817-abcd1234", "job").is_err(),
                "{requested} was satisfied by the boilerplate around the ref segment"
            );
        }
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
