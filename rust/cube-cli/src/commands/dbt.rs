use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use clap::Subcommand;
use owo_colors::OwoColorize;
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
    /// Show a dbt sync's phase timeline, including the text a failed phase produced
    Logs {
        /// Deployment id
        deployment: i64,
        /// Sync job id, as returned by `sync`
        sync_job_id: String,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// List a deployment's recent dbt syncs
    #[command(aliases = ["list", "ls"])]
    History {
        /// Deployment id
        deployment: i64,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
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

/// The error a terminal FAILED leaves behind, from both wait paths through one function
/// so the two cannot drift.
///
/// Two sentences, because the reason alone is not the whole answer. The first is the
/// workflow's own words, which is what a human reading a failed job wants — collapsed
/// like the build failure in `deployments`, since a dbt reason is a compile or warehouse
/// error that arrives multi-line and `main` renders an anyhow chain on one line with
/// `{err:#}`; and `is_blank` rather than `is_empty`, because a reason of blanks would
/// fill the slot without answering it on the one line somebody reads when the gate goes
/// red. The second says WHICH PHASE produced it, which is the difference between a red
/// CI step that explains itself and one that only says "dbt sync failed" — and it earns
/// its place most in exactly the case the first sentence cannot fill.
///
/// In the message rather than printed beside it, so it survives `--json` (where advice
/// has no place in the document, but stderr still carries it into the job log) and lands
/// after the reason rather than above it. The only signal a gate itself needs is still
/// the non-zero exit.
fn failure(deployment: i64, sync_job_id: &str, status: &Value) -> anyhow::Error {
    let error = util::one_line(&output::field(status, "error"), util::REASON_LIMIT);
    let reason = if util::is_blank(&error) {
        "(no reason reported)"
    } else {
        &error
    };

    anyhow::anyhow!(
        "dbt sync {sync_job_id} failed: {reason}. See which phase failed with \
         `cube dbt logs {deployment} {}`",
        util::shell_quote(sync_job_id)
    )
}

/// The first of `keys` the payload actually answered with.
///
/// The history and log endpoints are newer than the sync endpoints the rest of this
/// file speaks to, so each field is read under the name its own payload uses and the
/// name the sync payloads already use for the same thing — a run identified as `id`
/// still renders, rather than leaving a column of blanks. Nothing is derived and
/// nothing is guessed at beyond the spelling: a field no key matches stays empty.
///
/// Blank counts as "did not answer", so a padded-empty field cannot win over a real
/// one later in the list.
fn pick(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .map(|key| output::field(value, key))
        .find(|found| !util::is_blank(found))
        .unwrap_or_default()
}

/// Server text as a terminal may safely show it: every control character except the
/// line breaks and tabs the timeline keeps on purpose is dropped.
///
/// This is text the CLI did not write — dbt compile output, warehouse messages, model
/// names — and an ESC sequence in it can retitle a window, move the cursor, or overwrite
/// the lines above it in a CI log. `one_line` is not the guard it looks like: it drops
/// control characters that are WHITESPACE as a side effect of splitting on it, and ESC
/// is not whitespace. Printing raw would be safe only under `--json`, where `serde_json`
/// escapes them.
fn printable(text: &str) -> String {
    text.chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
        .collect()
}

/// How much of one server-supplied value a table cell or a line prefix keeps. Long
/// enough for a branch name, a timestamp or a trigger with room to spare, short enough
/// that one row stays one row: a table is laid out to its widest cell, so an unbounded
/// one would push every other column off the screen.
const CELL_LIMIT: usize = 120;

/// One bounded, printable line of server text — what a table cell and a timeline
/// prefix both need, and where trimming comes from: `one_line` splits on whitespace, so
/// padding and interior newlines go the same way.
fn one_cell(text: &str) -> String {
    util::one_line(&printable(text), CELL_LIMIT)
}

/// A `durationMs` rendered as time, because a sync runs for minutes and `912345` is
/// not a thing anyone reads off a table.
///
/// Anything that is not a plain count of milliseconds is passed through untouched:
/// blank stays blank, and a value this build cannot parse is shown as it arrived
/// rather than turned into a confident `0s`.
fn human_duration_ms(raw: &str) -> String {
    let raw = raw.trim();
    let ms = match raw.parse::<u64>() {
        Ok(ms) => ms,
        // A whole number of milliseconds that arrived as a float (`912345.0`) is still
        // a duration; a negative or non-numeric one is not, and falls through.
        //
        // Bounded, not merely non-negative: an `as` cast SATURATES, so `1e30` would
        // otherwise render as a confident five-billion-hour duration instead of passing
        // through as the nonsense it is.
        Err(_) => match raw.parse::<f64>() {
            Ok(value) if value.is_finite() && value >= 0.0 && value < u64::MAX as f64 => {
                value.round() as u64
            }
            _ => return raw.to_string(),
        },
    };

    let seconds = ms / 1000;
    let (minutes, seconds) = (seconds / 60, seconds % 60);
    let (hours, minutes) = (minutes / 60, minutes % 60);
    match (hours, minutes) {
        (0, 0) if ms < 1000 => format!("{ms}ms"),
        (0, 0) => format!("{seconds}s"),
        (0, _) => format!("{minutes}m {seconds}s"),
        _ => format!("{hours}h {minutes}m"),
    }
}

/// The columns of `history`, paired with the row `history_row` builds — the two are
/// positional, so they are declared next to each other and a test holds them the same
/// width.
const HISTORY_COLUMNS: [&str; 6] = [
    "SYNC JOB ID",
    "STATUS",
    "TRIGGER",
    "STARTED",
    "DURATION",
    "BRANCH",
];

/// The column a row has to fill to be usable at all: without an id, nothing in it can
/// be passed to `logs` or `result`.
const ID_COLUMN: usize = 0;

/// One run as a table row.
fn history_row(run: &Value) -> Vec<String> {
    // Every cell read the same way, and through `one_cell` rather than `pick` alone:
    // these are server strings landing in a laid-out table, where an interior newline
    // breaks the row and an unbounded value pushes the other columns off the screen.
    // Padding goes with them, so a `COMPLETED ` cannot sit beside a `COMPLETED` and
    // read as two outcomes.
    let cell = |keys: &[&str]| one_cell(&pick(run, keys));

    vec![
        cell(&["syncJobId", "id"]),
        // One key, unlike its neighbours, and not an oversight: `status` is the field the
        // sync endpoints already publish and whose two terminal values this file acts on,
        // so a second spelling here would be an invention rather than the other name for
        // a thing already named.
        cell(&["status"]),
        cell(&["trigger", "triggeredBy"]),
        cell(&["startedAt", "createdAt"]),
        // `durationMs` ONLY — never `completedAt` minus `startedAt`. Those two stamps
        // are written by different processes, so their difference can disagree with the
        // server's own figure and, for a run that fails moments after starting, be
        // negative. A run that reports no `durationMs` gets an empty cell, which is the
        // honest answer; a computed one would be a plausible wrong number.
        human_duration_ms(&cell(&["durationMs"])),
        cell(&["branchName", "branch"]),
    ]
}

/// One line of a sync's timeline, read out of whichever fields the entry carried.
///
/// Reading is separated from printing so it can be tested: colour is applied at the
/// call site, where the terminal is, and asserting on ANSI escapes would test
/// owo-colors rather than this.
struct LogEntry {
    /// When it happened; blank when the entry did not say.
    time: String,
    /// The phase it belongs to. Blank stays blank rather than becoming empty brackets,
    /// for the same reason `status_label` does not print them either.
    stage: String,
    message: String,
    /// Whether this entry is a failure, so the line can be red. A level this build
    /// does not recognise leaves it plain: colouring an unknown level red would
    /// announce a failure the server never reported.
    error: bool,
}

fn log_entry(value: &Value) -> LogEntry {
    let level = pick(value, &["level", "severity"]);
    LogEntry {
        time: one_cell(&pick(value, &["timestamp", "createdAt"])),
        stage: one_cell(&pick(value, &["stage", "phase"])),
        // Kept whole, unlike the prefix beside it and the poll label's `one_line`: this
        // is the failure text itself, printed once, and a dbt compile error means its
        // line breaks. Collapsing them would apply the label's rule where it does harm —
        // so the control characters `one_line` would have taken with the newlines are
        // dropped deliberately instead.
        message: printable(pick(value, &["message", "text"]).trim_end()),
        error: matches!(
            level.trim().to_ascii_uppercase().as_str(),
            "ERROR" | "FATAL" | "CRITICAL"
        ),
    }
}

/// A failure's text is red wherever it lands, the raw-entry fallback below included:
/// the entry this build understood least is the last place to drop the signal that it
/// is a failure.
fn paint_failure(text: String, error: bool) -> String {
    if error {
        text.red().to_string()
    } else {
        text
    }
}

/// One rendered line of the timeline.
///
/// The colour lives here rather than at the call site, next to the choice of what to
/// show: the fallback below has to drop a prefix as well as swap the text, and those
/// are one decision rather than two.
fn log_line(value: &Value) -> String {
    let LogEntry {
        time,
        stage,
        message,
        error,
    } = log_entry(value);

    // The text is why somebody ran this command, so an entry this build cannot find it
    // in is shown as it arrived rather than dropped — and on its own: the raw JSON
    // already carries the timestamp and the stage that would otherwise prefix it, and
    // `serde_json` escapes the control characters `printable` exists to drop.
    if util::is_blank(&message) {
        return paint_failure(value.to_string(), error);
    }

    let mut parts = Vec::new();
    if !util::is_blank(&time) {
        parts.push(time.dimmed().to_string());
    }
    // A blank stage adds no empty brackets, for the reason `status_label` prints none.
    if !util::is_blank(&stage) {
        parts.push(format!("[{stage}]").cyan().to_string());
    }
    parts.push(paint_failure(message, error));

    parts.join(" ")
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

                return Err(failure(deployment, &sync_job_id, &status));
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
                    return Err(failure(deployment, &sync_job_id, &status));
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
        Cmd::Logs {
            deployment,
            sync_job_id,
            first,
            after,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let path = format!("{}/{sync_job_id}/logs", base(deployment));
            // 404 is the tenant answering rather than a transport failure, and the three
            // things it can mean are all actionable — so say them, the way `status` does,
            // instead of leaving a bare status line to be interpreted.
            let Some(res) = api.get_optional(&path, &query).await? else {
                bail!(
                    "no logs for dbt sync {sync_job_id} on deployment {deployment}. It may \
                     belong to another deployment, have aged out, or this tenant may not \
                     serve the dbt sync history endpoints yet"
                );
            };

            if ctx.json {
                output::print_json(&res);

                return Ok(());
            }

            let entries = output::items(&res);
            if entries.is_empty() {
                // Not a failure: a sync that has only just started has no timeline yet.
                eprintln!("{}", "No log entries".dimmed());

                return Ok(());
            }

            for entry in entries {
                println!("{}", log_line(&entry));
            }
        }
        Cmd::History {
            deployment,
            first,
            after,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let Some(res) = api.get_optional(&base(deployment), &query).await? else {
                bail!(
                    "no dbt sync history for deployment {deployment}. The deployment may \
                     not exist or may not be visible to this credential, or this tenant \
                     may not serve the dbt sync history endpoints yet"
                );
            };

            if ctx.json {
                output::print_json(&res);

                return Ok(());
            }

            let rows: Vec<Vec<String>> = output::items(&res).iter().map(history_row).collect();
            // A page whose rows name no sync at all is one this build could not read:
            // every run has an id, and a row without one cannot be passed on to `logs` or
            // `result` either. Printed as blanks it would read as "these syncs are empty"
            // rather than "this CLI did not understand them", so say so and point at
            // `--json`, which answers whatever the columns cannot name.
            //
            // Keyed on the id rather than on every cell being blank, which a single
            // filled column was enough to defeat. It still does not claim to catch ONE
            // renamed field: a column of blanks beside filled ones is visible on its own,
            // and a warning per column would fire on every legitimately empty one.
            if !rows.is_empty() && rows.iter().all(|row| util::is_blank(&row[ID_COLUMN])) {
                eprintln!(
                    "warning: these sync rows name no sync job id — re-run with --json, \
                     or update the CLI with `cube update`"
                );
            }
            output::table(&HISTORY_COLUMNS, rows);
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

    /// The cell a named column holds, so a test names the column rather than an index
    /// that a reordering would quietly point somewhere else.
    fn cell(row: &[String], column: &str) -> String {
        let index = HISTORY_COLUMNS
            .iter()
            .position(|header| *header == column)
            .unwrap_or_else(|| panic!("no {column} column"));

        row[index].clone()
    }

    #[test]
    fn a_history_row_fills_every_column() {
        // Positional, so a column added to one and not the other would shift every cell
        // after it into the wrong header.
        let row = history_row(&json!({}));
        assert_eq!(row.len(), HISTORY_COLUMNS.len());
        assert!(row.iter().all(|value| value.is_empty()));
        // And the column `history`'s warning reads is the one it means.
        assert_eq!(HISTORY_COLUMNS[ID_COLUMN], "SYNC JOB ID");
    }

    #[test]
    fn a_run_renders_under_either_spelling_of_its_fields() {
        let canonical = json!({
            "syncJobId": "abc", "status": "COMPLETED", "trigger": "API",
            "startedAt": "2026-08-24T10:00:00Z", "durationMs": 912_345,
            "branchName": "dbt-sync/main-1"
        });
        assert_eq!(
            history_row(&canonical),
            vec![
                "abc",
                "COMPLETED",
                "API",
                "2026-08-24T10:00:00Z",
                "15m 12s",
                "dbt-sync/main-1"
            ]
        );
        // The names the sync payloads use for the same things: a row is worth showing
        // under either, and a field no key matches stays empty rather than inventing one.
        let alternate = json!({
            "id": "abc", "triggeredBy": "API", "createdAt": "2026-08-24T10:00:00Z",
            "branch": "dbt-sync/main-1"
        });
        assert_eq!(cell(&history_row(&alternate), "SYNC JOB ID"), "abc");
        assert_eq!(cell(&history_row(&alternate), "TRIGGER"), "API");
        assert_eq!(cell(&history_row(&alternate), "BRANCH"), "dbt-sync/main-1");
        // Blank is "did not answer", so it cannot win over the spelling that did.
        let padded = json!({ "syncJobId": "   ", "id": "abc" });
        assert_eq!(cell(&history_row(&padded), "SYNC JOB ID"), "abc");
        // And a padded status names the state it reports, like everywhere else here.
        assert_eq!(
            cell(&history_row(&json!({"status": " FAILED\n"})), "STATUS"),
            "FAILED"
        );
    }

    #[test]
    fn a_duration_is_the_servers_own_figure_or_nothing() {
        // The trap this column exists to avoid: both stamps present, no `durationMs`.
        // They are written by different processes, so their difference can disagree with
        // the server's figure and — for a run that fails moments after starting — be
        // negative. An empty cell is the honest answer; a computed one would be a
        // plausible wrong number.
        let stamps_only = json!({
            "syncJobId": "abc",
            "startedAt": "2026-08-24T10:00:00Z",
            "completedAt": "2026-08-24T10:05:00Z"
        });
        assert_eq!(cell(&history_row(&stamps_only), "DURATION"), "");
    }

    #[test]
    fn a_duration_reads_as_time() {
        // A sync runs for minutes, so the unit has to survive being read off a table.
        assert_eq!(human_duration_ms("999"), "999ms");
        assert_eq!(human_duration_ms("1000"), "1s");
        assert_eq!(human_duration_ms("59999"), "59s");
        assert_eq!(human_duration_ms("60000"), "1m 0s");
        assert_eq!(human_duration_ms("912345"), "15m 12s");
        assert_eq!(human_duration_ms("3600000"), "1h 0m");
        assert_eq!(human_duration_ms("5430000"), "1h 30m");
        // Serialised as a float, which is still a duration.
        assert_eq!(human_duration_ms("912345.0"), "15m 12s");
        // Not a count of milliseconds: shown as it arrived rather than as a confident
        // `0s`, which would report a run that took a quarter of an hour as instant.
        assert_eq!(human_duration_ms(""), "");
        assert_eq!(human_duration_ms("  "), "");
        assert_eq!(human_duration_ms("-1"), "-1");
        assert_eq!(human_duration_ms("PT15M"), "PT15M");
        // An `as` cast saturates, so this has to be rejected before it becomes a
        // confident five-billion-hour duration.
        assert_eq!(human_duration_ms("1e30"), "1e30");
        assert_eq!(human_duration_ms("inf"), "inf");
    }

    #[test]
    fn a_failure_names_the_reason_and_where_the_phase_is() {
        let message = failure(
            42,
            "sync-1",
            &json!({"error": "Compilation Error in model fct_orders\n  depends on 'stg_orders'"}),
        )
        .to_string();
        // One line, because `main` renders the chain with `{err:#}`.
        assert_eq!(
            message,
            "dbt sync sync-1 failed: Compilation Error in model fct_orders depends on \
             'stg_orders'. See which phase failed with `cube dbt logs 42 'sync-1'`"
        );
        // A reason of blanks fills the slot without answering it, so it is not a reason —
        // and this is the case where the second sentence is the only answer there is.
        for status in [json!({}), json!({"error": "   "})] {
            let message = failure(42, "sync-1", &status).to_string();
            assert!(message.contains("(no reason reported)"), "{message}");
            assert!(message.contains("cube dbt logs 42 'sync-1'"), "{message}");
        }
        // Quoted, like every other suggested command here: an id is opaque in practice,
        // but these are copied out of CI logs without being reread.
        assert!(failure(42, "a;rm -rf b", &json!({}))
            .to_string()
            .contains("`cube dbt logs 42 'a;rm -rf b'`"));
    }

    #[test]
    fn a_log_entry_says_only_what_it_carried() {
        let entry = log_entry(&json!({
            "timestamp": "2026-08-24T10:00:01Z",
            "stage": "COMPILING_DBT",
            "level": "info",
            "message": "Parsing dbt project\n"
        }));
        assert_eq!(entry.time, "2026-08-24T10:00:01Z");
        assert_eq!(entry.stage, "COMPILING_DBT");
        // Trailing whitespace only: this is the failure text itself, printed once, and a
        // dbt compile error means its line breaks — unlike a poll label, which repeats.
        assert_eq!(entry.message, "Parsing dbt project");
        assert!(!entry.error);
        // A blank stage stays blank, so the line cannot render as empty brackets.
        assert!(log_entry(&json!({"stage": "  "})).stage.is_empty());
        // Failure levels colour the line; anything else is left plain rather than
        // announcing a failure the server never reported.
        for level in ["error", "ERROR", " Fatal ", "critical"] {
            assert!(log_entry(&json!({"level": level})).error, "{level}");
        }
        for level in ["warn", "WARNING", "info", "", "  "] {
            assert!(!log_entry(&json!({"level": level})).error, "{level}");
        }
    }

    #[test]
    fn a_log_line_carries_the_prefix_its_entry_answered_for() {
        let line = log_line(&json!({
            "timestamp": "2026-08-24T10:00:01Z",
            "stage": "COMPILING_DBT",
            "message": "Parsing dbt project"
        }));
        assert!(line.contains("2026-08-24T10:00:01Z"), "{line}");
        assert!(line.contains("[COMPILING_DBT]"), "{line}");
        // Uncoloured, so the text arrives verbatim rather than wrapped.
        assert!(line.ends_with("Parsing dbt project"), "{line}");
        // A blank stage adds no empty brackets.
        assert!(!log_line(&json!({"stage": " ", "message": "x"})).contains('['));
    }

    #[test]
    fn an_entry_whose_text_this_build_cannot_find_is_shown_as_it_arrived() {
        // Exactly the entry, once: the JSON already carries whatever a prefix would
        // repeat, so it is printed alone rather than after a timestamp and a stage.
        let unknown = json!({"ts": "2026-08-24T10:00:01Z", "detail": "a newer shape"});
        assert_eq!(log_line(&unknown), unknown.to_string());
        // A failure keeps its colour even here — the entry this build understood least is
        // the last place to drop the signal that it is one.
        let failed = json!({"level": "error", "detail": "no message field"});
        let line = log_line(&failed);
        assert!(line.contains(&failed.to_string()), "{line}");
        assert_ne!(line, failed.to_string(), "still coloured: {line}");
    }

    #[test]
    fn server_text_cannot_drive_the_terminal() {
        // dbt output and warehouse errors are text this CLI did not write, and an ESC
        // sequence in one can retitle a window, move the cursor, or overwrite the lines
        // above it in a CI log. The line breaks a compile error means are kept; the rest
        // of the control characters are not.
        let entry = log_entry(&json!({
            "stage": "COMPILING_DBT\u{1b}[2J",
            "message": "Compilation Error\n\u{1b}]0;retitled\u{7}  in model fct_orders\tx"
        }));
        assert_eq!(entry.stage, "COMPILING_DBT[2J");
        assert_eq!(
            entry.message,
            "Compilation Error\n]0;retitled  in model fct_orders\tx"
        );
        // The same for a cell, which is one line as well: a newline in a value would
        // otherwise break the row it sits in, and an unbounded one the whole layout.
        let row = history_row(&json!({
            "branchName": "dbt-sync/a\u{1b}[2J\nb",
            "trigger": "T".repeat(500)
        }));
        assert_eq!(cell(&row, "BRANCH"), "dbt-sync/a[2J b");
        let trigger = cell(&row, "TRIGGER");
        assert!(trigger.ends_with('…'), "bounded: {trigger}");
        assert_eq!(trigger.chars().count(), CELL_LIMIT + 1);
    }

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
