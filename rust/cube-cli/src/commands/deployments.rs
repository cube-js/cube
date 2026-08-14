use std::time::Duration;

use anyhow::Result;
use clap::Subcommand;

use crate::client::{Client, Query};
use crate::wait::{self, Progress, Wait};
use crate::{output, util, Ctx};

/// The states this endpoint reports, split by what a wait should do with them.
///
/// The API normalises its internal build-job and dev-worker states into one set:
/// `built` (a finished build, or a running dev worker), `building` (queued, in
/// progress, or a worker spinning up), `warmup`, `failed`, `cancelled`, `stopped`,
/// and `none`. Only the first group ends a wait happily and only the second ends it
/// unhappily; everything else is progress or absence, handled below. An unknown
/// value counts as progress on purpose — a new state server-side should slow a wait
/// down, never make an older CLI call a build finished.
const BUILD_DONE: &[&str] = &["built"];
const BUILD_FAILED: &[&str] = &["failed", "cancelled"];

/// The `errorText` verdicts that mean this branch will never build, each paired
/// with what to actually do about it.
///
/// Both were measured against a live deployment. They need different advice, which
/// is why they aren't one message: "not active" is fixed by opening the branch in
/// dev mode, while "Bad branch" means there is no such branch — and telling someone
/// who mistyped a name to open it in dev mode sends them somewhere that cannot help.
const NOT_BUILDING: &[(&str, &str)] = &[
    (
        "Bad branch",
        "There is no such branch — check the name with `cube data-model branches <deployment>`",
    ),
    (
        "Branch is not active",
        "A branch only compiles once it is opened in dev mode — run \
         `cube data-model dev-mode <deployment> <branch>` and wait on the dev-… branch it prints",
    ),
];

/// How much of a server complaint to keep. Long enough for the known verdicts and
/// a sentence of context, short enough that one poll stays one line.
const COMPLAINT_LIMIT: usize = 120;

/// Squash arbitrary server text into a single short line.
///
/// Kept separate because both users care about different halves: the label needs it
/// STABLE (it is a dedupe key), and the failure message needs it SHORT. Truncation
/// keeps the leading text, which is where these verdicts put their meaning.
fn one_line(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }

    collapsed.chars().take(max_chars).collect::<String>() + "…"
}

/// How long to tolerate an explicit `none`/`stopped` before giving up.
///
/// The backstop, not the main defence: against a live deployment the cases that
/// never finish — an unknown branch, a shared branch with no dev worker — report
/// `building` with an `errorText`, which the loop catches directly and in seconds.
/// This covers a worker that reports itself stopped instead, and gives a starting
/// one a moment before concluding anything.
///
/// Measured in TIME, not polls, so `--poll 1s` doesn't silently cut the window to
/// six seconds and turn a slow worker start into the wrong diagnosis.
const IDLE_GRACE: Duration = Duration::from_secs(60);

/// Poll build-status until the build (or dev-mode worker) reaches a terminal state.
/// Returns the terminal payload; whether that state is a failure is the caller's
/// call, since only it knows what the exit should be.
async fn wait_for_build(
    api: &Client,
    path: &str,
    query: &Query,
    timeout: Duration,
    interval: Duration,
) -> Result<serde_json::Value> {
    let idle_since = std::cell::Cell::new(None::<std::time::Instant>);

    wait::poll(Wait::new("build", timeout, interval), || async {
        let res = api.get(path, query).await?;
        let status = output::field(&res, "status");

        if BUILD_DONE.contains(&status.as_str()) || BUILD_FAILED.contains(&status.as_str()) {
            return Ok(Progress::Done(res));
        }

        // A non-terminal status carrying one of the known verdicts is the endpoint
        // saying "this will never finish", and it is the only way to hear it: both
        // look exactly like a worker spinning up if you read the status alone, so
        // waiting on either sat out the whole timeout in silence.
        //
        // Matched against known verdicts rather than "any errorText", because a
        // healthy in-progress build is NOT guaranteed to carry an empty one. The
        // field is the dev worker's own status proxied through, so whether it can
        // still hold a PREVIOUS failure while a new build runs isn't knowable from
        // this side — and if it can, aborting on any error would fail a rebuild after
        // a red build, on its first poll. That is a worse failure than the wait it
        // replaces, and it would land exactly when CI retries. An unrecognised
        // verdict is therefore waited through, carried in the progress label below.
        // Two forms of the same field, and the split is deliberate. Matching reads the
        // RAW text, since the match is `contains` and truncating first could cut off a
        // verdict that arrives after some prefix — losing the fatal case. Everything
        // that a human reads uses the normalised one: the label is a dedupe key and
        // this is arbitrary text from a worker, so anything volatile in it — a counter,
        // a rotating address — would make every poll a "change" and turn a 15-minute
        // wait into a line per poll, which is exactly the promise `poll` makes.
        // Collapsing whitespace also keeps a multi-line error from becoming several
        // progress lines, or landing whole inside `(last seen: …)` or a failure.
        let error = output::field(&res, "errorText");
        let complaint = one_line(&error, COMPLAINT_LIMIT);

        if let Some((verdict, hint)) = NOT_BUILDING
            .iter()
            .find(|(verdict, _)| error.contains(verdict))
        {
            // Name the verdict, not just the text: matching reads raw while printing
            // is truncated, so in the very case raw matching exists for — a verdict
            // after a long prefix — the printed complaint need not contain it, and a
            // message whose quoted cause doesn't support its own advice is what makes
            // someone doubt the tool. The fuller text is appended only when it says
            // something the verdict doesn't, which today it usually doesn't.
            //
            // Exact equality, not `contains`, and that asymmetry with the match above
            // is deliberate: a decorated verdict ("Error: Bad branch") therefore
            // prints twice, which is untidy, while `contains` would swallow a
            // decoration that carries the detail ("Bad branch: refs/heads/typo not
            // found" would print as the bare verdict). Between repeating a few words
            // and dropping the one line that says WHICH ref was wrong, the repetition
            // is the cheaper mistake. Neither shape is live: both known verdicts
            // arrive bare.
            anyhow::bail!(
                "branch {} is not building ({verdict}){}. {hint}",
                output::field(&res, "branchName"),
                if complaint == *verdict {
                    String::new()
                } else {
                    format!(": {complaint}")
                }
            );
        }

        if status == "none" || status == "stopped" {
            let since = idle_since.get().unwrap_or_else(std::time::Instant::now);
            idle_since.set(Some(since));
            if since.elapsed() > IDLE_GRACE {
                anyhow::bail!(
                    "nothing is building branch {} (status {status}{}). If the branch exists, \
                     it only compiles once it is opened in dev mode — run \
                     `cube data-model dev-mode <deployment> <branch>` and wait on the \
                     dev-… branch it prints",
                    output::field(&res, "branchName"),
                    // This branch bails instead of timing out, so the timeout's
                    // "(last seen: …)" never speaks for it.
                    if complaint.is_empty() {
                        String::new()
                    } else {
                        format!(": {complaint}")
                    }
                );
            }
        } else {
            idle_since.set(None);
        }

        // An unrecognised complaint rides along in the label rather than being
        // announced once and forgotten: the loop reports labels when they CHANGE, so
        // this surfaces a new complaint, stays quiet while it persists, and — since
        // the timeout names the last label — leaves the eventual failure explaining
        // itself instead of pointing at a line printed fifteen minutes earlier.
        Ok(Progress::Waiting(if complaint.is_empty() {
            status
        } else {
            format!("{status} ({complaint})")
        }))
    })
    .await
}

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List deployments
    #[command(alias = "ls")]
    List {
        /// Filter by creation step (repeatable): project, upload, schema, github, ssh, databases, ready, demo
        #[arg(long = "creation-step")]
        creation_step: Vec<String>,
        /// Deprecated: pagination offset (use --after)
        #[arg(long)]
        offset: Option<u64>,
        /// Deprecated: maximum number of items to return (use --first)
        #[arg(long)]
        limit: Option<u64>,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for fetching the next page
        #[arg(long)]
        after: Option<String>,
    },
    /// Show a single deployment
    Get {
        /// Deployment id
        deployment: i64,
    },
    /// Create a deployment
    Create {
        /// Deployment name
        #[arg(long)]
        name: Option<String>,
        /// Region name (see `cube regions`), e.g. aws-us-east-1-2
        #[arg(long)]
        region: Option<String>,
        /// Cloud provider: cubecloud, aws, gcp
        #[arg(long, default_value = "cubecloud")]
        cloud_provider: String,
        /// Target platform, e.g. aws, gcp
        #[arg(long, default_value = "aws")]
        target_platform: String,
        /// Provision a self-managed (BYOC/k8s-hybrid) deployment instead of managed
        #[arg(long)]
        unmanaged: bool,
        /// Creation step: project, upload, schema, github, ssh, databases, ready, demo
        #[arg(long, default_value = "project")]
        creation_step: String,
        /// Deprecated no-op: create always scaffolds and builds now
        #[arg(long, short = 'b', hide = true)]
        bootstrap: bool,
        /// Full CreateDeploymentInput as JSON (overrides the flags above)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Update a deployment (rename, or full UpdateDeploymentInput via --data)
    Update {
        /// Deployment id
        deployment: i64,
        /// Name
        #[arg(long)]
        name: Option<String>,
        /// Release channel: latest, release
        #[arg(long)]
        release_channel: Option<String>,
        /// Cube version to run — see `cube deployments versions <id>`
        #[arg(long)]
        release_channel_version: Option<String>,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Show every setting of a deployment
    Settings {
        /// Deployment id
        deployment: i64,
    },
    /// List the Cube versions a deployment can switch to
    Versions {
        /// Deployment id
        deployment: i64,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Delete a deployment
    #[command(alias = "rm")]
    Delete {
        /// Deployment id
        deployment: i64,
    },
    /// Generate a Cube API token for a deployment
    Token {
        /// Deployment id
        deployment: i64,
    },
    /// Show the latest build status for a branch
    BuildStatus {
        /// Deployment id
        deployment: i64,
        /// Branch (defaults to the active dev-mode branch, else the deploy branch)
        #[arg(long)]
        branch: Option<String>,
        /// Wait for the build (or dev-mode worker) to finish, exiting non-zero if
        /// it fails
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long (30s, 15m, 1h)
        #[arg(long, default_value = "15m", value_parser = util::parse_duration)]
        timeout: std::time::Duration,
        /// How often to poll while waiting
        #[arg(long, default_value = "5s", value_parser = util::parse_duration)]
        poll: std::time::Duration,
    },
    /// Advance onboarding to a target creation step
    AdvanceStep {
        /// Deployment id
        deployment: i64,
        /// Target step: project, upload, schema, github, ssh, databases, ready, demo
        step: String,
    },
    /// Reset onboarding back to the first creation step (project)
    ResetStep {
        /// Deployment id
        deployment: i64,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            creation_step,
            offset,
            limit,
            first,
            after,
        } => {
            let page_field = util::paging_field(
                util::OFFSET_LIMIT_IN_QUERY,
                offset,
                limit,
                first,
                after.as_deref(),
            )?;
            let mut query = Vec::new();
            for step in creation_step {
                query.push(("creationStep".to_string(), step));
            }
            util::push(&mut query, "offset", &offset);
            util::push(&mut query, "limit", &limit);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api.get("/api/v1/deployments/", &query).await?;
            // Deployments page in the database: `items` already holds exactly the
            // requested page and keeps doing so once the deprecated `data` is
            // removed, so render `items`. Asking for `data` here would turn a
            // future no-op into a failure on a command the server still honors.
            // The flags are still validated: the two paging styles can't mix.
            // The server rejects that too; catching it here names the flags.
            output::print_list_from(
                ctx.json,
                &res,
                page_field,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("URL", "deploymentUrl"),
                    ("STEP", "creationStep"),
                ],
            )?;
        }
        Cmd::Get { deployment } => {
            let res = api
                .get(&format!("/api/v1/deployments/{deployment}"), &Vec::new())
                .await?;
            output::print_json(&res);
        }
        Cmd::Create {
            name,
            region,
            cloud_provider,
            target_platform,
            unmanaged,
            creation_step,
            bootstrap,
            data,
        } => {
            // Flags populate the body; --data (if given) overrides them.
            let mut body = serde_json::Map::new();
            util::set(&mut body, "name", &name);
            util::set(&mut body, "region", &region);
            body.insert("cloudProvider".into(), serde_json::json!(cloud_provider));
            body.insert("targetPlatform".into(), serde_json::json!(target_platform));
            body.insert("isManaged".into(), serde_json::json!(!unmanaged));
            body.insert("creationStep".into(), serde_json::json!(creation_step));
            for (k, v) in util::parse_data(data.as_deref())? {
                body.insert(k, v);
            }
            for required in ["name", "region"] {
                if !body.contains_key(required) {
                    anyhow::bail!("--{required} is required (or provide it via --data)");
                }
            }
            // Deployment creation is build-served: it scaffolds the project
            // (unless creationMethod says otherwise) and runs the first
            // build. The old row-only POST /api/v1/deployments is gone —
            // --bootstrap is kept as a hidden no-op for compatibility.
            let _ = bootstrap;
            let res = api
                .post("/build/api/v1/deployments", Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        Cmd::Update {
            deployment,
            name,
            release_channel,
            release_channel_version,
            data,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            util::set(&mut body, "name", &name);
            util::set(&mut body, "releaseChannel", &release_channel);
            util::set(&mut body, "releaseChannelVersion", &release_channel_version);
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Settings { deployment } => {
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/settings"),
                    &Vec::new(),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Versions {
            deployment,
            first,
            after,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/versions"),
                    &query,
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("VERSION", "version"),
                    ("CHANNEL", "releaseChannel"),
                    ("LATEST", "isLatestInChannel"),
                    ("CURRENT", "isCurrent"),
                    ("PASS AS", "releaseChannelVersion"),
                ],
            );
        }
        Cmd::Delete { deployment } => {
            let res = api
                .delete(&format!("/api/v1/deployments/{deployment}"), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Deleted deployment {deployment}"));
            }
        }
        Cmd::BuildStatus {
            deployment,
            branch,
            wait,
            timeout,
            poll,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "branchName", &branch);
            let path = format!("/api/v1/deployments/{deployment}/build-status");

            if !wait {
                let res = api.get(&path, &query).await?;
                output::print_json(&res);

                return Ok(());
            }

            let res = wait_for_build(&api, &path, &query, timeout, poll).await?;
            output::print_json(&res);

            let status = output::field(&res, "status");
            if BUILD_FAILED.contains(&status.as_str()) {
                let error = output::field(&res, "errorText");
                anyhow::bail!(
                    "build {status} for branch {}{}",
                    output::field(&res, "branchName"),
                    if error.is_empty() {
                        String::new()
                    } else {
                        format!(": {error}")
                    }
                );
            }
        }
        Cmd::AdvanceStep { deployment, step } => {
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/creation-step/advance"),
                    Some(&serde_json::json!({ "creationStep": step })),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::ResetStep { deployment } => {
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/creation-step/reset"),
                    None,
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Token { deployment } => {
            let res = api
                .post(&format!("/api/v1/deployments/{deployment}/token"), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                println!("{}", output::field(&res, "cubeApiToken"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_line_collapses_and_truncates() {
        assert_eq!(
            one_line("Branch is not active", 120),
            "Branch is not active"
        );
        assert_eq!(one_line("", 120), "");
        // A multi-line error becomes one progress line, not several.
        assert_eq!(
            one_line("Build failed:\n  cube orders\n  line 3", 120),
            "Build failed: cube orders line 3"
        );
        // Long text is cut, keeping the leading meaning.
        let long = "x".repeat(200);
        let cut = one_line(&long, 10);
        assert_eq!(cut.chars().count(), 11, "10 chars plus the ellipsis");
        assert!(cut.starts_with("xxxxxxxxxx"));
    }

    #[test]
    fn one_line_counts_characters_not_bytes() {
        // Truncating on bytes would panic mid-character here.
        let cut = one_line(&"é".repeat(50), 10);
        assert_eq!(cut.chars().count(), 11);
    }
}
