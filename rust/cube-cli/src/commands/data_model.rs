use std::io::Read;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use serde_json::json;

use reqwest::Method;

use crate::client::{Client, Query};
use crate::{output, util, Ctx};

/// Manage a deployment's data model files (schema).
#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List the data model source tree
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Include each file's content in the output
        #[arg(long)]
        content: bool,
        /// Branch to read (defaults to the deployment's default branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// Print a single file's content
    Get {
        /// Deployment id
        deployment: i64,
        /// File path within the project, e.g. model/cubes/orders.yml
        path: String,
        /// Branch name (defaults to the deployment default branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// Create or overwrite a file (writes require a dev-mode branch)
    Put {
        /// Deployment id
        deployment: i64,
        /// Destination path, e.g. model/cubes/orders.yml
        path: String,
        /// Read content from a local file
        #[arg(long, conflicts_with = "content")]
        file: Option<String>,
        /// Inline content (use `-` to read stdin)
        #[arg(long)]
        content: Option<String>,
        /// Dev-mode branch to write to, as returned by `dev-mode` (defaults to
        /// your active dev-mode branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// Delete files (writes require a dev-mode branch)
    #[command(alias = "rm")]
    Delete {
        /// Deployment id
        deployment: i64,
        /// One or more file paths to delete
        #[arg(required = true)]
        paths: Vec<String>,
        /// Dev-mode branch to write to, as returned by `dev-mode` (defaults to
        /// your active dev-mode branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// Rename (move) a file (writes require a dev-mode branch)
    Rename {
        /// Deployment id
        deployment: i64,
        /// Source path
        from: String,
        /// Destination path
        to: String,
        /// Dev-mode branch to write to, as returned by `dev-mode` (defaults to
        /// your active dev-mode branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// List branches
    Branches {
        /// Deployment id
        deployment: i64,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Create a branch (optionally entering dev mode)
    CreateBranch {
        /// Deployment id
        deployment: i64,
        /// Name
        name: String,
        /// Enter dev mode on the new branch
        #[arg(long)]
        dev_mode: bool,
    },
    /// Enable a branch: keep its staging environment always active
    EnableBranch {
        /// Deployment id
        deployment: i64,
        /// Branch to enable (a shared branch — not a personal dev branch)
        branch: String,
    },
    /// Delete a branch and its Cube-side git ref
    DeleteBranch {
        /// Deployment id
        deployment: i64,
        /// Branch to delete
        branch: String,
        /// Also delete the branch on the connected git provider (GitHub/GitLab).
        /// Off by default, so your own remote is left alone unless you say so.
        #[arg(long)]
        remove_on_upstream: bool,
    },
    /// Disable a branch: its staging environment is active only while the
    /// branch is viewed in the Cube UI
    DisableBranch {
        /// Deployment id
        deployment: i64,
        /// Branch to disable
        branch: String,
    },
    /// Enter dev mode on a branch (prints the personal dev-mode branch that
    /// file writes must target)
    DevMode {
        /// Deployment id
        deployment: i64,
        /// Branch to base dev mode on (required by the API)
        branch: String,
    },
    /// Exit dev mode
    ExitDevMode {
        /// Deployment id
        deployment: i64,
    },
    /// Commit and push a branch
    Commit {
        /// Deployment id
        deployment: i64,
        /// Commit message
        #[arg(long, short = 'm')]
        message: Option<String>,
        /// Branch to commit (defaults to the active dev-mode branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// List server-side content hashes of data model files
    FileHashes {
        /// Deployment id
        deployment: i64,
        /// Branch name (defaults to the deployment default branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// Sync a branch from its remote and rebuild if it moved
    Pull {
        /// Deployment id
        deployment: i64,
        /// Branch name (defaults to the deployment default branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// Merge a branch into its parent branch
    Merge {
        /// Deployment id
        deployment: i64,
        /// Branch to merge (defaults to the active dev-mode branch)
        #[arg(long)]
        branch: Option<String>,
        /// Squash commits into one
        #[arg(long)]
        squash: bool,
        /// Switch to the parent branch after merging
        #[arg(long)]
        switch_to_parent: bool,
        /// Delete the branch after merging
        #[arg(long)]
        delete_branch: bool,
    },
    /// Merge a branch straight into the deploy/default branch (production)
    MergeToDefault {
        /// Deployment id
        deployment: i64,
        /// Branch to merge (defaults to the active dev-mode branch)
        #[arg(long)]
        branch: Option<String>,
        /// Commit message
        #[arg(long, short = 'm')]
        message: Option<String>,
        /// Keep the branch after merging (default removes it)
        #[arg(long)]
        keep_branch: bool,
    },
}

fn base(deployment: i64) -> String {
    format!("/build/api/v1/deployments/{deployment}/data-model/files")
}

/// The files endpoint returns a nested tree, each node carrying `path`,
/// `type` (file|directory), `content`, and `children`. Flatten it depth-first
/// into a single list of nodes.
fn flatten(nodes: &[serde_json::Value], out: &mut Vec<serde_json::Value>) {
    for n in nodes {
        out.push(n.clone());
        if let Some(children) = n.get("children").and_then(|c| c.as_array()) {
            flatten(children, out);
        }
    }
}

/// Printed after entering dev mode, so the user knows which branch the write
/// commands accept.
fn dev_branch_hint(dev_branch: &str) -> String {
    format!(
        "Data-model writes target it: pass --branch {dev_branch} \
         (or omit --branch to use your active dev-mode branch)."
    )
}

/// The source tree reports absolute paths (`/model/cubes/orders.yml`) while the
/// write endpoints accept them with or without the leading slash, so compare
/// paths without it.
fn same_path(a: &str, b: &str) -> bool {
    a.trim_start_matches('/') == b.trim_start_matches('/')
}

/// Flatten the whole tree the files endpoint returned. The CLI never asks for
/// a page, so `items` (the canonical field, cursor-sliced over the *top-level*
/// nodes only) carries every root, same as the deprecated `data`.
///
/// `list_field` rather than `items` because only a real array of roots is a
/// tree: `items` would hand back an envelope carrying neither array as a lone
/// node, rendering one blank row where "No results" is the honest answer.
fn tree_nodes(res: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    flatten(&output::list_field(res).unwrap_or_default(), &mut out);
    out
}

/// Write endpoints take the target branch as a `branchName` body field (with
/// the caller's active dev-mode branch as the fallback when omitted).
fn write_body(
    mut body: serde_json::Map<String, serde_json::Value>,
    branch: &Option<String>,
) -> serde_json::Value {
    if let Some(b) = branch {
        body.insert("branchName".into(), json!(b));
    }
    util::body(body)
}

/// Enabling a branch keeps its staging environment always active and accessible
/// at `<deploymentUrl>/dev-mode/<branch>/cubejs-api/v1`; disabled (the default)
/// it is only active while someone views the branch in the Cube UI. Enabled
/// branches are the ones `cube environments list <deployment> --type staging`
/// reports. Only shared branches qualify — personal dev branches are served as
/// your own development environment, and the deploy branch is production.
async fn set_branch_enabled(
    api: &Client,
    ctx: &Ctx,
    deployment: i64,
    branch: &str,
    enabled: bool,
) -> Result<()> {
    let body = json!({ "branchName": branch, "enabled": enabled });
    let res = api
        .put(
            &format!("/build/api/v1/deployments/{deployment}/branches/staging-environment"),
            Some(&body),
        )
        .await?;
    if ctx.json {
        output::print_json(&res);
    } else if enabled {
        output::success(&format!(
            "Enabled branch {branch}; its staging environment stays active \
             (see `cube environments list {deployment} --type staging`)"
        ));
    } else {
        output::success(&format!(
            "Disabled branch {branch}; its staging environment is active only while viewed"
        ));
    }
    Ok(())
}

fn read_content(file: Option<String>, content: Option<String>) -> Result<String> {
    if let Some(path) = file {
        return std::fs::read_to_string(&path).with_context(|| format!("failed to read {path}"));
    }
    match content.as_deref() {
        Some("-") => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            Ok(buf)
        }
        Some(c) => Ok(c.to_string()),
        None => anyhow::bail!("provide --file <path> or --content <text>"),
    }
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            content,
            branch,
        } => {
            let mut query: Query = Vec::new();
            if content {
                query.push(("withContent".into(), "true".into()));
            }
            util::push(&mut query, "branchName", &branch);
            let res = api.get(&base(deployment), &query).await?;
            if ctx.json || content {
                output::print_json(&res);
            } else {
                // Depth-first tree view: mark directories with a trailing slash.
                for n in tree_nodes(&res) {
                    let path = output::field(&n, "path");
                    if output::field(&n, "type") == "directory" {
                        println!("{path}/");
                    } else {
                        println!("{path}");
                    }
                }
            }
        }
        Cmd::Get {
            deployment,
            path,
            branch,
        } => {
            let mut query: Query = vec![("withContent".into(), "true".into())];
            util::push(&mut query, "branchName", &branch);
            let res = api.get(&base(deployment), &query).await?;
            let file = tree_nodes(&res).into_iter().find(|f| {
                same_path(&output::field(f, "path"), &path) && output::field(f, "type") == "file"
            });
            match file {
                Some(f) => print!("{}", output::field(&f, "content")),
                None => anyhow::bail!("file not found: {path}"),
            }
        }
        Cmd::Put {
            deployment,
            path,
            file,
            content,
            branch,
        } => {
            let text = read_content(file, content)?;
            let mut map = serde_json::Map::new();
            map.insert("files".into(), json!([{ "path": path, "content": text }]));
            let res = api
                .put(&base(deployment), Some(&write_body(map, &branch)))
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Wrote {path}"));
            }
        }
        Cmd::Delete {
            deployment,
            paths,
            branch,
        } => {
            // The delete endpoint expects `files` as an array of objects.
            let files: Vec<_> = paths.iter().map(|p| json!({ "path": p })).collect();
            let mut map = serde_json::Map::new();
            map.insert("files".into(), json!(files));
            let res = api
                .delete(&base(deployment), Some(&write_body(map, &branch)))
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Deleted {} file(s)", paths.len()));
            }
        }
        Cmd::Rename {
            deployment,
            from,
            to,
            branch,
        } => {
            // Like put/delete, the rename endpoint expects `files` as an array
            // of objects — here `{ path, newPath }`.
            let mut map = serde_json::Map::new();
            map.insert("files".into(), json!([{ "path": from, "newPath": to }]));
            let res = api
                .post(
                    &format!("{}/rename", base(deployment)),
                    Some(&write_body(map, &branch)),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Renamed {from} -> {to}"));
            }
        }
        Cmd::Branches {
            deployment,
            first,
            after,
        } => {
            let mut query: Query = Vec::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(
                    &format!("/build/api/v1/deployments/{deployment}/branches"),
                    &query,
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("NAME", "name"),
                    ("PARENT", "parentBranch"),
                    ("ENABLED", "isStagingEnvironmentEnabled"),
                ],
            );
        }
        Cmd::CreateBranch {
            deployment,
            name,
            dev_mode,
        } => {
            let body = json!({ "name": name, "enterDevMode": dev_mode });
            let res = api
                .post(
                    &format!("/build/api/v1/deployments/{deployment}/branches"),
                    Some(&body),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                // With --dev-mode the server forks a personal dev-mode branch off
                // the new branch; that's the branch file writes must target.
                let dev_branch = output::field(&res, "branchName");
                if dev_mode && !dev_branch.is_empty() && dev_branch != name {
                    output::success(&format!(
                        "Created branch {name}; entered dev mode on {dev_branch}"
                    ));
                    println!("{}", dev_branch_hint(&dev_branch));
                } else {
                    output::success(&format!("Created branch {name}"));
                }
            }
        }
        Cmd::EnableBranch { deployment, branch } => {
            set_branch_enabled(&api, ctx, deployment, &branch, true).await?;
        }
        Cmd::DeleteBranch {
            deployment,
            branch,
            remove_on_upstream,
        } => {
            // `branchName` travels as a query parameter, not a path segment: branch
            // names contain slashes (`dbt-sync/main-…`). `Client::delete` takes no
            // query, so this goes through `request` directly.
            //
            // Checked here rather than left to the server: a query parameter can be
            // present-but-empty, and what `branchName=` means is then entirely the
            // server's choice — not something to discover with a DELETE. It is also
            // reachable from a script rather than only from a typo, since
            // `jq -r .branchName` yields an empty string when the field is missing.
            anyhow::ensure!(
                !branch.trim().is_empty(),
                "a branch name is required — an empty one would leave the server to \
                 decide what `branchName=` means"
            );
            let mut query: Query = vec![("branchName".to_string(), branch.clone())];
            if remove_on_upstream {
                query.push(("removeOnUpstream".to_string(), "true".to_string()));
            }
            let res = api
                .request(
                    Method::DELETE,
                    &format!("/build/api/v1/deployments/{deployment}/branches"),
                    &query,
                    None,
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else if remove_on_upstream {
                output::success(&format!(
                    "Deleted branch {branch}, including its ref on the git provider"
                ));
            } else {
                output::success(&format!("Deleted branch {branch}"));
            }
        }
        Cmd::DisableBranch { deployment, branch } => {
            set_branch_enabled(&api, ctx, deployment, &branch, false).await?;
        }
        Cmd::DevMode { deployment, branch } => {
            let body = json!({ "branchName": branch });
            let res = api
                .post(
                    &format!("/build/api/v1/deployments/{deployment}/dev-mode"),
                    Some(&body),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                // Dev mode runs on a personal `dev-…` branch forked from the
                // requested one — expose it, since file writes only accept it.
                let dev_branch = output::field(&res, "branchName");
                if dev_branch.is_empty() || dev_branch == branch {
                    output::success(&format!("Entered dev mode on {branch}"));
                } else {
                    output::success(&format!(
                        "Entered dev mode on {dev_branch} (forked from {branch})"
                    ));
                    println!("{}", dev_branch_hint(&dev_branch));
                }
            }
        }
        Cmd::ExitDevMode { deployment } => {
            api.delete(
                &format!("/build/api/v1/deployments/{deployment}/dev-mode"),
                None,
            )
            .await?;
            output::success("Exited dev mode");
        }
        Cmd::Commit {
            deployment,
            message,
            branch,
        } => {
            let mut body = serde_json::Map::new();
            util::set(&mut body, "message", &message);
            util::set(&mut body, "branchName", &branch);
            let res = api
                .post(
                    &format!("/build/api/v1/deployments/{deployment}/commit"),
                    Some(&util::body(body)),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success("Committed and pushed");
            }
        }
        Cmd::FileHashes { deployment, branch } => {
            let mut query = Vec::new();
            util::push(&mut query, "branchName", &branch);
            let res = api
                .get(
                    &format!("/build/api/v1/deployments/{deployment}/data-model/file-hashes"),
                    &query,
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Pull { deployment, branch } => {
            let body = branch.as_ref().map(|b| json!({ "branchName": b }));
            let res = api
                .post(
                    &format!("/build/api/v1/deployments/{deployment}/pull"),
                    body.as_ref(),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success("Pulled");
            }
        }
        Cmd::Merge {
            deployment,
            branch,
            squash,
            switch_to_parent,
            delete_branch,
        } => {
            let mut map = serde_json::Map::new();
            map.insert("squashCommits".into(), json!(squash));
            map.insert("switchToParentBranch".into(), json!(switch_to_parent));
            map.insert("deleteBranch".into(), json!(delete_branch));
            let res = api
                .post(
                    &format!("/build/api/v1/deployments/{deployment}/merge"),
                    Some(&write_body(map, &branch)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::MergeToDefault {
            deployment,
            branch,
            message,
            keep_branch,
        } => {
            let mut map = serde_json::Map::new();
            util::set(&mut map, "message", &message);
            map.insert("removeBranchAfterMerge".into(), json!(!keep_branch));
            let res = api
                .post(
                    &format!("/build/api/v1/deployments/{deployment}/merge-to-default"),
                    Some(&write_body(map, &branch)),
                )
                .await?;
            output::print_json(&res);
        }
    }
    Ok(())
}
