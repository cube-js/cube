use std::io::Read;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use serde_json::json;

use crate::client::Query;
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
    /// Create or overwrite a file
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
        /// Branch name (defaults to the deployment default branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// Delete files
    #[command(alias = "rm")]
    Delete {
        /// Deployment id
        deployment: i64,
        /// One or more file paths to delete
        #[arg(required = true)]
        paths: Vec<String>,
        /// Branch name (defaults to the deployment default branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// Rename (move) a file
    Rename {
        /// Deployment id
        deployment: i64,
        /// Source path
        from: String,
        /// Destination path
        to: String,
        /// Branch name (defaults to the deployment default branch)
        #[arg(long)]
        branch: Option<String>,
    },
    /// List branches
    Branches {
        /// Deployment id
        deployment: i64,
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
    /// Enter dev mode / switch to a branch
    DevMode {
        /// Deployment id
        deployment: i64,
        /// Branch to switch to (required by the API)
        branch: String,
    },
    /// Exit dev mode
    ExitDevMode {
        /// Deployment id
        deployment: i64,
    },
    /// Commit and push the active branch
    Commit {
        /// Deployment id
        deployment: i64,
        /// Commit message
        #[arg(long, short = 'm')]
        message: Option<String>,
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

/// The files endpoint returns a nested tree under `data`, each node carrying
/// `path`, `type` (file|directory), `content`, and `children`. Flatten it
/// depth-first into a single list of nodes.
fn flatten(nodes: &[serde_json::Value], out: &mut Vec<serde_json::Value>) {
    for n in nodes {
        out.push(n.clone());
        if let Some(children) = n.get("children").and_then(|c| c.as_array()) {
            flatten(children, out);
        }
    }
}

fn tree_nodes(res: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    if let Some(arr) = res.get("data").and_then(|d| d.as_array()) {
        flatten(arr, &mut out);
    }
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
            let file = tree_nodes(&res)
                .into_iter()
                .find(|f| output::field(f, "path") == path && output::field(f, "type") == "file");
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
            let mut map = serde_json::Map::new();
            map.insert("from".into(), json!(from));
            map.insert("to".into(), json!(to));
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
        Cmd::Branches { deployment } => {
            let res = api
                .get(
                    &format!("/build/api/v1/deployments/{deployment}/branches"),
                    &Vec::new(),
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("NAME", "name"),
                    ("DEFAULT", "isDefault"),
                    ("CURRENT", "isCurrent"),
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
                output::success(&format!("Created branch {name}"));
            }
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
                output::success(&format!("Entered dev mode on {branch}"));
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
        } => {
            let mut body = serde_json::Map::new();
            util::set(&mut body, "message", &message);
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
