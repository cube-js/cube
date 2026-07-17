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
        deployment: i64,
        /// File path within the project, e.g. model/cubes/orders.yml
        path: String,
        #[arg(long)]
        branch: Option<String>,
    },
    /// Create or overwrite a file
    Put {
        deployment: i64,
        /// Destination path, e.g. model/cubes/orders.yml
        path: String,
        /// Read content from a local file
        #[arg(long, conflicts_with = "content")]
        file: Option<String>,
        /// Inline content (use `-` to read stdin)
        #[arg(long)]
        content: Option<String>,
        #[arg(long)]
        branch: Option<String>,
    },
    /// Delete files
    #[command(alias = "rm")]
    Delete {
        deployment: i64,
        /// One or more file paths to delete
        #[arg(required = true)]
        paths: Vec<String>,
        #[arg(long)]
        branch: Option<String>,
    },
    /// Rename (move) a file
    Rename {
        deployment: i64,
        from: String,
        to: String,
        #[arg(long)]
        branch: Option<String>,
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

/// Append `?branchName=` to a path when a branch is given (the endpoints take
/// the branch as a query parameter, same as GET's `branchName`).
fn with_branch(path: String, branch: &Option<String>) -> String {
    match branch {
        Some(b) => format!("{path}?branchName={b}"),
        None => path,
    }
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
            let body = json!({ "files": [{ "path": path, "content": text }] });
            let res = api
                .put(&with_branch(base(deployment), &branch), Some(&body))
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
            let body = json!({ "paths": paths });
            let res = api
                .delete(&with_branch(base(deployment), &branch), Some(&body))
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
            let body = json!({ "from": from, "to": to });
            let res = api
                .post(
                    &with_branch(format!("{}/rename", base(deployment)), &branch),
                    Some(&body),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Renamed {from} -> {to}"));
            }
        }
    }
    Ok(())
}
