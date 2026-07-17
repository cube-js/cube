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

/// Files write endpoints take a `branchName` alongside the payload.
fn with_branch(mut body: serde_json::Map<String, serde_json::Value>, branch: &Option<String>) -> serde_json::Value {
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
                // Print just the paths for a quick tree view.
                for f in output::items(&res) {
                    println!("{}", output::field(&f, "path"));
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
            let file = output::items(&res)
                .into_iter()
                .find(|f| output::field(f, "path") == path);
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
            let mut body = serde_json::Map::new();
            body.insert(
                "files".into(),
                json!([{ "path": path, "content": text }]),
            );
            let res = api.put(&base(deployment), Some(&with_branch(body, &branch))).await?;
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
            let mut body = serde_json::Map::new();
            body.insert("paths".into(), json!(paths));
            let res = api
                .delete(&base(deployment), Some(&with_branch(body, &branch)))
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
            let mut body = serde_json::Map::new();
            body.insert("from".into(), json!(from));
            body.insert("to".into(), json!(to));
            let res = api
                .post(
                    &format!("{}/rename", base(deployment)),
                    Some(&with_branch(body, &branch)),
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
