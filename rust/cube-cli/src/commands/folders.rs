use anyhow::Result;
use clap::Subcommand;

use crate::{output, util, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List folders at the workspace root or inside a parent folder
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// List children of this folder instead of the root
        #[arg(long)]
        parent: Option<i64>,
        /// Page size (cursor pagination)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Create a folder
    Create {
        /// Deployment id
        deployment: i64,
        /// Name
        #[arg(long)]
        name: String,
        /// Parent folder id (omit for workspace root)
        #[arg(long)]
        parent: Option<i64>,
        /// Ordering among siblings
        #[arg(long)]
        position: Option<i64>,
    },
    /// Rename a folder or change its position
    Update {
        /// Deployment id
        deployment: i64,
        /// Folder id
        folder: i64,
        /// Name
        #[arg(long)]
        name: Option<String>,
        /// Position
        #[arg(long)]
        position: Option<i64>,
    },
    /// Delete a folder (must have no sub-folders; content moves to root)
    #[command(alias = "rm")]
    Delete {
        /// Deployment id
        deployment: i64,
        /// Folder id
        folder: i64,
    },
    /// Show the ancestor chain of a folder (breadcrumb)
    Ancestors {
        /// Deployment id
        deployment: i64,
        /// Folder id
        folder: i64,
    },
}

const COLUMNS: &[(&str, &str)] = &[
    ("ID", "id"),
    ("NAME", "name"),
    ("PARENT", "parentId"),
    ("POSITION", "position"),
    ("UPDATED", "updatedAt"),
];

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            parent,
            first,
            after,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "parentId", &parent);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(&format!("/api/v1/deployments/{deployment}/folders"), &query)
                .await?;
            output::print_list(ctx.json, &res, COLUMNS);
        }
        Cmd::Create {
            deployment,
            name,
            parent,
            position,
        } => {
            let mut body = serde_json::Map::new();
            util::set(&mut body, "name", &Some(name));
            util::set(&mut body, "parentId", &parent);
            util::set(&mut body, "position", &position);
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/folders"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Update {
            deployment,
            folder,
            name,
            position,
        } => {
            let mut body = serde_json::Map::new();
            util::set(&mut body, "name", &name);
            util::set(&mut body, "position", &position);
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}/folders/{folder}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete { deployment, folder } => {
            api.delete(
                &format!("/api/v1/deployments/{deployment}/folders/{folder}"),
                None,
            )
            .await?;
            output::success(&format!("Deleted folder {folder}"));
        }
        Cmd::Ancestors { deployment, folder } => {
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/folders/{folder}/ancestors"),
                    &Vec::new(),
                )
                .await?;
            output::print_list(ctx.json, &res, COLUMNS);
        }
    }
    Ok(())
}
