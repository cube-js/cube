use anyhow::Result;
use clap::Subcommand;
use serde_json::json;

use crate::client::Query;
use crate::{output, util, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args)]
struct ListFlags {
    /// List the contents of this folder (omit for the root)
    #[arg(long)]
    folder: Option<i64>,
    /// Restrict to item types (repeatable): FOLDER, WORKBOOK, REPORT
    #[arg(long = "type")]
    types: Vec<String>,
    /// Case-insensitive substring match on item names
    #[arg(long)]
    search: Option<String>,
    /// Sort field: updated_at, created_at, name, viewer_last_viewed_at
    #[arg(long)]
    order_by: Option<String>,
    /// ASC or DESC
    #[arg(long)]
    direction: Option<String>,
    /// Page size (cursor pagination)
    #[arg(long)]
    first: Option<u64>,
    /// Cursor for the next page (from a previous pageInfo.endCursor)
    #[arg(long)]
    after: Option<String>,
}

#[derive(Subcommand)]
enum Cmd {
    /// List workspace items (folders, workbooks, reports)
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        #[command(flatten)]
        flags: ListFlags,
    },
    /// List items shared with embed users
    Shared {
        /// Deployment id
        deployment: i64,
        #[command(flatten)]
        flags: ListFlags,
    },
    /// Move a workbook, report, or folder into a folder
    Move {
        /// Deployment id
        deployment: i64,
        /// Item type: WORKBOOK, REPORT, or FOLDER
        #[arg(long = "type")]
        item_type: String,
        /// Id of the item to move
        #[arg(long)]
        id: i64,
        /// Destination folder id (omit to move to the workspace root)
        #[arg(long)]
        folder: Option<i64>,
    },
}

fn list_query(flags: &ListFlags) -> Query {
    let mut query = Vec::new();
    util::push(&mut query, "folderId", &flags.folder);
    for t in &flags.types {
        query.push(("types".to_string(), t.clone()));
    }
    util::push(&mut query, "search", &flags.search);
    util::push(&mut query, "orderByField", &flags.order_by);
    util::push(&mut query, "orderByDirection", &flags.direction);
    util::push(&mut query, "first", &flags.first);
    util::push(&mut query, "after", &flags.after);
    query
}

const COLUMNS: &[(&str, &str)] = &[
    ("ID", "id"),
    ("TYPE", "type"),
    ("NAME", "name"),
    ("FOLDER", "folderId"),
    ("UPDATED", "updatedAt"),
];

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List { deployment, flags } => {
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/workspace"),
                    &list_query(&flags),
                )
                .await?;
            output::print_list(ctx.json, &res, COLUMNS);
        }
        Cmd::Shared { deployment, flags } => {
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/shared-workspace"),
                    &list_query(&flags),
                )
                .await?;
            output::print_list(ctx.json, &res, COLUMNS);
        }
        Cmd::Move {
            deployment,
            item_type,
            id,
            folder,
        } => {
            let body = json!({
                "type": item_type,
                "id": id,
                "folderId": folder,
            });
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/workspace/move"),
                    Some(&body),
                )
                .await?;
            output::print_json(&res);
        }
    }
    Ok(())
}
