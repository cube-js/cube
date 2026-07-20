use anyhow::Result;
use clap::Subcommand;
use serde_json::json;

use crate::{output, util, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List workbooks
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Folder id
        #[arg(long)]
        folder: Option<i64>,
        /// Page size (cursor pagination)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Show a workbook
    Get {
        /// Deployment id
        deployment: i64,
        /// Workbook id
        workbook: i64,
    },
    /// Create a workbook
    Create {
        /// Deployment id
        deployment: i64,
        /// Name
        #[arg(long)]
        name: Option<String>,
        /// Folder id
        #[arg(long)]
        folder: Option<i64>,
        /// Full CreateWorkbookInput as JSON (inline, @file, or -)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Update a workbook (rename, move, metadata, slug)
    Update {
        /// Deployment id
        deployment: i64,
        /// Workbook id
        workbook: i64,
        /// Name
        #[arg(long)]
        name: Option<String>,
        /// Destination folder id; pass 0 to move to the workspace root
        #[arg(long)]
        folder: Option<i64>,
        /// Slug
        #[arg(long)]
        slug: Option<String>,
        /// Full UpdateWorkbookInput as JSON (inline, @file, or -)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Delete a workbook
    #[command(alias = "rm")]
    Delete {
        /// Deployment id
        deployment: i64,
        /// Workbook id
        workbook: i64,
    },
    /// Clone a workbook including reports and published dashboard
    Duplicate {
        /// Deployment id
        deployment: i64,
        /// Workbook id
        workbook: i64,
        /// Clone from the shared workspace (creator-mode embed sessions)
        #[arg(long)]
        shared: bool,
    },
    /// Publish a workbook's dashboard
    Publish {
        /// Deployment id
        deployment: i64,
        /// Workbook id
        workbook: i64,
        /// PublishDashboardInput as JSON; workbookId is filled in automatically
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Update a workbook's dashboard draft
    Dashboard {
        /// Deployment id
        deployment: i64,
        /// Workbook id
        workbook: i64,
        /// WorkbookDashboardInput as JSON (inline, @file, or -)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Attach an AI widget thread to a published dashboard
    AiThread {
        /// Deployment id
        deployment: i64,
        /// Workbook id
        workbook: i64,
        /// Widget id
        #[arg(long)]
        widget_id: String,
        /// Thread id
        #[arg(long)]
        thread_id: String,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            folder,
            first,
            after,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "folderId", &folder);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/workbooks"),
                    &query,
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("FOLDER", "folderId"),
                    ("OWNER", "user.email"),
                    ("UPDATED", "updatedAt"),
                ],
            );
        }
        Cmd::Get {
            deployment,
            workbook,
        } => {
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/workbooks/{workbook}"),
                    &Vec::new(),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Create {
            deployment,
            name,
            folder,
            data,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            util::set(&mut body, "name", &name);
            util::set(&mut body, "folderId", &folder);
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/workbooks"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Update {
            deployment,
            workbook,
            name,
            folder,
            slug,
            data,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            util::set(&mut body, "name", &name);
            util::set(&mut body, "slug", &slug);
            if let Some(folder) = folder {
                body.insert(
                    "folderId".to_string(),
                    if folder == 0 {
                        serde_json::Value::Null
                    } else {
                        json!(folder)
                    },
                );
            }
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}/workbooks/{workbook}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete {
            deployment,
            workbook,
        } => {
            api.delete(
                &format!("/api/v1/deployments/{deployment}/workbooks/{workbook}"),
                None,
            )
            .await?;
            output::success(&format!("Deleted workbook {workbook}"));
        }
        Cmd::Duplicate {
            deployment,
            workbook,
            shared,
        } => {
            let body = if shared {
                Some(json!({ "shared": true }))
            } else {
                None
            };
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/workbooks/{workbook}/duplicate"),
                    body.as_ref(),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Publish {
            deployment,
            workbook,
            data,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            body.insert("workbookId".to_string(), json!(workbook));
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/workbooks/{workbook}/publish"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Dashboard {
            deployment,
            workbook,
            data,
        } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}/workbooks/{workbook}/dashboard"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::AiThread {
            deployment,
            workbook,
            widget_id,
            thread_id,
        } => {
            let res = api
                .post(
                    &format!(
                        "/api/v1/deployments/{deployment}/workbooks/{workbook}/dashboard/ai-widget-thread"
                    ),
                    Some(&json!({ "widgetId": widget_id, "threadId": thread_id })),
                )
                .await?;
            output::print_json(&res);
        }
    }
    Ok(())
}
