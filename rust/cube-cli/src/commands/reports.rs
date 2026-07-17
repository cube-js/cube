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
    /// List reports
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Workbook id
        #[arg(long)]
        workbook: Option<i64>,
        /// Folder id
        #[arg(long)]
        folder: Option<i64>,
        /// External workbook
        #[arg(long)]
        external_workbook: Option<String>,
        /// Maximum number of items to return
        #[arg(long)]
        limit: Option<u64>,
        /// Page number
        #[arg(long)]
        page: Option<u64>,
        /// Sort by: name, createdAt, updatedAt, lastViewedAt
        #[arg(long)]
        sort_by: Option<String>,
        /// ASC or DESC
        #[arg(long)]
        sort_direction: Option<String>,
        /// Page size (cursor pagination)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Show a report
    Get {
        /// Deployment id
        deployment: i64,
        /// Report id
        report: i64,
    },
    /// Create a report (CreateReportInput as JSON)
    Create {
        /// Deployment id
        deployment: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: Option<String>,
        /// Name
        #[arg(long)]
        name: Option<String>,
        /// Folder id
        #[arg(long)]
        folder: Option<i64>,
        /// Workbook id
        #[arg(long)]
        workbook: Option<i64>,
        /// Cube JSON query for the report
        #[arg(long)]
        json_query: Option<String>,
        /// SQL query for the report
        #[arg(long)]
        sql_query: Option<String>,
    },
    /// Update a report (UpdateReportInput as JSON)
    Update {
        /// Deployment id
        deployment: i64,
        /// Report id
        report: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: Option<String>,
        /// Name
        #[arg(long)]
        name: Option<String>,
        /// Folder id
        #[arg(long)]
        folder: Option<i64>,
        /// Json query
        #[arg(long)]
        json_query: Option<String>,
        /// Sql query
        #[arg(long)]
        sql_query: Option<String>,
    },
    /// Delete a report
    #[command(alias = "rm")]
    Delete {
        /// Deployment id
        deployment: i64,
        /// Report id
        report: i64,
    },
    /// Re-run a report's query
    Refresh {
        /// Deployment id
        deployment: i64,
        /// Report id
        report: i64,
    },
    /// Link a report to a spreadsheet placement
    ConnectWorkbook {
        /// Deployment id
        deployment: i64,
        /// Report id
        report: i64,
        /// External workbook id
        #[arg(long)]
        external_workbook_id: String,
        /// Result location
        #[arg(long)]
        result_location: String,
        /// End result cell
        #[arg(long)]
        end_result_cell: Option<String>,
    },
    /// List report folders
    Folders {
        /// Deployment id
        deployment: i64,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            workbook,
            folder,
            external_workbook,
            limit,
            page,
            sort_by,
            sort_direction,
            first,
            after,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "workbookId", &workbook);
            util::push(&mut query, "folderId", &folder);
            util::push(&mut query, "externalWorkbookId", &external_workbook);
            util::push(&mut query, "limit", &limit);
            util::push(&mut query, "page", &page);
            util::push(&mut query, "sortBy", &sort_by);
            util::push(&mut query, "sortDirection", &sort_direction);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(&format!("/api/v1/deployments/{deployment}/reports"), &query)
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("PUBLIC ID", "publicId"),
                    ("WORKBOOK", "workbookId"),
                    ("OWNER", "user.email"),
                    ("UPDATED", "updatedAt"),
                ],
            );
        }
        Cmd::Get { deployment, report } => {
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/reports/{report}"),
                    &Vec::new(),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Create {
            deployment,
            data,
            name,
            folder,
            workbook,
            json_query,
            sql_query,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            util::set(&mut body, "name", &name);
            util::set(&mut body, "folderId", &folder);
            util::set(&mut body, "workbookId", &workbook);
            util::set(&mut body, "jsonQuery", &json_query);
            util::set(&mut body, "sqlQuery", &sql_query);
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/reports"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Update {
            deployment,
            report,
            data,
            name,
            folder,
            json_query,
            sql_query,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            util::set(&mut body, "name", &name);
            util::set(&mut body, "folderId", &folder);
            util::set(&mut body, "jsonQuery", &json_query);
            util::set(&mut body, "sqlQuery", &sql_query);
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}/reports/{report}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete { deployment, report } => {
            api.delete(
                &format!("/api/v1/deployments/{deployment}/reports/{report}"),
                None,
            )
            .await?;
            output::success(&format!("Deleted report {report}"));
        }
        Cmd::Refresh { deployment, report } => {
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}/reports/{report}/refresh"),
                    None,
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Refreshed report {report}"));
            }
        }
        Cmd::ConnectWorkbook {
            deployment,
            report,
            external_workbook_id,
            result_location,
            end_result_cell,
        } => {
            let mut body = serde_json::Map::new();
            body.insert(
                "externalWorkbookId".to_string(),
                json!(external_workbook_id),
            );
            body.insert("resultLocation".to_string(), json!(result_location));
            util::set(&mut body, "endResultCell", &end_result_cell);
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}/reports/{report}/connect-workbook"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Folders { deployment } => {
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/report-folders"),
                    &Vec::new(),
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[("ID", "id"), ("NAME", "name"), ("REPORTS", "reportsCount")],
            );
        }
    }
    Ok(())
}
