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
    /// Show user and group policies for a resource
    Get {
        /// Global, Deployment, Report, ReportFolder, Agent, AgentSpace, Workbook, Dashboard, Folder, ChatThread
        #[arg(long)]
        resource_type: String,
        #[arg(long)]
        resource_id: i64,
    },
    /// Set (or clear) a user's policy on a resource
    SetUser {
        #[arg(long)]
        resource_type: String,
        #[arg(long)]
        resource_id: i64,
        #[arg(long)]
        user: Option<i64>,
        /// Action to grant, e.g. WorkbookRead; omit to clear
        #[arg(long)]
        action: Option<String>,
    },
    /// Set (or clear) a group's policy on a resource
    SetGroup {
        #[arg(long)]
        resource_type: String,
        #[arg(long)]
        resource_id: i64,
        #[arg(long)]
        group: i64,
        /// Action to grant, e.g. WorkbookRead; omit to clear
        #[arg(long)]
        action: Option<String>,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::Get {
            resource_type,
            resource_id,
        } => {
            let query = vec![
                ("resourceType".to_string(), resource_type),
                ("resourceId".to_string(), resource_id.to_string()),
            ];
            let res = api.get("/api/v1/resource-policies/", &query).await?;
            output::print_json(&res);
        }
        Cmd::SetUser {
            resource_type,
            resource_id,
            user,
            action,
        } => {
            let mut body = serde_json::Map::new();
            body.insert("resourceType".to_string(), json!(resource_type));
            body.insert("resourceId".to_string(), json!(resource_id));
            util::set(&mut body, "userId", &user);
            util::set(&mut body, "action", &action);
            api.put("/api/v1/resource-policies/user", Some(&util::body(body)))
                .await?;
            output::success("Updated user policy");
        }
        Cmd::SetGroup {
            resource_type,
            resource_id,
            group,
            action,
        } => {
            let mut body = serde_json::Map::new();
            body.insert("resourceType".to_string(), json!(resource_type));
            body.insert("resourceId".to_string(), json!(resource_id));
            body.insert("groupId".to_string(), json!(group));
            util::set(&mut body, "action", &action);
            api.put("/api/v1/resource-policies/group", Some(&util::body(body)))
                .await?;
            output::success("Updated group policy");
        }
    }
    Ok(())
}
