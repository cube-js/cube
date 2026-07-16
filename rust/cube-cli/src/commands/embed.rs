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
    /// Generate a one-time embed session (GenerateSession as JSON; admin only)
    GenerateSession {
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Exchange a session id for a signed embed JWT
    Token {
        #[arg(long)]
        session_id: String,
    },
    /// Fetch an embeddable dashboard by public id
    Dashboard { public_id: String },
    /// Manage embed tenants
    Tenant {
        #[command(subcommand)]
        cmd: TenantCmd,
    },
}

#[derive(Subcommand)]
enum TenantCmd {
    /// Delete an embed tenant
    #[command(alias = "rm")]
    Delete { name: String },
    /// List an embed tenant's groups
    Groups {
        name: String,
        #[arg(long)]
        first: Option<u64>,
        #[arg(long)]
        after: Option<String>,
    },
    /// Delete a group of an embed tenant
    DeleteGroup { name: String, group: i64 },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::GenerateSession { data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .post("/api/v1/embed/generate-session", Some(&util::body(body)))
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                println!("{}", output::field(&res, "sessionId"));
            }
        }
        Cmd::Token { session_id } => {
            let res = api
                .post(
                    "/api/v1/embed/session/token",
                    Some(&json!({ "sessionId": session_id })),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                println!("{}", output::field(&res, "token"));
            }
        }
        Cmd::Dashboard { public_id } => {
            let res = api
                .get(&format!("/api/v1/embed/dashboard/{public_id}"), &Vec::new())
                .await?;
            output::print_json(&res);
        }
        Cmd::Tenant { cmd } => match cmd {
            TenantCmd::Delete { name } => {
                api.delete(&format!("/api/v1/embed-tenants/{name}/"), None)
                    .await?;
                output::success(&format!("Deleted embed tenant `{name}`"));
            }
            TenantCmd::Groups { name, first, after } => {
                let mut query = Vec::new();
                util::push(&mut query, "first", &first);
                util::push(&mut query, "after", &after);
                let res = api
                    .get(&format!("/api/v1/embed-tenants/{name}/groups"), &query)
                    .await?;
                output::print_list(
                    ctx.json,
                    &res,
                    &[("ID", "id"), ("NAME", "name"), ("USERS", "userCount")],
                );
            }
            TenantCmd::DeleteGroup { name, group } => {
                api.delete(&format!("/api/v1/embed-tenants/{name}/groups/{group}"), None)
                    .await?;
                output::success(&format!("Deleted group {group} of embed tenant `{name}`"));
            }
        },
    }
    Ok(())
}
