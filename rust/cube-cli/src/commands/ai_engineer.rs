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
    /// Show AI Engineer settings
    Settings {
        #[arg(long)]
        deployment: Option<i64>,
        #[arg(long)]
        agent: Option<i64>,
        /// Security context as a JSON string
        #[arg(long)]
        security_context: Option<String>,
    },
    /// Show the caller's active agent region
    Region,
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::Settings {
            deployment,
            agent,
            security_context,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "deploymentId", &deployment);
            util::push(&mut query, "agentId", &agent);
            util::push(&mut query, "securityContext", &security_context);
            let res = api.get("/api/v1/ai-engineer/settings", &query).await?;
            output::print_json(&res);
        }
        Cmd::Region => {
            let res = api
                .get("/api/v1/ai-engineer/active-region", &Vec::new())
                .await?;
            output::print_json(&res);
        }
    }
    Ok(())
}
