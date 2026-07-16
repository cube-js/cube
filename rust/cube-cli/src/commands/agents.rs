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
    /// List agents of a deployment
    #[command(alias = "ls")]
    List {
        deployment: i64,
        #[arg(long)]
        allow_embedding: Option<bool>,
    },
    /// List agent skills of a deployment
    Skills {
        deployment: i64,
        #[arg(long)]
        space: Option<String>,
        #[arg(long)]
        branch: Option<String>,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            allow_embedding,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "allowEmbedding", &allow_embedding);
            let res = api
                .get(&format!("/api/v1/deployments/{deployment}/agents"), &query)
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("CONFIG", "agentConfigName"),
                    ("SPACE", "agentSpace.name"),
                ],
            );
        }
        Cmd::Skills {
            deployment,
            space,
            branch,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "space", &space);
            util::push(&mut query, "branchName", &branch);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/agent-skills"),
                    &query,
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("NAME", "name"),
                    ("TITLE", "title"),
                    ("DESCRIPTION", "description"),
                ],
            );
        }
    }
    Ok(())
}
