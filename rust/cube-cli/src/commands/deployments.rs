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
    /// List deployments
    #[command(alias = "ls")]
    List {
        /// Filter by creation step (repeatable): project, upload, schema, github, ssh, databases, ready, demo
        #[arg(long = "creation-step")]
        creation_step: Vec<String>,
        #[arg(long)]
        offset: Option<u64>,
        #[arg(long)]
        limit: Option<u64>,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for fetching the next page
        #[arg(long)]
        after: Option<String>,
    },
    /// Show a single deployment
    Get { deployment: i64 },
    /// Generate a Cube API token for a deployment
    Token { deployment: i64 },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            creation_step,
            offset,
            limit,
            first,
            after,
        } => {
            let mut query = Vec::new();
            for step in creation_step {
                query.push(("creationStep".to_string(), step));
            }
            util::push(&mut query, "offset", &offset);
            util::push(&mut query, "limit", &limit);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api.get("/api/v1/deployments/", &query).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("URL", "deploymentUrl"),
                    ("STEP", "creationStep"),
                ],
            );
        }
        Cmd::Get { deployment } => {
            let res = api
                .get(&format!("/api/v1/deployments/{deployment}"), &Vec::new())
                .await?;
            output::print_json(&res);
        }
        Cmd::Token { deployment } => {
            let res = api
                .post(&format!("/api/v1/deployments/{deployment}/token"), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                println!("{}", output::field(&res, "cubeApiToken"));
            }
        }
    }
    Ok(())
}
