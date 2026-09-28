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
    /// List environment variables (secret values are masked by the API)
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Upsert environment variables; omitted variables keep their values
    Set {
        /// Deployment id
        deployment: i64,
        /// Variables as KEY=VALUE pairs
        #[arg(value_parser = util::parse_kv, required = true)]
        vars: Vec<(String, String)>,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            first,
            after,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/env-vars"),
                    &query,
                )
                .await?;
            output::print_list(ctx.json, &res, &[("NAME", "name"), ("VALUE", "value")]);
        }
        Cmd::Set { deployment, vars } => {
            let env_variables: Vec<_> = vars
                .iter()
                .map(|(name, value)| json!({ "name": name, "value": value }))
                .collect();
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}/env-vars"),
                    Some(&json!({ "env_variables": env_variables })),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Set {} variable(s)", vars.len()));
            }
        }
    }
    Ok(())
}
