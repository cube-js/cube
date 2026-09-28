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
    /// List API keys
    #[command(alias = "ls")]
    List,
    /// Create an API key (the secret is only returned once)
    Create {
        /// Key name
        #[arg(long)]
        name: Option<String>,
        /// Extra request body fields as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Show a single API key
    Get {
        /// API key id
        id: String,
    },
    /// Revoke (delete) an API key
    #[command(alias = "rm", alias = "revoke")]
    Delete {
        /// API key id
        id: String,
    },
}

const BASE: &str = "/api/v1/api-keys";

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List => {
            let res = api.get(BASE, &Vec::new()).await?;
            output::print_list(ctx.json, &res, &[("ID", "id"), ("NAME", "name")]);
        }
        Cmd::Create { name, data } => {
            let mut body = util::parse_data(data.as_deref())?;
            util::set(&mut body, "name", &name);
            let res = api.post(BASE, Some(&util::body(body))).await?;
            output::print_json(&res);
        }
        Cmd::Get { id } => {
            let res = api.get(&format!("{BASE}/{id}"), &Vec::new()).await?;
            output::print_json(&res);
        }
        Cmd::Delete { id } => {
            let res = api.delete(&format!("{BASE}/{id}"), None).await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Deleted API key {id}"));
            }
        }
    }
    Ok(())
}
