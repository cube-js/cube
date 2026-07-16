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
    /// List user groups
    #[command(alias = "ls")]
    List {
        #[arg(long)]
        first: Option<u64>,
        #[arg(long)]
        after: Option<String>,
    },
    /// Delete a user group
    #[command(alias = "rm")]
    Delete { group: i64 },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List { first, after } => {
            let mut query = Vec::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api.get("/api/v1/user-groups/", &query).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("USERS", "userCount"),
                    ("DESCRIPTION", "description"),
                ],
            );
        }
        Cmd::Delete { group } => {
            api.delete(&format!("/api/v1/groups/{group}"), None).await?;
            output::success(&format!("Deleted group {group}"));
        }
    }
    Ok(())
}
