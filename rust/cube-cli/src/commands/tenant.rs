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
    /// Show tenant settings
    Settings,
    /// Update tenant settings (TenantSettingsInput as JSON)
    Update {
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::Settings => {
            let res = api.get("/api/v1/tenant/settings", &Vec::new()).await?;
            output::print_json(&res);
        }
        Cmd::Update { data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .put("/api/v1/tenant/settings", Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
    }
    Ok(())
}
