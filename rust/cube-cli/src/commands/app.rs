use anyhow::Result;
use clap::Subcommand;

use crate::{output, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Show app-level configuration (theme, creator mode, embedding)
    Config,
    /// Show the app theme
    Theme,
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    let res = match args.cmd {
        Cmd::Config => api.get("/api/v1/app-config", &Vec::new()).await?,
        Cmd::Theme => api.get("/api/v1/app-theme", &Vec::new()).await?,
    };
    output::print_json(&res);
    Ok(())
}
