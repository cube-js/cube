use anyhow::Result;

use crate::{output, Ctx};

/// List the account's available deployment regions (names usable as the
/// `--region` value for `cube deployments create`).
#[derive(clap::Args)]
pub struct Args {}

pub async fn command(_args: Args, ctx: &Ctx) -> Result<()> {
    let res = ctx.api()?.get("/api/v1/regions", &Vec::new()).await?;
    output::print_list(
        ctx.json,
        &res,
        &[
            ("ID", "id"),
            ("NAME", "name"),
            ("TITLE", "title"),
            ("PROVIDER", "provider"),
        ],
    );
    Ok(())
}
