use anyhow::Result;

use crate::{output, util, Ctx};

/// List the account's available deployment regions (names usable as the
/// `--region` value for `cube deployments create`).
#[derive(clap::Args)]
pub struct Args {
    /// Page size for cursor-based pagination
    #[arg(long)]
    first: Option<u64>,
    /// Cursor for the next page (from a previous pageInfo.endCursor)
    #[arg(long)]
    after: Option<String>,
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let mut query = Vec::new();
    util::push(&mut query, "first", &args.first);
    util::push(&mut query, "after", &args.after);
    let res = ctx.api()?.get("/api/v1/regions", &query).await?;
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
