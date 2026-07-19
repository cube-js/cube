use anyhow::Result;
use reqwest::Method;

use crate::{output, util, Ctx};

/// Raw authenticated request against the public API — the escape hatch for
/// endpoints without a dedicated command (mirrors `gh api`).
#[derive(clap::Args)]
pub struct Args {
    /// HTTP method: GET, POST, PUT, PATCH, DELETE
    method: String,
    /// Request path, e.g. /api/v1/deployments/
    path: String,
    /// JSON request body (inline, @file, or - for stdin)
    #[arg(long, short = 'd')]
    data: Option<String>,
    /// Query parameters as key=value (repeatable)
    #[arg(long = "query", short = 'q', value_parser = util::parse_kv)]
    query: Vec<(String, String)>,
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let method: Method = args.method.to_uppercase().parse()?;
    let body = match args.data.as_deref() {
        Some(data) => Some(util::body(util::parse_data(Some(data))?)),
        None => None,
    };
    let res = ctx
        .api()?
        .request(method, &args.path, &args.query, body.as_ref())
        .await?;
    output::print_json(&res);
    Ok(())
}
