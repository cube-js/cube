use anyhow::Result;
use serde_json::json;

use crate::{output, Ctx};

#[derive(clap::Args)]
pub struct Args {
    /// Metadata selectors as JSON (inline, @file, or - for stdin)
    #[arg(long)]
    selectors: String,
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let selectors: serde_json::Value = {
        // Selectors may be any JSON shape, so parse directly rather than
        // requiring an object like `--data` does.
        let raw = if args.selectors == "-" {
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        } else if let Some(path) = args.selectors.strip_prefix('@') {
            std::fs::read_to_string(path)?
        } else {
            args.selectors.clone()
        };
        serde_json::from_str(&raw)?
    };
    let res = ctx
        .api()?
        .post("/api/v1/meta/", Some(&json!({ "selectors": selectors })))
        .await?;
    output::print_json(&res);
    Ok(())
}
