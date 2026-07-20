use anyhow::Result;
use owo_colors::OwoColorize;

use crate::{output, util, Ctx};

/// Tail a deployment's pod logs (API and worker pods; the same source as
/// the UI logs page).
#[derive(clap::Args)]
pub struct Args {
    /// Deployment id
    deployment: i64,
    /// Only this pod (defaults to all tailable pods)
    #[arg(long)]
    pod: Option<String>,
    /// Container within the pod, e.g. cubejs-server
    #[arg(long)]
    container: Option<String>,
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    let mut query = Vec::new();
    util::push(&mut query, "pod", &args.pod);
    util::push(&mut query, "container", &args.container);
    let res = api
        .get(
            &format!("/api/v1/deployments/{}/logs", args.deployment),
            &query,
        )
        .await?;
    if ctx.json {
        output::print_json(&res);
        return Ok(());
    }
    let items = output::items(&res);
    if items.is_empty() {
        eprintln!("{}", "No log lines".dimmed());
        return Ok(());
    }
    for item in items {
        let date = output::field(&item, "date");
        let pod = output::field(&item, "pod");
        let message = output::field(&item, "message");
        println!(
            "{} {} {}",
            date.dimmed(),
            format!("[{pod}]").cyan(),
            message.trim_end()
        );
    }
    Ok(())
}
