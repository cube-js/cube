use anyhow::Result;

use crate::{output, Ctx};

#[derive(clap::Args)]
pub struct Args {
    /// Context to remove (defaults to the active one)
    #[arg(long)]
    name: Option<String>,
}

pub async fn command(args: Args, ctx: &mut Ctx) -> Result<()> {
    let name = args
        .name
        .or_else(|| ctx.config.default_context.clone())
        .unwrap_or_else(|| "default".to_string());
    if ctx.config.contexts.remove(&name).is_none() {
        output::success(&format!("No saved credentials for context `{name}`"));
        return Ok(());
    }
    if ctx.config.default_context.as_deref() == Some(name.as_str()) {
        ctx.config.default_context = ctx.config.contexts.keys().next().cloned();
    }
    ctx.config.save()?;
    output::success(&format!("Removed context `{name}`"));
    Ok(())
}
