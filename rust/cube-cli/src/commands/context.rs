use anyhow::{bail, Result};
use clap::Subcommand;

use crate::{output, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List saved contexts
    List,
    /// Switch the default context
    Use { name: String },
}

pub async fn command(args: Args, ctx: &mut Ctx) -> Result<()> {
    match args.cmd {
        Cmd::List => {
            let rows = ctx
                .config
                .contexts
                .iter()
                .map(|(name, c)| {
                    let active = if ctx.config.default_context.as_deref() == Some(name.as_str()) {
                        "*"
                    } else {
                        ""
                    };
                    vec![active.to_string(), name.clone(), c.url.clone()]
                })
                .collect();
            output::table(&["", "NAME", "URL"], rows);
        }
        Cmd::Use { name } => {
            if !ctx.config.contexts.contains_key(&name) {
                bail!("context `{name}` not found (run `cube login --name {name}`)");
            }
            ctx.config.default_context = Some(name.clone());
            ctx.config.save()?;
            output::success(&format!("Switched to context `{name}`"));
        }
    }
    Ok(())
}
