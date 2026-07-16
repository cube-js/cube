use anyhow::Result;

use crate::client::Client;
use crate::config::ContextConfig;
use crate::{output, Ctx};

#[derive(clap::Args)]
pub struct Args {
    /// Cube Cloud URL, e.g. https://<tenant>.cubecloud.dev
    #[arg(long)]
    url: Option<String>,
    /// API key (prompted interactively when omitted)
    #[arg(long)]
    api_key: Option<String>,
    /// Name to save this context under
    #[arg(long, default_value = "default")]
    name: String,
}

pub async fn command(args: Args, ctx: &mut Ctx) -> Result<()> {
    let url = match args.url {
        Some(url) => url,
        None => inquire::Text::new("Cube Cloud URL:")
            .with_placeholder("https://<tenant>.cubecloud.dev")
            .prompt()?,
    };
    let api_key = match args.api_key {
        Some(key) => key,
        None => inquire::Password::new("API key:")
            .without_confirmation()
            .prompt()?,
    };

    // Validate the credentials before saving them.
    let client = Client::new(&url, &api_key)?;
    let me = client.get("/api/v1/users/me", &Vec::new()).await?;
    let email = output::field(&me, "email");

    ctx.config.contexts.insert(
        args.name.clone(),
        ContextConfig {
            url: url.trim_end_matches('/').to_string(),
            api_key,
        },
    );
    ctx.config.default_context = Some(args.name.clone());
    ctx.config.save()?;

    output::success(&format!(
        "Logged in as {email} (context `{}`, saved to {})",
        args.name,
        crate::config::config_path()?.display()
    ));
    Ok(())
}
