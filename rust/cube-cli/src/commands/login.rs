use anyhow::Result;
use owo_colors::OwoColorize;

use crate::client::Client;
use crate::config::ContextConfig;
use crate::{oauth, output, Ctx};

#[derive(clap::Args)]
pub struct Args {
    /// Cube Cloud URL, e.g. https://<tenant>.cubecloud.dev
    #[arg(long)]
    url: Option<String>,
    /// Authenticate with an API key instead of the browser device flow
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
    let url = url.trim_end_matches('/').to_string();

    let (token, refresh_token) = match args.api_key {
        Some(key) => (key, None),
        None => device_login(&url).await?,
    };

    // Validate the credentials before saving them.
    let client = Client::new(&url, &token)?;
    let me = client.get("/api/v1/users/me", &Vec::new()).await?;
    let email = output::field(&me, "email");

    ctx.config.contexts.insert(
        args.name.clone(),
        ContextConfig {
            url,
            api_key: token,
            refresh_token,
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

/// Drive the OAuth 2.0 device authorization grant and return
/// (access_token, refresh_token).
async fn device_login(url: &str) -> Result<(String, Option<String>)> {
    let cfg = oauth::OAuthConfig::from_env();
    let http = reqwest::Client::builder()
        .user_agent(concat!("cube-cli/", env!("CARGO_PKG_VERSION")))
        .build()?;

    let device = oauth::request_device_code(&http, url, &cfg).await?;

    let verification = device
        .verification_uri_complete
        .clone()
        .unwrap_or_else(|| device.verification_uri.clone());

    println!();
    println!("To authorize this CLI, open the following URL in your browser:");
    println!("  {}", verification.bold().underline());
    println!();
    println!("and confirm this code:  {}", device.user_code.bold().green());
    println!();

    if oauth::open_browser(&verification) {
        println!("{}", "Opened your browser automatically…".dimmed());
    }
    println!("{}", "Waiting for authorization…".dimmed());

    let token = oauth::poll_for_token(&http, url, &cfg, &device).await?;
    Ok((token.access_token, token.refresh_token))
}
