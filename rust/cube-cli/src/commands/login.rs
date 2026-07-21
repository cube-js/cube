use anyhow::Result;
use owo_colors::OwoColorize;

use crate::client::Client;
use crate::config::ContextConfig;
use crate::{oauth, output, Ctx};

/// Base used when the user doesn't know their tenant URL: the generic
/// sign-in resolves the tenant from the signed-in account and returns it
/// as `tenantUrl` in the token response. `CUBE_GENERIC_LOGIN_URL`
/// overrides the host (e.g. to point at a staging console).
fn generic_url() -> String {
    std::env::var("CUBE_GENERIC_LOGIN_URL").unwrap_or_else(|_| "https://cubecloud.dev".to_string())
}

#[derive(clap::Args)]
pub struct Args {
    /// Cube Cloud URL, e.g. https://<tenant>.cubecloud.dev (omit to sign in
    /// via cubecloud.dev, which finds your tenant from your account)
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
            .with_help_message(
                "don't know your tenant? press Enter to sign in via cubecloud.dev \
                 — it finds your tenant from your account",
            )
            .prompt()?,
    };
    let mut url = url.trim().trim_end_matches('/').to_string();
    if url.is_empty() {
        url = generic_url();
    }

    let (token, refresh_token) = match args.api_key {
        Some(key) => (key, None),
        None => {
            let (token, refresh, tenant_url) = device_login(&url).await?;
            // The generic sign-in reports which tenant the account belongs
            // to — save and validate against that, not cubecloud.dev.
            if let Some(tenant) = tenant_url {
                url = tenant.trim_end_matches('/').to_string();
            }
            (token, refresh)
        }
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

    // Same event name the legacy `cubejs auth` emitted.
    crate::telemetry::event("Cube Cloud CLI Authenticate", serde_json::Map::new());

    output::success(&format!(
        "Logged in as {email} (context `{}`, saved to {})",
        args.name,
        crate::config::config_path()?.display()
    ));
    Ok(())
}

/// Drive the OAuth 2.0 device authorization grant and return
/// (access_token, refresh_token, tenant_url).
async fn device_login(url: &str) -> Result<(String, Option<String>, Option<String>)> {
    let cfg = oauth::OAuthConfig::from_env();
    let http = reqwest::Client::builder()
        .user_agent(concat!("cube-cli/", env!("CUBE_CLI_VERSION")))
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
    println!(
        "and confirm this code:  {}",
        device.user_code.bold().green()
    );
    println!();

    if oauth::open_browser(&verification) {
        println!("{}", "Opened your browser automatically…".dimmed());
    }
    println!("{}", "Waiting for authorization…".dimmed());

    let token = oauth::poll_for_token(&http, url, &cfg, &device).await?;
    Ok((token.access_token, token.refresh_token, token.tenant_url))
}
