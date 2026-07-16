use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use serde::Deserialize;

/// OAuth 2.0 Device Authorization Grant (RFC 8628).
///
/// Cube Cloud exposes a confidential authorization-code + device server under
/// `<url>/auth/oauth2`. The device-authorization and token endpoints, the CLI
/// `client_id`, and the optional `client_secret` are the only deployment-
/// specific knobs — everything else is standards-compliant.
///
/// Endpoints implemented by the console-server `DeviceOAuthController`
/// (base `/auth/device`). The `cube-cli` client is public (empty secret),
/// so no `client_secret` is sent. Overridable at runtime with `CUBE_OAUTH_*`
/// env vars for non-default deployments.
const DEVICE_CODE_PATH: &str = "/auth/device/code";
const TOKEN_PATH: &str = "/auth/device/token";
const REFRESH_PATH: &str = "/auth/oauth2/refresh";
const DEFAULT_CLIENT_ID: &str = "cube-cli";
/// Empty scope lets the server default to all OAUTH_SCOPES.
const DEFAULT_SCOPE: &str = "";
const DEVICE_CODE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";

pub struct OAuthConfig {
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scope: String,
}

impl OAuthConfig {
    pub fn from_env() -> Self {
        Self {
            client_id: std::env::var("CUBE_OAUTH_CLIENT_ID")
                .unwrap_or_else(|_| DEFAULT_CLIENT_ID.to_string()),
            client_secret: std::env::var("CUBE_OAUTH_CLIENT_SECRET").ok(),
            scope: std::env::var("CUBE_OAUTH_SCOPE").unwrap_or_else(|_| DEFAULT_SCOPE.to_string()),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeviceAuthorization {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
    #[serde(default = "default_expires_in")]
    pub expires_in: u64,
    #[serde(default = "default_interval")]
    pub interval: u64,
}

fn default_expires_in() -> u64 {
    900
}
fn default_interval() -> u64 {
    5
}

/// Token response from `POST /auth/device/token`. The controller returns
/// camelCase `accessToken`/`refreshToken` (with expiry timestamps, scope, and
/// tenantUrl); aliases keep it tolerant of the RFC 8628 snake_case spelling.
#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    #[serde(alias = "accessToken")]
    pub access_token: String,
    #[serde(default, alias = "refreshToken")]
    pub refresh_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenError {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

fn base(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

/// Step 1 — request a device + user code.
pub async fn request_device_code(
    http: &reqwest::Client,
    url: &str,
    cfg: &OAuthConfig,
) -> Result<DeviceAuthorization> {
    let endpoint = format!("{}{}", base(url), DEVICE_CODE_PATH);
    let mut form = vec![("client_id", cfg.client_id.as_str())];
    if !cfg.scope.is_empty() {
        form.push(("scope", cfg.scope.as_str()));
    }
    if let Some(secret) = &cfg.client_secret {
        form.push(("client_secret", secret));
    }
    let res = http.post(&endpoint).form(&form).send().await?;
    let status = res.status();
    let text = res.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!(
            "device authorization request failed ({status}) at {endpoint}: {}",
            text.trim()
        );
    }
    serde_json::from_str(&text)
        .map_err(|e| anyhow!("could not parse device authorization response: {e}\n{text}"))
}

/// Step 3 — poll the token endpoint until the user approves (or it fails).
pub async fn poll_for_token(
    http: &reqwest::Client,
    url: &str,
    cfg: &OAuthConfig,
    device: &DeviceAuthorization,
) -> Result<TokenResponse> {
    let endpoint = format!("{}{}", base(url), TOKEN_PATH);
    let deadline = Instant::now() + Duration::from_secs(device.expires_in);
    let mut interval = device.interval.max(1);

    loop {
        if Instant::now() >= deadline {
            bail!("device code expired before it was authorized; run `cube login` again");
        }
        tokio::time::sleep(Duration::from_secs(interval)).await;

        let mut form = vec![
            ("grant_type", DEVICE_CODE_GRANT),
            ("device_code", device.device_code.as_str()),
            ("client_id", cfg.client_id.as_str()),
        ];
        if let Some(secret) = &cfg.client_secret {
            form.push(("client_secret", secret));
        }
        let res = http.post(&endpoint).form(&form).send().await?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();

        if status.is_success() {
            return serde_json::from_str(&text)
                .map_err(|e| anyhow!("could not parse token response: {e}\n{text}"));
        }

        // RFC 8628 §3.5: pending/slow_down keep polling; anything else is fatal.
        match serde_json::from_str::<TokenError>(&text) {
            Ok(err) => match err.error.as_str() {
                "authorization_pending" => continue,
                "slow_down" => {
                    interval += 5;
                    continue;
                }
                "access_denied" => bail!("authorization was denied in the browser"),
                "expired_token" => {
                    bail!("device code expired before it was authorized; run `cube login` again")
                }
                other => bail!(
                    "authorization failed: {other}{}",
                    err.error_description
                        .map(|d| format!(" ({d})"))
                        .unwrap_or_default()
                ),
            },
            Err(_) => bail!("token poll failed ({status}) at {endpoint}: {}", text.trim()),
        }
    }
}

/// Exchange a refresh token for a new access/refresh token pair
/// (OAuth 2.0 refresh_token grant). Used transparently by the API client
/// when an access token has expired.
pub async fn refresh(
    http: &reqwest::Client,
    url: &str,
    cfg: &OAuthConfig,
    refresh_token: &str,
) -> Result<TokenResponse> {
    let endpoint = format!("{}{}", base(url), REFRESH_PATH);
    let mut form = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", cfg.client_id.as_str()),
    ];
    if let Some(secret) = &cfg.client_secret {
        form.push(("client_secret", secret));
    }
    let res = http.post(&endpoint).form(&form).send().await?;
    let status = res.status();
    let text = res.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("token refresh failed ({status}): {}", text.trim());
    }
    serde_json::from_str(&text)
        .map_err(|e| anyhow!("could not parse refresh response: {e}\n{text}"))
}

/// Best-effort attempt to open a URL in the user's browser (no extra deps).
pub fn open_browser(url: &str) -> bool {
    let candidates: &[(&str, &[&str])] = if cfg!(target_os = "macos") {
        &[("open", &[])]
    } else if cfg!(target_os = "windows") {
        &[("cmd", &["/C", "start", ""])]
    } else {
        &[("xdg-open", &[]), ("gio", &["open"]), ("wslview", &[])]
    };
    for (cmd, prefix) in candidates {
        let mut c = std::process::Command::new(cmd);
        c.args(prefix.iter()).arg(url);
        c.stdout(std::process::Stdio::null());
        c.stderr(std::process::Stdio::null());
        if c.spawn().is_ok() {
            return true;
        }
    }
    false
}
