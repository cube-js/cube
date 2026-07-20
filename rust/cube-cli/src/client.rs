use std::sync::Mutex;

use anyhow::{anyhow, bail, Result};
use reqwest::{Method, StatusCode};
use serde_json::Value;

use crate::oauth;

/// Thin HTTP client over the Cube Cloud public REST API.
///
/// All endpoints take/return JSON; commands work with `serde_json::Value`
/// so the CLI stays forward-compatible with server-side schema additions.
///
/// When constructed with a refresh token (`with_refresh`), the client
/// transparently refreshes an expired access token on a `401` and retries
/// the request once, persisting the new token pair back to the config.
pub struct Client {
    http: reqwest::Client,
    base_url: String,
    token: Mutex<String>,
    refresh: Option<RefreshAuth>,
}

/// State needed to refresh the access token and persist the result.
struct RefreshAuth {
    refresh_token: Mutex<String>,
    /// Config context to write refreshed tokens back to. `None` disables
    /// persistence (e.g. env/flag credentials).
    context_name: Option<String>,
}

pub type Query = Vec<(String, String)>;

impl Client {
    fn build(base_url: &str, token: &str, refresh: Option<RefreshAuth>) -> Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if base_url.is_empty() {
            bail!("API URL is empty");
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .user_agent(concat!("cube-cli/", env!("CARGO_PKG_VERSION")))
                .build()?,
            base_url,
            token: Mutex::new(token.to_string()),
            refresh,
        })
    }

    pub fn new(base_url: &str, token: &str) -> Result<Self> {
        Self::build(base_url, token, None)
    }

    /// Construct a client that can auto-refresh its access token. When
    /// `context_name` is set, refreshed tokens are written back to that
    /// config context.
    pub fn with_refresh(
        base_url: &str,
        token: &str,
        refresh_token: &str,
        context_name: Option<String>,
    ) -> Result<Self> {
        Self::build(
            base_url,
            token,
            Some(RefreshAuth {
                refresh_token: Mutex::new(refresh_token.to_string()),
                context_name,
            }),
        )
    }

    fn token(&self) -> String {
        self.token.lock().unwrap().clone()
    }

    /// Authorization header value for a credential. JWTs (three dot-separated
    /// segments — OAuth access tokens, legacy deploy JWTs) use the `Bearer`
    /// scheme; opaque credentials are Cube Cloud API keys and use `Api-Key`.
    /// `CUBE_AUTH_SCHEME=bearer|api-key` overrides the heuristic.
    fn authorization(token: &str) -> String {
        let scheme = match std::env::var("CUBE_AUTH_SCHEME").as_deref() {
            Ok("bearer") | Ok("Bearer") => "Bearer",
            Ok("api-key") | Ok("Api-Key") => "Api-Key",
            _ if token.split('.').count() == 3 => "Bearer",
            _ => "Api-Key",
        };
        format!("{scheme} {token}")
    }

    /// Send a single attempt, returning the status and body text.
    async fn send_once(
        &self,
        method: &Method,
        path: &str,
        query: &Query,
        body: Option<&Value>,
    ) -> Result<(StatusCode, String)> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method.clone(), &url).header(
            reqwest::header::AUTHORIZATION,
            Self::authorization(&self.token()),
        );
        if !query.is_empty() {
            req = req.query(query);
        }
        if let Some(body) = body {
            req = req.json(body);
        } else if matches!(
            *method,
            Method::POST | Method::PUT | Method::PATCH | Method::DELETE
        ) {
            // Bodyless writes still need an explicit Content-Length: 0, otherwise
            // some frontends (e.g. Google GFE) reject them with 411 Length
            // Required. reqwest omits the header for an empty body, so set it.
            req = req
                .header(reqwest::header::CONTENT_LENGTH, "0")
                .body(Vec::<u8>::new());
        }
        let res = req
            .send()
            .await
            .map_err(|e| anyhow!("request to {url} failed: {e}"))?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        Ok((status, text))
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        query: &Query,
        body: Option<&Value>,
    ) -> Result<Value> {
        let (mut status, mut text) = self.send_once(&method, path, query, body).await?;

        // On 401, try a one-shot token refresh and retry.
        if status == StatusCode::UNAUTHORIZED && self.try_refresh().await? {
            let (s, t) = self.send_once(&method, path, query, body).await?;
            status = s;
            text = t;
        }

        self.finish_response(&method, path, status, text)
    }

    /// POST a multipart form. `build_form` is called per attempt because a
    /// form can only be sent once (the 401-refresh retry needs a fresh one).
    pub async fn post_multipart<F>(&self, path: &str, build_form: F) -> Result<Value>
    where
        F: Fn() -> reqwest::multipart::Form,
    {
        let url = format!("{}{}", self.base_url, path);
        let send = |form: reqwest::multipart::Form, token: String| {
            let http = &self.http;
            let url = &url;
            async move {
                let res = http
                    .post(url)
                    .header(reqwest::header::AUTHORIZATION, Self::authorization(&token))
                    .multipart(form)
                    .send()
                    .await
                    .map_err(|e| anyhow!("request to {url} failed: {e}"))?;
                let status = res.status();
                let text = res.text().await.unwrap_or_default();
                Ok::<_, anyhow::Error>((status, text))
            }
        };

        let (mut status, mut text) = send(build_form(), self.token()).await?;
        if status == StatusCode::UNAUTHORIZED && self.try_refresh().await? {
            let (s, t) = send(build_form(), self.token()).await?;
            status = s;
            text = t;
        }
        self.finish_response(&Method::POST, path, status, text)
    }

    /// Shared tail of every request: error mapping + JSON/HTML handling.
    fn finish_response(
        &self,
        method: &Method,
        path: &str,
        status: StatusCode,
        text: String,
    ) -> Result<Value> {
        if !status.is_success() {
            let detail = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| {
                    v.get("message")
                        .or_else(|| v.get("error"))
                        .map(|m| m.to_string())
                })
                .unwrap_or_else(|| text.clone());
            match status {
                StatusCode::UNAUTHORIZED => bail!(
                    "unauthorized (401): session expired — run `cube login` (or set CUBE_API_KEY). {detail}"
                ),
                StatusCode::FORBIDDEN => bail!("forbidden (403): {detail}"),
                StatusCode::NOT_FOUND => bail!("not found (404): {method} {path}. {detail}"),
                _ => bail!("{method} {path} failed with {status}: {detail}"),
            }
        }

        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        // Cube Cloud serves the web app (200 + HTML) for unknown routes.
        // Surface that as "endpoint not available" instead of returning the
        // HTML as a JSON string, which downstream renders as an empty table.
        let trimmed = text.trim_start();
        let looks_like_html =
            trimmed.len() >= 2 && trimmed.starts_with('<') && !trimmed.starts_with("<?xml");
        if looks_like_html {
            bail!(
                "{method} {path} returned the Cube Cloud web app instead of JSON — \
                 this endpoint is not available on this tenant (the server may be \
                 running an older version)"
            );
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    /// Attempt to refresh the access token. Returns `true` if a new token was
    /// obtained (and the caller should retry), `false` if refresh isn't
    /// possible. Only surfaces an error for unexpected failures.
    async fn try_refresh(&self) -> Result<bool> {
        let Some(refresh) = &self.refresh else {
            return Ok(false);
        };
        let refresh_token = refresh.refresh_token.lock().unwrap().clone();
        let cfg = oauth::OAuthConfig::from_env();
        match oauth::refresh(&self.http, &self.base_url, &cfg, &refresh_token).await {
            Ok(tokens) => {
                *self.token.lock().unwrap() = tokens.access_token.clone();
                let new_refresh = tokens.refresh_token.clone();
                if let Some(rt) = &new_refresh {
                    *refresh.refresh_token.lock().unwrap() = rt.clone();
                }
                if let Some(name) = &refresh.context_name {
                    persist(name, &tokens.access_token, new_refresh.as_deref());
                }
                Ok(true)
            }
            // Fall through to the 401 message, but surface the underlying
            // reason so a transient/endpoint failure isn't mistaken for an
            // expired refresh token ("session expired").
            Err(e) => {
                eprintln!("warning: token refresh failed: {e:#}");
                Ok(false)
            }
        }
    }

    pub async fn get(&self, path: &str, query: &Query) -> Result<Value> {
        self.request(Method::GET, path, query, None).await
    }

    pub async fn post(&self, path: &str, body: Option<&Value>) -> Result<Value> {
        self.request(Method::POST, path, &Vec::new(), body).await
    }

    pub async fn put(&self, path: &str, body: Option<&Value>) -> Result<Value> {
        self.request(Method::PUT, path, &Vec::new(), body).await
    }

    pub async fn patch(&self, path: &str, body: Option<&Value>) -> Result<Value> {
        self.request(Method::PATCH, path, &Vec::new(), body).await
    }

    pub async fn delete(&self, path: &str, body: Option<&Value>) -> Result<Value> {
        self.request(Method::DELETE, path, &Vec::new(), body).await
    }
}

/// Write refreshed tokens back to the named config context (best effort).
fn persist(context_name: &str, access_token: &str, refresh_token: Option<&str>) {
    let Ok(mut config) = crate::config::Config::load() else {
        return;
    };
    if let Some(ctx) = config.contexts.get_mut(context_name) {
        ctx.api_key = access_token.to_string();
        if let Some(rt) = refresh_token {
            ctx.refresh_token = Some(rt.to_string());
        }
        let _ = config.save();
    }
}
