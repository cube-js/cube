use anyhow::{anyhow, bail, Result};
use reqwest::{Method, StatusCode};
use serde_json::Value;

/// Thin HTTP client over the Cube Cloud public REST API.
///
/// All endpoints take/return JSON; commands work with `serde_json::Value`
/// so the CLI stays forward-compatible with server-side schema additions.
pub struct Client {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

pub type Query = Vec<(String, String)>;

impl Client {
    pub fn new(base_url: &str, token: &str) -> Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if base_url.is_empty() {
            bail!("API URL is empty");
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .user_agent(concat!("cube-cli/", env!("CARGO_PKG_VERSION")))
                .build()?,
            base_url,
            token: token.to_string(),
        })
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        query: &Query,
        body: Option<&Value>,
    ) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self
            .http
            .request(method.clone(), &url)
            .bearer_auth(&self.token);
        if !query.is_empty() {
            req = req.query(query);
        }
        if let Some(body) = body {
            req = req.json(body);
        } else if matches!(method, Method::POST | Method::PUT | Method::PATCH | Method::DELETE) {
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
                    "unauthorized (401): check your API key (`cube login` or CUBE_API_KEY). {detail}"
                ),
                StatusCode::FORBIDDEN => bail!("forbidden (403): {detail}"),
                StatusCode::NOT_FOUND => bail!("not found (404): {method} {path}. {detail}"),
                _ => bail!("{method} {path} failed with {status}: {detail}"),
            }
        }

        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
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
