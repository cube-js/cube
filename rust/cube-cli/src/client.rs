use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use reqwest::{Method, StatusCode};
use serde_json::Value;

use crate::error::api_bail;
use crate::oauth;
use crate::util;

/// The API answered, with a status that isn't a success.
///
/// Typed, rather than a bare message, so callers can branch on the status without
/// matching on wording: [`Client::get_optional`] recognises a 404, and
/// [`is_transient`] tells a wait loop whether retrying could possibly help. The
/// `Display` reproduces exactly the message each status produced before, so
/// anything that only prints the error reads the same.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub method: Method,
    pub path: String,
    pub detail: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            status,
            method,
            path,
            detail,
        } = self;
        match *status {
            StatusCode::UNAUTHORIZED => write!(
                f,
                "unauthorized (401): session expired — run `cube login` (or set CUBE_API_KEY). {detail}"
            ),
            StatusCode::FORBIDDEN => write!(f, "forbidden (403): {detail}"),
            StatusCode::NOT_FOUND => write!(f, "not found (404): {method} {path}. {detail}"),
            _ => write!(f, "{method} {path} failed with {status}: {detail}"),
        }
    }
}

impl std::error::Error for ApiError {}

/// The request never got an answer — DNS, TLS, a dropped connection, a timeout.
#[derive(Debug)]
pub struct TransportError {
    pub url: String,
    pub source: String,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request to {} failed: {}", self.url, self.source)
    }
}

impl std::error::Error for TransportError {}

/// True when the failure was a 404 (see [`ApiError`]).
fn is_not_found(err: &anyhow::Error) -> bool {
    matches!(err.downcast_ref::<ApiError>(), Some(e) if e.status == StatusCode::NOT_FOUND)
}

/// Whether retrying the same request could plausibly succeed — for wait loops,
/// which poll hundreds of times and must not fail a whole job over one blip.
///
/// Deliberately a WHITELIST of "the server was there but unwell" (5xx) plus the
/// two statuses that explicitly mean "later" (408, 429), and requests that never
/// landed at all. Everything else is fatal, which matters most for the errors this
/// function never sees the inside of: a wait closure that has decided something is
/// wrong — an unknown sync id, a branch nothing is building — raises a plain error,
/// and treating THAT as transient would retry a settled verdict until the timeout.
/// Note one failure this cannot see: a 200 whose body isn't JSON is an untyped error
/// from `finish_response`, treated as permanent on purpose — see the comment there.
pub fn is_transient(err: &anyhow::Error) -> bool {
    if let Some(api) = err.downcast_ref::<ApiError>() {
        return api.status.is_server_error()
            || api.status == StatusCode::REQUEST_TIMEOUT
            || api.status == StatusCode::TOO_MANY_REQUESTS;
    }

    err.downcast_ref::<TransportError>().is_some()
}

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

/// The explanation to carry in an [`ApiError`], from whatever the server sent.
///
/// A JSON body's `message`/`error` when there is one, the whole body when there isn't —
/// and normalised either way. The fallback is what makes that matter: a proxy or ingress
/// answering 502 with an HTML page would otherwise put the page inside `POST /… failed
/// with 502: <!DOCTYPE html>…`. That lands worst in the waits, where `poll` retries a 502
/// five times printing this each time, then repeats it as `Last error:` in the timeout —
/// so one bad gateway could bury the handful of progress lines the loop exists to produce.
///
/// `REASON_LIMIT` rather than the label's budget: a real API message is short and stays
/// whole, and the cap only ever bites on the body-as-detail case. Nothing matches on this
/// text — it exists to be read — so collapsing costs nothing.
fn failure_detail(text: &str) -> String {
    let detail = serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| {
            v.get("message")
                .or_else(|| v.get("error"))
                .map(|m| match m.as_str() {
                    // A string message IS the text: `to_string` would render it as a
                    // JSON value and print the quotes around it, in the common case.
                    Some(text) => text.to_string(),
                    // Anything else — an object, a list of validation errors — has no
                    // plainer form, so JSON is the honest rendering.
                    None => m.to_string(),
                })
        })
        .unwrap_or_else(|| text.to_string());

    util::one_line(&detail, util::REASON_LIMIT)
}

impl Client {
    fn build(base_url: &str, token: &str, refresh: Option<RefreshAuth>) -> Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if base_url.is_empty() {
            bail!("API URL is empty");
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .user_agent(concat!("cube-cli/", env!("CUBE_CLI_VERSION")))
                // Three deadlines, because one number can't do this job.
                //
                // `read_timeout` is the working one: it resets after each read, so a
                // server that accepts the connection and then says nothing fails in two
                // minutes, while a transfer making progress is never cut off.
                //
                // The total is a backstop for what `read_timeout` cannot see — a
                // response trickling a byte every 119s, and a server that never reads
                // the request body at all (no read has been attempted, so no read
                // deadline applies). Deliberately far above anything legitimate:
                // `deploy` posts one file per request rather than a whole project, so
                // the sizing case is a single large file plus `upload/finish`
                // committing the manifest, not a project's total size. Waits bound their
                // own attempts at the caller's `--timeout` on top of all of this.
                .connect_timeout(Duration::from_secs(30))
                .read_timeout(Duration::from_secs(120))
                .timeout(Duration::from_secs(1800))
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

    /// Authorization header value for a credential. Cube Cloud API keys are
    /// prefixed `sk-` and use the `Api-Key` scheme; everything else (OAuth
    /// access tokens — not necessarily JWTs — and legacy deploy JWTs) uses
    /// `Bearer`. `CUBE_AUTH_SCHEME=bearer|api-key` overrides the detection.
    fn authorization(token: &str) -> String {
        let scheme = match std::env::var("CUBE_AUTH_SCHEME").as_deref() {
            Ok("bearer") | Ok("Bearer") => "Bearer",
            Ok("api-key") | Ok("Api-Key") => "Api-Key",
            _ if token.starts_with("sk-") => "Api-Key",
            _ => "Bearer",
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
        let res = req.send().await.map_err(|e| {
            anyhow::Error::new(TransportError {
                url: url.clone(),
                source: e.to_string(),
            })
        })?;
        let status = res.status();
        let text = res.text().await.map_err(|e| TransportError {
            url: url.clone(),
            source: e.to_string(),
        })?;
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
                let text = res
                    .text()
                    .await
                    .map_err(|e| anyhow!("reading the response from {url} failed: {e}"))?;
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
            return Err(anyhow::Error::new(ApiError {
                status,
                method: method.clone(),
                path: path.to_string(),
                detail: failure_detail(&text),
            }));
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
            api_bail!(
                "{method} {path} returned the Cube Cloud web app instead of JSON — \
                 this endpoint is not available on this tenant (the server may be \
                 running an older version)"
            );
        }
        // Deliberately NOT one of the typed errors `is_transient` inspects: a body that
        // arrived in full and still isn't JSON is a server that answers this route with
        // something else (a gateway page, a plain-text error), which retrying cannot
        // fix. Bodies lost mid-transfer surface as `TransportError` from `res.text()`
        // instead, and those ARE retried.
        serde_json::from_str(&text).map_err(|e| {
            anyhow!(
                // The same rule as `failure_detail`, which this predated: `replace('\n',
                // …)` collapses newlines only, so an indented gateway page kept its
                // indentation runs, and a hand-rolled `take` cut without saying it had.
                // Same producer as the 502 case, on a status the branch above lets by.
                "{method} {path} returned a body that is not JSON ({e}): {}",
                util::one_line(&text, util::REASON_LIMIT)
            )
        })
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

    /// GET where a 404 is an ANSWER rather than a failure, returned as `None`.
    ///
    /// For endpoints whose "not there" is a normal state a caller polls through —
    /// a dbt sync that has just started and is not yet visible, or one still
    /// running and so without a result. `get` turns every non-2xx into an error,
    /// which is right for a one-shot read and wrong inside a wait loop, where it
    /// would abort the wait instead of continuing it. Every other status still
    /// errors, so a 401 or a 500 stays loud.
    pub async fn get_optional(&self, path: &str, query: &Query) -> Result<Option<Value>> {
        match self.request(Method::GET, path, query, None).await {
            Ok(value) => Ok(Some(value)),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::is_api_error;

    fn client() -> Client {
        Client::new("https://example.cubecloud.dev", "sk-test").unwrap()
    }

    fn finish(status: StatusCode, body: &str) -> Result<Value> {
        client().finish_response(
            &Method::GET,
            "/api/v1/deployments",
            status,
            body.to_string(),
        )
    }

    #[test]
    fn unsuccessful_status_is_an_api_error() {
        let err = finish(StatusCode::BAD_REQUEST, r#"{"message":"unknown field"}"#).unwrap_err();
        assert!(is_api_error(&err));
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn every_mapped_status_is_an_api_error() {
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            let err = finish(status, "").unwrap_err();
            assert!(is_api_error(&err), "{status} should be an API error");
        }
    }

    #[test]
    fn web_app_html_instead_of_json_is_an_api_error() {
        let err = finish(StatusCode::OK, "<!doctype html><html></html>").unwrap_err();
        assert!(is_api_error(&err));
    }

    #[test]
    fn successful_responses_still_parse() {
        assert_eq!(
            finish(StatusCode::OK, r#"{"items":[]}"#).unwrap(),
            serde_json::json!({"items": []})
        );
        assert_eq!(finish(StatusCode::NO_CONTENT, "").unwrap(), Value::Null);
    }

    #[test]
    fn a_gateway_error_page_does_not_become_the_error_message() {
        let page = "<!DOCTYPE html>\n<html>\n  <body>502 Bad Gateway</body>\n</html>";
        let detail = failure_detail(page);
        assert!(!detail.contains('\n'), "one line: {detail}");
        assert!(detail.starts_with("<!DOCTYPE html> <html>"));

        // The cap only bites on a body this size; `poll` prints this up to six times.
        let long = failure_detail(&"x".repeat(5000));
        assert!(long.ends_with('…'));
        assert!(long.chars().count() <= util::REASON_LIMIT + 1);
    }

    #[test]
    fn a_real_api_message_survives_whole() {
        // The common path: short, already one line, and the part worth reading.
        let detail = failure_detail(r#"{"message":"deployment 42 not found"}"#);
        assert_eq!(
            detail, "deployment 42 not found",
            "no JSON quotes around it"
        );
        assert_eq!(
            failure_detail(r#"{"error":"branch is not active"}"#),
            "branch is not active"
        );
        // A message that isn't a string has no plainer form than JSON.
        assert_eq!(
            failure_detail(r#"{"message":{"field":"ref","problem":"unknown"}}"#),
            r#"{"field":"ref","problem":"unknown"}"#
        );
    }
}
