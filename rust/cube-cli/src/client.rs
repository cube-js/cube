use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use reqwest::{Method, StatusCode};
use serde_json::Value;

use crate::oauth;
use crate::util;

/// The API answered, with a status that isn't a success.
///
/// Typed, rather than a bare message, so callers can branch on the status without
/// matching on wording: [`Client::get_optional`] recognises a 404, and
/// [`is_transient`] tells a wait loop whether retrying could possibly help. The
/// `Display` reproduces the message each status produced before, with one deliberate
/// exception: a detail that says nothing takes its separator with it, rather than
/// leaving a dangling `: ` promising a reason that never comes.
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
        // Each arm joins the detail with its own punctuation, and drops both when there
        // is nothing to join: a 502 that answers with no body at all would otherwise end
        // at `failed with 502 Bad Gateway: `, a separator promising a reason that never
        // comes. Four arms, one rule, so it can't be fixed in three of them.
        let tail = |separator: &str| {
            if util::is_blank(detail) {
                String::new()
            } else {
                format!("{separator}{detail}")
            }
        };
        match *status {
            StatusCode::UNAUTHORIZED => write!(
                f,
                "unauthorized (401): session expired — run `cube login` (or set CUBE_API_KEY).{}",
                tail(" ")
            ),
            StatusCode::FORBIDDEN => write!(f, "forbidden (403){}", tail(": ")),
            StatusCode::NOT_FOUND => write!(f, "not found (404): {method} {path}{}", tail(". ")),
            _ => write!(f, "{method} {path} failed with {status}{}", tail(": ")),
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
    let body = serde_json::from_str::<Value>(text).ok();

    // Each candidate is tried for what it SAYS, not for being present: a key that is
    // there and empty is the server declining to explain, and must not win over the next
    // candidate or over the body. Same rule `reported_ref_verified` and `is_blank`
    // settled elsewhere on this branch.
    let said = body.as_ref().and_then(|v| {
        ["message", "error"]
            .iter()
            .filter_map(|key| v.get(*key))
            .find_map(explains)
    });

    let detail = said.unwrap_or_else(|| match body.as_ref() {
        // A different question down here, and `says_nothing` rather than `explains` is
        // what asks it: not "does this explain better than what's beside it" — nothing is
        // — but "did the server say anything at all". A body of `400` is a poor reason
        // and the only one there is, and rejecting it would have printed less than both
        // its neighbours: `{"message":400}` renders, and an unparseable `Bad Gateway`
        // renders. When there is genuinely nothing, an empty detail is honest, and
        // `Display` drops its separator rather than promising a reason that never comes.
        Some(body) if says_nothing(body) => String::new(),
        // A body that IS a string is text that happened to arrive quoted, and unwrapping
        // it is the same rule `explains` applies to a string candidate — otherwise the
        // same content renders two ways depending on whether it came wrapped in an
        // object. The unwrapped shape is what a proxy sends (`send(JSON.stringify("Bad
        // gateway"))`), and proxies are what this fallback is for.
        Some(Value::String(said)) => said.to_string(),
        // Everything else verbatim rather than re-serialised, which is what keeps an
        // object's keys in the order the server sent them. See `explains`' "On key order"
        // for why that differs one level up, and for what would change it.
        _ => text.to_string(),
    });

    util::one_line(&detail, util::REASON_LIMIT)
}

/// Whether a value is the server saying nothing at all: a `null`, an empty container, a
/// blank string. The floor both levels of [`failure_detail`] share.
fn says_nothing(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(said) => util::is_blank(said),
        Value::Object(fields) => fields.is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    }
}

/// The explanation a JSON value carries, if it carries one — the stricter of the two
/// questions, asked where a BETTER candidate may sit beside this one.
///
/// A non-blank string is the text itself: rendering it as a JSON value would print the
/// quotes around it, in the commonest case of all. A non-empty object or list has no
/// plainer form, so JSON is the honest rendering — that is where validation errors arrive.
///
/// A bare number or bool is rejected here even though it says something, because what it
/// says is a CODE: `failed with 400 Bad Request: 400` adds nothing to a line that already
/// said 400, while the `error` one candidate along may be the sentence somebody can act
/// on. That reasoning is why the fallback does NOT reuse this — there, nothing sits
/// beside it, and rejecting a bare scalar would drop the only thing the server said.
///
/// # On key order
///
/// This is the one place that spells the rule out; every other site that depends on it
/// points here rather than restating it, so there is a single thing to revisit.
///
/// An object candidate is re-rendered from the parsed value, and `serde_json`'s map is a
/// `BTreeMap` here, so its keys come back SORTED. The fallback in [`failure_detail`]
/// escapes that by keeping the raw text — its key ORDER, precisely: `one_line` collapses
/// every whitespace run to one space, so nothing about the spacing survives either way —
/// and no such text exists for a value nested inside the body. Matching it here would
/// mean carrying the source slice down to every candidate, for a difference that
/// REORDERS a validation error rather than losing any of it. Worth knowing when holding
/// the output next to `curl`; not worth the plumbing.
///
/// Enabling `serde_json/preserve_order` would close it for nothing, and it is deliberately
/// off: `output::print_json` serialises the same `Value`, so the same feature reorders
/// every `--json` document this CLI prints, which is a wider promise to move than an
/// error message is worth. A lockfile doesn't prevent it either — features unify across
/// the whole dependency graph, and it pins versions rather than the features they are
/// built with — so if it ever arrives, three things want revisiting: this paragraph, the
/// reason the catch-all arm of [`failure_detail`] gives for keeping the body verbatim,
/// and the pair of assertions in
/// `a_response_that_explains_nothing_does_not_promise_that_it_will`. The nested one flips,
/// along with the sentence above it saying the keys come back sorted — though not the
/// recovery note beneath that, which is the one thing there that becomes live rather than
/// stale. The other assertion keeps passing with its own comment true word for word; what
/// it loses is the thing it was for, since with both orders preserved it no longer
/// contrasts with anything, so the question there is whether it still earns its place.
fn explains(value: &Value) -> Option<String> {
    if says_nothing(value) {
        return None;
    }

    match value {
        Value::String(said) => Some(said.to_string()),
        Value::Object(_) | Value::Array(_) => Some(value.to_string()),
        _ => None,
    }
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
            bail!(
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

    #[test]
    fn a_key_that_says_nothing_does_not_win_over_one_that_does() {
        // Present-but-empty is the server declining to explain, so the next candidate —
        // and failing that the body — is what a reader needs. Rendering the literal
        // `null` beside a perfectly good `error` was the shape this replaced.
        assert_eq!(
            failure_detail(r#"{"message":null,"error":"branch is not active"}"#),
            "branch is not active"
        );
        assert_eq!(
            failure_detail(r#"{"message":"  ","error":"branch is not active"}"#),
            "branch is not active"
        );
        // Nothing useful anywhere: the body beats a message rendering as empty, which
        // would print `failed with 400: ` and stop.
        assert_eq!(
            failure_detail(r#"{"message":"","statusCode":400}"#),
            r#"{"message":"","statusCode":400}"#
        );
        // The same, one type wider: an empty container says as little as a blank string.
        assert_eq!(
            failure_detail(r#"{"message":{},"error":"branch is not active"}"#),
            "branch is not active"
        );
        assert_eq!(
            failure_detail(r#"{"message":[],"error":"branch is not active"}"#),
            "branch is not active"
        );
        // A bare number is a CODE, not an explanation: the status line already carries
        // it, so it must not beat the sentence one candidate along.
        assert_eq!(
            failure_detail(r#"{"message":400,"error":"branch is not active"}"#),
            "branch is not active"
        );
        // With nothing better anywhere, the body shows that number in context, which
        // beats rendering it alone as though it were the reason.
        assert_eq!(failure_detail(r#"{"message":400}"#), r#"{"message":400}"#);
        // A structured message is an explanation — that is where validation errors live.
        assert_eq!(
            failure_detail(r#"{"message":["ref is unknown"],"error":"x"}"#),
            r#"["ref is unknown"]"#
        );
    }

    #[test]
    fn a_response_that_explains_nothing_does_not_promise_that_it_will() {
        // The likeliest empty answer of all: a gateway that returns the status and no
        // body. `Display` must not end at `failed with 502 Bad Gateway: `.
        assert_eq!(failure_detail(""), "");
        assert_eq!(failure_detail("{}"), "");
        assert_eq!(failure_detail("[]"), "");
        assert_eq!(failure_detail("null"), "");
        assert_eq!(failure_detail(r#""  ""#), "");

        // But a poor reason is still a reason when it is the only one. Rejecting a bare
        // scalar here would print less than either neighbour does: wrapped in an object
        // it renders, and unparseable it renders.
        assert_eq!(failure_detail("400"), "400");
        assert_eq!(failure_detail("Bad Gateway"), "Bad Gateway");
        assert_eq!(failure_detail(r#"{"code":400}"#), r#"{"code":400}"#);

        // Wrapped or not, one rendering: a quoted body is text that arrived quoted.
        assert_eq!(failure_detail(r#""Bad gateway""#), "Bad gateway");
        assert_eq!(
            failure_detail(r#"{"message":"Bad gateway"}"#),
            "Bad gateway"
        );
        // An object keeps the order the server sent, because the fallback hands back the
        // raw text rather than re-rendering it. See `explains`' "On key order".
        assert_eq!(
            failure_detail(r#"{"z":1,"a":2}"#),
            r#"{"z":1,"a":2}"#,
            "keys in the order they arrived"
        );
        // A NESTED object can't: there is no raw slice for it, so it is re-rendered and
        // the keys come back sorted. Asserted so the asymmetry with the line above is a
        // documented fact rather than something a reader meets in a log — the content is
        // all there, only the order is the crate's rather than the server's.
        //
        // If this ever fails with the keys IN ORDER, nothing regressed: something in the
        // graph turned on `serde_json/preserve_order`. Swap the expectation, then work
        // through `explains`' "On key order", which lists what else assumed otherwise —
        // including one claim this failure can't surface, because it keeps passing.
        assert_eq!(
            failure_detail(r#"{"error":{"z":1,"a":2}}"#),
            r#"{"a":2,"z":1}"#
        );

        let err = ApiError {
            status: StatusCode::BAD_GATEWAY,
            method: Method::POST,
            path: "/api/v1/deployments/42/dbt-sync".to_string(),
            detail: String::new(),
        };
        assert_eq!(
            err.to_string(),
            "POST /api/v1/deployments/42/dbt-sync failed with 502 Bad Gateway"
        );

        // And every other arm's punctuation goes with it.
        for (status, expected) in [
            (StatusCode::FORBIDDEN, "forbidden (403)"),
            (
                StatusCode::NOT_FOUND,
                "not found (404): POST /api/v1/deployments/42/dbt-sync",
            ),
        ] {
            let err = ApiError {
                status,
                method: Method::POST,
                path: "/api/v1/deployments/42/dbt-sync".to_string(),
                detail: "   ".to_string(),
            };
            assert_eq!(err.to_string(), expected, "{status}");
        }

        // A detail that does say something keeps its separator.
        let err = ApiError {
            status: StatusCode::FORBIDDEN,
            method: Method::GET,
            path: "/x".to_string(),
            detail: "not your deployment".to_string(),
        };
        assert_eq!(err.to_string(), "forbidden (403): not your deployment");
    }
}
