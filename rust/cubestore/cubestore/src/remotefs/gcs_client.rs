//! GCS JSON API access with cached, refreshable ADC credentials.
//!
//! Object names and listing prefixes intentionally remain strings: `object_store::Path`
//! normalizes leading slashes and uses segment prefixes, whereas existing CubeStore
//! buckets contain raw GCS names (including `/metastore-*` with no configured subpath).
use crate::CubeError;
use chrono::{DateTime, Utc};
use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder};
use reqwest::{Client, Method, Response, StatusCode, Url};
use serde::Deserialize;
use std::fmt;
use std::sync::Once;
use std::time::Duration;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Credentials {
    KeyFile(String),
    Json(String),
    ApplicationFile(String),
    Default,
}

/// Credential files, most specific first. The flag marks names that predate
/// `CUBESTORE_GCP_KEY_FILE` and are kept only for compatibility.
const CREDENTIAL_FILES: [(&str, bool); 5] = [
    ("CUBESTORE_GCP_KEY_FILE", false),
    ("CUBESTORE_GCP_SERVICE_ACCOUNT", true),
    ("SERVICE_ACCOUNT", true),
    ("CUBESTORE_GCP_GOOGLE_APPLICATION_CREDENTIALS", true),
    // The standard ADC variable; not deprecated.
    ("GOOGLE_APPLICATION_CREDENTIALS", false),
];

/// Inline service-account JSON aliases that predate `CUBESTORE_GCP_CREDENTIALS`.
const LEGACY_CREDENTIAL_JSON: [&str; 4] = [
    "CUBESTORE_GCP_SERVICE_ACCOUNT_JSON",
    "SERVICE_ACCOUNT_JSON",
    "CUBESTORE_GCP_GOOGLE_APPLICATION_CREDENTIALS_JSON",
    "GOOGLE_APPLICATION_CREDENTIALS_JSON",
];

static LEGACY_NAME_WARNING: Once = Once::new();

fn warn_legacy_name(name: &str, replacement: &str) {
    // Clients are created per bucket, so say it once per process.
    LEGACY_NAME_WARNING.call_once(|| {
        log::warn!(
            "Environment variable '{}' is deprecated and will be ignored in future versions, use '{}' instead",
            name,
            replacement
        );
    });
}

/// Picks the configured credential source, or ADC when nothing is configured.
///
/// The order is the one the previous client resolved: any credential file wins over
/// inline JSON, and within each group `CUBESTORE_GCP_KEY_FILE` / `CUBESTORE_GCP_CREDENTIALS`
/// win over the legacy aliases. Deployments that set several of them keep the identity
/// they had. Empty values are treated as unset.
fn credentials_from(mut env: impl FnMut(&str) -> Option<String>) -> Result<Credentials, CubeError> {
    let mut value = |name: &str| env(name).filter(|v| !v.is_empty());
    for (name, legacy) in CREDENTIAL_FILES {
        if let Some(path) = value(name) {
            if legacy {
                warn_legacy_name(name, "CUBESTORE_GCP_KEY_FILE");
            }
            return Ok(if name.ends_with("GOOGLE_APPLICATION_CREDENTIALS") {
                // ADC files may hold a service account or an authorized user.
                Credentials::ApplicationFile(path)
            } else {
                Credentials::KeyFile(path)
            });
        }
    }
    let mut undecodable = None;
    if let Some(encoded) = value("CUBESTORE_GCP_CREDENTIALS") {
        match base64::decode(encoded)
            .map_err(CubeError::from)
            .and_then(|json| Ok(String::from_utf8(json)?))
        {
            Ok(json) => return Ok(Credentials::Json(json)),
            Err(e) => {
                log::error!("Could not decode 'CUBESTORE_GCP_CREDENTIALS': {}", e);
                undecodable = Some(e);
            }
        }
    }
    for name in LEGACY_CREDENTIAL_JSON {
        if let Some(json) = value(name) {
            warn_legacy_name(name, "CUBESTORE_GCP_CREDENTIALS");
            return Ok(Credentials::Json(json));
        }
    }
    // A key that was configured but cannot be read must not silently become ADC,
    // which could be a different identity.
    match undecodable {
        Some(e) => Err(e),
        None => Ok(Credentials::Default),
    }
}

#[derive(Clone)]
pub(crate) struct GcsClient {
    http: Client,
    credentials: GcpCredentialProvider,
    base_url: Url,
    bucket: String,
}

// Not derived: the credential provider's `Debug` output includes the cached bearer token
// and, for service accounts, the private key. `RemoteFs` requires `Debug`.
impl fmt::Debug for GcsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcsClient")
            .field("bucket", &self.bucket)
            .field("base_url", &self.base_url.as_str())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct GcsObject {
    pub name: String,
    pub updated: DateTime<Utc>,
    size: String,
}

impl GcsObject {
    pub fn size(&self) -> Result<u64, CubeError> {
        self.size
            .parse()
            .map_err(|e| CubeError::internal(format!("Invalid GCS object size: {}", e)))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GcsPage {
    #[serde(default)]
    pub items: Vec<GcsObject>,
    pub next_page_token: Option<String>,
}

impl GcsClient {
    pub fn new(bucket: &str) -> Result<Self, CubeError> {
        let builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
        let builder = match credentials_from(|name| std::env::var(name).ok())? {
            Credentials::KeyFile(path) => builder.with_service_account_path(path),
            Credentials::Json(json) => builder.with_service_account_key(json),
            Credentials::ApplicationFile(path) => builder.with_application_credentials(path),
            Credentials::Default => builder,
        };
        let store = builder
            .build()
            .map_err(|e| CubeError::internal(format!("GCS credentials: {}", e)))?;
        Ok(Self {
            http: Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30 * 60))
                // Never forward a credential to a redirect target.
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            credentials: store.credentials().clone(),
            base_url: Url::parse("https://storage.googleapis.com").unwrap(),
            bucket: bucket.to_owned(),
        })
    }

    fn url(&self, upload: bool, object: Option<&str>) -> Url {
        let mut url = self.base_url.clone();
        {
            let mut path = url.path_segments_mut().unwrap();
            path.clear();
            if upload {
                path.push("upload");
            }
            path.extend(["storage", "v1", "b", &self.bucket, "o"]);
            if let Some(object) = object {
                path.push(object);
            }
        }
        url
    }

    async fn request(
        &self,
        method: Method,
        url: Url,
    ) -> Result<reqwest::RequestBuilder, CubeError> {
        let credential = self
            .credentials
            .get_credential()
            .await
            .map_err(|e| CubeError::internal(format!("GCS access token: {}", e)))?;
        Ok(self
            .http
            .request(method, url)
            .bearer_auth(&credential.bearer))
    }

    /// Turns a non-2xx answer into an error that keeps what GCS said. The status alone
    /// does not tell a missing permission from a missing bucket or a bad request.
    async fn ensure_success(
        &self,
        response: Response,
        action: &str,
        target: &str,
    ) -> Result<Response, CubeError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        Err(CubeError::internal(format!(
            "GCS {} failed for gs://{}/{}: {}{}",
            action,
            self.bucket,
            target,
            status,
            gcs_error_detail(&body)
        )))
    }

    pub async fn upload(&self, object: &str, file: File, size: u64) -> Result<(), CubeError> {
        let mut url = self.url(true, None);
        url.query_pairs_mut()
            .append_pair("uploadType", "media")
            .append_pair("name", object);
        let response = self
            .request(Method::POST, url)
            .await?
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .header(reqwest::header::CONTENT_LENGTH, size)
            .body(reqwest::Body::wrap_stream(ReaderStream::new(file)))
            .send()
            .await?;
        self.ensure_success(response, "upload", object).await?;
        Ok(())
    }

    pub async fn download(&self, object: &str) -> Result<Response, CubeError> {
        let mut url = self.url(false, Some(object));
        url.query_pairs_mut().append_pair("alt", "media");
        let response = self.request(Method::GET, url).await?.send().await?;
        self.ensure_success(response, "download", object).await
    }

    pub async fn metadata(&self, object: &str) -> Result<GcsObject, CubeError> {
        let response = self
            .request(Method::GET, self.url(false, Some(object)))
            .await?
            .send()
            .await?;
        Ok(self
            .ensure_success(response, "metadata lookup", object)
            .await?
            .json()
            .await?)
    }

    /// Deletes an object. Returns `false` when it was already gone: GCS answers 404 where
    /// S3 answers 204, absent is the state the caller asked for, and none of them can act
    /// on the difference. Every other failure, a missing permission included, is an error.
    pub async fn delete(&self, object: &str) -> Result<bool, CubeError> {
        let response = self
            .request(Method::DELETE, self.url(false, Some(object)))
            .await?
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        self.ensure_success(response, "delete", object).await?;
        Ok(true)
    }

    pub async fn list_page(
        &self,
        prefix: &str,
        page_token: Option<&str>,
    ) -> Result<GcsPage, CubeError> {
        let mut url = self.url(false, None);
        url.query_pairs_mut()
            .append_pair("prefix", prefix)
            .append_pair("maxResults", "1000");
        if let Some(token) = page_token {
            url.query_pairs_mut().append_pair("pageToken", token);
        }
        let response = self.request(Method::GET, url).await?.send().await?;
        Ok(self
            .ensure_success(response, "list", prefix)
            .await?
            .json()
            .await?)
    }
}

/// The `error.message` of a GCS JSON API error, or the start of the body when the answer
/// is not in that shape (media downloads answer in plain text).
fn gcs_error_detail(body: &str) -> String {
    const MAX_DETAIL_CHARS: usize = 512;
    let message = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v["error"]["message"].as_str().map(str::to_owned))
        .unwrap_or_else(|| body.trim().to_owned());
    if message.is_empty() {
        return String::new();
    }
    let mut detail: String = message.chars().take(MAX_DETAIL_CHARS).collect();
    if message.chars().count() > MAX_DETAIL_CHARS {
        detail.push_str("...");
    }
    format!(": {}", detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::gcp::GcpCredential;
    use object_store::StaticCredentialProvider;
    use std::sync::Arc;
    use warp::Filter;

    fn credentials(values: &[(&str, &str)]) -> Result<Credentials, CubeError> {
        credentials_from(|key| {
            values
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        })
    }

    #[test]
    fn credential_selection_ignores_empty_values_and_preserves_aliases() {
        assert_eq!(credentials(&[]).unwrap(), Credentials::Default);
        assert_eq!(
            credentials(&[
                ("CUBESTORE_GCP_KEY_FILE", ""),
                ("CUBESTORE_GCP_CREDENTIALS", "e30=")
            ])
            .unwrap(),
            Credentials::Json("{}".into())
        );
        assert_eq!(
            credentials(&[
                ("CUBESTORE_GCP_KEY_FILE", "key.json"),
                ("CUBESTORE_GCP_CREDENTIALS", "invalid")
            ])
            .unwrap(),
            Credentials::KeyFile("key.json".into())
        );
        for name in ["CUBESTORE_GCP_SERVICE_ACCOUNT", "SERVICE_ACCOUNT"] {
            assert_eq!(
                credentials(&[(name, "key.json")]).unwrap(),
                Credentials::KeyFile("key.json".into())
            );
        }
        for name in [
            "SERVICE_ACCOUNT_JSON",
            "GOOGLE_APPLICATION_CREDENTIALS_JSON",
            "CUBESTORE_GCP_SERVICE_ACCOUNT_JSON",
            "CUBESTORE_GCP_GOOGLE_APPLICATION_CREDENTIALS_JSON",
        ] {
            assert_eq!(
                credentials(&[(name, "{}")]).unwrap(),
                Credentials::Json("{}".into())
            );
        }
        assert_eq!(
            credentials(&[("GOOGLE_APPLICATION_CREDENTIALS", "adc.json")]).unwrap(),
            Credentials::ApplicationFile("adc.json".into())
        );
        assert!(credentials(&[("CUBESTORE_GCP_CREDENTIALS", "invalid")]).is_err());
    }

    /// The previous client read a credential file before any inline JSON, whichever
    /// variables they came from. A deployment that sets both keeps its identity.
    #[test]
    fn credential_files_win_over_inline_json_in_the_previous_order() {
        const FILES: [&str; 5] = [
            "CUBESTORE_GCP_KEY_FILE",
            "CUBESTORE_GCP_SERVICE_ACCOUNT",
            "SERVICE_ACCOUNT",
            "CUBESTORE_GCP_GOOGLE_APPLICATION_CREDENTIALS",
            "GOOGLE_APPLICATION_CREDENTIALS",
        ];
        const INLINE: [&str; 5] = [
            "CUBESTORE_GCP_CREDENTIALS",
            "CUBESTORE_GCP_SERVICE_ACCOUNT_JSON",
            "SERVICE_ACCOUNT_JSON",
            "CUBESTORE_GCP_GOOGLE_APPLICATION_CREDENTIALS_JSON",
            "GOOGLE_APPLICATION_CREDENTIALS_JSON",
        ];
        // Every inline source carries JSON naming its own variable, so the selected
        // value shows which one won. CUBESTORE_GCP_CREDENTIALS is the base64 form of
        // {"from":"CUBESTORE_GCP_CREDENTIALS"}.
        let inline_value = |name: &str| {
            if name == "CUBESTORE_GCP_CREDENTIALS" {
                "eyJmcm9tIjoiQ1VCRVNUT1JFX0dDUF9DUkVERU5USUFMUyJ9".to_string()
            } else {
                format!("{{\"from\":\"{}\"}}", name)
            }
        };
        let expected_file = |name: &str| {
            if name.ends_with("GOOGLE_APPLICATION_CREDENTIALS") {
                Credentials::ApplicationFile(name.to_string())
            } else {
                Credentials::KeyFile(name.to_string())
            }
        };
        // Each file beats every later file and every inline source.
        for (i, winner) in FILES.iter().enumerate() {
            let mut values: Vec<(&str, String)> =
                FILES[i..].iter().map(|n| (*n, n.to_string())).collect();
            values.extend(INLINE.iter().map(|n| (*n, inline_value(n))));
            let selected = credentials_from(|key| {
                values
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.clone())
            })
            .unwrap();
            assert_eq!(selected, expected_file(winner), "winner {}", winner);
        }
        // Without files, each inline source beats every later inline source.
        for (i, winner) in INLINE.iter().enumerate() {
            let values: Vec<(&str, String)> =
                INLINE[i..].iter().map(|n| (*n, inline_value(n))).collect();
            let selected = credentials_from(|key| {
                values
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.clone())
            })
            .unwrap();
            assert_eq!(
                selected,
                Credentials::Json(format!("{{\"from\":\"{}\"}}", winner)),
                "winner {}",
                winner
            );
        }
    }

    #[test]
    fn undecodable_credentials_fall_back_to_legacy_json_but_never_to_adc() {
        assert_eq!(
            credentials(&[
                ("CUBESTORE_GCP_CREDENTIALS", "invalid"),
                ("SERVICE_ACCOUNT_JSON", "{}")
            ])
            .unwrap(),
            Credentials::Json("{}".into())
        );
        assert!(credentials(&[("CUBESTORE_GCP_CREDENTIALS", "invalid")]).is_err());
    }

    #[test]
    fn debug_output_does_not_expose_credentials() {
        let client = client(Url::parse("http://localhost").unwrap());
        let debug = format!("{:?}", client);
        assert!(debug.contains("bucket"), "{}", debug);
        assert!(!debug.contains("test-token"), "{}", debug);
    }

    #[test]
    fn error_detail_prefers_the_gcs_message_and_is_bounded() {
        assert_eq!(
            gcs_error_detail(
                r#"{"error":{"code":403,"message":"caller does not have storage.objects.get access","errors":[]}}"#
            ),
            ": caller does not have storage.objects.get access"
        );
        assert_eq!(
            gcs_error_detail("No such object: b/o"),
            ": No such object: b/o"
        );
        assert_eq!(gcs_error_detail(""), "");
        let long = gcs_error_detail(&"x".repeat(2000));
        assert_eq!(long.chars().count(), 2 + 512 + 3);
    }

    fn client(base_url: Url) -> GcsClient {
        GcsClient {
            http: Client::new(),
            credentials: Arc::new(StaticCredentialProvider::new(GcpCredential {
                bearer: "test-token".into(),
            })),
            base_url,
            bucket: "bucket".into(),
        }
    }

    #[test]
    fn object_urls_preserve_exact_gcs_names() {
        let client = client(Url::parse("http://localhost").unwrap());
        let url = client.url(false, Some("/tenant a/100%?#.parquet"));
        assert_eq!(
            url.path(),
            "/storage/v1/b/bucket/o/%2Ftenant%20a%2F100%25%3F%23.parquet"
        );
        assert!(url.query().is_none());
        assert!(url.fragment().is_none());
    }

    #[tokio::test]
    async fn list_uses_raw_prefixes_and_page_tokens_and_propagates_errors() {
        let route = warp::header::exact("authorization", "Bearer test-token")
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .map(|q: std::collections::HashMap<String, String>| {
                let prefix = q.get("prefix").unwrap();
                let (status, body) = if prefix == "/denied" {
                    (StatusCode::FORBIDDEN, serde_json::json!({"error": "denied"}))
                } else if q.get("pageToken").map(String::as_str) == Some("second+/=") {
                    (StatusCode::OK, serde_json::json!({"items": [{"name": format!("{}2", prefix), "size": "2", "updated": "2026-01-01T00:00:00Z"}]}))
                } else {
                    (StatusCode::OK, serde_json::json!({"items": [{"name": format!("{}1", prefix), "size": "1", "updated": "2026-01-01T00:00:00Z"}], "nextPageToken": "second+/="}))
                };
                warp::reply::with_status(warp::reply::json(&body), warp::http::StatusCode::from_u16(status.as_u16()).unwrap())
            });
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        let server = tokio::spawn(server);
        let client = client(Url::parse(&format!("http://{}", addr)).unwrap());
        for prefix in [
            "/metastore-",
            "/metastore-123-logs",
            "tenant.a/metastore-",
            "tenant.a/",
        ] {
            let first = client.list_page(prefix, None).await.unwrap();
            assert_eq!(first.items[0].name, format!("{}1", prefix));
            let second = client
                .list_page(prefix, first.next_page_token.as_deref())
                .await
                .unwrap();
            assert_eq!(second.items[0].size().unwrap(), 2);
            assert!(second.next_page_token.is_none());
        }
        let denied = client.list_page("/denied", None).await.unwrap_err();
        assert!(
            denied.message.contains("403") && denied.message.contains("denied"),
            "{}",
            denied.message
        );
        server.abort();
    }

    #[tokio::test]
    async fn delete_reports_absent_objects_and_fails_on_everything_else() {
        const FORBIDDEN: &str = r#"{"error":{"code":403,"message":"sa@p.iam.gserviceaccount.com does not have storage.objects.delete access to the Google Cloud Storage object."}}"#;
        for (status, body, expected) in [
            (204, "", Some(true)),
            (
                404,
                r#"{"error":{"code":404,"message":"No such object: bucket//object"}}"#,
                Some(false),
            ),
            (403, FORBIDDEN, None),
            (500, "backend error", None),
        ] {
            let route = warp::delete()
                .and(warp::header::exact("authorization", "Bearer test-token"))
                .map(move || {
                    warp::reply::with_status(
                        body,
                        warp::http::StatusCode::from_u16(status).unwrap(),
                    )
                });
            let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
            let server = tokio::spawn(server);
            let client = client(Url::parse(&format!("http://{}", addr)).unwrap());
            let result = client.delete("/object").await;
            server.abort();
            match expected {
                Some(deleted) => assert_eq!(result.unwrap(), deleted, "DELETE answered {status}"),
                None => {
                    let message = result.unwrap_err().message;
                    assert!(
                        message.contains("gs://bucket//object")
                            && message.contains(&status.to_string()),
                        "{}",
                        message
                    );
                    if status == 403 {
                        assert!(message.contains("storage.objects.delete"), "{}", message);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn streams_uploads_and_downloads_and_handles_not_found() {
        use tokio::io::AsyncWriteExt;
        let route = warp::header::exact("authorization", "Bearer test-token")
            .and(warp::method())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and(warp::body::bytes())
            .map(|method: warp::http::Method, query: std::collections::HashMap<String, String>, body: bytes::Bytes| {
                let (status, body) = if method == warp::http::Method::POST {
                    assert_eq!(query.get("name").unwrap(), "/tenant/a%?#");
                    assert_eq!(query.get("uploadType").unwrap(), "media");
                    assert_eq!(body.len(), 128 * 1024);
                    assert!(body.iter().all(|b| *b == 42));
                    (warp::http::StatusCode::OK, Vec::new())
                } else if method == warp::http::Method::DELETE {
                    (warp::http::StatusCode::NOT_FOUND, Vec::new())
                } else if query.get("alt").map(String::as_str) == Some("media") {
                    (warp::http::StatusCode::OK, vec![42; 128 * 1024])
                } else {
                    (warp::http::StatusCode::OK, br#"{"name":"/tenant/a%?#","size":"131072","updated":"2026-01-01T00:00:00Z"}"#.to_vec())
                };
                warp::http::Response::builder().status(status).body(body).unwrap()
            });
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        let server = tokio::spawn(server);
        let client = client(Url::parse(&format!("http://{}", addr)).unwrap());
        let mut file = File::from_std(tempfile::tempfile().unwrap());
        file.write_all(&vec![42; 128 * 1024]).await.unwrap();
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        client
            .upload("/tenant/a%?#", file, 128 * 1024)
            .await
            .unwrap();
        assert_eq!(
            client
                .download("/tenant/a%?#")
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            &vec![42; 128 * 1024]
        );
        assert_eq!(
            client
                .metadata("/tenant/a%?#")
                .await
                .unwrap()
                .size()
                .unwrap(),
            128 * 1024
        );
        assert!(!client.delete("/already-gone").await.unwrap());
        server.abort();
    }
}
