//! Read access to GCS objects through the JSON API with cached, refreshable ADC credentials.
//!
//! Used to import `gs://` locations with Cube Store's own Google identity. Object names
//! intentionally remain strings: `object_store::Path` normalizes leading slashes, whereas
//! GCS object names are arbitrary and have to be requested exactly as they were exported.
use crate::CubeError;
use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder};
use reqwest::{Client, Method, Response, Url};
use serde::Deserialize;
use std::fmt;
use std::sync::Once;
use std::time::Duration;

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
// and, for service accounts, the private key.
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
    size: String,
}

impl GcsObject {
    pub fn size(&self) -> Result<u64, CubeError> {
        self.size
            .parse()
            .map_err(|e| CubeError::internal(format!("Invalid GCS object size: {}", e)))
    }
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

    fn url(&self, object: &str) -> Url {
        let mut url = self.base_url.clone();
        url.path_segments_mut().unwrap().clear().extend([
            "storage",
            "v1",
            "b",
            &self.bucket,
            "o",
            object,
        ]);
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

    pub async fn download(&self, object: &str) -> Result<Response, CubeError> {
        let mut url = self.url(object);
        url.query_pairs_mut().append_pair("alt", "media");
        let response = self.request(Method::GET, url).await?.send().await?;
        self.ensure_success(response, "download", object).await
    }

    pub async fn metadata(&self, object: &str) -> Result<GcsObject, CubeError> {
        let response = self
            .request(Method::GET, self.url(object))
            .await?
            .send()
            .await?;
        Ok(self
            .ensure_success(response, "metadata lookup", object)
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
        let url = client.url("/tenant a/100%?#.parquet");
        assert_eq!(
            url.path(),
            "/storage/v1/b/bucket/o/%2Ftenant%20a%2F100%25%3F%23.parquet"
        );
        assert!(url.query().is_none());
        assert!(url.fragment().is_none());
    }

    #[tokio::test]
    async fn downloads_exact_objects_and_tells_missing_from_denied() {
        const OBJECT: &str = "/export a/part-000000000000%?#.parquet";
        const FORBIDDEN: &str = r#"{"error":{"code":403,"message":"sa@p.iam.gserviceaccount.com does not have storage.objects.get access to the Google Cloud Storage object."}}"#;
        let route = warp::get()
            .and(warp::header::exact("authorization", "Bearer test-token"))
            .and(warp::path::full())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .map(
                |path: warp::path::FullPath, query: std::collections::HashMap<String, String>| {
                    let media = query.get("alt").map(String::as_str) == Some("media");
                    let (status, body) = if path.as_str().ends_with("denied.parquet") {
                        (403, FORBIDDEN.as_bytes().to_vec())
                    } else if path.as_str().ends_with("missing.parquet") {
                        (404, b"No such object: bucket/missing.parquet".to_vec())
                    } else if media {
                        (200, vec![42; 128 * 1024])
                    } else {
                        (200, br#"{"name":"ignored","size":"131072"}"#.to_vec())
                    };
                    warp::http::Response::builder()
                        .status(status)
                        .body(body)
                        .unwrap()
                },
            );
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        let server = tokio::spawn(server);
        let client = client(Url::parse(&format!("http://{}", addr)).unwrap());

        let body = client
            .download(OBJECT)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(body.as_ref(), &vec![42; 128 * 1024]);
        assert_eq!(
            client.metadata(OBJECT).await.unwrap().size().unwrap(),
            128 * 1024
        );

        for (object, status, detail) in [
            ("missing.parquet", "404", "No such object"),
            ("denied.parquet", "403", "storage.objects.get"),
        ] {
            for message in [
                client.download(object).await.unwrap_err().message,
                client.metadata(object).await.unwrap_err().message,
            ] {
                assert!(
                    message.contains(&format!("gs://bucket/{}", object))
                        && message.contains(status)
                        && message.contains(detail),
                    "{}",
                    message
                );
            }
        }
        server.abort();
    }
}
