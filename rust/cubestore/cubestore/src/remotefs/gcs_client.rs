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

fn credentials_from(mut env: impl FnMut(&str) -> Option<String>) -> Result<Credentials, CubeError> {
    let mut value = |name: &str| env(name).filter(|v| !v.is_empty());
    if let Some(path) = value("CUBESTORE_GCP_KEY_FILE") {
        return Ok(Credentials::KeyFile(path));
    }
    if let Some(encoded) = value("CUBESTORE_GCP_CREDENTIALS") {
        return Ok(Credentials::Json(String::from_utf8(base64::decode(
            encoded,
        )?)?));
    }
    for name in ["CUBESTORE_GCP_SERVICE_ACCOUNT", "SERVICE_ACCOUNT"] {
        if let Some(path) = value(name) {
            return Ok(Credentials::KeyFile(path));
        }
    }
    for name in [
        "CUBESTORE_GCP_SERVICE_ACCOUNT_JSON",
        "SERVICE_ACCOUNT_JSON",
        "CUBESTORE_GCP_GOOGLE_APPLICATION_CREDENTIALS_JSON",
        "GOOGLE_APPLICATION_CREDENTIALS_JSON",
    ] {
        if let Some(json) = value(name) {
            return Ok(Credentials::Json(json));
        }
    }
    for name in [
        "CUBESTORE_GCP_GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ] {
        if let Some(path) = value(name) {
            return Ok(Credentials::ApplicationFile(path));
        }
    }
    Ok(Credentials::Default)
}

#[derive(Debug, Clone)]
pub(crate) struct GcsClient {
    http: Client,
    credentials: GcpCredentialProvider,
    base_url: Url,
    bucket: String,
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

    pub async fn upload(&self, object: &str, file: File, size: u64) -> Result<(), CubeError> {
        let mut url = self.url(true, None);
        url.query_pairs_mut()
            .append_pair("uploadType", "media")
            .append_pair("name", object);
        self.request(Method::POST, url)
            .await?
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .header(reqwest::header::CONTENT_LENGTH, size)
            .body(reqwest::Body::wrap_stream(ReaderStream::new(file)))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn download(&self, object: &str) -> Result<Response, CubeError> {
        let mut url = self.url(false, Some(object));
        url.query_pairs_mut().append_pair("alt", "media");
        Ok(self
            .request(Method::GET, url)
            .await?
            .send()
            .await?
            .error_for_status()?)
    }

    pub async fn metadata(&self, object: &str) -> Result<GcsObject, CubeError> {
        Ok(self
            .request(Method::GET, self.url(false, Some(object)))
            .await?
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn delete(&self, object: &str) -> Result<(), CubeError> {
        let response = self
            .request(Method::DELETE, self.url(false, Some(object)))
            .await?
            .send()
            .await?;
        if response.status() != StatusCode::NOT_FOUND {
            response.error_for_status()?;
        }
        Ok(())
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
        Ok(self
            .request(Method::GET, url)
            .await?
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
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
        assert!(client.list_page("/denied", None).await.is_err());
        server.abort();
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
        client.delete("/already-gone").await.unwrap();
        server.abort();
    }
}
