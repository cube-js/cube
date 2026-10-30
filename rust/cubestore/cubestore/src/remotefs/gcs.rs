use crate::app_metrics;
use crate::di_service;
use crate::remotefs::ExtendedRemoteFs;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::cube_ext;
use google_cloud_auth::credentials::{
    self, external_account, impersonated, service_account, user_account, CacheableResource,
    Credentials,
};
use google_cloud_storage::client::Storage;
use log::{debug, info};
use regex::{NoExpand, Regex};
use reqwest::header::HeaderMap;
use reqwest::{RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use url::Url;

/// List and delete use the JSON API through `reqwest`, which honors `HTTPS_PROXY`. The SDK only
/// offers them over gRPC, which does not.
const JSON_API_URL: &str = "https://storage.googleapis.com/storage/v1";
const JSON_API_TIMEOUT: Duration = Duration::from_secs(60);
const JSON_API_ATTEMPTS: u32 = 4;

/// Finds the service account key in the environment. Returns the key JSON and the variable it was
/// read from, or `None` when no variable is set.
fn service_account_key(
    env: impl Fn(&str) -> Option<String>,
) -> Result<Option<(String, String)>, CubeError> {
    let mut creds_json = None;
    let mut decode_error = None;
    if let Some(c) = env("CUBESTORE_GCP_CREDENTIALS") {
        match decode_credentials(&c) {
            Ok(s) => creds_json = Some((s, "CUBESTORE_GCP_CREDENTIALS".to_string())),
            Err(e) => {
                log::error!("Could not decode 'CUBESTORE_GCP_CREDENTIALS': {}", e);
                decode_error = Some(e);
            }
        }
    }
    let mut creds_path =
        env("CUBESTORE_GCP_KEY_FILE").map(|s| (s, "CUBESTORE_GCP_KEY_FILE".to_string()));

    // TODO: this handles deprecated variable names, remove them.
    for (var, is_path) in &[
        ("SERVICE_ACCOUNT", true),
        ("GOOGLE_APPLICATION_CREDENTIALS", true),
        ("SERVICE_ACCOUNT_JSON", false),
        ("GOOGLE_APPLICATION_CREDENTIALS_JSON", false),
    ] {
        for var in &[&format!("CUBESTORE_GCP_{}", var), *var] {
            if let Some(var_value) = env(var) {
                let (upgrade_var, read_value) = if *is_path {
                    ("CUBESTORE_GCP_KEY_FILE", &mut creds_path)
                } else {
                    ("CUBESTORE_GCP_CREDENTIALS", &mut creds_json)
                };

                match read_value {
                    None => {
                        *read_value = Some((var_value, var.to_string()));
                        log::warn!(
                            "Environment variable '{}' is deprecated and will be ignored in future versions, use '{}' instead",
                            var,
                            upgrade_var
                        );
                    }
                    Some((prev_val, prev_var)) => {
                        if prev_val != &var_value {
                            log::warn!(
                                "Values of '{}' and '{}' differ, preferring the latter",
                                var,
                                prev_var
                            );
                        }
                    }
                }
            }
        }
    }

    // A key file wins over inline JSON, as it did with the `cloud-storage` crate.
    match creds_path {
        Some((path, var)) => {
            let json = std::fs::read_to_string(&path).map_err(|e| {
                CubeError::user(format!(
                    "Could not read GCP key file '{}' from '{}': {}",
                    path, var, e
                ))
            })?;
            Ok(Some((json, var)))
        }
        // Falling back to ambient credentials here would start Cube Store as another identity.
        None => match (creds_json, decode_error) {
            (None, Some(e)) => Err(CubeError::user(format!(
                "Could not decode 'CUBESTORE_GCP_CREDENTIALS': {}",
                e
            ))),
            (creds_json, _) => Ok(creds_json),
        },
    }
}

/// The credentials from the environment, or Application Default Credentials (the metadata server
/// on GKE and GCE) when none is set.
fn gcp_credentials() -> Result<Credentials, CubeError> {
    match service_account_key(|name| std::env::var(name).ok())? {
        Some((json, var)) => {
            info!("Using GCP credentials from '{}'", var);
            let json: serde_json::Value = serde_json::from_str(&json).map_err(|e| {
                CubeError::user(format!(
                    "Could not parse GCP credentials from '{}': {}",
                    var, e
                ))
            })?;
            credentials_from_json(json).map_err(|e| {
                CubeError::user(format!("Invalid GCP credentials in '{}': {}", var, e))
            })
        }
        None => {
            info!("Using GCP Application Default Credentials");
            credentials::Builder::default().build().map_err(|e| {
                CubeError::user(format!(
                    "Could not load GCP Application Default Credentials, set CUBESTORE_GCP_CREDENTIALS or CUBESTORE_GCP_KEY_FILE instead: {}",
                    e
                ))
            })
        }
    }
}

/// The credential types a `GOOGLE_APPLICATION_CREDENTIALS` file can hold, as ADC reads them.
fn credentials_from_json(
    json: serde_json::Value,
) -> Result<Credentials, google_cloud_auth::build_errors::Error> {
    match json.get("type").and_then(|t| t.as_str()) {
        Some("authorized_user") => user_account::Builder::new(json).build(),
        Some("external_account") => external_account::Builder::new(json).build(),
        Some("impersonated_service_account") => impersonated::Builder::new(json).build(),
        _ => service_account::Builder::new(json).build(),
    }
}

fn decode_credentials(creds_base64: &str) -> Result<String, CubeError> {
    Ok(String::from_utf8(base64::decode(creds_base64)?)?)
}

pub struct GCSRemoteFs {
    dir: PathBuf,
    bucket: String,
    sub_path: Option<String>,
    credentials: Credentials,
    storage: Storage,
    http: reqwest::Client,
    delete_mut: Mutex<()>,
}

impl fmt::Debug for GCSRemoteFs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("GCSRemoteFs")
            .field("dir", &self.dir)
            .field("bucket", &self.bucket)
            .field("sub_path", &self.sub_path)
            .finish_non_exhaustive()
    }
}

impl GCSRemoteFs {
    pub async fn new(
        dir: PathBuf,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        let credentials = gcp_credentials()?;
        let storage = Storage::builder()
            .with_credentials(credentials.clone())
            .build()
            .await
            .map_err(|e| CubeError::internal(format!("Could not create GCS client: {}", e)))?;
        Ok(Arc::new(Self {
            dir,
            bucket: bucket_name.to_string(),
            sub_path,
            credentials,
            storage,
            http: reqwest::Client::builder()
                .timeout(JSON_API_TIMEOUT)
                .build()?,
            delete_mut: Mutex::new(()),
        }))
    }
}

di_service!(GCSRemoteFs, [RemoteFs, ExtendedRemoteFs]);

/// GCS answers 404 for an object that is already gone, where S3 answers 204. Absent is
/// what a `delete_file` caller asked for, so it counts as done. A missing bucket answers 404 too
/// and is not told apart: it fails every upload and download loudly.
fn is_absent_object(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND
}

#[async_trait]
impl RemoteFs for GCSRemoteFs {
    async fn temp_upload_path(&self, remote_path: String) -> Result<String, CubeError> {
        CommonRemoteFsUtils::temp_upload_path(self, remote_path).await
    }

    async fn uploads_dir(&self) -> Result<String, CubeError> {
        CommonRemoteFsUtils::uploads_dir(self).await
    }

    async fn check_upload_file(
        &self,
        remote_path: String,
        expected_size: u64,
    ) -> Result<(), CubeError> {
        CommonRemoteFsUtils::check_upload_file(self, remote_path, expected_size).await
    }

    async fn upload_file(
        &self,
        temp_upload_path: String,
        remote_path: String,
    ) -> Result<u64, CubeError> {
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            1,
            Some(&vec![
                "operation:upload_file".to_string(),
                "driver:gcs".to_string(),
            ]),
        );
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let file = File::open(temp_upload_path.clone()).await?;
        self.storage
            .write_object(self.bucket_resource(), self.gcs_path(&remote_path), file)
            .set_content_type("application/octet-stream")
            .send_unbuffered()
            .await?;

        let size = fs::metadata(temp_upload_path.clone()).await?.len();
        self.check_upload_file(remote_path.clone(), size).await?;

        let local_path = self.dir.as_path().join(&remote_path);
        if Path::new(&temp_upload_path) != local_path {
            fs::create_dir_all(local_path.parent().unwrap())
                .await
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Create dir {}: {}",
                        local_path.parent().as_ref().unwrap().to_string_lossy(),
                        e
                    ))
                })?;
            fs::rename(&temp_upload_path, local_path.clone()).await?;
        }
        info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);
        Ok(fs::metadata(local_path).await?.len())
    }

    async fn download_file(
        &self,
        remote_path: String,
        _expected_file_size: Option<u64>,
    ) -> Result<String, CubeError> {
        let mut local_file = self.dir.as_path().join(&remote_path);
        let local_dir = local_file.parent().unwrap();
        let downloads_dirs = local_dir.join("downloads");

        fs::create_dir_all(&downloads_dirs).await?;
        if !local_file.exists() {
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec![
                    "operation:download_file".to_string(),
                    "driver:gcs".to_string(),
                ]),
            );
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);
            let (temp_file, temp_path) =
                cube_ext::spawn_blocking(move || NamedTempFile::new_in(downloads_dirs))
                    .await??
                    .into_parts();
            let mut writer = BufWriter::new(tokio::fs::File::from_std(temp_file));
            let mut response = self
                .storage
                .read_object(self.bucket_resource(), self.gcs_path(&remote_path))
                .send()
                .await?;

            let mut c = 0;
            while let Some(chunk) = response.next().await {
                let chunk = chunk?;
                writer.write_all(&chunk).await?;
                c += chunk.len();
            }
            writer.flush().await?;

            local_file = cube_ext::spawn_blocking(move || -> Result<PathBuf, PathPersistError> {
                temp_path.persist(&local_file)?;
                Ok(local_file)
            })
            .await??;

            info!(
                "Downloaded {} ({:?}) ({} bytes)",
                remote_path,
                time.elapsed()?,
                c
            );
        }
        Ok(local_file.into_os_string().into_string().unwrap())
    }

    async fn delete_file(&self, remote_path: String) -> Result<(), CubeError> {
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            1,
            Some(&vec![
                "operation:delete_file".to_string(),
                "driver:gcs".to_string(),
            ]),
        );
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);
        let url = object_url(&self.bucket, Some(&self.gcs_path(&remote_path)))?;
        let response = self.send(self.http.delete(url)).await?;
        match response.status() {
            s if s.is_success() => info!("Deleting {} ({:?})", remote_path, time.elapsed()?),
            s if is_absent_object(s) => {
                debug!("File {} is already absent in remote fs", remote_path);
            }
            _ => return Err(json_api_error(response).await),
        }

        let _guard = acquire_lock("delete file", self.delete_mut.lock()).await?;
        let local = self.dir.as_path().join(remote_path);
        if fs::metadata(local.clone()).await.is_ok() {
            fs::remove_file(local.clone()).await?;
            LocalDirRemoteFs::remove_empty_paths(self.dir.as_path().to_path_buf(), local.clone())
                .await?;
        }

        Ok(())
    }

    async fn list(&self, remote_prefix: String) -> Result<Vec<String>, CubeError> {
        let leading_subpath = self.leading_subpath_regex();
        self.list_with_metadata_and_map(remote_prefix, |obj: ListedObject| {
            Ok(Self::object_key_to_remote_path(&leading_subpath, &obj.name))
        })
        .await
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let leading_subpath = self.leading_subpath_regex();
        self.list_with_metadata_and_map(remote_prefix, |obj: ListedObject| {
            Ok(RemoteFile {
                remote_path: Self::object_key_to_remote_path(&leading_subpath, &obj.name),
                updated: DateTime::parse_from_rfc3339(&obj.updated)?.with_timezone(&Utc),
                file_size: obj.size.parse()?,
            })
        })
        .await
    }

    async fn local_path(&self) -> Result<String, CubeError> {
        Ok(self.dir.to_str().unwrap().to_owned())
    }

    async fn local_file(&self, remote_path: String) -> Result<String, CubeError> {
        let buf = self.dir.join(remote_path);
        fs::create_dir_all(buf.parent().unwrap()).await?;
        Ok(buf.to_str().unwrap().to_string())
    }
}

// TODO: Make a faster implementation
#[async_trait]
impl ExtendedRemoteFs for GCSRemoteFs {}

struct LeadingSubpath(Regex);

impl GCSRemoteFs {
    fn leading_subpath_regex(&self) -> LeadingSubpath {
        LeadingSubpath(Regex::new(format!("^{}", self.gcs_path("")).as_str()).unwrap())
    }

    fn object_key_to_remote_path(leading_subpath: &LeadingSubpath, obj_name: &String) -> String {
        leading_subpath
            .0
            .replace(&obj_name, NoExpand(""))
            .to_string()
    }

    async fn list_with_metadata_and_map<T, F>(
        &self,
        remote_prefix: String,
        mut f: F,
    ) -> Result<Vec<T>, CubeError>
    where
        F: FnMut(ListedObject) -> Result<T, CubeError> + Copy,
    {
        let prefix = self.gcs_path(&remote_prefix);
        let mut result = Vec::new();
        let mut pages_count: i64 = 0;
        let mut page_token: Option<String> = None;
        loop {
            let mut url = object_url(&self.bucket, None)?;
            url.query_pairs_mut()
                .append_pair("prefix", &prefix)
                .append_pair("fields", "items(name,size,updated),nextPageToken");
            if let Some(token) = page_token.as_deref() {
                url.query_pairs_mut().append_pair("pageToken", token);
            }
            let response = self.send(self.http.get(url)).await?;
            if !response.status().is_success() {
                return Err(json_api_error(response).await);
            }
            let page: ObjectList = response.json().await?;
            pages_count += 1;
            for object in page.items {
                result.push(f(object)?);
            }
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }
        if pages_count > 100 {
            log::warn!("GCS list returned more than 100 pages: {}", pages_count);
        }
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            pages_count,
            Some(&vec![
                "operation:list".to_string(),
                "driver:gcs".to_string(),
            ]),
        );
        Ok(result)
    }

    fn bucket_resource(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket)
    }

    /// List and delete are idempotent (a repeated delete answers 404), so a failed connection, a
    /// timeout, a 429 or a 5xx is retried with backoff, as the SDK does for upload and download.
    async fn send(&self, request: RequestBuilder) -> Result<Response, CubeError> {
        let mut attempt = 1;
        loop {
            let retry = request
                .try_clone()
                .ok_or_else(|| CubeError::internal("GCS request is not retryable".to_string()))?;
            let headers = self.auth_headers().await?;
            let last = attempt == JSON_API_ATTEMPTS;
            match retry.headers(headers).send().await {
                Ok(response) if last || !is_transient_status(response.status()) => {
                    return Ok(response)
                }
                Err(e) if last || !(e.is_connect() || e.is_timeout()) => return Err(e.into()),
                Ok(response) => debug!("GCS answered {}, retrying", response.status()),
                Err(e) => debug!("GCS request failed, retrying: {}", e),
            }
            tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
            attempt += 1;
        }
    }

    async fn auth_headers(&self) -> Result<HeaderMap, CubeError> {
        match self.credentials.headers(Default::default()).await {
            Ok(CacheableResource::New { data, .. }) => Ok(data),
            Ok(CacheableResource::NotModified) => Err(CubeError::internal(
                "GCP credentials returned no headers".to_string(),
            )),
            Err(e) => Err(CubeError::internal(format!(
                "Could not get a GCP access token: {}",
                e
            ))),
        }
    }

    fn gcs_path(&self, remote_path: &str) -> String {
        format!(
            "{}/{}",
            self.sub_path
                .as_ref()
                .map(|p| p.to_string())
                .unwrap_or_else(|| "".to_string()),
            remote_path
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ObjectList {
    #[serde(default)]
    items: Vec<ListedObject>,
    next_page_token: Option<String>,
}

/// The JSON API sends `size` as a decimal string and `updated` as RFC 3339.
#[derive(Deserialize)]
struct ListedObject {
    name: String,
    size: String,
    updated: String,
}

/// `objects` of the bucket, or one object when `object` is given. An object name is
/// percent-encoded as one path segment, `/` included.
fn object_url(bucket: &str, object: Option<&str>) -> Result<Url, CubeError> {
    let mut url = Url::parse(JSON_API_URL)?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| CubeError::internal(format!("Invalid URL: {}", JSON_API_URL)))?;
        segments.extend(&["b", bucket, "o"]);
        if let Some(object) = object {
            segments.push(object);
        }
    }
    Ok(url)
}

fn is_transient_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

async fn json_api_error(response: Response) -> CubeError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    CubeError::internal(format!("GCS answered {}: {}", status, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_object_is_recognized() {
        assert!(is_absent_object(StatusCode::NOT_FOUND));
    }

    #[test]
    fn other_failures_are_not_absent_objects() {
        assert!(!is_absent_object(StatusCode::FORBIDDEN));
        assert!(!is_absent_object(StatusCode::SERVICE_UNAVAILABLE));
    }

    #[test]
    fn undecodable_inline_credentials_fail_only_when_used() {
        let key_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(key_file.path(), r#"{"type":"service_account"}"#).unwrap();
        let key_file_path = key_file.path().to_str().unwrap().to_string();
        let env = |key_file: Option<String>| {
            move |name: &str| match name {
                "CUBESTORE_GCP_CREDENTIALS" => Some("not base64".to_string()),
                "CUBESTORE_GCP_KEY_FILE" => key_file.clone(),
                _ => None,
            }
        };

        let (json, var) = service_account_key(env(Some(key_file_path)))
            .unwrap()
            .unwrap();
        assert_eq!(var, "CUBESTORE_GCP_KEY_FILE");
        assert_eq!(json, r#"{"type":"service_account"}"#);

        assert!(service_account_key(env(None)).is_err());
    }

    #[test]
    fn only_rate_limits_and_server_errors_are_retried() {
        assert!(is_transient_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_transient_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_transient_status(StatusCode::NOT_FOUND));
        assert!(!is_transient_status(StatusCode::FORBIDDEN));
    }

    #[tokio::test]
    async fn user_credentials_are_not_read_as_a_service_account_key() {
        let authorized_user = serde_json::json!({
            "type": "authorized_user",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "refresh_token": "refresh-token",
        });
        assert!(credentials_from_json(authorized_user).is_ok());

        let incomplete_key = serde_json::json!({ "type": "service_account" });
        assert!(credentials_from_json(incomplete_key).is_err());
    }

    #[test]
    fn object_names_are_one_path_segment() {
        assert_eq!(
            object_url("bucket", Some("/metastore-1/a b.parquet"))
                .unwrap()
                .as_str(),
            "https://storage.googleapis.com/storage/v1/b/bucket/o/%2Fmetastore-1%2Fa%20b.parquet"
        );
        assert_eq!(
            object_url("bucket", None).unwrap().as_str(),
            "https://storage.googleapis.com/storage/v1/b/bucket/o"
        );
    }
}
