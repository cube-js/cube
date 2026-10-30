use crate::app_metrics;
use crate::di_service;
use crate::remotefs::ExtendedRemoteFs;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::cube_ext;
use google_cloud_auth::credentials::{self, service_account, Credentials};
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::paginator::Paginator;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::Object;
use log::{debug, info};
use regex::{NoExpand, Regex};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

/// Finds the service account key in the environment. Returns the key JSON and the variable it was
/// read from, or `None` when no variable is set.
fn service_account_key() -> Result<Option<(String, String)>, CubeError> {
    let mut creds_json = None;
    if let Ok(c) = std::env::var("CUBESTORE_GCP_CREDENTIALS") {
        match decode_credentials(&c) {
            Ok(s) => creds_json = Some((s, "CUBESTORE_GCP_CREDENTIALS".to_string())),
            Err(e) => log::error!("Could not decode 'CUBESTORE_GCP_CREDENTIALS': {}", e),
        }
    }
    let mut creds_path = match std::env::var("CUBESTORE_GCP_KEY_FILE") {
        Ok(s) => Some((s, "CUBESTORE_GCP_KEY_FILE".to_string())),
        Err(_) => None,
    };

    // TODO: this handles deprecated variable names, remove them.
    for (var, is_path) in &[
        ("SERVICE_ACCOUNT", true),
        ("GOOGLE_APPLICATION_CREDENTIALS", true),
        ("SERVICE_ACCOUNT_JSON", false),
        ("GOOGLE_APPLICATION_CREDENTIALS_JSON", false),
    ] {
        for var in &[&format!("CUBESTORE_GCP_{}", var), *var] {
            if let Ok(var_value) = std::env::var(&var) {
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
        None => Ok(creds_json),
    }
}

/// The service account key from the environment, or Application Default Credentials (the
/// metadata server on GKE and GCE) when none is set.
fn gcp_credentials() -> Result<Credentials, CubeError> {
    match service_account_key()? {
        Some((json, var)) => {
            info!("Using GCP service account key from '{}'", var);
            let key = serde_json::from_str(&json).map_err(|e| {
                CubeError::user(format!(
                    "Could not parse GCP credentials from '{}': {}",
                    var, e
                ))
            })?;
            service_account::Builder::new(key).build().map_err(|e| {
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

fn decode_credentials(creds_base64: &str) -> Result<String, CubeError> {
    Ok(String::from_utf8(base64::decode(creds_base64)?)?)
}

pub struct GCSRemoteFs {
    dir: PathBuf,
    bucket: String,
    sub_path: Option<String>,
    storage: Storage,
    control: StorageControl,
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
        let control = StorageControl::builder()
            .with_credentials(credentials)
            .build()
            .await
            .map_err(|e| CubeError::internal(format!("Could not create GCS client: {}", e)))?;
        Ok(Arc::new(Self {
            dir,
            bucket: bucket_name.to_string(),
            sub_path,
            storage,
            control,
            delete_mut: Mutex::new(()),
        }))
    }
}

di_service!(GCSRemoteFs, [RemoteFs, ExtendedRemoteFs]);

/// GCS answers NOT_FOUND for an object that is already gone, where S3 answers 204. Absent is
/// what a `delete_file` caller asked for, so it counts as done. A missing bucket answers NOT_FOUND
/// too and is not told apart: it fails every upload and download loudly.
fn is_absent_object(e: &google_cloud_gax::error::Error) -> bool {
    e.status().map_or(false, |s| s.code == Code::NotFound) || e.http_status_code() == Some(404)
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
        let result = self
            .control
            .delete_object()
            .set_bucket(self.bucket_resource())
            .set_object(self.gcs_path(&remote_path))
            .send()
            .await;
        match result {
            Ok(()) => info!("Deleting {} ({:?})", remote_path, time.elapsed()?),
            Err(e) if is_absent_object(&e) => {
                debug!("File {} is already absent in remote fs", remote_path);
            }
            Err(e) => return Err(e.into()),
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
        self.list_with_metadata_and_map(remote_prefix, |obj: Object| {
            Self::object_key_to_remote_path(&leading_subpath, &obj.name)
        })
        .await
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let leading_subpath = self.leading_subpath_regex();
        self.list_with_metadata_and_map(remote_prefix, |obj: Object| RemoteFile {
            remote_path: Self::object_key_to_remote_path(&leading_subpath, &obj.name),
            updated: obj
                .update_time
                .and_then(|t| DateTime::<Utc>::from_timestamp(t.seconds(), t.nanos() as u32))
                .unwrap_or_else(Utc::now),
            file_size: obj.size as u64,
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
        F: FnMut(Object) -> T + Copy,
    {
        let prefix = self.gcs_path(&remote_prefix);
        let mut pages = self
            .control
            .list_objects()
            .set_parent(self.bucket_resource())
            .set_prefix(prefix)
            .by_page();
        let mut result = Vec::new();
        let mut pages_count: i64 = 0;
        while let Some(page) = pages.next().await {
            let page = page?;
            pages_count += 1;
            result.extend(page.objects.into_iter().map(&mut f));
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

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_gax::error::Error;

    fn service_error(code: Code, message: &str) -> Error {
        Error::service(Status::default().set_code(code).set_message(message))
    }

    #[test]
    fn absent_object_is_recognized() {
        assert!(is_absent_object(&service_error(
            Code::NotFound,
            "No such object: bucket/1829139-ihr4wfxi.parquet"
        )));
    }

    #[test]
    fn other_failures_are_not_absent_objects() {
        assert!(!is_absent_object(&service_error(
            Code::PermissionDenied,
            "Access denied"
        )));
        assert!(!is_absent_object(&Error::io("connection reset")));
    }
}
