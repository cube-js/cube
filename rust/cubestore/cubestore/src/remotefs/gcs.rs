use crate::app_metrics;
use crate::di_service;
use crate::remotefs::ExtendedRemoteFs;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::cube_ext;
use futures::StreamExt;
use log::{debug, info, warn};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

// WIF-native GCS implementation using the `object_store` crate.
//
// Replaces the original `cloud_storage`-based implementation which requires
// SERVICE_ACCOUNT or SERVICE_ACCOUNT_JSON env vars and panics without them.
// This implementation uses GoogleCloudStorageBuilder::from_env() which supports:
//   1. GOOGLE_APPLICATION_CREDENTIALS (key file path — backward compatible)
//   2. GKE Workload Identity Federation via metadata server at 169.254.169.254
//   3. gcloud CLI credentials (dev machines)
//
// Also accepts CUBESTORE_GCP_KEY_FILE and CUBESTORE_GCP_CREDENTIALS for
// backward compatibility with existing deployments.
//
// No credentials required when running on GKE with Workload Identity configured.
// OSS issue: https://github.com/cube-js/cube/issues/9837

fn decode_credentials(creds_base64: &str) -> Result<String, CubeError> {
    // base64 = "0.13.0" uses the old decode() API (pre-0.21 Engine API)
    let bytes = base64::decode(creds_base64)
        .map_err(|e| CubeError::internal(format!("Failed to decode base64 credentials: {}", e)))?;
    String::from_utf8(bytes)
        .map_err(|e| CubeError::internal(format!("Credentials not valid UTF-8: {}", e)))
}

#[derive(Debug)]
pub struct GCSRemoteFs {
    dir: PathBuf,
    #[allow(dead_code)]
    bucket: String,
    sub_path: Option<String>,
    store: Arc<dyn ObjectStore>,
    delete_mut: Mutex<()>,
}

impl GCSRemoteFs {
    pub fn new(
        dir: PathBuf,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        let mut builder = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&bucket_name);

        if let Ok(key_file) = std::env::var("CUBESTORE_GCP_KEY_FILE") {
            if !key_file.is_empty() {
                log::info!("[GCS] Using CUBESTORE_GCP_KEY_FILE for authentication");
                builder = builder.with_service_account_path(key_file);
            }
        } else if let Ok(creds_b64) = std::env::var("CUBESTORE_GCP_CREDENTIALS") {
            if !creds_b64.is_empty() {
                log::info!("[GCS] Using CUBESTORE_GCP_CREDENTIALS for authentication");
                let creds_json = decode_credentials(&creds_b64)?;
                builder = builder.with_service_account_key(creds_json);
            }
        } else {
            log::info!(
                "[GCS] No explicit credentials — using Application Default Credentials (WIF/ADC)"
            );
        }

        let store = builder.build().map_err(|e| {
            CubeError::internal(format!(
                "Failed to initialize GCS client for bucket '{}': {}. \
                Ensure Workload Identity is configured on this GKE node pool, \
                or set CUBESTORE_GCP_KEY_FILE / GOOGLE_APPLICATION_CREDENTIALS.",
                bucket_name, e
            ))
        })?;

        Ok(Arc::new(Self {
            dir,
            bucket: bucket_name,
            sub_path,
            store: Arc::new(store),
            delete_mut: Mutex::new(()),
        }))
    }

    fn gcs_path(&self, remote_path: &str) -> ObjPath {
        match &self.sub_path {
            Some(prefix) => ObjPath::from(format!("{}/{}", prefix, remote_path).as_str()),
            None => ObjPath::from(remote_path),
        }
    }

    fn strip_subpath_prefix(&self, obj_path: &str) -> String {
        match &self.sub_path {
            Some(prefix) => {
                let full_prefix = format!("{}/", prefix);
                obj_path
                    .strip_prefix(&full_prefix)
                    .unwrap_or(obj_path)
                    .to_string()
            }
            None => obj_path.to_string(),
        }
    }
}

di_service!(GCSRemoteFs, [RemoteFs, ExtendedRemoteFs]);

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
        // GCS list() is eventually consistent — a freshly uploaded object may not
        // appear in list() immediately.  Use head() instead, which is strongly
        // consistent on GCS (single-object metadata fetch, not a list scan).
        let obj_path = self.gcs_path(&remote_path);
        match self.store.head(&obj_path).await {
            Ok(meta) => {
                if meta.size as u64 != expected_size {
                    return Err(CubeError::internal(format!(
                        "check_upload_file: size mismatch for {}: expected {} bytes, got {} bytes",
                        remote_path, expected_size, meta.size
                    )));
                }
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => {
                Err(CubeError::internal(format!(
                    "check_upload_file: {} not found after upload (GCS consistency error)",
                    remote_path
                )))
            }
            Err(e) => Err(CubeError::internal(format!(
                "check_upload_file: head({}) failed: {}",
                remote_path, e
            ))),
        }
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

        let data = fs::read(&temp_upload_path).await.map_err(|e| {
            CubeError::internal(format!("Failed to read {}: {}", temp_upload_path, e))
        })?;
        let size = data.len() as u64;
        let obj_path = self.gcs_path(&remote_path);

        self.store
            .put(&obj_path, object_store::PutPayload::from(data))
            .await
            .map_err(|e| {
                CubeError::internal(format!("GCS put {} failed: {}", obj_path, e))
            })?;

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
            fs::rename(&temp_upload_path, local_path.clone()).await.map_err(|e| {
                CubeError::internal(format!("Rename temp file failed: {}", e))
            })?;
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

            let obj_path = self.gcs_path(&remote_path);
            let get_result = self.store.get(&obj_path).await.map_err(|e| {
                CubeError::internal(format!("GCS get {} failed: {}", obj_path, e))
            })?;
            let bytes: Bytes = get_result.bytes().await.map_err(|e| {
                CubeError::internal(format!("GCS read stream {} failed: {}", obj_path, e))
            })?;
            let size = bytes.len();

            let (temp_file, temp_path) =
                cube_ext::spawn_blocking(move || NamedTempFile::new_in(downloads_dirs))
                    .await??
                    .into_parts();
            let mut writer = BufWriter::new(tokio::fs::File::from_std(temp_file));
            writer.write_all(&bytes).await?;
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
                size
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

        let obj_path = self.gcs_path(&remote_path);
        match self.store.delete(&obj_path).await {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => {
                debug!("GCS object already gone: {}", obj_path);
            }
            Err(e) => {
                return Err(CubeError::internal(format!(
                    "GCS delete {} failed: {}",
                    obj_path, e
                )))
            }
        }
        info!("Deleted {} ({:?})", remote_path, time.elapsed()?);

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
        // CubeStore calls list() to either:
        //   A) Check existence of a root-level pointer file:
        //      "cachestore-current", "metastore-current" → ends with "current"
        //      "cachestore-XYZ-logs"                     → ends with "logs"
        //   B) Enumerate all snapshots: "cachestore-", "metastore-" → ends with "-"
        //
        // For A: use head() — direct metadata fetch, strongly consistent on GCS.
        //        object_store::list() with prefix "cachestore-current" may not return
        //        the flat object because GCS list uses a delimiter and object_store
        //        normalises the path, causing the flat file to be silently skipped.
        // For B: use list(prefix) as normal.
        let is_pointer_file =
            remote_prefix.ends_with("current") || remote_prefix.ends_with("logs");

        if is_pointer_file {
            let obj_path = self.gcs_path(&remote_prefix);
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec!["operation:list".to_string(), "driver:gcs".to_string()]),
            );
            match self.store.head(&obj_path).await {
                Ok(_) => return Ok(vec![remote_prefix]),
                Err(object_store::Error::NotFound { .. }) => return Ok(vec![]),
                Err(e) => {
                    return Err(CubeError::internal(format!(
                        "GCS list (head for pointer {}) failed: {}",
                        remote_prefix, e
                    )))
                }
            }
        }

        let obj_path = self.gcs_path(&remote_prefix);
        let path_str = obj_path.as_ref();
        let prefix_opt = if path_str.is_empty() { None } else { Some(obj_path.clone()) };

        let mut stream = self.store.list(prefix_opt.as_ref());
        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => {
                    results.push(self.strip_subpath_prefix(meta.location.as_ref()));
                }
                Err(e) => {
                    return Err(CubeError::internal(format!("GCS list failed: {}", e)))
                }
            }
        }
        let pages = (results.len() / 1_000).max(1);
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            pages as i64,
            Some(&vec!["operation:list".to_string(), "driver:gcs".to_string()]),
        );
        Ok(results)
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        // Three call patterns from CubeStore:
        //
        // 1. Root-level pointer file: "cachestore-current", "metastore-current"
        //    → ends with "current" or "logs", no slash.
        //    Use head() — strongly consistent, avoids list() path normalisation bug.
        //
        // 2. Exact file inside a snapshot folder: "cachestore-XYZ/CURRENT"
        //    → contains a slash.
        //    List the parent folder and filter by exact filename. Avoids head()
        //    which can be slow when called for many files in parallel.
        //
        // 3. Folder/prefix scan: "cachestore-1772823222184" or "cachestore-"
        //    → no slash, does not end with "current"/"logs".
        //    Use list(prefix) directly.

        let is_root_pointer =
            !remote_prefix.contains('/') &&
            (remote_prefix.ends_with("current") || remote_prefix.ends_with("logs"));

        // ── Case 1: Root-level pointer file ──────────────────────────────────
        if is_root_pointer {
            let obj_path = self.gcs_path(&remote_prefix);
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec!["operation:list".to_string(), "driver:gcs".to_string()]),
            );
            match self.store.head(&obj_path).await {
                Ok(meta) => {
                    return Ok(vec![RemoteFile {
                        remote_path: remote_prefix,
                        updated: meta.last_modified,
                        file_size: meta.size as u64,
                    }]);
                }
                Err(object_store::Error::NotFound { .. }) => {
                    warn!(
                        "[GCS] list_with_metadata: pointer file not found (just uploaded?): {}",
                        remote_prefix
                    );
                    return Ok(vec![]);
                }
                Err(e) => {
                    return Err(CubeError::internal(format!(
                        "GCS list_with_metadata (head for {}) failed: {}",
                        remote_prefix, e
                    )));
                }
            }
        }

        let obj_path = self.gcs_path(&remote_prefix);
        let path_str = obj_path.as_ref();

        // ── Case 2: Exact file inside a snapshot folder ───────────────────────
        if path_str.contains('/') {
            let slash_pos = path_str.rfind('/').unwrap();
            let folder = &path_str[..=slash_pos];    // "cachestore-XYZ/"
            let file_name = &path_str[slash_pos + 1..]; // "MANIFEST-000487"
            let folder_path = ObjPath::from(folder);

            let mut stream = self.store.list(Some(&folder_path));
            let mut results = Vec::new();
            while let Some(item) = stream.next().await {
                match item {
                    Ok(meta) => {
                        let key = self.strip_subpath_prefix(meta.location.as_ref());
                        if key.ends_with(&format!("/{}", file_name)) || key == remote_prefix {
                            results.push(RemoteFile {
                                remote_path: key,
                                updated: meta.last_modified,
                                file_size: meta.size as u64,
                            });
                        }
                    }
                    Err(e) => {
                        return Err(CubeError::internal(format!(
                            "GCS list_with_metadata (folder scan for {}) failed: {}",
                            remote_prefix, e
                        )))
                    }
                }
            }
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec!["operation:list".to_string(), "driver:gcs".to_string()]),
            );
            return Ok(results);
        }

        // ── Case 3: Folder/prefix scan ────────────────────────────────────────
        let prefix_opt = if path_str.is_empty() { None } else { Some(obj_path.clone()) };
        let mut stream = self.store.list(prefix_opt.as_ref());
        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => {
                    results.push(RemoteFile {
                        remote_path: self.strip_subpath_prefix(meta.location.as_ref()),
                        updated: meta.last_modified,
                        file_size: meta.size as u64,
                    });
                }
                Err(e) => {
                    return Err(CubeError::internal(format!(
                        "GCS list_with_metadata failed: {}",
                        e
                    )))
                }
            }
        }
        let pages = (results.len() / 1_000).max(1);
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            pages as i64,
            Some(&vec!["operation:list".to_string(), "driver:gcs".to_string()]),
        );
        Ok(results)
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

#[async_trait]
impl ExtendedRemoteFs for GCSRemoteFs {}
