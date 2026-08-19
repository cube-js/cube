use crate::app_metrics;
use crate::di_service;
use crate::remotefs::ExtendedRemoteFs;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use cloud_storage::Object;
use datafusion::cube_ext;
use futures::StreamExt;
use log::{debug, info};
use regex::{NoExpand, Regex};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::SystemTime;
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tokio_util::codec::{BytesCodec, FramedRead};

static INIT_CREDENTIALS: Once = Once::new();
fn ensure_credentials_init() {
    // The cloud storage library uses env vars to get access tokens.
    // We decided CubeStore needs its own alias for it, so rewrite and hope no one read it before.
    // TODO: switch to something that allows to configure without env vars.
    // TODO: remove `SERVICE_ACCOUNT` completely.
    INIT_CREDENTIALS.call_once(|| {
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

        match creds_json {
            Some((v, _)) => std::env::set_var("SERVICE_ACCOUNT_JSON", v),
            None => std::env::remove_var("SERVICE_ACCOUNT_JSON"),
        }
        match creds_path {
            Some((v, _)) => std::env::set_var("SERVICE_ACCOUNT", v),
            None => std::env::remove_var("SERVICE_ACCOUNT"),
        }
    })
}

fn decode_credentials(creds_base64: &str) -> Result<String, CubeError> {
    Ok(String::from_utf8(base64::decode(creds_base64)?)?)
}

#[derive(Debug)]
pub struct GCSRemoteFs {
    dir: PathBuf,
    bucket: String,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
}

impl GCSRemoteFs {
    pub fn new(
        dir: PathBuf,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        ensure_credentials_init();
        Ok(Arc::new(Self {
            dir,
            bucket: bucket_name.to_string(),
            sub_path,
            delete_mut: Mutex::new(()),
        }))
    }
}

di_service!(GCSRemoteFs, [RemoteFs, ExtendedRemoteFs]);

/// GCS answers 404 for an object that is already gone where S3 answers 204. Absent is the state a
/// caller of `delete_file` asked for and none of them can act on the difference, so it is reported
/// as done rather than as a failure they have to log.
///
/// A missing bucket carries the same reason, and the client crate keeps the message and the code
/// private, so the two cannot be told apart here. A bucket that is not there fails every upload and
/// download loudly, which is the signal to go by.
fn is_absent_object(e: &cloud_storage::Error) -> bool {
    match e {
        cloud_storage::Error::Google(response) => {
            response.errors_has_reason(&cloud_storage::Reason::NotFound)
        }
        _ => false,
    }
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
        let stream = FramedRead::new(file, BytesCodec::new());
        let stream = stream.map(|r| r.map(|b| b.to_vec()));
        Object::create_streamed(
            self.bucket.as_str(),
            stream,
            None,
            self.gcs_path(&remote_path).as_str(),
            "application/octet-stream",
        )
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
            let mut stream = Object::download_streamed(
                self.bucket.as_str(),
                self.gcs_path(&remote_path).as_str(),
            )
            .await?;

            let mut c = 0;
            while let Some(byte) = stream.next().await {
                // TODO it might be very slow
                writer.write_all(&[byte?]).await?;
                c += 1;
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
        match Object::delete(self.bucket.as_str(), self.gcs_path(&remote_path).as_str()).await {
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
            updated: obj.updated,
            file_size: obj.size,
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
        let list = Object::list_prefix(self.bucket.as_str(), prefix.as_str()).await?;
        tokio::pin!(list);
        let mut result = Vec::new();
        let mut pages_count: i64 = 0;
        while let Some(objects) = list.next().await {
            let objects = objects?;
            pages_count += 1;
            result.extend(objects.into_iter().map(&mut f));
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

    /// The payload a bucket really answers with, so a change in how the crate maps reasons shows
    /// up here instead of as a returning flood of errors from the scheduler.
    const NOT_FOUND_RESPONSE: &str = r#"{"error":{"errors":[{"domain":"global",
        "reason":"notFound","message":"No such object: bucket/1829139-ihr4wfxi.parquet"}],
        "code":404,"message":"No such object: bucket/1829139-ihr4wfxi.parquet"}}"#;

    const FORBIDDEN_RESPONSE: &str = r#"{"error":{"errors":[{"domain":"global",
        "reason":"forbidden","message":"Access denied"}],"code":403,"message":"Access denied"}}"#;

    fn google_error(payload: &str) -> cloud_storage::Error {
        cloud_storage::Error::Google(serde_json::from_str(payload).unwrap())
    }

    #[test]
    fn absent_object_is_recognized() {
        assert!(is_absent_object(&google_error(NOT_FOUND_RESPONSE)));
    }

    #[test]
    fn other_failures_are_not_absent_objects() {
        assert!(!is_absent_object(&google_error(FORBIDDEN_RESPONSE)));
        assert!(!is_absent_object(&cloud_storage::Error::Other(
            "connection reset".to_string()
        )));
    }
}
