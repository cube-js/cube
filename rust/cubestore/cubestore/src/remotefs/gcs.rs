use crate::di_service;
use crate::remotefs::{LocalDirRemoteFs, RemoteFile, RemoteFs};
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

di_service!(GCSRemoteFs, [RemoteFs]);

#[async_trait]
impl RemoteFs for GCSRemoteFs {
    async fn upload_file(
        &self,
        temp_upload_path: &str,
        remote_path: &str,
    ) -> Result<u64, CubeError> {
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let file = File::open(temp_upload_path).await?;
        let stream = FramedRead::new(file, BytesCodec::new());
        let stream = stream.map(|r| r.map(|b| b.to_vec()));
        Object::create_streamed(
            self.bucket.as_str(),
            stream,
            None,
            self.gcs_path(remote_path).as_str(),
            "application/octet-stream",
        )
        .await?;

        let size = fs::metadata(temp_upload_path).await?.len();
        self.check_upload_file(remote_path, size).await?;

        let local_path = self.dir.as_path().join(remote_path);
        if Path::new(temp_upload_path) != local_path {
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
        remote_path: &str,
        _expected_file_size: Option<u64>,
    ) -> Result<String, CubeError> {
        let mut local_file = self.dir.as_path().join(remote_path);
        let local_dir = local_file.parent().unwrap();
        let downloads_dirs = local_dir.join("downloads");

        fs::create_dir_all(&downloads_dirs).await?;
        if !local_file.exists() {
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);
            let (temp_file, temp_path) =
                cube_ext::spawn_blocking(move || NamedTempFile::new_in(downloads_dirs))
                    .await??
                    .into_parts();
            let mut writer = BufWriter::new(tokio::fs::File::from_std(temp_file));
            let mut stream = Object::download_streamed(
                self.bucket.as_str(),
                self.gcs_path(remote_path).as_str(),
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

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);
        Object::delete(self.bucket.as_str(), self.gcs_path(remote_path).as_str()).await?;
        info!("Deleting {} ({:?})", remote_path, time.elapsed()?);

        let _guard = acquire_lock("delete file", self.delete_mut.lock()).await?;
        let local = self.dir.as_path().join(remote_path);
        if fs::metadata(local.clone()).await.is_ok() {
            fs::remove_file(local.clone()).await?;
            LocalDirRemoteFs::remove_empty_paths(self.dir.as_path().to_path_buf(), local.clone())
                .await?;
        }

        Ok(())
    }

    async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError> {
        Ok(self
            .list_with_metadata(remote_prefix)
            .await?
            .into_iter()
            .map(|f| f.remote_path)
            .collect::<Vec<_>>())
    }

    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError> {
        let prefix = self.gcs_path(remote_prefix);
        let list = Object::list_prefix(self.bucket.as_str(), prefix.as_str()).await?;
        let leading_slash = Regex::new(format!("^{}", self.gcs_path("")).as_str()).unwrap();
        let result = list
            .map(|objects| -> Result<Vec<RemoteFile>, CubeError> {
                Ok(objects?
                    .into_iter()
                    .map(|obj| RemoteFile {
                        remote_path: leading_slash.replace(&obj.name, NoExpand("")).to_string(),
                        updated: obj.updated.clone(),
                        file_size: obj.size,
                    })
                    .collect())
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .flatten()
            .collect::<Vec<_>>();
        Ok(result)
    }

    async fn local_path(&self) -> String {
        self.dir.to_str().unwrap().to_owned()
    }

    async fn local_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let buf = self.dir.join(remote_path);
        fs::create_dir_all(buf.parent().unwrap()).await?;
        Ok(buf.to_str().unwrap().to_string())
    }
}

impl GCSRemoteFs {
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
