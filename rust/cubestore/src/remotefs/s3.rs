use crate::di_service;
use crate::remotefs::{LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{debug, info};
use regex::{NoExpand, Regex};
use s3::creds::Credentials;
use s3::Bucket;
use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct S3RemoteFs {
    dir: PathBuf,
    bucket: Bucket,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
}

impl S3RemoteFs {
    pub fn new(
        dir: PathBuf,
        region: String,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        let credentials = Credentials::new(
            env::var("CUBESTORE_AWS_ACCESS_KEY_ID").as_deref().ok(),
            env::var("CUBESTORE_AWS_SECRET_ACCESS_KEY").as_deref().ok(),
            None,
            None,
            None,
        )?;
        let bucket = Bucket::new(&bucket_name, region.parse()?, credentials)?;
        Ok(Arc::new(Self {
            dir,
            bucket,
            sub_path,
            delete_mut: Mutex::new(()),
        }))
    }
}

di_service!(S3RemoteFs, [RemoteFs]);

#[async_trait]
impl RemoteFs for S3RemoteFs {
    async fn upload_file(
        &self,
        temp_upload_path: &str,
        remote_path: &str,
    ) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let path = self.s3_path(remote_path);
        let bucket = self.bucket.clone();
        let temp_upload_path_copy = temp_upload_path.to_string();
        let status_code = tokio::task::spawn_blocking(move || {
            bucket.put_object_stream_blocking(temp_upload_path_copy, path)
        })
        .await??;
        let local_path = self.dir.as_path().join(remote_path);
        if Path::new(temp_upload_path) != local_path {
            fs::rename(&temp_upload_path, local_path).await?;
        }
        info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);
        if status_code != 200 {
            return Err(CubeError::user(format!(
                "S3 upload returned non OK status: {}",
                status_code
            )));
        }
        Ok(())
    }

    async fn download_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let local_file = self.dir.as_path().join(remote_path);
        let local_dir = local_file.parent().unwrap();
        let downloads_dir = local_dir.join("downloads");

        let local_file_str = local_file.to_str().unwrap().to_string(); // return value.

        fs::create_dir_all(&downloads_dir).await?;
        if !local_file.exists() {
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);
            let path = self.s3_path(remote_path);
            let bucket = self.bucket.clone();
            let status_code = tokio::task::spawn_blocking(move || -> Result<u16, CubeError> {
                let (mut temp_file, temp_path) =
                    NamedTempFile::new_in(&downloads_dir)?.into_parts();

                let res = bucket.get_object_stream_blocking(path.as_str(), &mut temp_file)?;
                temp_file.flush()?;

                temp_path.persist(local_file)?;

                Ok(res)
            })
            .await??;
            info!("Downloaded {} ({:?})", remote_path, time.elapsed()?);
            if status_code != 200 {
                return Err(CubeError::user(format!(
                    "S3 download returned non OK status: {}",
                    status_code
                )));
            }
        }
        Ok(local_file_str)
    }

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);
        let path = self.s3_path(remote_path);
        let bucket = self.bucket.clone();
        let (_, status_code) =
            tokio::task::spawn_blocking(move || bucket.delete_object_blocking(path)).await??;
        info!("Deleting {} ({:?})", remote_path, time.elapsed()?);
        if status_code != 204 {
            return Err(CubeError::user(format!(
                "S3 delete returned non OK status: {}",
                status_code
            )));
        }

        self.delete_local_copy(remote_path).await
    }

    async fn delete_local_copy(&self, remote_path: &str) -> Result<(), CubeError> {
        let _guard = self.delete_mut.lock().await;
        let local = self.dir.as_path().join(remote_path);
        if let Err(e) = fs::remove_file(local.clone()).await {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(());
            } else {
                return Err(e)?;
            }
        }
        // We have removed a file, cleanup.
        LocalDirRemoteFs::remove_empty_paths(self.dir.as_path().to_path_buf(), local.clone()).await
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
        let path = self.s3_path(remote_prefix);
        let bucket = self.bucket.clone();
        let list = tokio::task::spawn_blocking(move || bucket.list_blocking(path, None)).await??;
        let leading_slash = Regex::new(format!("^{}", self.s3_path("")).as_str()).unwrap();
        let result = list
            .iter()
            .flat_map(|(res, _)| {
                res.contents
                    .iter()
                    .map(|o| -> Result<RemoteFile, CubeError> {
                        Ok(RemoteFile {
                            remote_path: leading_slash.replace(&o.key, NoExpand("")).to_string(),
                            updated: DateTime::parse_from_rfc3339(&o.last_modified)?
                                .with_timezone(&Utc),
                        })
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
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

impl S3RemoteFs {
    fn s3_path(&self, remote_path: &str) -> String {
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
