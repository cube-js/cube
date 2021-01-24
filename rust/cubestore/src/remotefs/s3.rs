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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct S3RemoteFs {
    dir: RwLock<PathBuf>,
    bucket: Bucket,
    sub_path: Option<String>,
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
            dir: RwLock::new(dir),
            bucket,
            sub_path,
        }))
    }
}

#[async_trait]
impl RemoteFs for S3RemoteFs {
    async fn upload_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let path = self.s3_path(remote_path);
        let bucket = self.bucket.clone();
        let local_path = self.dir.read().await.as_path().join(remote_path);
        let status_code = tokio::task::spawn_blocking(move || {
            bucket.put_object_stream_blocking(local_path, path)
        })
        .await??;
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
        let local = self.dir.write().await.as_path().join(remote_path);
        let local_path = local.to_str().unwrap().to_owned();
        let local_to_move = local_path.clone();
        fs::create_dir_all(local.parent().unwrap()).await?;
        if !local.exists() {
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);
            let path = self.s3_path(remote_path);
            let bucket = self.bucket.clone();
            let status_code = tokio::task::spawn_blocking(move || {
                let mut output_file = std::fs::File::create(local_to_move.as_str())?;
                let res = bucket.get_object_stream_blocking(path.as_str(), &mut output_file);
                output_file.flush()?;
                res
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
        Ok(local_path)
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

        let dir = self.dir.write().await;
        let local = dir.as_path().join(remote_path);
        if fs::metadata(local.clone()).await.is_ok() {
            fs::remove_file(local.clone()).await?;
            LocalDirRemoteFs::remove_empty_paths(dir.as_path().to_path_buf(), local.clone())
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
        self.dir.read().await.to_str().unwrap().to_owned()
    }

    async fn local_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let buf = self.dir.read().await.join(remote_path);
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
