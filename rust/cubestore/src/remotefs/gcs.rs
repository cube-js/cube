use crate::remotefs::{LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::CubeError;
use async_trait::async_trait;
use cloud_storage::Object;
use futures::StreamExt;
use log::{debug, info};
use regex::{NoExpand, Regex};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::RwLock;
use tokio_util::codec::{BytesCodec, FramedRead};

#[derive(Debug)]
pub struct GCSRemoteFs {
    dir: RwLock<PathBuf>,
    bucket: String,
    sub_path: Option<String>,
}

impl GCSRemoteFs {
    pub fn new(
        dir: PathBuf,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        Ok(Arc::new(Self {
            dir: RwLock::new(dir),
            bucket: bucket_name.to_string(),
            sub_path,
        }))
    }
}

#[async_trait]
impl RemoteFs for GCSRemoteFs {
    async fn upload_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let local_path = self.dir.read().await.as_path().join(remote_path);
        let file = File::open(local_path).await?;
        let size = file.metadata().await?.len();
        let stream = FramedRead::new(file, BytesCodec::new());
        let stream = stream.map(|r| r.map(|b| b.to_vec()));
        Object::create_streamed(
            self.bucket.as_str(),
            stream,
            Some(size),
            self.gcs_path(remote_path).as_str(),
            "application/octet-stream",
        )
        .await?;
        info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);
        Ok(())
    }

    async fn download_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let local = self.dir.write().await.as_path().join(remote_path);
        let path = local.to_str().unwrap().to_owned();
        fs::create_dir_all(local.parent().unwrap()).await?;
        if !local.exists() {
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);
            let output_file = File::create(path.as_str()).await?;
            let mut writer = BufWriter::new(output_file);
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

            info!(
                "Downloaded {} ({:?}) ({} bytes)",
                remote_path,
                time.elapsed()?,
                c
            );
        }
        Ok(path)
    }

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);
        Object::delete(self.bucket.as_str(), self.gcs_path(remote_path).as_str()).await?;
        info!("Deleting {} ({:?})", remote_path, time.elapsed()?);

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
        self.dir.read().await.to_str().unwrap().to_owned()
    }

    async fn local_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let buf = self.dir.read().await.join(remote_path);
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
