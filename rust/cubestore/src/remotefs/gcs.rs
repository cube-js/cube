use crate::di_service;
use crate::remotefs::remote_storage::RemoteStorage;
use crate::remotefs::RemoteFile;
use crate::CubeError;
use async_trait::async_trait;
use cloud_storage::Object;
use futures::StreamExt;
use log::{debug, info};
use regex::{NoExpand, Regex};
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::codec::{BytesCodec, FramedRead};

#[derive(Debug)]
pub struct GCSRemoteFs {
    bucket: String,
    sub_path: Option<String>,
}

impl GCSRemoteFs {
    pub fn new(bucket_name: String, sub_path: Option<String>) -> Result<Arc<Self>, CubeError> {
        Ok(Arc::new(Self {
            bucket: bucket_name.to_string(),
            sub_path,
        }))
    }
}

di_service!(GCSRemoteFs, [RemoteStorage]);

#[async_trait]
impl RemoteStorage for GCSRemoteFs {
    async fn upload_file(
        &self,
        temp_upload_path: &str,
        remote_path: &str,
    ) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let file = File::open(temp_upload_path).await?;
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

    async fn download_file(
        &self,
        _local_file_path: &Path,
        local_file: std::fs::File,
        remote_path: &str,
    ) -> Result<(), CubeError> {
        let time = SystemTime::now();
        let mut writer = BufWriter::new(tokio::fs::File::from_std(local_file));
        let mut stream =
            Object::download_streamed(self.bucket.as_str(), self.gcs_path(remote_path).as_str())
                .await?;

        let mut c: usize = 0;
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
        Ok(())
    }

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);
        Object::delete(self.bucket.as_str(), self.gcs_path(remote_path).as_str()).await?;
        info!("Deleting {} ({:?})", remote_path, time.elapsed()?);
        Ok(())
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
