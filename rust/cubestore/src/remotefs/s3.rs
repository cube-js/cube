use crate::di_service;
use crate::remotefs::remote_storage::RemoteStorage;
use crate::remotefs::RemoteFile;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{debug, info};
use regex::{NoExpand, Regex};
use s3::creds::Credentials;
use s3::Bucket;
use std::env;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug)]
pub struct S3RemoteFs {
    bucket: Bucket,
    sub_path: Option<String>,
}

impl S3RemoteFs {
    pub fn new(
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
        Ok(Arc::new(Self { bucket, sub_path }))
    }
}

di_service!(S3RemoteFs, [RemoteStorage]);

#[async_trait]
impl RemoteStorage for S3RemoteFs {
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
        info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);
        if status_code != 200 {
            return Err(CubeError::user(format!(
                "S3 upload returned non OK status: {}",
                status_code
            )));
        }
        Ok(())
    }

    async fn download_file(
        &self,
        _local_file_path: &Path,
        mut local_file: File,
        remote_path: &str,
    ) -> Result<(), CubeError> {
        let time = SystemTime::now();
        debug!("Downloading {}", remote_path);
        let path = self.s3_path(remote_path);
        let bucket = self.bucket.clone();
        let status_code = tokio::task::spawn_blocking(move || {
            bucket.get_object_stream_blocking(path.as_str(), &mut local_file)
        })
        .await??;
        info!("Downloaded {} ({:?})", remote_path, time.elapsed()?);
        if status_code != 200 {
            return Err(CubeError::user(format!(
                "S3 download returned non OK status: {}",
                status_code
            )));
        }
        Ok(())
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

        Ok(())
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
