use crate::app_metrics;
use crate::di_service;
use crate::remotefs::ExtendedRemoteFs;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_s3::{Client, Config};
use chrono::{DateTime, Utc};
use datafusion::cube_ext;
use futures::stream::BoxStream;
use log::{debug, info};
use regex::{NoExpand, Regex};
use std::env;
use std::fmt;
use std::fmt::Formatter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

pub struct S3RemoteFs {
    dir: PathBuf,
    client: Client,
    bucket_name: String,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
}

impl fmt::Debug for S3RemoteFs {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("S3RemoteFs")
            .field("dir", &self.dir)
            .field("bucket_name", &self.bucket_name)
            .field("sub_path", &self.sub_path)
            .finish_non_exhaustive()
    }
}

impl S3RemoteFs {
    pub async fn new(
        dir: PathBuf,
        region: String,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        let region = Region::new(region);
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region)
            .load()
            .await;
        let s3_config = Config::builder().region(config.region().cloned()).build();
        let client = Client::from_conf(s3_config);

        Ok(Arc::new(Self {
            dir,
            client,
            bucket_name,
            sub_path,
            delete_mut: Mutex::new(()),
        }))
    }
}


fn refresh_interval_from_env() -> Duration {
    let mut mins = 180; // 3 hours by default.
    if let Ok(s) = std::env::var("CUBESTORE_AWS_CREDS_REFRESH_EVERY_MINS") {
        match s.parse::<u64>() {
            Ok(i) => mins = i,
            Err(e) => log::error!("Could not parse CUBESTORE_AWS_CREDS_REFRESH_EVERY_MINS. Refreshing every {} minutes. Error: {}", mins, e),
        };
    };
    Duration::from_secs(60 * mins)
}

di_service!(S3RemoteFs, [RemoteFs, ExtendedRemoteFs]);

#[async_trait]
impl RemoteFs for S3RemoteFs {
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
                "driver:s3".to_string(),
            ]),
        );

        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let s3_key = self.s3_path(&remote_path);
        
        let mut file = File::open(&temp_upload_path).await?;
        let mut body = Vec::new();
        file.read_to_end(&mut body).await?;
        
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&s3_key)
            .body(body.into())
            .send()
            .await
            .map_err(|err| {
                CubeError::user(format!("S3 upload failed: {}", err))
            })?;

        info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);

        let size = fs::metadata(&temp_upload_path).await?.len();
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
        Ok(fs::metadata(local_path).await?.len())
    }

    async fn download_file(
        &self,
        remote_path: String,
        _expected_file_size: Option<u64>,
    ) -> Result<String, CubeError> {
        let local_file = self.dir.as_path().join(&remote_path);
        let local_dir = local_file.parent().unwrap();
        let downloads_dir = local_dir.join("downloads");

        let local_file_str = local_file.to_str().unwrap().to_string();

        fs::create_dir_all(&downloads_dir).await?;
        if !local_file.exists() {
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec![
                    "operation:download_file".to_string(),
                    "driver:s3".to_string(),
                ]),
            );
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);
            let s3_key = self.s3_path(&remote_path);

            let (temp_file, temp_path) =
                cube_ext::spawn_blocking(move || NamedTempFile::new_in(&downloads_dir))
                    .await??
                    .into_parts();

            let response = self.client
                .get_object()
                .bucket(&self.bucket_name)
                .key(&s3_key)
                .send()
                .await
                .map_err(|err| {
                    CubeError::user(format!("S3 download failed: {}", err))
                })?;

            let body_bytes = response.body.collect().await?;
            let mut file = File::from_std(temp_file);
            file.write_all(&body_bytes.into_bytes()).await?;
            file.flush().await?;

            cube_ext::spawn_blocking(move || -> Result<(), PathPersistError> {
                temp_path.persist(&local_file)
            })
            .await??;

            info!("Downloaded {} ({:?})", remote_path, time.elapsed()?);
        }

        Ok(local_file_str)
    }

    async fn delete_file(&self, remote_path: String) -> Result<(), CubeError> {
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            1,
            Some(&vec![
                "operation:delete_file".to_string(),
                "driver:s3".to_string(),
            ]),
        );
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);
        let s3_key = self.s3_path(&remote_path);

        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(&s3_key)
            .send()
            .await
            .map_err(|err| {
                CubeError::user(format!("S3 delete failed: {}", err))
            })?;

        let _guard = acquire_lock("delete file", self.delete_mut.lock()).await?;
        let local = self.dir.as_path().join(&remote_path);
        if fs::metadata(local.clone()).await.is_ok() {
            fs::remove_file(local.clone()).await?;
            LocalDirRemoteFs::remove_empty_paths(self.dir.as_path().to_path_buf(), local.clone())
                .await?;
        }

        info!("Deleted {} ({:?})", remote_path, time.elapsed()?);
        Ok(())
    }

    async fn list(&self, remote_prefix: String) -> Result<Vec<String>, CubeError> {
        let leading_subpath = self.leading_subpath_regex();
        self.list_objects_and_map(remote_prefix, |key: String| {
            Ok(Self::object_key_to_remote_path(&leading_subpath, &key))
        })
        .await
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let leading_subpath = self.leading_subpath_regex();
        self.list_objects_and_map(remote_prefix, |obj: (String, i64, String)| {
            let (key, size, last_modified) = obj;
            Ok(RemoteFile {
                remote_path: Self::object_key_to_remote_path(&leading_subpath, &key),
                updated: DateTime::parse_from_rfc3339(&last_modified)?.with_timezone(&Utc),
                file_size: size,
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

#[async_trait]
impl ExtendedRemoteFs for S3RemoteFs {
    async fn list_by_page(
        &self,
        remote_prefix: String,
    ) -> Result<BoxStream<'static, Result<Vec<String>, CubeError>>, CubeError> {
        let prefix = self.s3_path(&remote_prefix);
        let client = self.client.clone();
        let bucket = self.bucket_name.clone();
        let leading_subpath = self.leading_subpath_regex();

        let stream = async_stream::stream! {
            let mut continuation_token = None;
            let mut pages_count: i64 = 0;

            loop {
                let mut list_req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                if let Some(token) = continuation_token.take() {
                    list_req = list_req.continuation_token(token);
                }

                let response = match list_req.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        yield Err(CubeError::user(format!("S3 list failed: {}", e)));
                        break;
                    }
                };

                pages_count += 1;

                let page: Vec<String> = response
                    .contents()
                    .unwrap_or_default()
                    .iter()
                    .map(|obj| Self::object_key_to_remote_path(&leading_subpath, &obj.key().unwrap_or("")))
                    .collect();
                
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
                yield Ok(page);

                if continuation_token.is_none() {
                    break;
                }
            }

            Self::pages_count_app_metrics_and_logging(pages_count, "streaming");
        };

        Ok(Box::pin(stream))
    }
}

struct LeadingSubpath(Regex);

impl S3RemoteFs {
    fn leading_subpath_regex(&self) -> LeadingSubpath {
        LeadingSubpath(Regex::new(format!("^{}", self.s3_path("")).as_str()).unwrap())
    }

    fn object_key_to_remote_path(leading_subpath: &LeadingSubpath, o_key: &String) -> String {
        leading_subpath.0.replace(o_key, NoExpand("")).to_string()
    }

    async fn list_objects_and_map<T, F>(
        &self,
        remote_prefix: String,
        mut f: F,
    ) -> Result<Vec<T>, CubeError>
    where
        F: FnMut((String, i64, String)) -> Result<T, CubeError> + Copy,
    {
        let prefix = self.s3_path(&remote_prefix);
        let mut mapped_results = Vec::new();
        let mut continuation_token = None;
        let mut pages_count: i64 = 0;

        loop {
            let mut list_req = self.client.list_objects_v2().bucket(&self.bucket_name).prefix(&prefix);
            if let Some(token) = continuation_token.take() {
                list_req = list_req.continuation_token(token);
            }

            let response = list_req.send().await.map_err(|err| {
                CubeError::user(format!("S3 list failed: {}", err))
            })?;

            pages_count += 1;

            for obj in response.contents().unwrap_or_default().iter() {
                let key = obj.key().unwrap_or("").to_string();
                let size = obj.size().unwrap_or(0);
                let last_modified = obj.last_modified().map(|dt| dt.to_chrono_datetime().to_rfc3339()).unwrap_or_else(|| Utc::now().to_rfc3339());
                mapped_results.push(f((key, size, last_modified))?);
            }

            continuation_token = response.next_continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }

        Self::pages_count_app_metrics_and_logging(pages_count, "non-streaming");

        Ok(mapped_results)
    }

    fn pages_count_app_metrics_and_logging(pages_count: i64, log_op: &str) {
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            pages_count as i64,
            Some(&vec!["operation:list".to_string(), "driver:s3".to_string()]),
        );
        if pages_count > 100 {
            // Probably only "S3 list (non-streaming)" messages are of concern, not "S3 list (streaming)".
            log::warn!(
                "S3 list ({}) returned more than 100 pages: {}",
                log_op,
                pages_count
            );
        }
    }

    fn s3_path(&self, remote_path: &str) -> String {
        format!(
            "{}{}",
            self.sub_path
                .as_ref()
                .map(|p| format!("{}/", p.to_string()))
                .unwrap_or_else(|| "".to_string()),
            remote_path
        )
    }
}
