use crate::app_metrics;
use crate::di_service;
use crate::remotefs::ExtendedRemoteFs;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::cube_ext;
use futures::stream::BoxStream;
use log::{debug, info};
use regex::{NoExpand, Regex};
use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::env;
use std::fmt;
use std::fmt::Formatter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

pub struct S3RemoteFs {
    dir: PathBuf,
    bucket: arc_swap::ArcSwap<Bucket>,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
    /// When set, the refresh loop watches this file for changes and calls
    /// STS AssumeRoleWithWebIdentity with the JWT inside it.
    web_identity_token_file: Option<String>,
    web_identity_role_arn: Option<String>,
}

impl fmt::Debug for S3RemoteFs {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut s = f.debug_struct("S3RemoteFs");
        s.field("dir", &self.dir).field("sub_path", &self.sub_path);
        let bucket = self.bucket.load();
        // Do not expose AWS credentials.
        s.field("bucket_name", &bucket.name)
            .field("bucket_region", &bucket.region);
        s.finish_non_exhaustive()
    }
}

impl S3RemoteFs {
    pub fn new(
        dir: PathBuf,
        region: String,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        let access_key = env::var("CUBESTORE_AWS_ACCESS_KEY_ID").ok();
        let secret_key = env::var("CUBESTORE_AWS_SECRET_ACCESS_KEY").ok();
        let token_file = env::var("CUBESTORE_AWS_WEB_IDENTITY_TOKEN_FILE").ok();
        let role_arn = env::var("CUBESTORE_AWS_ROLE_ARN").ok();

        let credentials = if let (Some(ref tf), Some(ref arn)) = (&token_file, &role_arn) {
            // Web identity mode: read JWT from file and exchange via STS.
            let jwt = std::fs::read_to_string(tf).map_err(|e| {
                CubeError::internal(format!(
                    "Failed to read web identity token file '{}': {}",
                    tf, e
                ))
            })?;
            info!(
                "Using web identity token file for S3 credentials (role={})",
                arn
            );
            Credentials::from_sts(arn, "cubestore", &jwt).map_err(|e| {
                CubeError::internal(format!("STS AssumeRoleWithWebIdentity failed: {}", e))
            })?
        } else {
            // Static credentials mode (or credential chain fallback).
            Credentials::new(
                access_key.as_deref(),
                secret_key.as_deref(),
                None,
                None,
                None,
            )
            .map_err(|e| CubeError::internal(format!("Failed to create S3 credentials: {}", e)))?
        };

        let region = region.parse::<Region>().map_err(|e| {
            CubeError::internal(format!("Failed to parse Region '{}': {}", region, e))
        })?;
        let bucket = Bucket::new(&bucket_name, region.clone(), credentials)?;
        let fs = Arc::new(Self {
            dir,
            bucket: arc_swap::ArcSwap::new(Arc::new(bucket)),
            sub_path,
            delete_mut: Mutex::new(()),
            web_identity_token_file: token_file,
            web_identity_role_arn: role_arn,
        });
        spawn_creds_refresh_loop(access_key, secret_key, bucket_name, region, &fs);

        Ok(fs)
    }
}

fn spawn_creds_refresh_loop(
    access_key: Option<String>,
    secret_key: Option<String>,
    bucket_name: String,
    region: Region,
    fs: &Arc<S3RemoteFs>,
) {
    let token_file = fs.web_identity_token_file.clone();
    let role_arn = fs.web_identity_role_arn.clone();
    let is_web_identity = token_file.is_some() && role_arn.is_some();

    // Web identity STS credentials expire in ~1 hour, so poll the token file
    // every 30s by default. Static credentials use 3-hour default.
    // CUBESTORE_AWS_CREDS_REFRESH_EVERY_MINS overrides both.
    let refresh_every = {
        let configured = refresh_interval_from_env();
        if is_web_identity && configured == Duration::from_secs(60 * 180) {
            Duration::from_secs(30)
        } else {
            configured
        }
    };

    if refresh_every.as_secs() == 0 {
        return;
    }

    let fs = Arc::downgrade(fs);
    let mut last_modified = token_file
        .as_ref()
        .and_then(|f| std::fs::metadata(f).ok()?.modified().ok());

    std::thread::spawn(move || {
        log::debug!(
            "Started S3 credentials refresh loop (web_identity={})",
            is_web_identity
        );
        loop {
            std::thread::sleep(refresh_every);
            let fs = match fs.upgrade() {
                None => {
                    log::debug!("Stopping S3 credentials refresh loop");
                    return;
                }
                Some(fs) => fs,
            };

            // In web identity mode, only refresh when the token file changed.
            if let (Some(ref file), Some(_)) = (&token_file, &role_arn) {
                let current_modified = std::fs::metadata(file).ok().and_then(|m| m.modified().ok());
                if current_modified == last_modified {
                    continue;
                }
                last_modified = current_modified;
                info!("Web identity token file changed, refreshing S3 credentials");
            }

            let c = if let (Some(ref file), Some(ref arn)) = (&token_file, &role_arn) {
                match std::fs::read_to_string(file) {
                    Ok(jwt) => Credentials::from_sts(arn, "cubestore", &jwt),
                    Err(e) => {
                        log::error!("Failed to read web identity token file: {}", e);
                        continue;
                    }
                }
            } else {
                Credentials::new(
                    access_key.as_deref(),
                    secret_key.as_deref(),
                    None,
                    None,
                    None,
                )
            };

            let c = match c {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to refresh S3 credentials: {}", e);
                    continue;
                }
            };
            let b = match Bucket::new(&bucket_name, region.clone(), c) {
                Ok(b) => b,
                Err(e) => {
                    log::error!("Failed to refresh S3 credentials: {}", e);
                    continue;
                }
            };
            fs.bucket.swap(Arc::new(b));
            log::debug!("Successfully refreshed S3 credentials")
        }
    });
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
        {
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec![
                    "operation:upload_file".to_string(),
                    "driver:s3".to_string(),
                ]),
            );

            let time = SystemTime::now();
            debug!("Uploading {}", remote_path);
            let path = self.s3_path(&remote_path);
            let bucket = self.bucket.load();
            let mut temp_upload_file = File::open(&temp_upload_path).await?;

            let status_code = bucket
                .put_object_stream(&mut temp_upload_file, path)
                .await?;
            if status_code != 200 {
                return Err(CubeError::user(format!(
                    "S3 upload returned non OK status: {}",
                    status_code
                )));
            }

            info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);
        }
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

        let local_file_str = local_file.to_str().unwrap().to_string(); // return value.

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
            let path = self.s3_path(&remote_path);
            let bucket = self.bucket.load();

            let (temp_file, temp_path) =
                cube_ext::spawn_blocking(move || NamedTempFile::new_in(&downloads_dir))
                    .await??
                    .into_parts();

            let mut writter = File::from_std(temp_file);
            let status_code = bucket
                .get_object_stream(path.as_str(), &mut writter)
                .await?;
            if status_code != 200 {
                return Err(CubeError::user(format!(
                    "S3 download returned non OK status: {}",
                    status_code
                )));
            }

            writter.flush().await?;

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
        let path = self.s3_path(&remote_path);
        let bucket = self.bucket.load();

        let res = bucket.delete_object(path).await?;
        if res.status_code() != 204 {
            return Err(CubeError::user(format!(
                "S3 delete returned non OK status: {}",
                res.status_code()
            )));
        }

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
        self.list_objects_and_map(remote_prefix, |o: s3::serde_types::Object| {
            Ok(Self::object_key_to_remote_path(&leading_subpath, &o.key))
        })
        .await
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let leading_subpath = self.leading_subpath_regex();
        self.list_objects_and_map(remote_prefix, |o: s3::serde_types::Object| {
            Ok(RemoteFile {
                remote_path: Self::object_key_to_remote_path(&leading_subpath, &o.key),
                updated: DateTime::parse_from_rfc3339(&o.last_modified)?.with_timezone(&Utc),
                file_size: o.size,
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
        let path = self.s3_path(&remote_prefix);
        let bucket = self.bucket.load();
        let leading_subpath = self.leading_subpath_regex();

        let stream = async_stream::stream! {
            let mut continuation_token = None;
            let mut pages_count: i64 = 0;

            loop {
                let (result, _) = bucket
                    .list_page(path.clone(), None, continuation_token, None, None)
                    .await?;

                pages_count += 1;

                let page: Vec<String> = result.contents.into_iter().map(|obj| Self::object_key_to_remote_path(&leading_subpath, &obj.key)).collect();
                continuation_token = result.next_continuation_token;

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
        F: FnMut(s3::serde_types::Object) -> Result<T, CubeError> + Copy,
    {
        let path = self.s3_path(&remote_prefix);
        let bucket = self.bucket.load();
        let mut mapped_results = Vec::new();
        let mut continuation_token = None;
        let mut pages_count: i64 = 0;

        loop {
            let (result, _) = bucket
                .list_page(path.clone(), None, continuation_token, None, None)
                .await?;

            pages_count += 1;

            for obj in result.contents.into_iter() {
                mapped_results.push(f(obj)?);
            }

            continuation_token = result.next_continuation_token;
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
