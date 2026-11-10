use super::gcs_client::{GcsClient, GcsObject};
use crate::app_metrics;
use crate::di_service;
use crate::remotefs::ExtendedRemoteFs;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use datafusion::cube_ext;
use futures::StreamExt;
use log::{debug, info};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct GCSRemoteFs {
    dir: PathBuf,
    client: GcsClient,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
}

impl GCSRemoteFs {
    pub fn new(
        dir: PathBuf,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        Ok(Arc::new(Self {
            dir,
            client: GcsClient::new(&bucket_name)?,
            sub_path,
            delete_mut: Mutex::new(()),
        }))
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
        let actual_size = self
            .client
            .metadata(&self.gcs_path(&remote_path))
            .await?
            .size()?;
        if actual_size != expected_size {
            return Err(CubeError::internal(format!(
                "GCS upload size mismatch: expected {}, got {}",
                expected_size, actual_size
            )));
        }
        Ok(())
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
        let size = file.metadata().await?.len();
        self.client
            .upload(&self.gcs_path(&remote_path), file, size)
            .await?;

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
            let response = self.client.download(&self.gcs_path(&remote_path)).await?;
            let mut stream = response.bytes_stream();
            let mut c = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                writer.write_all(&chunk).await?;
                c += chunk.len();
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
        self.client.delete(&self.gcs_path(&remote_path)).await?;
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

    async fn list(&self, remote_prefix: String) -> Result<Vec<String>, CubeError> {
        Ok(self
            .list_with_metadata(remote_prefix)
            .await?
            .into_iter()
            .map(|f| f.remote_path)
            .collect())
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let root = self.gcs_path("");
        self.list_with_metadata_and_map(remote_prefix, |obj: GcsObject| {
            let size = obj.size()?;
            let remote_path = obj
                .name
                .strip_prefix(&root)
                .ok_or_else(|| {
                    CubeError::internal(
                        "GCS listing returned an object outside the configured prefix".to_string(),
                    )
                })?
                .to_owned();
            Ok(RemoteFile {
                remote_path,
                updated: obj.updated,
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

// TODO: Make a faster implementation
#[async_trait]
impl ExtendedRemoteFs for GCSRemoteFs {}

impl GCSRemoteFs {
    async fn list_with_metadata_and_map<T>(
        &self,
        remote_prefix: String,
        mut f: impl FnMut(GcsObject) -> Result<T, CubeError>,
    ) -> Result<Vec<T>, CubeError> {
        let prefix = self.gcs_path(&remote_prefix);
        let mut token = None;
        let mut result = Vec::new();
        loop {
            let page = self.client.list_page(&prefix, token.as_deref()).await?;
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec![
                    "operation:list".to_string(),
                    "driver:gcs".to_string(),
                ]),
            );
            for object in page.items {
                result.push(f(object)?);
            }
            match page.next_page_token.filter(|t| !t.is_empty()) {
                Some(next) if token.as_ref() != Some(&next) => token = Some(next),
                Some(_) => {
                    return Err(CubeError::internal(
                        "GCS listing repeated a page token".to_string(),
                    ))
                }
                None => break,
            }
        }
        Ok(result)
    }

    fn gcs_path(&self, remote_path: &str) -> String {
        // Preserve the existing on-disk namespace, including the leading slash
        // when no subpath is configured. Do not normalize GCS object names.
        gcs_object_name(self.sub_path.as_deref(), remote_path)
    }
}

fn gcs_object_name(sub_path: Option<&str>, remote_path: &str) -> String {
    format!("{}/{}", sub_path.unwrap_or(""), remote_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_existing_object_namespace() {
        for (subpath, expected) in [
            (None, "/metastore-"),
            (Some("tenant.a"), "tenant.a/metastore-"),
            (Some("tenant/"), "tenant//metastore-"),
        ] {
            assert_eq!(gcs_object_name(subpath, "metastore-"), expected);
        }
    }
}
