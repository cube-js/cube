pub mod gcs;
pub mod queue;
pub mod s3;

use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::FutureExt;
use log::debug;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
pub struct RemoteFile {
    remote_path: String,
    updated: DateTime<Utc>,
}

impl RemoteFile {
    pub fn remote_path(&self) -> &str {
        self.remote_path.as_str()
    }

    pub fn updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

#[async_trait]
pub trait RemoteFs: Send + Sync + Debug {
    /// Use this path to prepare files for upload. Writing into `local_path()` directly can result
    /// in files being deleted by the background cleanup process, see `QueueRemoteFs::cleanup_loop`.
    async fn temp_upload_path(&self, remote_path: &str) -> Result<String, CubeError> {
        // Putting files into a subdirectory prevents cleanups from removing them.
        self.local_file(&format!("uploads/{}", remote_path)).await
    }

    /// In addition to uploading this file to the remote filesystem, this function moves the file
    /// from `temp_upload_path` to `self.local_path(remote_path)` on the local file system.
    async fn upload_file(&self, temp_upload_path: &str, remote_path: &str)
        -> Result<(), CubeError>;

    async fn download_file(&self, remote_path: &str) -> Result<String, CubeError>;

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError>;

    /// Deletes the local copy of `remote_path`, if the latter exists.
    /// If no file was downloaded, nothing happens.
    async fn delete_local_copy(&self, remote_path: &str) -> Result<(), CubeError>;

    async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError>;

    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError>;

    async fn local_path(&self) -> String;

    async fn local_file(&self, remote_path: &str) -> Result<String, CubeError>;
}

#[derive(Debug)]
pub struct LocalDirRemoteFs {
    remote_dir_for_debug: PathBuf,
    remote_dir: RwLock<PathBuf>,
    dir: PathBuf,
    dir_delete_mut: Mutex<()>,
}

impl LocalDirRemoteFs {
    pub fn new(remote_dir: PathBuf, dir: PathBuf) -> Arc<LocalDirRemoteFs> {
        Arc::new(LocalDirRemoteFs {
            remote_dir_for_debug: remote_dir.clone(),
            remote_dir: RwLock::new(remote_dir),
            dir,
            dir_delete_mut: Mutex::new(()),
        })
    }

    pub async fn drop_local_path(&self) -> Result<(), CubeError> {
        Ok(fs::remove_dir_all(&*self.dir).await?)
    }
}

#[async_trait]
impl RemoteFs for LocalDirRemoteFs {
    async fn upload_file(
        &self,
        temp_upload_path: &str,
        remote_path: &str,
    ) -> Result<(), CubeError> {
        debug!("Uploading {}", remote_path);
        let remote_dir = self.remote_dir.write().await;
        let dest = remote_dir.as_path().join(remote_path);
        fs::create_dir_all(dest.parent().unwrap()).await?;
        fs::copy(&temp_upload_path, dest.clone()).await?;
        let local_path = self.dir.as_path().join(remote_path);
        if Path::new(temp_upload_path) != local_path {
            fs::rename(&temp_upload_path, local_path).await?;
        }
        Ok(())
    }

    async fn download_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let local_file = self.dir.as_path().join(remote_path);
        let local_dir = local_file.parent().unwrap();
        fs::create_dir_all(local_dir).await?;
        if !local_file.exists() {
            debug!("Downloading {}", remote_path);
            let remote_dir = self.remote_dir.read().await;
            let temp_path = NamedTempFile::new_in(local_dir)?.into_temp_path();
            fs::copy(remote_dir.as_path().join(remote_path), &temp_path)
                .await
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Error during downloading of {}: {}",
                        remote_path, e
                    ))
                })?;
            temp_path.persist(&local_file)?;
        }
        Ok(local_file.into_os_string().into_string().unwrap())
    }

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        debug!("Deleting {}", remote_path);
        {
            let remote_dir = self.remote_dir.write().await;
            let remote = remote_dir.as_path().join(remote_path);
            if fs::metadata(remote.clone()).await.is_ok() {
                fs::remove_file(remote.clone()).await?;
                Self::remove_empty_paths(remote_dir.clone(), remote.clone()).await?;
            }
        }

        self.delete_local_copy(remote_path).await
    }

    async fn delete_local_copy(&self, remote_path: &str) -> Result<(), CubeError> {
        let _guard = self.dir_delete_mut.lock().await;
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
        let remote_dir = self.remote_dir.read().await;
        let result = Self::list_recursive(
            remote_dir.clone(),
            remote_prefix.to_string(),
            remote_dir.clone(),
        )
        .await?;
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

impl LocalDirRemoteFs {
    fn remove_empty_paths_boxed(
        root: PathBuf,
        path: PathBuf,
    ) -> BoxFuture<'static, Result<(), CubeError>> {
        async move { Self::remove_empty_paths(root, path).await }.boxed()
    }

    async fn remove_empty_paths(root: PathBuf, path: PathBuf) -> Result<(), CubeError> {
        if let Some(parent_path) = path.parent() {
            let mut dir = fs::read_dir(parent_path).await?;
            if dir.next_entry().await?.is_none() {
                fs::remove_dir(parent_path).await?;
            }
            if root != parent_path.to_path_buf() {
                return Ok(Self::remove_empty_paths_boxed(root, parent_path.to_path_buf()).await?);
            }
        }
        Ok(())
    }

    fn list_recursive_boxed(
        remote_dir: PathBuf,
        remote_prefix: String,
        dir: PathBuf,
    ) -> BoxFuture<'static, Result<Vec<RemoteFile>, CubeError>> {
        async move { Self::list_recursive(remote_dir, remote_prefix, dir).await }.boxed()
    }

    pub async fn list_recursive(
        remote_dir: PathBuf,
        remote_prefix: String,
        dir: PathBuf,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let mut result = Vec::new();
        if fs::metadata(dir.clone()).await.is_err() {
            return Ok(vec![]);
        }
        let mut dir = fs::read_dir(dir).await?;
        while let Some(file) = dir.next_entry().await? {
            if file.file_type().await?.is_dir() {
                result.append(
                    &mut Self::list_recursive_boxed(
                        remote_dir.clone(),
                        remote_prefix.to_string(),
                        file.path(),
                    )
                    .await?,
                );
            } else {
                let relative_name = file
                    .path()
                    .to_str()
                    .unwrap()
                    .to_string()
                    .replace(&remote_dir.to_str().unwrap().to_string(), "")
                    .trim_start_matches("/")
                    .to_string();
                if relative_name.starts_with(&remote_prefix) {
                    result.push(RemoteFile {
                        remote_path: relative_name.to_string(),
                        updated: DateTime::from(file.metadata().await?.modified()?),
                    });
                }
            }
        }
        Ok(result)
    }
}
