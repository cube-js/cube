pub mod s3;

use async_trait::async_trait;
use crate::CubeError;
use chrono::{DateTime, Utc};
use std::path::PathBuf;
use tokio::fs;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RemoteFile {
    remote_path: String,
    updated: DateTime<Utc>
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
pub trait RemoteFs: Send + Sync {
    async fn upload_file(&self, remote_path: &str) -> Result<(), CubeError>;

    async fn download_file(&self, remote_path: &str) -> Result<String, CubeError>;

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError>;

    async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError>;

    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError>;

    async fn local_path(&self) -> String;

    async fn local_file(&self, remote_path: &str) -> Result<String, CubeError>;
}

#[derive(Clone)]
pub struct LocalDirRemoteFs {
    remote_dir: PathBuf,
    dir: PathBuf
}

impl LocalDirRemoteFs {
    pub fn new(remote_dir: PathBuf, dir: PathBuf) -> Arc<LocalDirRemoteFs> {
        Arc::new(LocalDirRemoteFs { remote_dir, dir })
    }

    pub async fn drop_local_path(&self) -> Result<(), CubeError> {
        Ok(fs::remove_dir_all(&self.dir).await?)
    }
}

#[async_trait]
impl RemoteFs for LocalDirRemoteFs {
    async fn upload_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let dest = self.remote_dir.as_path().join(remote_path);
        fs::create_dir_all(dest.parent().unwrap()).await?;
        fs::copy(
            self.dir.as_path().join(remote_path),
            dest.clone()
        ).await?;
        Ok(())
    }

    async fn download_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let local = self.dir.as_path().join(remote_path);
        let path = local.to_str().unwrap().to_owned();
        fs::create_dir_all(local.parent().unwrap()).await?;
        if !local.exists() {
            fs::copy(
                self.remote_dir.as_path().join(remote_path),
                local
            ).await.map_err(
                |e| CubeError::internal(format!("Error during downloading of {}: {}", remote_path, e))
            )?;
        }
        Ok(path)
    }

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        let remote = self.remote_dir.as_path().join(remote_path);
        fs::remove_file(remote.clone()).await?;
        Self::remove_empty_paths(self.remote_dir.clone(), remote.clone()).await?;

        let local = self.dir.as_path().join(remote_path);
        fs::remove_file(local.clone()).await?;
        Self::remove_empty_paths(self.dir.clone(), local.clone()).await?;

        Ok(())
    }

    async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError> {
        Ok(self.list_with_metadata(remote_prefix).await?.into_iter().map(|f| f.remote_path).collect::<Vec<_>>())
    }

    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError> {
        let result = Self::list_recursive(self.remote_dir.clone(), remote_prefix.to_string(), self.remote_dir.clone()).await?;
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
    fn remove_empty_paths_boxed(root: PathBuf, path: PathBuf) -> BoxFuture<'static, Result<(), CubeError>> {
        async move {
            Self::remove_empty_paths(root, path).await
        }.boxed()
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

    fn list_recursive_boxed(remote_dir: PathBuf, remote_prefix: String, dir: PathBuf) -> BoxFuture<'static, Result<Vec<RemoteFile>, CubeError>> {
        async move {
            Self::list_recursive(remote_dir, remote_prefix, dir).await
        }.boxed()
    }

    async fn list_recursive(remote_dir: PathBuf, remote_prefix: String, dir: PathBuf) -> Result<Vec<RemoteFile>, CubeError> {
        let mut result = Vec::new();
        if fs::metadata(dir.clone()).await.is_err() {
            return Ok(vec![]);
        }
        let mut dir = fs::read_dir(dir).await?;
        while let Some(file) = dir.next_entry().await? {
            if file.file_type().await?.is_dir() {
                result.append(&mut Self::list_recursive_boxed(remote_dir.clone(), remote_prefix.to_string(), file.path()).await?);
            } else {
                let relative_name = file.path()
                    .to_str()
                    .unwrap()
                    .to_string()
                    .replace(&remote_dir.to_str().unwrap().to_string(), "")
                    .trim_start_matches("/")
                    .to_string();
                if relative_name.starts_with(&remote_prefix) {
                    result.push(RemoteFile {
                        remote_path: relative_name.to_string(),
                        updated: DateTime::from(file.metadata().await?.modified()?)
                    });
                }
            }
        }
        Ok(result)
    }
}
