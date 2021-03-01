use crate::config::injection::DIService;
use crate::di_service;
use crate::remotefs::{LocalDirRemoteStorage, RemoteFile};
use crate::CubeError;
use async_trait::async_trait;
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};

#[async_trait]
pub trait RemoteStorage: Debug + DIService {
    async fn upload_file(&self, local_path: &str, remote_path: &str) -> Result<(), CubeError>;
    async fn download_file(
        &self,
        local_file_path: &Path,
        local_file: File,
        remote_path: &str,
    ) -> Result<(), CubeError>;
    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError>;
    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError>;
}

pub async fn list_remote(
    s: &dyn RemoteStorage,
    remote_prefix: &str,
) -> Result<Vec<String>, CubeError> {
    Ok(s.list_with_metadata(remote_prefix)
        .await?
        .into_iter()
        .map(|f| f.remote_path)
        .collect::<Vec<_>>())
}

/// Point this into '.cubestore' to avoid creating extra copies of the data. Directory only used to
/// implement `list`, modification operations are nops.
#[derive(Debug)]
pub struct NoopRemoteStorage {
    dir: PathBuf,
}

impl NoopRemoteStorage {
    pub fn new(dir: PathBuf) -> NoopRemoteStorage {
        NoopRemoteStorage { dir }
    }
}

di_service!(NoopRemoteStorage, [RemoteStorage]);

#[async_trait]
impl RemoteStorage for NoopRemoteStorage {
    async fn upload_file(&self, _local_path: &str, _remote_path: &str) -> Result<(), CubeError> {
        Ok(())
    }

    async fn download_file(
        &self,
        _local_file_path: &Path,
        _local_file: File,
        _remote_path: &str,
    ) -> Result<(), CubeError> {
        Ok(())
    }

    async fn delete_file(&self, _remote_path: &str) -> Result<(), CubeError> {
        Ok(())
    }

    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError> {
        let result = LocalDirRemoteStorage::list_recursive(
            self.dir.clone().clone(),
            remote_prefix.to_string(),
            self.dir.clone(),
        )
        .await?;
        Ok(result)
    }
}
