//! Wraps some fs operations to get more detailed logs.
//! The name of the module is deliberately short and different from `fs` to keep clients readable.
use crate::CubeError;
use std::path::Path;

pub async fn create_dir_all(path: impl AsRef<Path>) -> Result<(), CubeError> {
    tokio::fs::create_dir_all(path.as_ref()).await.map_err(|e| {
        CubeError::internal(format!("create_dir_all({:?}) failed: {}", path.as_ref(), e))
    })
}

pub async fn copy(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<u64, CubeError> {
    tokio::fs::copy(from.as_ref(), to.as_ref())
        .await
        .map_err(|e| {
            CubeError::internal(format!(
                "copy({:?}, {:?}) failed: {}",
                from.as_ref(),
                to.as_ref(),
                e
            ))
        })
}

pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<(), CubeError> {
    tokio::fs::rename(from.as_ref(), to.as_ref())
        .await
        .map_err(|e| {
            CubeError::internal(format!(
                "rename({:?}, {:?}) failed: {}",
                from.as_ref(),
                to.as_ref(),
                e
            ))
        })
}

pub async fn remove_file(path: impl AsRef<Path>) -> Result<(), CubeError> {
    tokio::fs::remove_file(path.as_ref())
        .await
        .map_err(|e| CubeError::internal(format!("remove_file({:?}) failed: {}", path.as_ref(), e)))
}

pub async fn remove_dir(path: impl AsRef<Path>) -> Result<(), CubeError> {
    tokio::fs::remove_dir(path.as_ref())
        .await
        .map_err(|e| CubeError::internal(format!("remove_dir({:?}) failed: {}", path.as_ref(), e)))
}
