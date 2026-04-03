use crate::app_metrics;
use crate::di_service;
use crate::remotefs::ExtendedRemoteFs;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use datafusion::cube_ext;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::{debug, info};
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::{ObjectStore, PutPayload};
use std::env;
use std::fmt;
use std::fmt::Formatter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

pub struct AzureBlobRemoteFs {
    dir: PathBuf,
    store: Arc<MicrosoftAzure>,
    container: String,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
}

impl fmt::Debug for AzureBlobRemoteFs {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut s = f.debug_struct("AzureBlobRemoteFs");
        s.field("dir", &self.dir)
            .field("container", &self.container)
            .field("sub_path", &self.sub_path);
        s.finish_non_exhaustive()
    }
}

impl AzureBlobRemoteFs {
    pub fn new(
        dir: PathBuf,
        account: String,
        container: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(&account)
            .with_container_name(&container);

        if let Ok(endpoint) = env::var("CUBESTORE_AZURE_ENDPOINT") {
            builder = builder.with_endpoint(endpoint);
        }

        if let Ok(access_key) = env::var("CUBESTORE_AZURE_ACCESS_KEY") {
            builder = builder.with_access_key(access_key);
        } else if let Ok(sas_token) = env::var("CUBESTORE_AZURE_SAS_TOKEN") {
            // Parse SAS token query string into key-value pairs
            let query_pairs: Vec<(String, String)> = sas_token
                .trim_start_matches('?')
                .split('&')
                .filter_map(|pair| {
                    let mut parts = pair.splitn(2, '=');
                    match (parts.next(), parts.next()) {
                        (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                        _ => None,
                    }
                })
                .collect();
            builder = builder.with_sas_authorization(query_pairs);
        } else if let (Ok(client_id), Ok(tenant_id), Ok(token_file)) = (
            env::var("AZURE_CLIENT_ID"),
            env::var("AZURE_TENANT_ID"),
            env::var("AZURE_FEDERATED_TOKEN_FILE"),
        ) {
            // Workload identity: explicitly pass federated token credentials
            builder = builder
                .with_client_id(client_id)
                .with_tenant_id(tenant_id)
                .with_federated_token_file(token_file);
        }

        let store = builder.build().map_err(|e| {
            CubeError::internal(format!("Failed to create Azure Blob Storage client: {}", e))
        })?;

        Ok(Arc::new(Self {
            dir,
            store: Arc::new(store),
            container,
            sub_path,
            delete_mut: Mutex::new(()),
        }))
    }

    fn azure_path(&self, remote_path: &str) -> object_store::path::Path {
        let full = match &self.sub_path {
            Some(p) => format!("{}/{}", p, remote_path),
            None => remote_path.to_string(),
        };
        object_store::path::Path::from(full)
    }

    fn strip_sub_path(&self, key: &str) -> String {
        match &self.sub_path {
            Some(p) => {
                let prefix = format!("{}/", p);
                key.strip_prefix(&prefix).unwrap_or(key).to_string()
            }
            None => key.to_string(),
        }
    }
}

di_service!(AzureBlobRemoteFs, [RemoteFs, ExtendedRemoteFs]);

#[async_trait]
impl RemoteFs for AzureBlobRemoteFs {
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
        // Use head() instead of list() because the object_store crate's
        // list(Some(&exact_path)) returns empty on Azure Blob Storage.
        let path = self.azure_path(&remote_path);
        let meta = self.store.head(&path).await.map_err(|e| {
            CubeError::internal(format!(
                "File {} can't be found after upload: {}",
                remote_path, e
            ))
        })?;
        if meta.size as u64 != expected_size {
            return Err(CubeError::internal(format!(
                "File sizes for {} don't match after upload. Expected {} but got {}",
                remote_path, expected_size, meta.size
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
                "driver:azure_blob".to_string(),
            ]),
        );
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);

        let path = self.azure_path(&remote_path);
        let data = fs::read(&temp_upload_path).await?;
        let payload = PutPayload::from(data);

        self.store.put(&path, payload).await.map_err(|e| {
            CubeError::internal(format!("Azure Blob upload error for {}: {}", remote_path, e))
        })?;

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
        info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);
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
                    "driver:azure_blob".to_string(),
                ]),
            );
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);

            let path = self.azure_path(&remote_path);
            let result = self.store.get(&path).await.map_err(|e| {
                CubeError::internal(format!(
                    "Azure Blob download error for {}: {}",
                    remote_path, e
                ))
            })?;
            let bytes = result.bytes().await.map_err(|e| {
                CubeError::internal(format!(
                    "Azure Blob read bytes error for {}: {}",
                    remote_path, e
                ))
            })?;

            let (temp_file, temp_path) =
                cube_ext::spawn_blocking(move || NamedTempFile::new_in(&downloads_dir))
                    .await??
                    .into_parts();

            let mut writer = File::from_std(temp_file);
            writer.write_all(&bytes).await?;
            writer.flush().await?;

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
                "driver:azure_blob".to_string(),
            ]),
        );
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);

        let path = self.azure_path(&remote_path);
        self.store.delete(&path).await.map_err(|e| {
            CubeError::internal(format!("Azure Blob delete error for {}: {}", remote_path, e))
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
        let prefix = self.azure_path(&remote_prefix);
        let mut result = Vec::new();
        let mut pages_count: i64 = 0;
        let mut stream = self.store.list(Some(&prefix));
        let mut page_items = 0;

        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| {
                CubeError::internal(format!("Azure Blob list error: {}", e))
            })?;
            result.push(self.strip_sub_path(&meta.location.to_string()));
            page_items += 1;
            if page_items >= 1000 {
                pages_count += 1;
                page_items = 0;
            }
        }
        if page_items > 0 {
            pages_count += 1;
        }

        if pages_count > 100 {
            log::warn!(
                "Azure Blob list returned more than 100 pages: {}",
                pages_count
            );
        }
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            pages_count,
            Some(&vec![
                "operation:list".to_string(),
                "driver:azure_blob".to_string(),
            ]),
        );

        Ok(result)
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let prefix = self.azure_path(&remote_prefix);
        let mut result = Vec::new();
        let mut stream = self.store.list(Some(&prefix));

        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| {
                CubeError::internal(format!("Azure Blob list error: {}", e))
            })?;
            result.push(RemoteFile {
                remote_path: self.strip_sub_path(&meta.location.to_string()),
                updated: meta.last_modified,
                file_size: meta.size as u64,
            });
        }

        Ok(result)
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
impl ExtendedRemoteFs for AzureBlobRemoteFs {
    async fn list_by_page(
        &self,
        remote_prefix: String,
    ) -> Result<BoxStream<'static, Result<Vec<String>, CubeError>>, CubeError> {
        let prefix = self.azure_path(&remote_prefix);
        let store = self.store.clone();
        let sub_path = self.sub_path.clone();

        let stream = async_stream::stream! {
            let mut object_stream = store.list(Some(&prefix));
            let mut page = Vec::new();
            let page_size = 1000;

            while let Some(result) = object_stream.next().await {
                match result {
                    Ok(meta) => {
                        let key = meta.location.to_string();
                        let remote_path = match &sub_path {
                            Some(p) => {
                                let pfx = format!("{}/", p);
                                key.strip_prefix(&pfx).unwrap_or(&key).to_string()
                            }
                            None => key,
                        };
                        page.push(remote_path);
                        if page.len() >= page_size {
                            yield Ok(std::mem::take(&mut page));
                        }
                    }
                    Err(e) => {
                        yield Err(CubeError::internal(format!("Azure Blob list error: {}", e)));
                        return;
                    }
                }
            }
            if !page.is_empty() {
                yield Ok(page);
            }
        };

        Ok(Box::pin(stream))
    }
}
