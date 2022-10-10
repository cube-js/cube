use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::{RocksMetaStore, WriteBatchContainer};
use crate::remotefs::RemoteFs;
use crate::CubeError;
use async_trait::async_trait;
use datafusion::cube_ext;
use futures::future::join_all;
use log::{error, info, trace};
use regex::Regex;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs;
use tokio::fs::File;

#[async_trait]
pub trait MetaStoreFs: DIService + Send + Sync {
    async fn load_from_remote(
        self: Arc<Self>,
        path: &str,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<RocksMetaStore>, CubeError>;
    async fn upload_log(
        &self,
        log_name: &str,
        serializer: &WriteBatchContainer,
    ) -> Result<u64, CubeError>;
    async fn upload_checkpoint(
        &self,
        remote_path: String,
        checkpoint_path: PathBuf,
    ) -> Result<(), CubeError>;
}

#[derive(Clone)]
pub struct RocksMetaStoreFs {
    remote_fs: Arc<dyn RemoteFs>,
}

impl RocksMetaStoreFs {
    pub fn new(remote_fs: Arc<dyn RemoteFs>) -> Arc<Self> {
        Arc::new(Self { remote_fs })
    }
}

#[async_trait]
impl MetaStoreFs for RocksMetaStoreFs {
    async fn load_from_remote(
        self: Arc<Self>,
        path: &str,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<RocksMetaStore>, CubeError> {
        if !fs::metadata(path).await.is_ok() {
            if self.is_remote_metadata_exists().await? {
                let last_metastore_snapshot = self.load_current_snapshot_id().await?;

                if let Some(snapshot) = last_metastore_snapshot {
                    let to_load = self.files_to_load(snapshot).await?;
                    let meta_store_path = self.make_local_metastore_dir().await?;
                    for (file, _) in to_load.iter() {
                        // TODO check file size
                        self.remote_fs.download_file(file, None).await?;
                        let local = self.remote_fs.local_file(file).await?;
                        let path = Path::new(&local);
                        fs::copy(
                            path,
                            PathBuf::from(&meta_store_path)
                                .join(path.file_name().unwrap().to_str().unwrap()),
                        )
                        .await?;
                    }

                    return self
                        .check_meta_store(
                            RocksMetaStore::new(path, self.clone(), config),
                            Some(snapshot),
                        )
                        .await;
                }
            } else {
                trace!("Can't find metastore-current in {:?}", self.remote_fs);
            }
            info!("Creating metastore from scratch in {}", path);
        } else {
            info!("Using existing metastore in {}", path);
        }

        return self
            .check_meta_store(RocksMetaStore::new(path, self.clone(), config), None)
            .await;
    }

    async fn upload_log(
        &self,
        log_name: &str,
        serializer: &WriteBatchContainer,
    ) -> Result<u64, CubeError> {
        let file_name = self.remote_fs.local_file(log_name).await?;
        serializer.write_to_file(&file_name).await?;
        // TODO persist file size
        self.remote_fs.upload_file(&file_name, &log_name).await
    }
    async fn upload_checkpoint(
        &self,
        remote_path: String,
        checkpoint_path: PathBuf,
    ) -> Result<(), CubeError> {
        self.upload_snapsots_files(&remote_path, &checkpoint_path)
            .await?;

        self.delete_old_snapshots().await?;

        self.write_metastore_current(&remote_path).await?;

        Ok(())
    }
}

impl RocksMetaStoreFs {
    pub fn remote_fs(&self) -> Arc<dyn RemoteFs> {
        self.remote_fs.clone()
    }

    pub async fn upload_snapsots_files(
        &self,
        remote_path: &str,
        checkpoint_path: &PathBuf,
    ) -> Result<Vec<(String, u64)>, CubeError> {
        let mut dir = fs::read_dir(checkpoint_path).await?;

        let mut files_to_upload = Vec::new();
        while let Some(file) = dir.next_entry().await? {
            let file = file.file_name();
            files_to_upload.push(format!("{}/{}", remote_path, file.to_string_lossy()));
        }
        let upload_results = join_all(
            files_to_upload
                .into_iter()
                .map(|f| {
                    let remote_fs = self.remote_fs.clone();
                    return async move {
                        let local = remote_fs.local_file(&f).await?;
                        // TODO persist file size
                        Ok::<_, CubeError>((f.clone(), remote_fs.upload_file(&local, &f).await?))
                    };
                })
                .collect::<Vec<_>>(),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        Ok(upload_results)
    }
    pub async fn delete_old_snapshots(&self) -> Result<Vec<String>, CubeError> {
        let existing_metastore_files = self.remote_fs.list("metastore-").await?;
        let to_delete = existing_metastore_files
            .into_iter()
            .filter_map(|existing| {
                let path = existing
                    .split("/")
                    .nth(0)
                    .map(|p| u128::from_str(&p.replace("metastore-", "").replace("-logs", "")));
                if let Some(Ok(millis)) = path {
                    if SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        - millis
                        > 15 * 60 * 1000
                    {
                        return Some(existing);
                    }
                }
                None
            })
            .collect::<Vec<_>>();
        for v in join_all(
            to_delete
                .iter()
                .map(|f| self.remote_fs.delete_file(&f))
                .collect::<Vec<_>>(),
        )
        .await
        .into_iter()
        {
            v?;
        }
        Ok(to_delete)
    }
    pub async fn write_metastore_current(&self, remote_path: &str) -> Result<(), CubeError> {
        let uploads_dir = self.remote_fs.uploads_dir().await?;
        let (file, file_path) = cube_ext::spawn_blocking(move || {
            tempfile::Builder::new()
                .prefix("metastore-current")
                .tempfile_in(uploads_dir)
        })
        .await??
        .into_parts();

        tokio::io::AsyncWriteExt::write_all(&mut fs::File::from_std(file), remote_path.as_bytes())
            .await?;

        self.remote_fs
            .upload_file(file_path.keep()?.to_str().unwrap(), "metastore-current")
            .await?;
        Ok(())
    }
    pub async fn is_remote_metadata_exists(&self) -> Result<bool, CubeError> {
        let res = self.remote_fs.list("metastore-current").await?.len() > 0;
        Ok(res)
    }
    pub async fn load_current_snapshot_id(&self) -> Result<Option<u128>, CubeError> {
        if self.remote_fs.list("metastore-current").await?.len() == 0 {
            return Ok(None);
        }
        let re = Regex::new(r"^metastore-(\d+)").unwrap();
        info!("Downloading remote metastore");
        let current_metastore_file = self.remote_fs.local_file("metastore-current").await?;
        if fs::metadata(current_metastore_file.as_str()).await.is_ok() {
            fs::remove_file(current_metastore_file.as_str()).await?;
        }
        self.remote_fs
            .download_file("metastore-current", None)
            .await?;

        let mut file = File::open(current_metastore_file.as_str()).await?;
        let mut buffer = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut file, &mut buffer).await?;
        let last_metastore_snapshot = {
            let parse_result = re
                .captures(&String::from_utf8(buffer)?)
                .map(|c| c.get(1).unwrap().as_str())
                .map(|p| u128::from_str(p));
            if let Some(Ok(millis)) = parse_result {
                Some(millis)
            } else {
                None
            }
        };
        Ok(last_metastore_snapshot)
    }
    pub async fn load_metastore_logs(
        &self,
        snapshot: u128,
        meta_store: &Arc<RocksMetaStore>,
    ) -> Result<(), CubeError> {
        let logs_to_batch = self
            .remote_fs
            .list(&format!("metastore-{}-logs", snapshot))
            .await?;
        for log_file in logs_to_batch.iter() {
            let path_to_log = self.remote_fs.local_file(log_file).await?;
            let batch = WriteBatchContainer::read_from_file(&path_to_log).await;
            if let Ok(batch) = batch {
                let db = meta_store.db.clone();
                db.write(batch.write_batch())?;
            } else if let Err(e) = batch {
                error!(
                    "Corrupted metastore WAL file. Discarding: {:?} {}",
                    log_file, e
                );
                break;
            }
        }
        Ok(())
    }
    pub async fn check_meta_store(
        self: &Arc<Self>,
        meta_store: Arc<RocksMetaStore>,
        snapshot: Option<u128>,
    ) -> Result<Arc<RocksMetaStore>, CubeError> {
        if let Some(snapshot) = snapshot {
            self.load_metastore_logs(snapshot, &meta_store).await?;
        }

        RocksMetaStore::migrate(&meta_store).await?;

        Ok(meta_store)
    }

    pub async fn files_to_load(&self, snapshot: u128) -> Result<Vec<(String, u64)>, CubeError> {
        let res = self
            .remote_fs
            .list_with_metadata(&format!("metastore-{}", snapshot))
            .await?
            .into_iter()
            .map(|f| (f.remote_path, f.file_size))
            .collect::<Vec<_>>();
        Ok(res)
    }
    pub async fn make_local_metastore_dir(&self) -> Result<String, CubeError> {
        let meta_store_path = self.remote_fs.local_file("metastore").await?;
        fs::create_dir_all(meta_store_path.to_string()).await?;
        Ok(meta_store_path)
    }
}
crate::di_service!(RocksMetaStoreFs, [MetaStoreFs]);
