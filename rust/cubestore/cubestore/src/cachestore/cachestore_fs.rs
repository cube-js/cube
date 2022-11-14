use crate::config::ConfigObj;
use crate::metastore::{
    BaseRocksStoreFs, MetaStoreFs, RocksStore, RocksStoreDetails, WriteBatchContainer,
};
use crate::remotefs::RemoteFs;
use crate::CubeError;
use async_trait::async_trait;

use log::{info, trace};

use std::path::{Path, PathBuf};

use std::sync::Arc;

use tokio::fs;

#[derive(Clone)]
pub struct RocksCacheStoreFs {
    store_fs: BaseRocksStoreFs,
}

impl RocksCacheStoreFs {
    pub fn new(remote_fs: Arc<dyn RemoteFs>) -> Arc<Self> {
        Arc::new(Self {
            store_fs: BaseRocksStoreFs::new(remote_fs, "cachestore".to_string()),
        })
    }
}

#[async_trait]
impl MetaStoreFs for RocksCacheStoreFs {
    async fn load_from_remote(
        self: Arc<Self>,
        path: &str,
        config: Arc<dyn ConfigObj>,
        rocks_details: Arc<dyn RocksStoreDetails>,
    ) -> Result<Arc<RocksStore>, CubeError> {
        if !fs::metadata(path).await.is_ok() {
            if self.store_fs.is_remote_metadata_exists().await? {
                let last_metastore_snapshot = self.store_fs.load_current_snapshot_id().await?;

                if let Some(snapshot) = last_metastore_snapshot {
                    let to_load = self.store_fs.files_to_load(snapshot.clone()).await?;
                    let meta_store_path = self.store_fs.make_local_metastore_dir().await?;
                    for (file, _) in to_load.iter() {
                        // TODO check file size
                        self.store_fs
                            .remote_fs_ref()
                            .download_file(file, None)
                            .await?;
                        let local = self.store_fs.remote_fs_ref().local_file(file).await?;
                        let path = Path::new(&local);
                        fs::copy(
                            path,
                            PathBuf::from(&meta_store_path)
                                .join(path.file_name().unwrap().to_str().unwrap()),
                        )
                        .await?;
                    }

                    return self
                        .store_fs
                        .check_rocks_store(
                            RocksStore::new(Path::new(path), self.clone(), config, rocks_details),
                            Some(snapshot),
                        )
                        .await;
                }
            } else {
                trace!(
                    "Can't find cachestore-current in {:?}",
                    self.store_fs.remote_fs_ref()
                );
            }
            info!("Creating cachestore from scratch in {}", path);
        } else {
            info!("Using existing cachestore in {}", path);
        }

        return self
            .store_fs
            .check_rocks_store(
                RocksStore::new(Path::new(path), self.clone(), config, rocks_details),
                None,
            )
            .await;
    }

    async fn upload_log(
        &self,
        log_name: &str,
        serializer: &WriteBatchContainer,
    ) -> Result<u64, CubeError> {
        let file_name = self.store_fs.remote_fs_ref().local_file(log_name).await?;
        serializer.write_to_file(&file_name).await?;
        // TODO persist file size
        self.store_fs
            .remote_fs_ref()
            .upload_file(&file_name, &log_name)
            .await
    }

    async fn upload_checkpoint(
        &self,
        remote_path: String,
        checkpoint_path: PathBuf,
    ) -> Result<(), CubeError> {
        self.store_fs
            .upload_snapsots_files(&remote_path, &checkpoint_path)
            .await?;

        self.store_fs.delete_old_snapshots().await?;

        self.store_fs.write_metastore_current(&remote_path).await?;

        Ok(())
    }
}

crate::di_service!(RocksCacheStoreFs, [MetaStoreFs]);
