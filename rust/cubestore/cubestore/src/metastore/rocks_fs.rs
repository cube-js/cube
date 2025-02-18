use crate::config::ConfigObj;
use crate::metastore::snapshot_info::SnapshotInfo;
use crate::metastore::{RocksStore, RocksStoreDetails, WriteBatchContainer};
use crate::remotefs::RemoteFs;
use crate::CubeError;
use async_trait::async_trait;
use datafusion::cube_ext;
use futures::future::join_all;
use itertools::Itertools;
use log::{error, info};
use regex::Regex;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs;
use tokio::fs::File;

#[async_trait]
pub trait MetaStoreFs: Send + Sync {
    async fn load_from_remote(
        self: Arc<Self>,
        path: &str,
        config: Arc<dyn ConfigObj>,
        rocks_details: Arc<dyn RocksStoreDetails>,
    ) -> Result<Arc<RocksStore>, CubeError>;
    async fn upload_log(
        &self,
        dir: &str,
        seq_number: u64,
        serializer: WriteBatchContainer,
    ) -> Result<u64, CubeError>;
    async fn upload_checkpoint(
        &self,
        remote_path: String,
        checkpoint_path: PathBuf,
    ) -> Result<(), CubeError>;
    async fn check_rocks_store(
        &self,
        rocks_store: Arc<RocksStore>,
        snapshot: Option<u128>,
    ) -> Result<Arc<RocksStore>, CubeError>;
    async fn load_metastore_logs(
        &self,
        snapshot: u128,
        rocks_store: &Arc<RocksStore>,
    ) -> Result<(), CubeError>;
    async fn get_snapshots_list(&self) -> Result<Vec<SnapshotInfo>, CubeError>;
    async fn write_metastore_current(&self, remote_path: &str) -> Result<(), CubeError>;
}

#[derive(Clone)]
pub struct BaseRocksStoreFs {
    remote_fs: Arc<dyn RemoteFs>,
    name: &'static str,
    minimum_snapshots_count: u64,
    snapshots_lifetime: u64,
}

impl BaseRocksStoreFs {
    pub fn new_for_metastore(
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Arc<Self> {
        let minimum_snapshots_count = config.minimum_metastore_snapshots_count();
        let snapshots_lifetime = config.metastore_snapshots_lifetime();
        Arc::new(Self {
            remote_fs,
            name: "metastore",
            minimum_snapshots_count,
            snapshots_lifetime,
        })
    }
    pub fn new_for_cachestore(
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Arc<Self> {
        let minimum_snapshots_count = config.minimum_cachestore_snapshots_count();
        let snapshots_lifetime = config.cachestore_snapshots_lifetime();
        Arc::new(Self {
            remote_fs,
            name: "cachestore",
            minimum_snapshots_count,
            snapshots_lifetime,
        })
    }

    pub fn get_name(&self) -> &'static str {
        &self.name
    }

    pub async fn make_local_metastore_dir(&self) -> Result<String, CubeError> {
        let meta_store_path = self.remote_fs.local_file(self.name.to_string()).await?;
        fs::create_dir_all(meta_store_path.to_string()).await?;
        Ok(meta_store_path)
    }

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
                        let local = remote_fs.local_file(f.clone()).await?;
                        // TODO persist file size
                        Ok::<_, CubeError>((
                            f.clone(),
                            remote_fs.upload_file(local, f.clone()).await?,
                        ))
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
        let existing_metastore_files = self.remote_fs.list(format!("{}-", self.name)).await?;
        let candidates = existing_metastore_files
            .iter()
            .filter_map(|existing| {
                let path = existing.split("/").nth(0).map(|p| {
                    u128::from_str(
                        &p.replace(&format!("{}-", self.name), "")
                            .replace("-index-logs", "")
                            .replace("-logs", ""),
                    )
                });
                if let Some(Ok(millis)) = path {
                    Some((existing, millis))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let lifetime_ms = (self.snapshots_lifetime as u128) * 1000;
        let min_snapshots_count = self.minimum_snapshots_count as usize;

        let mut snapshots_list = candidates
            .iter()
            .map(|(_, ms)| ms.to_owned())
            .unique()
            .collect::<Vec<_>>();
        snapshots_list.sort_unstable_by(|a, b| b.cmp(a));

        let snapshots_to_delete = snapshots_list
            .into_iter()
            .skip(min_snapshots_count)
            .filter(|ms| {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    - ms
                    > lifetime_ms
            })
            .collect::<HashSet<_>>();

        if !snapshots_to_delete.is_empty() {
            let to_delete = candidates
                .into_iter()
                .filter_map(|(path, ms)| {
                    if snapshots_to_delete.contains(&ms) {
                        Some(path.to_owned())
                    } else {
                        None
                    }
                })
                .unique()
                .collect::<Vec<_>>();
            for v in join_all(
                to_delete
                    .iter()
                    .map(|f| self.remote_fs.delete_file(f.to_string()))
                    .collect::<Vec<_>>(),
            )
            .await
            .into_iter()
            {
                v?;
            }

            Ok(to_delete)
        } else {
            Ok(vec![])
        }
    }

    pub async fn is_remote_metadata_exists(&self) -> Result<bool, CubeError> {
        let res = self
            .remote_fs
            .list(format!("{}-current", self.name))
            .await?
            .len()
            > 0;
        Ok(res)
    }

    pub async fn load_current_snapshot_id(&self) -> Result<Option<u128>, CubeError> {
        if self
            .remote_fs
            .list(format!("{}-current", self.name))
            .await?
            .len()
            == 0
        {
            return Ok(None);
        }

        info!("Downloading remote {}", self.name);

        let current_metastore_file = self
            .remote_fs
            .local_file(format!("{}-current", self.name))
            .await?;
        if fs::metadata(current_metastore_file.as_str()).await.is_ok() {
            fs::remove_file(current_metastore_file.as_str()).await?;
        }
        self.remote_fs
            .download_file(format!("{}-current", self.name), None)
            .await?;
        self.parse_local_current_snapshot_id().await
    }

    pub async fn parse_local_current_snapshot_id(&self) -> Result<Option<u128>, CubeError> {
        let current_metastore_file = self
            .remote_fs
            .local_file(format!("{}-current", self.name))
            .await?;

        let re = Regex::new(&format!(r"^{}-(\d+)", &self.name)).unwrap();
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

    pub async fn files_to_load(&self, snapshot: u128) -> Result<Vec<(String, u64)>, CubeError> {
        let res = self
            .remote_fs
            .list_with_metadata(format!("{}-{}", self.name, snapshot))
            .await?
            .into_iter()
            .map(|f| (f.remote_path, f.file_size))
            .collect::<Vec<_>>();
        Ok(res)
    }
}

#[async_trait]
impl MetaStoreFs for BaseRocksStoreFs {
    async fn check_rocks_store(
        &self,
        rocks_store: Arc<RocksStore>,
        snapshot: Option<u128>,
    ) -> Result<Arc<RocksStore>, CubeError> {
        if let Some(snapshot) = snapshot {
            self.load_metastore_logs(snapshot, &rocks_store).await?;
        }

        RocksStore::check_all_indexes(&rocks_store).await?;

        Ok(rocks_store)
    }
    async fn load_from_remote(
        self: Arc<Self>,
        path: &str,
        config: Arc<dyn ConfigObj>,
        rocks_details: Arc<dyn RocksStoreDetails>,
    ) -> Result<Arc<RocksStore>, CubeError> {
        if !fs::metadata(path).await.is_ok() {
            if self.is_remote_metadata_exists().await? {
                let last_metastore_snapshot = self.load_current_snapshot_id().await?;

                if let Some(snapshot) = last_metastore_snapshot {
                    let to_load = self.files_to_load(snapshot.clone()).await?;
                    let meta_store_path = self.make_local_metastore_dir().await?;
                    for (file, _) in to_load.iter() {
                        // TODO check file size
                        self.remote_fs.download_file(file.clone(), None).await?;
                        let local = self.remote_fs.local_file(file.clone()).await?;
                        let path = Path::new(&local);
                        fs::copy(
                            path,
                            PathBuf::from(&meta_store_path)
                                .join(path.file_name().unwrap().to_str().unwrap()),
                        )
                        .await?;
                    }

                    return self
                        .check_rocks_store(
                            RocksStore::new(Path::new(path), self.clone(), config, rocks_details)?,
                            Some(snapshot),
                        )
                        .await;
                }
            } else {
                //TODO FIX IT (Debug for ext service) trace!("Can't find {}-current in {:?}", self.name, self.remote_fs);
            }
            info!("Creating {} from scratch in {}", self.name, path);
        } else {
            info!("Using existing {} in {}", self.name, path);
        }

        return self
            .check_rocks_store(
                RocksStore::new(Path::new(path), self.clone(), config, rocks_details)?,
                None,
            )
            .await;
    }

    async fn upload_log(
        &self,
        dir: &str,
        seq_number: u64,
        serializer: WriteBatchContainer,
    ) -> Result<u64, CubeError> {
        let log_name = format!("{}/{}.flex", dir, seq_number);
        let file_name = self.remote_fs.local_file(log_name.clone()).await?;

        serializer.write_to_file(&file_name).await?;

        // TODO persist file size
        self.remote_fs.upload_file(file_name, log_name).await
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
    async fn load_metastore_logs(
        &self,
        snapshot: u128,
        rocks_store: &Arc<RocksStore>,
    ) -> Result<(), CubeError> {
        let logs_to_batch = self
            .remote_fs
            .list(format!("{}-{}-logs", self.name, snapshot))
            .await?;
        let mut logs_to_batch_to_seq = logs_to_batch
            .into_iter()
            .map(|f| -> Result<_, CubeError> {
                let last = f
                    .split("/")
                    .last()
                    .ok_or(CubeError::internal(format!("Can't split path: {}", f)))?;
                let result = last.replace(".flex", "").parse::<usize>().map_err(|e| {
                    CubeError::internal(format!("Can't parse flex path {}: {}", f, e))
                })?;
                Ok((f, result))
            })
            .collect::<Result<Vec<_>, _>>()?;
        logs_to_batch_to_seq.sort_unstable_by_key(|(_, seq)| *seq);

        for (log_file, _) in logs_to_batch_to_seq.iter() {
            let path_to_log = self.remote_fs.local_file(log_file.clone()).await?;
            let batch = WriteBatchContainer::read_from_file(&path_to_log).await;
            if let Ok(batch) = batch {
                let db = rocks_store.db.clone();
                db.write(batch.write_batch())?;
            } else if let Err(e) = batch {
                error!(
                    "Corrupted {} WAL file. Discarding: {:?} {}",
                    self.name, log_file, e
                );
                break;
            }
        }
        Ok(())
    }

    async fn get_snapshots_list(&self) -> Result<Vec<SnapshotInfo>, CubeError> {
        let remote_fs = self.remote_fs();

        let re = Regex::new(&*format!(r"^{}-(\d+)/", self.get_name())).unwrap();
        let stores = remote_fs.list(format!("{}-", self.get_name())).await?;
        let mut snapshots = BTreeSet::new();
        for store in stores.iter() {
            let parse_result = re
                .captures(store)
                .map(|c| c.get(1).unwrap().as_str())
                .map(|p| u128::from_str(p));
            if let Some(Ok(millis)) = parse_result {
                snapshots.insert(millis);
            }
        }
        let current_id = self.parse_local_current_snapshot_id().await.unwrap_or(None);
        let res = snapshots
            .into_iter()
            .map(|v| SnapshotInfo {
                id: v,
                current: current_id.map_or(false, |cid| cid == v),
            })
            .collect::<Vec<_>>();
        Ok(res)
    }
    async fn write_metastore_current(&self, remote_path: &str) -> Result<(), CubeError> {
        let uploads_dir = self.remote_fs.uploads_dir().await?;
        let prefix = format!("{}-current", self.name);
        let (file, file_path) = cube_ext::spawn_blocking(move || {
            tempfile::Builder::new()
                .prefix(&prefix)
                .tempfile_in(uploads_dir)
        })
        .await??
        .into_parts();

        tokio::io::AsyncWriteExt::write_all(&mut fs::File::from_std(file), remote_path.as_bytes())
            .await?;

        self.remote_fs
            .upload_file(
                file_path.keep()?.to_str().unwrap().to_string(),
                format!("{}-current", self.name),
            )
            .await?;
        Ok(())
    }
}

crate::di_service!(BaseRocksStoreFs, [MetaStoreFs]);
