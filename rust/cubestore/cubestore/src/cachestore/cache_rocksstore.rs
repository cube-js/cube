use crate::cachestore::cache_item::{CacheItemIndexKey, CacheItemRocksIndex, CacheItemRocksTable};
use crate::cachestore::{CacheItem, RocksCacheStoreFs};
use crate::config::injection::DIService;
use crate::config::{Config, ConfigObj};
use std::env;

use crate::metastore::{
    DbTableRef, IdRow, MetaStoreEvent, MetaStoreFs, RocksStore, RocksStoreDetails, RocksTable,
};
use crate::remotefs::LocalDirRemoteFs;
use crate::util::WorkerLoop;
use crate::CubeError;
use async_trait::async_trait;

use futures_timer::Delay;
use rocksdb::{Options, DB};

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Sender;

struct RocksCacheStoreDetails {}

impl RocksStoreDetails for RocksCacheStoreDetails {
    fn open_db(&self, path: &Path) -> Result<DB, CubeError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(13));

        DB::open(&opts, path)
            .map_err(|err| CubeError::internal(format!("DB::open error for cachestore: {}", err)))
    }

    fn migrate(&self, table_ref: DbTableRef) -> Result<(), CubeError> {
        CacheItemRocksTable::new(table_ref.clone()).check_indexes()?;

        Ok(())
    }

    fn get_name(&self) -> &'static str {
        &"cachestore"
    }
}

pub struct RocksCacheStore {
    store: Arc<RocksStore>,
    upload_loop: Arc<WorkerLoop>,
}

impl RocksCacheStore {
    pub fn new(
        path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Arc<Self> {
        Self::new_from_store(RocksStore::with_listener(
            path,
            vec![],
            metastore_fs,
            config,
            Arc::new(RocksCacheStoreDetails {}),
        ))
    }

    fn new_from_store(store: Arc<RocksStore>) -> Arc<Self> {
        Arc::new(Self {
            store,
            upload_loop: Arc::new(WorkerLoop::new("Cachestore upload")),
        })
    }

    pub async fn load_from_dump(
        path: &Path,
        dump_path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let store = RocksStore::load_from_dump(
            path,
            dump_path,
            metastore_fs,
            config,
            Arc::new(RocksCacheStoreDetails {}),
        )
        .await?;

        Ok(Self::new_from_store(store))
    }

    pub async fn load_from_remote(
        path: &str,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let store = metastore_fs
            .load_from_remote(&path, config, Arc::new(RocksCacheStoreDetails {}))
            .await?;

        Ok(Self::new_from_store(store))
    }

    pub async fn wait_upload_loop(meta_store: Arc<Self>) {
        if !meta_store.store.config.upload_to_remote() {
            log::info!("Not running cachestore upload loop");
            return;
        }

        let upload_interval = meta_store.store.config.meta_store_log_upload_interval();
        meta_store
            .upload_loop
            .process(
                meta_store.clone(),
                async move |_| Ok(Delay::new(Duration::from_secs(upload_interval)).await),
                async move |m, _| m.store.run_upload().await,
            )
            .await;
    }

    pub async fn stop_processing_loops(&self) {
        self.upload_loop.stop();
    }

    pub async fn add_listener(&self, listener: Sender<MetaStoreEvent>) {
        self.store.add_listener(listener).await;
    }

    pub fn prepare_test_cachestore(test_name: &str) -> (Arc<LocalDirRemoteFs>, Arc<Self>) {
        let config = Config::test(test_name);
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-local", test_name));
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-remote", test_name));

        let _ = std::fs::remove_dir_all(store_path.clone());
        let _ = std::fs::remove_dir_all(remote_store_path.clone());

        let details = Arc::new(RocksCacheStoreDetails {});
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        let store = RocksStore::new(
            store_path.clone().join(details.get_name()).as_path(),
            RocksCacheStoreFs::new(remote_fs.clone()),
            config.config_obj(),
            details,
        );

        (remote_fs, Self::new_from_store(store))
    }

    pub fn cleanup_test_cachestore(test_name: &str) {
        RocksStore::cleanup_test_store(test_name)
    }

    pub async fn run_upload(&self) -> Result<(), CubeError> {
        self.store.run_upload().await
    }

    pub async fn upload_check_point(&self) -> Result<(), CubeError> {
        self.store.upload_check_point().await
    }

    pub async fn has_pending_changes(&self) -> Result<bool, CubeError> {
        self.store.has_pending_changes().await
    }

    pub async fn check_all_indexes(&self) -> Result<(), CubeError> {
        RocksStore::check_all_indexes(&self.store).await
    }
}

#[cuberpc::service]
pub trait CacheStore: DIService + Send + Sync {
    async fn cache_incr(&self, key: String) -> Result<IdRow<CacheItem>, CubeError>;
}

#[async_trait]
impl CacheStore for RocksCacheStore {
    async fn cache_incr(&self, path: String) -> Result<IdRow<CacheItem>, CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let index_key = CacheItemIndexKey::ByPath(path.clone());
                let id_row_opt = cache_schema
                    .get_single_opt_row_by_index(&index_key, &CacheItemRocksIndex::ByPath)?;

                // TODO: Merge operator?
                if let Some(id_row) = id_row_opt {
                    let mut new = id_row.row.clone();

                    let last_val = id_row.row.value.parse::<i64>()?;
                    new.value = (last_val + 1).to_string();

                    cache_schema.update(id_row.id, new, &id_row.row, batch_pipe)
                } else {
                    let item = CacheItem::new(path, None, "1".to_string());
                    cache_schema.insert(item, batch_pipe)
                }
            })
            .await
    }
}

crate::di_service!(RocksCacheStore, [CacheStore]);
crate::di_service!(CacheStoreRpcClient, [CacheStore]);

pub struct ClusterCacheStoreClient {}

#[async_trait]
impl CacheStore for ClusterCacheStoreClient {
    async fn cache_incr(&self, _: String) -> Result<IdRow<CacheItem>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_incr was used.")
    }
}

crate::di_service!(ClusterCacheStoreClient, [CacheStore]);

#[cfg(test)]
mod tests {
    use crate::cachestore::*;
    use crate::CubeError;

    #[tokio::test]
    async fn test_cache_incr() -> Result<(), CubeError> {
        // arrange
        let (_, cachestore) = RocksCacheStore::prepare_test_cachestore("cache_incr");

        let key = "prefix:key".to_string();
        assert_eq!(
            cachestore.cache_incr(key.clone()).await?.get_row().value,
            "1"
        );
        assert_eq!(cachestore.cache_incr(key).await?.get_row().value, "2");

        RocksCacheStore::cleanup_test_cachestore("cache_incr");

        Ok(())
    }
}
