use crate::cachestore::cache_item::{CacheItemIndexKey, CacheItemRocksIndex, CacheItemRocksTable};
use crate::cachestore::compaction;
use crate::cachestore::CacheItem;
use crate::config::injection::DIService;
use crate::config::{Config, ConfigObj};
use std::collections::HashMap;
use std::env;

use crate::metastore::{
    BaseRocksStoreFs, DbTableRef, IdRow, MetaStoreEvent, MetaStoreFs, RocksStore,
    RocksStoreDetails, RocksTable,
};
use crate::remotefs::LocalDirRemoteFs;
use crate::util::WorkerLoop;
use crate::CubeError;
use async_trait::async_trait;

use futures_timer::Delay;
use rocksdb::{Options, DB};

use crate::cachestore::compaction::CompactionPreloadedState;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast::Sender;

pub(crate) struct RocksCacheStoreDetails {}

impl RocksCacheStoreDetails {
    pub fn get_compaction_state() -> CompactionPreloadedState {
        let mut indexes = HashMap::new();

        macro_rules! populate_indexes {
            ($TABLE:ident) => {
                for index in $TABLE::indexes() {
                    indexes.insert(
                        $TABLE::index_id(index.get_id()),
                        crate::metastore::SecondaryIndexInfo {
                            version: index.version(),
                            value_version: index.value_version(),
                        },
                    );
                }
            };
        }

        populate_indexes!(CacheItemRocksTable);

        CompactionPreloadedState::new(indexes)
    }
}

impl RocksStoreDetails for RocksCacheStoreDetails {
    fn open_db(&self, path: &Path) -> Result<DB, CubeError> {
        let compaction_state = Arc::new(Mutex::new(Some(
            RocksCacheStoreDetails::get_compaction_state(),
        )));

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(13));
        opts.set_compaction_filter_factory(compaction::MetaStoreCacheCompactionFactory::new(
            compaction_state,
        ));
        // Disable automatic compaction before migration, will be enabled later in after_migration
        opts.set_disable_auto_compactions(true);

        DB::open(&opts, path)
            .map_err(|err| CubeError::internal(format!("DB::open error for cachestore: {}", err)))
    }

    fn migrate(&self, table_ref: DbTableRef) -> Result<(), CubeError> {
        CacheItemRocksTable::new(table_ref.clone()).migrate()?;

        table_ref
            .db
            .set_options(&[("disable_auto_compactions", "false")])?;

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
    ) -> Result<Arc<Self>, CubeError> {
        Ok(Self::new_from_store(RocksStore::with_listener(
            path,
            vec![],
            metastore_fs,
            config,
            Arc::new(RocksCacheStoreDetails {}),
        )?))
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

    pub async fn wait_upload_loop(self: Arc<Self>) {
        if !self.store.config.upload_to_remote() {
            log::info!("Not running cachestore upload loop");
            return;
        }

        let upload_interval = self.store.config.meta_store_log_upload_interval();
        self.upload_loop
            .process(
                self.clone(),
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
            BaseRocksStoreFs::new(remote_fs.clone(), "cachestore", config.config_obj()),
            config.config_obj(),
            details,
        )
        .unwrap();

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
    async fn cache_all(&self) -> Result<Vec<IdRow<CacheItem>>, CubeError>;
    async fn cache_set(
        &self,
        item: CacheItem,
        update_if_not_exists: bool,
    ) -> Result<bool, CubeError>;
    async fn cache_truncate(&self) -> Result<(), CubeError>;
    async fn cache_delete(&self, key: String) -> Result<(), CubeError>;
    async fn cache_get(&self, key: String) -> Result<Option<IdRow<CacheItem>>, CubeError>;
    async fn cache_keys(&self, prefix: String) -> Result<Vec<IdRow<CacheItem>>, CubeError>;
    async fn cache_incr(&self, key: String) -> Result<IdRow<CacheItem>, CubeError>;
    // Force compaction for the whole RocksDB
    async fn compaction(&self) -> Result<(), CubeError>;
}

#[async_trait]
impl CacheStore for RocksCacheStore {
    async fn cache_get(&self, key: String) -> Result<Option<IdRow<CacheItem>>, CubeError> {
        self.store
            .read_operation(move |db_ref| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let index_key = CacheItemIndexKey::ByPath(key);
                let id_row_opt = cache_schema
                    .get_single_opt_row_by_index(&index_key, &CacheItemRocksIndex::ByPath)?;

                Ok(id_row_opt)
            })
            .await
    }

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

    async fn cache_all(&self) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.store
            .read_operation_out_of_queue(move |db_ref| {
                Ok(CacheItemRocksTable::new(db_ref).all_rows()?)
            })
            .await
    }

    async fn cache_keys(&self, prefix: String) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.store
            .read_operation(move |db_ref| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let index_key =
                    CacheItemIndexKey::ByPrefix(CacheItem::parse_path_to_prefix(prefix));
                let rows =
                    cache_schema.get_rows_by_index(&index_key, &CacheItemRocksIndex::ByPrefix)?;

                Ok(rows)
            })
            .await
    }

    async fn cache_set(
        &self,
        item: CacheItem,
        update_if_not_exists: bool,
    ) -> Result<bool, CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let index_key = CacheItemIndexKey::ByPath(item.get_path());
                let id_row_opt = cache_schema
                    .get_single_opt_row_by_index(&index_key, &CacheItemRocksIndex::ByPath)?;

                if let Some(id_row) = id_row_opt {
                    if update_if_not_exists {
                        return Ok(false);
                    };

                    let mut new = id_row.row.clone();
                    new.value = item.value;
                    new.expire = item.expire;

                    cache_schema.update(id_row.id, new, &id_row.row, batch_pipe)?;
                } else {
                    cache_schema.insert(item, batch_pipe)?;
                }

                Ok(true)
            })
            .await
    }

    async fn cache_truncate(&self) -> Result<(), CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref);
                let rows = cache_schema.all_rows()?;
                for row in rows.iter() {
                    cache_schema.delete(row.get_id(), batch_pipe)?;
                }

                Ok(())
            })
            .await?;

        Ok(())
    }

    async fn cache_delete(&self, key: String) -> Result<(), CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let index_key = CacheItemIndexKey::ByPath(key);
                let row_opt = cache_schema
                    .get_single_opt_row_by_index(&index_key, &CacheItemRocksIndex::ByPath)?;

                if let Some(row) = row_opt {
                    cache_schema.delete(row.id, batch_pipe)?;
                }

                Ok(())
            })
            .await?;

        Ok(())
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        self.store
            .write_operation(move |db_ref, _batch_pipe| {
                let start: Option<&[u8]> = None;
                let end: Option<&[u8]> = None;

                db_ref.db.compact_range(start, end);

                Ok(())
            })
            .await?;

        Ok(())
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

    async fn cache_all(&self) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_all was used.")
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! compaction was used.")
    }

    async fn cache_set(
        &self,
        _item: CacheItem,
        _update_if_not_exists: bool,
    ) -> Result<bool, CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_set was used.")
    }

    async fn cache_truncate(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_truncate was used.")
    }

    async fn cache_delete(&self, _key: String) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_delete was used.")
    }

    async fn cache_get(&self, _key: String) -> Result<Option<IdRow<CacheItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_get was used.")
    }

    async fn cache_keys(&self, _prefix: String) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_keys was used.")
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
