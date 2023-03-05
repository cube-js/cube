use crate::cachestore::cache_item::{
    CacheItem, CacheItemIndexKey, CacheItemRocksIndex, CacheItemRocksTable,
};
use crate::cachestore::queue_item::{
    QueueItem, QueueItemIndexKey, QueueItemRocksIndex, QueueItemRocksTable, QueueItemStatus,
    QueueResultAckEvent, QueueResultAckEventResult,
};
use crate::cachestore::queue_result::{
    QueueResultIndexKey, QueueResultRocksIndex, QueueResultRocksTable,
};
use crate::cachestore::{compaction, QueueResult};
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
use crate::cachestore::listener::RocksCacheStoreListener;
use crate::table::{Row, TableValue};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::trace;
use serde_derive::{Deserialize, Serialize};
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
        populate_indexes!(QueueItemRocksTable);
        populate_indexes!(QueueResultRocksTable);

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
        QueueItemRocksTable::new(table_ref.clone()).migrate()?;
        QueueResultRocksTable::new(table_ref.clone()).migrate()?;

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
    pub async fn get_listener(&self) -> RocksCacheStoreListener {
        let listeners = self.store.listeners.read().await;

        let sender = if listeners.len() > 0 {
            listeners.first().unwrap()
        } else {
            panic!("Unable to get listener for CacheStore");
        };

        RocksCacheStoreListener::new(sender.subscribe())
    }

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

impl RocksCacheStore {
    async fn lookup_queue_result(
        &self,
        key: String,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let result_schema = QueueResultRocksTable::new(db_ref.clone());
                let index_key = QueueResultIndexKey::ByPath(key);
                let queue_result = result_schema
                    .get_single_opt_row_by_index(&index_key, &QueueResultRocksIndex::ByPath)?;

                if let Some(queue_result) = queue_result {
                    result_schema.try_delete(queue_result.get_id(), batch_pipe)?;

                    Ok(Some(QueueResultResponse::Success {
                        value: Some(queue_result.row.value),
                    }))
                } else {
                    Ok(None)
                }
            })
            .await
    }

    fn queue_count_by_prefix_and_status(
        db_ref: DbTableRef,
        prefix: &Option<String>,
        status: QueueItemStatus,
    ) -> Result<u64, CubeError> {
        let queue_schema = QueueItemRocksTable::new(db_ref.clone());
        let index_key =
            QueueItemIndexKey::ByPrefixAndStatus(prefix.clone().unwrap_or("".to_string()), status);
        queue_schema.count_rows_by_index(&index_key, &QueueItemRocksIndex::ByPrefixAndStatus)
    }

    fn filter_to_cancel(
        now: DateTime<Utc>,
        items: Vec<IdRow<QueueItem>>,
        orphaned_timeout: Option<u32>,
        heartbeat_timeout: Option<u32>,
    ) -> Vec<IdRow<QueueItem>> {
        items
            .into_iter()
            .filter(|item| {
                if item.get_row().get_status() == &QueueItemStatus::Pending {
                    return if let Some(orphaned_timeout) = orphaned_timeout {
                        if let Some(orphaned) = item.get_row().get_orphaned() {
                            return if orphaned < &now { true } else { false };
                        }

                        let elapsed = now - item.get_row().get_created().clone();
                        if elapsed.num_milliseconds() > orphaned_timeout as i64 {
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                }

                if item.get_row().get_status() == &QueueItemStatus::Active {
                    if let Some(heartbeat_timeout) = heartbeat_timeout {
                        let elapsed = if let Some(heartbeat) = item.get_row().get_heartbeat() {
                            now - heartbeat.clone()
                        } else {
                            now - item.get_row().get_created().clone()
                        };
                        if elapsed.num_milliseconds() > heartbeat_timeout as i64 {
                            return true;
                        }
                    }
                }

                false
            })
            .collect()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct QueueAddResponse {
    pub added: bool,
    pub pending: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum QueueResultResponse {
    Success { value: Option<String> },
}

impl QueueResultResponse {
    pub fn into_queue_result_row(self) -> Row {
        match self {
            QueueResultResponse::Success { value } => Row::new(vec![
                if let Some(v) = value {
                    TableValue::String(v)
                } else {
                    TableValue::Null
                },
                TableValue::String("success".to_string()),
            ]),
        }
    }
}

#[cuberpc::service]
pub trait CacheStore: DIService + Send + Sync {
    // cache
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

    // queue
    async fn queue_all(&self) -> Result<Vec<IdRow<QueueItem>>, CubeError>;
    async fn queue_add(&self, item: QueueItem) -> Result<QueueAddResponse, CubeError>;
    async fn queue_truncate(&self) -> Result<(), CubeError>;
    async fn queue_get(&self, key: String) -> Result<Option<IdRow<QueueItem>>, CubeError>;
    async fn queue_to_cancel(
        &self,
        prefix: String,
        orphaned_timeout: Option<u32>,
        heartbeat_timeout: Option<u32>,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError>;
    async fn queue_list(
        &self,
        prefix: String,
        status_filter: Option<QueueItemStatus>,
        priority_sort: bool,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError>;
    async fn queue_cancel(&self, key: String) -> Result<Option<IdRow<QueueItem>>, CubeError>;
    async fn queue_heartbeat(&self, key: String) -> Result<(), CubeError>;
    async fn queue_retrieve(
        &self,
        key: String,
        allow_concurrency: u32,
    ) -> Result<Option<IdRow<QueueItem>>, CubeError>;
    async fn queue_ack(&self, key: String, result: Option<String>) -> Result<(), CubeError>;
    async fn queue_result(&self, key: String) -> Result<Option<QueueResultResponse>, CubeError>;
    async fn queue_result_blocking(
        &self,
        key: String,
        timeout: u64,
    ) -> Result<Option<QueueResultResponse>, CubeError>;
    async fn queue_merge_extra(&self, key: String, payload: String) -> Result<(), CubeError>;

    // Force compaction for the whole RocksDB
    async fn compaction(&self) -> Result<(), CubeError>;
    async fn healthcheck(&self) -> Result<(), CubeError>;
}

#[async_trait]
impl CacheStore for RocksCacheStore {
    async fn cache_all(&self) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.store
            .read_operation_out_of_queue(move |db_ref| {
                Ok(CacheItemRocksTable::new(db_ref).all_rows()?)
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

    async fn queue_all(&self) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        self.store
            .read_operation(move |db_ref| Ok(QueueItemRocksTable::new(db_ref).all_rows()?))
            .await
    }

    async fn queue_add(&self, item: QueueItem) -> Result<QueueAddResponse, CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let index_key = QueueItemIndexKey::ByPath(item.get_path());
                let id_row_opt = queue_schema
                    .get_single_opt_row_by_index(&index_key, &QueueItemRocksIndex::ByPath)?;

                let pending = Self::queue_count_by_prefix_and_status(
                    db_ref,
                    item.get_prefix(),
                    QueueItemStatus::Pending,
                )?;

                let added = if id_row_opt.is_none() {
                    queue_schema.insert(item, batch_pipe)?;

                    true
                } else {
                    false
                };

                Ok(QueueAddResponse {
                    added,
                    pending: if added { pending + 1 } else { pending },
                })
            })
            .await
    }

    async fn queue_truncate(&self) -> Result<(), CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let queue_item_schema = QueueItemRocksTable::new(db_ref.clone());
                let rows = queue_item_schema.all_rows()?;
                for row in rows.iter() {
                    queue_item_schema.delete(row.get_id(), batch_pipe)?;
                }

                let queue_result_schema = QueueResultRocksTable::new(db_ref);
                let rows = queue_result_schema.all_rows()?;
                for row in rows.iter() {
                    queue_result_schema.delete(row.get_id(), batch_pipe)?;
                }

                Ok(())
            })
            .await?;

        Ok(())
    }

    async fn queue_get(&self, key: String) -> Result<Option<IdRow<QueueItem>>, CubeError> {
        self.store
            .read_operation(move |db_ref| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let index_key = QueueItemIndexKey::ByPath(key);
                queue_schema.get_single_opt_row_by_index(&index_key, &QueueItemRocksIndex::ByPath)
            })
            .await
    }

    async fn queue_to_cancel(
        &self,
        prefix: String,
        orphaned_timeout: Option<u32>,
        heartbeat_timeout: Option<u32>,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        self.store
            .read_operation(move |db_ref| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let index_key = QueueItemIndexKey::ByPrefix(prefix);
                let items =
                    queue_schema.get_rows_by_index(&index_key, &QueueItemRocksIndex::ByPrefix)?;

                Ok(Self::filter_to_cancel(
                    db_ref.start_time.clone(),
                    items,
                    orphaned_timeout,
                    heartbeat_timeout,
                ))
            })
            .await
    }

    async fn queue_list(
        &self,
        prefix: String,
        status_filter: Option<QueueItemStatus>,
        priority_sort: bool,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        self.store
            .read_operation(move |db_ref| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());

                let items = if let Some(status_filter) = status_filter {
                    let index_key = QueueItemIndexKey::ByPrefixAndStatus(prefix, status_filter);
                    queue_schema
                        .get_rows_by_index(&index_key, &QueueItemRocksIndex::ByPrefixAndStatus)?
                } else {
                    let index_key = QueueItemIndexKey::ByPrefix(prefix);
                    queue_schema.get_rows_by_index(&index_key, &QueueItemRocksIndex::ByPrefix)?
                };

                if priority_sort {
                    Ok(items
                        .into_iter()
                        .sorted_by(|a, b| b.row.cmp(&a.row))
                        .collect())
                } else {
                    Ok(items)
                }
            })
            .await
    }

    async fn queue_cancel(&self, key: String) -> Result<Option<IdRow<QueueItem>>, CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let index_key = QueueItemIndexKey::ByPath(key);
                let id_row_opt = queue_schema
                    .get_single_opt_row_by_index(&index_key, &QueueItemRocksIndex::ByPath)?;

                if let Some(id_row) = id_row_opt {
                    Ok(Some(queue_schema.delete(id_row.get_id(), batch_pipe)?))
                } else {
                    Ok(None)
                }
            })
            .await
    }

    async fn queue_heartbeat(&self, key: String) -> Result<(), CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let index_key = QueueItemIndexKey::ByPath(key.clone());
                let id_row_opt = queue_schema
                    .get_single_opt_row_by_index(&index_key, &QueueItemRocksIndex::ByPath)?;

                if let Some(id_row) = id_row_opt {
                    let mut new = id_row.get_row().clone();
                    new.update_heartbeat();

                    queue_schema.update(id_row.id, new, id_row.get_row(), batch_pipe)?;
                    Ok(())
                } else {
                    trace!(
                        "Unable to update heartbeat for queue item with path: {}",
                        key
                    );

                    Ok(())
                }
            })
            .await
    }

    async fn queue_retrieve(
        &self,
        key: String,
        allow_concurrency: u32,
    ) -> Result<Option<IdRow<QueueItem>>, CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let index_key = QueueItemIndexKey::ByPath(key.clone());
                let id_row_opt = queue_schema
                    .get_single_opt_row_by_index(&index_key, &QueueItemRocksIndex::ByPath)?;

                if let Some(id_row) = id_row_opt {
                    if id_row.get_row().get_status() == &QueueItemStatus::Pending {
                        let current_active = Self::queue_count_by_prefix_and_status(
                            db_ref,
                            id_row.get_row().get_prefix(),
                            QueueItemStatus::Active,
                        )?;
                        if current_active >= (allow_concurrency as u64) {
                            return Ok(None);
                        }

                        let mut new = id_row.get_row().clone();
                        new.status = QueueItemStatus::Active;
                        // It's an important to insert heartbeat, because
                        // without that created datetime will be used for orphaned filtering
                        new.update_heartbeat();

                        let res = queue_schema.update(
                            id_row.get_id(),
                            new,
                            id_row.get_row(),
                            batch_pipe,
                        )?;

                        Ok(Some(res))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            })
            .await
    }

    async fn queue_ack(&self, path: String, result: Option<String>) -> Result<(), CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let result_schema = QueueResultRocksTable::new(db_ref.clone());
                let index_key = QueueItemIndexKey::ByPath(path.clone());
                let item_row = queue_schema
                    .get_single_opt_row_by_index(&index_key, &QueueItemRocksIndex::ByPath)?;

                if let Some(item_row) = item_row {
                    queue_schema.delete(item_row.get_id(), batch_pipe)?;

                    if let Some(result) = result {
                        let queue_result = QueueResult::new(path.clone(), result.clone());
                        let result_row = result_schema.insert(queue_result, batch_pipe)?;

                        batch_pipe.add_event(MetaStoreEvent::AckQueueItem(QueueResultAckEvent {
                            path,
                            result: QueueResultAckEventResult::WithResult {
                                row_id: result_row.get_id(),
                                result,
                            },
                        }));
                    } else {
                        batch_pipe.add_event(MetaStoreEvent::AckQueueItem(QueueResultAckEvent {
                            path,
                            result: QueueResultAckEventResult::Empty {},
                        }));
                    }

                    Ok(())
                } else {
                    Err(CubeError::user(format!(
                        "Unable ack queue, unknown id: {}",
                        path
                    )))
                }
            })
            .await
    }

    async fn queue_result(&self, key: String) -> Result<Option<QueueResultResponse>, CubeError> {
        self.lookup_queue_result(key).await
    }

    async fn queue_result_blocking(
        &self,
        key: String,
        timeout: u64,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        let store_in_result = self.lookup_queue_result(key.clone()).await?;
        if store_in_result.is_some() {
            return Ok(store_in_result);
        }

        let listener = self.get_listener().await;
        let fut = tokio::time::timeout(
            Duration::from_millis(timeout),
            listener.wait_for_queue_ack(key),
        );

        if let Ok(res) = fut.await {
            match res {
                Ok(Some(ack_event)) => match ack_event.result {
                    QueueResultAckEventResult::Empty => {
                        Ok(Some(QueueResultResponse::Success { value: None }))
                    }
                    QueueResultAckEventResult::WithResult { row_id, result } => {
                        self.store
                            .write_operation(move |db_ref, batch_pipe| {
                                let queue_schema = QueueResultRocksTable::new(db_ref.clone());
                                queue_schema.try_delete(row_id, batch_pipe)?;

                                Ok(Some(QueueResultResponse::Success {
                                    value: Some(result),
                                }))
                            })
                            .await
                    }
                },
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        } else {
            Ok(None)
        }
    }

    async fn queue_merge_extra(&self, key: String, payload: String) -> Result<(), CubeError> {
        self.store
            .write_operation(move |db_ref, batch_pipe| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let index_key = QueueItemIndexKey::ByPath(key.clone());
                let id_row_opt = queue_schema
                    .get_single_opt_row_by_index(&index_key, &QueueItemRocksIndex::ByPath)?;

                if let Some(id_row) = id_row_opt {
                    let new = id_row.get_row().merge_extra(payload)?;

                    queue_schema.update(id_row.id, new, id_row.get_row(), batch_pipe)?;

                    Ok(())
                } else {
                    Err(CubeError::user(format!(
                        "Unable to find queue with id: {}",
                        key
                    )))
                }
            })
            .await
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

    async fn healthcheck(&self) -> Result<(), CubeError> {
        self.store
            .read_operation(move |_| {
                // read_operation will call getSnapshot, which is enough to test that RocksDB works
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
    async fn cache_all(&self) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_all was used.")
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

    async fn cache_incr(&self, _: String) -> Result<IdRow<CacheItem>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! cache_incr was used.")
    }

    async fn queue_all(&self) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_all was used.")
    }

    async fn queue_add(&self, _item: QueueItem) -> Result<QueueAddResponse, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_add was used.")
    }

    async fn queue_truncate(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_truncate was used.")
    }

    async fn queue_get(&self, _key: String) -> Result<Option<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_get was used.")
    }

    async fn queue_to_cancel(
        &self,
        _prefix: String,
        _orphaned_timeout: Option<u32>,
        _heartbeat_timeout: Option<u32>,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_to_cancel was used.")
    }

    async fn queue_list(
        &self,
        _prefix: String,
        _status_filter: Option<QueueItemStatus>,
        _priority_sort: bool,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_list was used.")
    }

    async fn queue_cancel(&self, _key: String) -> Result<Option<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_cancel was used.")
    }

    async fn queue_heartbeat(&self, _key: String) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_heartbeat was used.")
    }

    async fn queue_retrieve(
        &self,
        _key: String,
        _allow_concurrency: u32,
    ) -> Result<Option<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_retrieve was used.")
    }

    async fn queue_ack(&self, _key: String, _result: Option<String>) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_ack was used.")
    }

    async fn queue_result(&self, _key: String) -> Result<Option<QueueResultResponse>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_result was used.")
    }

    async fn queue_result_blocking(
        &self,
        _key: String,
        _timeout: u64,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_result_blocking was used.")
    }

    async fn queue_merge_extra(&self, _key: String, _payload: String) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_merge_extra was used.")
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! compaction was used.")
    }

    async fn healthcheck(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! healthcheck was used.")
    }
}

crate::di_service!(ClusterCacheStoreClient, [CacheStore]);

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn test_filter_to_cancel() {
        let now = Utc::now();
        let item_pending_custom_orphaned = IdRow::new(
            1,
            QueueItem::new(
                "1".to_string(),
                "1".to_string(),
                QueueItemStatus::Pending,
                1,
                Some(10),
            ),
        );
        let item_pending_custom_orphaned_expired = IdRow::new(
            2,
            QueueItem::new(
                "2".to_string(),
                "2".to_string(),
                QueueItemStatus::Pending,
                1,
                Some(1),
            ),
        );
        let item_active_custom_orphaned = IdRow::new(
            3,
            QueueItem::new(
                "3".to_string(),
                "3".to_string(),
                QueueItemStatus::Active,
                1,
                Some(10),
            ),
        );
        let mut item_active_custom_orphaned_expired = IdRow::new(
            4,
            QueueItem::new(
                "4".to_string(),
                "4".to_string(),
                QueueItemStatus::Active,
                1,
                Some(1),
            ),
        );

        assert_eq!(
            RocksCacheStore::filter_to_cancel(
                now.clone(),
                vec![
                    item_pending_custom_orphaned.clone(),
                    item_pending_custom_orphaned_expired.clone(),
                    item_active_custom_orphaned.clone(),
                    item_active_custom_orphaned_expired.clone()
                ],
                Some(1000),
                None,
            )
            .iter()
            .map(|row| row.id)
            .collect::<Vec<u64>>()
            .len(),
            0
        );

        assert_eq!(
            RocksCacheStore::filter_to_cancel(
                now.clone(),
                vec![
                    item_pending_custom_orphaned.clone(),
                    item_pending_custom_orphaned_expired.clone(),
                    item_active_custom_orphaned.clone(),
                    item_active_custom_orphaned_expired.clone()
                ],
                Some(1000),
                Some(1000)
            )
            .iter()
            .map(|row| row.id)
            .collect::<Vec<u64>>()
            .len(),
            0
        );

        let now = now + chrono::Duration::seconds(2);

        assert_eq!(
            RocksCacheStore::filter_to_cancel(
                now.clone(),
                vec![
                    item_pending_custom_orphaned.clone(),
                    item_pending_custom_orphaned_expired.clone(),
                    item_active_custom_orphaned.clone(),
                    item_active_custom_orphaned_expired.clone()
                ],
                Some(1000),
                None,
            )
            .iter()
            .map(|row| row.id)
            .collect::<Vec<u64>>(),
            vec![2]
        );

        item_active_custom_orphaned_expired.row.heartbeat = Some(now.clone());

        assert_eq!(
            RocksCacheStore::filter_to_cancel(
                now,
                vec![
                    item_pending_custom_orphaned.clone(),
                    item_pending_custom_orphaned_expired.clone(),
                    item_active_custom_orphaned.clone(),
                    item_active_custom_orphaned_expired.clone()
                ],
                Some(1000),
                Some(1000)
            )
            .iter()
            .map(|row| row.id)
            .collect::<Vec<u64>>(),
            vec![2, 3]
        );
    }
}
