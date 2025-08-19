use crate::cachestore::cache_item::{
    CacheItem, CacheItemIndexKey, CacheItemRocksIndex, CacheItemRocksTable,
    CACHE_ITEM_SIZE_WITHOUT_VALUE,
};
use crate::cachestore::queue_item::{
    QueueItem, QueueItemIndexKey, QueueItemRocksIndex, QueueItemRocksTable, QueueItemStatus,
    QueueResultAckEvent, QueueResultAckEventResult, QueueRetrieveResponse,
};
use crate::cachestore::queue_result::{QueueResultRocksIndex, QueueResultRocksTable};
use crate::cachestore::{compaction, QueueItemPayload, QueueResult};
use crate::config::injection::DIService;
use crate::config::{Config, ConfigObj};
use std::collections::HashMap;
use std::env;

use crate::metastore::{
    BaseRocksStoreFs, BatchPipe, DbTableRef, IdRow, MetaStoreEvent, MetaStoreFs, RocksPropertyRow,
    RocksStore, RocksStoreDetails, RocksStoreRWLoop, RocksTable, RocksTableStats,
};
use crate::remotefs::LocalDirRemoteFs;
use crate::util::WorkerLoop;
use crate::{app_metrics, CubeError};
use async_trait::async_trait;

use cuberockstore::rocksdb::{BlockBasedOptions, Cache, Options, DB};
use futures_timer::Delay;

use crate::cachestore::cache_eviction_manager::{CacheEvictionManager, EvictionResult};
use crate::cachestore::compaction::CompactionPreloadedState;
use crate::cachestore::listener::RocksCacheStoreListener;
use crate::cachestore::queue_item_payload::QueueItemPayloadRocksTable;
use crate::table::{Row, TableValue};
use chrono::{DateTime, Utc};
use cuberockstore::rocksdb;
use datafusion::cube_ext;
use deepsize::DeepSizeOf;
use itertools::Itertools;
use log::{error, trace, warn};
use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinHandle;

pub(crate) struct RocksCacheStoreDetails {
    log_enabled: bool,
}

impl RocksCacheStoreDetails {
    pub fn new(config: &Arc<dyn ConfigObj>) -> Self {
        Self {
            log_enabled: config.cachestore_log_enabled(),
        }
    }

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
        populate_indexes!(QueueItemPayloadRocksTable);
        populate_indexes!(QueueResultRocksTable);

        CompactionPreloadedState::new(indexes)
    }
}

impl RocksStoreDetails for RocksCacheStoreDetails {
    fn open_db(&self, path: &Path, config: &Arc<dyn ConfigObj>) -> Result<DB, CubeError> {
        let rocksdb_config = config.cachestore_rocksdb_config();
        let compaction_state = Arc::new(Mutex::new(Some(
            RocksCacheStoreDetails::get_compaction_state(),
        )));

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_prefix_extractor(cuberockstore::rocksdb::SliceTransform::create_fixed_prefix(
            13,
        ));
        opts.set_compaction_filter_factory(compaction::MetaStoreCacheCompactionFactory::new(
            compaction_state,
        ));
        // TODO(ovr): Decrease after additional fix for get_updates_since
        opts.set_wal_ttl_seconds(
            config.meta_store_snapshot_interval() + config.meta_store_log_upload_interval(),
        );
        // Disable automatic compaction before migration, will be enabled later in after_migration
        opts.set_disable_auto_compactions(true);

        let block_opts = {
            let mut block_opts = BlockBasedOptions::default();
            // https://github.com/facebook/rocksdb/blob/v7.9.2/include/rocksdb/table.h#L524
            block_opts.set_format_version(5);
            block_opts.set_checksum_type(rocksdb_config.checksum_type.as_rocksdb_enum());

            let cache = Cache::new_lru_cache(rocksdb_config.cache_capacity)?;
            block_opts.set_block_cache(&cache);

            block_opts
        };

        opts.set_max_background_jobs(rocksdb_config.max_background_jobs as i32);
        opts.set_max_subcompactions(rocksdb_config.max_subcompactions);
        opts.set_block_based_table_factory(&block_opts);
        opts.set_compression_type(rocksdb_config.compression_type);
        opts.set_bottommost_compression_type(rocksdb_config.bottommost_compression_type);
        opts.increase_parallelism(rocksdb_config.parallelism as i32);

        DB::open(&opts, path)
            .map_err(|err| CubeError::internal(format!("DB::open error for cachestore: {}", err)))
    }

    fn open_readonly_db(&self, path: &Path, config: &Arc<dyn ConfigObj>) -> Result<DB, CubeError> {
        let rocksdb_config = config.cachestore_rocksdb_config();

        let mut opts = Options::default();
        opts.set_prefix_extractor(cuberockstore::rocksdb::SliceTransform::create_fixed_prefix(
            13,
        ));

        let block_opts = {
            let mut block_opts = BlockBasedOptions::default();
            // https://github.com/facebook/rocksdb/blob/v7.9.2/include/rocksdb/table.h#L524
            block_opts.set_format_version(5);
            block_opts.set_checksum_type(rocksdb_config.checksum_type.as_rocksdb_enum());

            let cache = Cache::new_lru_cache(rocksdb_config.cache_capacity)?;
            block_opts.set_block_cache(&cache);

            block_opts
        };

        opts.set_block_based_table_factory(&block_opts);
        opts.set_compression_type(rocksdb_config.compression_type);
        opts.set_bottommost_compression_type(rocksdb_config.bottommost_compression_type);

        DB::open_for_read_only(&opts, path, false).map_err(|err| {
            CubeError::internal(format!(
                "DB::open_for_read_only error for cachestore: {}",
                err
            ))
        })
    }

    fn migrate(&self, table_ref: DbTableRef) -> Result<(), CubeError> {
        CacheItemRocksTable::new(table_ref.clone()).migrate()?;
        QueueItemRocksTable::new(table_ref.clone()).migrate()?;
        QueueResultRocksTable::new(table_ref.clone()).migrate()?;
        QueueItemPayloadRocksTable::new(table_ref.clone()).migrate()?;

        table_ref
            .db
            .set_options(&[("disable_auto_compactions", "false")])?;

        Ok(())
    }

    fn get_name(&self) -> &'static str {
        &"cachestore"
    }

    fn log_enabled(&self) -> bool {
        self.log_enabled
    }
}

pub struct RocksCacheStore {
    store: Arc<RocksStore>,
    cache_eviction_manager: CacheEvictionManager,
    upload_loop: Arc<WorkerLoop>,
    metrics_loop: Arc<WorkerLoop>,
    rw_loop_queue_cf: RocksStoreRWLoop,
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
        let details = Arc::new(RocksCacheStoreDetails::new(&config));
        Self::new_from_store(RocksStore::with_listener(
            path,
            vec![],
            metastore_fs,
            config,
            details,
        )?)
    }

    fn new_from_store(store: Arc<RocksStore>) -> Result<Arc<Self>, CubeError> {
        let cache_eviction_manager = CacheEvictionManager::new(&store.config);

        Ok(Arc::new(Self {
            store,
            cache_eviction_manager,
            upload_loop: Arc::new(WorkerLoop::new("Cachestore upload")),
            metrics_loop: Arc::new(WorkerLoop::new("Cachestore metrics")),
            rw_loop_queue_cf: RocksStoreRWLoop::new("cachestore", "queue"),
        }))
    }

    pub async fn load_from_dump(
        path: &Path,
        dump_path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let details = Arc::new(RocksCacheStoreDetails::new(&config));

        let store =
            RocksStore::load_from_dump(path, dump_path, metastore_fs, config, details).await?;

        Self::new_from_store(store)
    }

    pub async fn load_from_remote(
        path: &str,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let details = Arc::new(RocksCacheStoreDetails::new(&config));
        let store = metastore_fs
            .load_from_remote(&path, config, details)
            .await?;

        Self::new_from_store(store)
    }

    async fn run_ttl_persist(&self) -> Result<(), CubeError> {
        self.cache_eviction_manager.run_persist(&self.store).await
    }

    async fn run_eviction(&self) -> Result<EvictionResult, CubeError> {
        self.cache_eviction_manager.run_eviction(&self.store).await
    }

    pub fn spawn_processing_loops(self: Arc<Self>) -> Vec<JoinHandle<Result<(), CubeError>>> {
        let mut loops = vec![];

        if self.store.config.upload_to_remote() {
            let upload_interval = self.store.config.cachestore_log_upload_interval();
            let cachestore = self.clone();
            loops.push(cube_ext::spawn(async move {
                cachestore
                    .upload_loop
                    .process(
                        cachestore.clone(),
                        move |_| async move {
                            Ok(Delay::new(Duration::from_secs(upload_interval)).await)
                        },
                        move |m, _| async move { m.store.run_upload().await },
                    )
                    .await;

                Ok(())
            }))
        } else {
            log::info!("Not running cachestore upload loop");
        }

        let metrics_interval = self.store.config.cachestore_metrics_interval();
        if metrics_interval > 0 {
            let cachestore = self.clone();
            loops.push(cube_ext::spawn(async move {
                cachestore
                    .metrics_loop
                    .process(
                        cachestore.clone(),
                        move |_| async move {
                            Ok(Delay::new(Duration::from_secs(metrics_interval)).await)
                        },
                        move |m, _| async move {
                            if let Err(err) = m.submit_metrics().await {
                                log::error!("Error while submitting cachestore metrics: {}", err)
                            };

                            Ok(())
                        },
                    )
                    .await;

                Ok(())
            }))
        } else {
            log::info!("Not running cachestore metrics loop");
        }

        let persist_interval = self
            .store
            .config
            .cachestore_cache_ttl_persist_loop_interval();
        if persist_interval > 0 {
            let cachestore = self.clone();
            loops.push(cube_ext::spawn(async move {
                cachestore
                    .cache_eviction_manager
                    .persist_loop
                    .process(cachestore.clone(), async move |m| m.run_ttl_persist().await)
                    .await;

                Ok(())
            }))
        } else {
            log::info!("Not running cachestore persist loop");
        }

        let eviction_interval = self.store.config.cachestore_cache_eviction_loop_interval();
        if eviction_interval > 0 {
            let cachestore = self.clone();
            loops.push(cube_ext::spawn(async move {
                cachestore
                    .cache_eviction_manager
                    .eviction_loop
                    .process(cachestore.clone(), async move |m| {
                        let _ = m.run_eviction().await?;

                        Ok(())
                    })
                    .await;

                Ok(())
            }))
        } else {
            log::info!("Not running cachestore eviction loop");
        }

        loops
    }

    pub async fn submit_metrics(&self) -> Result<(), CubeError> {
        app_metrics::CACHESTORE_ROCKSDB_ESTIMATE_LIVE_DATA_SIZE.report(
            self.store
                .db
                .property_int_value(rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE)?
                .unwrap_or(0) as i64,
        );

        app_metrics::CACHESTORE_ROCKSDB_LIVE_SST_FILES_SIZE.report(
            self.store
                .db
                .property_int_value(rocksdb::properties::LIVE_SST_FILES_SIZE)?
                .unwrap_or(0) as i64,
        );

        let cf_metadata = self.store.db.get_column_family_metadata();

        app_metrics::CACHESTORE_ROCKSDB_CF_DEFAULT_SIZE.report(cf_metadata.size as i64);

        Ok(())
    }

    fn send_zero_gauge_stats(&self) {
        app_metrics::CACHESTORE_ROCKSDB_ESTIMATE_LIVE_DATA_SIZE.report(0);
        app_metrics::CACHESTORE_ROCKSDB_LIVE_SST_FILES_SIZE.report(0);
        app_metrics::CACHESTORE_ROCKSDB_CF_DEFAULT_SIZE.report(0);
    }

    pub async fn stop_processing_loops(&self) {
        self.send_zero_gauge_stats();

        self.cache_eviction_manager.stop_processing_loops();
        self.upload_loop.stop();
        self.metrics_loop.stop();
    }

    pub async fn add_listener(&self, listener: Sender<MetaStoreEvent>) {
        self.store.add_listener(listener).await;
    }

    pub fn prepare_bench_cachestore(
        test_name: &str,
        config: Config,
    ) -> (Arc<LocalDirRemoteFs>, Arc<Self>) {
        let store_path = env::current_dir()
            .unwrap()
            .join("db-tmp")
            .join("benchmarks")
            .join(format!("{}", test_name));
        let _ = std::fs::remove_dir_all(store_path.clone());

        Self::prepare_test_cachestore_impl(test_name, store_path, config)
    }

    pub fn prepare_test_cachestore(
        test_name: &str,
        config: Config,
    ) -> (Arc<LocalDirRemoteFs>, Arc<Self>) {
        let store_path = env::current_dir()
            .unwrap()
            .join("db-tmp")
            .join("tests")
            .join(format!("{}-local", test_name));
        let _ = std::fs::remove_dir_all(store_path.clone());

        Self::prepare_test_cachestore_impl(test_name, store_path, config)
    }

    pub fn prepare_test_cachestore_from_fixtures(
        test_name: &str,
        remote_fixtures: &str,
        config: Config,
    ) -> (Arc<LocalDirRemoteFs>, Arc<Self>) {
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-local", test_name));
        let _ = std::fs::remove_dir_all(store_path.clone());

        let fixtures_path = env::current_dir()
            .unwrap()
            .join("testing-fixtures")
            .join(remote_fixtures);

        fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
            std::fs::create_dir_all(&dst)?;

            for entry in std::fs::read_dir(src)? {
                let entry = entry?;
                let ty = entry.file_type()?;
                if ty.is_dir() {
                    copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
                } else {
                    std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
                }
            }

            Ok(())
        }

        copy_dir_all(&fixtures_path, store_path.join("cachestore")).unwrap();

        Self::prepare_test_cachestore_impl(test_name, store_path, config)
    }

    fn prepare_test_cachestore_impl(
        test_name: &str,
        store_path: PathBuf,
        config: Config,
    ) -> (Arc<LocalDirRemoteFs>, Arc<Self>) {
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-remote", test_name));

        let _ = std::fs::remove_dir_all(remote_store_path.clone());

        let config_obj = config.config_obj();
        let details = Arc::new(RocksCacheStoreDetails::new(&config_obj));
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        let store = RocksStore::new(
            store_path.clone().join(details.get_name()).as_path(),
            BaseRocksStoreFs::new_for_cachestore(remote_fs.clone(), config_obj.clone()),
            config_obj,
            details,
        )
        .unwrap();

        (remote_fs, Self::new_from_store(store).unwrap())
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
    #[inline(always)]
    pub async fn write_operation_queue<F, R>(
        &self,
        op_name: &'static str,
        f: F,
    ) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>, &'a mut BatchPipe) -> Result<R, CubeError>
            + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
    {
        self.store
            .write_operation_impl::<F, R>(&self.rw_loop_queue_cf, op_name, f)
            .await
    }

    #[inline(always)]
    pub async fn read_operation_queue<F, R>(
        &self,
        op_name: &'static str,
        f: F,
    ) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        self.store
            .read_operation_impl::<F, R>(&self.rw_loop_queue_cf, op_name, f)
            .await
    }

    async fn queue_result_delete_by_id(&self, id: u64) -> Result<(), CubeError> {
        self.write_operation_queue("queue_result_delete_by_id", move |db_ref, batch_pipe| {
            let result_schema = QueueResultRocksTable::new(db_ref.clone());
            result_schema.try_delete(id, batch_pipe)?;

            Ok(())
        })
        .await
    }

    /// This method should be called when we are sure that we return data to the consumer
    async fn queue_result_ready_to_delete(&self, id: u64) -> Result<(), CubeError> {
        self.write_operation_queue("queue_result_ready_to_delete", move |db_ref, batch_pipe| {
            let result_schema = QueueResultRocksTable::new(db_ref.clone());
            if let Some(row) = result_schema.get_row(id)? {
                Self::queue_result_ready_to_delete_impl(&result_schema, batch_pipe, row)?;
            }

            Ok(())
        })
        .await
    }

    /// This method should be called when we are sure that we return data to the consumer
    fn queue_result_ready_to_delete_impl(
        result_schema: &QueueResultRocksTable,
        batch_pipe: &mut BatchPipe,
        queue_result: IdRow<QueueResult>,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        if queue_result.get_row().is_deleted() {
            return Ok(Some(QueueResultResponse::Success {
                value: Some(queue_result.into_row().value),
            }));
        }

        let row_id = queue_result.get_id();
        let row = queue_result.into_row();
        let mut new_row = row.clone();
        new_row.deleted = true;

        // TODO: Partial update? Index?
        let queue_result = result_schema.update(row_id, new_row, &row, batch_pipe)?;

        Ok(Some(QueueResultResponse::Success {
            value: Some(queue_result.into_row().value),
        }))
    }

    async fn lookup_queue_result_by_key(
        &self,
        key: QueueKey,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        self.write_operation_queue("lookup_queue_result_by_key", move |db_ref, batch_pipe| {
            let result_schema = QueueResultRocksTable::new(db_ref.clone());
            let query_key_is_path = key.is_path();
            let queue_result = result_schema.get_row_by_key(key.clone())?;

            if let Some(queue_result) = queue_result {
                if query_key_is_path {
                    if queue_result.get_row().is_deleted() {
                        Ok(None)
                    } else {
                        Self::queue_result_ready_to_delete_impl(
                            &result_schema,
                            batch_pipe,
                            queue_result,
                        )
                    }
                } else {
                    Ok(Some(QueueResultResponse::Success {
                        value: Some(queue_result.into_row().value),
                    }))
                }
            } else {
                Ok(None)
            }
        })
        .await
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DeepSizeOf)]
pub enum QueueKey {
    ById(u64),
    ByPath(String),
}

impl QueueKey {
    pub(crate) fn is_path(&self) -> bool {
        match self {
            QueueKey::ByPath(_) => true,
            QueueKey::ById(_) => false,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct CachestoreInfo {
    pub tables: Vec<RocksTableStats>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueAddResponse {
    pub id: u64,
    pub added: bool,
    pub pending: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueAddPayload {
    pub path: String,
    pub value: String,
    pub priority: i64,
    pub orphaned: Option<u32>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueCancelResponse {
    pub extra: Option<String>,
    pub value: String,
}

impl QueueCancelResponse {
    pub fn into_queue_cancel_row(self) -> Row {
        let res = vec![
            TableValue::String(self.value),
            if let Some(extra) = self.extra {
                TableValue::String(extra)
            } else {
                TableValue::Null
            },
        ];

        Row::new(res)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
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

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum QueueListItem {
    ItemOnly(IdRow<QueueItem>),
    WithPayload(IdRow<QueueItem>, String),
}

impl QueueListItem {
    pub fn into_queue_list_row(self) -> Row {
        let (id_row, payload) = match self {
            QueueListItem::ItemOnly(id_row) => (id_row, None),
            QueueListItem::WithPayload(id_row, payload) => (id_row, Some(payload)),
        };

        let row_id = id_row.get_id();
        let row = id_row.into_row();

        let mut res = vec![
            TableValue::String(row.key),
            TableValue::String(row_id.to_string()),
            TableValue::String(row.status.to_string()),
            if let Some(extra) = row.extra {
                TableValue::String(extra)
            } else {
                TableValue::Null
            },
        ];

        if let Some(payload) = payload {
            res.push(TableValue::String(payload));
        };

        Row::new(res)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueGetResponse {
    extra: Option<String>,
    payload: String,
}

impl QueueGetResponse {
    pub fn into_queue_get_row(self) -> Row {
        let res = vec![
            TableValue::String(self.payload),
            if let Some(extra) = self.extra {
                TableValue::String(extra)
            } else {
                TableValue::Null
            },
        ];

        Row::new(res)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueAllItem {
    pub item: IdRow<QueueItem>,
    pub payload: Option<String>,
}

#[cuberpc::service]
pub trait CacheStore: DIService + Send + Sync {
    // cache
    async fn cache_all(&self, limit: Option<usize>) -> Result<Vec<IdRow<CacheItem>>, CubeError>;
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
    async fn queue_all(&self, limit: Option<usize>) -> Result<Vec<QueueAllItem>, CubeError>;
    async fn queue_results_all(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<IdRow<QueueResult>>, CubeError>;
    async fn queue_results_multi_delete(&self, ids: Vec<u64>) -> Result<(), CubeError>;
    async fn queue_add(&self, payload: QueueAddPayload) -> Result<QueueAddResponse, CubeError>;
    async fn queue_truncate(&self) -> Result<(), CubeError>;
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
        with_payload: bool,
    ) -> Result<Vec<QueueListItem>, CubeError>;
    // API with Path
    async fn queue_get(&self, key: QueueKey) -> Result<Option<QueueGetResponse>, CubeError>;
    async fn queue_cancel(&self, key: QueueKey) -> Result<Option<QueueCancelResponse>, CubeError>;
    async fn queue_heartbeat(&self, key: QueueKey) -> Result<(), CubeError>;
    async fn queue_retrieve_by_path(
        &self,
        path: String,
        allow_concurrency: u32,
    ) -> Result<QueueRetrieveResponse, CubeError>;
    async fn queue_ack(&self, key: QueueKey, result: Option<String>) -> Result<bool, CubeError>;
    async fn queue_result_by_path(
        &self,
        path: String,
    ) -> Result<Option<QueueResultResponse>, CubeError>;
    async fn queue_result_blocking(
        &self,
        key: QueueKey,
        timeout: u64,
    ) -> Result<Option<QueueResultResponse>, CubeError>;
    async fn queue_merge_extra(&self, key: QueueKey, payload: String) -> Result<(), CubeError>;

    // Force compaction for the whole RocksDB
    async fn compaction(&self) -> Result<(), CubeError>;
    // DB stats + sizes
    async fn info(&self) -> Result<CachestoreInfo, CubeError>;
    // Force run for eviction
    async fn eviction(&self) -> Result<EvictionResult, CubeError>;
    // Force run for persist of lru/lfu stats
    async fn persist(&self) -> Result<(), CubeError>;
    async fn healthcheck(&self) -> Result<(), CubeError>;
    async fn rocksdb_properties(&self) -> Result<Vec<RocksPropertyRow>, CubeError>;
}

#[async_trait]
impl CacheStore for RocksCacheStore {
    async fn cache_all(&self, limit: Option<usize>) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.store
            .read_operation_out_of_queue("cache_all", move |db_ref| {
                Ok(CacheItemRocksTable::new(db_ref).scan_rows(limit)?)
            })
            .await
    }

    async fn cache_set(
        &self,
        item: CacheItem,
        update_if_not_exists: bool,
    ) -> Result<bool, CubeError> {
        if item.get_value().len() >= self.store.config.cachestore_cache_max_entry_size() {
            return Err(CubeError::user(format!(
                "Unable to SET cache with '{}' key, exceeds maximum allowed size for payload: {}, max allowed: {}",
                item.key,
                humansize::format_size(item.get_value().len(), humansize::DECIMAL),
                humansize::format_size(self.store.config.cachestore_cache_max_entry_size(), humansize::DECIMAL),
            )));
        }

        self.cache_eviction_manager
            .before_insert(item.get_value().len() as u64)
            .await?;

        let (result, inserted) = self
            .store
            .write_operation("cache_set", move |db_ref, batch_pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let index_key = CacheItemIndexKey::ByPath(item.get_path());
                let id_row_opt = cache_schema
                    .get_single_opt_row_by_index(&index_key, &CacheItemRocksIndex::ByPath)?;

                if let Some(id_row) = id_row_opt {
                    if update_if_not_exists {
                        return Ok((false, None));
                    };

                    cache_schema.update(id_row.id, item, &id_row.row, batch_pipe)?;
                    Ok((true, None))
                } else {
                    let raw_size = item.get_value().len();

                    cache_schema.insert(item, batch_pipe)?;
                    Ok((true, Some(raw_size)))
                }
            })
            .await?;

        if let Some(raw_size) = inserted {
            self.cache_eviction_manager.notify_insert(raw_size as u64)?;
        }

        Ok(result)
    }

    async fn cache_truncate(&self) -> Result<(), CubeError> {
        let block = self.cache_eviction_manager.truncation_block().await;

        let result = self
            .store
            .write_operation("cache_truncate", move |db_ref, batch_pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref);
                cache_schema.truncate(batch_pipe)?;

                Ok(())
            })
            .await;

        self.cache_eviction_manager.notify_truncate_end().await?;
        drop(block);

        result
    }

    async fn cache_delete(&self, key: String) -> Result<(), CubeError> {
        let result = self
            .store
            .write_operation("cache_delete", move |db_ref, batch_pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let index_key = CacheItemIndexKey::ByPath(key);
                let row_opt = cache_schema
                    .get_single_opt_row_by_index(&index_key, &CacheItemRocksIndex::ByPath)?;

                if let Some(row) = row_opt {
                    let row_id = row.id;
                    let raw_size = row.get_row().get_value().len();

                    cache_schema.delete_row(row, batch_pipe)?;

                    Ok(Some((row_id, raw_size)))
                } else {
                    Ok(None)
                }
            })
            .await?;

        if let Some((row_id, raw_size)) = result {
            self.cache_eviction_manager
                .notify_delete(row_id, raw_size as u64)?;
        }

        Ok(())
    }

    async fn cache_get(&self, key: String) -> Result<Option<IdRow<CacheItem>>, CubeError> {
        let res = self
            .store
            .read_operation("cache_get", move |db_ref| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let index_key = CacheItemIndexKey::ByPath(key);
                let id_row_opt = cache_schema
                    .get_single_opt_row_by_index(&index_key, &CacheItemRocksIndex::ByPath)?;

                Ok(id_row_opt)
            })
            .await?;

        if let Some(item) = &res {
            self.cache_eviction_manager.notify_lookup(item)?;
        };

        Ok(res)
    }

    async fn cache_keys(&self, prefix: String) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.store
            .read_operation("cache_keys", move |db_ref| {
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
        let item = self
            .store
            .write_operation("cache_incr", move |db_ref, batch_pipe| {
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
            .await?;

        self.cache_eviction_manager.notify_lookup(&item)?;

        Ok(item)
    }

    async fn queue_all(&self, limit: Option<usize>) -> Result<Vec<QueueAllItem>, CubeError> {
        self.store
            .read_operation("queue_all", move |db_ref| {
                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let queue_payload_schema = QueueItemPayloadRocksTable::new(db_ref.clone());

                let mut res = Vec::new();

                for item in queue_schema.scan_rows(limit)? {
                    let payload =
                        if let Some(payload) = queue_payload_schema.get_row(item.get_id())? {
                            Some(payload.into_row().value)
                        } else {
                            error!(
                                "Unable to find payload for queue item, id = {}",
                                item.get_id()
                            );

                            None
                        };

                    res.push(QueueAllItem { item, payload })
                }

                Ok(res)
            })
            .await
    }

    async fn queue_results_all(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<IdRow<QueueResult>>, CubeError> {
        self.read_operation_queue("queue_results_all", move |db_ref| {
            Ok(QueueResultRocksTable::new(db_ref).scan_rows(limit)?)
        })
        .await
    }

    async fn queue_results_multi_delete(&self, ids: Vec<u64>) -> Result<(), CubeError> {
        self.write_operation_queue("queue_results_multi_delete", move |db_ref, batch_pipe| {
            let queue_result_schema = QueueResultRocksTable::new(db_ref);

            for id in ids {
                queue_result_schema.try_delete(id, batch_pipe)?;
            }

            Ok(())
        })
        .await
    }

    async fn queue_add(&self, payload: QueueAddPayload) -> Result<QueueAddResponse, CubeError> {
        self.write_operation_queue("queue_add", move |db_ref, batch_pipe| {
            let queue_schema = QueueItemRocksTable::new(db_ref.clone());
            let pending = queue_schema.count_rows_by_index(
                &QueueItemIndexKey::ByPrefixAndStatus(
                    QueueItem::extract_prefix(payload.path.clone()).unwrap_or("".to_string()),
                    QueueItemStatus::Pending,
                ),
                &QueueItemRocksIndex::ByPrefixAndStatus,
            )?;

            let index_key = QueueItemIndexKey::ByPath(payload.path.clone());
            let id_row_opt = queue_schema
                .get_single_opt_row_by_index(&index_key, &QueueItemRocksIndex::ByPath)?;

            let (id, added) = if let Some(row) = id_row_opt {
                (row.id, false)
            } else {
                let queue_item_row = queue_schema.insert(
                    QueueItem::new(
                        payload.path,
                        QueueItem::status_default(),
                        payload.priority,
                        payload.orphaned.clone(),
                    ),
                    batch_pipe,
                )?;
                let queue_payload_schema = QueueItemPayloadRocksTable::new(db_ref.clone());
                queue_payload_schema.insert_with_pk(
                    queue_item_row.id,
                    QueueItemPayload::new(
                        payload.value,
                        queue_item_row.row.get_created().clone(),
                        queue_item_row.row.get_expire().clone(),
                    ),
                    batch_pipe,
                )?;

                (queue_item_row.id, true)
            };

            Ok(QueueAddResponse {
                id,
                added,
                pending: if added { pending + 1 } else { pending },
            })
        })
        .await
    }

    async fn queue_truncate(&self) -> Result<(), CubeError> {
        self.write_operation_queue("queue_truncate", move |db_ref, batch_pipe| {
            let queue_item_schema = QueueItemRocksTable::new(db_ref.clone());
            queue_item_schema.truncate(batch_pipe)?;

            let queue_item_payload_schema = QueueItemPayloadRocksTable::new(db_ref.clone());
            queue_item_payload_schema.truncate(batch_pipe)?;

            let queue_result_schema = QueueResultRocksTable::new(db_ref);
            queue_result_schema.truncate(batch_pipe)?;

            Ok(())
        })
        .await?;

        Ok(())
    }

    async fn queue_to_cancel(
        &self,
        prefix: String,
        orphaned_timeout: Option<u32>,
        heartbeat_timeout: Option<u32>,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        self.read_operation_queue("queue_to_cancel", move |db_ref| {
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
        with_payload: bool,
    ) -> Result<Vec<QueueListItem>, CubeError> {
        self.read_operation_queue("queue_list", move |db_ref| {
            let queue_schema = QueueItemRocksTable::new(db_ref.clone());

            let items = if let Some(status_filter) = status_filter {
                let index_key = QueueItemIndexKey::ByPrefixAndStatus(prefix, status_filter);
                queue_schema
                    .get_rows_by_index(&index_key, &QueueItemRocksIndex::ByPrefixAndStatus)?
            } else {
                let index_key = QueueItemIndexKey::ByPrefix(prefix);
                queue_schema.get_rows_by_index(&index_key, &QueueItemRocksIndex::ByPrefix)?
            };

            let items = if priority_sort {
                items
                    .into_iter()
                    .sorted_by(|a, b| b.row.cmp(&a.row))
                    .collect()
            } else {
                items
            };

            if with_payload {
                let queue_payload_schema = QueueItemPayloadRocksTable::new(db_ref.clone());
                let mut res = Vec::with_capacity(items.len());

                for item in items {
                    if let Some(payload_row) = queue_payload_schema.get_row(item.get_id())? {
                        res.push(QueueListItem::WithPayload(
                            item,
                            payload_row.into_row().value,
                        ));
                    } else {
                        res.push(QueueListItem::ItemOnly(item));
                    }
                }

                Ok(res)
            } else {
                Ok(items
                    .into_iter()
                    .map(|item| QueueListItem::ItemOnly(item))
                    .collect())
            }
        })
        .await
    }

    async fn queue_get(&self, key: QueueKey) -> Result<Option<QueueGetResponse>, CubeError> {
        self.read_operation_queue("queue_get", move |db_ref| {
            let queue_schema = QueueItemRocksTable::new(db_ref.clone());

            if let Some(item_row) = queue_schema.get_row_by_key(key)? {
                let queue_payload_schema = QueueItemPayloadRocksTable::new(db_ref.clone());

                if let Some(payload_row) = queue_payload_schema.get_row(item_row.get_id())? {
                    Ok(Some(QueueGetResponse {
                        extra: item_row.into_row().extra,
                        payload: payload_row.into_row().value,
                    }))
                } else {
                    error!(
                        "Unable to find payload for queue item, id = {}",
                        item_row.get_id()
                    );

                    Ok(None)
                }
            } else {
                Ok(None)
            }
        })
        .await
    }

    async fn queue_cancel(&self, key: QueueKey) -> Result<Option<QueueCancelResponse>, CubeError> {
        self.write_operation_queue("queue_cancel", move |db_ref, batch_pipe| {
            let queue_schema = QueueItemRocksTable::new(db_ref.clone());
            let queue_payload_schema = QueueItemPayloadRocksTable::new(db_ref.clone());

            if let Some(id_row) = queue_schema.get_row_by_key(key)? {
                let row_id = id_row.get_id();
                let queue_item = queue_schema.delete_row(id_row, batch_pipe)?;

                if let Some(queue_payload) = queue_payload_schema.try_delete(row_id, batch_pipe)? {
                    Ok(Some(QueueCancelResponse {
                        extra: queue_item.into_row().extra,
                        value: queue_payload.into_row().value,
                    }))
                } else {
                    error!("Unable to find payload for queue item, id = {}", row_id);

                    Ok(None)
                }
            } else {
                Ok(None)
            }
        })
        .await
    }

    async fn queue_heartbeat(&self, key: QueueKey) -> Result<(), CubeError> {
        self.write_operation_queue("queue_heartbeat", move |db_ref, batch_pipe| {
            let queue_schema = QueueItemRocksTable::new(db_ref.clone());
            let id_row_opt = queue_schema.get_row_by_key(key.clone())?;

            if let Some(id_row) = id_row_opt {
                let mut new = id_row.get_row().clone();
                new.update_heartbeat();

                queue_schema.update(id_row.id, new, id_row.get_row(), batch_pipe)?;
                Ok(())
            } else {
                trace!("Unable to update heartbeat, unknown key: {:?}", key);

                Ok(())
            }
        })
        .await
    }

    async fn queue_retrieve_by_path(
        &self,
        path: String,
        allow_concurrency: u32,
    ) -> Result<QueueRetrieveResponse, CubeError> {
        self.write_operation_queue("queue_retrieve_by_path", move |db_ref, batch_pipe| {
            let queue_schema = QueueItemRocksTable::new(db_ref.clone());
            let prefix = QueueItem::parse_path(path.clone())
                .0
                .unwrap_or("".to_string());
            let mut pending = queue_schema.count_rows_by_index(
                &QueueItemIndexKey::ByPrefixAndStatus(prefix.clone(), QueueItemStatus::Pending),
                &QueueItemRocksIndex::ByPrefixAndStatus,
            )?;

            let mut active: Vec<String> = queue_schema
                .get_rows_by_index(
                    &QueueItemIndexKey::ByPrefixAndStatus(prefix, QueueItemStatus::Active),
                    &QueueItemRocksIndex::ByPrefixAndStatus,
                )?
                .into_iter()
                .map(|item| item.into_row().key)
                .collect();
            if active.len() >= (allow_concurrency as usize) {
                return Ok(QueueRetrieveResponse::NotEnoughConcurrency { pending, active });
            }

            let id_row = queue_schema.get_single_opt_row_by_index(
                &QueueItemIndexKey::ByPath(path.clone()),
                &QueueItemRocksIndex::ByPath,
            )?;
            let id_row = if let Some(id_row) = id_row {
                id_row
            } else {
                return Ok(QueueRetrieveResponse::NotFound { pending, active });
            };

            if id_row.get_row().get_status() == &QueueItemStatus::Pending {
                let mut new = id_row.get_row().clone();
                new.status = QueueItemStatus::Active;
                // It's  important to insert heartbeat, because
                // without that created datetime will be used for orphaned filtering
                new.update_heartbeat();

                let queue_payload_schema = QueueItemPayloadRocksTable::new(db_ref.clone());

                let res =
                    queue_schema.update(id_row.get_id(), new, id_row.get_row(), batch_pipe)?;
                let payload = if let Some(r) = queue_payload_schema.get_row(res.get_id())? {
                    r.into_row().value
                } else {
                    error!(
                        "Unable to find payload for queue item, id = {}",
                        res.get_id()
                    );

                    queue_schema.delete_row(res, batch_pipe)?;

                    return Ok(QueueRetrieveResponse::NotFound { pending, active });
                };

                active.push(res.get_row().get_key().clone());
                pending -= 1;
                Ok(QueueRetrieveResponse::Success {
                    id: id_row.get_id(),
                    payload,
                    item: res.into_row(),
                    pending,
                    active,
                })
            } else {
                Ok(QueueRetrieveResponse::LockFailed { pending, active })
            }
        })
        .await
    }

    async fn queue_ack(&self, key: QueueKey, result: Option<String>) -> Result<bool, CubeError> {
        self.write_operation_queue("queue_ack", move |db_ref, batch_pipe| {
            let queue_item_tbl = QueueItemRocksTable::new(db_ref.clone());
            let queue_item_payload_tbl = QueueItemPayloadRocksTable::new(db_ref.clone());

            let item_row = queue_item_tbl.get_row_by_key(key.clone())?;
            if let Some(item_row) = item_row {
                let path = item_row.get_row().get_path();
                let id = item_row.get_id();

                queue_item_tbl.delete_row(item_row, batch_pipe)?;
                queue_item_payload_tbl.try_delete(id, batch_pipe)?;

                if let Some(result) = result {
                    let queue_result = QueueResult::new(path.clone(), result);
                    let result_schema = QueueResultRocksTable::new(db_ref.clone());
                    // QueueResult is a result of QueueItem, it's why we can use row_id of QueueItem
                    let result_row = result_schema.insert_with_pk(id, queue_result, batch_pipe)?;

                    batch_pipe.add_event(MetaStoreEvent::AckQueueItem(QueueResultAckEvent {
                        id,
                        path,
                        result: QueueResultAckEventResult::WithResult {
                            result: Arc::new(result_row.into_row().value),
                        },
                    }));
                } else {
                    batch_pipe.add_event(MetaStoreEvent::AckQueueItem(QueueResultAckEvent {
                        id,
                        path,
                        result: QueueResultAckEventResult::Empty {},
                    }));
                }

                Ok(true)
            } else {
                warn!("Unable to ack queue, unknown key: {:?}", key);

                Ok(false)
            }
        })
        .await
    }

    async fn queue_result_by_path(
        &self,
        path: String,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        self.lookup_queue_result_by_key(QueueKey::ByPath(path))
            .await
    }

    async fn queue_result_blocking(
        &self,
        key: QueueKey,
        timeout: u64,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        // It's important to open listener at the beginning to protect race condition
        // it will fix the position (subscribe) of a broadcast channel
        let listener = self.get_listener().await;

        let store_in_result = self.lookup_queue_result_by_key(key.clone()).await?;
        if store_in_result.is_some() {
            return Ok(store_in_result);
        }

        let query_key_is_path = key.is_path();
        let fut = tokio::time::timeout(
            Duration::from_millis(timeout),
            listener.wait_for_queue_ack_by_key(key),
        );

        if let Ok(res) = fut.await {
            match res {
                Ok(Some(ack_event)) => match ack_event.result {
                    QueueResultAckEventResult::Empty => {
                        Ok(Some(QueueResultResponse::Success { value: None }))
                    }
                    QueueResultAckEventResult::WithResult { result } => {
                        if query_key_is_path {
                            // Queue v1 behaviour
                            self.queue_result_delete_by_id(ack_event.id).await?;
                        } else {
                            // Queue v2 behaviour
                            self.queue_result_ready_to_delete(ack_event.id).await?;
                        }

                        Ok(Some(QueueResultResponse::Success {
                            value: Some(result.to_string()),
                        }))
                    }
                },
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        } else {
            Ok(None)
        }
    }

    async fn queue_merge_extra(&self, key: QueueKey, payload: String) -> Result<(), CubeError> {
        self.write_operation_queue("queue_merge_extra", move |db_ref, batch_pipe| {
            let queue_schema = QueueItemRocksTable::new(db_ref.clone());

            let id_row_opt = queue_schema.get_row_by_key(key.clone())?;

            if let Some(id_row) = id_row_opt {
                let new = id_row.get_row().merge_extra(payload)?;

                queue_schema.update(id_row.id, new, id_row.get_row(), batch_pipe)?;
            } else {
                warn!("Unable to merge extra, unknown key: {:?}", key);
            }

            Ok(())
        })
        .await
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        self.store
            .read_operation_out_of_queue("compaction", move |db_ref| {
                let start: Option<&[u8]> = None;
                let end: Option<&[u8]> = None;

                db_ref.db.compact_range(start, end);

                Ok(())
            })
            .await?;

        Ok(())
    }

    async fn info(&self) -> Result<CachestoreInfo, CubeError> {
        self.store
            .read_operation_out_of_queue("info", move |db_ref| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let cache_schema_stats = cache_schema.collect_table_stats_by_extended_index(
                    &CacheItemRocksIndex::ByPath,
                    CACHE_ITEM_SIZE_WITHOUT_VALUE as u64,
                )?;

                let queue_schema = QueueItemRocksTable::new(db_ref.clone());
                let queue_schema_stats = queue_schema.collect_table_stats_by_extended_index(
                    &QueueItemRocksIndex::ByPath,
                    0 as u64,
                )?;

                let queue_result_schema = QueueResultRocksTable::new(db_ref.clone());
                let queue_result_stats = queue_result_schema
                    .collect_table_stats_by_extended_index(
                        &QueueResultRocksIndex::ByPath,
                        0 as u64,
                    )?;

                Ok(CachestoreInfo {
                    tables: vec![cache_schema_stats, queue_schema_stats, queue_result_stats],
                })
            })
            .await
    }

    async fn eviction(&self) -> Result<EvictionResult, CubeError> {
        self.run_eviction().await
    }

    async fn persist(&self) -> Result<(), CubeError> {
        self.run_ttl_persist().await
    }

    async fn healthcheck(&self) -> Result<(), CubeError> {
        self.store.healthcheck().await?;

        Ok(())
    }

    async fn rocksdb_properties(&self) -> Result<Vec<RocksPropertyRow>, CubeError> {
        self.store.rocksdb_properties()
    }
}

crate::di_service!(RocksCacheStore, [CacheStore]);
crate::di_service!(CacheStoreRpcClient, [CacheStore]);

pub struct ClusterCacheStoreClient {}

#[async_trait]
impl CacheStore for ClusterCacheStoreClient {
    async fn cache_all(&self, _limit: Option<usize>) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
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

    async fn queue_all(&self, _limit: Option<usize>) -> Result<Vec<QueueAllItem>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_all was used.")
    }

    async fn queue_results_all(
        &self,
        _limit: Option<usize>,
    ) -> Result<Vec<IdRow<QueueResult>>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_results_all was used.")
    }

    async fn queue_results_multi_delete(&self, _ids: Vec<u64>) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_results_multi_delete was used.")
    }

    async fn queue_add(&self, _payload: QueueAddPayload) -> Result<QueueAddResponse, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_add was used.")
    }

    async fn queue_truncate(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_truncate was used.")
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
        _with_payload: bool,
    ) -> Result<Vec<QueueListItem>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_list was used.")
    }

    async fn queue_get(&self, _key: QueueKey) -> Result<Option<QueueGetResponse>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_get was used.")
    }

    async fn queue_cancel(&self, _key: QueueKey) -> Result<Option<QueueCancelResponse>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_cancel was used.")
    }

    async fn queue_heartbeat(&self, _key: QueueKey) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_heartbeat_by_id was used.")
    }

    async fn queue_retrieve_by_path(
        &self,
        _path: String,
        _allow_concurrency: u32,
    ) -> Result<QueueRetrieveResponse, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_retrieve_by_path was used.")
    }

    async fn queue_ack(&self, _key: QueueKey, _result: Option<String>) -> Result<bool, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_ack was used.")
    }

    async fn queue_result_by_path(
        &self,
        _path: String,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_result_by_path was used.")
    }

    async fn queue_result_blocking(
        &self,
        _key: QueueKey,
        _timeout: u64,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_result_blocking was used.")
    }

    async fn queue_merge_extra(&self, _key: QueueKey, _payload: String) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! queue_merge_extra was used.")
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! compaction was used.")
    }

    async fn info(&self) -> Result<CachestoreInfo, CubeError> {
        panic!("CacheStore cannot be used on the worker node! info was used.")
    }

    async fn eviction(&self) -> Result<EvictionResult, CubeError> {
        panic!("CacheStore cannot be used on the worker node! eviction was used.")
    }

    async fn persist(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! persist was used.")
    }

    async fn healthcheck(&self) -> Result<(), CubeError> {
        panic!("CacheStore cannot be used on the worker node! healthcheck was used.")
    }

    async fn rocksdb_properties(&self) -> Result<Vec<RocksPropertyRow>, CubeError> {
        panic!("CacheStore cannot be used on the worker node! rocksdb_properties was used.")
    }
}

crate::di_service!(ClusterCacheStoreClient, [CacheStore]);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cachestore::{CacheEvictionPolicy, EvictionFinishedResult};
    use crate::config::{init_test_logger, ConfigObjImpl, CubeServices};
    use crate::CubeError;

    #[tokio::test]
    async fn test_cachestore_migration() -> Result<(), CubeError> {
        init_test_logger().await;

        let (_, cachestore) = RocksCacheStore::prepare_test_cachestore_from_fixtures(
            "cachestore-migration",
            "cachestore-migration",
            Config::test("cachestore-migration"),
        );
        // Right now, this test is not complete, because there is a problem with error tracking on migration
        // TODO(ovr): fix me
        cachestore.check_all_indexes().await?;

        RocksCacheStore::cleanup_test_cachestore("cachestore-migration");

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_incr() -> Result<(), CubeError> {
        init_test_logger().await;

        let (_, cachestore) =
            RocksCacheStore::prepare_test_cachestore("cache_incr", Config::test("cachestore_incr"));

        let key = "prefix:key".to_string();
        assert_eq!(
            cachestore.cache_incr(key.clone()).await?.get_row().value,
            "1"
        );
        assert_eq!(cachestore.cache_incr(key).await?.get_row().value, "2");

        RocksCacheStore::cleanup_test_cachestore("cache_incr");

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_set() -> Result<(), CubeError> {
        init_test_logger().await;

        let (_, cachestore) =
            RocksCacheStore::prepare_test_cachestore("cache_set", Config::test("cachestore_set"));

        let path = "prefix:key-to-test-update".to_string();
        assert_eq!(
            cachestore
                .cache_set(
                    CacheItem::new(path.clone(), Some(60), "value1".to_string()),
                    false
                )
                .await?,
            true
        );

        assert_eq!(
            cachestore
                .cache_set(
                    CacheItem::new(path.clone(), Some(60), "value2".to_string()),
                    false
                )
                .await?,
            true
        );

        let row = cachestore
            .cache_get(path.clone())
            .await?
            .expect("must return row")
            .into_row();
        assert_eq!(row.get_path(), path);
        assert_eq!(row.value, "value2".to_string());
        assert_eq!(row.expire.is_some(), true);

        RocksCacheStore::cleanup_test_cachestore("cache_set");

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_set_max_entry_size() -> Result<(), CubeError> {
        init_test_logger().await;

        let config = Config::test("cachestore_set").update_config(|mut c| {
            c.cachestore_cache_max_entry_size = 1 << 20;
            c
        });

        let (_, cachestore) =
            RocksCacheStore::prepare_test_cachestore("cache_set_max_entry_size", config);

        let err = cachestore
            .cache_set(
                CacheItem::new(
                    "prefix:key-with-wrong-size".to_string(),
                    Some(60),
                    "a".repeat(2 << 20),
                ),
                false,
            )
            .await;

        assert_eq!(err, Err(CubeError::user(
            "Unable to SET cache with 'key-with-wrong-size' key, exceeds maximum allowed size for payload: 2.10 MB, max allowed: 1.05 MB".to_string()
        )));

        RocksCacheStore::cleanup_test_cachestore("cache_set_max_entry_size");

        Ok(())
    }

    async fn test_cachestore_force_eviction(
        name: &str,
        update_config: impl FnOnce(ConfigObjImpl) -> ConfigObjImpl,
        keys_to_insert: usize,
        key_size: usize,
    ) -> Result<EvictionFinishedResult, CubeError> {
        init_test_logger().await;

        let config = Config::test(name).update_config(update_config);

        let (_, cachestore) = RocksCacheStore::prepare_test_cachestore(name, config);

        let cachestore_to_move = cachestore.clone();

        tokio::task::spawn(async move {
            let loops = cachestore_to_move.spawn_processing_loops();
            CubeServices::wait_loops(loops).await
        });

        for i in (0..keys_to_insert).step_by(4) {
            let (r1, r2, r3, r4) = tokio::join!(
                cachestore.cache_set(
                    CacheItem::new(format!("test:{}", i), Some(3600), "a".repeat(key_size)),
                    false,
                ),
                cachestore.cache_set(
                    CacheItem::new(format!("test:{}", i + 1), Some(3600), "a".repeat(key_size)),
                    false,
                ),
                cachestore.cache_set(
                    CacheItem::new(format!("test:{}", i + 2), Some(3600), "a".repeat(key_size)),
                    false,
                ),
                cachestore.cache_set(
                    CacheItem::new(format!("test:{}", i + 3), Some(3600), "a".repeat(key_size)),
                    false,
                )
            );

            r1?;
            r2?;
            r3?;
            r4?;
        }

        let mut result = EvictionFinishedResult::empty();

        // 1 load state
        // check limits 3 (3 rounds for sampling)
        for _ in 0..4 {
            match cachestore.run_eviction().await? {
                EvictionResult::InProgress(status) => panic!("unexpected status: {}", status),
                EvictionResult::Finished(stats) => {
                    println!("eviction stats {:?}", stats);

                    result.add_eviction_result(stats);
                }
            };
        }

        // should not do anything
        {
            let eviction_results = match cachestore.run_eviction().await? {
                EvictionResult::InProgress(status) => panic!("unexpected status: {}", status),
                EvictionResult::Finished(stats) => stats,
            };
            assert_eq!(eviction_results.total_keys_removed, 0);
            assert_eq!(eviction_results.total_size_removed, 0);
            assert_eq!(eviction_results.total_delete_skipped, 0);

            result.stats_total_keys = eviction_results.stats_total_keys;
            result.stats_total_raw_size = eviction_results.stats_total_raw_size;
        }

        RocksCacheStore::cleanup_test_cachestore(name);

        Ok(result)
    }

    #[tokio::test]
    async fn test_cachestore_force_eviction_with_max_keys_limit_by_sampled_lru(
    ) -> Result<(), CubeError> {
        let result = test_cachestore_force_eviction(
            "cachestore_force_eviction_with_max_keys_limit_by_sampled_lru",
            |mut config| {
                // 512 as soft
                config.cachestore_cache_max_keys = 512;
                // 512 * 1.5 = 768
                config.cachestore_cache_threshold_to_force_eviction = 50;
                config.cachestore_cache_eviction_below_threshold = 15;
                config.cachestore_cache_max_size = 16384 << 20;
                config.cachestore_cache_policy = CacheEvictionPolicy::SampledLru;

                config
            },
            512 + 128,
            128 << 12,
        )
        .await?;

        // 640 -> 640 - (128 * 1.15 = 147.5) -> 492
        assert_eq!(result.stats_total_keys < 512, true);
        assert_eq!(result.stats_total_keys > 456, true);

        assert_eq!(result.total_keys_removed > 100, true);
        assert_eq!(result.total_keys_removed < 256, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_cachestore_force_eviction_with_max_keys_limit_by_allkeys_lru(
    ) -> Result<(), CubeError> {
        let result = test_cachestore_force_eviction(
            "cachestore_force_eviction_with_max_keys_limit_by_allkeys_lru",
            |mut config| {
                // 512 as soft
                config.cachestore_cache_max_keys = 512;
                // 512 * 1.5 = 768
                config.cachestore_cache_threshold_to_force_eviction = 50;
                config.cachestore_cache_eviction_below_threshold = 15;
                config.cachestore_cache_max_size = 16384 << 20;
                config.cachestore_cache_policy = CacheEvictionPolicy::AllKeysLru;

                config
            },
            512 + 128,
            128 << 12,
        )
        .await?;

        // 640 - (128 * 1.15 = 147.5) -> 492
        assert_eq!(result.stats_total_keys < 512, true);
        assert_eq!(result.stats_total_keys > 456, true);

        println!("result {:?}", result);
        assert_eq!(result.total_keys_removed > 100, true);
        assert_eq!(result.total_keys_removed < 256, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_cachestore_auto_eviction_with_max_keys_limit_by_allkeys_lru(
    ) -> Result<(), CubeError> {
        init_test_logger().await;

        let config =
            Config::test("test_cachestore_auto_eviction_with_max_keys_limit_by_allkeys_lru")
                .update_config(|mut config| {
                    // 512 as soft
                    config.cachestore_cache_max_keys = 512;
                    // 512 * 1.25 = 640
                    config.cachestore_cache_threshold_to_force_eviction = 25;
                    config.cachestore_cache_eviction_below_threshold = 15;
                    config.cachestore_cache_max_size = 16384 << 20;
                    config.cachestore_cache_policy = CacheEvictionPolicy::AllKeysLru;
                    // disable periodic eviction, this test should force eviction
                    config.cachestore_cache_eviction_loop_interval = 100000;

                    config
                });

        let (_, cachestore) = RocksCacheStore::prepare_test_cachestore(
            "test_cachestore_auto_eviction_with_max_keys_limit_by_allkeys_lru",
            config,
        );

        let cachestore_to_move = cachestore.clone();

        tokio::task::spawn(async move {
            let loops = cachestore_to_move.spawn_processing_loops();
            CubeServices::wait_loops(loops).await
        });

        for interval in 0..10 {
            for i in (0..300).step_by(4) {
                let (r1, r2, r3, r4) = tokio::join!(
                    cachestore.cache_set(
                        CacheItem::new(
                            format!("test:{}", (interval * 1000) + i),
                            Some(3600),
                            "a".repeat(1 << 12)
                        ),
                        false,
                    ),
                    cachestore.cache_set(
                        CacheItem::new(
                            format!("test:{}", (interval * 1000) + i + 1),
                            Some(3600),
                            "a".repeat(1 << 12)
                        ),
                        false,
                    ),
                    cachestore.cache_set(
                        CacheItem::new(
                            format!("test:{}", (interval * 1000) + i + 2),
                            Some(3600),
                            "a".repeat(1 << 12)
                        ),
                        false,
                    ),
                    cachestore.cache_set(
                        CacheItem::new(
                            format!("test:{}", (interval * 1000) + i + 3),
                            Some(3600),
                            "a".repeat(1 << 12)
                        ),
                        false,
                    )
                );

                r1?;
                r2?;
                r3?;
                r4?;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
            trace!(
                "after loop, total keys: {}",
                cachestore.cache_eviction_manager.get_stats_total_keys()
            );
            assert_eq!(
                cachestore.cache_eviction_manager.get_stats_total_keys() < 640,
                true
            );
            assert_eq!(
                cachestore.cache_eviction_manager.get_stats_total_keys() >= 300,
                true
            );
        }

        assert_eq!(
            cachestore.cache_eviction_manager.get_stats_total_keys() > 400,
            true
        );

        RocksCacheStore::cleanup_test_cachestore(
            "test_cachestore_auto_eviction_with_max_keys_limit_by_allkeys_lru",
        );

        Ok(())
    }

    #[test]
    fn test_filter_to_cancel() {
        let now = Utc::now();
        let item_pending_custom_orphaned = IdRow::new(
            1,
            QueueItem::new("1".to_string(), QueueItemStatus::Pending, 1, Some(10)),
        );
        let item_pending_custom_orphaned_expired = IdRow::new(
            2,
            QueueItem::new("2".to_string(), QueueItemStatus::Pending, 1, Some(1)),
        );
        let item_active_custom_orphaned = IdRow::new(
            3,
            QueueItem::new("3".to_string(), QueueItemStatus::Active, 1, Some(10)),
        );
        let mut item_active_custom_orphaned_expired = IdRow::new(
            4,
            QueueItem::new("4".to_string(), QueueItemStatus::Active, 1, Some(1)),
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
