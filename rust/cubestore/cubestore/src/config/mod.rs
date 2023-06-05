#![allow(deprecated)] // 'vtable' and 'TraitObject' are deprecated.
pub mod injection;
pub mod processing_loop;

use crate::cachestore::{
    CacheStore, CacheStoreSchedulerImpl, ClusterCacheStoreClient, LazyRocksCacheStore,
};
use crate::cluster::transport::{
    ClusterTransport, ClusterTransportImpl, MetaStoreTransport, MetaStoreTransportImpl,
};
use crate::cluster::{Cluster, ClusterImpl, ClusterMetaStoreClient};
use crate::config::injection::{DIService, Injector};
use crate::config::processing_loop::ProcessingLoop;
use crate::http::HttpServer;
use crate::import::limits::ConcurrencyLimits;
use crate::import::{ImportService, ImportServiceImpl};
use crate::metastore::{
    BaseRocksStoreFs, MetaStore, MetaStoreRpcClient, RocksMetaStore, RocksStoreConfig,
};
use crate::mysql::{MySqlServer, SqlAuthDefaultImpl, SqlAuthService};
use crate::queryplanner::query_executor::{QueryExecutor, QueryExecutorImpl};
use crate::queryplanner::{QueryPlanner, QueryPlannerImpl};
use crate::remotefs::gcs::GCSRemoteFs;
use crate::remotefs::minio::MINIORemoteFs;
use crate::remotefs::queue::QueueRemoteFs;
use crate::remotefs::s3::S3RemoteFs;
use crate::remotefs::{LocalDirRemoteFs, RemoteFs};
use crate::scheduler::SchedulerImpl;
use crate::sql::cache::SqlResultCache;
use crate::sql::{SqlService, SqlServiceImpl};
use crate::store::compaction::{CompactionService, CompactionServiceImpl};
use crate::store::{ChunkDataStore, ChunkStore, WALDataStore, WALStore};
use crate::streaming::kafka::{KafkaClientService, KafkaClientServiceImpl};
use crate::streaming::{KsqlClient, KsqlClientImpl, StreamingService, StreamingServiceImpl};
use crate::table::parquet::{CubestoreParquetMetadataCache, CubestoreParquetMetadataCacheImpl};
use crate::telemetry::tracing::{TracingHelper, TracingHelperImpl};
use crate::telemetry::{
    start_agent_event_loop, start_track_event_loop, stop_agent_event_loop, stop_track_event_loop,
};
use crate::util::memory::{MemoryHandler, MemoryHandlerImpl};
use crate::CubeError;
use datafusion::cube_ext;
use datafusion::physical_plan::parquet::{LruParquetMetadataCache, NoopParquetMetadataCache};
use futures::future::join_all;
use log::Level;
use log::{debug, error};
use mockall::automock;
use rocksdb::{Options, DB};
use simple_logger::SimpleLogger;
use std::fmt::Display;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::{env, fs};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{timeout_at, Duration, Instant};

#[derive(Clone)]
pub struct CubeServices {
    pub injector: Arc<Injector>,
    pub sql_service: Arc<dyn SqlService>,
    pub rocks_meta_store: Option<Arc<RocksMetaStore>>,
    pub rocks_cache_store: Option<Arc<LazyRocksCacheStore>>,
    pub meta_store: Arc<dyn MetaStore>,
    pub cluster: Arc<ClusterImpl>,
}

#[derive(Clone)]
pub struct WorkerServices {
    pub query_executor: Arc<dyn QueryExecutor>,
}

impl CubeServices {
    pub async fn start_processing_loops(&self) -> Result<(), CubeError> {
        let futures = self.spawn_processing_loops().await?;
        cube_ext::spawn(async move {
            if let Err(e) = Self::wait_loops(futures).await {
                error!("Error in processing loop: {}", e);
            }
        });
        Ok(())
    }

    pub async fn wait_processing_loops(&self) -> Result<(), CubeError> {
        Self::wait_loops(self.spawn_processing_loops().await?).await
    }

    async fn wait_loops(loops: Vec<LoopHandle>) -> Result<(), CubeError> {
        join_all(loops)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    async fn spawn_processing_loops(&self) -> Result<Vec<LoopHandle>, CubeError> {
        let mut futures = Vec::new();

        let cluster = self.cluster.clone();
        futures.push(cube_ext::spawn(async move {
            cluster.wait_processing_loops().await
        }));

        let remote_fs = self.injector.get_service_typed::<QueueRemoteFs>().await;
        futures.push(cube_ext::spawn(async move {
            QueueRemoteFs::wait_processing_loops(remote_fs.clone()).await
        }));

        if !self.cluster.is_select_worker() {
            let rocks_meta_store = self.rocks_meta_store.clone().unwrap();
            futures.push(cube_ext::spawn(async move {
                rocks_meta_store.wait_upload_loop().await;
                Ok(())
            }));

            let rocks_cache_store = self.rocks_cache_store.clone().unwrap();
            futures.push(cube_ext::spawn(async move {
                rocks_cache_store.wait_upload_loop().await;
                Ok(())
            }));

            let cluster = self.cluster.clone();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            futures.push(cube_ext::spawn(async move {
                ClusterImpl::listen_on_metastore_port(cluster, started_tx).await
            }));
            started_rx.await?;

            if self.injector.has_service_typed::<SchedulerImpl>().await {
                let scheduler = self.injector.get_service_typed::<SchedulerImpl>().await;
                futures.extend(scheduler.spawn_processing_loops());
            }

            if self
                .injector
                .has_service_typed::<CacheStoreSchedulerImpl>()
                .await
            {
                let scheduler = self
                    .injector
                    .get_service_typed::<CacheStoreSchedulerImpl>()
                    .await;
                futures.extend(scheduler.spawn_processing_loops());
            }

            if self.injector.has_service_typed::<MySqlServer>().await {
                let mysql_server = self.injector.get_service_typed::<MySqlServer>().await;
                futures.push(cube_ext::spawn(async move {
                    mysql_server.processing_loop().await
                }));
            }
            if self.injector.has_service_typed::<HttpServer>().await {
                let http_server = self.injector.get_service_typed::<HttpServer>().await;
                futures.push(cube_ext::spawn(
                    async move { http_server.run_server().await },
                ));
            }
        } else {
            let cluster = self.cluster.clone();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            futures.push(cube_ext::spawn(async move {
                ClusterImpl::listen_on_worker_port(cluster, started_tx).await
            }));
            started_rx.await?;

            let cluster = self.cluster.clone();
            futures.push(cube_ext::spawn(async move {
                cluster.warmup_select_worker().await;
                Ok(())
            }))
        }
        futures.push(cube_ext::spawn(async move {
            start_track_event_loop().await;
            Ok(())
        }));

        futures.push(cube_ext::spawn(async move {
            start_agent_event_loop().await;
            Ok(())
        }));
        Ok(futures)
    }

    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        #[cfg(not(target_os = "windows"))]
        self.cluster.stop_processing_loops().await?;

        let remote_fs = self.injector.get_service_typed::<QueueRemoteFs>().await;
        remote_fs.stop_processing_loops()?;

        if let Some(rocks_meta) = &self.rocks_meta_store {
            rocks_meta.stop_processing_loops().await;
        }

        if let Some(rocks_cache) = &self.rocks_cache_store {
            rocks_cache.stop_processing_loops().await;
        }

        if self.injector.has_service_typed::<MySqlServer>().await {
            self.injector
                .get_service_typed::<MySqlServer>()
                .await
                .stop_processing()
                .await?;
        }

        if self.injector.has_service_typed::<HttpServer>().await {
            self.injector
                .get_service_typed::<HttpServer>()
                .await
                .stop_processing()
                .await;
        }

        if self.injector.has_service_typed::<SchedulerImpl>().await {
            let scheduler = self.injector.get_service_typed::<SchedulerImpl>().await;
            scheduler.stop_processing_loops()?;
        }

        if self
            .injector
            .has_service_typed::<CacheStoreSchedulerImpl>()
            .await
        {
            let scheduler = self
                .injector
                .get_service_typed::<CacheStoreSchedulerImpl>()
                .await;
            scheduler.stop_processing_loops()?;
        }

        stop_track_event_loop().await;
        stop_agent_event_loop().await;
        Ok(())
    }
}

pub struct ValidationMessages {
    /// Hard errors, config cannot be used. Application must report these and exit.
    pub errors: Vec<String>,
    /// Must be reported to the user, but does not stop application from working.
    pub warnings: Vec<String>,
}

impl ValidationMessages {
    pub fn report_and_abort_on_errors(&self) {
        for w in &self.warnings {
            log::warn!("{}", w);
        }
        for e in &self.errors {
            log::error!("{}", e);
        }
        if !self.errors.is_empty() {
            log::error!("Cannot proceed with invalid configuration, exiting");
            std::process::exit(1)
        }
    }
}

/// This method also looks at environment variables and assumes [c] was obtained by calling
/// `Config::default()`.
pub fn validate_config(c: &dyn ConfigObj) -> ValidationMessages {
    let mut warnings = Vec::new();
    let mut errors = Vec::new();
    if is_router(c) && c.metastore_remote_address().is_some() {
        errors.push(
            "Router node cannot use remote metastore. Try removing CUBESTORE_META_ADDR".to_string(),
        );
    }
    if !is_router(c) && !c.select_workers().contains(c.server_name()) {
        warnings.push(format!("Current worker '{}' is missing in CUBESTORE_WORKERS. Please check CUBESTORE_SERVER_NAME and CUBESTORE_WORKERS variables", c.server_name()));
    }

    let mut router_vars = vec![
        "CUBESTORE_HTTP_BIND_ADDR",
        "CUBESTORE_HTTP_PORT",
        "CUBESTORE_BIND_ADDR",
        "CUBESTORE_PORT",
        "CUBESTORE_META_BIND_ADDR",
        "CUBESTORE_META_PORT",
    ];
    router_vars.retain(|v| env::var(v).is_ok());
    if !is_router(c) && !router_vars.is_empty() {
        warnings.push(format!(
            "The following router variable{} ignored on worker node: {}",
            if 1 < router_vars.len() { "s" } else { "" },
            router_vars.join(", ")
        ));
    }

    let mut remote_vars = vec![
        "CUBESTORE_MINIO_BUCKET",
        "CUBESTORE_S3_BUCKET",
        "CUBESTORE_GCS_BUCKET",
        "CUBESTORE_REMOTE_DIR",
    ];
    remote_vars.retain(|v| env::var(v).is_ok());
    if 1 < remote_vars.len() {
        warnings.push(format!(
            "{} variables specified together. Using {}",
            remote_vars.join(" and "),
            remote_vars[0],
        ));
    }

    ValidationMessages { errors, warnings }
}

#[derive(Debug, Clone)]
pub enum FileStoreProvider {
    Local,
    Filesystem {
        remote_dir: Option<PathBuf>,
    },
    S3 {
        region: String,
        bucket_name: String,
        sub_path: Option<String>,
    },
    GCS {
        bucket_name: String,
        sub_path: Option<String>,
    },
    MINIO {
        bucket_name: String,
        sub_path: Option<String>,
    },
}

#[derive(Clone)]
pub struct Config {
    config_obj: Arc<ConfigObjImpl>,
    injector: Arc<Injector>,
}

#[automock]
pub trait ConfigObj: DIService {
    fn partition_split_threshold(&self) -> u64;

    fn partition_size_split_threshold_bytes(&self) -> u64;

    fn max_partition_split_threshold(&self) -> u64;

    fn compaction_chunks_total_size_threshold(&self) -> u64;

    fn compaction_chunks_count_threshold(&self) -> u64;

    fn compaction_chunks_in_memory_size_threshold(&self) -> u64;

    fn compaction_chunks_max_lifetime_threshold(&self) -> u64;

    fn compaction_in_memory_chunks_max_lifetime_threshold(&self) -> u64;

    fn compaction_in_memory_chunks_size_limit(&self) -> u64;

    fn compaction_in_memory_chunks_total_size_limit(&self) -> u64;

    fn compaction_in_memory_chunks_count_threshold(&self) -> usize;

    fn compaction_in_memory_chunks_ratio_threshold(&self) -> u64;

    fn compaction_in_memory_chunks_ratio_check_threshold(&self) -> u64;

    fn compaction_in_memory_chunks_schedule_period_secs(&self) -> u64;

    fn wal_split_threshold(&self) -> u64;

    fn select_worker_pool_size(&self) -> usize;

    fn job_runners_count(&self) -> usize;

    fn long_term_job_runners_count(&self) -> usize;

    fn bind_address(&self) -> &Option<String>;

    fn status_bind_address(&self) -> &Option<String>;

    fn http_bind_address(&self) -> &Option<String>;

    fn query_timeout(&self) -> u64;

    fn not_used_timeout(&self) -> u64;

    fn in_memory_not_used_timeout(&self) -> u64;

    fn import_job_timeout(&self) -> u64;

    fn meta_store_snapshot_interval(&self) -> u64;

    fn meta_store_log_upload_interval(&self) -> u64;

    fn gc_loop_interval(&self) -> u64;

    fn stale_stream_timeout(&self) -> u64;

    fn select_workers(&self) -> &Vec<String>;

    fn worker_bind_address(&self) -> &Option<String>;

    fn metastore_bind_address(&self) -> &Option<String>;

    fn metastore_rocksdb_config(&self) -> &RocksStoreConfig;

    fn metastore_remote_address(&self) -> &Option<String>;

    fn cachestore_rocksdb_config(&self) -> &RocksStoreConfig;

    fn cachestore_gc_loop_interval(&self) -> u64;

    fn cachestore_queue_results_expire(&self) -> u64;

    fn download_concurrency(&self) -> u64;

    fn upload_concurrency(&self) -> u64;

    fn data_dir(&self) -> &PathBuf;

    fn connection_timeout(&self) -> u64;

    fn server_name(&self) -> &String;

    fn max_ingestion_data_frames(&self) -> usize;

    fn upload_to_remote(&self) -> bool;

    fn enable_topk(&self) -> bool;

    fn enable_startup_warmup(&self) -> bool;

    fn malloc_trim_every_secs(&self) -> u64;

    fn query_cache_max_capacity_bytes(&self) -> u64;

    fn query_cache_time_to_idle_secs(&self) -> Option<u64>;

    fn metadata_cache_max_capacity_bytes(&self) -> u64;

    fn metadata_cache_time_to_idle_secs(&self) -> u64;

    fn stream_replay_check_interval_secs(&self) -> u64;

    fn check_ws_orphaned_messages_interval_secs(&self) -> u64;

    fn drop_ws_processing_messages_after_secs(&self) -> u64;

    fn drop_ws_complete_messages_after_secs(&self) -> u64;

    fn skip_kafka_parsing_errors(&self) -> bool;

    fn dump_dir(&self) -> &Option<PathBuf>;

    fn minimum_metastore_snapshots_count(&self) -> u64;

    fn metastore_snapshots_lifetime(&self) -> u64;

    fn minimum_cachestore_snapshots_count(&self) -> u64;

    fn cachestore_snapshots_lifetime(&self) -> u64;

    fn max_disk_space(&self) -> u64;
    fn max_disk_space_per_worker(&self) -> u64;

    fn disk_space_cache_duration_secs(&self) -> u64;

    fn transport_max_message_size(&self) -> usize;
    fn transport_max_frame_size(&self) -> usize;

    fn local_files_cleanup_interval_secs(&self) -> u64;

    fn remote_files_cleanup_interval_secs(&self) -> u64;

    fn local_files_cleanup_size_threshold(&self) -> u64;

    fn local_files_cleanup_delay_secs(&self) -> u64;

    fn remote_files_cleanup_delay_secs(&self) -> u64;
}

#[derive(Debug, Clone)]
pub struct ConfigObjImpl {
    pub partition_split_threshold: u64,
    pub partition_size_split_threshold_bytes: u64,
    pub max_partition_split_threshold: u64,
    pub compaction_chunks_total_size_threshold: u64,
    pub compaction_chunks_count_threshold: u64,
    pub compaction_chunks_in_memory_size_threshold: u64,
    pub compaction_chunks_max_lifetime_threshold: u64,
    pub compaction_in_memory_chunks_max_lifetime_threshold: u64,
    pub compaction_in_memory_chunks_size_limit: u64,
    pub compaction_in_memory_chunks_total_size_limit: u64,
    pub compaction_in_memory_chunks_count_threshold: usize,
    pub compaction_in_memory_chunks_ratio_threshold: u64,
    pub compaction_in_memory_chunks_ratio_check_threshold: u64,
    pub compaction_in_memory_chunks_schedule_period_secs: u64,
    pub wal_split_threshold: u64,
    pub data_dir: PathBuf,
    pub dump_dir: Option<PathBuf>,
    pub store_provider: FileStoreProvider,
    pub select_worker_pool_size: usize,
    pub job_runners_count: usize,
    pub long_term_job_runners_count: usize,
    pub bind_address: Option<String>,
    pub status_bind_address: Option<String>,
    pub http_bind_address: Option<String>,
    pub query_timeout: u64,
    /// Must be set to 2*query_timeout in prod, only for overrides in tests.
    pub not_used_timeout: u64,
    pub in_memory_not_used_timeout: u64,
    pub import_job_timeout: u64,
    pub meta_store_log_upload_interval: u64,
    pub meta_store_snapshot_interval: u64,
    pub gc_loop_interval: u64,
    pub stale_stream_timeout: u64,
    pub select_workers: Vec<String>,
    pub worker_bind_address: Option<String>,
    pub metastore_bind_address: Option<String>,
    pub metastore_remote_address: Option<String>,
    pub metastore_rocks_store_config: RocksStoreConfig,
    pub cachestore_rocks_store_config: RocksStoreConfig,
    pub cachestore_gc_loop_interval: u64,
    pub cachestore_queue_results_expire: u64,
    pub upload_concurrency: u64,
    pub download_concurrency: u64,
    pub connection_timeout: u64,
    pub server_name: String,
    pub max_ingestion_data_frames: usize,
    pub upload_to_remote: bool,
    pub enable_topk: bool,
    pub enable_startup_warmup: bool,
    pub malloc_trim_every_secs: u64,
    pub query_cache_max_capacity_bytes: u64,
    pub query_cache_time_to_idle_secs: Option<u64>,
    pub metadata_cache_max_capacity_bytes: u64,
    pub metadata_cache_time_to_idle_secs: u64,
    pub stream_replay_check_interval_secs: u64,
    pub check_ws_orphaned_messages_interval_secs: u64,
    pub drop_ws_processing_messages_after_secs: u64,
    pub drop_ws_complete_messages_after_secs: u64,
    pub skip_kafka_parsing_errors: bool,
    pub minimum_metastore_snapshots_count: u64,
    pub metastore_snapshots_lifetime: u64,
    pub minimum_cachestore_snapshots_count: u64,
    pub cachestore_snapshots_lifetime: u64,
    pub max_disk_space: u64,
    pub max_disk_space_per_worker: u64,
    pub disk_space_cache_duration_secs: u64,
    pub transport_max_message_size: usize,
    pub transport_max_frame_size: usize,
    pub local_files_cleanup_interval_secs: u64,
    pub remote_files_cleanup_interval_secs: u64,
    pub local_files_cleanup_size_threshold: u64,
    pub local_files_cleanup_delay_secs: u64,
    pub remote_files_cleanup_delay_secs: u64,
}

crate::di_service!(ConfigObjImpl, [ConfigObj]);
crate::di_service!(MockConfigObj, [ConfigObj]);

impl ConfigObj for ConfigObjImpl {
    fn partition_split_threshold(&self) -> u64 {
        self.partition_split_threshold
    }

    fn partition_size_split_threshold_bytes(&self) -> u64 {
        self.partition_size_split_threshold_bytes
    }

    fn max_partition_split_threshold(&self) -> u64 {
        self.max_partition_split_threshold
    }

    fn compaction_chunks_total_size_threshold(&self) -> u64 {
        self.compaction_chunks_total_size_threshold
    }

    fn compaction_chunks_count_threshold(&self) -> u64 {
        self.compaction_chunks_count_threshold
    }

    fn compaction_chunks_in_memory_size_threshold(&self) -> u64 {
        self.compaction_chunks_in_memory_size_threshold
    }

    fn compaction_in_memory_chunks_size_limit(&self) -> u64 {
        self.compaction_in_memory_chunks_size_limit
    }

    fn compaction_chunks_max_lifetime_threshold(&self) -> u64 {
        self.compaction_chunks_max_lifetime_threshold
    }

    fn compaction_in_memory_chunks_max_lifetime_threshold(&self) -> u64 {
        self.compaction_in_memory_chunks_max_lifetime_threshold
    }

    fn compaction_in_memory_chunks_total_size_limit(&self) -> u64 {
        self.compaction_in_memory_chunks_total_size_limit
    }

    fn compaction_in_memory_chunks_count_threshold(&self) -> usize {
        self.compaction_in_memory_chunks_count_threshold
    }

    fn compaction_in_memory_chunks_ratio_threshold(&self) -> u64 {
        self.compaction_in_memory_chunks_ratio_threshold
    }

    fn compaction_in_memory_chunks_ratio_check_threshold(&self) -> u64 {
        self.compaction_in_memory_chunks_ratio_check_threshold
    }

    fn compaction_in_memory_chunks_schedule_period_secs(&self) -> u64 {
        self.compaction_in_memory_chunks_schedule_period_secs
    }

    fn wal_split_threshold(&self) -> u64 {
        self.wal_split_threshold
    }

    fn select_worker_pool_size(&self) -> usize {
        self.select_worker_pool_size
    }

    fn job_runners_count(&self) -> usize {
        self.job_runners_count
    }

    fn long_term_job_runners_count(&self) -> usize {
        self.long_term_job_runners_count
    }

    fn bind_address(&self) -> &Option<String> {
        &self.bind_address
    }

    fn status_bind_address(&self) -> &Option<String> {
        &self.status_bind_address
    }

    fn http_bind_address(&self) -> &Option<String> {
        &self.http_bind_address
    }

    fn query_timeout(&self) -> u64 {
        self.query_timeout
    }

    fn not_used_timeout(&self) -> u64 {
        self.not_used_timeout
    }

    fn in_memory_not_used_timeout(&self) -> u64 {
        self.in_memory_not_used_timeout
    }

    fn import_job_timeout(&self) -> u64 {
        self.import_job_timeout
    }

    fn meta_store_snapshot_interval(&self) -> u64 {
        self.meta_store_snapshot_interval
    }

    fn meta_store_log_upload_interval(&self) -> u64 {
        self.meta_store_log_upload_interval
    }

    fn gc_loop_interval(&self) -> u64 {
        self.gc_loop_interval
    }

    fn stale_stream_timeout(&self) -> u64 {
        self.stale_stream_timeout
    }

    fn select_workers(&self) -> &Vec<String> {
        &self.select_workers
    }

    fn worker_bind_address(&self) -> &Option<String> {
        &self.worker_bind_address
    }

    fn metastore_bind_address(&self) -> &Option<String> {
        &self.metastore_bind_address
    }

    fn metastore_remote_address(&self) -> &Option<String> {
        &self.metastore_remote_address
    }

    fn metastore_rocksdb_config(&self) -> &RocksStoreConfig {
        &self.metastore_rocks_store_config
    }

    fn cachestore_rocksdb_config(&self) -> &RocksStoreConfig {
        &self.cachestore_rocks_store_config
    }

    fn cachestore_gc_loop_interval(&self) -> u64 {
        self.cachestore_gc_loop_interval
    }

    fn cachestore_queue_results_expire(&self) -> u64 {
        self.cachestore_queue_results_expire
    }

    fn download_concurrency(&self) -> u64 {
        self.download_concurrency
    }

    fn upload_concurrency(&self) -> u64 {
        self.upload_concurrency
    }

    fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    fn connection_timeout(&self) -> u64 {
        self.connection_timeout
    }

    fn server_name(&self) -> &String {
        &self.server_name
    }

    fn max_ingestion_data_frames(&self) -> usize {
        self.max_ingestion_data_frames
    }

    fn upload_to_remote(&self) -> bool {
        self.upload_to_remote
    }

    fn enable_topk(&self) -> bool {
        self.enable_topk
    }

    fn enable_startup_warmup(&self) -> bool {
        self.enable_startup_warmup
    }
    fn malloc_trim_every_secs(&self) -> u64 {
        self.malloc_trim_every_secs
    }
    fn query_cache_max_capacity_bytes(&self) -> u64 {
        self.query_cache_max_capacity_bytes
    }
    fn query_cache_time_to_idle_secs(&self) -> Option<u64> {
        self.query_cache_time_to_idle_secs
    }
    fn metadata_cache_max_capacity_bytes(&self) -> u64 {
        self.metadata_cache_max_capacity_bytes
    }
    fn metadata_cache_time_to_idle_secs(&self) -> u64 {
        self.metadata_cache_time_to_idle_secs
    }
    fn stream_replay_check_interval_secs(&self) -> u64 {
        self.stream_replay_check_interval_secs
    }
    fn skip_kafka_parsing_errors(&self) -> bool {
        self.skip_kafka_parsing_errors
    }

    fn check_ws_orphaned_messages_interval_secs(&self) -> u64 {
        self.check_ws_orphaned_messages_interval_secs
    }

    fn drop_ws_processing_messages_after_secs(&self) -> u64 {
        self.drop_ws_processing_messages_after_secs
    }

    fn drop_ws_complete_messages_after_secs(&self) -> u64 {
        self.drop_ws_complete_messages_after_secs
    }

    fn dump_dir(&self) -> &Option<PathBuf> {
        &self.dump_dir
    }

    fn minimum_metastore_snapshots_count(&self) -> u64 {
        self.minimum_metastore_snapshots_count
    }

    fn metastore_snapshots_lifetime(&self) -> u64 {
        self.metastore_snapshots_lifetime
    }

    fn minimum_cachestore_snapshots_count(&self) -> u64 {
        self.minimum_cachestore_snapshots_count
    }

    fn cachestore_snapshots_lifetime(&self) -> u64 {
        self.cachestore_snapshots_lifetime
    }

    fn max_disk_space(&self) -> u64 {
        self.max_disk_space
    }

    fn max_disk_space_per_worker(&self) -> u64 {
        self.max_disk_space_per_worker
    }

    fn disk_space_cache_duration_secs(&self) -> u64 {
        self.disk_space_cache_duration_secs
    }

    fn transport_max_message_size(&self) -> usize {
        self.transport_max_message_size
    }

    fn transport_max_frame_size(&self) -> usize {
        self.transport_max_frame_size
    }

    fn local_files_cleanup_interval_secs(&self) -> u64 {
        self.local_files_cleanup_interval_secs
    }

    fn remote_files_cleanup_interval_secs(&self) -> u64 {
        self.remote_files_cleanup_interval_secs
    }

    fn local_files_cleanup_size_threshold(&self) -> u64 {
        self.local_files_cleanup_size_threshold
    }

    fn local_files_cleanup_delay_secs(&self) -> u64 {
        self.local_files_cleanup_delay_secs
    }

    fn remote_files_cleanup_delay_secs(&self) -> u64 {
        self.remote_files_cleanup_delay_secs
    }
}

lazy_static! {
    pub static ref TEST_LOGGING_INITIALIZED: tokio::sync::RwLock<bool> =
        tokio::sync::RwLock::new(false);
}

pub async fn init_test_logger() {
    if !*TEST_LOGGING_INITIALIZED.read().await {
        let mut initialized = TEST_LOGGING_INITIALIZED.write().await;
        if !*initialized {
            SimpleLogger::new()
                .with_level(Level::Error.to_level_filter())
                .with_module_level("cubestore", Level::Trace.to_level_filter())
                .init()
                .unwrap();
        }
        *initialized = true;
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|x| match x.as_str() {
            "0" => false,
            "1" => true,
            _ => panic!("expected '0' or '1' for '{}', found '{}'", name, &x),
        })
        .unwrap_or(default)
}

pub fn env_parse<T>(name: &str, default: T) -> T
where
    T: FromStr,
    T::Err: Display,
{
    env_optparse(name).unwrap_or(default)
}

pub fn env_parse_duration<T>(name: &str, default: T, max: Option<T>, min: Option<T>) -> T
where
    T: FromStr + PartialOrd + Display,
    T::Err: Display,
{
    let v = match env::var(name).ok() {
        None => {
            return default;
        }
        Some(v) => v,
    };

    let n = match v.parse::<T>() {
        Ok(n) => n,
        Err(e) => panic!(
            "could not parse environment variable '{}' with '{}' value: {}",
            name, v, e
        ),
    };

    if let Some(max) = max {
        if n > max {
            panic!(
                "wrong configuration for environment variable '{}' with '{}' value: greater then max size {}",
                name, v,
                max
            )
        }
    };

    if let Some(min) = min {
        if n < min {
            panic!(
                "wrong configuration for environment variable '{}' with '{}' value: lower then min size {}",
                name, v,
                min
            )
        }
    };

    n
}

pub fn env_parse_size(name: &str, default: usize, max: Option<usize>, min: Option<usize>) -> usize {
    let v = match env::var(name).ok() {
        None => {
            return default;
        }
        Some(v) => v,
    };

    let n = match parse_size::parse_size(&v) {
        Ok(n) => n as usize,
        Err(e) => panic!(
            "could not parse environment variable '{}' with '{}' value: {}",
            name, v, e
        ),
    };

    if let Some(max) = max {
        if n > max {
            panic!(
                "wrong configuration for environment variable '{}' with '{}' value: greater then max size {}",
                name, v,
                humansize::format_size(max, humansize::DECIMAL)
            )
        }
    };

    if let Some(min) = min {
        if n < min {
            panic!(
                "wrong configuration for environment variable '{}' with '{}' value: lower then min size {}",
                name, v,
                humansize::format_size(min, humansize::DECIMAL)
            )
        }
    };

    n
}

fn env_optparse<T>(name: &str) -> Option<T>
where
    T: FromStr,
    T::Err: Display,
{
    env::var(name).ok().map(|x| match x.parse::<T>() {
        Ok(v) => v,
        Err(e) => panic!(
            "could not parse environment variable '{}' with '{}' value: {}",
            name, x, e
        ),
    })
}

impl Config {
    pub fn default() -> Config {
        let query_timeout = env_parse("CUBESTORE_QUERY_TIMEOUT", 120);
        let query_cache_time_to_idle_secs = env_parse(
            "CUBESTORE_QUERY_CACHE_TIME_TO_IDLE",
            // 1 hour
            60 * 60,
        );

        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl {
                data_dir: env::var("CUBESTORE_DATA_DIR")
                    .ok()
                    .map(|v| PathBuf::from(v))
                    .unwrap_or(env::current_dir().unwrap().join(".cubestore").join("data")),
                dump_dir: env::var("CUBESTORE_DUMP_DIR")
                    .ok()
                    .map(|v| PathBuf::from(v)),
                partition_split_threshold: env_parse(
                    "CUBESTORE_PARTITION_SPLIT_THRESHOLD",
                    1048576 * 2,
                ),
                partition_size_split_threshold_bytes: env_parse_size(
                    "CUBESTORE_PARTITION_SIZE_SPLIT_THRESHOLD",
                    100 * 1024 * 1024,
                    None,
                    Some(32 << 20),
                ) as u64,
                max_partition_split_threshold: env_parse(
                    "CUBESTORE_PARTITION_MAX_SPLIT_THRESHOLD",
                    1048576 * 8,
                ),
                compaction_chunks_count_threshold: env_parse("CUBESTORE_CHUNKS_COUNT_THRESHOLD", 4),
                compaction_chunks_in_memory_size_threshold: env_parse_size(
                    "CUBESTORE_COMPACTION_CHUNKS_IN_MEMORY_SIZE_THRESHOLD",
                    1 * 1024 * 1024 * 1024,
                    None,
                    Some(1 * 1024 * 1024 * 1024),
                ) as u64,
                compaction_chunks_total_size_threshold: env_parse(
                    "CUBESTORE_CHUNKS_TOTAL_SIZE_THRESHOLD",
                    1048576 * 2,
                ),
                compaction_chunks_max_lifetime_threshold: env_parse(
                    "CUBESTORE_CHUNKS_MAX_LIFETIME_THRESHOLD",
                    600,
                ),
                compaction_in_memory_chunks_max_lifetime_threshold: env_parse(
                    "CUBESTORE_IN_MEMORY_CHUNKS_MAX_LIFETIME_THRESHOLD",
                    60,
                ),
                compaction_in_memory_chunks_size_limit: env_parse(
                    "CUBESTORE_IN_MEMORY_CHUNKS_SIZE_LIMIT",
                    262_144 / 4,
                ),
                compaction_in_memory_chunks_total_size_limit: env_parse(
                    "CUBESTORE_IN_MEMORY_CHUNKS_TOTAL_SIZE_LIMIT",
                    262_144,
                ),
                compaction_in_memory_chunks_count_threshold: env_parse(
                    "CUBESTORE_IN_MEMORY_CHUNKS_COUNT_THRESHOLD",
                    10,
                ),
                compaction_in_memory_chunks_ratio_threshold: env_parse(
                    "CUBESTORE_IN_MEMORY_CHUNKS_RATIO_THRESHOLD",
                    3,
                ),
                compaction_in_memory_chunks_ratio_check_threshold: env_parse(
                    "CUBESTORE_IN_MEMORY_CHUNKS_RATIO_CHECK_THRESHOLD",
                    1000,
                ),
                compaction_in_memory_chunks_schedule_period_secs: env_parse(
                    "CUBESTORE_IN_MEMORY_CHUNKS_SCHEDULE_PERIOD_SECS",
                    5,
                ),
                store_provider: {
                    if let Ok(bucket_name) = env::var("CUBESTORE_S3_BUCKET") {
                        FileStoreProvider::S3 {
                            bucket_name,
                            region: env::var("CUBESTORE_S3_REGION").expect(
                                "CUBESTORE_S3_REGION required when CUBESTORE_S3_BUCKET is set",
                            ),
                            sub_path: env::var("CUBESTORE_S3_SUB_PATH").ok(),
                        }
                    } else if let Ok(bucket_name) = env::var("CUBESTORE_MINIO_BUCKET") {
                        FileStoreProvider::MINIO {
                            bucket_name,
                            sub_path: env::var("CUBESTORE_MINIO_SUB_PATH").ok(),
                        }
                    } else if let Ok(bucket_name) = env::var("CUBESTORE_GCS_BUCKET") {
                        FileStoreProvider::GCS {
                            bucket_name,
                            sub_path: env::var("CUBESTORE_GCS_SUB_PATH").ok(),
                        }
                    } else if let Ok(remote_dir) = env::var("CUBESTORE_REMOTE_DIR") {
                        FileStoreProvider::Filesystem {
                            remote_dir: Some(PathBuf::from(remote_dir)),
                        }
                    } else {
                        FileStoreProvider::Filesystem { remote_dir: None }
                    }
                },
                select_worker_pool_size: env_parse("CUBESTORE_SELECT_WORKERS", 4),
                bind_address: Some(
                    env::var("CUBESTORE_BIND_ADDR")
                        .ok()
                        .unwrap_or(format!("0.0.0.0:{}", env_parse("CUBESTORE_PORT", 3306))),
                ),
                status_bind_address: Some(env::var("CUBESTORE_STATUS_BIND_ADDR").ok().unwrap_or(
                    format!("0.0.0.0:{}", env_parse("CUBESTORE_STATUS_PORT", 3031)),
                )),
                http_bind_address: Some(env::var("CUBESTORE_HTTP_BIND_ADDR").ok().unwrap_or(
                    format!("0.0.0.0:{}", env_parse("CUBESTORE_HTTP_PORT", 3030)),
                )),
                query_timeout,
                not_used_timeout: 2 * query_timeout,
                in_memory_not_used_timeout: 30,
                import_job_timeout: env_parse("CUBESTORE_IMPORT_JOB_TIMEOUT", 600),
                meta_store_log_upload_interval: 30,
                meta_store_snapshot_interval: 300,
                gc_loop_interval: 60,
                stale_stream_timeout: env_parse("CUBESTORE_STALE_STREAM_TIMEOUT", 600),
                select_workers: env::var("CUBESTORE_WORKERS")
                    .ok()
                    .map(|v| v.split(",").map(|s| s.to_string()).collect())
                    .unwrap_or(Vec::new()),
                worker_bind_address: env::var("CUBESTORE_WORKER_BIND_ADDR").ok().or_else(|| {
                    env_optparse::<u16>("CUBESTORE_WORKER_PORT").map(|v| format!("0.0.0.0:{}", v))
                }),
                metastore_bind_address: env::var("CUBESTORE_META_BIND_ADDR").ok().or_else(|| {
                    env_optparse::<u16>("CUBESTORE_META_PORT").map(|v| format!("0.0.0.0:{}", v))
                }),
                metastore_remote_address: env::var("CUBESTORE_META_ADDR").ok(),
                metastore_rocks_store_config: RocksStoreConfig::metastore_default(),
                cachestore_rocks_store_config: RocksStoreConfig::cachestore_default(),
                cachestore_gc_loop_interval: env_parse_duration(
                    "CUBESTORE_CACHESTORE_GC_LOOP",
                    15,
                    // 1 minute
                    Some(60 * 1),
                    Some(1),
                ),
                cachestore_queue_results_expire: env_parse_duration(
                    "CUBESTORE_QUEUE_RESULTS_EXPIRE",
                    60,
                    // 5 minutes = TTL of QueueResult
                    Some(60 * 5),
                    Some(1),
                ),
                upload_concurrency: env_parse("CUBESTORE_MAX_ACTIVE_UPLOADS", 4),
                download_concurrency: env_parse("CUBESTORE_MAX_ACTIVE_DOWNLOADS", 8),
                max_ingestion_data_frames: env_parse("CUBESTORE_MAX_DATA_FRAMES", 4),
                wal_split_threshold: env_parse("CUBESTORE_WAL_SPLIT_THRESHOLD", 1048576 / 2),
                job_runners_count: env_parse("CUBESTORE_JOB_RUNNERS", 4),
                long_term_job_runners_count: env_parse("CUBESTORE_LONG_TERM_JOB_RUNNERS", 32),
                connection_timeout: 60,
                server_name: env::var("CUBESTORE_SERVER_NAME")
                    .ok()
                    .unwrap_or("localhost".to_string()),
                upload_to_remote: !env::var("CUBESTORE_NO_UPLOAD").ok().is_some(),
                enable_topk: env_bool("CUBESTORE_ENABLE_TOPK", true),
                enable_startup_warmup: env_bool("CUBESTORE_STARTUP_WARMUP", true),
                malloc_trim_every_secs: env_parse("CUBESTORE_MALLOC_TRIM_EVERY_SECS", 30),
                query_cache_max_capacity_bytes: env_parse_size(
                    "CUBESTORE_QUERY_CACHE_MAX_CAPACITY",
                    512 << 20,
                    Some(16384 << 20),
                    Some(0),
                ) as u64,
                query_cache_time_to_idle_secs: if query_cache_time_to_idle_secs == 0 {
                    None
                } else {
                    Some(query_cache_time_to_idle_secs)
                },
                metadata_cache_max_capacity_bytes: env_parse(
                    "CUBESTORE_METADATA_CACHE_MAX_CAPACITY_BYTES",
                    0,
                ),
                metadata_cache_time_to_idle_secs: env_parse(
                    "CUBESTORE_METADATA_CACHE_TIME_TO_IDLE_SECS",
                    0,
                ),
                stream_replay_check_interval_secs: env_parse(
                    "CUBESTORE_STREAM_REPLAY_CHECK_INTERVAL",
                    60,
                ),
                check_ws_orphaned_messages_interval_secs: env_parse(
                    "CUBESTORE_CHECK_WS_ORPHANED_MESSAGES_INTERVAL",
                    30,
                ),
                drop_ws_processing_messages_after_secs: env_parse(
                    "CUBESTORE_DROP_WS_PROCESSING_MESSAGES_AFTER",
                    60 * 60,
                ),
                drop_ws_complete_messages_after_secs: env_parse(
                    "CUBESTORE_DROP_WS_COMPLETE_MESSAGES_AFTER",
                    10 * 60,
                ),
                skip_kafka_parsing_errors: env_parse("CUBESTORE_SKIP_KAFKA_PARSING_ERRORS", false),
                minimum_metastore_snapshots_count: env_parse(
                    "CUBESTORE_MINIMUM_METASTORE_SNAPSHOTS_COUNT",
                    5,
                ),
                metastore_snapshots_lifetime: env_parse(
                    "CUBESTORE_METASTORE_SNAPSHOTS_LIFETIME",
                    24 * 60 * 60,
                ),
                minimum_cachestore_snapshots_count: env_parse(
                    "CUBESTORE_MINIMUM_CACHESTORE_SNAPSHOTS_COUNT",
                    5,
                ),
                cachestore_snapshots_lifetime: env_parse(
                    "CUBESTORE_CACHESTORE_SNAPSHOTS_LIFETIME",
                    60 * 60,
                ),
                max_disk_space: env_parse("CUBESTORE_MAX_DISK_SPACE_GB", 0) * 1024 * 1024 * 1024,
                max_disk_space_per_worker: env_parse("CUBESTORE_MAX_DISK_SPACE_PER_WORKER_GB", 0)
                    * 1024
                    * 1024
                    * 1024,
                disk_space_cache_duration_secs: 300,
                transport_max_message_size: env_parse_size(
                    "CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE",
                    64 << 20,
                    Some(256 << 20),
                    Some(16 << 20),
                ),
                transport_max_frame_size: env_parse_size(
                    "CUBESTORE_TRANSPORT_MAX_FRAME_SIZE",
                    32 << 20,
                    Some(256 << 20),
                    Some(4 << 20),
                ),
                local_files_cleanup_interval_secs: env_parse(
                    "CUBESTORE_LOCAL_FILES_CLEANUP_INTERVAL_SECS",
                    600,
                ),
                remote_files_cleanup_interval_secs: env_parse(
                    "CUBESTORE_REMOTE_FILES_CLEANUP_INTERVAL_SECS",
                    6 * 600,
                ),
                local_files_cleanup_size_threshold: env_parse_size(
                    "CUBESTORE_LOCAL_FILES_CLEANUP_SIZE_THRESHOLD",
                    1024 * 1024 * 1024,
                    None,
                    None,
                ) as u64,
                local_files_cleanup_delay_secs: env_parse(
                    "CUBESTORE_LOCAL_FILES_CLEANUP_DELAY_SECS",
                    600,
                ),
                remote_files_cleanup_delay_secs: env_parse(
                    "CUBESTORE_REMOTE_FILES_CLEANUP_DELAY_SECS",
                    3600,
                ),
            }),
        }
    }

    pub fn test(name: &str) -> Config {
        let query_timeout = 15;
        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl {
                data_dir: env::current_dir()
                    .unwrap()
                    .join(format!("{}-local-store", name)),
                dump_dir: None,
                partition_split_threshold: 20,
                partition_size_split_threshold_bytes: 2 * 1024,
                max_partition_split_threshold: 20,
                compaction_chunks_count_threshold: 1,
                compaction_chunks_in_memory_size_threshold: 3 * 1024 * 1024 * 1024,
                compaction_chunks_total_size_threshold: 10,
                compaction_chunks_max_lifetime_threshold: 600,
                compaction_in_memory_chunks_max_lifetime_threshold: 60,
                compaction_in_memory_chunks_size_limit: 262_144 / 4,
                compaction_in_memory_chunks_total_size_limit: 262_144,
                compaction_in_memory_chunks_count_threshold: 10,
                compaction_in_memory_chunks_ratio_threshold: 3,
                compaction_in_memory_chunks_ratio_check_threshold: 1000,
                compaction_in_memory_chunks_schedule_period_secs: 5,
                store_provider: FileStoreProvider::Filesystem {
                    remote_dir: Some(
                        env::current_dir()
                            .unwrap()
                            .join(format!("{}-upstream", name)),
                    ),
                },
                select_worker_pool_size: 0,
                job_runners_count: 4,
                long_term_job_runners_count: 8,
                bind_address: None,
                status_bind_address: None,
                http_bind_address: None,
                query_timeout,
                not_used_timeout: 2 * query_timeout,
                in_memory_not_used_timeout: 30,
                import_job_timeout: 600,
                stale_stream_timeout: 60,
                select_workers: Vec::new(),
                worker_bind_address: None,
                metastore_bind_address: None,
                metastore_remote_address: None,
                metastore_rocks_store_config: RocksStoreConfig::metastore_default(),
                cachestore_rocks_store_config: RocksStoreConfig::cachestore_default(),
                cachestore_gc_loop_interval: 30,
                cachestore_queue_results_expire: 90,
                upload_concurrency: 4,
                download_concurrency: 8,
                max_ingestion_data_frames: 4,
                wal_split_threshold: 262144,
                connection_timeout: 60,
                server_name: "localhost".to_string(),
                upload_to_remote: true,
                enable_topk: true,
                enable_startup_warmup: true,
                malloc_trim_every_secs: 0,
                query_cache_max_capacity_bytes: 512 << 20,
                query_cache_time_to_idle_secs: Some(600),
                metadata_cache_max_capacity_bytes: 0,
                metadata_cache_time_to_idle_secs: 1_000,
                meta_store_log_upload_interval: 30,
                meta_store_snapshot_interval: 300,
                gc_loop_interval: 60,
                stream_replay_check_interval_secs: 60,
                check_ws_orphaned_messages_interval_secs: 1,
                drop_ws_processing_messages_after_secs: 60,
                drop_ws_complete_messages_after_secs: 10,
                skip_kafka_parsing_errors: false,
                minimum_metastore_snapshots_count: 3,
                metastore_snapshots_lifetime: 24 * 3600,
                minimum_cachestore_snapshots_count: 3,
                cachestore_snapshots_lifetime: 3600,
                max_disk_space: 0,
                max_disk_space_per_worker: 0,
                disk_space_cache_duration_secs: 0,
                transport_max_message_size: 64 << 20,
                transport_max_frame_size: 16 << 20,
                local_files_cleanup_interval_secs: 600,
                remote_files_cleanup_interval_secs: 600,
                local_files_cleanup_size_threshold: 0,
                local_files_cleanup_delay_secs: 600,
                remote_files_cleanup_delay_secs: 3600,
            }),
        }
    }

    pub fn update_config(
        &self,
        update_config: impl FnOnce(ConfigObjImpl) -> ConfigObjImpl,
    ) -> Config {
        let new_config = self.config_obj.as_ref().clone();
        Self {
            injector: self.injector.clone(),
            config_obj: Arc::new(update_config(new_config)),
        }
    }

    pub async fn start_test<T>(&self, test_fn: impl FnOnce(CubeServices) -> T)
    where
        T: Future<Output = ()> + Send,
    {
        self.start_test_with_options::<_, T, _, _>(
            true,
            Option::<
                Box<
                    dyn FnOnce(Arc<Injector>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                        + Send
                        + Sync,
                >,
            >::None,
            test_fn,
        )
        .await
    }

    pub async fn start_test_worker<T>(&self, test_fn: impl FnOnce(CubeServices) -> T)
    where
        T: Future<Output = ()> + Send,
    {
        self.start_test_with_options::<_, T, _, _>(
            false,
            Option::<
                Box<
                    dyn FnOnce(Arc<Injector>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                        + Send
                        + Sync,
                >,
            >::None,
            test_fn,
        )
        .await
    }

    pub async fn start_with_injector_override<T1, T2>(
        &self,
        configure_injector: impl FnOnce(Arc<Injector>) -> T1,
        test_fn: impl FnOnce(CubeServices) -> T2,
    ) where
        T1: Future<Output = ()> + Send,
        T2: Future<Output = ()> + Send,
    {
        self.start_test_with_options(true, Some(configure_injector), test_fn)
            .await
    }

    pub async fn start_test_with_options<T1, T2, I, F>(
        &self,
        clean_remote: bool,
        configure_injector: Option<I>,
        test_fn: F,
    ) where
        T1: Future<Output = ()> + Send,
        T2: Future<Output = ()> + Send,
        I: FnOnce(Arc<Injector>) -> T1,
        F: FnOnce(CubeServices) -> T2,
    {
        init_test_logger().await;

        let store_path = self.local_dir().clone();
        let remote_fs = self.remote_fs().await.unwrap();
        let _ = fs::remove_dir_all(store_path.clone());
        if clean_remote {
            let remote_files = remote_fs.list("").await.unwrap();
            for file in remote_files {
                debug!("Cleaning {}", file);
                let _ = remote_fs.delete_file(file.as_str()).await.unwrap();
            }
        }
        {
            self.configure_injector().await;
            if let Some(configure_injector) = configure_injector {
                configure_injector(self.injector.clone()).await;
            }
            let services = self.cube_services().await;
            services.start_processing_loops().await.unwrap();

            // Should be long enough even for CI.
            let timeout = Duration::from_secs(600);
            if let Err(_) = timeout_at(Instant::now() + timeout, test_fn(services.clone())).await {
                panic!("Test timed out after {} seconds", timeout.as_secs());
            }

            services.stop_processing_loops().await.unwrap();
        }

        let _ = DB::destroy(&Options::default(), self.meta_store_path());
        let _ = DB::destroy(&Options::default(), self.cache_store_path());
        let _ = fs::remove_dir_all(store_path.clone());
        if clean_remote {
            let remote_files = remote_fs.list("").await.unwrap();
            for file in remote_files {
                let _ = remote_fs.delete_file(file.as_str()).await;
            }
        }
    }

    pub async fn run_test<T>(name: &str, test_fn: impl FnOnce(CubeServices) -> T)
    where
        T: Future<Output = ()> + Send,
    {
        Self::test(name).start_test(test_fn).await;
    }

    pub fn config_obj(&self) -> Arc<dyn ConfigObj> {
        self.config_obj.clone()
    }

    pub fn local_dir(&self) -> &PathBuf {
        &self.config_obj.data_dir
    }

    pub fn remote_dir(&self) -> &PathBuf {
        match &self.config_obj.store_provider {
            FileStoreProvider::Filesystem { remote_dir } => remote_dir.as_ref().unwrap(),
            x => panic!("Remote dir called on {:?}", x),
        }
    }

    pub fn meta_store_path(&self) -> PathBuf {
        self.local_dir().join("metastore")
    }

    pub fn cache_store_path(&self) -> PathBuf {
        self.local_dir().join("cachestore")
    }

    pub async fn configure_remote_fs(&self) {
        let config_obj_to_register = self.config_obj.clone();
        self.injector
            .register_typed::<dyn ConfigObj, _, _, _>(async move |_| config_obj_to_register)
            .await;

        match &self.config_obj.store_provider {
            FileStoreProvider::Filesystem { remote_dir } => {
                let remote_dir = remote_dir.clone();
                let data_dir = self.config_obj.data_dir.clone();
                self.injector
                    .register("original_remote_fs", async move |_| {
                        let arc: Arc<dyn DIService> = LocalDirRemoteFs::new(remote_dir, data_dir);
                        arc
                    })
                    .await;
            }
            FileStoreProvider::S3 {
                region,
                bucket_name,
                sub_path,
            } => {
                let data_dir = self.config_obj.data_dir.clone();
                let region = region.to_string();
                let bucket_name = bucket_name.to_string();
                let sub_path = sub_path.clone();
                self.injector
                    .register("original_remote_fs", async move |_| {
                        let arc: Arc<dyn DIService> =
                            S3RemoteFs::new(data_dir, region, bucket_name, sub_path).unwrap();
                        arc
                    })
                    .await;
            }
            FileStoreProvider::GCS {
                bucket_name,
                sub_path,
            } => {
                let data_dir = self.config_obj.data_dir.clone();
                let bucket_name = bucket_name.to_string();
                let sub_path = sub_path.clone();
                self.injector
                    .register("original_remote_fs", async move |_| {
                        let arc: Arc<dyn DIService> =
                            GCSRemoteFs::new(data_dir, bucket_name, sub_path).unwrap();
                        arc
                    })
                    .await;
            }
            FileStoreProvider::MINIO {
                bucket_name,
                sub_path,
            } => {
                let data_dir = self.config_obj.data_dir.clone();
                let bucket_name = bucket_name.to_string();
                let sub_path = sub_path.clone();
                self.injector
                    .register("original_remote_fs", async move |_| {
                        let arc: Arc<dyn DIService> =
                            MINIORemoteFs::new(data_dir, bucket_name, sub_path).unwrap();
                        arc
                    })
                    .await;
            }
            FileStoreProvider::Local => unimplemented!(), // TODO
        };
    }

    pub async fn configure_cache_store(&self) {
        let (cachestore_event_sender, _) = broadcast::channel(2048); // TODO config
        let cachestore_event_sender_to_move = cachestore_event_sender.clone();

        if uses_remote_metastore(&self.injector).await {
            self.injector
                .register_typed::<dyn CacheStore, _, _, _>(async move |_| {
                    Arc::new(ClusterCacheStoreClient {})
                })
                .await;
        } else {
            self.injector
                .register("cachestore_fs", async move |i| {
                    // TODO metastore works with non queue remote fs as it requires loops to be started prior to load_from_remote call
                    let original_remote_fs = i.get_service("original_remote_fs").await;
                    let arc: Arc<dyn DIService> = BaseRocksStoreFs::new_for_cachestore(
                        original_remote_fs,
                        i.get_service_typed().await,
                    );

                    arc
                })
                .await;
            let path = self.cache_store_path().to_str().unwrap().to_string();
            self.injector
                .register_typed_with_default::<dyn CacheStore, LazyRocksCacheStore, _, _>(
                    async move |i| {
                        let config = i.get_service_typed::<dyn ConfigObj>().await;
                        let cachestore_fs = i.get_service("cachestore_fs").await;
                        let cache_store = if let Some(dump_dir) = config.clone().dump_dir() {
                            LazyRocksCacheStore::load_from_dump(
                                &Path::new(&path),
                                dump_dir,
                                cachestore_fs,
                                config,
                                vec![cachestore_event_sender],
                            )
                            .await
                            .unwrap()
                        } else {
                            LazyRocksCacheStore::load_from_remote(
                                &path,
                                cachestore_fs,
                                config,
                                vec![cachestore_event_sender],
                            )
                            .await
                            .unwrap()
                        };
                        cache_store
                    },
                )
                .await;
        }

        self.injector
            .register_typed::<CacheStoreSchedulerImpl, _, _, _>(async move |i| {
                Arc::new(CacheStoreSchedulerImpl::new(
                    cachestore_event_sender_to_move.subscribe(),
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                ))
            })
            .await;
    }

    pub async fn configure_meta_store(&self) {
        let (metastore_event_sender, _) = broadcast::channel(8192); // TODO config
        let metastore_event_sender_to_move = metastore_event_sender.clone();

        if let Some(_) = self.config_obj.metastore_remote_address() {
            self.injector
                .register_typed::<dyn MetaStoreTransport, _, _, _>(async move |i| {
                    MetaStoreTransportImpl::new(i.get_service_typed().await)
                })
                .await;
        }

        if uses_remote_metastore(&self.injector).await {
            self.injector
                .register_typed::<dyn MetaStore, _, _, _>(async move |i| {
                    let transport = ClusterMetaStoreClient::new(i.get_service_typed().await);
                    Arc::new(MetaStoreRpcClient::new(transport))
                })
                .await;
        } else {
            self.injector
                .register("metastore_fs", async move |i| {
                    // TODO metastore works with non queue remote fs as it requires loops to be started prior to load_from_remote call
                    let original_remote_fs = i.get_service("original_remote_fs").await;
                    let arc: Arc<dyn DIService> = BaseRocksStoreFs::new_for_metastore(
                        original_remote_fs,
                        i.get_service_typed().await,
                    );

                    arc
                })
                .await;
            let path = self.meta_store_path().to_str().unwrap().to_string();
            self.injector
                .register_typed_with_default::<dyn MetaStore, RocksMetaStore, _, _>(
                    async move |i| {
                        let config = i.get_service_typed::<dyn ConfigObj>().await;
                        let metastore_fs = i.get_service("metastore_fs").await;
                        let meta_store = if let Some(dump_dir) = config.clone().dump_dir() {
                            RocksMetaStore::load_from_dump(
                                &Path::new(&path),
                                dump_dir,
                                metastore_fs,
                                config,
                            )
                            .await
                            .unwrap()
                        } else {
                            RocksMetaStore::load_from_remote(&path, metastore_fs, config)
                                .await
                                .unwrap()
                        };
                        meta_store.add_listener(metastore_event_sender).await;
                        meta_store
                    },
                )
                .await;
        };

        self.injector
            .register_typed::<dyn WALDataStore, _, _, _>(async move |i| {
                WALStore::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed::<dyn ConfigObj>()
                        .await
                        .wal_split_threshold() as usize,
                )
            })
            .await;

        self.injector
            .register_typed::<dyn ChunkDataStore, _, _, _>(async move |i| {
                ChunkStore::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed::<dyn ConfigObj>()
                        .await
                        .wal_split_threshold() as usize,
                )
            })
            .await;

        self.injector
            .register_typed::<dyn CubestoreParquetMetadataCache, _, _, _>(async move |i| {
                let c = i.get_service_typed::<dyn ConfigObj>().await;
                CubestoreParquetMetadataCacheImpl::new(
                    match c.metadata_cache_max_capacity_bytes() {
                        0 => NoopParquetMetadataCache::new(),
                        max_cached_metadata => LruParquetMetadataCache::new(
                            max_cached_metadata,
                            Duration::from_secs(c.metadata_cache_time_to_idle_secs()),
                        ),
                    },
                )
            })
            .await;

        self.injector
            .register_typed::<dyn CompactionService, _, _, _>(async move |i| {
                CompactionServiceImpl::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                )
            })
            .await;

        self.injector
            .register_typed::<dyn ImportService, _, _, _>(async move |i| {
                ImportServiceImpl::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                )
            })
            .await;

        self.injector
            .register_typed::<dyn StreamingService, _, _, _>(async move |i| {
                StreamingServiceImpl::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                )
            })
            .await;

        self.injector
            .register_typed::<dyn KsqlClient, _, _, _>(async move |_| KsqlClientImpl::new())
            .await;

        self.injector
            .register_typed::<dyn KafkaClientService, _, _, _>(async move |i| {
                KafkaClientServiceImpl::new(i.get_service_typed().await)
            })
            .await;

        let cluster_meta_store_sender = metastore_event_sender_to_move.clone();

        self.injector
            .register_typed_with_default::<dyn Cluster, _, _, _>(async move |i| {
                ClusterImpl::new(
                    i.get_service_typed::<dyn ConfigObj>()
                        .await
                        .server_name()
                        .to_string(),
                    vec!["localhost".to_string()],
                    Arc::downgrade(&i),
                    i.get_service_typed().await,
                    Duration::from_secs(30),
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    cluster_meta_store_sender,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                )
            })
            .await;

        self.injector
            .register_typed::<SchedulerImpl, _, _, _>(async move |i| {
                Arc::new(SchedulerImpl::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    metastore_event_sender_to_move.subscribe(),
                    i.get_service_typed().await,
                ))
            })
            .await;
    }

    pub async fn configure_common(&self) {
        self.injector
            .register_typed_with_default::<dyn RemoteFs, QueueRemoteFs, _, _>(async move |i| {
                QueueRemoteFs::new(
                    i.get_service_typed::<dyn ConfigObj>().await,
                    i.get_service("original_remote_fs").await,
                    i.get_service_typed().await,
                )
            })
            .await;

        self.injector
            .register_typed::<dyn ClusterTransport, _, _, _>(async move |i| {
                ClusterTransportImpl::new(i.get_service_typed().await)
            })
            .await;

        let query_cache = Arc::new(SqlResultCache::new(
            self.config_obj.query_cache_max_capacity_bytes(),
            self.config_obj.query_cache_time_to_idle_secs(),
        ));

        let query_cache_to_move = query_cache.clone();
        self.injector
            .register_typed::<dyn QueryPlanner, _, _, _>(async move |i| {
                QueryPlannerImpl::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    query_cache_to_move,
                )
            })
            .await;

        self.injector
            .register_typed_with_default::<dyn QueryExecutor, _, _, _>(async move |i| {
                QueryExecutorImpl::new(i.get_service_typed().await, i.get_service_typed().await)
            })
            .await;

        self.injector
            .register_typed::<ConcurrencyLimits, _, _, _>(async move |i| {
                Arc::new(ConcurrencyLimits::new(
                    i.get_service_typed::<dyn ConfigObj>()
                        .await
                        .max_ingestion_data_frames(),
                ))
            })
            .await;

        self.injector
            .register_typed_with_default::<dyn TracingHelper, _, _, _>(async move |_| {
                TracingHelperImpl::new()
            })
            .await;

        self.injector
            .register_typed::<dyn MemoryHandler, _, _, _>(async move |_| MemoryHandlerImpl::new())
            .await;

        let query_cache_to_move = query_cache.clone();
        self.injector
            .register_typed::<dyn SqlService, _, _, _>(async move |i| {
                let c = i.get_service_typed::<dyn ConfigObj>().await;
                SqlServiceImpl::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    c.wal_split_threshold() as usize,
                    Duration::from_secs(c.query_timeout()),
                    Duration::from_secs(c.import_job_timeout() * 2),
                    query_cache_to_move,
                )
            })
            .await;

        if self.config_obj.bind_address().is_some() {
            self.injector
                .register_typed::<dyn SqlAuthService, _, _, _>(async move |_| {
                    Arc::new(SqlAuthDefaultImpl)
                })
                .await;

            self.injector
                .register_typed::<MySqlServer, _, _, _>(async move |i| {
                    MySqlServer::new(
                        i.get_service_typed::<dyn ConfigObj>()
                            .await
                            .bind_address()
                            .as_ref()
                            .unwrap()
                            .to_string(),
                        i.get_service_typed().await,
                        i.get_service_typed().await,
                    )
                })
                .await;

            self.injector
                .register_typed::<HttpServer, _, _, _>(async move |i| {
                    let config = i.get_service_typed::<dyn ConfigObj>().await;
                    HttpServer::new(
                        config.http_bind_address().as_ref().unwrap().to_string(),
                        i.get_service_typed().await,
                        i.get_service_typed().await,
                        Duration::from_secs(config.check_ws_orphaned_messages_interval_secs()),
                        Duration::from_secs(config.drop_ws_processing_messages_after_secs()),
                        Duration::from_secs(config.drop_ws_complete_messages_after_secs()),
                        config.transport_max_message_size(),
                        config.transport_max_frame_size(),
                    )
                })
                .await;
        }
    }

    async fn remote_fs(&self) -> Result<Arc<dyn RemoteFs + 'static>, CubeError> {
        self.configure_remote_fs().await;
        Ok(self.injector.get_service("original_remote_fs").await)
    }

    pub fn injector(&self) -> Arc<Injector> {
        self.injector.clone()
    }

    pub async fn configure_injector(&self) {
        self.configure_remote_fs().await;
        self.configure_cache_store().await;
        self.configure_meta_store().await;
        self.configure_common().await;
    }

    pub async fn cube_services(&self) -> CubeServices {
        CubeServices {
            injector: self.injector.clone(),
            sql_service: self.injector.get_service_typed().await,
            rocks_meta_store: if self.injector.has_service_typed::<RocksMetaStore>().await {
                Some(self.injector.get_service_typed().await)
            } else {
                None
            },
            rocks_cache_store: if self
                .injector
                .has_service_typed::<LazyRocksCacheStore>()
                .await
            {
                Some(self.injector.get_service_typed().await)
            } else {
                None
            },
            meta_store: self.injector.get_service_typed().await,
            cluster: self.injector.get_service_typed().await,
        }
    }

    pub async fn worker_services(&self) -> WorkerServices {
        WorkerServices {
            query_executor: self.injector.get_service_typed().await,
        }
    }

    pub async fn configure(&self) -> CubeServices {
        self.configure_injector().await;
        self.cube_services().await
    }
}

type LoopHandle = JoinHandle<Result<(), CubeError>>;

pub async fn uses_remote_metastore(i: &Injector) -> bool {
    i.has_service_typed::<dyn MetaStoreTransport>().await
}

pub fn is_router(c: &dyn ConfigObj) -> bool {
    !c.worker_bind_address().is_some()
}
