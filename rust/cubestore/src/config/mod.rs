#![allow(deprecated)] // 'vtable' and 'TraitObject' are deprecated.
pub mod injection;
pub mod processing_loop;

use crate::cluster::transport::{
    ClusterTransport, ClusterTransportImpl, MetaStoreTransport, MetaStoreTransportImpl,
};
use crate::cluster::{Cluster, ClusterImpl, ClusterMetaStoreClient};
use crate::config::injection::{DIService, Injector};
use crate::config::processing_loop::ProcessingLoop;
use crate::http::HttpServer;
use crate::import::limits::ConcurrencyLimits;
use crate::import::{ImportService, ImportServiceImpl};
use crate::metastore::{MetaStore, MetaStoreRpcClient, RocksMetaStore};
use crate::mysql::{MySqlServer, SqlAuthDefaultImpl, SqlAuthService};
use crate::queryplanner::query_executor::{QueryExecutor, QueryExecutorImpl};
use crate::queryplanner::{QueryPlanner, QueryPlannerImpl};
use crate::remotefs::gcs::GCSRemoteFs;
use crate::remotefs::minio::MINIORemoteFs;
use crate::remotefs::queue::QueueRemoteFs;
use crate::remotefs::s3::S3RemoteFs;
use crate::remotefs::{LocalDirRemoteFs, RemoteFs};
use crate::scheduler::SchedulerImpl;
use crate::sql::{SqlService, SqlServiceImpl};
use crate::store::compaction::{CompactionService, CompactionServiceImpl};
use crate::store::{ChunkDataStore, ChunkStore, WALDataStore, WALStore};
use crate::streaming::{StreamingService, StreamingServiceImpl};
use crate::telemetry::{
    start_agent_event_loop, start_track_event_loop, stop_agent_event_loop, stop_track_event_loop,
};
use crate::CubeError;
use datafusion::cube_ext;
use futures::future::join_all;
use log::Level;
use log::{debug, error};
use mockall::automock;
use rocksdb::{Options, DB};
use simple_logger::SimpleLogger;
use std::fmt::Display;
use std::future::Future;
use std::path::PathBuf;
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
    pub scheduler: Arc<SchedulerImpl>,
    pub rocks_meta_store: Option<Arc<RocksMetaStore>>,
    pub meta_store: Arc<dyn MetaStore>,
    pub cluster: Arc<ClusterImpl>,
    pub remote_fs: Arc<QueueRemoteFs>,
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
        let remote_fs = self.remote_fs.clone();
        futures.push(cube_ext::spawn(async move {
            QueueRemoteFs::wait_processing_loops(remote_fs.clone()).await
        }));
        if !self.cluster.is_select_worker() {
            let rocks_meta_store = self.rocks_meta_store.clone().unwrap();
            futures.push(cube_ext::spawn(async move {
                RocksMetaStore::wait_upload_loop(rocks_meta_store).await;
                Ok(())
            }));
            let cluster = self.cluster.clone();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            futures.push(cube_ext::spawn(async move {
                ClusterImpl::listen_on_metastore_port(cluster, started_tx).await
            }));
            started_rx.await?;

            let scheduler = self.scheduler.clone();
            futures.extend(SchedulerImpl::spawn_processing_loops(scheduler));

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

        self.remote_fs.stop_processing_loops()?;
        if let Some(rocks_meta) = &self.rocks_meta_store {
            rocks_meta.stop_processing_loops().await;
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
        self.scheduler.stop_processing_loops()?;
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

    fn compaction_chunks_total_size_threshold(&self) -> u64;

    fn compaction_chunks_count_threshold(&self) -> u64;

    fn wal_split_threshold(&self) -> u64;

    fn select_worker_pool_size(&self) -> usize;

    fn job_runners_count(&self) -> usize;

    fn bind_address(&self) -> &Option<String>;

    fn status_bind_address(&self) -> &Option<String>;

    fn http_bind_address(&self) -> &Option<String>;

    fn query_timeout(&self) -> u64;

    fn not_used_timeout(&self) -> u64;

    fn import_job_timeout(&self) -> u64;

    fn stale_stream_timeout(&self) -> u64;

    fn select_workers(&self) -> &Vec<String>;

    fn worker_bind_address(&self) -> &Option<String>;

    fn metastore_bind_address(&self) -> &Option<String>;

    fn metastore_remote_address(&self) -> &Option<String>;

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

    fn max_cached_queries(&self) -> usize;
}

#[derive(Debug, Clone)]
pub struct ConfigObjImpl {
    pub partition_split_threshold: u64,
    pub compaction_chunks_total_size_threshold: u64,
    pub compaction_chunks_count_threshold: u64,
    pub wal_split_threshold: u64,
    pub data_dir: PathBuf,
    pub store_provider: FileStoreProvider,
    pub select_worker_pool_size: usize,
    pub job_runners_count: usize,
    pub bind_address: Option<String>,
    pub status_bind_address: Option<String>,
    pub http_bind_address: Option<String>,
    pub query_timeout: u64,
    /// Must be set to 2*query_timeout in prod, only for overrides in tests.
    pub not_used_timeout: u64,
    pub import_job_timeout: u64,
    pub stale_stream_timeout: u64,
    pub select_workers: Vec<String>,
    pub worker_bind_address: Option<String>,
    pub metastore_bind_address: Option<String>,
    pub metastore_remote_address: Option<String>,
    pub upload_concurrency: u64,
    pub download_concurrency: u64,
    pub connection_timeout: u64,
    pub server_name: String,
    pub max_ingestion_data_frames: usize,
    pub upload_to_remote: bool,
    pub enable_topk: bool,
    pub enable_startup_warmup: bool,
    pub malloc_trim_every_secs: u64,
    pub max_cached_queries: usize,
}

crate::di_service!(ConfigObjImpl, [ConfigObj]);
crate::di_service!(MockConfigObj, [ConfigObj]);

impl ConfigObj for ConfigObjImpl {
    fn partition_split_threshold(&self) -> u64 {
        self.partition_split_threshold
    }

    fn compaction_chunks_total_size_threshold(&self) -> u64 {
        self.compaction_chunks_total_size_threshold
    }

    fn compaction_chunks_count_threshold(&self) -> u64 {
        self.compaction_chunks_count_threshold
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

    fn import_job_timeout(&self) -> u64 {
        self.import_job_timeout
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
    fn max_cached_queries(&self) -> usize {
        self.max_cached_queries
    }
}

lazy_static! {
    pub static ref WORKER_SERVICES: std::sync::RwLock<Option<WorkerServices>> =
        std::sync::RwLock::new(None);
}

lazy_static! {
    pub static ref TEST_LOGGING_INITIALIZED: tokio::sync::RwLock<bool> =
        tokio::sync::RwLock::new(false);
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

fn env_parse<T>(name: &str, default: T) -> T
where
    T: FromStr,
    T::Err: Display,
{
    env_optparse(name).unwrap_or(default)
}

fn env_optparse<T>(name: &str) -> Option<T>
where
    T: FromStr,
    T::Err: Display,
{
    env::var(name).ok().map(|x| match x.parse::<T>() {
        Ok(v) => v,
        Err(e) => panic!("could not parse environment variable '{}': {}", name, e),
    })
}

impl Config {
    pub fn default() -> Config {
        let query_timeout = env_parse("CUBESTORE_QUERY_TIMEOUT", 120);
        Config {
            injector: Injector::new(),
            config_obj: Arc::new(ConfigObjImpl {
                data_dir: env::var("CUBESTORE_DATA_DIR")
                    .ok()
                    .map(|v| PathBuf::from(v))
                    .unwrap_or(env::current_dir().unwrap().join(".cubestore").join("data")),
                partition_split_threshold: 524288 * 2,
                compaction_chunks_count_threshold: 4,
                compaction_chunks_total_size_threshold: 524288,
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
                import_job_timeout: env_parse("CUBESTORE_IMPORT_JOB_TIMEOUT", 600),
                stale_stream_timeout: 60,
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
                upload_concurrency: env_parse("CUBESTORE_MAX_ACTIVE_UPLOADS", 4),
                download_concurrency: env_parse("CUBESTORE_MAX_ACTIVE_DOWNLOADS", 8),
                max_ingestion_data_frames: env_parse("CUBESTORE_MAX_DATA_FRAMES", 4),
                wal_split_threshold: env_parse("CUBESTORE_WAL_SPLIT_THRESHOLD", 524288 / 2),
                job_runners_count: env_parse("CUBESTORE_JOB_RUNNERS", 4),
                connection_timeout: 60,
                server_name: env::var("CUBESTORE_SERVER_NAME")
                    .ok()
                    .unwrap_or("localhost".to_string()),
                upload_to_remote: !env::var("CUBESTORE_NO_UPLOAD").ok().is_some(),
                enable_topk: env_bool("CUBESTORE_ENABLE_TOPK", true),
                enable_startup_warmup: env_bool("CUBESTORE_STARTUP_WARMUP", true),
                malloc_trim_every_secs: env_parse("CUBESTORE_MALLOC_TRIM_EVERY_SECS", 30),
                max_cached_queries: env_parse("CUBESTORE_MAX_CACHED_QUERIES", 10_000),
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
                partition_split_threshold: 20,
                compaction_chunks_count_threshold: 1,
                compaction_chunks_total_size_threshold: 10,
                store_provider: FileStoreProvider::Filesystem {
                    remote_dir: Some(
                        env::current_dir()
                            .unwrap()
                            .join(format!("{}-upstream", name)),
                    ),
                },
                select_worker_pool_size: 0,
                job_runners_count: 4,
                bind_address: None,
                status_bind_address: None,
                http_bind_address: None,
                query_timeout,
                not_used_timeout: 2 * query_timeout,
                import_job_timeout: 600,
                stale_stream_timeout: 60,
                select_workers: Vec::new(),
                worker_bind_address: None,
                metastore_bind_address: None,
                metastore_remote_address: None,
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
                max_cached_queries: 10_000,
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
        self.start_test_with_options(true, test_fn).await
    }

    pub async fn start_test_worker<T>(&self, test_fn: impl FnOnce(CubeServices) -> T)
    where
        T: Future<Output = ()> + Send,
    {
        self.start_test_with_options(false, test_fn).await
    }

    pub async fn start_test_with_options<T>(
        &self,
        clean_remote: bool,
        test_fn: impl FnOnce(CubeServices) -> T,
    ) where
        T: Future<Output = ()> + Send,
    {
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
            let services = self.configure().await;
            services.start_processing_loops().await.unwrap();

            // Should be long enough even for CI.
            let timeout = Duration::from_secs(600);
            if let Err(_) = timeout_at(Instant::now() + timeout, test_fn(services.clone())).await {
                panic!("Test timed out after {} seconds", timeout.as_secs());
            }

            services.stop_processing_loops().await.unwrap();
        }
        let _ = DB::destroy(&Options::default(), self.meta_store_path());
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

    async fn configure_remote_fs(&self) {
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

    async fn remote_fs(&self) -> Result<Arc<dyn RemoteFs + 'static>, CubeError> {
        self.configure_remote_fs().await;
        Ok(self.injector.get_service("original_remote_fs").await)
    }

    pub fn injector(&self) -> Arc<Injector> {
        self.injector.clone()
    }

    pub async fn configure_injector(&self) {
        self.configure_remote_fs().await;

        self.injector
            .register_typed_with_default::<dyn RemoteFs, QueueRemoteFs, _, _>(async move |i| {
                QueueRemoteFs::new(
                    i.get_service_typed::<dyn ConfigObj>().await,
                    i.get_service("original_remote_fs").await,
                )
            })
            .await;

        let (event_sender, _) = broadcast::channel(10000); // TODO config
        let event_sender_to_move = event_sender.clone();

        self.injector
            .register_typed::<dyn ClusterTransport, _, _, _>(async move |i| {
                ClusterTransportImpl::new(i.get_service_typed().await)
            })
            .await;

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
            let path = self.meta_store_path().to_str().unwrap().to_string();
            self.injector
                .register_typed_with_default::<dyn MetaStore, RocksMetaStore, _, _>(
                    async move |i| {
                        let meta_store = RocksMetaStore::load_from_remote(
                            &path,
                            // TODO metastore works with non queue remote fs as it requires loops to be started prior to load_from_remote call
                            i.get_service("original_remote_fs").await,
                            i.get_service_typed::<dyn ConfigObj>().await,
                        )
                        .await
                        .unwrap();
                        meta_store.add_listener(event_sender).await;
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
            .register_typed::<ConcurrencyLimits, _, _, _>(async move |i| {
                Arc::new(ConcurrencyLimits::new(
                    i.get_service_typed::<dyn ConfigObj>()
                        .await
                        .max_ingestion_data_frames(),
                ))
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
                )
            })
            .await;

        self.injector
            .register_typed::<dyn QueryPlanner, _, _, _>(async move |i| {
                QueryPlannerImpl::new(i.get_service_typed().await, i.get_service_typed().await)
            })
            .await;

        self.injector
            .register_typed::<dyn QueryExecutor, _, _, _>(async move |_| {
                Arc::new(QueryExecutorImpl)
            })
            .await;

        let cluster_meta_store_sender = event_sender_to_move.clone();

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
                )
            })
            .await;

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
                    c.wal_split_threshold() as usize,
                    Duration::from_secs(c.query_timeout()),
                    Duration::from_secs(c.import_job_timeout() * 2),
                    c.max_cached_queries(),
                )
            })
            .await;

        self.injector
            .register_typed::<SchedulerImpl, _, _, _>(async move |i| {
                Arc::new(SchedulerImpl::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    event_sender_to_move.subscribe(),
                    i.get_service_typed().await,
                ))
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
                    HttpServer::new(
                        i.get_service_typed::<dyn ConfigObj>()
                            .await
                            .http_bind_address()
                            .as_ref()
                            .unwrap()
                            .to_string(),
                        i.get_service_typed().await,
                        i.get_service_typed().await,
                    )
                })
                .await;
        }
    }

    pub async fn cube_services(&self) -> CubeServices {
        CubeServices {
            injector: self.injector.clone(),
            sql_service: self.injector.get_service_typed().await,
            scheduler: self.injector.get_service_typed().await,
            rocks_meta_store: if self.injector.has_service_typed::<RocksMetaStore>().await {
                Some(self.injector.get_service_typed().await)
            } else {
                None
            },
            meta_store: self.injector.get_service_typed().await,
            cluster: self.injector.get_service_typed().await,
            remote_fs: self.injector.get_service_typed().await,
        }
    }

    pub async fn configure(&self) -> CubeServices {
        self.configure_injector().await;
        self.cube_services().await
    }

    pub fn configure_worker_services() {
        let mut services = WORKER_SERVICES.write().unwrap();
        *services = Some(WorkerServices {
            query_executor: Arc::new(QueryExecutorImpl),
        })
    }

    pub fn current_worker_services() -> WorkerServices {
        WORKER_SERVICES.read().unwrap().as_ref().unwrap().clone()
    }
}

type LoopHandle = JoinHandle<Result<(), CubeError>>;

pub async fn uses_remote_metastore(i: &Injector) -> bool {
    i.has_service_typed::<dyn MetaStoreTransport>().await
}

pub fn is_router(c: &dyn ConfigObj) -> bool {
    !c.worker_bind_address().is_some()
}
