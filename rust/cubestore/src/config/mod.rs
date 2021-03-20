pub mod injection;
pub mod processing_loop;

use crate::cluster::transport::{
    ClusterTransport, ClusterTransportImpl, MetaStoreTransport, MetaStoreTransportImpl,
};
use crate::cluster::{Cluster, ClusterImpl, ClusterMetaStoreClient};
use crate::config::injection::{get_service, get_service_typed, DIService, Injector, InjectorRef};
use crate::config::processing_loop::ProcessingLoop;
use crate::http::HttpServer;
use crate::import::limits::ConcurrencyLimits;
use crate::import::{ImportService, ImportServiceImpl};
use crate::metastore::{MetaStore, MetaStoreRpcClient, RocksMetaStore};
use crate::mysql::{MySqlServer, SqlAuthDefaultImpl, SqlAuthService};
use crate::queryplanner::query_executor::{QueryExecutor, QueryExecutorImpl};
use crate::queryplanner::{QueryPlanner, QueryPlannerImpl};
use crate::remotefs::gcs::GCSRemoteFs;
use crate::remotefs::queue::QueueRemoteFs;
use crate::remotefs::s3::S3RemoteFs;
use crate::remotefs::{LocalDirRemoteFs, RemoteFs};
use crate::scheduler::SchedulerImpl;
use crate::sql::{SqlService, SqlServiceImpl};
use crate::store::compaction::{CompactionService, CompactionServiceImpl};
use crate::store::{ChunkDataStore, ChunkStore, WALDataStore, WALStore};
use crate::telemetry::{start_track_event_loop, stop_track_event_loop};
use crate::CubeError;
use futures::future::join_all;
use log::Level;
use log::{debug, error};
use mockall::automock;
use rocksdb::{Options, DB};
use simple_logger::SimpleLogger;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};
use tokio::sync::broadcast;
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
        let services = self.clone();
        tokio::spawn(async move {
            if let Err(e) = services.wait_processing_loops().await {
                error!("Error in processing loop: {}", e);
            }
        });
        Ok(())
    }

    pub async fn wait_processing_loops(&self) -> Result<(), CubeError> {
        let mut futures = Vec::new();
        let cluster = self.cluster.clone();
        futures.push(tokio::spawn(async move {
            cluster.wait_processing_loops().await
        }));
        let remote_fs = self.remote_fs.clone();
        futures.push(tokio::spawn(async move {
            QueueRemoteFs::wait_processing_loops(remote_fs.clone()).await
        }));
        if !self.cluster.is_select_worker() {
            let rocks_meta_store = self.rocks_meta_store.clone().unwrap();
            futures.push(tokio::spawn(async move {
                RocksMetaStore::wait_upload_loop(rocks_meta_store).await;
                Ok(())
            }));
            let cluster = self.cluster.clone();
            futures.push(tokio::spawn(async move {
                ClusterImpl::listen_on_metastore_port(cluster).await
            }));
            let scheduler = self.scheduler.clone();
            futures.push(tokio::spawn(async move {
                SchedulerImpl::run_scheduler(scheduler).await
            }));
            if self.injector.has_service_typed::<MySqlServer>().await {
                let mysql_server = self.injector.get_service_typed::<MySqlServer>().await;
                futures.push(tokio::spawn(
                    async move { mysql_server.processing_loop().await },
                ));
            }
            if self.injector.has_service_typed::<HttpServer>().await {
                let http_server = self.injector.get_service_typed::<HttpServer>().await;
                futures.push(tokio::spawn(async move { http_server.run_server().await }));
            }
        } else {
            let cluster = self.cluster.clone();
            futures.push(tokio::spawn(async move {
                ClusterImpl::listen_on_worker_port(cluster).await
            }));
        }
        futures.push(tokio::spawn(async move {
            start_track_event_loop().await;
            Ok(())
        }));
        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
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
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum FileStoreProvider {
    Local,
    Filesystem {
        remote_dir: PathBuf,
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

    fn http_bind_address(&self) -> &Option<String>;

    fn query_timeout(&self) -> u64;

    fn not_used_timeout(&self) -> u64;

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
    pub http_bind_address: Option<String>,
    pub query_timeout: u64,
    pub select_workers: Vec<String>,
    pub worker_bind_address: Option<String>,
    pub metastore_bind_address: Option<String>,
    pub metastore_remote_address: Option<String>,
    pub upload_concurrency: u64,
    pub download_concurrency: u64,
    pub connection_timeout: u64,
    pub server_name: String,
    pub max_ingestion_data_frames: usize,
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

    fn http_bind_address(&self) -> &Option<String> {
        &self.http_bind_address
    }

    fn query_timeout(&self) -> u64 {
        self.query_timeout
    }

    fn not_used_timeout(&self) -> u64 {
        self.query_timeout * 2
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
}

lazy_static! {
    pub static ref WORKER_SERVICES: std::sync::RwLock<Option<WorkerServices>> =
        std::sync::RwLock::new(None);
}

lazy_static! {
    pub static ref TEST_LOGGING_INITIALIZED: tokio::sync::RwLock<bool> =
        tokio::sync::RwLock::new(false);
}

impl Config {
    pub fn default() -> Config {
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
                            region: env::var("CUBESTORE_S3_REGION").unwrap(),
                            sub_path: env::var("CUBESTORE_S3_SUB_PATH").ok(),
                        }
                    } else if let Ok(bucket_name) = env::var("CUBESTORE_GCS_BUCKET") {
                        FileStoreProvider::GCS {
                            bucket_name,
                            sub_path: env::var("CUBESTORE_GCS_SUB_PATH").ok(),
                        }
                    } else if let Ok(remote_dir) = env::var("CUBESTORE_REMOTE_DIR") {
                        FileStoreProvider::Filesystem {
                            remote_dir: PathBuf::from(remote_dir),
                        }
                    } else {
                        FileStoreProvider::Filesystem {
                            remote_dir: env::current_dir().unwrap().join("upstream"),
                        }
                    }
                },
                select_worker_pool_size: env::var("CUBESTORE_SELECT_WORKERS")
                    .ok()
                    .map(|v| v.parse::<usize>().unwrap())
                    .unwrap_or(4),
                bind_address: Some(env::var("CUBESTORE_BIND_ADDR").ok().unwrap_or(
                    format!("0.0.0.0:{}", env::var("CUBESTORE_PORT")
                            .ok()
                            .map(|v| v.parse::<u16>().unwrap())
                            .unwrap_or(3306u16)),
                )),
                http_bind_address: Some(env::var("CUBESTORE_HTTP_BIND_ADDR").ok().unwrap_or(
                    format!("0.0.0.0:{}", env::var("CUBESTORE_HTTP_PORT")
                        .ok()
                        .map(|v| v.parse::<u16>().unwrap())
                        .unwrap_or(3030u16)),
                )),
                query_timeout: env::var("CUBESTORE_QUERY_TIMEOUT")
                    .ok()
                    .map(|v| v.parse::<u64>().unwrap())
                    .unwrap_or(120),
                select_workers: env::var("CUBESTORE_WORKERS")
                    .ok()
                    .map(|v| v.split(",").map(|s| s.to_string()).collect())
                    .unwrap_or(Vec::new()),
                worker_bind_address: env::var("CUBESTORE_WORKER_PORT")
                    .ok()
                    .map(|v| format!("0.0.0.0:{}", v)),
                metastore_bind_address: env::var("CUBESTORE_META_PORT")
                    .ok()
                    .map(|v| format!("0.0.0.0:{}", v)),
                metastore_remote_address: env::var("CUBESTORE_META_ADDR").ok(),
                upload_concurrency: 4,
                download_concurrency: 8,
                max_ingestion_data_frames: env::var("CUBESTORE_MAX_DATA_FRAMES")
                    .ok()
                    .map(|v| v.parse::<usize>().unwrap())
                    .unwrap_or(4),
                wal_split_threshold: env::var("CUBESTORE_WAL_SPLIT_THRESHOLD")
                    .ok()
                    .map(|v| v.parse::<u64>().unwrap())
                    .unwrap_or(524288 / 2),
                job_runners_count: env::var("CUBESTORE_JOB_RUNNERS")
                    .ok()
                    .map(|v| v.parse::<usize>().unwrap())
                    .unwrap_or(4),
                connection_timeout: 60,
                server_name: env::var("CUBESTORE_SERVER_NAME")
                    .ok()
                    .unwrap_or("localhost".to_string()),
            }),
        }
    }

    pub fn test(name: &str) -> Config {
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
                    remote_dir: env::current_dir()
                        .unwrap()
                        .join(format!("{}-upstream", name)),
                },
                select_worker_pool_size: 0,
                job_runners_count: 4,
                bind_address: None,
                http_bind_address: None,
                query_timeout: 15,
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
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.start_test_with_options(true, test_fn).await
    }

    pub async fn start_test_worker<T>(&self, test_fn: impl FnOnce(CubeServices) -> T)
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.start_test_with_options(false, test_fn).await
    }

    pub async fn start_test_with_options<T>(
        &self,
        clean_remote: bool,
        test_fn: impl FnOnce(CubeServices) -> T,
    ) where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
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
                let _ = remote_fs.delete_file(file.as_str()).await.unwrap();
            }
        }
    }

    pub async fn run_test<T>(name: &str, test_fn: impl FnOnce(CubeServices) -> T)
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
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
            FileStoreProvider::Filesystem { remote_dir } => remote_dir,
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

        if self
            .injector
            .has_service_typed::<dyn MetaStoreTransport>()
            .await
        {
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
                            get_service(&i, "original_remote_fs").await,
                            get_service_typed::<dyn ConfigObj>(&i).await,
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
                )
            })
            .await;

        self.injector
            .register_typed::<dyn QueryPlanner, _, _, _>(async move |i| {
                QueryPlannerImpl::new(i.get_service_typed().await)
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
                    i.get_service_typed().await,
                    Duration::from_secs(30),
                    i.get_service_typed().await,
                    i.get_service_typed().await,
                    i.get_service_typed().await,
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
                SqlServiceImpl::new(
                    i.get_service_typed().await,
                    i.get_service_typed().await,
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

    pub fn configure_worker(&self) {
        let mut services = WORKER_SERVICES.write().unwrap();
        *services = Some(WorkerServices {
            query_executor: Arc::new(QueryExecutorImpl),
        })
    }

    pub fn current_worker_services() -> WorkerServices {
        WORKER_SERVICES.read().unwrap().as_ref().unwrap().clone()
    }
}
