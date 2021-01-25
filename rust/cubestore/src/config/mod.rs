use crate::cluster::ClusterImpl;
use crate::import::ImportServiceImpl;
use crate::metastore::RocksMetaStore;
use crate::queryplanner::query_executor::{QueryExecutor, QueryExecutorImpl};
use crate::queryplanner::QueryPlannerImpl;
use crate::remotefs::gcs::GCSRemoteFs;
use crate::remotefs::queue::QueueRemoteFs;
use crate::remotefs::s3::S3RemoteFs;
use crate::remotefs::{LocalDirRemoteFs, RemoteFs};
use crate::scheduler::SchedulerImpl;
use crate::sql::{SqlService, SqlServiceImpl};
use crate::store::compaction::CompactionServiceImpl;
use crate::store::{ChunkStore, WALStore};
use crate::telemetry::{start_track_event_loop, stop_track_event_loop};
use crate::CubeError;
use log::debug;
use log::Level;
use mockall::automock;
use rocksdb::{Options, DB};
use simple_logger::SimpleLogger;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};
use tokio::sync::broadcast;
use tokio::time::Duration;

#[derive(Clone)]
pub struct CubeServices {
    pub sql_service: Arc<dyn SqlService>,
    pub scheduler: Arc<SchedulerImpl>,
    pub meta_store: Arc<RocksMetaStore>,
    pub cluster: Arc<ClusterImpl>,
    pub remote_fs: Arc<QueueRemoteFs>,
}

#[derive(Clone)]
pub struct WorkerServices {
    pub query_executor: Arc<dyn QueryExecutor>,
}

impl CubeServices {
    pub async fn start_processing_loops(&self) -> Result<(), CubeError> {
        self.cluster.start_processing_loops().await;
        QueueRemoteFs::start_processing_loops(self.remote_fs.clone());
        if !self.cluster.is_select_worker() {
            RocksMetaStore::run_upload_loop(self.meta_store.clone());
            let scheduler = self.scheduler.clone();
            tokio::spawn(async move { scheduler.run_scheduler().await });
        } else {
            let cluster = self.cluster.clone();
            tokio::spawn(async move { ClusterImpl::listen_on_worker_port(cluster).await });
        }
        start_track_event_loop().await;
        Ok(())
    }

    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.cluster.stop_processing_loops().await?;
        self.remote_fs.stop_processing_loops()?;
        self.meta_store.stop_processing_loops().await;
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

pub struct Config {
    config_obj: Arc<ConfigObjImpl>,
}

#[automock]
pub trait ConfigObj: Send + Sync {
    fn partition_split_threshold(&self) -> u64;

    fn compaction_chunks_total_size_threshold(&self) -> u64;

    fn compaction_chunks_count_threshold(&self) -> u64;

    fn select_worker_pool_size(&self) -> usize;

    fn bind_port(&self) -> u16;

    fn bind_address(&self) -> &str;

    fn query_timeout(&self) -> u64;

    fn not_used_timeout(&self) -> u64;

    fn select_workers(&self) -> &Vec<String>;

    fn worker_bind_address(&self) -> &Option<String>;

    fn download_concurrency(&self) -> u64;

    fn upload_concurrency(&self) -> u64;
}

#[derive(Debug, Clone)]
pub struct ConfigObjImpl {
    pub partition_split_threshold: u64,
    pub compaction_chunks_total_size_threshold: u64,
    pub compaction_chunks_count_threshold: u64,
    pub data_dir: PathBuf,
    pub store_provider: FileStoreProvider,
    pub select_worker_pool_size: usize,
    pub bind_port: u16,
    pub bind_address: String,
    pub query_timeout: u64,
    pub select_workers: Vec<String>,
    pub worker_bind_address: Option<String>,
    pub upload_concurrency: u64,
    pub download_concurrency: u64,
}

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

    fn select_worker_pool_size(&self) -> usize {
        self.select_worker_pool_size
    }

    fn bind_port(&self) -> u16 {
        self.bind_port
    }

    fn bind_address(&self) -> &str {
        &self.bind_address
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

    fn download_concurrency(&self) -> u64 {
        self.download_concurrency
    }

    fn upload_concurrency(&self) -> u64 {
        self.upload_concurrency
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
            config_obj: Arc::new(ConfigObjImpl {
                data_dir: env::var("CUBESTORE_DATA_DIR")
                    .ok()
                    .map(|v| PathBuf::from(v))
                    .unwrap_or(env::current_dir().unwrap().join(".cubestore").join("data")),
                partition_split_threshold: 1000000,
                compaction_chunks_count_threshold: 4,
                compaction_chunks_total_size_threshold: 500000,
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
                bind_address: env::var("CUBESTORE_BIND_ADDR")
                    .ok()
                    .unwrap_or("0.0.0.0".to_string()),
                bind_port: env::var("CUBESTORE_PORT")
                    .ok()
                    .map(|v| v.parse::<u16>().unwrap())
                    .unwrap_or(3306u16),
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
                upload_concurrency: 4,
                download_concurrency: 8,
            }),
        }
    }

    pub fn test(name: &str) -> Config {
        Config {
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
                bind_port: 3306,
                bind_address: "0.0.0.0".to_string(),
                query_timeout: 15,
                select_workers: Vec::new(),
                worker_bind_address: None,
                upload_concurrency: 4,
                download_concurrency: 8,
            }),
        }
    }

    pub fn update_config(
        &self,
        update_config: impl FnOnce(ConfigObjImpl) -> ConfigObjImpl,
    ) -> Config {
        let new_config = self.config_obj.as_ref().clone();
        Self {
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
        let remote_fs = self.remote_fs().unwrap();
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

            test_fn(services.clone()).await;

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

    fn remote_fs(&self) -> Result<Arc<dyn RemoteFs>, CubeError> {
        Ok(match &self.config_obj.store_provider {
            FileStoreProvider::Filesystem { remote_dir } => {
                LocalDirRemoteFs::new(remote_dir.clone(), self.config_obj.data_dir.clone())
            }
            FileStoreProvider::S3 {
                region,
                bucket_name,
                sub_path,
            } => S3RemoteFs::new(
                self.config_obj.data_dir.clone(),
                region.to_string(),
                bucket_name.to_string(),
                sub_path.clone(),
            )?,
            FileStoreProvider::GCS {
                bucket_name,
                sub_path,
            } => GCSRemoteFs::new(
                self.config_obj.data_dir.clone(),
                bucket_name.to_string(),
                sub_path.clone(),
            )?,
            FileStoreProvider::Local => unimplemented!(), // TODO
        })
    }

    pub async fn configure(&self) -> CubeServices {
        let original_remote_fs = self.remote_fs().unwrap();
        let remote_fs = QueueRemoteFs::new(self.config_obj.clone(), original_remote_fs.clone());
        let (event_sender, event_receiver) = broadcast::channel(10000); // TODO config

        let meta_store = RocksMetaStore::load_from_remote(
            self.meta_store_path().to_str().unwrap(),
            // TODO metastore works with non queue remote fs as it requires loops to be started prior to load_from_remote call
            original_remote_fs,
            self.config_obj.clone(),
        )
        .await
        .unwrap();
        meta_store.add_listener(event_sender).await;
        let wal_store = WALStore::new(meta_store.clone(), remote_fs.clone(), 500000);
        let chunk_store = ChunkStore::new(
            meta_store.clone(),
            remote_fs.clone(),
            wal_store.clone(),
            262144,
        );
        let compaction_service = CompactionServiceImpl::new(
            meta_store.clone(),
            chunk_store.clone(),
            remote_fs.clone(),
            self.config_obj.clone(),
        );
        let import_service = ImportServiceImpl::new(meta_store.clone(), wal_store.clone());
        let query_planner = QueryPlannerImpl::new(meta_store.clone());
        let query_executor = Arc::new(QueryExecutorImpl);
        let cluster = ClusterImpl::new(
            "localhost".to_string(),
            vec!["localhost".to_string()],
            remote_fs.clone(),
            Duration::from_secs(30),
            chunk_store.clone(),
            compaction_service.clone(),
            meta_store.clone(),
            import_service.clone(),
            self.config_obj.clone(),
            query_executor.clone(),
        );

        let sql_service = SqlServiceImpl::new(
            meta_store.clone(),
            wal_store.clone(),
            query_planner.clone(),
            query_executor.clone(),
            cluster.clone(),
        );
        let scheduler = SchedulerImpl::new(
            meta_store.clone(),
            cluster.clone(),
            remote_fs.clone(),
            event_receiver,
            self.config_obj.clone(),
        );

        CubeServices {
            sql_service,
            scheduler: Arc::new(scheduler),
            meta_store,
            cluster,
            remote_fs,
        }
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
