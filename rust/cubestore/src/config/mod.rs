use crate::remotefs::{LocalDirRemoteFs, RemoteFs};
use std::{env, fs};
use crate::metastore::RocksMetaStore;
use std::sync::Arc;
use crate::store::{WALStore, ChunkStore};
use crate::store::compaction::CompactionServiceImpl;
use crate::import::ImportServiceImpl;
use crate::cluster::ClusterImpl;
use tokio::time::Duration;
use crate::queryplanner::QueryPlannerImpl;
use crate::sql::{SqlServiceImpl, SqlService};
use crate::scheduler::SchedulerImpl;
use std::path::PathBuf;
use mockall::automock;
use tokio::sync::broadcast;
use crate::CubeError;
use crate::remotefs::s3::S3RemoteFs;
use crate::queryplanner::query_executor::{QueryExecutor, QueryExecutorImpl};
use rocksdb::{DB, Options};
use std::future::Future;

#[derive(Clone)]
pub struct CubeServices {
    pub sql_service: Arc<dyn SqlService>,
    pub scheduler: Arc<SchedulerImpl>,
    pub meta_store: Arc<RocksMetaStore>,
    pub cluster: Arc<ClusterImpl>
}

#[derive(Clone)]
pub struct WorkerServices {
    pub query_executor: Arc<dyn QueryExecutor>
}

impl CubeServices {
    pub async fn start_processing_loops(&self) -> Result<(), CubeError> {
        self.cluster.start_processing_loops().await;
        let meta_store = self.meta_store.clone();
        tokio::spawn(async move { meta_store.run_upload_loop().await });
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move { scheduler.run_scheduler().await });
        Ok(())
    }


    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.cluster.stop_processing_loops().await?;
        self.meta_store.stop_processing_loops().await;
        self.scheduler.stop_processing_loops()?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum FileStoreProvider {
    Local,
    Filesystem { remote_dir: PathBuf },
    S3 { region: String, bucket_name: String }
}

pub struct Config {
    config_obj: Arc<ConfigObjImpl>
}

#[automock]
pub trait ConfigObj: Send + Sync {
    fn partition_split_threshold(&self) -> u64;

    fn select_worker_pool_size(&self) -> usize;
}

pub struct ConfigObjImpl {
    partition_split_threshold: u64,
    data_dir: PathBuf,
    store_provider: FileStoreProvider,
    select_worker_pool_size: usize
}

impl ConfigObj for ConfigObjImpl {
    fn partition_split_threshold(&self) -> u64 {
        self.partition_split_threshold
    }

    fn select_worker_pool_size(&self) -> usize {
        self.select_worker_pool_size
    }
}

lazy_static! {
    pub static ref WORKER_SERVICES: std::sync::RwLock<Option<WorkerServices>> = std::sync::RwLock::new(None);
}

impl Config {
    pub fn default() -> Config {
        Config {
            config_obj: Arc::new(ConfigObjImpl {
                data_dir: env::current_dir().unwrap().join(".cubestore").join("data"),
                partition_split_threshold: 1000000,
                store_provider: {
                    if let Ok(bucket_name) = env::var("CUBESTORE_S3_BUCKET") {
                        FileStoreProvider::S3 { bucket_name, region: env::var("CUBESTORE_S3_REGION").unwrap() }
                    } else {
                        FileStoreProvider::Filesystem { remote_dir: env::current_dir().unwrap().join("upstream") }
                    }
                },
                select_worker_pool_size: env::var("CUBESTORE_SELECT_WORKERS").ok().map(|v| v.parse::<usize>().unwrap()).unwrap_or(4)
            })
        }
    }

    pub fn test(name: &str) -> Config {
        Config {
            config_obj: Arc::new(ConfigObjImpl {
                data_dir: env::current_dir().unwrap().join(format!("{}-local-store", name)),
                partition_split_threshold: 20,
                store_provider: FileStoreProvider::Filesystem { remote_dir: env::current_dir().unwrap().join(format!("{}-upstream", name)) },
                select_worker_pool_size: 0
            })
        }
    }

    pub async fn run_test<T>(name: &str, test_fn: impl FnOnce(CubeServices) -> T)
    where
    T: Future + Send + 'static,
    T::Output: Send + 'static
    {
        let config = Self::test(name);

        let store_path = config.local_dir().clone();
        let remote_store_path = config.remote_dir().clone();
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        {
            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();

            test_fn(services.clone()).await;

            services.stop_processing_loops().await.unwrap();
        }
        let _ = DB::destroy(&Options::default(), config.meta_store_path());
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
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
            x => panic!("Remote dir called on {:?}", x)
        }
    }

    pub fn meta_store_path(&self) -> PathBuf {
        self.local_dir().join("metastore")
    }

    fn remote_fs(&self) -> Result<Arc<dyn RemoteFs>, CubeError> {
        Ok(match &self.config_obj.store_provider {
            FileStoreProvider::Filesystem { remote_dir } => LocalDirRemoteFs::new(
                remote_dir.clone(),
                self.config_obj.data_dir.clone(),
            ),
            FileStoreProvider::S3 { region, bucket_name } => S3RemoteFs::new(
                self.config_obj.data_dir.clone(),
                region.to_string(),
                bucket_name.to_string(),
            )?,
            FileStoreProvider::Local => unimplemented!(), // TODO
        })
    }

    pub async fn configure(&self) -> CubeServices {
        let remote_fs = self.remote_fs().unwrap();
        let (event_sender, event_receiver) = broadcast::channel(10000); // TODO config

        let meta_store = RocksMetaStore::load_from_remote(self.meta_store_path().to_str().unwrap(), remote_fs.clone()).await.unwrap();
        meta_store.add_listener(event_sender).await;
        let wal_store = WALStore::new(meta_store.clone(), remote_fs.clone(), 500000);
        let chunk_store = ChunkStore::new(meta_store.clone(), remote_fs.clone(), wal_store.clone(), 262144);
        let compaction_service = CompactionServiceImpl::new(meta_store.clone(), chunk_store.clone(), remote_fs.clone(), self.config_obj.clone());
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
            query_executor.clone()
        );

        let sql_service = SqlServiceImpl::new(meta_store.clone(), wal_store.clone(), query_planner.clone(), query_executor.clone(), cluster.clone());
        let scheduler = SchedulerImpl::new(meta_store.clone(), cluster.clone(), remote_fs.clone(), event_receiver);

        CubeServices {
            sql_service,
            scheduler: Arc::new(scheduler),
            meta_store,
            cluster
        }
    }

    pub fn configure_worker(&self) {
        let mut services = WORKER_SERVICES.write().unwrap();
        *services = Some(WorkerServices {
            query_executor: Arc::new(QueryExecutorImpl)
        })
    }

    pub fn current_worker_services() -> WorkerServices {
        WORKER_SERVICES.read().unwrap().as_ref().unwrap().clone()
    }

}