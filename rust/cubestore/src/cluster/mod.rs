pub mod worker_pool;

use crate::CubeError;
use tokio::net::TcpStream;
use futures::future::join_all;
use tokio::time::{timeout};
use crate::remotefs::RemoteFs;
use std::sync::Arc;
use async_trait::async_trait;
use chrono::{Utc};
use crate::metastore::job::{Job, JobType, JobStatus};
use regex::Regex;
use std::collections::{HashSet, HashMap};
use tokio::fs::File;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use std::time::{SystemTime};
use tokio::{fs, time};
use crate::store::{ChunkDataStore, DataFrame};
use crate::metastore::{RowKey, TableId, MetaStore, IdRow};
use log::{info, error, debug};
use crate::store::compaction::CompactionService;
use tokio::sync::{Notify, oneshot, broadcast, RwLock};
use core::mem;
use std::time::Duration;
use futures::{FutureExt};
use crate::import::ImportService;
use mockall::automock;
use tokio::sync::broadcast::{Sender, Receiver};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use crate::queryplanner::serialized_plan::{SerializedPlan};
use crate::config::{Config, ConfigObj};
use crate::queryplanner::query_executor::QueryExecutor;
use crate::cluster::worker_pool::{WorkerPool, MessageProcessor};

#[automock]
#[async_trait]
pub trait Cluster: Send + Sync {
    async fn notify_job_runner(&self, node_name: String) -> Result<(), CubeError>;

    async fn run_select(&self, node_name: String, plan_node: SerializedPlan) -> Result<DataFrame, CubeError>;

    async fn available_nodes(&self) -> Result<Vec<String>, CubeError>;

    fn server_name(&self) -> &str;

    async fn download(&self, remote_path: &str) -> Result<String, CubeError>;

    fn get_chunk_store(&self) -> &Arc<dyn ChunkDataStore>;

    async fn wait_for_job_result(&self, row_key: RowKey, job_type: JobType) -> Result<JobEvent, CubeError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum JobEvent {
    Started(RowKey, JobType),
    Success(RowKey, JobType),
    Error(RowKey, JobType, String)
}

pub struct ClusterImpl {
    remote_fs: Arc<dyn RemoteFs>,
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    compaction_service: Arc<dyn CompactionService>,
    import_service: Arc<dyn ImportService>,
    connect_timeout: Duration,
    server_name: String,
    server_addresses: Vec<String>,
    connected_nodes: Vec<String>,
    job_notify: Arc<Notify>,
    event_sender: Sender<JobEvent>,
    jobs_enabled: Arc<RwLock<bool>>,
    // used just to hold a reference so event_sender won't be collected
    _receiver: Receiver<JobEvent>,
    select_process_pool: RwLock<Option<Arc<WorkerPool<WorkerMessage, DataFrame, WorkerProcessor>>>>,
    config_obj: Arc<dyn ConfigObj>,
    query_executor: Arc<dyn QueryExecutor>,
}

#[derive(Serialize, Deserialize)]
pub enum WorkerMessage {
    Select(SerializedPlan, HashMap<String, String>)
}

pub struct WorkerProcessor;

impl MessageProcessor<WorkerMessage, DataFrame> for WorkerProcessor {
    fn process(args: WorkerMessage) -> Result<DataFrame, CubeError> {
        match args {
            WorkerMessage::Select(plan_node, remote_to_local_names) => {
                debug!("Running select in worker started: {:?}", plan_node);
                let handle = Handle::current();
                let plan_node_to_send = plan_node.clone();
                let res = handle.block_on(async move { Config::current_worker_services().query_executor.execute_plan(plan_node_to_send, remote_to_local_names).await });
                debug!("Running select in worker completed: {:?}", plan_node);
                res
            }
        }
    }
}

pub struct JobRunner {
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    compaction_service: Arc<dyn CompactionService>,
    import_service: Arc<dyn ImportService>,
    server_name: String,
    notify: Arc<Notify>,
    event_sender: Sender<JobEvent>,
    jobs_enabled: Arc<RwLock<bool>>
}

lazy_static! {
    static ref HEART_BEAT_NODE_REGEX: Regex =
        Regex::new(r"^node-heart-beats/(?P<node>.*)$").unwrap();
}

#[async_trait]
impl Cluster for ClusterImpl {
    async fn notify_job_runner(&self, node_name: String) -> Result<(), CubeError> {
        if self.server_name == node_name {
            self.job_notify.notify();
            Ok(())
        } else {
            unimplemented!()
        }
    }

    async fn run_select(&self, node_name: String, plan_node: SerializedPlan) -> Result<DataFrame, CubeError> {
        if self.server_name == node_name {
            // TODO timeout config
            timeout(Duration::from_secs(60), self.run_local_select(plan_node)).await?
        } else {
            unimplemented!()
        }
    }

    async fn available_nodes(&self) -> Result<Vec<String>, CubeError> {
        Ok(self.connected_nodes.clone())
    }

    fn server_name(&self) -> &str {
        self.server_name.as_str()
    }

    async fn download(&self, remote_path: &str) -> Result<String, CubeError> {
        self.remote_fs.download_file(remote_path).await
    }

    fn get_chunk_store(&self) -> &Arc<dyn ChunkDataStore> {
        &self.chunk_store
    }

    async fn wait_for_job_result(&self, row_key: RowKey, job_type: JobType) -> Result<JobEvent, CubeError> {
        let mut receiver = self.event_sender.subscribe();
        loop {
            let event = receiver.recv().await?;
            if let JobEvent::Success(k, t) | JobEvent::Error(k, t, _) = &event {
                if k == &row_key && t == &job_type {
                    return Ok(event);
                }
            }
        }
    }
}

impl JobRunner {
    async fn processing_loop(&self) {
        loop {
            if !*self.jobs_enabled.read().await {
                return;
            }
            let res = tokio::select! {
                _ = self.notify.notified().fuse() => {
                    self.fetch_and_process().await
                }
                _ = time::delay_for(Duration::from_secs(60)) => {
                    self.fetch_and_process().await
                }
            };
            if let Err(e) = res {
                error!("Error in processing loop: {}", e);
            }
        }
    }

    async fn fetch_and_process(&self) -> Result<(), CubeError> {
        let job = self.meta_store.start_processing_job(self.server_name.to_string()).await?;
        if let Some(to_process) = job {
            self.run_local(to_process).await?;
        }
        Ok(())
    }

    async fn run_local(&self, job: IdRow<Job>) -> Result<(), CubeError> {
        let start = SystemTime::now();
        let job_id = job.get_id();
        let (mut tx, rx) = oneshot::channel::<()>();
        let meta_store = self.meta_store.clone();
        let heart_beat_timer = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tx.closed().fuse() => {
                        break;
                    }
                    _ = time::delay_for(Duration::from_secs(30)) => {
                        let _ = meta_store.update_heart_beat(job_id).await; // TODO handle result
                    }
                }
            }
        });
        debug!("Running job: {:?}", job);
        self.event_sender.send(JobEvent::Started(job.get_row().row_reference().clone(), job.get_row().job_type().clone()))?;
        let res = timeout(Duration::from_secs(300), self.route_job(job.get_row())).await;
        mem::drop(rx);
        heart_beat_timer.await?;
        if let Err(timeout_err) = res {
            self.meta_store.update_status(job_id, JobStatus::Timeout).await?;
            error!("Running job timed out ({:?}): {:?}", start.elapsed()?, self.meta_store.get_job(job_id).await?);
            self.event_sender.send(JobEvent::Error(job.get_row().row_reference().clone(), job.get_row().job_type().clone(), timeout_err.to_string()))?;
        } else if let Ok(Err(cube_err)) = res {
            self.meta_store.update_status(job_id, JobStatus::Error(cube_err.to_string())).await?;
            error!("Running job error ({:?}): {:?}", start.elapsed()?, self.meta_store.get_job(job_id).await?);
            self.event_sender.send(JobEvent::Error(job.get_row().row_reference().clone(), job.get_row().job_type().clone(), cube_err.to_string()))?;
        } else {
            let deleted_job = self.meta_store.delete_job(job_id).await?;
            debug!("Running job completed ({:?}): {:?}", start.elapsed()?, deleted_job);
            self.event_sender.send(JobEvent::Success(job.get_row().row_reference().clone(), job.get_row().job_type().clone()))?;
        }
        Ok(())
    }

    async fn route_job(&self, job: &Job) -> Result<(), CubeError> {
        match job.job_type() {
            JobType::WalPartitioning => {
                if let RowKey::Table(TableId::WALs, wal_id) = job.row_reference() {
                    self.chunk_store.partition(*wal_id).await?;
                } else {
                    Self::fail_job_row_key(job);
                }
            }
            JobType::Repartition => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    self.chunk_store.repartition(*partition_id).await?;
                } else {
                    Self::fail_job_row_key(job);
                }
            }
            JobType::PartitionCompaction => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    self.compaction_service.compact(*partition_id).await?;
                } else {
                    Self::fail_job_row_key(job);
                }
            }
            JobType::TableImport => {
                if let RowKey::Table(TableId::Tables, table_id) = job.row_reference() {
                    self.import_service.import_table(*table_id).await?;
                } else {
                    Self::fail_job_row_key(job);
                }
            }
        }
        Ok(())
    }

    fn fail_job_row_key(job: &Job) {
        panic!("Incorrect row key for {:?}: {:?}", job, job.row_reference());
    }
}

impl ClusterImpl {
    pub fn new(
        server_name: String,
        server_addresses: Vec<String>,
        remote_fs: Arc<dyn RemoteFs>,
        connect_timeout: Duration,
        chunk_store: Arc<dyn ChunkDataStore>,
        compaction_service: Arc<dyn CompactionService>,
        meta_store: Arc<dyn MetaStore>,
        import_service: Arc<dyn ImportService>,
        config_obj: Arc<dyn ConfigObj>,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Arc<ClusterImpl> {
        let (sender, receiver) = broadcast::channel(10000); // TODO config
        Arc::new(ClusterImpl {
            server_name,
            server_addresses,
            remote_fs,
            connect_timeout,
            chunk_store,
            compaction_service,
            import_service,
            meta_store,
            connected_nodes: Vec::new(),
            job_notify: Arc::new(Notify::new()),
            event_sender: sender,
            jobs_enabled: Arc::new(RwLock::new(true)),
            _receiver: receiver,
            select_process_pool: RwLock::new(None),
            config_obj,
            query_executor
        })
    }

    pub async fn start_processing_loops(&self) {
        if self.config_obj.select_worker_pool_size() > 0 {
            let mut pool = self.select_process_pool.write().await;
            *pool = Some(Arc::new(WorkerPool::new(self.config_obj.select_worker_pool_size(), Duration::from_secs(60))));
        }
        for _ in 0..4 { // TODO number of job event loops
            let job_runner = JobRunner {
                meta_store: self.meta_store.clone(),
                chunk_store: self.chunk_store.clone(),
                compaction_service: self.compaction_service.clone(),
                import_service: self.import_service.clone(),
                server_name: self.server_name.clone(),
                notify: self.job_notify.clone(),
                event_sender: self.event_sender.clone(),
                jobs_enabled: self.jobs_enabled.clone()
            };
            tokio::spawn(async move {
                job_runner.processing_loop().await;
            });
        }
    }

    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        let mut jobs_enabled = self.jobs_enabled.write().await;
        *jobs_enabled = false;
        for _ in 0..4 { // TODO number of job event loops
            self.job_notify.notify();
        }
        // if let Some(pool) = self.select_process_pool.read().await.as_ref() {
        //     pool.shutdown();
        // }
        Ok(())
    }

    async fn run_local_select(&self, plan_node: SerializedPlan) -> Result<DataFrame, CubeError> {
        let start = SystemTime::now();
        debug!("Running select: {:?}", plan_node);
        let to_download = plan_node.files_to_download();
        let file_futures = to_download.iter().map(|remote| {
            self.remote_fs.download_file(remote)
        }).collect::<Vec<_>>();
        let remote_to_local_names = to_download.clone().into_iter().zip(join_all(file_futures).await
            .into_iter().collect::<Result<Vec<_>, _>>()?.into_iter())
            .collect::<HashMap<_, _>>();
        let pool_option = self.select_process_pool.read().await.clone();
        let res = if let Some(pool) = pool_option {
            let serialized_plan_node = plan_node.clone();
            pool.process(WorkerMessage::Select(serialized_plan_node, remote_to_local_names)).await
        } else {
            self.query_executor.execute_plan(plan_node.clone(), remote_to_local_names).await
        };
        info!("Running select completed ({:?}): {:?}", start.elapsed()?, plan_node);
        res
    }

    pub async fn try_to_connect(&mut self) -> Result<(), CubeError> {
        let streams = self.server_addresses.iter().filter(|a| *a != &self.server_name)
            .map(|a| timeout(self.connect_timeout, TcpStream::connect(a)))
            .collect::<Vec<_>>();
        let _ = join_all(streams).await;
        // TODO
        Ok(())
    }

    pub async fn elect_leader(&self) -> Result<String, CubeError> {
        let heart_beats_dir = Path::new(self.remote_fs.local_path().await.as_str())
            .join("node-heart-beats");

        fs::create_dir_all(heart_beats_dir.clone()).await?;

        let heart_beat_path = heart_beats_dir
            .join(&self.server_name);

        {
            let mut heart_beat_file = File::create(
                heart_beat_path.clone()
            ).await?;

            heart_beat_file.write_u64(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs());
            heart_beat_file.flush().await?;
        }

        let to_upload = heart_beat_path
            .to_str()
            .unwrap()
            .to_string()
            .replace(&self.remote_fs.local_path().await, "")
            .trim_start_matches("/")
            .to_string();

        self.remote_fs.upload_file(
            &to_upload
        ).await?;

        let heart_beats = self.remote_fs.list_with_metadata("node-heart-beats/").await?;
        let leader_paths = heart_beats.iter()
            .filter(|f| f.updated().clone() + chrono::Duration::from_std(self.connect_timeout).unwrap() * 4 >= Utc::now())
            .flat_map(|f|
                HEART_BEAT_NODE_REGEX
                    .captures(f.remote_path())
                    .and_then(|v| v.name("node"))
                    .map(|v| v.as_str().to_string())
            ).collect::<HashSet<_>>();

        if let Some(leader) = self.server_addresses.iter().find(|a| leader_paths.contains(*a)) {
            return Ok(leader.to_string());
        }

        Err(CubeError::internal("No leader has been elected".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remotefs::LocalDirRemoteFs;
    use std::{env, fs};
    use crate::store::{DataFrame, WALDataStore};
    use crate::metastore::{WAL, table::Table, IdRow, Chunk, RocksMetaStore};
    use async_trait::async_trait;
    use crate::import::MockImportService;
    use crate::queryplanner::query_executor::QueryExecutorImpl;

    struct MockWalStore;

    #[async_trait]
    impl WALDataStore for MockWalStore {
        async fn add_wal(&self, _table: IdRow<Table>, _data: DataFrame) -> Result<IdRow<WAL>, CubeError> {
            unimplemented!()
        }

        async fn get_wal(&self, _wal_id: u64) -> Result<DataFrame, CubeError> {
            unimplemented!()
        }

        async fn delete_wal(&self, _wal_id: u64) -> Result<(), CubeError> {
            unimplemented!()
        }

        fn get_wal_chunk_size(&self) -> usize {
            unimplemented!()
        }
    }

    struct MockChunkStore;

    #[async_trait]
    impl ChunkDataStore for MockChunkStore {
        async fn partition(&self, _wal_id: u64) -> Result<(), CubeError> {
            unimplemented!()
        }

        async fn repartition(&self, _partition_id: u64) -> Result<(), CubeError> {
            unimplemented!()
        }

        async fn get_chunk(&self, _chunk: IdRow<Chunk>) -> Result<DataFrame, CubeError> {
            unimplemented!()
        }

        async fn download_chunk(&self, _chunk: IdRow<Chunk>) -> Result<String, CubeError> {
            unimplemented!()
        }

        async fn delete_remote_chunk(&self, _chunk: IdRow<Chunk>) -> Result<(), CubeError> {
            unimplemented!()
        }
    }

    struct MockCompaction;

    #[async_trait]
    impl CompactionService for MockCompaction {
        async fn compact(&self, _partition_id: u64) -> Result<(), CubeError> {
            unimplemented!()
        }
    }


    #[actix_rt::test]
    async fn elect_leader() {
        let config = Config::test("leader");
        let local = env::temp_dir().join(Path::new("local-leader"));
        if fs::read_dir(local.to_owned()).is_ok() {
            fs::remove_dir_all(local.to_owned()).unwrap();
        }
        fs::create_dir_all(local.to_owned()).unwrap();

        let remote = env::temp_dir().join(Path::new("remote-leader"));
        if fs::read_dir(remote.to_owned()).is_ok() {
            fs::remove_dir_all(remote.to_owned()).unwrap();
        }
        fs::create_dir_all(remote.to_owned()).unwrap();

        let remote_fs = LocalDirRemoteFs::new(remote, local);
        let chunk_store = Arc::new(MockChunkStore);
        let compaction = Arc::new(MockCompaction);
        let meta_store = RocksMetaStore::new(&remote_fs.local_file("meta").await.unwrap(), remote_fs.clone());

        let foo = ClusterImpl::new(
            "foo".to_string(),
            vec!["foo".to_string(), "bar".to_string()],
            remote_fs.clone(),
            Duration::from_secs(30),
            chunk_store.clone(),
            compaction.clone(),
            meta_store.clone(),
            Arc::new(MockImportService::new()),
            config.config_obj(),
            Arc::new(QueryExecutorImpl)
        );

        let bar = ClusterImpl::new(
            "bar".to_string(),
            vec!["foo".to_string(), "bar".to_string()],
            remote_fs.clone(),
            Duration::from_secs(30),
            chunk_store.clone(),
            compaction.clone(),
            meta_store.clone(),
            Arc::new(MockImportService::new()),
            config.config_obj(),
            Arc::new(QueryExecutorImpl)
        );

        remote_fs.drop_local_path().await.unwrap();

        assert_eq!(bar.elect_leader().await.unwrap(), "bar");
        assert_eq!(foo.elect_leader().await.unwrap(), "foo");
        assert_eq!(foo.elect_leader().await.unwrap(), "foo");
    }
}
