pub mod message;
pub mod worker_pool;

use crate::cluster::message::NetworkMessage;
use crate::cluster::worker_pool::{MessageProcessor, WorkerPool};
use crate::config::{Config, ConfigObj};
use crate::import::ImportService;
use crate::metastore::job::{Job, JobStatus, JobType};
use crate::metastore::{IdRow, MetaStore, RowKey, TableId};
use crate::metastore::{
    MetaStoreRpcClientTransport, MetaStoreRpcMethodCall, MetaStoreRpcMethodResult,
    MetaStoreRpcServer,
};
use crate::queryplanner::query_executor::{QueryExecutor, SerializedRecordBatchStream};
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::remotefs::RemoteFs;
use crate::store::compaction::CompactionService;
use crate::store::ChunkDataStore;
use crate::CubeError;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::Utc;
use core::mem;
use futures::future::join_all;
use futures::Future;
use futures_timer::Delay;
use itertools::Itertools;
use log::{debug, error, info};
use mockall::automock;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{broadcast, oneshot, watch, Notify, RwLock};
use tokio::time::timeout;

#[automock]
#[async_trait]
pub trait Cluster: Send + Sync {
    async fn notify_job_runner(&self, node_name: String) -> Result<(), CubeError>;

    async fn run_select(
        &self,
        node_name: &str,
        plan_node: SerializedPlan,
    ) -> Result<Vec<RecordBatch>, CubeError>;

    async fn available_nodes(&self) -> Result<Vec<String>, CubeError>;

    fn server_name(&self) -> &str;

    async fn warmup_download(&self, node_name: &str, remote_path: String) -> Result<(), CubeError>;

    fn job_result_listener(&self) -> JobResultListener;

    async fn node_name_by_partitions(&self, partition_ids: &[u64]) -> Result<String, CubeError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum JobEvent {
    Started(RowKey, JobType),
    Success(RowKey, JobType),
    Error(RowKey, JobType, String),
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
    job_notify: Arc<Notify>,
    event_sender: Sender<JobEvent>,
    jobs_enabled: Arc<RwLock<bool>>,
    // used just to hold a reference so event_sender won't be collected
    _receiver: Receiver<JobEvent>,
    select_process_pool: RwLock<
        Option<Arc<WorkerPool<WorkerMessage, SerializedRecordBatchStream, WorkerProcessor>>>,
    >,
    config_obj: Arc<dyn ConfigObj>,
    query_executor: Arc<dyn QueryExecutor>,
    close_worker_socket_tx: watch::Sender<bool>,
    close_worker_socket_rx: RwLock<watch::Receiver<bool>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Select(SerializedPlan, HashMap<String, String>),
}

pub struct WorkerProcessor;

#[async_trait]
impl MessageProcessor<WorkerMessage, SerializedRecordBatchStream> for WorkerProcessor {
    async fn process(args: WorkerMessage) -> Result<SerializedRecordBatchStream, CubeError> {
        match args {
            WorkerMessage::Select(plan_node, remote_to_local_names) => {
                debug!("Running select in worker started: {:?}", plan_node);
                let plan_node_to_send = plan_node.clone();
                let res = Config::current_worker_services()
                    .query_executor
                    .execute_worker_plan(plan_node_to_send, remote_to_local_names)
                    .await;
                debug!("Running select in worker completed: {:?}", plan_node);
                SerializedRecordBatchStream::write(res?)
            }
        }
    }
}

struct JobRunner {
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    compaction_service: Arc<dyn CompactionService>,
    import_service: Arc<dyn ImportService>,
    server_name: String,
    notify: Arc<Notify>,
    event_sender: Sender<JobEvent>,
    jobs_enabled: Arc<RwLock<bool>>,
}

lazy_static! {
    static ref HEART_BEAT_NODE_REGEX: Regex =
        Regex::new(r"^node-heart-beats/(?P<node>.*)$").unwrap();
}

#[async_trait]
impl Cluster for ClusterImpl {
    async fn notify_job_runner(&self, node_name: String) -> Result<(), CubeError> {
        if self.server_name == node_name {
            self.job_notify.notify_waiters();
        } else {
            self.send_to_worker(&node_name, &NetworkMessage::NotifyJobListeners)
                .await?;
        }
        Ok(())
    }

    async fn run_select(
        &self,
        node_name: &str,
        plan_node: SerializedPlan,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        let response = self
            .send_or_process_locally(node_name, NetworkMessage::Select(plan_node))
            .await?;
        match response {
            NetworkMessage::SelectResult(r) => r.and_then(|x| x.read()),
            _ => panic!("unexpected response for select"),
        }
    }

    async fn available_nodes(&self) -> Result<Vec<String>, CubeError> {
        Ok(vec![self.server_name.to_string()])
    }

    fn server_name(&self) -> &str {
        self.server_name.as_str()
    }

    async fn warmup_download(&self, node_name: &str, remote_path: String) -> Result<(), CubeError> {
        // We only wait for the result is to ensure our request is delivered.
        let response = self
            .send_or_process_locally(node_name, NetworkMessage::WarmupDownload(remote_path))
            .await?;
        match response {
            NetworkMessage::WarmupDownloadResult(r) => r,
            _ => panic!("unexpected result for warmup download"),
        }
    }

    fn job_result_listener(&self) -> JobResultListener {
        JobResultListener {
            receiver: self.event_sender.subscribe(),
        }
    }

    async fn node_name_by_partitions(&self, partition_ids: &[u64]) -> Result<String, CubeError> {
        let workers = self.config_obj.select_workers();
        if workers.is_empty() {
            return Ok(self.server_name.to_string());
        }

        let mut hasher = DefaultHasher::new();
        for p in partition_ids.iter() {
            p.hash(&mut hasher);
        }
        Ok(workers[(hasher.finish() % workers.len() as u64) as usize].to_string())
    }
}

pub struct JobResultListener {
    receiver: Receiver<JobEvent>,
}

impl JobResultListener {
    pub async fn wait_for_job_result(
        self,
        row_key: RowKey,
        job_type: JobType,
    ) -> Result<JobEvent, CubeError> {
        Ok(self
            .wait_for_job_results(vec![(row_key, job_type)])
            .await?
            .into_iter()
            .nth(0)
            .unwrap())
    }

    pub async fn wait_for_job_results(
        mut self,
        mut results: Vec<(RowKey, JobType)>,
    ) -> Result<Vec<JobEvent>, CubeError> {
        let mut res = Vec::new();
        loop {
            if results.len() == 0 {
                return Ok(res);
            }
            let event = self.receiver.recv().await?;
            if let JobEvent::Success(k, t) | JobEvent::Error(k, t, _) = &event {
                if let Some((index, _)) = results
                    .iter()
                    .find_position(|(row_key, job_type)| k == row_key && t == job_type)
                {
                    res.push(event);
                    results.remove(index);
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
                _ = self.notify.notified() => {
                    self.fetch_and_process().await
                }
                _ = Delay::new(Duration::from_secs(5)) => {
                    self.fetch_and_process().await
                }
            };
            if let Err(e) = res {
                error!("Error in processing loop: {}", e);
            }
        }
    }

    async fn fetch_and_process(&self) -> Result<(), CubeError> {
        let job = self
            .meta_store
            .start_processing_job(self.server_name.to_string())
            .await?;
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
                    _ = tx.closed() => {
                        break;
                    }
                    _ = Delay::new(Duration::from_secs(30)) => {
                        let _ = meta_store.update_heart_beat(job_id).await; // TODO handle result
                    }
                }
            }
        });
        debug!("Running job: {:?}", job);
        self.event_sender.send(JobEvent::Started(
            job.get_row().row_reference().clone(),
            job.get_row().job_type().clone(),
        ))?;
        let res = timeout(Duration::from_secs(300), self.route_job(job.get_row())).await;
        mem::drop(rx);
        heart_beat_timer.await?;
        if let Err(timeout_err) = res {
            self.meta_store
                .update_status(job_id, JobStatus::Timeout)
                .await?;
            error!(
                "Running job timed out ({:?}): {:?}",
                start.elapsed()?,
                self.meta_store.get_job(job_id).await?
            );
            self.event_sender.send(JobEvent::Error(
                job.get_row().row_reference().clone(),
                job.get_row().job_type().clone(),
                timeout_err.to_string(),
            ))?;
        } else if let Ok(Err(cube_err)) = res {
            self.meta_store
                .update_status(job_id, JobStatus::Error(cube_err.to_string()))
                .await?;
            error!(
                "Running job error ({:?}): {:?}",
                start.elapsed()?,
                self.meta_store.get_job(job_id).await?
            );
            self.event_sender.send(JobEvent::Error(
                job.get_row().row_reference().clone(),
                job.get_row().job_type().clone(),
                cube_err.to_string(),
            ))?;
        } else {
            let deleted_job = self.meta_store.delete_job(job_id).await?;
            debug!(
                "Running job completed ({:?}): {:?}",
                start.elapsed()?,
                deleted_job
            );
            self.event_sender.send(JobEvent::Success(
                job.get_row().row_reference().clone(),
                job.get_row().job_type().clone(),
            ))?;
        }
        Ok(())
    }

    async fn route_job(&self, job: &Job) -> Result<(), CubeError> {
        match job.job_type() {
            JobType::WalPartitioning => {
                if let RowKey::Table(TableId::WALs, wal_id) = job.row_reference() {
                    let chunk_store = self.chunk_store.clone();
                    let wal_id = *wal_id;
                    tokio::spawn(async move { chunk_store.partition(wal_id).await }).await??
                } else {
                    Self::fail_job_row_key(job);
                }
            }
            JobType::Repartition => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let chunk_store = self.chunk_store.clone();
                    let partition_id = *partition_id;
                    tokio::spawn(async move { chunk_store.repartition(partition_id).await })
                        .await??
                } else {
                    Self::fail_job_row_key(job);
                }
            }
            JobType::PartitionCompaction => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let compaction_service = self.compaction_service.clone();
                    let partition_id = *partition_id;
                    tokio::spawn(async move { compaction_service.compact(partition_id).await })
                        .await??;
                } else {
                    Self::fail_job_row_key(job);
                }
            }
            JobType::TableImport => {
                if let RowKey::Table(TableId::Tables, table_id) = job.row_reference() {
                    let import_service = self.import_service.clone();
                    let table_id = *table_id;
                    tokio::spawn(async move { import_service.import_table(table_id).await })
                        .await??
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
        let (close_worker_socket_tx, close_worker_socket_rx) = watch::channel(false);
        Arc::new(ClusterImpl {
            server_name,
            server_addresses,
            remote_fs,
            connect_timeout,
            chunk_store,
            compaction_service,
            import_service,
            meta_store,
            job_notify: Arc::new(Notify::new()),
            event_sender: sender,
            jobs_enabled: Arc::new(RwLock::new(true)),
            _receiver: receiver,
            select_process_pool: RwLock::new(None),
            config_obj,
            query_executor,
            close_worker_socket_tx,
            close_worker_socket_rx: RwLock::new(close_worker_socket_rx),
        })
    }

    pub fn is_select_worker(&self) -> bool {
        self.config_obj.worker_bind_address().is_some()
    }

    pub async fn wait_for_worker_to_close(&self) {
        let mut receiver = self.close_worker_socket_rx.read().await.clone();
        loop {
            if receiver.changed().await.is_err() {
                return;
            }
            if *receiver.borrow() {
                return;
            }
        }
    }

    pub async fn start_processing_loops(&self) {
        if self.config_obj.select_worker_pool_size() > 0 {
            let mut pool = self.select_process_pool.write().await;
            *pool = Some(Arc::new(WorkerPool::new(
                self.config_obj.select_worker_pool_size(),
                Duration::from_secs(self.config_obj.query_timeout()),
            )));
        }
        for _ in 0..self.config_obj.job_runners_count() {
            // TODO number of job event loops
            let job_runner = JobRunner {
                meta_store: self.meta_store.clone(),
                chunk_store: self.chunk_store.clone(),
                compaction_service: self.compaction_service.clone(),
                import_service: self.import_service.clone(),
                server_name: self.server_name.clone(),
                notify: self.job_notify.clone(),
                event_sender: self.event_sender.clone(),
                jobs_enabled: self.jobs_enabled.clone(),
            };
            tokio::spawn(async move {
                job_runner.processing_loop().await;
            });
        }
    }

    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        let mut jobs_enabled = self.jobs_enabled.write().await;
        *jobs_enabled = false;
        for _ in 0..4 {
            // TODO number of job event loops
            self.job_notify.notify_waiters();
        }
        if let Some(pool) = self.select_process_pool.read().await.as_ref() {
            pool.stop_workers().await?;
        }
        self.close_worker_socket_tx.send(true)?;
        Ok(())
    }

    pub async fn send_to_worker(
        &self,
        worker_node: &str,
        m: &NetworkMessage,
    ) -> Result<NetworkMessage, CubeError> {
        let mut stream = timeout(self.connect_timeout, TcpStream::connect(worker_node)).await??;
        m.send(&mut stream).await?;
        return Ok(NetworkMessage::receive(&mut stream).await?);
    }

    pub async fn listen_on_worker_port(cluster: Arc<ClusterImpl>) -> Result<(), CubeError> {
        if let Some(address) = cluster.config_obj.worker_bind_address() {
            ClusterImpl::listen_on_port("Worker", address, cluster.clone(), async move |c, m| {
                c.process_message_on_worker(m).await
            })
            .await?;
        }
        Ok(())
    }

    pub async fn listen_on_metastore_port(cluster: Arc<ClusterImpl>) -> Result<(), CubeError> {
        if let Some(address) = cluster.config_obj.metastore_bind_address() {
            ClusterImpl::listen_on_port(
                "Meta store",
                address,
                cluster.clone(),
                async move |c, m| c.process_metastore_message(m).await,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn listen_on_port<F: Future<Output = NetworkMessage> + Send + 'static>(
        name: &str,
        address: &str,
        cluster: Arc<ClusterImpl>,
        process_fn: impl Fn(Arc<ClusterImpl>, NetworkMessage) -> F + Send + Sync + Clone + 'static,
    ) -> Result<(), CubeError> {
        let listener = TcpListener::bind(address.clone()).await?;

        info!("{} port open on {}", name, address);

        loop {
            let mut stop_receiver = cluster.close_worker_socket_rx.write().await;
            let (mut socket, _) = tokio::select! {
                res = stop_receiver.changed() => {
                    if res.is_err() || *stop_receiver.borrow() {
                        return Ok(());
                    } else {
                        continue;
                    }
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok(res) => res,
                        Err(err) => {
                            error!("Network error: {}", err);
                            continue;
                        }
                    }
                }
            };
            let cluster_to_move = cluster.clone();
            let process_fn_to_move = process_fn.clone();

            tokio::spawn(async move {
                let request = NetworkMessage::receive(&mut socket).await;
                let response;
                match request {
                    Ok(m) => response = process_fn_to_move(cluster_to_move, m).await,
                    Err(e) => {
                        error!("Network error: {}", e);
                        return;
                    }
                }

                if let Err(e) = response.send(&mut socket).await {
                    error!("Network error: {}", e)
                }
            });
        }
    }

    async fn process_message_on_worker(&self, m: NetworkMessage) -> NetworkMessage {
        match m {
            NetworkMessage::Select(plan) => {
                let res = self.run_local_select_serialized(plan).await;
                NetworkMessage::SelectResult(res)
            }
            NetworkMessage::WarmupDownload(remote_path) => {
                let res = self.remote_fs.download_file(&remote_path).await;
                NetworkMessage::WarmupDownloadResult(res.map(|_| ()))
            }
            NetworkMessage::SelectResult(_) | NetworkMessage::WarmupDownloadResult(_) => {
                panic!("result sent to worker");
            }
            NetworkMessage::MetaStoreCall(_) | NetworkMessage::MetaStoreCallResult(_) => {
                panic!("MetaStoreCall sent to worker");
            }
            NetworkMessage::NotifyJobListeners => {
                self.job_notify.notify_waiters();
                NetworkMessage::NotifyJobListenersSuccess
            }
            NetworkMessage::NotifyJobListenersSuccess => {
                panic!("NotifyJobListenersSuccess sent to worker")
            }
        }
    }

    async fn process_metastore_message(&self, m: NetworkMessage) -> NetworkMessage {
        match m {
            NetworkMessage::MetaStoreCall(method_call) => {
                let server = MetaStoreRpcServer::new(self.meta_store.clone());
                let res = server.invoke_method(method_call).await;
                NetworkMessage::MetaStoreCallResult(res)
            }
            x => panic!("Unexpected message: {:?}", x),
        }
    }

    async fn run_local_select_serialized(
        &self,
        plan_node: SerializedPlan,
    ) -> Result<SerializedRecordBatchStream, CubeError> {
        let start = SystemTime::now();
        debug!("Running select: {:?}", plan_node);
        let to_download = plan_node.files_to_download();
        let file_futures = to_download
            .iter()
            .map(|remote| self.remote_fs.download_file(remote))
            .collect::<Vec<_>>();
        let remote_to_local_names = to_download
            .clone()
            .into_iter()
            .zip(
                join_all(file_futures)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter(),
            )
            .collect::<HashMap<_, _>>();
        let pool_option = self.select_process_pool.read().await.clone();
        let res = if let Some(pool) = pool_option {
            let serialized_plan_node = plan_node.clone();
            pool.process(WorkerMessage::Select(
                serialized_plan_node,
                remote_to_local_names,
            ))
            .await
        } else {
            // TODO optimize for no double conversion
            SerializedRecordBatchStream::write(
                self.query_executor
                    .execute_worker_plan(plan_node.clone(), remote_to_local_names)
                    .await?,
            )
        };
        info!("Running select completed ({:?})", start.elapsed()?);
        res
    }

    pub async fn try_to_connect(&mut self) -> Result<(), CubeError> {
        let streams = self
            .server_addresses
            .iter()
            .filter(|a| *a != &self.server_name)
            .map(|a| timeout(self.connect_timeout, TcpStream::connect(a)))
            .collect::<Vec<_>>();
        let _ = join_all(streams).await;
        // TODO
        Ok(())
    }

    pub async fn elect_leader(&self) -> Result<String, CubeError> {
        let heart_beats_dir =
            Path::new(self.remote_fs.local_path().await.as_str()).join("node-heart-beats");

        fs::create_dir_all(heart_beats_dir.clone()).await?;

        let heart_beat_path = heart_beats_dir.join(&self.server_name);

        {
            let mut heart_beat_file = File::create(heart_beat_path.clone()).await?;

            heart_beat_file
                .write_u64(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)?
                        .as_secs(),
                )
                .await?;
            heart_beat_file.flush().await?;
        }

        let to_upload = heart_beat_path
            .to_str()
            .unwrap()
            .to_string()
            .replace(&self.remote_fs.local_path().await, "")
            .trim_start_matches("/")
            .to_string();

        self.remote_fs.upload_file(&to_upload).await?;

        let heart_beats = self
            .remote_fs
            .list_with_metadata("node-heart-beats/")
            .await?;
        let leader_paths = heart_beats
            .iter()
            .filter(|f| {
                f.updated().clone() + chrono::Duration::from_std(self.connect_timeout).unwrap() * 4
                    >= Utc::now()
            })
            .flat_map(|f| {
                HEART_BEAT_NODE_REGEX
                    .captures(f.remote_path())
                    .and_then(|v| v.name("node"))
                    .map(|v| v.as_str().to_string())
            })
            .collect::<HashSet<_>>();

        if let Some(leader) = self
            .server_addresses
            .iter()
            .find(|a| leader_paths.contains(*a))
        {
            return Ok(leader.to_string());
        }

        Err(CubeError::internal(
            "No leader has been elected".to_string(),
        ))
    }

    async fn send_or_process_locally(
        &self,
        node_name: &str,
        m: NetworkMessage,
    ) -> Result<NetworkMessage, CubeError> {
        if self.server_name == node_name {
            // TODO: query_timeout currently used for all messages.
            // TODO timeout config
            Ok(timeout(
                Duration::from_secs(self.config_obj.query_timeout()),
                self.process_message_on_worker(m),
            )
            .await?)
        } else {
            timeout(
                Duration::from_secs(self.config_obj.query_timeout()),
                self.send_to_worker(node_name, &m),
            )
            .await?
        }
    }
}

pub struct ClusterMetaStoreClient {
    meta_remote_addr: String,
    config: Arc<dyn ConfigObj>,
}

impl ClusterMetaStoreClient {
    pub fn new(meta_remote_addr: String, config: Arc<dyn ConfigObj>) -> Arc<Self> {
        Arc::new(Self {
            meta_remote_addr,
            config,
        })
    }
}

#[async_trait]
impl MetaStoreRpcClientTransport for ClusterMetaStoreClient {
    async fn invoke_method(
        &self,
        method_call: MetaStoreRpcMethodCall,
    ) -> Result<MetaStoreRpcMethodResult, CubeError> {
        let m = NetworkMessage::MetaStoreCall(method_call);
        let mut stream = timeout(
            Duration::from_secs(self.config.connection_timeout()),
            TcpStream::connect(self.meta_remote_addr.to_string()),
        )
        .await??;
        m.send(&mut stream).await?;
        let message = NetworkMessage::receive(&mut stream).await?;
        Ok(match message {
            NetworkMessage::MetaStoreCallResult(res) => res,
            x => panic!("Unexpected message: {:?}", x),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::MockImportService;
    use crate::metastore::{table::Table, Chunk, IdRow, RocksMetaStore, WAL};
    use crate::queryplanner::query_executor::QueryExecutorImpl;
    use crate::remotefs::LocalDirRemoteFs;
    use crate::store::{DataFrame, WALDataStore};
    use async_trait::async_trait;
    use std::{env, fs};

    struct MockWalStore;

    #[async_trait]
    impl WALDataStore for MockWalStore {
        async fn add_wal(
            &self,
            _table: IdRow<Table>,
            _data: DataFrame,
        ) -> Result<IdRow<WAL>, CubeError> {
            unimplemented!()
        }

        async fn get_wal(&self, _wal_id: u64) -> Result<DataFrame, CubeError> {
            unimplemented!()
        }

        fn get_wal_chunk_size(&self) -> usize {
            unimplemented!()
        }
    }

    struct MockChunkStore;

    #[async_trait]
    impl ChunkDataStore for MockChunkStore {
        async fn partition_data(
            &self,
            _table_id: u64,
            _data: DataFrame,
        ) -> Result<Vec<u64>, CubeError> {
            unimplemented!()
        }

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

    #[tokio::test]
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
        let meta_store = RocksMetaStore::new(
            &remote_fs.local_file("meta").await.unwrap(),
            remote_fs.clone(),
            config.config_obj(),
        );

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
            Arc::new(QueryExecutorImpl),
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
            Arc::new(QueryExecutorImpl),
        );

        remote_fs.drop_local_path().await.unwrap();

        assert_eq!(bar.elect_leader().await.unwrap(), "bar");
        assert_eq!(foo.elect_leader().await.unwrap(), "foo");
        assert_eq!(foo.elect_leader().await.unwrap(), "foo");
    }
}
