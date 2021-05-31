pub mod message;

pub mod transport;
#[cfg(not(target_os = "windows"))]
pub mod worker_pool;

#[cfg(not(target_os = "windows"))]
use crate::cluster::worker_pool::{MessageProcessor, WorkerPool};

use crate::ack_error;
use crate::cluster::message::NetworkMessage;
use crate::cluster::transport::{ClusterTransport, MetaStoreTransport, WorkerConnection};
use crate::config::injection::DIService;
#[allow(unused_imports)]
use crate::config::{Config, ConfigObj};
use crate::import::ImportService;
use crate::metastore::chunks::chunk_file_name;
use crate::metastore::job::{Job, JobStatus, JobType};
use crate::metastore::partition::partition_file_name;
use crate::metastore::{Chunk, IdRow, MetaStore, MetaStoreEvent, Partition, RowKey, TableId};
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
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::Utc;
use core::mem;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use flatbuffers::bitflags::_core::pin::Pin;
use futures::future::join_all;
use futures::task::{Context, Poll};
use futures::{Future, Stream};
use futures_timer::Delay;
use itertools::Itertools;
use log::{debug, error, info, warn};
use mockall::automock;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Weak;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::SystemTime;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{oneshot, watch, Notify, RwLock};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};
use tracing_futures::WithSubscriber;

#[automock]
#[async_trait]
pub trait Cluster: DIService + Send + Sync {
    async fn notify_job_runner(&self, node_name: String) -> Result<(), CubeError>;

    async fn run_select(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<Vec<RecordBatch>, CubeError>;

    /// Like [run_select], but streams results as they are requested.
    /// This allows to send only a limited number of results, if the caller does not need all.
    async fn run_select_stream(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<SendableRecordBatchStream, CubeError>;

    async fn available_nodes(&self) -> Result<Vec<String>, CubeError>;

    fn server_name(&self) -> &str;

    async fn warmup_download(&self, node_name: &str, remote_path: String) -> Result<(), CubeError>;

    async fn warmup_partition(
        &self,
        partition: IdRow<Partition>,
        chunks: Vec<IdRow<Chunk>>,
    ) -> Result<(), CubeError>;

    fn job_result_listener(&self) -> JobResultListener;

    fn node_name_by_partitions(&self, partition_ids: &[u64]) -> String;

    async fn node_name_for_import(
        &self,
        table_id: u64,
        location: &str,
    ) -> Result<String, CubeError>;

    async fn process_message_on_worker(&self, m: NetworkMessage) -> NetworkMessage;

    async fn process_metastore_message(&self, m: NetworkMessage) -> NetworkMessage;
}

crate::di_service!(MockCluster, [Cluster]);

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum JobEvent {
    Started(RowKey, JobType),
    Success(RowKey, JobType),
    Error(RowKey, JobType, String),
}

pub struct ClusterImpl {
    this: Weak<ClusterImpl>,
    remote_fs: Arc<dyn RemoteFs>,
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    compaction_service: Arc<dyn CompactionService>,
    import_service: Arc<dyn ImportService>,
    cluster_transport: Arc<dyn ClusterTransport>,
    connect_timeout: Duration,
    server_name: String,
    server_addresses: Vec<String>,
    job_notify: Arc<Notify>,
    meta_store_sender: Sender<MetaStoreEvent>,
    jobs_enabled: Arc<RwLock<bool>>,
    #[cfg(not(target_os = "windows"))]
    select_process_pool: RwLock<
        Option<
            Arc<
                WorkerPool<
                    WorkerMessage,
                    (SchemaRef, Vec<SerializedRecordBatchStream>),
                    WorkerProcessor,
                >,
            >,
        >,
    >,
    config_obj: Arc<dyn ConfigObj>,
    query_executor: Arc<dyn QueryExecutor>,
    stop_token: CancellationToken,
    close_worker_socket_tx: watch::Sender<bool>,
    close_worker_socket_rx: RwLock<watch::Receiver<bool>>,
}

crate::di_service!(ClusterImpl, [Cluster]);

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Select(SerializedPlan, HashMap<String, String>),
}
#[cfg(not(target_os = "windows"))]
pub struct WorkerProcessor;

#[cfg(not(target_os = "windows"))]
#[async_trait]
impl MessageProcessor<WorkerMessage, (SchemaRef, Vec<SerializedRecordBatchStream>)>
    for WorkerProcessor
{
    async fn process(
        args: WorkerMessage,
    ) -> Result<(SchemaRef, Vec<SerializedRecordBatchStream>), CubeError> {
        match args {
            WorkerMessage::Select(plan_node, remote_to_local_names) => {
                debug!("Running select in worker started: {:?}", plan_node);
                let plan_node_to_send = plan_node.clone();
                let res = Config::current_worker_services()
                    .query_executor
                    .execute_worker_plan(plan_node_to_send, remote_to_local_names)
                    .await;
                debug!("Running select in worker completed: {:?}", plan_node);
                let (schema, records) = res?;
                let records = SerializedRecordBatchStream::write(schema.as_ref(), records)?;
                Ok((schema, records))
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
            self.send_to_worker(&node_name, NetworkMessage::NotifyJobListeners)
                .await?;
        }
        Ok(())
    }

    #[instrument(level = "trace", skip(self, plan_node))]
    async fn run_select(
        &self,
        node_name: &str,
        plan_node: SerializedPlan,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        let response = self
            .send_or_process_locally(node_name, NetworkMessage::Select(plan_node))
            .await?;
        match response {
            NetworkMessage::SelectResult(r) => {
                r.and_then(|(_, batches)| batches.into_iter().map(|b| b.read()).collect())
            }
            _ => panic!("unexpected response for select"),
        }
    }

    async fn run_select_stream(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<SendableRecordBatchStream, CubeError> {
        self.this
            .upgrade()
            .unwrap()
            .run_select_stream_impl(node_name, plan)
            .await
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
            receiver: self.meta_store_sender.subscribe(),
        }
    }

    fn node_name_by_partitions(&self, partition_ids: &[u64]) -> String {
        let workers = self.config_obj.select_workers();
        if workers.is_empty() {
            return self.server_name.to_string();
        }

        let mut hasher = DefaultHasher::new();
        for p in partition_ids.iter() {
            p.hash(&mut hasher);
        }
        workers[(hasher.finish() % workers.len() as u64) as usize].clone()
    }

    async fn node_name_for_import(
        &self,
        table_id: u64,
        location: &str,
    ) -> Result<String, CubeError> {
        let workers = self.config_obj.select_workers();
        if workers.is_empty() {
            return Ok(self.server_name.to_string());
        }
        let mut hasher = DefaultHasher::new();
        table_id.hash(&mut hasher);
        location.hash(&mut hasher);
        Ok(workers[(hasher.finish() % workers.len() as u64) as usize].to_string())
    }

    async fn warmup_partition(
        &self,
        partition: IdRow<Partition>,
        chunks: Vec<IdRow<Chunk>>,
    ) -> Result<(), CubeError> {
        let node_name = self.node_name_by_partitions(&[partition.get_id()]);
        let mut futures = Vec::new();
        if let Some(name) = partition.get_row().get_full_name(partition.get_id()) {
            futures.push(self.warmup_download(&node_name, name));
        }
        for chunk in chunks.iter() {
            let name = chunk.get_row().get_full_name(chunk.get_id());
            futures.push(self.warmup_download(&node_name, name));
        }
        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self, m))]
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
            NetworkMessage::SelectStart(..)
            | NetworkMessage::SelectResultSchema(..)
            | NetworkMessage::SelectResultBatch(..) => {
                panic!("streaming request passed to process_message")
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
}

#[async_trait]
pub trait MessageStream: Send + Sync {
    async fn next(&mut self) -> (NetworkMessage, /*finished*/ bool);
}

pub struct JobResultListener {
    receiver: Receiver<MetaStoreEvent>,
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
            if let MetaStoreEvent::UpdateJob(old, new) = &event {
                if old.get_row().status() != new.get_row().status() {
                    let job_event = match new.get_row().status() {
                        JobStatus::Scheduled(_) => None,
                        JobStatus::ProcessingBy(_) => None,
                        JobStatus::Completed => Some(JobEvent::Success(
                            new.get_row().row_reference().clone(),
                            new.get_row().job_type().clone(),
                        )),
                        JobStatus::Timeout => Some(JobEvent::Error(
                            new.get_row().row_reference().clone(),
                            new.get_row().job_type().clone(),
                            "Job timed out".to_string(),
                        )),
                        JobStatus::Error(e) => Some(JobEvent::Error(
                            new.get_row().row_reference().clone(),
                            new.get_row().job_type().clone(),
                            e.to_string(),
                        )),
                    };
                    if let Some(event) = job_event {
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
        let res = timeout(Duration::from_secs(600), self.route_job(job.get_row())).await;
        mem::drop(rx);
        heart_beat_timer.await?;
        if let Err(_) = res {
            self.meta_store
                .update_status(job_id, JobStatus::Timeout)
                .await?;
            error!(
                "Running job timed out ({:?}): {:?}",
                start.elapsed()?,
                self.meta_store.get_job(job_id).await?
            );
        } else if let Ok(Err(cube_err)) = res {
            self.meta_store
                .update_status(job_id, JobStatus::Error(cube_err.to_string()))
                .await?;
            error!(
                "Running job error ({:?}): {:?}",
                start.elapsed()?,
                self.meta_store.get_job(job_id).await?
            );
        } else {
            let job = self
                .meta_store
                .update_status(job_id, JobStatus::Completed)
                .await?;
            info!("Running job completed ({:?}): {:?}", start.elapsed()?, job);
            // TODO delete jobs on reconciliation
            self.meta_store.delete_job(job_id).await?;
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
            JobType::TableImportCSV(location) => {
                if let RowKey::Table(TableId::Tables, table_id) = job.row_reference() {
                    self.import_service
                        .clone()
                        .import_table_part(*table_id, location)
                        .await?
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
        meta_store_sender: Sender<MetaStoreEvent>,
        cluster_transport: Arc<dyn ClusterTransport>,
    ) -> Arc<ClusterImpl> {
        let (close_worker_socket_tx, close_worker_socket_rx) = watch::channel(false);
        Arc::new_cyclic(|this| ClusterImpl {
            this: this.clone(),
            server_name,
            server_addresses,
            remote_fs,
            connect_timeout,
            chunk_store,
            compaction_service,
            import_service,
            meta_store,
            cluster_transport,
            job_notify: Arc::new(Notify::new()),
            meta_store_sender,
            jobs_enabled: Arc::new(RwLock::new(true)),
            #[cfg(not(target_os = "windows"))]
            select_process_pool: RwLock::new(None),
            config_obj,
            query_executor,
            stop_token: CancellationToken::new(),
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

    pub async fn wait_processing_loops(&self) -> Result<(), CubeError> {
        let mut futures = Vec::new();
        #[cfg(not(target_os = "windows"))]
        if self.config_obj.select_worker_pool_size() > 0 {
            let mut pool = self.select_process_pool.write().await;
            let arc = Arc::new(WorkerPool::new(
                self.config_obj.select_worker_pool_size(),
                Duration::from_secs(self.config_obj.query_timeout()),
            ));
            *pool = Some(arc.clone());
            futures.push(tokio::spawn(
                async move { arc.wait_processing_loops().await },
            ));
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
                jobs_enabled: self.jobs_enabled.clone(),
            };
            futures.push(tokio::spawn(async move {
                job_runner.processing_loop().await;
            }));
        }
        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        let mut jobs_enabled = self.jobs_enabled.write().await;
        *jobs_enabled = false;
        for _ in 0..4 {
            // TODO number of job event loops
            self.job_notify.notify_waiters();
        }

        #[cfg(not(target_os = "windows"))]
        if let Some(pool) = self.select_process_pool.read().await.as_ref() {
            pool.stop_workers().await?;
        }

        self.close_worker_socket_tx.send(true)?;
        self.stop_token.cancel();
        Ok(())
    }

    pub async fn send_to_worker(
        &self,
        worker_node: &str,
        m: NetworkMessage,
    ) -> Result<NetworkMessage, CubeError> {
        self.cluster_transport
            .send_to_worker(worker_node.to_string(), m)
            .await
    }

    pub async fn listen_on_worker_port(
        cluster: Arc<ClusterImpl>,
        on_socket_bound: oneshot::Sender<()>,
    ) -> Result<(), CubeError> {
        let address = match cluster.config_obj.worker_bind_address() {
            Some(a) => a,
            None => {
                let _ = on_socket_bound.send(());
                return Ok(());
            }
        };
        ClusterImpl::listen_on_port(
            "Worker",
            address,
            cluster.clone(),
            on_socket_bound,
            async move |c, mut socket| {
                let m = match NetworkMessage::receive(&mut socket).await {
                    Ok(m) => m,
                    Err(e) => {
                        error!("Network error: {}", e);
                        return;
                    }
                };

                if !m.is_streaming_request() {
                    let response = c.process_message_on_worker(m).await;
                    if let Err(e) = response.send(&mut socket).await {
                        error!("Network error: {}", e);
                        return;
                    }
                } else {
                    let mut p = c.start_stream_on_worker(m).await;
                    loop {
                        let (response, finished) = p.next().await;
                        match response.maybe_send(&mut socket).await {
                            // All ok, continue streaming.
                            Ok(true) => {}
                            // Connection closed by client, stop streaming.
                            Ok(false) => {
                                break;
                            }
                            Err(e) => {
                                error!("Network error: {}", e);
                                return;
                            }
                        }
                        if finished {
                            break;
                        }
                    }
                }
            },
        )
        .await
    }

    pub async fn listen_on_metastore_port(
        cluster: Arc<ClusterImpl>,
        on_socket_bound: oneshot::Sender<()>,
    ) -> Result<(), CubeError> {
        if let Some(address) = cluster.config_obj.metastore_bind_address() {
            ClusterImpl::process_on_port(
                "Meta store",
                address,
                cluster.clone(),
                on_socket_bound,
                async move |c, m| c.process_metastore_message(m).await,
            )
            .await?;
        } else {
            let _ = on_socket_bound.send(());
        }
        Ok(())
    }

    pub async fn listen_on_port<F: Future<Output = ()> + Send>(
        name: &str,
        address: &str,
        cluster: Arc<ClusterImpl>,
        on_socket_bound: oneshot::Sender<()>,
        process_fn: impl Fn(Arc<ClusterImpl>, TcpStream) -> F + Send + Sync + Clone + 'static,
    ) -> Result<(), CubeError> {
        let listener = TcpListener::bind(address.clone()).await?;
        let _ = on_socket_bound.send(());

        info!("{} port open on {}", name, address);

        loop {
            let mut stop_receiver = cluster.close_worker_socket_rx.write().await;
            let (socket, _) = tokio::select! {
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
                process_fn_to_move(cluster_to_move, socket).await;
            });
        }
    }

    pub async fn process_on_port<F: Future<Output = NetworkMessage> + Send>(
        name: &str,
        address: &str,
        cluster: Arc<ClusterImpl>,
        on_socket_bound: oneshot::Sender<()>,
        process_fn: impl Fn(Arc<ClusterImpl>, NetworkMessage) -> F + Send + Sync + Clone + 'static,
    ) -> Result<(), CubeError> {
        Self::listen_on_port(
            name,
            address,
            cluster,
            on_socket_bound,
            move |c, mut socket| {
                let cluster = c.clone();
                let process_fn = process_fn.clone();
                async move {
                    let request = NetworkMessage::receive(&mut socket).await;
                    let response;
                    match request {
                        Ok(m) => response = process_fn(cluster.clone(), m).await,
                        Err(e) => {
                            error!("Network error: {}", e);
                            return;
                        }
                    }

                    if let Err(e) = response.send(&mut socket).await {
                        error!("Network error: {}", e)
                    }
                }
            },
        )
        .await
    }

    #[instrument(level = "trace", skip(self, plan_node))]
    async fn run_local_select_serialized(
        &self,
        plan_node: SerializedPlan,
    ) -> Result<(SchemaRef, Vec<SerializedRecordBatchStream>), CubeError> {
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
                    .instrument(tracing::span!(tracing::Level::TRACE, "warmup_download"))
                    .with_current_subscriber()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter(),
            )
            .collect::<HashMap<_, _>>();
        let warmup = start.elapsed()?;
        if warmup.as_millis() > 200 {
            warn!("Warmup download for select ({:?})", warmup);
        }

        #[cfg(target_os = "windows")]
        {
            // TODO optimize for no double conversion
            let (schema, records) = self
                .query_executor
                .execute_worker_plan(plan_node.clone(), remote_to_local_names)
                .await?;
            let records = SerializedRecordBatchStream::write(schema.as_ref(), records);
            info!("Running select completed ({:?})", start.elapsed()?);
            Ok((schema, records?))
        }

        #[cfg(not(target_os = "windows"))]
        {
            let pool_option = self.select_process_pool.read().await.clone();

            let res = if let Some(pool) = pool_option {
                let serialized_plan_node = plan_node.clone();
                pool.process(WorkerMessage::Select(
                    serialized_plan_node,
                    remote_to_local_names,
                ))
                .instrument(tracing::span!(
                    tracing::Level::TRACE,
                    "execute_worker_plan_on_pool"
                ))
                .await
            } else {
                // TODO optimize for no double conversion
                let (schema, records) = self
                    .query_executor
                    .execute_worker_plan(plan_node.clone(), remote_to_local_names)
                    .await?;
                let records = SerializedRecordBatchStream::write(schema.as_ref(), records);
                Ok((schema, records?))
            };

            info!("Running select completed ({:?})", start.elapsed()?);
            res
        }
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

        self.remote_fs
            .upload_file(heart_beat_path.to_str().unwrap(), &to_upload)
            .await?;

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

    #[instrument(level = "trace", skip(self, m))]
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
                self.send_to_worker(node_name, m),
            )
            .await?
        }
    }

    #[instrument(level = "trace", skip(self, m))]
    async fn call_streaming(
        self: &Arc<Self>,
        node_name: &str,
        m: NetworkMessage,
    ) -> Result<Box<dyn WorkerConnection>, CubeError> {
        assert!(m.is_streaming_request());
        if self.server_name == node_name {
            let c: Box<dyn WorkerConnection> = Box::new(LoopbackConnection {
                stream: ClusterImpl::start_stream_on_worker(self.clone(), m).await,
            });
            Ok(c)
        } else {
            let mut c = self
                .cluster_transport
                .connect_to_worker(node_name.to_string())
                .await?;
            c.send(m).await?;
            Ok(c)
        }
    }

    async fn start_stream_on_worker(self: Arc<Self>, m: NetworkMessage) -> Box<dyn MessageStream> {
        match m {
            NetworkMessage::SelectStart(p) => {
                let (schema, results) = match self.run_local_select_serialized(p).await {
                    Err(e) => return Box::new(QueryStream::new_error(e)),
                    Ok(x) => x,
                };
                Box::new(QueryStream::new(schema, results))
            }
            _ => panic!("non-streaming request passed to start_stream"),
        }
    }

    async fn run_select_stream_impl(
        self: &Arc<Self>,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<SendableRecordBatchStream, CubeError> {
        let init_message = NetworkMessage::SelectStart(plan);
        let mut c = self.call_streaming(node_name, init_message).await?;
        let schema = match c.receive().await? {
            NetworkMessage::SelectResultSchema(s) => s,
            _ => panic!("unexpected response to select stream"),
        }?;
        return Ok(Box::pin(SelectStream {
            schema,
            connection: Some(c),
            pending: Mutex::new(None),
            finished: false,
        }));

        type ConnPtr = Box<dyn WorkerConnection>;
        struct SelectStream {
            schema: SchemaRef,
            connection: Option<ConnPtr>,
            pending: Mutex<
                Option<
                    Pin<
                        Box<
                            dyn Future<Output = (Result<NetworkMessage, CubeError>, ConnPtr)>
                                + Send,
                        >,
                    >,
                >,
            >,
            finished: bool,
        }

        impl Stream for SelectStream {
            type Item = Result<RecordBatch, ArrowError>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if self.finished {
                    return Poll::Ready(None);
                }

                if self.pending.lock().unwrap().is_none() {
                    let mut connection = self.as_mut().connection.take().unwrap();
                    *self.pending.lock().unwrap() = Some(Box::pin(async move {
                        let res = connection.receive().await;
                        (res, connection)
                    }));
                }
                let (message, connection) = match self
                    .pending
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .poll(cx)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(r) => r,
                };
                *self.pending.lock().unwrap() = None;
                self.connection = Some(connection);

                let r = match message {
                    Err(e) => return self.on_error(e.into()),
                    Ok(NetworkMessage::SelectResultBatch(r)) => r,
                    _ => panic!("invalid result message for select"),
                };
                match r {
                    Ok(Some(batch)) => match batch.read() {
                        Ok(batch) => Poll::Ready(Some(Ok(batch))),
                        Err(e) => return self.on_error(e.into()),
                    },
                    Ok(None) => {
                        self.finished = true;
                        Poll::Ready(None)
                    }
                    Err(e) => return self.on_error(e.into()),
                }
            }
        }

        impl SelectStream {
            fn on_error<T>(
                mut self: Pin<&mut Self>,
                e: ArrowError,
            ) -> Poll<Option<Result<T, ArrowError>>> {
                self.as_mut().finished = true;
                return Poll::Ready(Some(Err(e)));
            }
        }

        impl RecordBatchStream for SelectStream {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }
    }

    /// Downloads missing data files for the current partition. Will do the downloads sequentially
    /// to avoid monopolizing the queue of selects that might follow.
    ///
    /// Can take awhile, use the passed cancellation token to stop the worker before it finishes.
    /// Designed to run in the background.
    pub async fn warmup_select_worker(&self) {
        if self.config_obj.select_workers().len() == 0 {
            log::error!("No select workers specified");
            return;
        }
        if !self.config_obj.select_workers().contains(&self.server_name) {
            log::error!("Current node is not a select worker");
            return;
        }
        if !self.config_obj.enable_startup_warmup() {
            log::info!("Startup warmup disabled");
            return;
        }

        log::debug!("Requesting partitions for startup warmup");
        let partitions = match self.meta_store.get_warmup_partitions().await {
            Ok(p) => p,
            Err(e) => {
                log::error!("Failed to get warmup partitions: {}", e);
                return;
            }
        };
        log::debug!("Got {} partitions, running the warmup", partitions.len());

        for (p, chunks) in partitions {
            if self.node_name_by_partitions(&[p.partition_id]) != self.server_name {
                continue;
            }
            if let Some(file) = partition_file_name(p.parent_partition_id, p.partition_id) {
                if self.stop_token.is_cancelled() {
                    log::debug!("Startup warmup cancelled");
                    return;
                }
                // TODO: propagate 'not found' and log in debug mode. Compaction might remove files,
                //       so they are not errors most of the time.
                ack_error!(self.remote_fs.download_file(&file).await);
            }
            for c in chunks {
                if self.stop_token.is_cancelled() {
                    log::debug!("Startup warmup cancelled");
                    return;
                }
                ack_error!(self.remote_fs.download_file(&chunk_file_name(c)).await);
            }
        }
        log::debug!("Startup warmup finished");
        return;
    }
}

struct LoopbackConnection {
    stream: Box<dyn MessageStream>,
}

#[async_trait]
impl WorkerConnection for LoopbackConnection {
    async fn maybe_send(&mut self, _: NetworkMessage) -> Result<bool, CubeError> {
        panic!("loopback used to send messages");
    }

    async fn maybe_receive(&mut self) -> Result<Option<NetworkMessage>, CubeError> {
        Ok(Some(self.stream.next().await.0))
    }
}

pub struct ClusterMetaStoreClient {
    meta_store_transport: Arc<dyn MetaStoreTransport>,
}

impl ClusterMetaStoreClient {
    pub fn new(meta_store_transport: Arc<dyn MetaStoreTransport>) -> Arc<Self> {
        Arc::new(Self {
            meta_store_transport,
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
        let message = self.meta_store_transport.meta_store_call(m).await?;
        Ok(match message {
            NetworkMessage::MetaStoreCallResult(res) => res,
            x => panic!("Unexpected message: {:?}", x),
        })
    }
}

pub struct QueryStream {
    schema: Option<Result<SchemaRef, CubeError>>,
    reversed_results: Vec<SerializedRecordBatchStream>,
}

impl QueryStream {
    pub fn new_error(e: CubeError) -> QueryStream {
        QueryStream {
            schema: Some(Err(e)),
            reversed_results: Vec::new(),
        }
    }

    pub fn new(schema: SchemaRef, mut results: Vec<SerializedRecordBatchStream>) -> QueryStream {
        // Reverse as we return items in reverse order later.
        results.reverse();
        QueryStream {
            schema: Some(Ok(schema)),
            reversed_results: results,
        }
    }
}

#[async_trait]
impl MessageStream for QueryStream {
    async fn next(&mut self) -> (NetworkMessage, bool) {
        if let Some(s) = self.schema.take() {
            let finished = s.is_err();
            return (NetworkMessage::SelectResultSchema(s), finished);
        }
        let batch = self.reversed_results.pop();
        let finished = batch.is_none();
        (NetworkMessage::SelectResultBatch(Ok(batch)), finished)
    }
}
