pub mod message;

pub mod transport;
#[cfg(not(target_os = "windows"))]
pub mod worker_pool;

#[cfg(not(target_os = "windows"))]
pub mod worker_services;

pub mod rate_limiter;

pub mod ingestion;

#[cfg(not(target_os = "windows"))]
use crate::cluster::worker_pool::{worker_main, WorkerPool};
#[cfg(not(target_os = "windows"))]
use crate::cluster::worker_services::{
    Callable, Configurator, DefaultServicesServerProcessor, DefaultServicesTransport,
    WorkerProcessing,
};

use crate::ack_error;
use crate::cluster::message::NetworkMessage;
use crate::cluster::rate_limiter::{ProcessRateLimiter, TaskType, TraceIndex};
use crate::cluster::transport::{ClusterTransport, MetaStoreTransport, WorkerConnection};
use crate::config::injection::{DIService, Injector};
use crate::config::is_router;
#[allow(unused_imports)]
use crate::config::{Config, ConfigObj};
use crate::metastore::chunks::chunk_file_name;
use crate::metastore::job::{Job, JobStatus, JobType};
use crate::metastore::{
    deactivate_table_on_corrupt_data, Chunk, IdRow, MetaStore, MetaStoreEvent, Partition, RowKey,
    TableId,
};
use crate::metastore::{
    MetaStoreRpcClientTransport, MetaStoreRpcMethodCall, MetaStoreRpcMethodResult,
    MetaStoreRpcServer,
};
use crate::queryplanner::query_executor::{QueryExecutor, SerializedRecordBatchStream};
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::remotefs::RemoteFs;
use crate::store::ChunkDataStore;
use crate::telemetry::tracing::{TraceIdAndSpanId, TracingHelper};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::cube_ext;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use flatbuffers::bitflags::_core::pin::Pin;
use futures::future::join_all;
use futures::future::BoxFuture;
use futures::task::{Context, Poll};
use futures::{Future, Stream};
use futures_timer::Delay;
use ingestion::job_processor::JobProcessor;
use ingestion::job_runner::JobRunner;
use itertools::Itertools;
use log::{debug, error, info, warn};
use mockall::automock;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId};
use opentelemetry::Context as OtelContext;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Weak;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::SystemTime;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{oneshot, watch, Notify, RwLock};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[automock]
#[async_trait]
pub trait Cluster: DIService + Send + Sync {
    async fn notify_job_runner(&self, node_name: String) -> Result<(), CubeError>;

    fn config(&self) -> Arc<dyn ConfigObj>;

    /// Send full select to a worker, which will act as the main node for the query.
    async fn route_select(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<(SchemaRef, Vec<SerializedRecordBatchStream>), CubeError>;

    /// Runs select on a single worker node to get partial results from that worker.
    async fn run_select(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<Vec<RecordBatch>, CubeError>;

    /// Runs explain analyze on a single worker node to get pretty printed physical plan
    /// from that worker.
    async fn run_explain_analyze(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<String, CubeError>;

    /// Like [run_select], but streams results as they are requested.
    /// This allows to send only a limited number of results, if the caller does not need all.
    async fn run_select_stream(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<SendableRecordBatchStream, CubeError>;

    async fn available_nodes(&self) -> Result<Vec<String>, CubeError>;

    fn server_name(&self) -> &str;

    async fn warmup_download(
        &self,
        node_name: &str,
        remote_path: String,
        expected_file_size: Option<u64>,
    ) -> Result<(), CubeError>;

    async fn warmup_partition(
        &self,
        partition: IdRow<Partition>,
        chunks: Vec<IdRow<Chunk>>,
    ) -> Result<(), CubeError>;

    async fn add_memory_chunk(
        &self,
        node_name: &str,
        chunk_name: String,
        batch: RecordBatch,
    ) -> Result<(), CubeError>;

    async fn free_memory_chunk(&self, node_name: &str, chunk_name: String)
        -> Result<(), CubeError>;
    async fn free_deleted_memory_chunks(
        &self,
        node_name: &str,
        chunk_names: Vec<String>,
    ) -> Result<(), CubeError>;

    fn job_result_listener(&self) -> JobResultListener;

    fn node_name_by_partition(&self, p: &IdRow<Partition>) -> String;

    async fn node_name_for_chunk_repartition(
        &self,
        chunk: &IdRow<Chunk>,
    ) -> Result<String, CubeError>;

    async fn node_name_for_import(
        &self,
        table_id: u64,
        location: &str,
    ) -> Result<String, CubeError>;

    async fn process_message_on_worker(&self, m: NetworkMessage) -> NetworkMessage;

    async fn process_metastore_message(&self, m: NetworkMessage) -> NetworkMessage;

    async fn schedule_repartition(&self, p: &IdRow<Partition>) -> Result<(), CubeError>;
}

crate::di_service!(MockCluster, [Cluster]);

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum JobEvent {
    Started(RowKey, JobType),
    Success(RowKey, JobType),
    Orphaned(RowKey, JobType),
    Error(RowKey, JobType, String),
}

pub struct ClusterImpl {
    this: Weak<ClusterImpl>,
    // Used in order to avoid cycle dependencies.
    // Convention is every service can reference cluster but cluster shouldn't reference services.
    // TODO revisit cycle dependencies: try to extract cluster transport separately?
    // Weak to avoid cycle reference counting and memory leaks
    injector: Weak<Injector>,
    remote_fs: Arc<dyn RemoteFs>,
    meta_store: Arc<dyn MetaStore>,
    cluster_transport: Arc<dyn ClusterTransport>,
    connect_timeout: Duration,
    server_name: String,
    server_addresses: Vec<String>,
    job_notify: Arc<Notify>,
    long_running_job_notify: Arc<Notify>,
    meta_store_sender: Sender<MetaStoreEvent>,
    #[cfg(not(target_os = "windows"))]
    select_process_pool: RwLock<
        Option<Arc<WorkerPool<WorkerConfigurator, WorkerProcessor, WorkerServicesTransport>>>,
    >,
    config_obj: Arc<dyn ConfigObj>,
    query_executor: Arc<dyn QueryExecutor>,
    stop_token: CancellationToken,
    close_worker_socket_tx: watch::Sender<bool>,
    close_worker_socket_rx: RwLock<watch::Receiver<bool>>,
    tracing_helper: Arc<dyn TracingHelper>,
    process_rate_limiter: Arc<dyn ProcessRateLimiter>,
}

crate::di_service!(ClusterImpl, [Cluster]);

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Select(
        SerializedPlan,
        HashMap<String, String>,
        HashMap<u64, Vec<SerializedRecordBatchStream>>,
        Option<TraceIdAndSpanId>,
    ),
}

#[cfg(not(target_os = "windows"))]
pub struct WorkerConfigurator;

#[cfg(not(target_os = "windows"))]
#[async_trait]
impl Configurator for WorkerConfigurator {
    type Config = Config;
    type ServicesRequest = ();
    type ServicesResponse = ();

    fn setup(runtime: &Runtime) {
        let startup = SELECT_WORKER_SETUP.read().unwrap();
        if startup.is_some() {
            startup.as_ref().unwrap()(runtime);
        }
    }

    async fn configure(
        _services_client: Arc<
            dyn Callable<Request = Self::ServicesRequest, Response = Self::ServicesResponse>,
        >,
    ) -> Result<Self::Config, CubeError> {
        let custom_fn = SELECT_WORKER_CONFIGURE_FN.read().unwrap().clone();
        let config = if let Some(func) = custom_fn.as_ref() {
            func().await
        } else {
            let config = Config::default();
            config.configure_injector().await;
            config
        };
        Ok(config)
    }

    fn teardown() {
        let teardown = SELECT_WORKER_SHUTDOWN.read().unwrap();
        if teardown.is_some() {
            teardown.as_ref().unwrap()();
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub struct WorkerProcessor;

#[cfg(not(target_os = "windows"))]
#[async_trait]
impl WorkerProcessing for WorkerProcessor {
    type Request = WorkerMessage;
    type Response = (SchemaRef, Vec<SerializedRecordBatchStream>, usize);
    type Config = Config;

    fn spawn_background_processes(config: Self::Config) -> Result<(), CubeError> {
        let custom_fn = SELECT_WORKER_SPAWN_BACKGROUND_FN.read().unwrap();
        if let Some(func) = custom_fn.as_ref() {
            func(config);
        }
        Ok(())
    }

    fn is_single_job_process() -> bool {
        false
    }

    async fn process(
        config: &Self::Config,
        args: WorkerMessage,
    ) -> Result<(SchemaRef, Vec<SerializedRecordBatchStream>, usize), CubeError> {
        let services = config.worker_services().await;
        match args {
            WorkerMessage::Select(
                plan_node,
                remote_to_local_names,
                chunk_id_to_record_batches,
                trace_id_and_span_id,
            ) => {
                let future = async move {
                    let time = SystemTime::now();
                    debug!("Running select in worker started");
                    let plan_node_to_send = plan_node.clone();
                    let result = tracing::trace_span!("Deserialize in_memory chunks").in_scope(
                        move || {
                            chunk_id_to_record_batches
                                .into_iter()
                                .map(|(id, batches)| -> Result<_, CubeError> {
                                    Ok((
                                        id,
                                        batches
                                            .into_iter()
                                            .map(|b| b.read())
                                            .collect::<Result<Vec<_>, _>>()?,
                                    ))
                                })
                                .collect::<Result<HashMap<_, _>, _>>()
                        },
                    )?;
                    let res = services
                        .query_executor
                        .clone()
                        .execute_worker_plan(plan_node_to_send, remote_to_local_names, result)
                        .await;
                    debug!(
                        "Running select in worker completed ({:?})",
                        time.elapsed().unwrap()
                    );
                    let (schema, records, data_loaded_size) = res?;
                    let records = SerializedRecordBatchStream::write(schema.as_ref(), records)?;
                    Ok((schema, records, data_loaded_size))
                };

                let span = trace_id_and_span_id.map(|(t, s)| {
                    let trace_id = TraceId::from(t);
                    let span_id = SpanId::from(s);
                    let span_context = SpanContext::new(
                        trace_id,
                        span_id,
                        TraceFlags::SAMPLED,
                        true,
                        Default::default(),
                    );

                    let context = OtelContext::new().with_remote_span_context(span_context);
                    let span = tracing::info_span!("Process on select worker");

                    span.set_parent(context);
                    span
                });

                if let Some(span) = span {
                    future.instrument(span).await
                } else {
                    future.await
                }
            }
        }
    }

    fn process_titile() -> String {
        std::env::var("CUBESTORE_SELECT_WORKER_TITLE")
            .ok()
            .unwrap_or("--sel-worker".to_string())
    }
}

#[cfg(not(target_os = "windows"))]
type WorkerServicesTransport = DefaultServicesTransport<DefaultServicesServerProcessor>;

#[cfg(not(target_os = "windows"))]
#[ctor::ctor]
fn proc_handler() {
    crate::util::respawn::register_handler(
        worker_main::<WorkerConfigurator, WorkerProcessor, WorkerServicesTransport>,
    );
}

lazy_static! {
    static ref SELECT_WORKER_CONFIGURE_FN: std::sync::RwLock<Option<Arc<dyn Fn() -> BoxFuture<'static, Config> + Send + Sync>>> =
        std::sync::RwLock::new(None);
}

lazy_static! {
    static ref SELECT_WORKER_SPAWN_BACKGROUND_FN: std::sync::RwLock<Option<Box<dyn Fn(Config) + Send + Sync>>> =
        std::sync::RwLock::new(None);
}

lazy_static! {
    static ref SELECT_WORKER_SETUP: std::sync::RwLock<Option<Box<dyn Fn(&Runtime) + Send + Sync>>> =
        std::sync::RwLock::new(None);
}

lazy_static! {
    static ref SELECT_WORKER_SHUTDOWN: std::sync::RwLock<Option<Box<dyn Fn() + Send + Sync>>> =
        std::sync::RwLock::new(None);
}

pub fn register_select_worker_setup(f: fn(&Runtime)) {
    let mut setup = SELECT_WORKER_SETUP.write().unwrap();
    assert!(setup.is_none(), "select worker setup already registered");
    *setup = Some(Box::new(f));
}

pub fn register_select_worker_teardown(f: fn()) {
    let mut setup = SELECT_WORKER_SHUTDOWN.write().unwrap();
    assert!(setup.is_none(), "select worker teardown already registered");
    *setup = Some(Box::new(f));
}

pub fn register_select_worker_configure_fn(f: fn() -> BoxFuture<'static, Config>) {
    let mut func = SELECT_WORKER_CONFIGURE_FN.write().unwrap();
    assert!(
        func.is_none(),
        "select worker configure function already registered"
    );
    *func = Some(Arc::new(f));
}

pub fn register_select_worker_spawn_background_fn(f: fn(Config)) {
    let mut func = SELECT_WORKER_SPAWN_BACKGROUND_FN.write().unwrap();
    assert!(
        func.is_none(),
        "select worker spawn background function already registered"
    );
    *func = Some(Box::new(f));
}

lazy_static! {
    static ref HEART_BEAT_NODE_REGEX: Regex =
        Regex::new(r"^node-heart-beats/(?P<node>.*)$").unwrap();
}

#[async_trait]
impl Cluster for ClusterImpl {
    async fn notify_job_runner(&self, node_name: String) -> Result<(), CubeError> {
        if self.server_name == node_name || is_self_reference(&node_name) {
            // TODO `notify_one()` was replaced by `notify_waiters()` here. Revisit in case of delays in job processing.
            self.job_notify.notify_one();
            self.long_running_job_notify.notify_one();
        } else {
            self.send_to_worker(&node_name, NetworkMessage::NotifyJobListeners)
                .await?;
        }
        Ok(())
    }

    fn config(&self) -> Arc<dyn ConfigObj> {
        self.config_obj.clone()
    }

    async fn route_select(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<(SchemaRef, Vec<SerializedRecordBatchStream>), CubeError> {
        let response = self
            .send_or_process_locally(&node_name, NetworkMessage::RouterSelect(plan))
            .await?;
        match response {
            NetworkMessage::SelectResult(r) => r,
            _ => panic!("unexpected response for route select"),
        }
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

    async fn run_explain_analyze(
        &self,
        node_name: &str,
        plan: SerializedPlan,
    ) -> Result<String, CubeError> {
        let response = self
            .send_or_process_locally(node_name, NetworkMessage::ExplainAnalyze(plan))
            .await?;
        match response {
            NetworkMessage::ExplainAnalyzeResult(r) => r,
            _ => panic!("unexpected result for explain analize"),
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

    async fn warmup_download(
        &self,
        node_name: &str,
        remote_path: String,
        expected_file_size: Option<u64>,
    ) -> Result<(), CubeError> {
        // We only wait for the result is to ensure our request is delivered.
        let response = self
            .send_or_process_locally(
                node_name,
                NetworkMessage::WarmupDownload(remote_path, expected_file_size),
            )
            .await?;
        match response {
            NetworkMessage::WarmupDownloadResult(r) => r,
            _ => panic!("unexpected result for warmup download"),
        }
    }

    async fn add_memory_chunk(
        &self,
        node_name: &str,
        chunk_name: String,
        batch: RecordBatch,
    ) -> Result<(), CubeError> {
        let record_batch = SerializedRecordBatchStream::write(&batch.schema(), vec![batch])?;
        let response = self
            .send_or_process_locally(
                node_name,
                NetworkMessage::AddMemoryChunk {
                    chunk_name,
                    data: record_batch.into_iter().next().unwrap(),
                },
            )
            .await?;
        match response {
            NetworkMessage::AddMemoryChunkResult(r) => r,
            x => panic!("Unexpected result for add chunk: {:?}", x),
        }
    }

    async fn free_memory_chunk(
        &self,
        node_name: &str,
        chunk_name: String,
    ) -> Result<(), CubeError> {
        let response = self
            .send_or_process_locally(node_name, NetworkMessage::FreeMemoryChunk { chunk_name })
            .await?;
        match response {
            NetworkMessage::FreeMemoryChunkResult(r) => r,
            x => panic!("Unexpected result for add chunk: {:?}", x),
        }
    }

    async fn free_deleted_memory_chunks(
        &self,
        node_name: &str,
        chunk_names: Vec<String>,
    ) -> Result<(), CubeError> {
        let response = self
            .send_or_process_locally(
                node_name,
                NetworkMessage::FreeDeletedMemoryChunks(chunk_names),
            )
            .await?;
        match response {
            NetworkMessage::FreeDeletedMemoryChunksResult(r) => r,
            x => panic!("Unexpected result for add chunk: {:?}", x),
        }
    }

    fn job_result_listener(&self) -> JobResultListener {
        JobResultListener {
            receiver: self.meta_store_sender.subscribe(),
        }
    }

    fn node_name_by_partition(&self, p: &IdRow<Partition>) -> String {
        node_name_by_partition(self.config_obj.as_ref(), p)
    }

    async fn node_name_for_chunk_repartition(
        &self,
        chunk: &IdRow<Chunk>,
    ) -> Result<String, CubeError> {
        if chunk.get_row().in_memory() {
            Ok(self.node_name_by_partition(
                &self
                    .meta_store
                    .get_partition(chunk.get_row().get_partition_id())
                    .await?,
            ))
        } else {
            Ok(pick_worker_by_ids(self.config_obj.as_ref(), [chunk.get_id()]).to_string())
        }
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
        let node_name = self.node_name_by_partition(&partition);
        let mut futures = Vec::new();
        if let Some(name) = partition.get_row().get_full_name(partition.get_id()) {
            futures.push(self.warmup_download_with_corruption_check(
                &node_name,
                name,
                partition.get_row().file_size(),
                &partition,
                None,
            ));
        }
        for chunk in chunks.iter() {
            let name = chunk.get_row().get_full_name(chunk.get_id());
            futures.push(self.warmup_download_with_corruption_check(
                &node_name,
                name,
                chunk.get_row().file_size(),
                &partition,
                Some(chunk.get_id()),
            ));
        }
        let res = join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>();

        res?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self, m))]
    async fn process_message_on_worker(&self, m: NetworkMessage) -> NetworkMessage {
        match m {
            NetworkMessage::RouterSelect(plan) => {
                let res = self
                    .query_executor
                    .execute_router_plan(plan, self.this.upgrade().unwrap())
                    .await
                    .and_then(|(schema, records)| {
                        let records = SerializedRecordBatchStream::write(&schema, records)?;
                        Ok((schema, records))
                    });
                NetworkMessage::SelectResult(res)
            }
            NetworkMessage::Select(plan) => {
                let res = self.run_local_select_worker(plan).await;
                NetworkMessage::SelectResult(res)
            }
            NetworkMessage::ExplainAnalyze(plan) => {
                let res = self.run_local_explain_analyze_worker(plan).await;
                NetworkMessage::ExplainAnalyzeResult(res)
            }
            NetworkMessage::WarmupDownload(remote_path, expected_file_size) => {
                let res = self
                    .remote_fs
                    .download_file(remote_path, expected_file_size)
                    .await;
                NetworkMessage::WarmupDownloadResult(res.map(|_| ()))
            }
            NetworkMessage::SelectResult(_)
            | NetworkMessage::WarmupDownloadResult(_)
            | NetworkMessage::ExplainAnalyzeResult(_) => {
                panic!("result sent to worker");
            }
            NetworkMessage::AddMemoryChunk { chunk_name, data } => {
                let res = match data.read() {
                    Ok(batch) => {
                        let chunk_store = self
                            .injector
                            .upgrade()
                            .unwrap()
                            .get_service_typed::<dyn ChunkDataStore>()
                            .await;
                        chunk_store.add_memory_chunk(chunk_name, batch).await
                    }
                    Err(e) => Err(e),
                };
                NetworkMessage::AddMemoryChunkResult(res)
            }
            NetworkMessage::AddMemoryChunkResult(_) => {
                panic!("AddChunkResult sent to worker");
            }
            NetworkMessage::FreeMemoryChunk { chunk_name } => {
                let chunk_store = self
                    .injector
                    .upgrade()
                    .unwrap()
                    .get_service_typed::<dyn ChunkDataStore>()
                    .await;
                let res = chunk_store.free_memory_chunk(chunk_name).await;
                NetworkMessage::FreeMemoryChunkResult(res)
            }
            NetworkMessage::FreeDeletedMemoryChunks(names) => {
                let chunk_store = self
                    .injector
                    .upgrade()
                    .unwrap()
                    .get_service_typed::<dyn ChunkDataStore>()
                    .await;
                let res = chunk_store.free_deleted_memory_chunks(names).await;
                NetworkMessage::FreeDeletedMemoryChunksResult(res)
            }
            NetworkMessage::FreeMemoryChunkResult(_) => {
                panic!("FreeMemoryChunkResult sent to worker");
            }
            NetworkMessage::FreeDeletedMemoryChunksResult(_) => {
                panic!("FreeDeletedMemoryChunksResult sent to worker");
            }
            NetworkMessage::MetaStoreCall(_) | NetworkMessage::MetaStoreCallResult(_) => {
                panic!("MetaStoreCall sent to worker");
            }
            NetworkMessage::NotifyJobListeners => {
                // TODO `notify_one()` was replaced by `notify_waiters()` here. Revisit in case of delays in job processing.
                self.job_notify.notify_one();
                self.long_running_job_notify.notify_one();
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

    async fn schedule_repartition(&self, p: &IdRow<Partition>) -> Result<(), CubeError> {
        let in_memory_node = self.node_name_by_partition(p);
        let in_memory_job = self
            .meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::Partitions, p.get_id()),
                JobType::Repartition,
                in_memory_node.to_string(),
            ))
            .await?;
        if in_memory_job.is_some() {
            self.notify_job_runner(in_memory_node).await?;
        }

        let chunks = self
            .meta_store
            .get_chunks_by_partition(p.get_id(), false)
            .await?
            .into_iter()
            .filter(|c| !c.get_row().in_memory())
            .collect::<Vec<_>>();

        for chunk in chunks {
            let node = self.node_name_for_chunk_repartition(&chunk).await?;

            let job = self
                .meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Chunks, chunk.get_id()),
                    JobType::RepartitionChunk,
                    node.to_string(),
                ))
                .await?;
            if job.is_some() {
                self.notify_job_runner(node).await?;
            }
        }
        Ok(())
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
                        JobStatus::Orphaned => Some(JobEvent::Orphaned(
                            new.get_row().row_reference().clone(),
                            new.get_row().job_type().clone(),
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

impl ClusterImpl {
    pub fn new(
        server_name: String,
        server_addresses: Vec<String>,
        injector: Weak<Injector>,
        remote_fs: Arc<dyn RemoteFs>,
        connect_timeout: Duration,
        meta_store: Arc<dyn MetaStore>,
        config_obj: Arc<dyn ConfigObj>,
        query_executor: Arc<dyn QueryExecutor>,
        meta_store_sender: Sender<MetaStoreEvent>,
        cluster_transport: Arc<dyn ClusterTransport>,
        tracing_helper: Arc<dyn TracingHelper>,
        process_rate_limiter: Arc<dyn ProcessRateLimiter>,
    ) -> Arc<ClusterImpl> {
        let (close_worker_socket_tx, close_worker_socket_rx) = watch::channel(false);
        Arc::new_cyclic(|this| ClusterImpl {
            this: this.clone(),
            injector,
            server_name,
            server_addresses,
            remote_fs,
            connect_timeout,
            meta_store,
            cluster_transport,
            job_notify: Arc::new(Notify::new()),
            long_running_job_notify: Arc::new(Notify::new()),
            meta_store_sender,
            #[cfg(not(target_os = "windows"))]
            select_process_pool: RwLock::new(None),
            config_obj,
            query_executor,
            stop_token: CancellationToken::new(),
            close_worker_socket_tx,
            close_worker_socket_rx: RwLock::new(close_worker_socket_rx),
            tracing_helper,
            process_rate_limiter,
        })
    }

    pub fn is_select_worker(&self) -> bool {
        !is_router(self.config_obj.as_ref())
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
        if (self.config_obj.select_workers().is_empty()
            || self.config_obj.worker_bind_address().is_some())
            && self.config_obj.select_worker_pool_size() > 0
        {
            let mut pool = self.select_process_pool.write().await;
            let arc = Arc::new(WorkerPool::new(
                DefaultServicesServerProcessor::new(),
                self.config_obj.select_worker_pool_size(),
                Duration::from_secs(self.config_obj.query_timeout()),
                Duration::from_secs(self.config_obj.select_worker_idle_timeout()),
                "sel",
                vec![(
                    "_CUBESTORE_SUBPROCESS_TYPE".to_string(),
                    "SELECT_WORKER".to_string(),
                )],
            ));
            *pool = Some(arc.clone());
            futures.push(cube_ext::spawn(
                async move { arc.wait_processing_loops().await },
            ));
        }
        let job_processor = {
            let job_processor = self
                .injector
                .upgrade()
                .unwrap()
                .get_service_typed::<dyn JobProcessor>()
                .await; //TODO - timeout
            let job_processor_to_move = job_processor.clone();
            futures.push(cube_ext::spawn(async move {
                job_processor_to_move.wait_processing_loops().await
            }));
            job_processor
        };

        for i in
            0..self.config_obj.job_runners_count() + self.config_obj.long_term_job_runners_count()
        {
            // TODO number of job event loops
            let is_long_running = i >= self.config_obj.job_runners_count();
            let job_runner = JobRunner {
                config_obj: self.config_obj.clone(),
                meta_store: self.meta_store.clone(),
                chunk_store: self.injector.upgrade().unwrap().get_service_typed().await,
                compaction_service: self.injector.upgrade().unwrap().get_service_typed().await,
                import_service: self.injector.upgrade().unwrap().get_service_typed().await,
                process_rate_limiter: self.process_rate_limiter.clone(),
                server_name: self.server_name.clone(),
                notify: if is_long_running {
                    self.long_running_job_notify.clone()
                } else {
                    self.job_notify.clone()
                },
                stop_token: self.stop_token.clone(),
                is_long_term: is_long_running,
                job_processor: job_processor.clone(),
            };
            futures.push(cube_ext::spawn(async move {
                job_runner.processing_loop().await;
            }));
        }
        let process_rate_limiter = self.process_rate_limiter.clone();
        futures.extend(process_rate_limiter.spawn_processing_loop().await);

        let stop_token = self.stop_token.clone();
        let long_running_job_notify = self.long_running_job_notify.clone();
        let job_notify = self.job_notify.clone();

        futures.push(cube_ext::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_token.cancelled() => {
                        return;
                    }
                    _ = Delay::new(Duration::from_secs(5)) => {
                        job_notify.notify_one();
                        long_running_job_notify.notify_one();
                    }
                };
            }
        }));

        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.stop_token.cancel();

        #[cfg(not(target_os = "windows"))]
        if let Some(pool) = self.select_process_pool.read().await.as_ref() {
            pool.stop_workers().await?;
        }

        self.process_rate_limiter.stop_processing_loops();

        self.close_worker_socket_tx.send(true)?;
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
        let listener = TcpListener::bind(address).await?;
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

            cube_ext::spawn(async move {
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

    async fn run_local_select_worker(
        &self,
        plan_node: SerializedPlan,
    ) -> Result<(SchemaRef, Vec<SerializedRecordBatchStream>), CubeError> {
        let wait_ms = self
            .process_rate_limiter
            .wait_for_allow(
                TaskType::Select,
                Some(Duration::from_secs(self.config_obj.query_timeout())),
            )
            .await?;
        let trace_index = TraceIndex {
            table_id: None,
            trace_obj: plan_node.trace_obj(),
        };
        let res = self.run_local_select_worker_impl(plan_node).await;
        match res {
            Ok((schema, records, data_loaded_size)) => {
                self.process_rate_limiter
                    .commit_task_usage(
                        TaskType::Select,
                        data_loaded_size as i64,
                        wait_ms,
                        trace_index,
                    )
                    .await;
                Ok((schema, records))
            }
            Err(e) => {
                self.process_rate_limiter
                    .commit_task_usage(TaskType::Select, 0, wait_ms, trace_index)
                    .await;
                Err(e)
            }
        }
    }
    #[instrument(level = "trace", skip(self, plan_node))]
    async fn run_local_select_worker_impl(
        &self,
        plan_node: SerializedPlan,
    ) -> Result<(SchemaRef, Vec<SerializedRecordBatchStream>, usize), CubeError> {
        let start = SystemTime::now();
        debug!("Running select");
        let remote_to_local_names = self.warmup_select_worker_files(&plan_node).await?;
        let warmup = start.elapsed()?;
        if warmup.as_millis() > 200 {
            warn!("Warmup download for select ({:?})", warmup);
        }

        let chunk_store = self
            .injector
            .upgrade()
            .unwrap()
            .get_service_typed::<dyn ChunkDataStore>()
            .await;

        let in_memory_chunks_to_load = plan_node.in_memory_chunks_to_load();
        let in_memory_chunks_futures = in_memory_chunks_to_load
            .iter()
            .map(|(c, p, i)| {
                chunk_store
                    .get_chunk_columns_with_preloaded_meta(c.clone(), p.clone(), i.clone())
                    .instrument(tracing::span!(
                        tracing::Level::TRACE,
                        "get in memory chunk columns",
                        row_count = c.get_row().get_row_count()
                    ))
            })
            .collect::<Vec<_>>();

        let chunk_id_to_record_batches = in_memory_chunks_to_load
            .clone()
            .into_iter()
            .map(|(c, _, _)| c.get_id())
            .zip(
                join_all(in_memory_chunks_futures)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter(),
            )
            .collect::<HashMap<_, _>>();

        let mut res = None;
        #[cfg(not(target_os = "windows"))]
        {
            if let Some(pool) = self
                .select_process_pool
                .read()
                .instrument(tracing::span!(
                    tracing::Level::TRACE,
                    "awaiting process_pool lock"
                ))
                .await
                .clone()
            {
                let span = tracing::span!(
                    tracing::Level::TRACE,
                    "Serialize chunks into SerializedRecordBatchStream"
                );
                let chunk_id_to_record_batches = span.in_scope(|| {
                    chunk_id_to_record_batches
                    .iter()
                    .map(
                        |(id, b)| -> Result<(u64, Vec<SerializedRecordBatchStream>), CubeError> {
                            Ok((
                                *id,
                                SerializedRecordBatchStream::write(
                                    &b.iter().next().unwrap().schema(),
                                    b.to_vec(),
                                )?,
                            ))
                        },
                    )
                    .collect::<Result<HashMap<_, _>, _>>()
                })?;
                res = Some(
                    pool.process(WorkerMessage::Select(
                        plan_node.clone(),
                        remote_to_local_names.clone(),
                        chunk_id_to_record_batches,
                        self.tracing_helper.trace_and_span_id(),
                    ))
                    .instrument(tracing::span!(
                        tracing::Level::TRACE,
                        "execute_worker_plan_on_pool"
                    ))
                    .await,
                )
            }
        }

        if res.is_none() {
            // TODO optimize for no double conversion
            let (schema, records, data_loaded_size) = self
                .query_executor
                .execute_worker_plan(
                    plan_node.clone(),
                    remote_to_local_names,
                    chunk_id_to_record_batches,
                )
                .await?;
            let records = SerializedRecordBatchStream::write(schema.as_ref(), records);
            res = Some(Ok((schema, records?, data_loaded_size)))
        }

        info!("Running select completed ({:?})", start.elapsed()?);
        res.unwrap()
    }

    async fn run_local_explain_analyze_worker(
        &self,
        plan_node: SerializedPlan,
    ) -> Result<String, CubeError> {
        let remote_to_local_names = self.warmup_select_worker_files(&plan_node).await?;
        let in_memory_chunks_to_load = plan_node.in_memory_chunks_to_load();
        let chunk_id_to_record_batches = in_memory_chunks_to_load
            .clone()
            .into_iter()
            .map(|(c, _, _)| (c.get_id(), Vec::new()))
            .collect();

        let res = self
            .query_executor
            .pp_worker_plan(plan_node, remote_to_local_names, chunk_id_to_record_batches)
            .await;

        res
    }

    async fn warmup_select_worker_files(
        &self,
        plan_node: &SerializedPlan,
    ) -> Result<HashMap<String, String>, CubeError> {
        let to_download = plan_node.files_to_download();
        let file_futures = to_download
            .iter()
            .map(|(partition, remote, file_size, chunk_id)| {
                let meta_store = self.meta_store.clone();
                async move {
                    let res = self
                        .remote_fs
                        .download_file(remote.clone(), file_size.clone())
                        .await;
                    deactivate_table_on_corrupt_data(
                        meta_store,
                        &res,
                        &partition,
                        chunk_id.clone(),
                    )
                    .await;
                    res
                }
            })
            .collect::<Vec<_>>();

        let remote_to_local_names = to_download
            .clone()
            .into_iter()
            .zip(
                join_all(file_futures)
                    .instrument(tracing::span!(tracing::Level::TRACE, "warmup_download"))
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter(),
            )
            .map(|((_, remote_path, _, _), path)| (remote_path, path))
            .collect::<HashMap<_, _>>();

        Ok(remote_to_local_names)
    }

    async fn warmup_download_with_corruption_check(
        &self,
        node_name: &str,
        remote_path: String,
        expected_file_size: Option<u64>,
        partition: &IdRow<Partition>,
        chunk_id: Option<u64>,
    ) -> Result<(), CubeError> {
        let res = self
            .warmup_download(&node_name, remote_path, expected_file_size)
            .await;
        deactivate_table_on_corrupt_data(self.meta_store.clone(), &res, partition, chunk_id).await;
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

    #[instrument(level = "trace", skip(self, m))]
    async fn send_or_process_locally(
        &self,
        node_name: &str,
        m: NetworkMessage,
    ) -> Result<NetworkMessage, CubeError> {
        if self.server_name == node_name || is_self_reference(node_name) {
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
        if self.server_name == node_name || is_self_reference(node_name) {
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
                let (schema, results) = match self.run_local_select_worker(p).await {
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
            if self.node_name_by_partition(&p) != self.server_name {
                continue;
            }
            if let Some(file) = p.get_row().get_full_name(p.get_id()) {
                if self.stop_token.is_cancelled() {
                    log::debug!("Startup warmup cancelled");
                    return;
                }
                // TODO: propagate 'not found' and log in debug mode. Compaction might remove files,
                //       so they are not errors most of the time.
                ack_error!(
                    self.remote_fs
                        .download_file(file, p.get_row().file_size())
                        .await
                );
            }
            for c in chunks {
                if self.stop_token.is_cancelled() {
                    log::debug!("Startup warmup cancelled");
                    return;
                }
                if c.get_row().in_memory() {
                    continue;
                }
                let result = self
                    .remote_fs
                    .download_file(
                        chunk_file_name(c.get_id(), c.get_row().suffix()),
                        c.get_row().file_size(),
                    )
                    .await;
                // TODO: propagate 'not found' and log in debug mode. Compaction might remove files,
                //       so they are not errors most of the time.
                ack_error!(result);
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

fn is_self_reference(name: &str) -> bool {
    name.starts_with("@loop:")
}

pub fn node_name_by_partition<'a>(config: &'a dyn ConfigObj, p: &IdRow<Partition>) -> String {
    if let Some(id) = p.get_row().multi_partition_id() {
        pick_worker_by_ids(config, [id]).to_string()
    } else {
        pick_worker_by_partitions(config, [p]).to_string()
    }
}
/// Picks a worker by opaque id for any distributing work in a cluster.
/// Ids usually come from multi-partitions of the metastore.
pub fn pick_worker_by_ids<'a>(
    config: &'a dyn ConfigObj,
    ids: impl IntoIterator<Item = u64>,
) -> &'a str {
    let workers = config.select_workers();
    if workers.is_empty() {
        return config.server_name().as_str();
    }

    let mut hasher = DefaultHasher::new();
    for p in ids {
        p.hash(&mut hasher);
    }
    workers[(hasher.finish() % workers.len() as u64) as usize].as_str()
}

/// Same as [pick_worker_by_ids], but uses ranges of partitions. This is a hack
/// to keep the same node for partitions produced by compaction that merged
/// chunks into the main table of a single partition.
pub fn pick_worker_by_partitions<'a>(
    config: &'a dyn ConfigObj,
    partitions: impl IntoIterator<Item = &'a IdRow<Partition>>,
) -> &'a str {
    let workers = config.select_workers();
    if workers.is_empty() {
        return config.server_name().as_str();
    }

    let mut hasher = DefaultHasher::new();
    for partition in partitions {
        partition.get_row().get_min_val().hash(&mut hasher);
        partition.get_row().get_max_val().hash(&mut hasher);
        partition.get_row().get_index_id().hash(&mut hasher);
    }
    workers[(hasher.finish() % workers.len() as u64) as usize].as_str()
}
