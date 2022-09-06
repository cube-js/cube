use crate::cluster::{pick_worker_by_ids, Cluster};
use crate::config::ConfigObj;
use crate::metastore::job::{Job, JobType};
use crate::metastore::partition::partition_file_name;
use crate::metastore::table::Table;
use crate::metastore::{
    deactivate_table_on_corrupt_data, IdRow, MetaStore, MetaStoreEvent, Partition, RowKey, TableId,
};
use crate::remotefs::RemoteFs;
use crate::store::{ChunkStore, WALStore};
use crate::util::time_span::warn_long_fut;
use crate::util::WorkerLoop;
use crate::CubeError;
use chrono::Utc;
use datafusion::cube_ext;
use flatbuffers::bitflags::_core::cmp::Ordering;
use flatbuffers::bitflags::_core::time::Duration;
use futures_timer::Delay;
use log::error;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{broadcast, Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub struct SchedulerImpl {
    meta_store: Arc<dyn MetaStore>,
    cluster: Arc<dyn Cluster>,
    remote_fs: Arc<dyn RemoteFs>,
    event_receiver: Mutex<Receiver<MetaStoreEvent>>,
    cancel_token: CancellationToken,
    gc_loop: Arc<DataGCLoop>,
    config: Arc<dyn ConfigObj>,
    reconcile_loop: WorkerLoop,
}

crate::di_service!(SchedulerImpl, []);

impl SchedulerImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        cluster: Arc<dyn Cluster>,
        remote_fs: Arc<dyn RemoteFs>,
        event_receiver: Receiver<MetaStoreEvent>,
        config: Arc<dyn ConfigObj>,
    ) -> SchedulerImpl {
        let cancel_token = CancellationToken::new();
        let gc_loop = DataGCLoop::new(
            meta_store.clone(),
            remote_fs.clone(),
            config.clone(),
            cancel_token.clone(),
        );
        SchedulerImpl {
            meta_store,
            cluster,
            remote_fs,
            event_receiver: Mutex::new(event_receiver),
            cancel_token,
            gc_loop,
            config,
            reconcile_loop: WorkerLoop::new("Reconcile"),
        }
    }

    pub fn spawn_processing_loops(
        scheduler: Arc<SchedulerImpl>,
    ) -> Vec<JoinHandle<Result<(), CubeError>>> {
        let scheduler2 = scheduler.clone();
        let scheduler3 = scheduler.clone();
        vec![
            cube_ext::spawn(async move {
                let gc_loop = scheduler.gc_loop.clone();
                gc_loop.run().await;
                Ok(())
            }),
            cube_ext::spawn(async move {
                Self::run_scheduler(scheduler2).await;
                Ok(())
            }),
            cube_ext::spawn(async move {
                scheduler3
                    .reconcile_loop
                    .process(
                        scheduler3.clone(),
                        async move |_| Ok(Delay::new(Duration::from_secs(30)).await),
                        async move |s, _| s.reconcile().await,
                    )
                    .await;
                Ok(())
            }),
        ]
    }

    async fn run_scheduler(scheduler: Arc<SchedulerImpl>) {
        loop {
            let mut event_receiver = scheduler.event_receiver.lock().await;
            let event = tokio::select! {
                _ = scheduler.cancel_token.cancelled() => {
                    return;
                }
                event = event_receiver.recv() => {
                    match event {
                        Err(broadcast::error::RecvError::Lagged(messages)) => {
                            error!("Scheduler is lagging on meta store event processing for {} messages", messages);
                            continue;
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            return;
                        },
                        Ok(event) => event,
                    }
                }
            };
            let scheduler_to_move = scheduler.clone();
            cube_ext::spawn(async move {
                let res = scheduler_to_move.process_event(event.clone()).await;
                if let Err(e) = res {
                    error!("Error processing event {:?}: {}", event, e);
                }
            });
        }
    }

    pub async fn reconcile(&self) -> Result<(), CubeError> {
        if let Err(e) = warn_long_fut(
            "Removing orphaned jobs",
            Duration::from_millis(5000),
            self.remove_orphaned_jobs(),
        )
        .await
        {
            error!("Error removing orphaned jobs: {}", e);
        }

        if let Err(e) = warn_long_fut(
            "Table import reconciliation",
            Duration::from_millis(5000),
            self.reconcile_table_imports(),
        )
        .await
        {
            error!("Error reconciling table imports: {}", e);
        };

        if let Err(e) = warn_long_fut(
            "Drop not ready tables reconciliation",
            Duration::from_millis(5000),
            self.drop_not_ready_tables(),
        )
        .await
        {
            error!("Error during dropping not ready tables: {}", e);
        };

        if let Err(e) = warn_long_fut(
            "Remove inactive chunks",
            Duration::from_millis(5000),
            self.remove_inactive_chunks(),
        )
        .await
        {
            error!("Error removing inactive chunks: {}", e);
        }

        if let Err(e) = warn_long_fut(
            "Remove inactive not uploaded chunks",
            Duration::from_millis(5000),
            self.remove_inactive_not_uploaded_chunks(),
        )
        .await
        {
            error!("Error removing inactive not uploaded chunks: {}", e);
        }

        if let Err(e) = warn_long_fut(
            "Scheduling compactions",
            Duration::from_millis(5000),
            self.schedule_all_pending_compactions(),
        )
        .await
        {
            error!("Error scheduling partitions compaction: {}", e);
        }

        if let Err(e) = warn_long_fut(
            "Scheduling repartition",
            Duration::from_millis(5000),
            self.schedule_all_pending_repartitions(),
        )
        .await
        {
            error!("Error scheduling repartition: {}", e);
        }

        if let Err(e) = warn_long_fut(
            "Delete orphaned partitions",
            Duration::from_millis(5000),
            self.delete_created_but_not_written_partitions(),
        )
        .await
        {
            error!("Error deleting orphaned partitions: {}", e);
        }

        if let Err(e) = warn_long_fut(
            "Delete middle man partitions",
            Duration::from_millis(5000),
            self.delete_middle_man_partitions(),
        )
        .await
        {
            error!("Error deleting middle man partitions: {}", e);
        }

        Ok(())
    }

    async fn schedule_all_pending_repartitions(&self) -> Result<(), CubeError> {
        let all_inactive_partitions_to_repartition = self
            .meta_store
            .all_inactive_partitions_to_repartition()
            .await?;

        for partition in all_inactive_partitions_to_repartition.iter() {
            self.schedule_repartition(&partition).await?;
        }
        Ok(())
    }

    async fn delete_created_but_not_written_partitions(&self) -> Result<(), CubeError> {
        let all_inactive_partitions = self.meta_store.all_just_created_partitions().await?;

        for partition in all_inactive_partitions.iter() {
            let deadline = Instant::now() + Duration::from_secs(self.config.import_job_timeout());
            self.gc_loop
                .send(GCTimedTask {
                    deadline,
                    task: GCTask::DeletePartition(partition.get_id()),
                })
                .await?;
        }
        Ok(())
    }

    async fn delete_middle_man_partitions(&self) -> Result<(), CubeError> {
        let all_inactive_partitions = self.meta_store.all_inactive_middle_man_partitions().await?;

        for partition in all_inactive_partitions.iter() {
            let deadline = Instant::now() + Duration::from_secs(self.config.import_job_timeout());
            self.gc_loop
                .send(GCTimedTask {
                    deadline,
                    task: GCTask::DeleteMiddleManPartition(partition.get_id()),
                })
                .await?;
        }
        Ok(())
    }

    async fn schedule_all_pending_compactions(&self) -> Result<(), CubeError> {
        let partition_compaction_candidates_id = self
            .meta_store
            // TODO config
            .get_partitions_with_chunks_created_seconds_ago(60)
            .await?;

        for p in partition_compaction_candidates_id {
            self.schedule_compaction_if_needed(&p).await?;
        }
        Ok(())
    }

    async fn remove_inactive_not_uploaded_chunks(&self) -> Result<(), CubeError> {
        let all_inactive_not_uploaded_chunks =
            self.meta_store.all_inactive_not_uploaded_chunks().await?;

        for chunk in all_inactive_not_uploaded_chunks.iter() {
            let deadline = Instant::now() + Duration::from_secs(self.config.import_job_timeout());
            self.gc_loop
                .send(GCTimedTask {
                    deadline,
                    task: GCTask::DeleteChunk(chunk.get_id()),
                })
                .await?;
        }
        Ok(())
    }

    async fn remove_inactive_chunks(&self) -> Result<(), CubeError> {
        // TODO we can do this reconciliation more rarely
        let all_inactive_chunks = self.meta_store.all_inactive_chunks().await?;

        for chunk in all_inactive_chunks.iter() {
            let deadline = Instant::now() + Duration::from_secs(self.config.not_used_timeout());
            self.gc_loop
                .send(GCTimedTask {
                    deadline,
                    task: GCTask::DeleteChunk(chunk.get_id()),
                })
                .await?;
        }
        Ok(())
    }

    async fn reconcile_table_imports(&self) -> Result<(), CubeError> {
        // Using get_tables_with_path due to it's cached
        let tables = self.meta_store.get_tables_with_path(true).await?;
        for table in tables.iter() {
            if table.table.get_row().is_ready() {
                if let Some(locations) = table.table.get_row().locations() {
                    for location in locations.iter() {
                        if Table::is_stream_location(location) {
                            let job = self
                                .meta_store
                                .get_job_by_ref(
                                    RowKey::Table(TableId::Tables, table.table.get_id()),
                                    JobType::TableImportCSV(location.to_string()),
                                )
                                .await?;
                            if job.is_none() {
                                self.schedule_table_import(table.table.get_id(), &[location])
                                    .await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn drop_not_ready_tables(&self) -> Result<(), CubeError> {
        // TODO config
        let not_ready_tables = self.meta_store.not_ready_tables(1800).await?;
        for table in not_ready_tables.into_iter() {
            self.meta_store.drop_table(table.get_id()).await?;
        }
        Ok(())
    }

    async fn remove_orphaned_jobs(&self) -> Result<(), CubeError> {
        let orphaned_jobs = self
            .meta_store
            .get_orphaned_jobs(Duration::from_secs(120)) // TODO config
            .await?;
        for job in orphaned_jobs {
            log::info!("Removing orphaned job: {:?}", job);
            self.meta_store.delete_job(job.get_id()).await?;
        }
        Ok(())
    }

    pub fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.cancel_token.cancel();
        self.reconcile_loop.stop();
        Ok(())
    }

    async fn process_event(&self, event: MetaStoreEvent) -> Result<(), CubeError> {
        if let MetaStoreEvent::Insert(TableId::WALs, row_id)
        | MetaStoreEvent::Update(TableId::WALs, row_id) = event
        {
            let wal = self.meta_store.get_wal(row_id).await?;
            if wal.get_row().uploaded() {
                self.schedule_wal_to_process(row_id).await?;
            }
        }
        if let MetaStoreEvent::Insert(TableId::Partitions, row_id)
        | MetaStoreEvent::Update(TableId::Partitions, row_id) = event
        {
            let p = self.meta_store.get_partition(row_id).await?;
            if p.get_row().is_active() && !p.get_row().is_warmed_up() {
                if let Some(path) = p.get_row().get_full_name(p.get_id()) {
                    self.schedule_partition_warmup(&p, path).await?;
                    self.meta_store.mark_partition_warmed_up(row_id).await?;
                }
            }
        }
        if let MetaStoreEvent::Insert(TableId::MultiPartitions, id)
        | MetaStoreEvent::Update(TableId::MultiPartitions, id) = event
        {
            let p = self.meta_store.get_multi_partition(id).await?;
            let active = p.get_row().active();
            // TODO should it respect table partition_split_threshold?
            if active && self.config.partition_split_threshold() < p.get_row().total_row_count() {
                self.schedule_multi_partition_split(id).await?;
            }
        }
        if let MetaStoreEvent::Insert(TableId::Chunks, row_id)
        | MetaStoreEvent::Update(TableId::Chunks, row_id) = event
        {
            let chunk = self.meta_store.get_chunk(row_id).await?;
            if chunk.get_row().uploaded() {
                let partition = self
                    .meta_store
                    .get_partition(chunk.get_row().get_partition_id())
                    .await?;
                if chunk.get_row().active() {
                    if partition.get_row().is_active() {
                        if chunk.get_row().in_memory() {
                            self.schedule_compaction_in_memory_chunks_if_needed(&partition)
                                .await
                                .unwrap();
                        }
                        self.schedule_compaction_if_needed(&partition).await?;
                    } else {
                        self.schedule_repartition(&partition).await?;
                    }
                } else {
                    let deadline =
                        Instant::now() + Duration::from_secs(self.config.not_used_timeout());
                    self.gc_loop
                        .send(GCTimedTask {
                            deadline,
                            task: GCTask::DeleteChunk(chunk.get_id()),
                        })
                        .await?;
                }
            }
        }
        if let MetaStoreEvent::Insert(TableId::Tables, row_id) = event {
            let table = self.meta_store.get_table_by_id(row_id).await?;
            if let Some(locations) = table.get_row().locations() {
                self.schedule_table_import(row_id, &locations).await?;
            }
        }
        if let MetaStoreEvent::Delete(TableId::WALs, row_id) = event {
            let file = self
                .remote_fs
                .local_file(WALStore::wal_remote_path(row_id).as_str())
                .await?;
            tokio::fs::remove_file(file).await?;
        }
        if let MetaStoreEvent::DeleteChunk(chunk) = &event {
            if chunk.get_row().in_memory() {
                let partition = self
                    .meta_store
                    .get_partition(chunk.get_row().get_partition_id())
                    .await?;
                let node_name = self.cluster.node_name_by_partition(&partition);
                self.cluster
                    .free_memory_chunk(&node_name, chunk.get_id())
                    .await?;
            } else if chunk.get_row().uploaded() {
                let file_name =
                    ChunkStore::chunk_remote_path(chunk.get_id(), chunk.get_row().suffix());
                let deadline = Instant::now()
                    + Duration::from_secs(self.config.meta_store_snapshot_interval() * 2);
                self.gc_loop
                    .send(GCTimedTask {
                        deadline,
                        task: GCTask::RemoveRemoteFile(file_name),
                    })
                    .await?;
            }
        }
        if let MetaStoreEvent::DeletePartition(partition) = &event {
            // remove file only if partition is active otherwise it should be removed when it's deactivated
            if partition.get_row().is_active() {
                if let Some(file_name) = partition.get_row().get_full_name(partition.get_id()) {
                    let deadline = Instant::now()
                        + Duration::from_secs(self.config.meta_store_snapshot_interval() * 2);
                    self.gc_loop
                        .send(GCTimedTask {
                            deadline,
                            task: GCTask::RemoveRemoteFile(file_name),
                        })
                        .await?;
                }
            }
        }
        if let MetaStoreEvent::Update(TableId::Partitions, row_id) = event {
            let partition = self.meta_store.get_partition(row_id).await?;
            if !partition.get_row().is_active() {
                self.schedule_repartition_if_needed(&partition).await?;
                if partition.get_row().main_table_row_count() > 0 {
                    let file_name =
                        partition_file_name(partition.get_id(), partition.get_row().suffix());
                    let deadline =
                        Instant::now() + Duration::from_secs(self.config.not_used_timeout());
                    self.gc_loop
                        .send(GCTimedTask {
                            deadline,
                            task: GCTask::RemoveRemoteFile(file_name),
                        })
                        .await?;
                }
            }
        }
        if let MetaStoreEvent::DeleteJob(job) = event {
            match job.get_row().job_type() {
                JobType::RepartitionChunk => match job.get_row().row_reference() {
                    RowKey::Table(TableId::Chunks, c) => {
                        let c = self.meta_store.get_chunk(*c).await?;
                        let p = self
                            .meta_store
                            .get_partition(c.get_row().get_partition_id())
                            .await?;
                        self.schedule_repartition_if_needed(&p).await?
                    }
                    _ => panic!(
                        "Unexpected row reference: {:?}",
                        job.get_row().row_reference()
                    ),
                },
                JobType::MultiPartitionSplit => match job.get_row().row_reference() {
                    RowKey::Table(TableId::MultiPartitions, m) => {
                        self.schedule_finish_multi_split_if_needed(*m).await?
                    }
                    _ => panic!(
                        "Unexpected row reference: {:?}",
                        job.get_row().row_reference()
                    ),
                },
                JobType::FinishMultiSplit => match job.get_row().row_reference() {
                    RowKey::Table(TableId::MultiPartitions, m) => {
                        self.schedule_finish_multi_split_if_needed(*m).await?;
                        for c in self.meta_store.get_child_multi_partitions(*m).await? {
                            if !c.get_row().active() && c.get_row().prepared_for_split() {
                                self.schedule_finish_multi_split_if_needed(c.get_id())
                                    .await?;
                            }
                        }
                    }
                    _ => panic!(
                        "Unexpected row reference: {:?}",
                        job.get_row().row_reference()
                    ),
                },
                _ => {}
            }
        }
        Ok(())
    }

    async fn schedule_compaction_if_needed(
        &self,
        partition: &IdRow<Partition>,
    ) -> Result<(), CubeError> {
        let partition_id = partition.get_id();
        let all_chunks = self
            .meta_store
            .get_chunks_by_partition_out_of_queue(partition_id, false)
            .await?;

        let chunk_sizes = all_chunks
            .iter()
            .map(|r| r.get_row().get_row_count())
            .sum::<u64>();

        let chunks = all_chunks
            .iter()
            .filter(|c| !c.get_row().in_memory())
            .collect::<Vec<_>>();

        let in_memory_chunks = all_chunks
            .iter()
            .filter(|c| c.get_row().in_memory())
            .collect::<Vec<_>>();
        let min_in_memory_created_at = in_memory_chunks
            .iter()
            .filter_map(|c| c.get_row().oldest_insert_at().clone())
            .min();
        let min_created_at = chunks
            .iter()
            .filter_map(|c| c.get_row().created_at().clone())
            .min();
        let check_row_counts = partition.get_row().multi_partition_id().is_none();
        if check_row_counts && chunk_sizes > self.config.compaction_chunks_total_size_threshold()
            || chunks.len() > self.config.compaction_chunks_count_threshold() as usize
            // Force compaction if in_memory chunks were created far ago
            || min_in_memory_created_at.map(|min| Utc::now().signed_duration_since(min).num_seconds() > self.config.compaction_in_memory_chunks_max_lifetime_threshold()  as i64).unwrap_or(false)
            // Force compaction if other chunks were created far ago
            || min_created_at.map(|min| Utc::now().signed_duration_since(min).num_seconds() > self.config.compaction_chunks_max_lifetime_threshold() as i64).unwrap_or(false)
        {
            self.schedule_partition_to_compact(partition).await?;
        }
        Ok(())
    }

    async fn schedule_compaction_in_memory_chunks_if_needed(
        &self,
        partition: &IdRow<Partition>,
    ) -> Result<(), CubeError> {
        let compaction_in_memory_chunks_count_threshold =
            self.config.compaction_in_memory_chunks_count_threshold();
        let compaction_in_memory_chunks_size_limit =
            self.config.compaction_in_memory_chunks_size_limit();

        let partition_id = partition.get_id();

        let chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?
            .into_iter()
            .filter(|c| {
                c.get_row().in_memory()
                    && c.get_row().active()
                    && c.get_row().get_row_count() < compaction_in_memory_chunks_size_limit
                    && c.get_row()
                        .oldest_insert_at()
                        .map(|m| {
                            Utc::now().signed_duration_since(m).num_seconds()
                                < self
                                    .config
                                    .compaction_in_memory_chunks_max_lifetime_threshold()
                                    as i64
                        })
                        .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        if chunks.len() > compaction_in_memory_chunks_count_threshold {
            let node = self.cluster.node_name_by_partition(partition);
            let job = self
                .meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, partition_id),
                    JobType::InMemoryChunksCompaction,
                    node.to_string(),
                ))
                .await?;
            if job.is_some() {
                self.cluster.notify_job_runner(node).await?;
            }
        }
        Ok(())
    }

    pub async fn schedule_repartition_if_needed(
        &self,
        p: &IdRow<Partition>,
    ) -> Result<(), CubeError> {
        let chunk_rows = self
            .meta_store
            .get_partition_chunk_sizes(p.get_id())
            .await?;
        if 0 < chunk_rows {
            self.schedule_repartition(p).await?;
        }
        Ok(())
    }

    async fn schedule_repartition(&self, p: &IdRow<Partition>) -> Result<(), CubeError> {
        self.cluster.schedule_repartition(p).await
    }

    async fn schedule_table_import(
        &self,
        table_id: u64,
        locations: &[&String],
    ) -> Result<(), CubeError> {
        for &l in locations {
            let node = self.cluster.node_name_for_import(table_id, &l).await?;
            let job = self
                .meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Tables, table_id),
                    JobType::TableImportCSV(l.clone()),
                    node.to_string(),
                ))
                .await?;
            if job.is_some() {
                // TODO queue failover
                self.cluster.notify_job_runner(node).await?;
            }
        }
        Ok(())
    }

    async fn schedule_wal_to_process(&self, wal_id: u64) -> Result<(), CubeError> {
        let wal_node_name = self.cluster.server_name().to_string(); // TODO move to WAL
        let job = self
            .meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::WALs, wal_id),
                JobType::WalPartitioning,
                wal_node_name.clone(),
            ))
            .await?;
        if job.is_some() {
            // TODO queue failover
            self.cluster.notify_job_runner(wal_node_name).await?;
        }
        Ok(())
    }

    async fn schedule_multi_partition_split(
        &self,
        multi_partition_id: u64,
    ) -> Result<(), CubeError> {
        let node = pick_worker_by_ids(self.config.as_ref(), [multi_partition_id]).to_string();
        let job = self
            .meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::MultiPartitions, multi_partition_id),
                JobType::MultiPartitionSplit,
                node.clone(),
            ))
            .await?;
        if job.is_some() {
            // TODO queue failover
            self.cluster.notify_job_runner(node).await?;
        }
        Ok(())
    }

    async fn schedule_finish_multi_split_if_needed(
        &self,
        multi_partition_id: u64,
    ) -> Result<(), CubeError> {
        if self
            .meta_store
            .find_unsplit_partitions(multi_partition_id)
            .await?
            .is_empty()
        {
            return Ok(());
        }
        let node = pick_worker_by_ids(self.config.as_ref(), [multi_partition_id]).to_string();
        let job = self
            .meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::MultiPartitions, multi_partition_id),
                JobType::FinishMultiSplit,
                node.clone(),
            ))
            .await?;
        if job.is_some() {
            // TODO queue failover
            self.cluster.notify_job_runner(node).await?;
        }
        Ok(())
    }

    pub async fn schedule_partition_to_compact(
        &self,
        p: &IdRow<Partition>,
    ) -> Result<(), CubeError> {
        let node = self.cluster.node_name_by_partition(p);
        let job = self
            .meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::Partitions, p.get_id()),
                JobType::PartitionCompaction,
                node.clone(),
            ))
            .await?;
        if job.is_some() {
            // TODO queue failover
            self.cluster.notify_job_runner(node).await?;
        }
        Ok(())
    }

    async fn schedule_partition_warmup(
        &self,
        p: &IdRow<Partition>,
        path: String,
    ) -> Result<(), CubeError> {
        let node_name = self.cluster.node_name_by_partition(p);
        let result = self
            .cluster
            .warmup_download(&node_name, path, p.get_row().file_size())
            .await;

        deactivate_table_on_corrupt_data(self.meta_store.clone(), &result, p).await;

        result
    }
}

#[derive(Debug, Eq, PartialEq)]
struct GCTimedTask {
    pub deadline: Instant,
    pub task: GCTask,
}

impl PartialOrd for GCTimedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reverse order to have min heap
        other.deadline.partial_cmp(&self.deadline)
    }
}

impl Ord for GCTimedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order to have min heap
        other.deadline.cmp(&self.deadline)
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
enum GCTask {
    RemoveRemoteFile(/*remote_path*/ String),
    DeleteChunk(/*chunk_id*/ u64),
    DeleteMiddleManPartition(/*partition_id*/ u64),
    DeletePartition(/*partition_id*/ u64),
}

/// Cleans up deactivated partitions and chunks on remote fs.
/// Ensures enough time has passed that queries over those files finish.
struct DataGCLoop {
    metastore: Arc<dyn MetaStore>,
    remote_fs: Arc<dyn RemoteFs>,
    config: Arc<dyn ConfigObj>,
    stop: CancellationToken,
    task_notify: Notify,
    pending: RwLock<(BinaryHeap<GCTimedTask>, HashSet<GCTask>)>,
}

impl DataGCLoop {
    fn new(
        metastore: Arc<dyn MetaStore>,
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
        stop: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(DataGCLoop {
            metastore,
            remote_fs,
            config,
            stop,
            task_notify: Notify::new(),
            pending: RwLock::new((BinaryHeap::new(), HashSet::new())),
        })
    }

    async fn send(&self, task: GCTimedTask) -> Result<(), CubeError> {
        if self.pending.read().await.1.get(&task.task).is_none() {
            let mut pending_lock = self.pending.write().await;
            // Double-checked locking
            if pending_lock.1.get(&task.task).is_none() {
                log::trace!(
                    "Posting GCTask {}: {:?}",
                    task.deadline
                        .checked_duration_since(Instant::now())
                        .map(|d| format!("in {:?}", d))
                        .unwrap_or("now".to_string()),
                    task
                );
                pending_lock.1.insert(task.task.clone());
                pending_lock.0.push(task);
                self.task_notify.notify_waiters();
            }
        }

        Ok(())
    }

    async fn run(&self) {
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => {
                    return;
                }
                _ = Delay::new(Duration::from_secs(self.config.gc_loop_interval())) => {}
                _ = self.task_notify.notified() => {}
            };

            while self
                .pending
                .read()
                .await
                .0
                .peek()
                .map(|current| current.deadline <= Instant::now())
                .unwrap_or(false)
            {
                let task = {
                    let mut pending_lock = self.pending.write().await;
                    // Double-checked locking
                    if pending_lock
                        .0
                        .peek()
                        .map(|current| current.deadline <= Instant::now())
                        .unwrap_or(false)
                    {
                        let task = pending_lock.0.pop().unwrap();
                        pending_lock.1.remove(&task.task);
                        task.task
                    } else {
                        continue;
                    }
                };

                log::trace!("Executing GCTask: {:?}", task);

                match task {
                    GCTask::RemoveRemoteFile(remote_path) => {
                        log::trace!("Removing deactivated data file: {}", remote_path);
                        if let Err(e) = self.remote_fs.delete_file(&remote_path).await {
                            log::error!(
                                "Could not remove deactivated data file({}): {}",
                                remote_path,
                                e
                            );
                        }
                    }
                    GCTask::DeleteChunk(chunk_id) => {
                        if let Ok(chunk) = self.metastore.get_chunk(chunk_id).await {
                            if !chunk.get_row().active() {
                                log::trace!("Removing deactivated chunk {}", chunk_id);
                                if let Err(e) = self.metastore.delete_chunk(chunk_id).await {
                                    log::error!(
                                        "Could not remove deactivated chunk ({}): {}",
                                        chunk_id,
                                        e
                                    );
                                }
                            } else {
                                log::trace!(
                                    "Skipping removing of chunk {} because it was activated",
                                    chunk_id
                                );
                            }
                        } else {
                            log::trace!("Skipping removing of deactivated chunk {} because it was already removed", chunk_id);
                        }
                    }
                    GCTask::DeleteMiddleManPartition(partition_id) => {
                        if let Ok(true) = self
                            .metastore
                            .can_delete_middle_man_partition(partition_id)
                            .await
                        {
                            log::trace!("Removing middle man partition {}", partition_id);
                            if let Err(e) = self
                                .metastore
                                .delete_middle_man_partition(partition_id)
                                .await
                            {
                                log::error!(
                                    "Could not remove middle man partition ({}): {}",
                                    partition_id,
                                    e
                                );
                            }
                        } else {
                            log::trace!(
                                "Skipping removing of middle man partition {}",
                                partition_id
                            );
                        }
                    }
                    GCTask::DeletePartition(partition_id) => {
                        if let Ok(true) = self.metastore.can_delete_partition(partition_id).await {
                            log::trace!("Removing orphaned partition {}", partition_id);
                            if let Err(e) = self.metastore.delete_partition(partition_id).await {
                                log::error!(
                                    "Could not remove orphaned partition ({}): {}",
                                    partition_id,
                                    e
                                );
                            }
                        } else {
                            log::trace!("Skipping removing orphaned partition {}", partition_id);
                        }
                    }
                }
            }
        }
    }
}
