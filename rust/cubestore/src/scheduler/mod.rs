use crate::cluster::{pick_worker_by_ids, Cluster};
use crate::config::ConfigObj;
use crate::metastore::job::{Job, JobType};
use crate::metastore::partition::partition_file_name;
use crate::metastore::table::Table;
use crate::metastore::{IdRow, MetaStore, MetaStoreEvent, Partition, RowKey, TableId};
use crate::remotefs::RemoteFs;
use crate::store::{ChunkStore, WALStore};
use crate::util::time_span::warn_long_fut;
use crate::util::WorkerLoop;
use crate::CubeError;
use chrono::Utc;
use datafusion::cube_ext;
use flatbuffers::bitflags::_core::time::Duration;
use futures_timer::Delay;
use log::error;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub struct SchedulerImpl {
    meta_store: Arc<dyn MetaStore>,
    cluster: Arc<dyn Cluster>,
    remote_fs: Arc<dyn RemoteFs>,
    event_receiver: Mutex<Receiver<MetaStoreEvent>>,
    stop_sender: watch::Sender<bool>,
    stop_receiver: Mutex<watch::Receiver<bool>>,
    gc_loop: Mutex<DataGCLoop>,
    gc_sender: UnboundedSender<GCTimedTask>,
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
        let (tx, rx) = watch::channel(false);
        let (gc_loop, gc_sender) =
            DataGCLoop::new(meta_store.clone(), remote_fs.clone(), rx.clone());
        SchedulerImpl {
            meta_store,
            cluster,
            remote_fs,
            event_receiver: Mutex::new(event_receiver),
            stop_sender: tx,
            stop_receiver: Mutex::new(rx),
            gc_loop: Mutex::new(gc_loop),
            gc_sender,
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
                let mut gc_loop = scheduler
                    .gc_loop
                    .try_lock()
                    .expect("Trying to spawn loops multiple times");
                gc_loop.run().await;
                Ok(())
            }),
            cube_ext::spawn(async move { Self::run_scheduler(scheduler2).await }),
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

    async fn run_scheduler(scheduler: Arc<SchedulerImpl>) -> Result<(), CubeError> {
        loop {
            let mut stop_receiver = scheduler.stop_receiver.lock().await;
            let mut event_receiver = scheduler.event_receiver.lock().await;
            let event = tokio::select! {
                res = stop_receiver.changed() => {
                    if res.is_err() || *stop_receiver.borrow() {
                        return Ok(());
                    } else {
                        continue;
                    }
                }
                event = event_receiver.recv() => {
                    event?
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

    async fn delete_middle_man_partitions(&self) -> Result<(), CubeError> {
        let all_inactive_partitions = self.meta_store.all_inactive_middle_man_partitions().await?;

        for partition in all_inactive_partitions.iter() {
            let deadline = Instant::now() + Duration::from_secs(self.config.import_job_timeout());
            self.gc_sender.send(GCTimedTask(
                deadline,
                GCTask::DeleteMiddleManPartition(partition.get_id()),
            ))?;
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
            self.gc_sender
                .send(GCTimedTask(deadline, GCTask::DeleteChunk(chunk.get_id())))?;
        }
        Ok(())
    }

    async fn remove_inactive_chunks(&self) -> Result<(), CubeError> {
        // TODO we can do this reconciliation more rarely
        let all_inactive_chunks = self.meta_store.all_inactive_chunks().await?;

        for chunk in all_inactive_chunks.iter() {
            let deadline = Instant::now() + Duration::from_secs(self.config.not_used_timeout());
            self.gc_sender
                .send(GCTimedTask(deadline, GCTask::DeleteChunk(chunk.get_id())))?;
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
        self.stop_sender.send(true)?;
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
                        self.schedule_compaction_if_needed(&partition).await?;
                    } else {
                        self.schedule_repartition(&partition).await?;
                    }
                } else {
                    let deadline =
                        Instant::now() + Duration::from_secs(self.config.not_used_timeout());
                    self.gc_sender
                        .send(GCTimedTask(deadline, GCTask::DeleteChunk(chunk.get_id())))?;
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
                self.remote_fs
                    .delete_file(
                        ChunkStore::chunk_remote_path(chunk.get_id(), chunk.get_row().suffix())
                            .as_str(),
                    )
                    .await?
            }
        }
        if let MetaStoreEvent::DeletePartition(partition) = &event {
            // remove file only if partition is active otherwise it should be removed when it's deactivated
            if partition.get_row().is_active() {
                if let Some(file_name) = partition.get_row().get_full_name(partition.get_id()) {
                    self.remote_fs.delete_file(file_name.as_str()).await?;
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
                    self.gc_sender
                        .send(GCTimedTask(deadline, GCTask::RemoveRemoteFile(file_name)))?;
                }
            }
        }
        if let MetaStoreEvent::DeleteJob(job) = event {
            match job.get_row().job_type() {
                JobType::Repartition => match job.get_row().row_reference() {
                    RowKey::Table(TableId::Partitions, p) => {
                        let p = self.meta_store.get_partition(*p).await?;
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
        let chunk_sizes = self
            .meta_store
            .get_partition_chunk_sizes(partition_id)
            .await?;
        let all_chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?;
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
            .filter_map(|c| c.get_row().created_at().clone())
            .min();
        let max_created_at = chunks
            .iter()
            .filter_map(|c| c.get_row().created_at().clone())
            .max();
        let check_row_counts = partition.get_row().multi_partition_id().is_none();
        if check_row_counts && chunk_sizes > self.config.compaction_chunks_total_size_threshold()
            || chunks.len()
            > self.config.compaction_chunks_count_threshold() as usize
            // TODO config
            || in_memory_chunks.len() > 100
            || min_in_memory_created_at.map(|min| Utc::now().signed_duration_since(min).num_seconds() > 60).unwrap_or(false)
            || max_created_at.map(|min| Utc::now().signed_duration_since(min).num_seconds() > 600).unwrap_or(false)
        {
            self.schedule_partition_to_compact(partition).await?;
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
        self.cluster.warmup_download(&node_name, path).await
    }
}

#[derive(Debug)]
struct GCTimedTask(/*deadline*/ Instant, GCTask);
#[derive(Debug)]
enum GCTask {
    RemoveRemoteFile(/*remote_path*/ String),
    DeleteChunk(/*chunk_id*/ u64),
    DeleteMiddleManPartition(/*partition_id*/ u64),
}

/// Cleans up deactivated partitions and chunks on remote fs.
/// Ensures enough time has passed that queries over those files finish.
struct DataGCLoop {
    metastore: Arc<dyn MetaStore>,
    remote_fs: Arc<dyn RemoteFs>,
    stop: watch::Receiver<bool>,
    to_delete: UnboundedReceiver<GCTimedTask>,
}

impl DataGCLoop {
    fn new(
        metastore: Arc<dyn MetaStore>,
        remote_fs: Arc<dyn RemoteFs>,
        stop: watch::Receiver<bool>,
    ) -> (DataGCLoop, UnboundedSender<GCTimedTask>) {
        let (sender, receiver) = unbounded_channel();
        (
            DataGCLoop {
                metastore,
                remote_fs,
                stop,
                to_delete: receiver,
            },
            sender,
        )
    }

    async fn run(&mut self) {
        loop {
            let GCTimedTask(deadline, task) = tokio::select! {
                res = self.stop.changed() => {
                    if res.is_err() || *self.stop.borrow() {
                        return;
                    } else {
                        continue;
                    }
                }
                event = self.to_delete.recv() => {
                    match event {
                        None => return, // channel closed.
                        Some(e) => e,
                    }
                }
            };

            // Sleep until the deadline or cancellation.
            loop {
                tokio::select! {
                    res = self.stop.changed() => {
                        if res.is_err() || *self.stop.borrow() {
                            return;
                        } else {
                            continue;
                        }
                    }
                    () = tokio::time::sleep_until(deadline) => {break;}
                }
            }

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
                    if self.metastore.get_chunk(chunk_id).await.is_ok() {
                        log::trace!("Removing deactivated chunk {}", chunk_id);
                        if let Err(e) = self.metastore.delete_chunk(chunk_id).await {
                            log::error!("Could not remove deactivated chunk ({}): {}", chunk_id, e);
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
                        log::trace!("Skipping removing of middle man partition {}", partition_id);
                    }
                }
            }
        }
    }
}
