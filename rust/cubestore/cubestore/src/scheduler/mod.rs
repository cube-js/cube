use crate::cluster::{pick_import_worker, pick_worker_by_ids, Cluster};
use crate::config::{ConfigObj, RepartitionStrategy};
use crate::metastore::chunks::chunk_file_name;
use crate::metastore::job::{Job, JobStatus, JobType};
use crate::metastore::partition::partition_file_name;
use crate::metastore::replay_handle::ReplayHandle;
use crate::metastore::replay_handle::{
    subtract_from_right_seq_pointer_by_location, subtract_if_covers_seq_pointer_by_location,
    union_seq_pointer_by_location, SeqPointerForLocation,
};
use crate::metastore::table::Table;
use crate::metastore::{
    deactivate_table_due_to_corrupt_data, deactivate_table_on_corrupt_data, Chunk, IdRow,
    MetaStore, MetaStoreEvent, Partition, RowKey, TableId,
};
use crate::remotefs::RemoteFs;
use crate::shared::deadline_queue::DeadlineQueue;
use crate::store::{ChunkStore, WALStore};
use crate::util::lock::acquire_lock;
use crate::util::time_span::warn_long_fut;
use crate::util::WorkerLoop;
use crate::CubeError;
use chrono::Utc;
use datafusion::cube_ext;
use futures::future::join_all;
use futures_timer::Delay;
use itertools::Itertools;
use log::error;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast::Receiver;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub struct SchedulerImpl {
    meta_store: Arc<dyn MetaStore>,
    cluster: Arc<dyn Cluster>,
    remote_fs: Arc<dyn RemoteFs>,
    event_receiver: Mutex<Receiver<MetaStoreEvent>>,
    cancel_token: CancellationToken,
    gc_queue: DeadlineQueue<GCTask>,
    config: Arc<dyn ConfigObj>,
    reconcile_loop: WorkerLoop,
    chunk_processing_loop: WorkerLoop,
    chunk_events_queue: Mutex<Vec<(SystemTime, u64)>>,
    in_memory_chunks_to_delete: Mutex<Vec<(String, String)>>, //(node, chunk_is)
    node_last_actions: Mutex<HashMap<String, LastNodeActionTimes>>,
    // Serializes load-aware import placement so concurrent CREATE TABLE events don't read
    // the same in-flight counts and pile onto the same worker before either job is scheduled.
    import_placement_lock: Mutex<()>,
    // Short-TTL memo of the inactive parents still draining (range repartition), so the GC
    // loop doesn't re-scan the whole chunk table on every cycle just to gate their deletion.
    repartition_parents_cache: Mutex<Option<(Instant, Arc<HashSet<u64>>)>>,
}

crate::di_service!(SchedulerImpl, []);

struct LastNodeActionTimes {
    in_memory_compaction: SystemTime,
}

impl LastNodeActionTimes {
    pub fn new() -> Self {
        Self {
            in_memory_compaction: SystemTime::now(),
        }
    }
    pub fn in_memory_compaction(&self) -> SystemTime {
        self.in_memory_compaction.clone()
    }
    pub fn set_in_memory_compaction(&mut self, time: SystemTime) {
        self.in_memory_compaction = time;
    }
}

impl SchedulerImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        cluster: Arc<dyn Cluster>,
        remote_fs: Arc<dyn RemoteFs>,
        event_receiver: Receiver<MetaStoreEvent>,
        config: Arc<dyn ConfigObj>,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        let gc_queue = DeadlineQueue::new(config.gc_loop_interval(), cancel_token.clone());

        let mut workers = config.select_workers().clone();
        if workers.is_empty() {
            workers.push(config.server_name().clone());
        }
        let workers = workers
            .into_iter()
            .map(|n| (n, LastNodeActionTimes::new()))
            .collect::<HashMap<_, _>>();

        Self {
            meta_store,
            cluster,
            remote_fs,
            event_receiver: Mutex::new(event_receiver),
            cancel_token,
            config,
            gc_queue,
            reconcile_loop: WorkerLoop::new("Reconcile"),
            chunk_events_queue: Mutex::new(Vec::with_capacity(1000)),
            in_memory_chunks_to_delete: Mutex::new(Vec::with_capacity(1000)),
            node_last_actions: Mutex::new(workers),
            chunk_processing_loop: WorkerLoop::new("ChunkProcessing"),
            import_placement_lock: Mutex::new(()),
            repartition_parents_cache: Mutex::new(None),
        }
    }

    // Inactive parents that still have active chunks (range repartition in progress),
    // memoized with a short TTL. The GC loop consults this to keep such parents' inactive
    // chunks until they fully drain; the underlying query scans the chunk table, so caching
    // avoids re-scanning it on every GC cycle. Staleness up to the TTL is harmless: the
    // actual chunk deletion is already delayed by not_used_timeout.
    async fn draining_repartition_parents(&self) -> Result<Arc<HashSet<u64>>, CubeError> {
        const TTL: Duration = Duration::from_secs(5);
        {
            let guard = self.repartition_parents_cache.lock().await;
            if let Some((at, set)) = guard.as_ref() {
                if at.elapsed() < TTL {
                    return Ok(set.clone());
                }
            }
        }
        let set = Arc::new(
            self.meta_store
                .all_inactive_partitions_to_repartition()
                .await?
                .into_iter()
                .map(|p| p.get_id())
                .collect::<HashSet<u64>>(),
        );
        *self.repartition_parents_cache.lock().await = Some((Instant::now(), set.clone()));
        Ok(set)
    }

    pub fn spawn_processing_loops(self: Arc<Self>) -> Vec<JoinHandle<Result<(), CubeError>>> {
        let scheduler1 = self.clone();
        let scheduler2 = self.clone();
        let scheduler3 = self.clone();
        let scheduler4 = self.clone();

        vec![
            cube_ext::spawn(async move {
                scheduler1
                    .gc_queue
                    .run(scheduler1.clone(), async move |s, tasks| {
                        s.process_gc_tasks(tasks).await
                    })
                    .await;

                Ok(())
            }),
            cube_ext::spawn(async move {
                scheduler2.run_meta_event_processor().await;
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
            cube_ext::spawn(async move {
                scheduler4
                    .chunk_processing_loop
                    .process(
                        scheduler4.clone(),
                        async move |_| Ok(Delay::new(Duration::from_millis(200)).await),
                        async move |s, _| s.process_chunk_events().await,
                    )
                    .await;
                Ok(())
            }),
        ]
    }

    pub async fn process_gc_tasks(self: Arc<Self>, task: GCTask) -> Result<(), CubeError> {
        log::trace!("Executing GC task: {:?}", task);

        match task {
            GCTask::RemoveRemoteFile(remote_path) => {
                log::trace!("Removing deactivated data file: {}", remote_path);
                if let Err(e) = self.remote_fs.delete_file(remote_path.clone()).await {
                    log::error!(
                        "Could not remove deactivated data file({}): {}",
                        remote_path,
                        e
                    );
                }
            }
            GCTask::DeleteChunks(chunk_ids) => {
                match self.meta_store.get_chunks_out_of_queue(chunk_ids).await {
                    Ok(chunks) => {
                        let ids = chunks
                            .iter()
                            .filter_map(|c| {
                                if c.get_row().active() {
                                    None
                                } else {
                                    Some(c.get_id())
                                }
                            })
                            .collect::<Vec<_>>();

                        if let Err(e) = self.meta_store.delete_chunks_without_checks(ids).await {
                            log::error!(
                                "Could not delete chunks. Get error {} when deleting chunks",
                                e
                            );
                        }
                    }
                    Err(e) => {
                        log::error!(
                                    "Could not delete chunks. Get error {} when trying get chunks for deletion",
                                    e
                                );
                    }
                }
            }
            GCTask::DeleteMiddleManPartition(partition_id) => {
                if let Ok(true) = self
                    .meta_store
                    .can_delete_middle_man_partition(partition_id)
                    .await
                {
                    log::trace!("Removing middle man partition {}", partition_id);
                    if let Err(e) = self
                        .meta_store
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
            GCTask::DeletePartition(partition_id) => {
                if let Ok(true) = self.meta_store.can_delete_partition(partition_id).await {
                    log::trace!("Removing orphaned partition {}", partition_id);
                    if let Err(e) = self.meta_store.delete_partition(partition_id).await {
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
        };

        Ok(())
    }

    async fn run_meta_event_processor(self: Arc<Self>) {
        loop {
            let mut event_receiver = self.event_receiver.lock().await;
            let event = tokio::select! {
                _ = self.cancel_token.cancelled() => {
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

            let scheduler_to_move = self.clone();
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
            "Removing jobs on non-existing nodes",
            Duration::from_millis(5000),
            self.remove_jobs_on_non_exists_nodes(),
        )
        .await
        {
            error!("Error removing orphaned jobs: {}", e);
        }
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
            "Removing unknown jobs",
            Duration::from_millis(5000),
            self.remove_unknown_jobs(),
        )
        .await
        {
            error!("Error removing unknown jobs: {}", e);
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
            "deactivate_chunks_without_partitions",
            Duration::from_millis(5000),
            self.deactivate_chunks_without_partitions(),
        )
        .await
        {
            error!(
                "Error scheduling deactivation chunks without partitions: {}",
                e
            );
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

        if let Err(e) = warn_long_fut(
            "Merge replay handles",
            Duration::from_millis(5000),
            self.merge_replay_handles(),
        )
        .await
        {
            error!("Error merging replay handles: {}", e);
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
            self.gc_queue
                .send(GCTask::DeletePartition(partition.get_id()), deadline)
                .await?;
        }
        Ok(())
    }

    async fn delete_middle_man_partitions(&self) -> Result<(), CubeError> {
        let all_inactive_partitions = self.meta_store.all_inactive_middle_man_partitions().await?;

        for partition in all_inactive_partitions.iter() {
            let deadline = Instant::now() + Duration::from_secs(self.config.import_job_timeout());
            self.gc_queue
                .send(
                    GCTask::DeleteMiddleManPartition(partition.get_id()),
                    deadline,
                )
                .await?;
        }
        Ok(())
    }

    /// This method is responsible for merging `ReplayHandle` to keep their numbers low.
    /// Important merge algorithm points:
    /// - Only `ReplayHandle` without chunks can be merged. Those either persisted or already merged by compaction.
    /// In case of compaction merge we actually doesn't care about orphaned `ReplayHandle` because
    /// `SeqPointer` union yields the same result.
    /// - For failed `ReplayHandle` we always try to find if subsequent `ReplayHandle` already covered
    /// due to replay so we can safely remove it.
    /// Otherwise we just subtract it from resulting `SeqPointer` so freshly created `ReplayHandle`
    /// can't remove failed one.
    pub async fn merge_replay_handles(&self) -> Result<(), CubeError> {
        fn is_newest_handle(handle: &IdRow<ReplayHandle>) -> bool {
            Utc::now()
                .signed_duration_since(handle.get_row().created_at().clone())
                .num_seconds()
                < 60
        }
        let (failed, mut without_failed) = self
            .meta_store
            .all_replay_handles_to_merge()
            .await?
            .into_iter()
            .partition::<Vec<_>, _>(|(h, _)| h.get_row().has_failed_to_persist_chunks());

        without_failed.sort_by_key(|(h, _)| h.get_row().table_id());

        let table_to_failed = failed
            .into_iter()
            .map(|(h, no_active_chunks)| (h.get_row().table_id(), (h, no_active_chunks)))
            .into_group_map();

        let mut to_merge = Vec::new();

        for (table_id, handles) in &without_failed
            .into_iter()
            .chunk_by(|(h, _)| h.get_row().table_id())
        {
            let mut seq_pointer_by_location = None;
            let mut ids = Vec::new();
            let handles = handles.collect::<Vec<_>>();
            for (handle, _) in handles
                .iter()
                .filter(|(handle, no_active_chunks)| !is_newest_handle(handle) && *no_active_chunks)
            {
                union_seq_pointer_by_location(
                    &mut seq_pointer_by_location,
                    handle.get_row().seq_pointers_by_location(),
                )?;
                ids.push(handle.get_id());
            }
            let empty_vec = Vec::new();
            let failed = table_to_failed.get(&table_id).unwrap_or(&empty_vec);

            for (failed_handle, no_active_chunks) in failed.iter() {
                let mut failed_seq_pointers =
                    failed_handle.get_row().seq_pointers_by_location().clone();
                let mut replay_after_failed_union = None;
                let replay_after_failed = handles
                    .iter()
                    .filter(|(h, _)| {
                        h.get_id() > failed_handle.get_id()
                            && !h.get_row().has_failed_to_persist_chunks()
                    })
                    .collect::<Vec<_>>();
                for (replay, _) in replay_after_failed.iter() {
                    union_seq_pointer_by_location(
                        &mut replay_after_failed_union,
                        replay.get_row().seq_pointers_by_location(),
                    )?;
                }
                subtract_if_covers_seq_pointer_by_location(
                    &mut failed_seq_pointers,
                    &replay_after_failed_union,
                )?;
                let empty_seq_pointers = failed_seq_pointers
                    .map(|p| {
                        p.iter()
                            .all(|p| p.as_ref().map(|p| p.is_empty()).unwrap_or(true))
                    })
                    .unwrap_or(true);
                if empty_seq_pointers && *no_active_chunks {
                    ids.push(failed_handle.get_id());
                } else if !empty_seq_pointers {
                    subtract_from_right_seq_pointer_by_location(
                        &mut seq_pointer_by_location,
                        failed_handle.get_row().seq_pointers_by_location(),
                    )?;
                }
            }

            to_merge.push((ids, seq_pointer_by_location));
        }

        for (ids, seq_pointer_by_location) in to_merge.into_iter() {
            if !ids.is_empty() {
                self.meta_store
                    .replace_replay_handles(ids, seq_pointer_by_location)
                    .await?;
            }
        }

        Ok(())
    }

    async fn schedule_all_pending_compactions(&self) -> Result<(), CubeError> {
        let partition_compaction_candidates_id = self
            .meta_store
            // TODO config
            .get_partitions_with_chunks_created_seconds_ago(60)
            .await?;
        let mut nodes_for_in_memory_compactions = HashSet::new();
        for p in partition_compaction_candidates_id {
            let node_name = self.cluster.node_name_by_partition(&p);
            nodes_for_in_memory_compactions.insert(node_name);
            self.schedule_compaction_if_needed(&p).await?;
        }
        for node in nodes_for_in_memory_compactions {
            self.schedule_node_in_memory_compaction(node).await?;
        }
        Ok(())
    }
    async fn deactivate_chunks_without_partitions(&self) -> Result<(), CubeError> {
        let chunks_without_partitions = self
            .meta_store
            .get_chunks_without_partition_created_seconds_ago(60)
            .await?;

        let mut ids = Vec::new();
        for chunk in chunks_without_partitions {
            if let Some(handle_id) = chunk.get_row().replay_handle_id() {
                self.meta_store
                    .update_replay_handle_failed_if_exists(*handle_id, true)
                    .await?;
            }
            ids.push(chunk.get_id());
        }
        self.meta_store.deactivate_chunks_without_check(ids).await?;
        Ok(())
    }

    async fn remove_inactive_not_uploaded_chunks(&self) -> Result<(), CubeError> {
        let all_inactive_not_uploaded_chunks =
            self.meta_store.all_inactive_not_uploaded_chunks().await?;

        let ids = all_inactive_not_uploaded_chunks
            .iter()
            .map(|c| c.get_id().clone())
            .collect::<Vec<_>>();

        let deadline = Instant::now() + Duration::from_secs(self.config.import_job_timeout());
        for part in ids.as_slice().chunks(10000) {
            self.gc_queue
                .send(
                    GCTask::DeleteChunks(part.iter().cloned().collect_vec()),
                    deadline,
                )
                .await?;
        }
        Ok(())
    }

    async fn remove_inactive_chunks(&self) -> Result<(), CubeError> {
        // TODO we can do this reconciliation more rarely
        let all_inactive_chunks = self.meta_store.all_inactive_chunks().await?;

        let (in_memory_inactive, persistent_inactive): (Vec<_>, Vec<_>) = all_inactive_chunks
            .iter()
            .partition(|c| c.get_row().in_memory());

        if !in_memory_inactive.is_empty() {
            let seconds = self.config.in_memory_not_used_timeout();
            let deadline = Instant::now() + Duration::from_secs(seconds);
            let ids = in_memory_inactive
                .iter()
                .map(|c| c.get_id().clone())
                .collect::<Vec<_>>();
            for part in ids.as_slice().chunks(10000) {
                self.gc_queue
                    .send(
                        GCTask::DeleteChunks(part.iter().cloned().collect_vec()),
                        deadline,
                    )
                    .await?;
            }
        }

        if !persistent_inactive.is_empty() {
            let seconds = self.config.not_used_timeout();
            let deadline = Instant::now() + Duration::from_secs(seconds);
            // Range-based repartition slices an inactive parent over ALL its chunks
            // (active and inactive) so the [start, end] ranges stay pinned to chunk ids.
            // While such a parent is still draining, keep its inactive chunks: deleting
            // them would shift the slicing boundaries on the next re-slice. Once the
            // parent has no active chunks it is no longer in this set and its inactive
            // chunks are collected normally.
            let keep_parents = if self.config.repartition_strategy() == RepartitionStrategy::Range {
                self.draining_repartition_parents().await?
            } else {
                Arc::new(HashSet::new())
            };
            let ids = persistent_inactive
                .iter()
                .filter(|c| !keep_parents.contains(&c.get_row().get_partition_id()))
                .map(|c| c.get_id())
                .collect::<Vec<_>>();
            for part in ids.as_slice().chunks(10000) {
                self.gc_queue
                    .send(
                        GCTask::DeleteChunks(part.iter().cloned().collect_vec()),
                        deadline,
                    )
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn reconcile_table_imports(&self) -> Result<(), CubeError> {
        // Using get_tables_with_path due to it's cached
        let tables = self.meta_store.get_tables_with_path(true).await?;
        for table in tables.iter() {
            if table.table.get_row().is_ready() && !table.table.get_row().sealed() {
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

    async fn remove_jobs_on_non_exists_nodes(&self) -> Result<(), CubeError> {
        let jobs_to_remove = self.meta_store.get_jobs_on_non_exists_nodes().await?;
        for job in jobs_to_remove.into_iter() {
            log::info!("Removing job {:?} on non-existing node", job);
            self.meta_store.delete_job(job.get_id()).await?;
        }
        Ok(())
    }

    async fn remove_unknown_jobs(&self) -> Result<(), CubeError> {
        let deleted = self.meta_store.delete_unknown_jobs().await?;
        if deleted > 0 {
            crate::app_metrics::JOBS_UNKNOWN_DELETED.add(deleted as i64);
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
            self.meta_store
                .update_status(job.get_id(), JobStatus::Orphaned)
                .await?;
            self.meta_store.delete_job(job.get_id()).await?;
        }
        Ok(())
    }

    pub fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.cancel_token.cancel();
        self.reconcile_loop.stop();
        self.chunk_processing_loop.stop();
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
            let mut chunk_queue = self.chunk_events_queue.lock().await;
            if let Some(itm) = chunk_queue.iter_mut().find(|(_, id)| id == &row_id) {
                itm.0 = SystemTime::now();
            } else {
                chunk_queue.push((SystemTime::now(), row_id))
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
                .local_file(WALStore::wal_remote_path(row_id))
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
                let chunk_name = chunk_file_name(chunk.get_id(), chunk.get_row().suffix());
                let mut to_delete = self.in_memory_chunks_to_delete.lock().await;
                to_delete.push((node_name, chunk_name))
            } else if chunk.get_row().uploaded() {
                let file_name =
                    ChunkStore::chunk_remote_path(chunk.get_id(), chunk.get_row().suffix());
                let deadline = Instant::now()
                    + Duration::from_secs(self.config.meta_store_snapshot_interval() * 2);
                self.gc_queue
                    .send(GCTask::RemoveRemoteFile(file_name), deadline)
                    .await?;
            }
        }
        if let MetaStoreEvent::DeletePartition(partition) = &event {
            // remove file only if partition is active otherwise it should be removed when it's deactivated
            if partition.get_row().is_active() {
                if let Some(file_name) = partition.get_row().get_full_name(partition.get_id()) {
                    let deadline = Instant::now()
                        + Duration::from_secs(self.config.meta_store_snapshot_interval() * 2);
                    self.gc_queue
                        .send(GCTask::RemoveRemoteFile(file_name), deadline)
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
                    self.gc_queue
                        .send(GCTask::RemoveRemoteFile(file_name), deadline)
                        .await?;
                }
            }
        }
        if let MetaStoreEvent::UpdateJob(_, new_job) = &event {
            match new_job.get_row().job_type() {
                JobType::TableImportCSV(location) if Table::is_stream_location(location) => {
                    match new_job.get_row().status() {
                        JobStatus::Error(e) if e.contains("Stale stream timeout") => {
                            log::info!("Removing stale stream job: {:?}", new_job);
                            self.meta_store.delete_job(new_job.get_id()).await?;
                            self.reconcile_table_imports().await?;
                        }
                        JobStatus::Error(e) if e.contains("Stream requires replay") => {
                            log::info!("Removing stream job that requires replay: {:?}", new_job);
                            self.meta_store.delete_job(new_job.get_id()).await?;
                            self.reconcile_table_imports().await?;
                        }
                        JobStatus::Error(e) if e.contains("CorruptData") => {
                            let table_id = match new_job.get_row().row_reference() {
                                RowKey::Table(TableId::Tables, table_id) => table_id,
                                x => panic!("Unexpected job key: {:?}", x),
                            };
                            deactivate_table_due_to_corrupt_data(
                                self.meta_store.clone(),
                                *table_id,
                                e.to_string(),
                            )
                            .await?;
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        if let MetaStoreEvent::DeleteJob(job) = event {
            match job.get_row().job_type() {
                JobType::RepartitionChunk | JobType::RepartitionRange(_) => {
                    match job.get_row().row_reference() {
                        RowKey::Table(TableId::Chunks, c) => {
                            let c = self.meta_store.get_chunk(*c).await?;
                            let p = self
                                .meta_store
                                .get_partition(c.get_row().get_partition_id())
                                .await?;
                            // Re-slice the parent: drains any range whose job has not run
                            // yet and picks up a tail chunk that landed after the previous
                            // slicing (re-slicing is id-pinned, so existing ranges dedup).
                            self.schedule_repartition_if_needed(&p).await?
                        }
                        _ => panic!(
                            "Unexpected row reference: {:?}",
                            job.get_row().row_reference()
                        ),
                    }
                }
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

    async fn process_chunk_events(self: &Arc<Self>) -> Result<(), CubeError> {
        let ids = {
            let mut chunk_queue = self.chunk_events_queue.lock().await;
            let dur = Duration::from_millis(200);
            let (to_process, mut rest) = chunk_queue
                .iter()
                .partition::<Vec<_>, _>(|(t, _)| t.elapsed().map_or(true, |d| d > dur));
            std::mem::swap(&mut rest, &mut chunk_queue);
            to_process.into_iter().map(|(_, id)| id).collect::<Vec<_>>()
        };
        if !ids.is_empty() {
            let uploaded_chunks = self
                .meta_store
                .get_chunks_out_of_queue(ids)
                .await?
                .into_iter()
                .filter(|c| c.get_row().uploaded())
                .collect::<Vec<_>>();
            let (active_chunks, inactive_chunks) = uploaded_chunks
                .into_iter()
                .partition(|c| c.get_row().active());

            self.process_active_chunks(active_chunks).await?;
            self.process_inactive_chunks(inactive_chunks).await?;
        }
        {
            let chunks_to_delete = {
                let mut chunks = self.in_memory_chunks_to_delete.lock().await;
                if chunks.is_empty() {
                    Vec::new()
                } else {
                    let mut result = Vec::new();
                    std::mem::swap(&mut *chunks, &mut result);
                    result
                }
            };
            if !chunks_to_delete.is_empty() {
                let chunks_to_delete = chunks_to_delete.into_iter().into_group_map();
                for (node, names) in chunks_to_delete {
                    if !names.is_empty() {
                        if let Err(e) = self
                            .cluster
                            .free_deleted_memory_chunks(&node, names.clone())
                            .await
                        {
                            log::error!(
                                "Error while trying release in memory chunks in node {}: {}",
                                node,
                                e
                            );

                            let mut chunks = self.in_memory_chunks_to_delete.lock().await;
                            chunks.extend(names.iter().map(|v| (node.clone(), v.clone())));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_active_chunks(
        self: &Arc<Self>,
        chunks: Vec<IdRow<Chunk>>,
    ) -> Result<(), CubeError> {
        let mut partition_ids_map: HashMap<u64, bool> = HashMap::new(); // id -> has in_memory chunks
        for chunk in chunks.into_iter() {
            if !chunk.get_row().active() {
                continue;
            }

            let entry = partition_ids_map
                .entry(chunk.get_row().get_partition_id())
                .or_insert(false);
            if chunk.get_row().in_memory() {
                *entry = true;
            }
        }

        if !partition_ids_map.is_empty() {
            let partition_ids = partition_ids_map.iter().map(|(id, _)| *id).collect();
            let partitions = self
                .meta_store
                .get_partitions_out_of_queue(partition_ids)
                .await?;
            let mut futures = Vec::with_capacity(partitions.len());
            let mut nodes_for_in_memory_compactions = HashSet::new();
            for partition in partitions.into_iter() {
                if partition.get_row().is_active() {
                    if *partition_ids_map.get(&partition.get_id()).unwrap_or(&false) {
                        let node_name = self.cluster.node_name_by_partition(&partition);
                        nodes_for_in_memory_compactions.insert(node_name);
                    }
                    let partition_to_move = partition.clone();
                    let self_to_move = self.clone();
                    futures.push(cube_ext::spawn(async move {
                        self_to_move
                            .schedule_compaction_if_needed(&partition_to_move)
                            .await
                    }));
                    /* features.push(async move {
                        self.schedule_compaction_if_needed(&partition_to_move).await?
                    }); */
                } else {
                    let partition_to_move = partition.clone();
                    let self_to_move = self.clone();
                    futures.push(cube_ext::spawn(async move {
                        self_to_move.schedule_repartition(&partition_to_move).await
                    }));
                }
            }

            for node in nodes_for_in_memory_compactions {
                self.schedule_node_in_memory_compaction(node).await?;
            }

            join_all(futures)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .collect::<Result<(), _>>()?;
        }

        Ok(())
    }

    async fn process_inactive_chunks(
        self: &Arc<Self>,
        chunks: Vec<IdRow<Chunk>>,
    ) -> Result<(), CubeError> {
        let (in_memory_inactive, persistent_inactive): (Vec<_>, Vec<_>) = chunks
            .into_iter()
            .filter(|c| !c.get_row().active())
            .partition(|c| c.get_row().in_memory());

        if !in_memory_inactive.is_empty() {
            let seconds = self.config.in_memory_not_used_timeout();
            let deadline = Instant::now() + Duration::from_secs(seconds);
            self.gc_queue
                .send(
                    GCTask::DeleteChunks(
                        in_memory_inactive
                            .into_iter()
                            .map(|c| c.get_id())
                            .collect::<Vec<_>>(),
                    ),
                    deadline,
                )
                .await?;
        }
        if !persistent_inactive.is_empty() {
            let seconds = self.config.not_used_timeout();
            let deadline = Instant::now() + Duration::from_secs(seconds);
            self.gc_queue
                .send(
                    GCTask::DeleteChunks(
                        persistent_inactive
                            .into_iter()
                            .map(|c| c.get_id())
                            .collect::<Vec<_>>(),
                    ),
                    deadline,
                )
                .await?;
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

        let min_created_at = chunks
            .iter()
            .filter_map(|c| c.get_row().created_at().clone())
            .min();
        let check_row_counts = partition.get_row().multi_partition_id().is_none();
        if check_row_counts && chunk_sizes > self.config.compaction_chunks_total_size_threshold()
            || chunks.len() > self.config.compaction_chunks_count_threshold() as usize
            // Force compaction if other chunks were created far ago
            || min_created_at.map(|min| Utc::now().signed_duration_since(min).num_seconds() > self.config.compaction_chunks_max_lifetime_threshold() as i64).unwrap_or(false)
        {
            self.schedule_partition_to_compact(partition).await?;
        }
        Ok(())
    }

    pub async fn schedule_compaction_in_memory_chunks_if_needed(
        &self,
        partition: &IdRow<Partition>,
    ) -> Result<(), CubeError> {
        let compaction_in_memory_chunks_count_threshold =
            self.config.compaction_in_memory_chunks_count_threshold();

        let partition_id = partition.get_id();

        let chunks = self
            .meta_store
            .get_chunks_by_partition_out_of_queue(partition_id, false)
            .await?
            .into_iter()
            .filter(|c| c.get_row().in_memory() && c.get_row().active())
            .collect::<Vec<_>>();

        let oldest_insert_at = chunks
            .iter()
            .filter_map(|c| c.get_row().oldest_insert_at().clone())
            .min();

        if chunks.len() > compaction_in_memory_chunks_count_threshold
            || oldest_insert_at
                .map(|min| {
                    Utc::now().signed_duration_since(min).num_seconds()
                        > self
                            .config
                            .compaction_in_memory_chunks_max_lifetime_threshold()
                            as i64
                })
                .unwrap_or(false)
        {
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
        let table = self.meta_store.get_table_by_id(table_id).await?;
        if table.get_row().sealed() {
            return Ok(());
        }

        if !self.config.load_aware_import_placement_enabled() {
            // Default: stateless hash placement of (table_id, location). Empty counts make
            // every worker tie, so pick_import_worker degenerates to the plain hash pick.
            let empty = HashMap::new();
            for &l in locations {
                let node = pick_import_worker(self.config.as_ref(), table_id, l, &empty);
                let job = self
                    .meta_store
                    .add_job(Job::new(
                        RowKey::Table(TableId::Tables, table_id),
                        JobType::TableImportCSV(l.clone()),
                        node.clone(),
                    ))
                    .await?;
                if job.is_some() {
                    // TODO queue failover
                    self.cluster.notify_job_runner(node).await?;
                }
            }
            return Ok(());
        }

        // Load-aware placement. Scan in-flight import counts once, then place each location on
        // the least-loaded worker, updating counts in memory. The lock serializes concurrent
        // CREATE TABLE placements so they don't read the same counts and pile onto the same
        // workers. It is best-effort: on timeout we proceed without it — the worst case is an
        // uneven spread, never data corruption.
        let mut nodes_to_notify = Vec::new();
        {
            let guard = acquire_lock("import placement", self.import_placement_lock.lock()).await;
            if let Err(e) = &guard {
                log::warn!(
                    "Placing import jobs without serialization, spread may be uneven: {}",
                    e
                );
            }
            let mut counts = self.meta_store.in_flight_import_jobs_by_node().await?;
            for &l in locations {
                let node = pick_import_worker(self.config.as_ref(), table_id, l, &counts);
                let job = self
                    .meta_store
                    .add_job(Job::new(
                        RowKey::Table(TableId::Tables, table_id),
                        JobType::TableImportCSV(l.clone()),
                        node.clone(),
                    ))
                    .await?;
                if job.is_some() {
                    *counts.entry(node.clone()).or_insert(0) += 1;
                    nodes_to_notify.push(node);
                }
            }
        }
        for node in nodes_to_notify {
            // TODO queue failover
            self.cluster.notify_job_runner(node).await?;
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

    pub async fn schedule_node_in_memory_compaction(&self, node: String) -> Result<(), CubeError> {
        let mut node_last_actions = self.node_last_actions.lock().await;
        if let Some(last_action) = node_last_actions.get_mut(&node) {
            if last_action
                .in_memory_compaction()
                .elapsed()
                .ok()
                .map_or(false, |d| {
                    d >= Duration::from_secs(
                        self.config
                            .compaction_in_memory_chunks_schedule_period_secs(),
                    )
                })
            {
                let job = self
                    .meta_store
                    .add_job(Job::new(
                        RowKey::Table(TableId::Tables, 0),
                        JobType::NodeInMemoryChunksCompaction(node.clone()),
                        node.clone(),
                    ))
                    .await?;
                if job.is_some() {
                    self.cluster.notify_job_runner(node).await?;
                    last_action.set_in_memory_compaction(SystemTime::now());
                }
            }
            Ok(())
        } else {
            Err(CubeError::internal(format!(
                "schedule_node_in_memory_compaction: undefined node {}",
                node
            )))
        }
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

        deactivate_table_on_corrupt_data(self.meta_store.clone(), &result, p, None).await;

        result
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum GCTask {
    RemoveRemoteFile(/*remote_path*/ String),
    DeleteChunks(/*chunk_ids*/ Vec<u64>),
    DeleteMiddleManPartition(/*partition_id*/ u64),
    DeletePartition(/*partition_id*/ u64),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::metastore::RocksMetaStore;
    use std::fs;

    #[tokio::test]
    async fn test_remove_jobs_on_non_exists_nodes() {
        let config = Config::test("remove_jobs_on_non_exists_nodes");

        let _ = fs::remove_dir_all(config.local_dir());
        let _ = fs::remove_dir_all(config.remote_dir());

        let services = config.configure().await;
        services.start_processing_loops().await.unwrap();
        let meta_store = services.meta_store.clone();
        meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::Partitions, 1),
                JobType::PartitionCompaction,
                "not_existis_node".to_string(),
            ))
            .await
            .unwrap();
        let exists_job = meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::Partitions, 2),
                JobType::PartitionCompaction,
                config.config_obj().server_name().to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let all_jobs = meta_store.all_jobs().await.unwrap();
        assert_eq!(all_jobs.len(), 2);
        let scheduler = services.injector.get_service_typed::<SchedulerImpl>().await;
        scheduler.remove_jobs_on_non_exists_nodes().await.unwrap();
        let all_jobs = meta_store.all_jobs().await.unwrap();
        assert_eq!(all_jobs.len(), 1);
        assert_eq!(all_jobs[0].get_id(), exists_job.get_id());
        services.stop_processing_loops().await.unwrap();
        let _ = fs::remove_dir_all(config.local_dir());
        let _ = fs::remove_dir_all(config.remote_dir());
    }

    #[tokio::test]
    async fn test_remove_jobs_on_non_exists_nodes_several_workers() {
        let config = Config::test("remove_jobs_on_non_exists_nodes_several_workers").update_config(
            |mut config| {
                config.select_workers = vec!["worker1".to_string(), "worker2".to_string()];
                config
            },
        );

        let _ = fs::remove_dir_all(config.local_dir());
        let _ = fs::remove_dir_all(config.remote_dir());

        let services = config.configure().await;
        services.start_processing_loops().await.unwrap();
        let meta_store = services.meta_store.clone();
        let mut existing_ids = Vec::new();
        existing_ids.push(
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 2),
                    JobType::PartitionCompaction,
                    "worker1".to_string(),
                ))
                .await
                .unwrap()
                .unwrap()
                .get_id(),
        );

        existing_ids.push(
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 3),
                    JobType::PartitionCompaction,
                    "worker2".to_string(),
                ))
                .await
                .unwrap()
                .unwrap()
                .get_id(),
        );

        meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::Partitions, 1),
                JobType::PartitionCompaction,
                "not_existis_node".to_string(),
            ))
            .await
            .unwrap();

        meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::Partitions, 4),
                JobType::PartitionCompaction,
                "not_existis_node2".to_string(),
            ))
            .await
            .unwrap();
        existing_ids.sort();
        let all_jobs = meta_store.all_jobs().await.unwrap();
        assert_eq!(all_jobs.len(), 4);
        let scheduler = services.injector.get_service_typed::<SchedulerImpl>().await;
        scheduler.remove_jobs_on_non_exists_nodes().await.unwrap();
        let all_jobs = meta_store.all_jobs().await.unwrap();
        assert_eq!(all_jobs.len(), 2);
        let mut job_ids = all_jobs.into_iter().map(|j| j.get_id()).collect::<Vec<_>>();
        job_ids.sort();
        assert_eq!(job_ids, existing_ids);
        services.stop_processing_loops().await.unwrap();
        let _ = fs::remove_dir_all(config.local_dir());
        let _ = fs::remove_dir_all(config.remote_dir());
    }

    // Creates a non-sealed table with no locations, so the Insert(Tables) event does not
    // auto-schedule imports and the test drives schedule_table_import explicitly.
    async fn create_import_table(meta_store: &Arc<dyn MetaStore>, name: &str) -> u64 {
        use crate::metastore::{Column, ColumnType};
        let table = meta_store
            .create_table(
                "s".to_string(),
                name.to_string(),
                vec![Column::new("n".to_string(), ColumnType::Int, 0)],
                None,
                None,
                vec![],
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
        table.get_id()
    }

    async fn add_import_job(
        meta_store: &Arc<dyn MetaStore>,
        table_id: u64,
        location: &str,
        node: &str,
    ) {
        meta_store
            .add_job(Job::new(
                RowKey::Table(TableId::Tables, table_id),
                JobType::TableImportCSV(location.to_string()),
                node.to_string(),
            ))
            .await
            .unwrap();
    }

    async fn import_placement(
        meta_store: &Arc<dyn MetaStore>,
        table_id: u64,
    ) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for j in meta_store.all_jobs().await.unwrap() {
            match j.get_row().row_reference() {
                RowKey::Table(TableId::Tables, t) if *t == table_id => {}
                _ => continue,
            }
            if let (JobType::TableImportCSV(loc), JobStatus::Scheduled(node)) =
                (j.get_row().job_type(), j.get_row().status())
            {
                out.insert(loc.clone(), node.clone());
            }
        }
        out
    }

    // Builds a scheduler over a fresh test metastore with a no-op cluster (notify_job_runner
    // does nothing, since the worker names are not real nodes). select_workers and the
    // load-aware flag come from the passed config.
    fn test_scheduler(
        test_name: &str,
        config: Arc<dyn ConfigObj>,
    ) -> (Arc<dyn MetaStore>, SchedulerImpl) {
        use crate::cluster::MockCluster;
        let (remote_fs, rocks) = RocksMetaStore::prepare_test_metastore(test_name);
        let meta_store: Arc<dyn MetaStore> = rocks;
        let mut cluster = MockCluster::new();
        cluster.expect_notify_job_runner().returning(|_| Ok(()));
        let (_tx, rx) = broadcast::channel(16);
        let scheduler =
            SchedulerImpl::new(meta_store.clone(), Arc::new(cluster), remote_fs, rx, config);
        (meta_store, scheduler)
    }

    #[tokio::test]
    async fn schedule_table_import_load_aware_balances() {
        let config = Config::test("schedule_table_import_load_aware").update_config(|mut c| {
            c.select_workers = (0..4).map(|i| format!("worker{}", i)).collect();
            c.load_aware_import_placement_enabled = true;
            c
        });
        let (meta_store, scheduler) =
            test_scheduler("schedule_table_import_load_aware", config.config_obj());
        meta_store
            .create_schema("s".to_string(), false)
            .await
            .unwrap();

        // Pre-seed worker0 with 2 in-flight imports. Starting counts [2,0,0,0]; placing 6
        // locations load-aware fills the deficit and levels every worker to exactly 2.
        let seed = create_import_table(&meta_store, "seed").await;
        add_import_job(&meta_store, seed, "seed-a", "worker0").await;
        add_import_job(&meta_store, seed, "seed-b", "worker0").await;

        let table = create_import_table(&meta_store, "bar").await;
        let locs: Vec<String> = (0..6).map(|i| format!("loc-{}", i)).collect();
        let loc_refs: Vec<&String> = locs.iter().collect();
        scheduler
            .schedule_table_import(table, &loc_refs)
            .await
            .unwrap();

        let counts = meta_store.in_flight_import_jobs_by_node().await.unwrap();
        for i in 0..4 {
            assert_eq!(
                counts.get(&format!("worker{}", i)).copied(),
                Some(2),
                "load-aware must level all workers to 2, got {:?}",
                counts
            );
        }

        // cleanup_test_metastore only removes the test-<name>-* dirs; the metastore's local
        // cache also uses the config data_dir (<name>-local-store), so clean that too.
        RocksMetaStore::cleanup_test_metastore("schedule_table_import_load_aware");
        let _ = fs::remove_dir_all(config.local_dir());
        let _ = fs::remove_dir_all(config.remote_dir());
    }

    #[tokio::test]
    async fn schedule_table_import_hash_placement_by_default() {
        let config = Config::test("schedule_table_import_hash").update_config(|mut c| {
            c.select_workers = (0..4).map(|i| format!("worker{}", i)).collect();
            // load_aware_import_placement_enabled defaults to false (old behavior).
            c
        });
        let (meta_store, scheduler) =
            test_scheduler("schedule_table_import_hash", config.config_obj());
        meta_store
            .create_schema("s".to_string(), false)
            .await
            .unwrap();

        // Heavily pre-seed worker0; the default hash placement must ignore in-flight load.
        let seed = create_import_table(&meta_store, "seed").await;
        for i in 0..5 {
            add_import_job(&meta_store, seed, &format!("seed-{}", i), "worker0").await;
        }

        let table = create_import_table(&meta_store, "bar").await;
        let locs: Vec<String> = (0..6).map(|i| format!("loc-{}", i)).collect();
        let loc_refs: Vec<&String> = locs.iter().collect();
        scheduler
            .schedule_table_import(table, &loc_refs)
            .await
            .unwrap();

        // Each location is placed by the pure hash of (table_id, location), independent of load.
        let placement = import_placement(&meta_store, table).await;
        let empty = HashMap::new();
        for l in &locs {
            let expected = pick_import_worker(config.config_obj().as_ref(), table, l, &empty);
            assert_eq!(
                placement.get(l),
                Some(&expected),
                "default placement must follow the stateless hash"
            );
        }

        // cleanup_test_metastore only removes the test-<name>-* dirs; the metastore's local
        // cache also uses the config data_dir (<name>-local-store), so clean that too.
        RocksMetaStore::cleanup_test_metastore("schedule_table_import_hash");
        let _ = fs::remove_dir_all(config.local_dir());
        let _ = fs::remove_dir_all(config.remote_dir());
    }
}
