use crate::app_metrics;
use crate::cluster::ingestion::job_processor::JobProcessor;
use crate::cluster::rate_limiter::{ProcessRateLimiter, TaskType, TraceIndex};
use crate::config::ConfigObj;
use crate::import::ImportService;
use crate::metastore::job::{Job, JobRunnerPool, JobStatus, JobType};
use crate::metastore::table::Table;
use crate::metastore::{IdRow, MetaStore, RowKey, TableId};
use crate::queryplanner::trace_data_loaded::DataLoadedSize;
use crate::store::compaction::CompactionService;
use crate::store::ChunkDataStore;
use crate::util::aborting_join_handle::AbortingJoinHandle;
use crate::util::metrics;
use crate::CubeError;
use core::mem;
use datafusion::cube_ext;
use futures_timer::Delay;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

pub struct JobRunner {
    pub config_obj: Arc<dyn ConfigObj>,
    pub meta_store: Arc<dyn MetaStore>,
    pub chunk_store: Arc<dyn ChunkDataStore>,
    pub compaction_service: Arc<dyn CompactionService>,
    pub import_service: Arc<dyn ImportService>,
    pub process_rate_limiter: Arc<dyn ProcessRateLimiter>,
    pub server_name: String,
    pub notify: Arc<Notify>,
    pub stop_token: CancellationToken,
    pub pool: JobRunnerPool,
    pub job_processor: Arc<dyn JobProcessor>,
}

impl JobRunner {
    pub async fn processing_loop(&self) {
        loop {
            let res = tokio::select! {
                _ = self.stop_token.cancelled() => {
                    let _ = self.job_processor.stop_processing_loops().await;
                    return;
                }
                _ = self.notify.notified() => {
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
            .start_processing_job(self.server_name.to_string(), self.pool)
            .await?;
        if let Some(to_process) = job {
            self.run_local(to_process).await?;
            // In case of job queue is in place jump to the next job immediately
            self.notify.notify_one();
        }
        Ok(())
    }

    fn job_timeout(&self, job: &IdRow<Job>) -> Option<Duration> {
        if let JobType::TableImportCSV(location) = job.get_row().job_type() {
            if Table::is_stream_location(location) {
                return None;
            }
        }
        Some(Duration::from_secs(self.config_obj.import_job_timeout()))
    }

    async fn run_local(&self, job: IdRow<Job>) -> Result<(), CubeError> {
        let start = SystemTime::now();
        let job_id = job.get_id();
        let (mut tx, rx) = oneshot::channel::<()>();
        let meta_store = self.meta_store.clone();
        let fenced_attempt = self
            .retries_import(job.get_row())
            .then(|| job.get_row().attempt());
        let heart_beat_timer = cube_ext::spawn(async move {
            loop {
                tokio::select! {
                    _ = tx.closed() => {
                        break;
                    }
                    _ = Delay::new(Duration::from_secs(30)) => {
                        // A superseded attempt must not keep its successor looking alive.
                        if let Some(attempt) = fenced_attempt {
                            if let Ok(false) = meta_store.update_import_heart_beat(job_id, attempt).await {
                                break;
                            }
                        } else {
                            let _ = meta_store.update_heart_beat(job_id).await; // TODO handle result
                        }
                    }
                }
            }
        });
        debug!("Running job: {:?}", job);
        let handle = AbortingJoinHandle::new(self.route_job(job.get_row())?);
        // TODO cancel job if this worker isn't job owner anymore
        let res = if let Some(duration) = self.job_timeout(&job) {
            let future = timeout(duration, handle);
            // TODO duplicate
            tokio::select! {
                _ = self.stop_token.cancelled() => {
                    Err(CubeError::user("shutting down".to_string()))
                }
                res = future => {
                    res.map_err(|_| CubeError::user("timed out".to_string()))
                }
            }
        } else {
            // TODO duplicate
            tokio::select! {
                _ = self.stop_token.cancelled() => {
                    Err(CubeError::user("shutting down".to_string()))
                }
                res = handle => {
                    Ok(res)
                }
            }
        };

        mem::drop(rx);
        heart_beat_timer.await?;
        if let Err(e) = res {
            self.record_failure(&job, JobStatus::Timeout).await?;
            error!(
                "Running job {} ({:?}): {:?}",
                e.message,
                start.elapsed()?,
                // Job can be removed by the time of fetch
                self.meta_store.get_job(job_id).await.unwrap_or(job)
            );
        } else if let Ok(Err(cube_err)) = res {
            self.record_failure(&job, JobStatus::Error(cube_err.to_string()))
                .await?;
            error!(
                "Running job join error ({:?}): {:?}",
                start.elapsed()?,
                // Job can be removed by the time of fetch
                self.meta_store.get_job(job_id).await.unwrap_or(job)
            );
        } else if let Ok(Ok(Err(cube_err))) = res {
            self.record_failure(&job, JobStatus::Error(cube_err.to_string()))
                .await?;
            error!(
                "Error while running job {}: {}",
                job_id,
                cube_err.display_with_backtrace()
            );
            error!(
                "Running job error ({:?}): {:?}",
                start.elapsed()?,
                // Job can be removed by the time of fetch
                self.meta_store.get_job(job_id).await.unwrap_or(job)
            );
        } else {
            let job = self
                .meta_store
                .update_status(job_id, JobStatus::Completed)
                .await?;
            info!("Running job completed ({:?}): {:?}", start.elapsed()?, job);
            if job.get_row().is_csv_import() && job.get_row().attempt() > 0 {
                app_metrics::JOBS_CSV_IMPORT_RECOVERED.add(1);
            }
            // TODO delete jobs on reconciliation
            self.meta_store.delete_job(job_id).await?;
        }
        Ok(())
    }

    fn retries_import(&self, job: &Job) -> bool {
        job.is_csv_import() && self.config_obj.import_job_retry_attempts() > 0
    }

    async fn record_failure(&self, job: &IdRow<Job>, status: JobStatus) -> Result<(), CubeError> {
        let row = job.get_row();
        // Must match the condition under which the import deferred its chunk activation: only
        // an attempt that left no active rows behind may be retried.
        if !self.retries_import(row) {
            self.meta_store.update_status(job.get_id(), status).await?;
            return Ok(());
        }
        let reason = if status == JobStatus::Timeout {
            "timeout"
        } else {
            "error"
        };
        let updated = match self
            .meta_store
            .fail_import_job(
                row.row_reference().clone(),
                row.job_type().clone(),
                row.attempt(),
                status.clone(),
            )
            .await
        {
            Ok(updated) => updated,
            Err(e) => {
                // A main node that predates the retry RPC rejects it, so fail the job with the RPCs
                // it knows. The error may also be a lost reply to a requeue that did land.
                let current = self.meta_store.get_job(job.get_id()).await?;
                let still_running = current.get_row().status() == row.status()
                    && current.get_row().attempt() == row.attempt();
                if !still_running {
                    info!(
                        "Import job {} attempt {} is superseded, its result is discarded",
                        job.get_id(),
                        row.attempt()
                    );
                    return Ok(());
                }
                warn!(
                    "Failing import job {} without retry: {}",
                    job.get_id(),
                    e.display_with_backtrace()
                );
                self.meta_store.update_status(job.get_id(), status).await?;
                app_metrics::JOBS_CSV_IMPORT_EXHAUSTED
                    .add_with_tags(1, Some(&vec![metrics::format_tag("reason", "rpc_error")]));
                return Ok(());
            }
        };
        match updated.as_ref().map(|j| j.get_row().status()) {
            Some(JobStatus::Scheduled(node)) => {
                app_metrics::JOBS_CSV_IMPORT_RETRIED
                    .add_with_tags(1, Some(&vec![metrics::format_tag("reason", reason)]));
                info!(
                    "Import job {} attempt {} failed on {}, retrying on {}",
                    job.get_id(),
                    row.attempt(),
                    self.server_name,
                    node
                )
            }
            Some(_) => app_metrics::JOBS_CSV_IMPORT_EXHAUSTED
                .add_with_tags(1, Some(&vec![metrics::format_tag("reason", reason)])),
            None => {
                let current = self.meta_store.get_job(job.get_id()).await.ok();
                let committed = current.map_or(false, |j| {
                    j.get_row().status() == &JobStatus::Completed
                        && j.get_row().attempt() == row.attempt()
                });
                if committed {
                    // The commit landed, yet a later step (or a timeout racing it) reported failure.
                    info!(
                        "Import job {} attempt {} failed after its commit, its rows are kept",
                        job.get_id(),
                        row.attempt()
                    );
                    if row.attempt() > 0 {
                        app_metrics::JOBS_CSV_IMPORT_RECOVERED.add(1);
                    }
                    self.meta_store.delete_job(job.get_id()).await?;
                } else {
                    info!(
                        "Import job {} attempt {} is superseded, its result is discarded",
                        job.get_id(),
                        row.attempt()
                    );
                }
            }
        }
        Ok(())
    }

    fn route_job(&self, job: &Job) -> Result<JoinHandle<Result<(), CubeError>>, CubeError> {
        // spawn here is required in case there's a panic in a job. If job panics worker process loop will survive it.
        match job.job_type() {
            JobType::WalPartitioning => {
                if let RowKey::Table(TableId::WALs, wal_id) = job.row_reference() {
                    let chunk_store = self.chunk_store.clone();
                    let wal_id = *wal_id;
                    Ok(cube_ext::spawn(async move {
                        chunk_store.partition(wal_id).await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::Repartition => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let chunk_store = self.chunk_store.clone();
                    let partition_id = *partition_id;
                    Ok(cube_ext::spawn(async move {
                        chunk_store.repartition(partition_id).await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::PartitionCompaction => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let partition_id = *partition_id;
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_processor = self.job_processor.clone();
                    let job_to_move = job.clone();
                    Ok(cube_ext::spawn(async move {
                        let wait_ms = process_rate_limiter
                            .wait_for_allow(TaskType::Job, timeout)
                            .await?; //TODO config, may be same ad orphaned timeout

                        let (_, _, table, _) =
                            metastore.get_partition_for_compaction(partition_id).await?;
                        let table_id = table.get_id();
                        let trace_obj = metastore.get_trace_obj_by_table_id(table_id).await?;
                        let trace_index = TraceIndex {
                            table_id: Some(table_id),
                            trace_obj,
                        };

                        match job_processor.process_job(job_to_move).await {
                            Ok(job_res) => {
                                process_rate_limiter
                                    .commit_task_usage(
                                        TaskType::Job,
                                        job_res.data_loaded_size() as i64,
                                        wait_ms,
                                        trace_index,
                                    )
                                    .await;
                                Ok(())
                            }
                            Err(e) => Err(e),
                        }
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::InMemoryChunksCompaction => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let compaction_service = self.compaction_service.clone();
                    let partition_id = *partition_id;
                    log::warn!(
                        "JobType::InMemoryChunksCompaction is deprecated and should not be used"
                    );
                    Ok(cube_ext::spawn(async move {
                        compaction_service
                            .compact_in_memory_chunks(partition_id)
                            .await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::NodeInMemoryChunksCompaction(_) => {
                if let RowKey::Table(TableId::Tables, _) = job.row_reference() {
                    let compaction_service = self.compaction_service.clone();
                    let node_name = self.server_name.clone();
                    Ok(cube_ext::spawn(async move {
                        compaction_service
                            .compact_node_in_memory_chunks(node_name)
                            .await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::MultiPartitionSplit => {
                if let RowKey::Table(TableId::MultiPartitions, _) = job.row_reference() {
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(cube_ext::spawn(async move {
                        job_processor.process_job(job_to_move).await.map(|_| ())
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::FinishMultiSplit => {
                if let RowKey::Table(TableId::MultiPartitions, _) = job.row_reference() {
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(cube_ext::spawn(async move {
                        job_processor.process_job(job_to_move).await.map(|_| ())
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::TableImport => {
                if let RowKey::Table(TableId::Tables, _) = job.row_reference() {
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(cube_ext::spawn(async move {
                        job_processor.process_job(job_to_move).await.map(|_| ())
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::TableImportCSV(location) => {
                if let RowKey::Table(TableId::Tables, table_id) = job.row_reference() {
                    let table_id = *table_id;
                    let import_service = self.import_service.clone();
                    let location = location.to_string();
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(cube_ext::spawn(async move {
                        let is_streaming = Table::is_stream_location(&location);
                        let data_loaded_size = if is_streaming {
                            None
                        } else {
                            Some(DataLoadedSize::new())
                        };
                        if !is_streaming {
                            let wait_ms = process_rate_limiter
                                .wait_for_allow(TaskType::Job, timeout)
                                .await?; //TODO config, may be same ad orphaned timeout
                            match job_processor.process_job(job_to_move).await {
                                Ok(job_res) => {
                                    let trace_obj =
                                        metastore.get_trace_obj_by_table_id(table_id).await?;
                                    let trace_index = TraceIndex {
                                        table_id: Some(table_id),
                                        trace_obj,
                                    };
                                    process_rate_limiter
                                        .commit_task_usage(
                                            TaskType::Job,
                                            job_res.data_loaded_size() as i64,
                                            wait_ms,
                                            trace_index,
                                        )
                                        .await;
                                    Ok(())
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            import_service
                                .clone()
                                .import_table_part(
                                    table_id,
                                    &location,
                                    data_loaded_size.clone(),
                                    None,
                                )
                                .await
                        }
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::RepartitionChunk => {
                if let RowKey::Table(TableId::Chunks, chunk_id) = job.row_reference() {
                    let chunk_store = self.chunk_store.clone();
                    let chunk_id = *chunk_id;
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(cube_ext::spawn(async move {
                        let wait_ms = process_rate_limiter
                            .wait_for_allow(TaskType::Job, timeout)
                            .await?; //TODO config, may be same ad orphaned timeout
                        let chunk = metastore.get_chunk(chunk_id).await?;
                        if !chunk.get_row().in_memory() {
                            let (_, _, table, _) = metastore
                                .get_partition_for_compaction(chunk.get_row().get_partition_id())
                                .await?;
                            let table_id = table.get_id();
                            let trace_obj = metastore.get_trace_obj_by_table_id(table_id).await?;
                            let trace_index = TraceIndex {
                                table_id: Some(table_id),
                                trace_obj,
                            };
                            match job_processor.process_job(job_to_move).await {
                                Ok(job_res) => {
                                    process_rate_limiter
                                        .commit_task_usage(
                                            TaskType::Job,
                                            job_res.data_loaded_size() as i64,
                                            wait_ms,
                                            trace_index,
                                        )
                                        .await;
                                    Ok(())
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            chunk_store
                                .repartition_chunk(chunk_id, DataLoadedSize::new())
                                .await
                        }
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::RepartitionRange(_) => {
                if let RowKey::Table(TableId::Chunks, start_chunk_id) = job.row_reference() {
                    let start_chunk_id = *start_chunk_id;
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_to_move = job.clone();
                    let job_processor = self.job_processor.clone();
                    Ok(cube_ext::spawn(async move {
                        let wait_ms = process_rate_limiter
                            .wait_for_allow(TaskType::Job, timeout)
                            .await?;
                        let chunk = metastore.get_chunk(start_chunk_id).await?;
                        let (_, _, table, _) = metastore
                            .get_partition_for_compaction(chunk.get_row().get_partition_id())
                            .await?;
                        let table_id = table.get_id();
                        let trace_obj = metastore.get_trace_obj_by_table_id(table_id).await?;
                        let trace_index = TraceIndex {
                            table_id: Some(table_id),
                            trace_obj,
                        };
                        match job_processor.process_job(job_to_move).await {
                            Ok(job_res) => {
                                process_rate_limiter
                                    .commit_task_usage(
                                        TaskType::Job,
                                        job_res.data_loaded_size() as i64,
                                        wait_ms,
                                        trace_index,
                                    )
                                    .await;
                                Ok(())
                            }
                            Err(e) => Err(e),
                        }
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            // Defense-in-depth: start_processing_job never selects an Unknown job, so
            // this arm is not a live path — it just guarantees we never panic on one.
            JobType::Unknown => Err(CubeError::internal(format!(
                "Unknown job type, likely written by a newer CubeStore version: {:?}",
                job
            ))),
        }
    }

    fn fail_job_row_key(job: &Job) -> Result<JoinHandle<Result<(), CubeError>>, CubeError> {
        Err(CubeError::internal(format!(
            "Incorrect row key for {:?}: {:?}",
            job,
            job.row_reference()
        )))
    }
}
