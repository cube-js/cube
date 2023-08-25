use crate::config::{Config, ConfigObj};
use crate::metastore::{
    deactivate_table_on_corrupt_data, Chunk, IdRow, MetaStore, MetaStoreEvent, Partition, RowKey,
    TableId,
};
use crate::metastore::job::{Job, JobStatus, JobType};
use crate::import::ImportService;
use crate::store::ChunkDataStore;
use crate::store::compaction::CompactionService;
use tokio_util::sync::CancellationToken;
use crate::queryplanner::trace_data_loaded::DataLoadedSize;
use crate::metastore::table::Table;
use tokio::time::timeout;
use tokio::task::JoinHandle;
use crate::util::aborting_join_handle::AbortingJoinHandle;
use std::time::SystemTime;
use crate::cluster::rate_limiter::{ProcessRateLimiter, TaskType, TraceIndex};
use futures_timer::Delay;
use std::time::Duration;
use crate::CubeError;
use std::sync::Arc;
use log::{debug, error, info, warn};
use tokio::sync::{oneshot, watch, Notify, RwLock};
use core::mem;
use datafusion::cube_ext;
use crate::cluster::ingestion::job_processor::JobProcessor;

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
    pub is_long_term: bool,
    pub job_processor: Arc<JobProcessor>,
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
            .start_processing_job(self.server_name.to_string(), self.is_long_term)
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
        let heart_beat_timer = cube_ext::spawn(async move {
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
            self.meta_store
                .update_status(job_id, JobStatus::Timeout)
                .await?;
            error!(
                "Running job {} ({:?}): {:?}",
                e.message,
                start.elapsed()?,
                // Job can be removed by the time of fetch
                self.meta_store.get_job(job_id).await.unwrap_or(job)
            );
        } else if let Ok(Err(cube_err)) = res {
            self.meta_store
                .update_status(job_id, JobStatus::Error(cube_err.to_string()))
                .await?;
            error!(
                "Running job join error ({:?}): {:?}",
                start.elapsed()?,
                // Job can be removed by the time of fetch
                self.meta_store.get_job(job_id).await.unwrap_or(job)
            );
        } else if let Ok(Ok(Err(cube_err))) = res {
            self.meta_store
                .update_status(job_id, JobStatus::Error(cube_err.to_string()))
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
            // TODO delete jobs on reconciliation
            self.meta_store.delete_job(job_id).await?;
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
                    let compaction_service = self.compaction_service.clone();
                    let partition_id = *partition_id;
                    let process_rate_limiter = self.process_rate_limiter.clone();
                    let timeout = Some(Duration::from_secs(self.config_obj.import_job_timeout()));
                    let metastore = self.meta_store.clone();
                    let job_processor = self.job_processor.clone();
                    let job_to_move = job.clone();
                    Ok(cube_ext::spawn(async move {
                        process_rate_limiter
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

                        let data_loaded_size = DataLoadedSize::new();
                        let res = job_processor.process_job(job_to_move).await;
                        if let Ok(job_res) = res {
                            process_rate_limiter
                                .commit_task_usage(
                                    TaskType::Job,
                                    job_res.data_loaded_size() as i64,
                                    trace_index,
                                )
                                .await;
                        }
                        Ok(())
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
                if let RowKey::Table(TableId::MultiPartitions, id) = job.row_reference() {
                    let compaction_service = self.compaction_service.clone();
                    let id = *id;
                    Ok(cube_ext::spawn(async move {
                        compaction_service.split_multi_partition(id).await
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::FinishMultiSplit => {
                if let RowKey::Table(TableId::MultiPartitions, multi_part_id) = job.row_reference()
                {
                    let meta_store = self.meta_store.clone();
                    let compaction_service = self.compaction_service.clone();
                    let multi_part_id = *multi_part_id;
                    Ok(cube_ext::spawn(async move {
                        for p in meta_store.find_unsplit_partitions(multi_part_id).await? {
                            compaction_service
                                .finish_multi_split(multi_part_id, p)
                                .await?
                        }
                        Ok(())
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::TableImport => {
                if let RowKey::Table(TableId::Tables, table_id) = job.row_reference() {
                    let import_service = self.import_service.clone();
                    let table_id = *table_id;
                    Ok(cube_ext::spawn(async move {
                        import_service.import_table(table_id).await
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
                    Ok(cube_ext::spawn(async move {
                        let is_streaming = Table::is_stream_location(&location);
                        let data_loaded_size = if is_streaming {
                            None
                        } else {
                            Some(DataLoadedSize::new())
                        };
                        if !is_streaming {
                            process_rate_limiter
                                .wait_for_allow(TaskType::Job, timeout)
                                .await?; //TODO config, may be same ad orphaned timeout
                        }
                        let res = import_service
                            .clone()
                            .import_table_part(table_id, &location, data_loaded_size.clone())
                            .await;
                        if let Some(data_loaded) = &data_loaded_size {
                            let trace_obj = metastore.get_trace_obj_by_table_id(table_id).await?;
                            let trace_index = TraceIndex {
                                table_id: Some(table_id),
                                trace_obj,
                            };
                            process_rate_limiter
                                .commit_task_usage(
                                    TaskType::Job,
                                    data_loaded.get() as i64,
                                    trace_index,
                                )
                                .await;
                        }
                        res
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
                    Ok(cube_ext::spawn(async move {
                        process_rate_limiter
                            .wait_for_allow(TaskType::Job, timeout)
                            .await?; //TODO config, may be same ad orphaned timeout
                        let chunk = metastore.get_chunk(chunk_id).await?;
                        let (_, _, table, _) = metastore
                            .get_partition_for_compaction(chunk.get_row().get_partition_id())
                            .await?;
                        let table_id = table.get_id();
                        let trace_obj = metastore.get_trace_obj_by_table_id(table_id).await?;
                        let trace_index = TraceIndex {
                            table_id: Some(table_id),
                            trace_obj,
                        };
                        let data_loaded_size = DataLoadedSize::new();
                        let res = chunk_store
                            .repartition_chunk(chunk_id, data_loaded_size.clone())
                            .await;
                        process_rate_limiter
                            .commit_task_usage(
                                TaskType::Job,
                                data_loaded_size.get() as i64,
                                trace_index,
                            )
                            .await;
                        res
                    }))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
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
