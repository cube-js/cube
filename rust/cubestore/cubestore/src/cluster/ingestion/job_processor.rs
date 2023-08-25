#[cfg(not(target_os = "windows"))]
use crate::cluster::ingestion::worker::{IngestionWorkerMessage, IngestionWorkerProcessor};
#[cfg(not(target_os = "windows"))]
use crate::cluster::worker_pool::{worker_main, MessageProcessor, WorkerPool};
use crate::config::{Config, ConfigObj};
use crate::import::ImportService;
use crate::metastore::job::{Job, JobStatus, JobType};
use crate::metastore::table::Table;
use crate::metastore::{MetaStore, RowKey, TableId};
use crate::queryplanner::trace_data_loaded::DataLoadedSize;
use crate::store::compaction::CompactionService;
use crate::CubeError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobProcessResult {
    data_loaded_size: usize,
}

impl JobProcessResult {
    pub fn new(data_loaded_size: usize) -> Self {
        Self { data_loaded_size }
    }

    pub fn data_loaded_size(&self) -> usize {
        self.data_loaded_size
    }
}

impl Default for JobProcessResult {
    fn default() -> Self {
        Self {
            data_loaded_size: 0,
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub struct JobProcessor {
    process_pool:
        Arc<WorkerPool<IngestionWorkerMessage, JobProcessResult, IngestionWorkerProcessor>>,
}

#[cfg(not(target_os = "windows"))]
impl JobProcessor {
    pub fn new(pool_size: usize, timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            process_pool: Arc::new(WorkerPool::new(pool_size, timeout)),
        })
    }
    pub async fn wait_processing_loops(&self) {
        self.process_pool.wait_processing_loops().await
    }
    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.process_pool.stop_workers().await
    }
    pub async fn process_job(&self, job: Job) -> Result<JobProcessResult, CubeError> {
        println!("^^^^^^^ ");
        self.process_pool.process(IngestionWorkerMessage::Job(job)).await
    }
}

//TODO
#[cfg(target_os = "windows")]
pub struct JobProcessor {
    process_pool:
        Arc<WorkerPool<IngestionWorkerMessage, JobProcessResult, IngestionWorkerProcessor>>,
}

#[cfg(target_os = "windows")]
impl JobProcessor {
    pub fn new(pool_size: usize, timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            process_pool: Arc::new(WorkerPool::new(pool_size, timeout)),
        })
    }
    pub async fn wait_processing_loops(&self) {
        self.process_pool.wait_processing_loops().await
    }
    pub async fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.process_pool.stop_workers().await
    }
    pub async fn process_job(&self, job: Job) -> Result<JobProcessResult, CubeError> {
        self.process_pool.process(IngestionWorkerMessage::Job(job)).await
    }
}

pub struct JobIsolatedProcessor {
    config_obj: Arc<dyn ConfigObj>,
    meta_store: Arc<dyn MetaStore>,
    compaction_service: Arc<dyn CompactionService>,
    import_service: Arc<dyn ImportService>,
}

impl JobIsolatedProcessor {
    pub fn new(
        config_obj: Arc<dyn ConfigObj>,
        meta_store: Arc<dyn MetaStore>,
        compaction_service: Arc<dyn CompactionService>,
        import_service: Arc<dyn ImportService>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config_obj,
            meta_store,
            compaction_service,
            import_service,
        })
    }

    pub async fn new_from_config(config: &Config) -> Arc<Self> {
        Self::new(
            config.config_obj(),
            config.injector().get_service_typed().await,
            config.injector().get_service_typed().await,
            config.injector().get_service_typed().await,
        )
    }

    pub async fn process_separate_job(&self, job: &Job) -> Result<JobProcessResult, CubeError> {
        match job.job_type() {
            JobType::PartitionCompaction => {
                if let RowKey::Table(TableId::Partitions, partition_id) = job.row_reference() {
                    let compaction_service = self.compaction_service.clone();
                    let partition_id = *partition_id;
                    let data_loaded_size = DataLoadedSize::new();
                    compaction_service
                        .compact(partition_id, data_loaded_size.clone())
                        .await?;
                    Ok(JobProcessResult::new(data_loaded_size.get()))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::MultiPartitionSplit => {
                if let RowKey::Table(TableId::MultiPartitions, id) = job.row_reference() {
                    let compaction_service = self.compaction_service.clone();
                    let id = *id;
                    compaction_service.split_multi_partition(id).await?;
                    Ok(JobProcessResult::default())
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
                    for p in meta_store.find_unsplit_partitions(multi_part_id).await? {
                        compaction_service
                            .finish_multi_split(multi_part_id, p)
                            .await?
                    }

                    Ok(JobProcessResult::default())
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::TableImport => {
                if let RowKey::Table(TableId::Tables, table_id) = job.row_reference() {
                    let import_service = self.import_service.clone();
                    let table_id = *table_id;
                    import_service.import_table(table_id).await?;
                    Ok(JobProcessResult::default())
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            JobType::TableImportCSV(location) => {
                if Table::is_stream_location(&location) {
                    return Err(CubeError::internal(
                        "Streaming import cannot be processed in separate process".to_string(),
                    ));
                }
                if let RowKey::Table(TableId::Tables, table_id) = job.row_reference() {
                    let table_id = *table_id;
                    let import_service = self.import_service.clone();
                    let location = location.to_string();
                    let data_loaded_size = Some(DataLoadedSize::new());
                    import_service
                        .clone()
                        .import_table_part(table_id, &location, data_loaded_size.clone())
                        .await?;
                    Ok(JobProcessResult::new(
                        data_loaded_size.map_or(0, |d| d.get()),
                    ))
                } else {
                    Self::fail_job_row_key(job)
                }
            }
            _ => Err(CubeError::internal(format!(
                "Job {:?} cannot be processed in separate process",
                job.job_type()
            ))),
        }
    }

    fn fail_job_row_key(job: &Job) -> Result<JobProcessResult, CubeError> {
        Err(CubeError::internal(format!(
            "Incorrect row key for {:?}: {:?}",
            job,
            job.row_reference()
        )))
    }
}
