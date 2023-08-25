use crate::cluster::ingestion::job_processor::{JobIsolatedProcessor, JobProcessResult};
#[cfg(not(target_os = "windows"))]
use crate::cluster::worker_pool::{worker_main, MessageProcessor, WorkerPool};
use crate::config::Config;
use crate::metastore::job::{Job, JobStatus, JobType};
use crate::CubeError;
use async_trait::async_trait;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Serialize, Deserialize)]
pub enum IngestionWorkerMessage {
    Job(Job),
}

#[cfg(not(target_os = "windows"))]
pub struct IngestionWorkerProcessor;

#[cfg(not(target_os = "windows"))]
#[async_trait]
impl MessageProcessor<IngestionWorkerMessage, JobProcessResult> for IngestionWorkerProcessor {
    async fn process(
        config: &Config,
        args: IngestionWorkerMessage,
    ) -> Result<JobProcessResult, CubeError> {
        let processor = JobIsolatedProcessor::new_from_config(config).await;
        println!("!!!AAAAAAAAAAA!!!!!!");
        match args {
            IngestionWorkerMessage::Job(job) => {
                let processor_to_move = processor.clone();
                let future = async move {
                    let time = SystemTime::now();
                    debug!("Running job in worker started");
                    let res = processor_to_move.process_separate_job(&job).await;
                    debug!(
                        "Running job in worker completed ({:?})",
                        time.elapsed().unwrap()
                    );
                    res
                };
                future.await
            }
        }
    }
}
