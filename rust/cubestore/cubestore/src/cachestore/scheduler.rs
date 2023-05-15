use crate::cachestore::CacheStore;
use crate::config::ConfigObj;
use crate::metastore::MetaStoreEvent;
use crate::shared::deadline_queue::DeadlineQueue;
use crate::CubeError;
use datafusion::cube_ext;
use log::error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub enum GCTask {
    DeleteQueue(u64),
}

pub struct CacheStoreSchedulerImpl {
    event_receiver: Mutex<Receiver<MetaStoreEvent>>,
    cancel_token: CancellationToken,
    gc_queue: DeadlineQueue<GCTask>,
    cache_store: Arc<dyn CacheStore>,
    cachestore_queue_results_expire: Duration,
}

crate::di_service!(CacheStoreSchedulerImpl, []);

impl CacheStoreSchedulerImpl {
    pub fn new(
        event_receiver: Receiver<MetaStoreEvent>,
        cache_store: Arc<dyn CacheStore>,
        config: Arc<dyn ConfigObj>,
    ) -> CacheStoreSchedulerImpl {
        let cancel_token = CancellationToken::new();

        Self {
            event_receiver: Mutex::new(event_receiver),
            gc_queue: DeadlineQueue::new(
                config.cachestore_gc_loop_interval(),
                cancel_token.clone(),
            ),
            cachestore_queue_results_expire: Duration::from_secs(
                config.cachestore_queue_results_expire(),
            ),
            cancel_token,
            cache_store,
        }
    }

    pub fn spawn_processing_loops(self: Arc<Self>) -> Vec<JoinHandle<Result<(), CubeError>>> {
        let scheduler1 = self.clone();

        vec![
            cube_ext::spawn(async move {
                scheduler1
                    .gc_queue
                    .run_batching(scheduler1.clone(), async move |s, tasks| {
                        s.process_gc_tasks(tasks).await
                    })
                    .await;
                Ok(())
            }),
            cube_ext::spawn(async move {
                self.run_event_processor().await;
                Ok(())
            }),
        ]
    }

    pub async fn process_gc_tasks(self: Arc<Self>, tasks: Vec<GCTask>) -> Result<(), CubeError> {
        log::trace!("Executing GC tasks: {:?}", tasks);

        let queue_results_to_remove = tasks
            .into_iter()
            .map(|i| match i {
                GCTask::DeleteQueue(id) => id,
            })
            .collect();

        if let Err(e) = self
            .cache_store
            .queue_results_multi_delete(queue_results_to_remove)
            .await
        {
            error!("Error while removing olds queue results: {}", e);
        };

        Ok(())
    }

    async fn run_event_processor(self: Arc<Self>) {
        loop {
            let mut event_receiver = self.event_receiver.lock().await;
            let event: MetaStoreEvent = tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    return;
                }
                event = event_receiver.recv() => {
                    match event {
                        Err(broadcast::error::RecvError::Lagged(messages)) => {
                            error!("Scheduler is lagging on cache store event processing for {} messages", messages);
                            continue;
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            return;
                        },
                        Ok(event) => event,
                    }
                }
            };

            match event {
                MetaStoreEvent::AckQueueItem(event) => {
                    if let Err(e) = self
                        .gc_queue
                        .send(
                            GCTask::DeleteQueue(event.id),
                            Instant::now() + self.cachestore_queue_results_expire,
                        )
                        .await
                    {
                        println!("error while scheduling delete, error: {}", e);
                    };
                }
                _ => {}
            }
        }
    }

    pub fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.cancel_token.cancel();
        Ok(())
    }
}
