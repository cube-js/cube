use crate::metastore::MetaStoreEvent;
use crate::CubeError;
use datafusion::cube_ext;
use log::error;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct CacheStoreSchedulerImpl {
    event_receiver: Mutex<Receiver<MetaStoreEvent>>,
    cancel_token: CancellationToken,
}

crate::di_service!(CacheStoreSchedulerImpl, []);

impl CacheStoreSchedulerImpl {
    pub fn new(event_receiver: Receiver<MetaStoreEvent>) -> CacheStoreSchedulerImpl {
        let cancel_token = CancellationToken::new();
        Self {
            event_receiver: Mutex::new(event_receiver),
            cancel_token,
        }
    }

    pub fn spawn_processing_loops(self: Arc<Self>) -> Vec<JoinHandle<Result<(), CubeError>>> {
        vec![cube_ext::spawn(async move {
            self.run_event_processor().await;
            Ok(())
        })]
    }

    async fn run_event_processor(self: Arc<Self>) {
        loop {
            let mut event_receiver = self.event_receiver.lock().await;
            let _ = tokio::select! {
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

            // Right now, it's used to free channel
        }
    }

    pub fn stop_processing_loops(&self) -> Result<(), CubeError> {
        self.cancel_token.cancel();
        Ok(())
    }
}
