pub mod aborting_join_handle;
pub mod batch_memory;
pub mod decimal;
pub mod error;
pub mod lock;
pub mod logger;
mod malloc_trim_loop;
pub mod maybe_owned;
pub mod memory;
pub mod metrics;
#[cfg(not(target_os = "windows"))]
pub mod respawn;
pub mod strings;
pub mod time_span;

pub use malloc_trim_loop::spawn_malloc_trim_loop;

use crate::CubeError;
use futures_timer::Delay;
use log::error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct WorkerLoop {
    name: String,
    notify: tokio::sync::Notify,
    stopped_token: CancellationToken,
}

impl WorkerLoop {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            notify: tokio::sync::Notify::new(),
            stopped_token: CancellationToken::new(),
        }
    }

    pub async fn process<T, S, F, FR>(
        &self,
        service: Arc<S>,
        wait_for: impl Fn(Arc<S>) -> F + Send + Sync + 'static,
        loop_fn: impl Fn(Arc<S>, T) -> FR + Send + Sync + 'static,
    ) where
        T: Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Future<Output = Result<T, CubeError>> + Send + 'static,
        FR: Future<Output = Result<(), CubeError>> + Send + 'static,
    {
        let token = self.stopped_token.child_token();
        let loop_name = self.name.clone();

        loop {
            let service_to_move = service.clone();
            let res = tokio::select! {
                _ = token.cancelled() => {
                    return;
                }
                _ = self.notify.notified() => {
                    return;
                }
                res = wait_for(service_to_move) => {
                    res
                }
            };
            match res {
                Ok(r) => {
                    let loop_res = loop_fn(service.clone(), r).await;
                    if let Err(e) = loop_res {
                        error!("Error during {}: {:?}", loop_name, e);
                    }
                }
                Err(e) => {
                    error!("Error during {}: {:?}", loop_name, e);
                }
            };
        }
    }

    pub async fn process_channel<T, S: ?Sized, FR>(
        &self,
        service: Arc<S>,
        rx: &mut mpsc::Receiver<T>,
        loop_fn: impl Fn(Arc<S>, T) -> FR + Send + Sync + 'static,
    ) where
        T: Send + Sync + 'static,
        S: Send + Sync + 'static,
        FR: Future<Output = Result<(), CubeError>> + Send + 'static,
    {
        let token = self.stopped_token.child_token();
        let loop_name = self.name.clone();

        loop {
            let res = tokio::select! {
                _ = token.cancelled() => {
                    return;
                }
                _ = self.notify.notified() => {
                    return;
                }
                res = rx.recv() => {
                    res
                }
            };
            match res {
                Some(r) => {
                    let loop_res = loop_fn(service.clone(), r).await;
                    if let Err(e) = loop_res {
                        error!("Error during {}: {:?}", loop_name, e);
                    }
                }
                None => {
                    return;
                }
            };
        }
    }

    /// Trigger process is a method which allows to force processing without waiting for delay fn
    pub fn trigger_process(&self) {
        self.notify.notify_waiters()
    }

    pub fn stop(&self) {
        self.stopped_token.cancel()
    }
}
