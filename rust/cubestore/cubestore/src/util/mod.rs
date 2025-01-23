pub mod aborting_join_handle;
pub mod batch_memory;
pub mod cancellation_token_guard;
pub mod decimal;
pub mod error;
pub mod int96;
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
use log::error;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct WorkerLoop {
    name: String,
    stopped_token: CancellationToken,
}

impl WorkerLoop {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            stopped_token: CancellationToken::new(),
        }
    }

    pub async fn process<WFR, S, F, FR>(
        &self,
        service: Arc<S>,
        wait_for: impl Fn(Arc<S>) -> F + Send + Sync + 'static,
        loop_fn: impl Fn(Arc<S>, WFR) -> FR + Send + Sync + 'static,
    ) where
        WFR: Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Future<Output = Result<WFR, CubeError>> + Send + 'static,
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

    pub fn stop(&self) {
        self.stopped_token.cancel()
    }
}

#[derive(Debug)]
pub struct IntervalLoop {
    name: String,
    interval: tokio::time::Duration,
    notify: tokio::sync::Notify,
    stopped_token: CancellationToken,
}

impl IntervalLoop {
    pub fn new(name: &str, interval: tokio::time::Duration) -> Self {
        Self {
            name: name.to_string(),
            interval,
            notify: tokio::sync::Notify::new(),
            stopped_token: CancellationToken::new(),
        }
    }

    pub async fn process<S, FR>(
        &self,
        service: Arc<S>,
        loop_fn: impl Fn(Arc<S>) -> FR + Send + Sync + 'static,
    ) where
        S: Send + Sync + 'static,
        FR: Future<Output = Result<(), CubeError>> + Send + 'static,
    {
        let token = self.stopped_token.child_token();

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    return;
                },
                _ = self.notify.notified() => {
                    ()
                },
                _ = tokio::time::sleep(self.interval.clone()) => {
                    ()
                }
            };

            if let Err(e) = loop_fn(service.clone()).await {
                error!("Error during {}: {:?}", self.name, e);
            }
        }
    }

    /// Method which allows to force processing without waiting for delay fn
    pub fn trigger_process(&self) {
        self.notify.notify_one()
    }

    pub fn stop(&self) {
        self.stopped_token.cancel()
    }
}

pub fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
    std::fs::create_dir_all(&dst)?;

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CubeError;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_interval_loop_trigger_process() -> Result<(), CubeError> {
        struct TestService {
            counter: tokio::sync::Mutex<u32>,
        }

        impl TestService {
            pub async fn tick(&self) {
                let mut guard = self.counter.lock().await;
                *guard += 1;
            }

            pub async fn get_counter(&self) -> u32 {
                let guard = self.counter.lock().await;
                *guard
            }
        }

        let wl = Arc::new(IntervalLoop::new("test", Duration::from_secs(100000)));
        let service = Arc::new(TestService {
            counter: Default::default(),
        });

        let wl_to_move = wl.clone();
        let service_to_move = service.clone();

        tokio::spawn(async move {
            wl_to_move
                .process(service_to_move, async move |m| {
                    m.tick().await;

                    Ok(())
                })
                .await;
        });

        wl.trigger_process();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert_eq!(service.get_counter().await, 1);

        wl.trigger_process();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert_eq!(service.get_counter().await, 2);

        Ok(())
    }
}
