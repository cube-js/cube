pub mod maybe_owned;

use crate::CubeError;
use log::error;
use std::future::Future;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

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

    pub fn process<T, S, F, FR>(
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
        tokio::spawn(async move {
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
        });
    }

    pub fn stop(&self) {
        self.stopped_token.cancel()
    }
}
