use crate::CubeError;
use futures::TryFutureExt;
use std::future::Future;
use std::time::Duration;
use tracing::Instrument;

pub async fn acquire_lock<F: Future>(name: &str, lock: F) -> Result<F::Output, CubeError> {
    acquire_lock_duration(name, lock, Duration::from_secs(10)).await
}

pub async fn acquire_lock_duration<F: Future>(
    name: &str,
    lock: F,
    duration: Duration,
) -> Result<F::Output, CubeError> {
    tokio::time::timeout(duration, lock)
        .instrument(tracing::span!(tracing::Level::INFO, "Wait for lock", name))
        .map_err(|e| CubeError::internal(format!("Can't acquire {} lock: {}", name, e)))
        .await
}
