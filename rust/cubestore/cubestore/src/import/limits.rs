use crate::CubeError;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Use to limit memory usage on parallel operations. The object has reference semantics, i.e.
/// cloning will share the same underlying limits.
///
/// Example:
/// ```ignore
/// use cubestore::import::limits::ConcurrencyLimits;
/// async fn do_work(limits: &ConcurrencyLimits) {
///     loop // run processing in parallel, but limit the number of active data frames.
///     {
///         let permit = limits.acquire_data_frame().await?;
///         // ... read data frame
///         tokio::spawn(async move || {
///             // ... process data frame
///             std::mem::drop(permit)
///         })
///     }
/// }
/// ```
#[derive(Clone)]
pub struct ConcurrencyLimits {
    active_data_frames: Arc<Semaphore>,
}

crate::di_service!(ConcurrencyLimits, []);

impl ConcurrencyLimits {
    pub fn new(max_data_frames: usize) -> ConcurrencyLimits {
        assert!(1 <= max_data_frames, "no data frames can be processed");
        ConcurrencyLimits {
            active_data_frames: Arc::new(Semaphore::new(max_data_frames)),
        }
    }

    pub async fn acquire_data_frame(&self) -> Result<OwnedSemaphorePermit, CubeError> {
        Ok(self.active_data_frames.clone().acquire_owned().await?)
    }
}
