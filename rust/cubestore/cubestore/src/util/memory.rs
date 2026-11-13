use crate::config::injection::DIService;
use crate::CubeError;

use std::fmt::Debug;

use std::sync::Arc;

pub trait MemoryHandler: DIService + Debug + Send + Sync {
    fn check_memory(&self) -> Result<(), CubeError>;

    /// Heap bytes the process holds now, `None` when the allocator is not tracked.
    fn allocated_bytes(&self) -> Option<u64> {
        None
    }

    /// Highest `allocated_bytes` since the last `reset_peak`.
    fn peak_allocated_bytes(&self) -> Option<u64> {
        None
    }

    fn reset_peak(&self) {}
}

#[derive(Debug)]
pub struct MemoryHandlerImpl;

impl MemoryHandler for MemoryHandlerImpl {
    fn check_memory(&self) -> Result<(), CubeError> {
        Ok(())
    }
}

impl MemoryHandlerImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

crate::di_service!(MemoryHandlerImpl, [MemoryHandler]);
