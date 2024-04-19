use crate::config::injection::DIService;
use crate::CubeError;
use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::Debug;
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::sync::Arc;

pub trait MemoryHandler: DIService + Debug + Send + Sync {
    fn check_memory(&self) -> Result<(), CubeError>;
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
