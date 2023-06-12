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

struct TrackingAllocator {
    allocated: AtomicI64,
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            self.allocated.fetch_add(layout.size() as i64, Relaxed);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        self.allocated.fetch_sub(layout.size() as i64, Relaxed);
    }
}

impl TrackingAllocator {
    pub fn allocated(&self) -> i64 {
        self.allocated.load(Relaxed)
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator {
    allocated: AtomicI64::new(0),
};

pub fn get_allocated_memory() -> i64 {
    ALLOCATOR.allocated()
}
