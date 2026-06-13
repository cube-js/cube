use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct TrackingAllocator {
    inner: System,
    allocations: AtomicUsize,
    deallocations: AtomicUsize,
    reallocations: AtomicUsize,
    current_allocated: AtomicUsize,
    peak_allocated: AtomicUsize,
    total_allocated: AtomicUsize,
}

impl TrackingAllocator {
    pub const fn new() -> Self {
        Self {
            inner: System,
            allocations: AtomicUsize::new(0),
            deallocations: AtomicUsize::new(0),
            reallocations: AtomicUsize::new(0),
            current_allocated: AtomicUsize::new(0),
            peak_allocated: AtomicUsize::new(0),
            total_allocated: AtomicUsize::new(0),
        }
    }

    pub fn reset_stats(&self) {
        self.allocations.store(0, Ordering::Relaxed);
        self.deallocations.store(0, Ordering::Relaxed);
        self.reallocations.store(0, Ordering::Relaxed);
        self.current_allocated.store(0, Ordering::Relaxed);
        self.peak_allocated.store(0, Ordering::Relaxed);
        self.total_allocated.store(0, Ordering::Relaxed);
    }

    pub fn print_stats(&self) {
        let allocations = self.allocations.load(Ordering::Relaxed);
        let deallocations = self.deallocations.load(Ordering::Relaxed);
        let reallocations = self.reallocations.load(Ordering::Relaxed);
        let current_allocated = self.current_allocated.load(Ordering::Relaxed);
        let peak_allocated = self.peak_allocated.load(Ordering::Relaxed);
        let total_allocated = self.total_allocated.load(Ordering::Relaxed);

        println!("=== FINAL MEMORY STATISTICS ===");
        println!("Total allocations: {}", allocations);
        println!("Total deallocations: {}", deallocations);
        println!("Total reallocations: {}", reallocations);
        println!(
            "Current allocated: {} bytes ({:.2} MB)",
            current_allocated,
            current_allocated as f64 / 1024.0 / 1024.0
        );
        println!(
            "Peak allocated: {} bytes ({:.2} MB)",
            peak_allocated,
            peak_allocated as f64 / 1024.0 / 1024.0
        );
        println!(
            "Total allocated: {} bytes ({:.2} MB)",
            total_allocated,
            total_allocated as f64 / 1024.0 / 1024.0
        );
        println!("===============================");
    }

    fn update_allocated(&self, size: usize, is_allocation: bool) {
        if is_allocation {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            self.total_allocated.fetch_add(size, Ordering::Relaxed);
            let current = self.current_allocated.fetch_add(size, Ordering::Relaxed) + size;

            // Update peak if current exceeds it
            let mut peak = self.peak_allocated.load(Ordering::Relaxed);
            while current > peak {
                match self.peak_allocated.compare_exchange_weak(
                    peak,
                    current,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(new_peak) => peak = new_peak,
                }
            }
        } else {
            self.deallocations.fetch_add(1, Ordering::Relaxed);
            // Use saturating_sub to prevent underflow
            let current = self.current_allocated.load(Ordering::Relaxed);
            let new_current = current.saturating_sub(size);
            self.current_allocated.store(new_current, Ordering::Relaxed);
        }
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc(layout);
        if !ptr.is_null() {
            self.update_allocated(layout.size(), true);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.inner.dealloc(ptr, layout);
        self.update_allocated(layout.size(), false);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = self.inner.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            self.reallocations.fetch_add(1, Ordering::Relaxed);

            // Update counters: subtract old size, add new size
            let current = self.current_allocated.load(Ordering::Relaxed);
            let after_sub = current.saturating_sub(layout.size());
            self.current_allocated.store(after_sub, Ordering::Relaxed);
            self.total_allocated.fetch_add(new_size, Ordering::Relaxed);
            let current = self
                .current_allocated
                .fetch_add(new_size, Ordering::Relaxed)
                + new_size;

            // Update peak if current exceeds it
            let mut peak = self.peak_allocated.load(Ordering::Relaxed);
            while current > peak {
                match self.peak_allocated.compare_exchange_weak(
                    peak,
                    current,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(new_peak) => peak = new_peak,
                }
            }
        }
        new_ptr
    }
}
