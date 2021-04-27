use crate::sys::malloc::{trim_allocs, HAS_TRIM_ALLOC};
use std::time::Duration;

/// See the comment on [trim_alloc] on discussion on why we need it.
pub fn spawn_malloc_trim_loop(period: Duration) {
    if !HAS_TRIM_ALLOC {
        return;
    }

    // We detach the thread, so have to be prepared it gets killed at any point.
    std::thread::spawn(move || loop {
        std::thread::sleep(period);
        trim_allocs()
    });
}
