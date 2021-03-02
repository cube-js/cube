use crate::util::time_span::warn_long;
use std::time::Duration;

/// Ask the memory allocator to returned the freed memory to the system.
/// This only has effect when compiled for glibc, this is a no-op on other systems.
///
/// Cubestore produces allocation patterns that hit the limitations of glibc`s malloc, which results
/// in too many physical memory pages being retained in the allocator's arena. This leads to the
/// resident set size growing over the acceptable limits.
/// Probably related to https://sourceware.org/bugzilla/show_bug.cgi?id=11261.
///
/// Use this function after code that produces considerable amount of memory allocations that
/// **have been already freed**.
#[cfg(all(target_os = "linux", not(target_env = "musl")))] // Musl doesnt support malloc_trim, probably only gnu has it.
pub fn trim_allocs() {
    let _s = warn_long("malloc_trim", Duration::from_millis(100));
    unsafe {
        malloc_trim(0);
    }
}

#[cfg(any(not(target_os = "linux"), target_env = "musl"))]
pub fn trim_allocs() {}

#[cfg(all(target_os = "linux", not(target_env = "musl")))] // Musl doesnt support malloc_trim, probably only gnu has it.
extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}
