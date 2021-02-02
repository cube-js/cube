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
#[cfg(target_os = "linux")] // We use `linux` to test for glibc.
pub fn trim_allocs() {
    unsafe {
        malloc_trim(0);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn trim_allocs() {}

#[cfg(target_os = "linux")] // we assume glibc is linked on linux.
extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}
