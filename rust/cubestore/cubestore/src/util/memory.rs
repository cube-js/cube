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

/// Memory limit of the cgroup this process runs in (a container's limit), if one is set.
pub fn cgroup_memory_limit() -> Option<u64> {
    // cgroup v2 reports "max" when unlimited; v1 reports a near-u64::MAX sentinel.
    let v2 = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok();
    let v1 = || std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok();
    parse_cgroup_memory_limit(&v2.or_else(v1)?)
}

fn parse_cgroup_memory_limit(raw: &str) -> Option<u64> {
    const UNLIMITED_THRESHOLD: u64 = 1 << 60;
    raw.trim()
        .parse::<u64>()
        .ok()
        .filter(|&limit| 0 < limit && limit < UNLIMITED_THRESHOLD)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cgroup_memory_limit() {
        assert_eq!(parse_cgroup_memory_limit("8589934592\n"), Some(8589934592));
        assert_eq!(parse_cgroup_memory_limit("max\n"), None);
        assert_eq!(parse_cgroup_memory_limit("9223372036854771712\n"), None);
    }
}
