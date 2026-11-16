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

const MIB: usize = 1024 * 1024;

/// Node-wide memory budget that bounds how many index pipelines build chunks at once during
/// import. Each pipeline acquires permits proportional to its estimated working memory; when the
/// budget is exhausted further pipelines wait, so large chunks serialize while small ones still
/// run in parallel. Permits are counted in MiB to stay within the `u32` semaphore range.
#[derive(Clone)]
pub struct IndexMemoryBudget {
    budget: Arc<Semaphore>,
    total_permits: u32,
}

impl IndexMemoryBudget {
    pub fn new(budget_bytes: usize) -> IndexMemoryBudget {
        let total_permits = Self::bytes_to_permits(budget_bytes, u32::MAX);
        IndexMemoryBudget {
            budget: Arc::new(Semaphore::new(total_permits as usize)),
            total_permits,
        }
    }

    /// Acquire a permit sized to `bytes`. Clamped to the whole budget so an index whose estimate
    /// exceeds the budget still runs (alone) instead of deadlocking. The returned guard releases
    /// the permit on drop, including on error.
    pub async fn acquire(&self, bytes: usize) -> Result<OwnedSemaphorePermit, CubeError> {
        let permits = Self::bytes_to_permits(bytes, self.total_permits);
        Ok(self.budget.clone().acquire_many_owned(permits).await?)
    }

    fn bytes_to_permits(bytes: usize, cap: u32) -> u32 {
        let mib = bytes.div_ceil(MIB).max(1);
        mib.min(cap as usize) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Runs `count` index-sized pipelines against `budget`, each acquiring `per_index_bytes`,
    /// and returns the peak observed concurrency.
    async fn peak_concurrency(
        budget: IndexMemoryBudget,
        count: usize,
        per_index_bytes: usize,
    ) -> usize {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let tasks = (0..count).map(|_| {
            let budget = budget.clone();
            let active = active.clone();
            let peak = peak.clone();
            tokio::spawn(async move {
                let _permit = budget.acquire(per_index_bytes).await.unwrap();
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                active.fetch_sub(1, Ordering::SeqCst);
            })
        });
        tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(tasks))
            .await
            .expect("budget acquisition must not deadlock");
        peak.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn serializes_when_budget_is_small() {
        // 1 MiB budget, each index needs 1 MiB -> only one runs at a time.
        let budget = IndexMemoryBudget::new(MIB);
        assert_eq!(peak_concurrency(budget, 4, MIB).await, 1);
    }

    #[tokio::test]
    async fn parallelizes_when_budget_is_large() {
        // 16 MiB budget, each index needs 1 MiB -> all four run together.
        let budget = IndexMemoryBudget::new(16 * MIB);
        assert_eq!(peak_concurrency(budget, 4, MIB).await, 4);
    }

    #[tokio::test]
    async fn oversized_index_runs_alone_without_deadlock() {
        // A single index larger than the whole budget is clamped and still runs.
        let budget = IndexMemoryBudget::new(MIB);
        assert_eq!(peak_concurrency(budget, 1, 64 * MIB).await, 1);
    }
}
