use crate::app_metrics;
use crate::sql::InlineTables;
use crate::CubeError;
use datafusion::logical_expr::LogicalPlan;
use moka::future::Cache;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Physical state — indexes, partitions, chunks — is deliberately absent: it only enters the
/// plan later, in `choose_index_ext`.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct LogicalPlanCacheKey {
    statement: String,
    tables_version: u64,
}

impl LogicalPlanCacheKey {
    /// `None` for a query carrying inline tables: their data is not in the key, so two of
    /// them sharing a statement would collide.
    pub fn new(
        statement: String,
        inline_tables: &InlineTables,
        tables_version: u64,
    ) -> Option<Self> {
        if !inline_tables.is_empty() {
            return None;
        }
        Some(Self {
            statement,
            tables_version,
        })
    }
}

pub struct LogicalPlanCache {
    cache: Cache<LogicalPlanCacheKey, LogicalPlan>,
    tables_version: AtomicU64,
}

impl LogicalPlanCache {
    pub fn new(max_entries: u64) -> Self {
        Self {
            cache: Cache::builder().max_capacity(max_entries).build(),
            tables_version: AtomicU64::new(0),
        }
    }

    /// Entries keyed on an earlier table list can never be read again. Dropping them keeps
    /// them from filling the entry budget and evicting the live ones to make room.
    pub fn forget_plans_for_older_tables(&self, tables_version: u64) {
        let previous = self.tables_version.swap(tables_version, Ordering::Relaxed);
        if previous != tables_version {
            self.cache.invalidate_all();
            app_metrics::PLAN_CACHE_INVALIDATED.increment();
        }
    }

    /// Runs `plan` once per key while every other caller for that key waits for it, so a
    /// burst of identical queries arriving on a cold cache plans once rather than N times.
    /// Failures are propagated to all waiters and not cached.
    pub async fn get_or_plan<F>(
        &self,
        key: LogicalPlanCacheKey,
        plan: F,
    ) -> Result<LogicalPlan, CubeError>
    where
        F: Future<Output = Result<LogicalPlan, CubeError>>,
    {
        let entry = self.cache.entry(key).or_try_insert_with(plan).await;

        // A failure is not a cache outcome: it reaches every waiter, and counting those
        // waiters as hits would report plans that were never produced.
        match &entry {
            Ok(entry) if entry.is_fresh() => app_metrics::PLAN_CACHE_MISS.increment(),
            Ok(_) => app_metrics::PLAN_CACHE_HIT.increment(),
            Err(_) => {}
        }
        app_metrics::PLAN_CACHE_SIZE.report(self.cache.entry_count() as i64);

        entry
            .map(|entry| entry.into_value())
            .map_err(|e: Arc<CubeError>| (*e).clone())
    }
}
