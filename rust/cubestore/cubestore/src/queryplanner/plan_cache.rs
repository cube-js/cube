use crate::app_metrics;
use crate::sql::InlineTables;
use crate::CubeError;
use datafusion::logical_expr::LogicalPlan;
use moka::future::Cache;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Identifies a logical plan by everything it is derived from: the statement with its
/// parameters already substituted, and the version of the table list it was resolved
/// against. Physical state — indexes, partitions, chunks — is deliberately absent: it only
/// enters the plan later, in `choose_index_ext`. Queries carrying inline tables are not
/// cached at all, so their data never has to appear here.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct LogicalPlanCacheKey {
    statement: String,
    tables_version: u64,
}

impl LogicalPlanCacheKey {
    /// `None` for a query carrying inline tables. Their data is part of what the plan is
    /// built from but is deliberately not part of the key, so there must be no way to build
    /// one for such a query: two of them sharing a statement would collide.
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
}

impl LogicalPlanCache {
    pub fn new(max_entries: u64) -> Self {
        Self {
            cache: Cache::builder().max_capacity(max_entries).build(),
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
        let planned = Arc::new(AtomicBool::new(false));
        let planned_here = planned.clone();
        let result = self
            .cache
            .try_get_with(key, async move {
                planned_here.store(true, Ordering::Relaxed);
                plan.await
            })
            .await
            .map_err(|e| (*e).clone());

        if planned.load(Ordering::Relaxed) {
            app_metrics::PLAN_CACHE_MISS.increment();
        } else {
            app_metrics::PLAN_CACHE_HIT.increment();
        }
        app_metrics::PLAN_CACHE_SIZE.report(self.cache.entry_count() as i64);

        result
    }
}
