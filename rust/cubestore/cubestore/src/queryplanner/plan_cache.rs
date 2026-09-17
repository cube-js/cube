use crate::sql::InlineTables;
use crate::CubeError;
use datafusion::logical_expr::LogicalPlan;
use moka::future::Cache;
use std::future::Future;

/// Identifies a logical plan by everything it is derived from: the statement with its
/// parameters already substituted, the inline tables it may reference, and the version of
/// the table list it was resolved against. Physical state — indexes, partitions, chunks —
/// is deliberately absent: it only enters the plan later, in `choose_index_ext`.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct LogicalPlanCacheKey {
    statement: String,
    inline_tables: InlineTables,
    tables_version: u64,
}

impl LogicalPlanCacheKey {
    pub fn new(statement: String, inline_tables: &InlineTables, tables_version: u64) -> Self {
        Self {
            statement,
            inline_tables: inline_tables.clone(),
            tables_version,
        }
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
        self.cache
            .try_get_with(key, plan)
            .await
            .map_err(|e| (*e).clone())
    }

    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }
}
