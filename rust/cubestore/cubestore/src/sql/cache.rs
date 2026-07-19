use crate::metastore::{table::Table, IdRow};
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::sql::InlineTables;
use crate::sql::SqlQueryContext;
use crate::store::DataFrame;
use crate::{app_metrics, CubeError};
use deepsize::DeepSizeOf;
use futures::Future;
use log::trace;
use moka::future::{Cache, ConcurrentCacheExt, Iter};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, Mutex};

#[derive(Clone, Hash, Eq, PartialEq, Debug, DeepSizeOf)]
pub struct SqlResultCacheKey {
    query: String,
    inline_tables: InlineTables,
    partition_ids: Vec<u64>,
    chunk_ids: Vec<u64>,
}

impl SqlResultCacheKey {
    pub fn get_query(&self) -> &String {
        &self.query
    }

    pub fn from_plan(query: &str, inline_tables: &InlineTables, plan: &SerializedPlan) -> Self {
        let mut partition_ids = HashSet::new();
        let mut chunk_ids = HashSet::new();
        for index in plan.index_snapshots().iter() {
            for p in index.partitions.iter() {
                partition_ids.insert(p.partition.get_id());
                for c in p.chunks.iter() {
                    chunk_ids.insert(c.get_id());
                }
            }
        }
        let mut partition_ids = partition_ids.into_iter().collect::<Vec<_>>();
        partition_ids.sort();
        let mut chunk_ids = chunk_ids.into_iter().collect::<Vec<_>>();
        chunk_ids.sort();
        Self {
            query: query.to_string(),
            inline_tables: (*inline_tables).clone(),
            partition_ids,
            chunk_ids,
        }
    }
}

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct SqlQueueCacheKey {
    query: String,
    inline_tables: InlineTables,
}

impl SqlQueueCacheKey {
    pub fn from_query(query: &str, inline_tables: &InlineTables) -> Self {
        Self {
            query: query.to_string(),
            inline_tables: (*inline_tables).clone(),
        }
    }
}

#[derive(Clone)]
struct StaleEntry {
    result: Arc<DataFrame>,
    created_at: Instant,
}

pub struct SqlResultCache {
    queue_cache: Mutex<
        lru::LruCache<SqlQueueCacheKey, watch::Receiver<Option<Result<Arc<DataFrame>, CubeError>>>>,
    >,
    result_cache: Cache<SqlResultCacheKey, Arc<DataFrame>>,
    stale_cache: Option<Cache<SqlQueueCacheKey, StaleEntry>>,
    stale_while_revalidate_timeout: Option<Duration>,
    create_table_cache:
        Mutex<HashMap<(String, String), watch::Receiver<Option<Result<IdRow<Table>, CubeError>>>>>,
}

pub fn sql_result_cache_sizeof(key: &SqlResultCacheKey, df: &Arc<DataFrame>) -> u32 {
    (key.deep_size_of() + df.deep_size_of())
        .try_into()
        .unwrap_or(u32::MAX)
}

fn stale_cache_sizeof(_key: &SqlQueueCacheKey, entry: &StaleEntry) -> u32 {
    (std::mem::size_of::<SqlQueueCacheKey>()
        + std::mem::size_of::<StaleEntry>()
        + entry.result.deep_size_of())
    .try_into()
    .unwrap_or(u32::MAX)
}

impl SqlResultCache {
    pub fn new(
        capacity_bytes: u64,
        time_to_idle_secs: Option<u64>,
        queue_cache_max_capacity: u64,
        stale_while_revalidate_secs: Option<u64>,
    ) -> Self {
        let cache_builder = if let Some(time_to_idle_secs) = time_to_idle_secs {
            Cache::builder().time_to_idle(Duration::from_secs(time_to_idle_secs))
        } else {
            Cache::builder()
        };

        let stale_while_revalidate_timeout = stale_while_revalidate_secs.map(Duration::from_secs);

        let stale_cache = stale_while_revalidate_timeout.map(|timeout| {
            Cache::builder()
                .time_to_idle(timeout * 2)
                .max_capacity(capacity_bytes)
                .weigher(stale_cache_sizeof)
                .build()
        });

        Self {
            queue_cache: Mutex::new(lru::LruCache::new(queue_cache_max_capacity as usize)),
            result_cache: cache_builder
                .max_capacity(capacity_bytes)
                .weigher(sql_result_cache_sizeof)
                .build(),
            stale_cache,
            stale_while_revalidate_timeout,
            create_table_cache: Mutex::new(HashMap::new()),
        }
    }

    fn report_stale_cache_metrics(&self) {
        if let Some(stale_cache) = &self.stale_cache {
            app_metrics::DATA_QUERIES_STALE_CACHE_SIZE.report(stale_cache.entry_count() as i64);
            app_metrics::DATA_QUERIES_STALE_CACHE_WEIGHT.report(stale_cache.weighted_size() as i64);
        }
    }

    pub async fn clear(&self) {
        // invalidation will be done in the background
        self.result_cache.invalidate_all();
        // it doesnt flush all, blocking, but it's ok because it's used in one command.
        self.result_cache.sync();

        if let Some(stale_cache) = &self.stale_cache {
            stale_cache.invalidate_all();
            stale_cache.sync();
        }

        app_metrics::DATA_QUERIES_CACHE_SIZE.report(self.result_cache.entry_count() as i64);
        app_metrics::DATA_QUERIES_CACHE_WEIGHT.report(self.result_cache.weighted_size() as i64);
        self.report_stale_cache_metrics();
    }

    pub fn entry_count(&self) -> u64 {
        self.result_cache.entry_count()
    }

    pub fn iter(&self) -> Iter<'_, SqlResultCacheKey, Arc<DataFrame>> {
        self.result_cache.iter()
    }

    fn try_get_stale(&self, queue_key: &SqlQueueCacheKey) -> Option<Arc<DataFrame>> {
        let stale_cache = self.stale_cache.as_ref()?;
        let timeout = self.stale_while_revalidate_timeout?;
        let entry = stale_cache.get(queue_key)?;
        if entry.created_at.elapsed() <= timeout {
            Some(entry.result)
        } else {
            None
        }
    }

    async fn update_stale_cache(&self, queue_key: &SqlQueueCacheKey, result: &Arc<DataFrame>) {
        if let Some(stale_cache) = &self.stale_cache {
            stale_cache
                .insert(
                    queue_key.clone(),
                    StaleEntry {
                        result: result.clone(),
                        created_at: Instant::now(),
                    },
                )
                .await;
        }
    }

    #[tracing::instrument(level = "trace", skip(self, context, plan, exec))]
    pub async fn get<F>(
        self: &Arc<Self>,
        query: &str,
        context: SqlQueryContext,
        plan: SerializedPlan,
        exec: impl FnOnce(SerializedPlan) -> F + Send + 'static,
    ) -> Result<Arc<DataFrame>, CubeError>
    where
        F: Future<Output = Result<DataFrame, CubeError>> + Send + 'static,
    {
        Arc::clone(self)
            .get_inner(query.to_string(), context, plan, exec, false)
            .await
    }

    fn get_inner<F>(
        self: Arc<Self>,
        query: String,
        context: SqlQueryContext,
        plan: SerializedPlan,
        exec: impl FnOnce(SerializedPlan) -> F + Send + 'static,
        is_background_refresh: bool,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Arc<DataFrame>, CubeError>> + Send>>
    where
        F: Future<Output = Result<DataFrame, CubeError>> + Send + 'static,
    {
        Box::pin(async move {
            let result_key = SqlResultCacheKey::from_plan(&query, &context.inline_tables, &plan);

            if let Some(result) = self.result_cache.get(&result_key) {
                app_metrics::DATA_QUERIES_CACHE_HIT.increment();
                trace!("Using result cache for '{}'", query);
                return Ok(result);
            }

            let queue_key = SqlQueueCacheKey::from_query(&query, &context.inline_tables);

            if !is_background_refresh {
                if let Some(stale_result) = self.try_get_stale(&queue_key) {
                    app_metrics::DATA_QUERIES_CACHE_STALE_HIT.increment();
                    trace!(
                        "Using stale-while-revalidate cache for '{}', spawning background refresh",
                        query
                    );
                    let this = Arc::clone(&self);
                    let query_clone = query.clone();
                    tokio::spawn(async move {
                        if let Err(e) = this.get_inner(query_clone, context, plan, exec, true).await
                        {
                            log::error!("Background stale-while-revalidate refresh failed: {}", e);
                        }
                    });
                    return Ok(stale_result);
                }
            }

            let (sender, receiver) = {
                let key = queue_key.clone();
                let mut cache = self.queue_cache.lock().await;

                if cache.contains(&key) {
                    if let Some(receiver) = cache.get(&key) {
                        if receiver.has_changed().is_err() {
                            log::error!("Queue cache contains closed channel");
                            cache.pop(&key);
                        }
                    } else {
                        log::error!("Queue cache doesn't contains channel");
                        cache.pop(&key);
                    }
                }

                if !cache.contains(&key) {
                    let (tx, rx) = watch::channel(None);
                    cache.put(key, rx);

                    app_metrics::DATA_QUERIES_CACHE_SIZE
                        .report(self.result_cache.entry_count() as i64);
                    app_metrics::DATA_QUERIES_CACHE_WEIGHT
                        .report(self.result_cache.weighted_size() as i64);
                    self.report_stale_cache_metrics();

                    (Some(tx), None)
                } else {
                    (None, cache.get(&key).cloned())
                }
            };

            if let Some(sender) = sender {
                trace!("Missing cache for '{}'", query);
                let result = exec(plan).await.map(|d| Arc::new(d));
                if let Err(e) = sender.send(Some(result.clone())) {
                    trace!(
                        "Failed to set cached query result, possibly flushed from LRU cache: {}",
                        e
                    );
                }
                match &result {
                    Ok(r) => {
                        if !self.result_cache.contains_key(&result_key) {
                            self.result_cache
                                .insert(result_key.clone(), r.clone())
                                .await;

                            app_metrics::DATA_QUERIES_CACHE_SIZE
                                .report(self.result_cache.entry_count() as i64);
                            app_metrics::DATA_QUERIES_CACHE_WEIGHT
                                .report(self.result_cache.weighted_size() as i64);
                        }
                        self.update_stale_cache(&queue_key, r).await;
                        self.report_stale_cache_metrics();
                    }
                    Err(_) => {
                        trace!("Removing error result from cache");
                    }
                }

                self.queue_cache.lock().await.pop(&queue_key);

                return result;
            }

            std::mem::drop(plan);
            std::mem::drop(result_key);
            std::mem::drop(context);

            self.wait_for_queue(receiver, &query).await
        })
    }

    pub async fn create_table<F>(
        &self,
        schema_name: String,
        table_name: String,
        exec: impl FnOnce() -> F,
    ) -> Result<IdRow<Table>, CubeError>
    where
        F: Future<Output = Result<IdRow<Table>, CubeError>> + Send + 'static,
    {
        let key = (schema_name.clone(), table_name.clone());
        let (sender, mut receiver) = {
            let mut cache = self.create_table_cache.lock().await;
            let key = key.clone();
            if !cache.contains_key(&key) {
                let (tx, rx) = watch::channel(None);
                cache.insert(key, rx);

                (Some(tx), None)
            } else {
                (None, cache.get(&key).cloned())
            }
        };

        if let Some(sender) = sender {
            let result = exec().await;
            if let Err(e) = sender.send(Some(result.clone())) {
                trace!(
                    "Failed to set cached query result, possibly flushed from LRU cache: {}",
                    e
                );
            }

            self.create_table_cache.lock().await.remove(&key);

            return result;
        }

        if let Some(receiver) = &mut receiver {
            loop {
                receiver.changed().await?;
                let x = receiver.borrow();
                let value = x.as_ref();
                if let Some(value) = value {
                    return value.clone();
                }
            }
        }
        panic!("Unexpected state: wait receiver expected but cache was empty")
    }

    #[tracing::instrument(level = "trace", skip(self, receiver))]
    async fn wait_for_queue(
        &self,
        mut receiver: Option<watch::Receiver<Option<Result<Arc<DataFrame>, CubeError>>>>,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError> {
        if let Some(receiver) = &mut receiver {
            loop {
                // Currently we should never loop -- we only send sender a `Some(_)` value.
                receiver.changed().await?;
                let x = receiver.borrow_and_update();
                let value = x.as_ref();
                if let Some(value) = value {
                    trace!("Using cache for '{}'", query);
                    return value.clone();
                }
                log::warn!("Queue query cache is (impossibly) looping for '{}'", query);
            }
        }
        panic!("Unexpected state: wait receiver expected but cache was empty")
    }
}

impl Drop for SqlResultCache {
    fn drop(&mut self) {
        app_metrics::DATA_QUERIES_CACHE_SIZE.report(0);
        app_metrics::DATA_QUERIES_CACHE_WEIGHT.report(0);
        app_metrics::DATA_QUERIES_STALE_CACHE_SIZE.report(0);
        app_metrics::DATA_QUERIES_STALE_CACHE_WEIGHT.report(0);
    }
}

#[cfg(test)]
mod tests {
    use crate::queryplanner::serialized_plan::SerializedPlan;
    use crate::queryplanner::PlanningMeta;
    use crate::sql::cache::SqlResultCache;
    use crate::sql::SqlQueryContext;
    use crate::store::DataFrame;
    use crate::table::{Row, TableValue};
    use crate::CubeError;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use futures::future::join_all;
    use futures_timer::Delay;
    use moka::future::ConcurrentCacheExt;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn simple() -> Result<(), CubeError> {
        let cache = Arc::new(SqlResultCache::new(1 << 20, Some(120), 1000, None));
        let schema = Arc::new(DFSchema::empty());
        let plan = SerializedPlan::try_new(
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            }),
            PlanningMeta {
                indices: Vec::new(),
                multi_part_subtree: HashMap::new(),
                pushable_chunk_filters: Vec::new(),
            },
            None,
        )
        .await?;
        let counter = Arc::new(AtomicI64::new(1));
        let exec = async move |_p| {
            Delay::new(Duration::from_millis(500)).await;
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(
                    counter.fetch_add(1, Ordering::Relaxed),
                )])],
            ))
        };

        let futures = vec![
            cache.get(
                "SELECT 1",
                SqlQueryContext::default(),
                plan.clone(),
                exec.clone(),
            ),
            cache.get(
                "SELECT 2",
                SqlQueryContext::default(),
                plan.clone(),
                exec.clone(),
            ),
            cache.get(
                "SELECT 3",
                SqlQueryContext::default(),
                plan.clone(),
                exec.clone(),
            ),
            cache.get(
                "SELECT 1",
                SqlQueryContext::default(),
                plan.clone(),
                exec.clone(),
            ),
            cache.get("SELECT 1", SqlQueryContext::default(), plan, exec),
        ];

        let res = join_all(futures)
            .await
            .iter()
            .map(|v| {
                v.as_ref()
                    .unwrap()
                    .get_rows()
                    .get(0)
                    .unwrap()
                    .values()
                    .get(0)
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![
                TableValue::Int(1),
                TableValue::Int(2),
                TableValue::Int(3),
                TableValue::Int(1),
                TableValue::Int(1),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_while_revalidate() -> Result<(), CubeError> {
        let cache = Arc::new(SqlResultCache::new(1 << 20, Some(120), 1000, Some(30)));
        let schema = Arc::new(DFSchema::empty());
        let plan = SerializedPlan::try_new(
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            }),
            PlanningMeta {
                indices: Vec::new(),
                multi_part_subtree: HashMap::new(),
                pushable_chunk_filters: Vec::new(),
            },
            None,
        )
        .await?;

        let counter = Arc::new(AtomicI64::new(1));
        let counter_clone = counter.clone();
        let exec = async move |_p| {
            Delay::new(Duration::from_millis(100)).await;
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(
                    counter_clone.fetch_add(1, Ordering::Relaxed),
                )])],
            ))
        };

        let result = cache
            .get("SELECT 1", SqlQueryContext::default(), plan.clone(), exec)
            .await?;
        assert_eq!(
            result.get_rows().get(0).unwrap().values().get(0).unwrap(),
            &TableValue::Int(1)
        );

        // Simulate a partition change: clear the exact result cache so the next get()
        // misses the exact key but still finds the stale entry (keyed by SQL only).
        cache.result_cache.invalidate_all();
        cache.result_cache.sync();

        let counter_clone2 = counter.clone();
        let exec2 = async move |_p| {
            Delay::new(Duration::from_millis(500)).await;
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(
                    counter_clone2.fetch_add(1, Ordering::Relaxed),
                )])],
            ))
        };

        let stale_result = cache
            .get("SELECT 1", SqlQueryContext::default(), plan.clone(), exec2)
            .await?;
        assert_eq!(
            stale_result
                .get_rows()
                .get(0)
                .unwrap()
                .values()
                .get(0)
                .unwrap(),
            &TableValue::Int(1),
            "Should return stale result immediately"
        );

        // Wait for the background refresh to complete.
        Delay::new(Duration::from_millis(800)).await;

        // The background refresh should have populated the exact result cache.
        // Verify by fetching again — this should hit the exact cache with the
        // refreshed value, or at minimum the stale cache was updated.
        let counter_clone3 = counter.clone();
        let exec3 = async move |_p| {
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(
                    counter_clone3.fetch_add(1, Ordering::Relaxed),
                )])],
            ))
        };
        let fresh_result = cache
            .get("SELECT 1", SqlQueryContext::default(), plan, exec3)
            .await?;

        let val = fresh_result
            .get_rows()
            .get(0)
            .unwrap()
            .values()
            .get(0)
            .unwrap()
            .clone();
        assert!(
            val == TableValue::Int(2) || val == TableValue::Int(3),
            "Should see the updated value from background refresh: got {:?}",
            val
        );

        Ok(())
    }

    #[tokio::test]
    async fn stale_while_revalidate_background_failure() -> Result<(), CubeError> {
        let cache = Arc::new(SqlResultCache::new(1 << 20, Some(120), 1000, Some(30)));
        let schema = Arc::new(DFSchema::empty());
        let plan = SerializedPlan::try_new(
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            }),
            PlanningMeta {
                indices: Vec::new(),
                multi_part_subtree: HashMap::new(),
                pushable_chunk_filters: Vec::new(),
            },
            None,
        )
        .await?;

        let exec = async move |_p| {
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(42)])],
            ))
        };
        cache
            .get("SELECT 1", SqlQueryContext::default(), plan.clone(), exec)
            .await?;

        // Simulate partition change
        cache.result_cache.invalidate_all();
        cache.result_cache.sync();

        let exec_fail = async move |_p| -> Result<DataFrame, CubeError> {
            Err(CubeError::internal("background exec failed".to_string()))
        };

        let stale_result = cache
            .get(
                "SELECT 1",
                SqlQueryContext::default(),
                plan.clone(),
                exec_fail,
            )
            .await?;
        assert_eq!(
            stale_result
                .get_rows()
                .get(0)
                .unwrap()
                .values()
                .get(0)
                .unwrap(),
            &TableValue::Int(42),
            "Should still return stale result when background refresh will fail"
        );

        Delay::new(Duration::from_millis(200)).await;

        // Stale entry should still be intact after background failure
        cache.result_cache.invalidate_all();
        cache.result_cache.sync();

        let exec_after = async move |_p| {
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(99)])],
            ))
        };
        let result = cache
            .get("SELECT 1", SqlQueryContext::default(), plan, exec_after)
            .await?;
        assert_eq!(
            result.get_rows().get(0).unwrap().values().get(0).unwrap(),
            &TableValue::Int(42),
            "Stale entry should still be available after background failure"
        );

        Ok(())
    }

    #[tokio::test]
    async fn stale_while_revalidate_timeout_expiry() -> Result<(), CubeError> {
        let cache = Arc::new(SqlResultCache::new(1 << 20, Some(120), 1000, Some(1)));
        let schema = Arc::new(DFSchema::empty());
        let plan = SerializedPlan::try_new(
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            }),
            PlanningMeta {
                indices: Vec::new(),
                multi_part_subtree: HashMap::new(),
                pushable_chunk_filters: Vec::new(),
            },
            None,
        )
        .await?;

        let exec = async move |_p| {
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(1)])],
            ))
        };
        cache
            .get("SELECT 1", SqlQueryContext::default(), plan.clone(), exec)
            .await?;

        // Simulate partition change
        cache.result_cache.invalidate_all();
        cache.result_cache.sync();

        // Wait for the 1-second stale timeout to expire
        Delay::new(Duration::from_millis(1200)).await;

        let exec2 = async move |_p| {
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(99)])],
            ))
        };
        let result = cache
            .get("SELECT 1", SqlQueryContext::default(), plan, exec2)
            .await?;
        assert_eq!(
            result.get_rows().get(0).unwrap().values().get(0).unwrap(),
            &TableValue::Int(99),
            "After stale timeout expires, should execute fresh query instead of serving stale"
        );

        Ok(())
    }
}
