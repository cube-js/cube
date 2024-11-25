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
use std::time::Duration;
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

pub struct SqlResultCache {
    queue_cache: Mutex<
        lru::LruCache<SqlQueueCacheKey, watch::Receiver<Option<Result<Arc<DataFrame>, CubeError>>>>,
    >,
    result_cache: Cache<SqlResultCacheKey, Arc<DataFrame>>,
    create_table_cache:
        Mutex<HashMap<(String, String), watch::Receiver<Option<Result<IdRow<Table>, CubeError>>>>>,
}

pub fn sql_result_cache_sizeof(key: &SqlResultCacheKey, df: &Arc<DataFrame>) -> u32 {
    (key.deep_size_of() + df.deep_size_of())
        .try_into()
        .unwrap_or(u32::MAX)
}

impl SqlResultCache {
    pub fn new(
        capacity_bytes: u64,
        time_to_idle_secs: Option<u64>,
        queue_cache_max_capacity: u64,
    ) -> Self {
        let cache_builder = if let Some(time_to_idle_secs) = time_to_idle_secs {
            Cache::builder().time_to_idle(Duration::from_secs(time_to_idle_secs))
        } else {
            Cache::builder()
        };

        Self {
            queue_cache: Mutex::new(lru::LruCache::new(queue_cache_max_capacity as usize)),
            result_cache: cache_builder
                .max_capacity(capacity_bytes)
                .weigher(sql_result_cache_sizeof)
                .build(),
            create_table_cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn clear(&self) {
        // invalidation will be done in the background
        self.result_cache.invalidate_all();
        // it doesnt flush all, blocking, but it's ok because it's used in one command.
        self.result_cache.sync();

        app_metrics::DATA_QUERIES_CACHE_SIZE.report(self.result_cache.entry_count() as i64);
        app_metrics::DATA_QUERIES_CACHE_WEIGHT.report(self.result_cache.weighted_size() as i64);
    }

    pub fn entry_count(&self) -> u64 {
        self.result_cache.entry_count()
    }

    pub fn iter(&self) -> Iter<'_, SqlResultCacheKey, Arc<DataFrame>> {
        self.result_cache.iter()
    }

    #[tracing::instrument(level = "trace", skip(self, context, plan, exec))]
    pub async fn get<F>(
        &self,
        query: &str,
        context: SqlQueryContext,
        plan: SerializedPlan,
        exec: impl FnOnce(SerializedPlan) -> F,
    ) -> Result<Arc<DataFrame>, CubeError>
    where
        F: Future<Output = Result<DataFrame, CubeError>> + Send + 'static,
    {
        let result_key = SqlResultCacheKey::from_plan(query, &context.inline_tables, &plan);

        if let Some(result) = self.result_cache.get(&result_key) {
            app_metrics::DATA_QUERIES_CACHE_HIT.increment();
            trace!("Using result cache for '{}'", query);
            return Ok(result);
        }

        let queue_key = SqlQueueCacheKey::from_query(query, &context.inline_tables);
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

                app_metrics::DATA_QUERIES_CACHE_SIZE.report(self.result_cache.entry_count() as i64);
                app_metrics::DATA_QUERIES_CACHE_WEIGHT
                    .report(self.result_cache.weighted_size() as i64);

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

        self.wait_for_queue(receiver, query).await
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
    use flatbuffers::bitflags::_core::sync::atomic::AtomicI64;
    use futures::future::join_all;
    use futures_timer::Delay;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn simple() -> Result<(), CubeError> {
        let cache = SqlResultCache::new(1 << 20, Some(120), 1000);
        let schema = Arc::new(DFSchema::empty());
        let plan = SerializedPlan::try_new(
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            }),
            PlanningMeta {
                indices: Vec::new(),
                multi_part_subtree: HashMap::new(),
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
}
