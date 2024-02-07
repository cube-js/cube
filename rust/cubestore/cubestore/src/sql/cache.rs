use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::sql::InlineTables;
use crate::sql::SelectExecutor;
use crate::sql::SqlQueryContext;
use crate::store::DataFrame;
use crate::{app_metrics, CubeError};
use datafusion::cube_ext;
use deepsize::DeepSizeOf;
use log::trace;
use moka::future::{Cache, ConcurrentCacheExt, Iter};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{watch, Mutex, Notify, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Hash, Eq, PartialEq, Debug, DeepSizeOf)]
pub struct SqlResultCacheKey {
    cache_key: SqlQueueCacheKey,
    partition_ids: Vec<u64>,
    chunk_ids: Vec<u64>,
}

impl SqlResultCacheKey {
    pub fn get_query(&self) -> &String {
        &self.cache_key.query
    }

    pub fn cache_key(&self) -> &SqlQueueCacheKey {
        &self.cache_key
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
            cache_key: SqlQueueCacheKey {
                query: query.to_string(),
                inline_tables: (*inline_tables).clone(),
            },
            partition_ids,
            chunk_ids,
        }
    }
}

#[derive(Clone, Hash, Eq, PartialEq, Debug, DeepSizeOf)]
pub struct SqlQueueCacheKey {
    pub query: String,
    pub inline_tables: InlineTables,
}

impl SqlQueueCacheKey {
    pub fn from_query(query: &str, inline_tables: &InlineTables) -> Self {
        Self {
            query: query.to_string(),
            inline_tables: (*inline_tables).clone(),
        }
    }
}

struct SqlPendingCacheItem {
    pub last_touch: RwLock<SystemTime>,
    pub executor: Arc<dyn SelectExecutor>,
    pub sender: watch::Sender<Option<Result<Arc<DataFrame>, CubeError>>>,
    pub reciever: watch::Receiver<Option<Result<Arc<DataFrame>, CubeError>>>,
}

impl SqlPendingCacheItem {
    pub async fn update_last_touch(&self) {
        *self.last_touch.write().await = SystemTime::now();
    }
    pub async fn last_touch(&self) -> SystemTime {
        self.last_touch.read().await.clone()
    }
}

struct QueueCache {
    pub pending_queue: VecDeque<(SqlResultCacheKey, Arc<SqlPendingCacheItem>)>,
    pub pending_hash: HashMap<SqlQueueCacheKey, Arc<SqlPendingCacheItem>>,
    pub queue:
        HashMap<SqlQueueCacheKey, watch::Receiver<Option<Result<Arc<DataFrame>, CubeError>>>>,
}

impl QueueCache {
    pub fn new() -> Self {
        Self {
            pending_queue: VecDeque::new(),
            pending_hash: HashMap::new(),
            queue: HashMap::new(),
        }
    }

    pub fn pop_back_pending(&mut self) -> Option<(SqlResultCacheKey, Arc<SqlPendingCacheItem>)> {
        let itm = self.pending_queue.pop_back();
        if let Some(itm) = &itm {
            self.pending_hash.remove(itm.0.cache_key());
        }
        itm
    }
}

pub struct SqlResultCache {
    queue_cache: Mutex<QueueCache>,
    result_cache: Arc<Cache<SqlResultCacheKey, Arc<DataFrame>>>,
    pending_notify: Arc<Notify>,
    stopped_token: CancellationToken,
    max_queue_size: usize,
    max_pending_size: usize,
    pending_timout: Duration,
}

crate::di_service!(SqlResultCache, []);

pub fn sql_result_cache_sizeof(key: &SqlResultCacheKey, df: &Arc<DataFrame>) -> u32 {
    (key.deep_size_of() + df.deep_size_of())
        .try_into()
        .unwrap_or(u32::MAX)
}

impl SqlResultCache {
    pub fn new(
        capacity_bytes: u64,
        time_to_idle_secs: Option<u64>,
        max_queue_size: usize,
        max_pending_size: usize,
        pending_timout_secs: u64,
    ) -> Self {
        let cache_builder = if let Some(time_to_idle_secs) = time_to_idle_secs {
            Cache::builder().time_to_idle(Duration::from_secs(time_to_idle_secs))
        } else {
            Cache::builder()
        };

        Self {
            queue_cache: Mutex::new(QueueCache::new()),
            result_cache: Arc::new(
                cache_builder
                    .max_capacity(capacity_bytes)
                    .weigher(sql_result_cache_sizeof)
                    .build(),
            ),
            pending_notify: Arc::new(Notify::new()),
            stopped_token: CancellationToken::new(),
            max_queue_size,
            max_pending_size,
            pending_timout: Duration::from_secs(pending_timout_secs),
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

    pub fn stop_processing_loops(&self) {
        self.stopped_token.cancel();
    }

    pub async fn wait_processing_loop(&self) -> Result<(), CubeError> {
        loop {
            tokio::select! {
                _ = self.stopped_token.cancelled() => {
                    return Ok(())
                }
                _ = self.pending_notify.notified() => {}

            };
            self.process_pending().await?;
        }
    }

    async fn process_pending(&self) -> Result<(), CubeError> {
        let mut queue_cache = self.queue_cache.lock().await;
        if queue_cache.pending_queue.is_empty() {
            return Ok(());
        }

        while let Some((_, back)) = queue_cache.pending_queue.back() {
            if back.last_touch().await.elapsed()? <= self.pending_timout {
                break;
            }
            queue_cache.pop_back_pending();
        }

        while queue_cache.queue.len() < self.max_queue_size {
            let itm = queue_cache.pop_back_pending();
            if let Some(itm) = itm {
                let (result_key, cache_itm) = itm;
                if cache_itm.last_touch().await.elapsed()? <= self.pending_timout {
                    let result_cache = self.result_cache.clone();
                    let pending_notify = self.pending_notify.clone();
                    queue_cache
                        .queue
                        .insert(result_key.cache_key().clone(), cache_itm.reciever.clone());
                    cube_ext::spawn(async move {
                        let result = cache_itm.executor.execute().await.map(|d| Arc::new(d));
                        if let Err(e) = cache_itm.sender.send(Some(result.clone())) {
                            trace!(
                    "Failed to set cached query result, possibly flushed from LRU cache: {}",
                    e
                );
                        }
                        match &result {
                            Ok(r) => {
                                if !result_cache.contains_key(&result_key) {
                                    result_cache.insert(result_key.clone(), r.clone()).await;

                                    app_metrics::DATA_QUERIES_CACHE_SIZE
                                        .report(result_cache.entry_count() as i64);
                                    app_metrics::DATA_QUERIES_CACHE_WEIGHT
                                        .report(result_cache.weighted_size() as i64);
                                }
                            }
                            Err(_) => {
                                trace!("Removing error result from cache");
                            }
                        }
                        pending_notify.notify_one();
                    });
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    pub fn entry_count(&self) -> u64 {
        self.result_cache.entry_count()
    }

    pub fn iter(&self) -> Iter<'_, SqlResultCacheKey, Arc<DataFrame>> {
        self.result_cache.iter()
    }

    #[tracing::instrument(level = "trace", skip(self, context, executor))]
    pub async fn get(
        &self,
        query: &str,
        context: SqlQueryContext,
        executor: Arc<dyn SelectExecutor>,
    ) -> Result<Arc<DataFrame>, CubeError> {
        let result_key = SqlResultCacheKey::from_plan(
            query,
            &context.inline_tables,
            &executor.serialized_plan(),
        );

        if let Some(result) = self.result_cache.get(&result_key) {
            app_metrics::DATA_QUERIES_CACHE_HIT.increment();
            trace!("Using result cache for '{}'", query);
            return Ok(result);
        }

        let reciever = self.get_from_queue_cache(result_key, executor).await?;
        self.wait_for_queue(reciever, query).await
    }

    async fn get_from_queue_cache(
        &self,
        result_key: SqlResultCacheKey,
        executor: Arc<dyn SelectExecutor>,
    ) -> Result<watch::Receiver<Option<Result<Arc<DataFrame>, CubeError>>>, CubeError> {
        let mut queue_cache = self.queue_cache.lock().await;
        if let Some(reciever) = queue_cache.queue.get(result_key.cache_key()) {
            Ok(reciever.clone())
        } else if let Some(pending_item) = queue_cache.pending_hash.get(result_key.cache_key()) {
            pending_item.update_last_touch().await;
            Ok(pending_item.reciever.clone())
        } else {
            if queue_cache.pending_queue.len() >= self.max_pending_size {
                return Err(CubeError::user(
                    "Too many pending queries. Try again later.".to_string(),
                ));
            }
            let (sender, reciever) = watch::channel(None);
            let cache_item = Arc::new(SqlPendingCacheItem {
                last_touch: RwLock::new(SystemTime::now()),
                executor,
                sender,
                reciever: reciever.clone(),
            });
            queue_cache
                .pending_hash
                .insert(result_key.cache_key().clone(), cache_item.clone());
            queue_cache
                .pending_queue
                .push_front((result_key, cache_item));
            self.pending_notify.notify_one();
            Ok(reciever)
        }
    }

    #[tracing::instrument(level = "trace", skip(self, receiver))]
    async fn wait_for_queue(
        &self,
        mut receiver: watch::Receiver<Option<Result<Arc<DataFrame>, CubeError>>>,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError> {
        loop {
            receiver.changed().await?;
            let x = receiver.borrow();
            let value = x.as_ref();
            if let Some(value) = value {
                trace!("Using cache for '{}'", query);
                return value.clone();
            }
        }
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
    use super::*;
    use crate::queryplanner::serialized_plan::SerializedPlan;
    use crate::queryplanner::PlanningMeta;
    use crate::sql::cache::SqlResultCache;
    use crate::sql::SqlQueryContext;
    use crate::store::DataFrame;
    use crate::table::{Row, TableValue};
    use crate::CubeError;
    use async_trait::async_trait;
    use datafusion::logical_plan::{DFSchema, LogicalPlan};
    use flatbuffers::bitflags::_core::sync::atomic::AtomicI64;
    use futures::future::join_all;
    use futures_timer::Delay;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    struct SampleExecutor {
        serialized_plan: SerializedPlan,
        counter: Arc<AtomicI64>,
    }

    impl SampleExecutor {
        fn new(serialized_plan: SerializedPlan) -> Arc<Self> {
            Arc::new(Self {
                serialized_plan,
                counter: Arc::new(AtomicI64::new(1)),
            })
        }
    }

    #[async_trait]
    impl SelectExecutor for SampleExecutor {
        fn serialized_plan(&self) -> &SerializedPlan {
            &self.serialized_plan
        }

        async fn execute(&self) -> Result<DataFrame, CubeError> {
            Delay::new(Duration::from_millis(500)).await;
            Ok(DataFrame::new(
                Vec::new(),
                vec![Row::new(vec![TableValue::Int(
                    self.counter.fetch_add(1, Ordering::Relaxed),
                )])],
            ))
        }
    }
    #[tokio::test]
    async fn simple() -> Result<(), CubeError> {
        let cache = Arc::new(SqlResultCache::new(1 << 20, Some(120), 500, 1500, 300));
        let schema = Arc::new(DFSchema::new(Vec::new())?);
        let plan = SerializedPlan::try_new(
            LogicalPlan::EmptyRelation {
                produce_one_row: false,
                schema,
            },
            PlanningMeta {
                indices: Vec::new(),
                multi_part_subtree: HashMap::new(),
            },
            None,
        )
        .await?;
        let executor = SampleExecutor::new(plan.clone());
        let cache_to_move = cache.clone();
        cube_ext::spawn(async move { cache_to_move.wait_processing_loop().await });

        let futures = vec![
            cache.get("SELECT 1", SqlQueryContext::default(), executor.clone()),
            cache.get("SELECT 2", SqlQueryContext::default(), executor.clone()),
            cache.get("SELECT 3", SqlQueryContext::default(), executor.clone()),
            cache.get("SELECT 1", SqlQueryContext::default(), executor.clone()),
            cache.get("SELECT 1", SqlQueryContext::default(), executor.clone()),
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
        cache.stop_processing_loops();
        Ok(())
    }
}
