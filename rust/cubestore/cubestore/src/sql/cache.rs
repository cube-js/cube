use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::sql::InlineTables;
use crate::store::DataFrame;
use crate::CubeError;
use futures::Future;
use log::trace;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct SqlResultCacheKey {
    query: String,
    inline_tables: InlineTables,
    partition_ids: Vec<u64>,
    chunk_ids: Vec<u64>,
}

impl SqlResultCacheKey {
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
    result_cache: Mutex<lru::LruCache<SqlResultCacheKey, Arc<DataFrame>>>,
}

impl SqlResultCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue_cache: Mutex::new(lru::LruCache::new(capacity)),
            result_cache: Mutex::new(lru::LruCache::new(capacity)),
        }
    }

    pub async fn get<F>(
        &self,
        query: &str,
        inline_tables: &InlineTables,
        plan: SerializedPlan,
        exec: impl FnOnce(SerializedPlan) -> F,
    ) -> Result<Arc<DataFrame>, CubeError>
    where
        F: Future<Output = Result<DataFrame, CubeError>> + Send + 'static,
    {
        let result_key = SqlResultCacheKey::from_plan(query, inline_tables, &plan);
        let cached_result = {
            let mut result_cache = self.result_cache.lock().await;
            result_cache.get(&result_key).cloned()
        };
        if let Some(result) = cached_result {
            trace!("Using result cache for '{}'", query);
            return Ok(result);
        }

        let queue_key = SqlQueueCacheKey::from_query(query, inline_tables);
        let (sender, mut receiver) = {
            let key = queue_key.clone();
            let mut cache = self.queue_cache.lock().await;
            if !cache.contains(&key) {
                let (tx, rx) = watch::channel(None);
                cache.put(key, rx);
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
                    let mut result_cache = self.result_cache.lock().await;
                    if !result_cache.contains(&result_key) {
                        result_cache.put(result_key.clone(), r.clone());
                    }
                }
                Err(_) => {
                    trace!("Removing error result from cache");
                }
            }
            self.queue_cache.lock().await.pop(&queue_key);

            return result;
        }

        if let Some(receiver) = &mut receiver {
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

        panic!("Unexpected state: wait receiver expected but cache was empty")
    }
}

#[cfg(test)]
mod tests {
    use crate::queryplanner::serialized_plan::SerializedPlan;
    use crate::queryplanner::PlanningMeta;
    use crate::sql::cache::SqlResultCache;
    use crate::sql::InlineTables;
    use crate::store::DataFrame;
    use crate::table::{Row, TableValue};
    use crate::CubeError;
    use datafusion::logical_plan::{DFSchema, LogicalPlan};
    use flatbuffers::bitflags::_core::sync::atomic::AtomicI64;
    use futures::future::join_all;
    use futures_timer::Delay;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn simple() -> Result<(), CubeError> {
        let cache = SqlResultCache::new(100);
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
        let inline_tables = InlineTables::new();
        let futures = vec![
            cache.get("SELECT 1", &inline_tables, plan.clone(), exec.clone()),
            cache.get("SELECT 2", &inline_tables, plan.clone(), exec.clone()),
            cache.get("SELECT 3", &inline_tables, plan.clone(), exec.clone()),
            cache.get("SELECT 1", &inline_tables, plan.clone(), exec.clone()),
            cache.get("SELECT 1", &inline_tables, plan, exec),
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
