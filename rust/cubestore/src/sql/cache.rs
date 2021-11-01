use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::store::DataFrame;
use crate::CubeError;
use futures::Future;
use log::trace;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct SqlResultCacheKey {
    query: String,
    partition_ids: Vec<u64>,
    chunk_ids: Vec<u64>,
}

impl SqlResultCacheKey {
    pub fn from_plan(query: &str, plan: &SerializedPlan) -> Self {
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
            partition_ids,
            chunk_ids,
        }
    }
}

pub struct SqlResultCache {
    cache: RwLock<
        lru::LruCache<
            SqlResultCacheKey,
            watch::Receiver<Option<Result<Arc<DataFrame>, CubeError>>>,
        >,
    >,
}

impl SqlResultCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RwLock::new(lru::LruCache::new(capacity)),
        }
    }

    pub async fn get<F>(
        &self,
        query: &str,
        plan: SerializedPlan,
        exec: impl FnOnce(SerializedPlan) -> F,
    ) -> Result<Arc<DataFrame>, CubeError>
    where
        F: Future<Output = Result<DataFrame, CubeError>> + Send + 'static,
    {
        let key = SqlResultCacheKey::from_plan(query, &plan);
        let (sender, mut receiver) = {
            let key = key.clone();
            let mut cache = self.cache.write().await;
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
            if result.is_err() {
                trace!("Removing error result from cache");
                self.cache.write().await.pop(&key);
            }
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
        let futures = vec![
            cache.get("SELECT 1", plan.clone(), exec.clone()),
            cache.get("SELECT 2", plan.clone(), exec.clone()),
            cache.get("SELECT 3", plan.clone(), exec.clone()),
            cache.get("SELECT 1", plan.clone(), exec.clone()),
            cache.get("SELECT 1", plan, exec),
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
