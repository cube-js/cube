use crate::cachestore::cache_item::{CacheItemRocksIndex, CacheItemRocksTable};
use crate::cachestore::CacheItem;
use crate::config::ConfigObj;
use crate::metastore::{
    BaseRocksSecondaryIndex, IdRow, PackedDateTime, RocksSecondaryIndexValueTTLExtended,
    RocksSecondaryIndexValueVersionDecoder, RocksSecondaryIndexValueVersionEncoder, RocksStore,
    RocksTable,
};
use crate::util::aborting_join_handle::AbortingJoinHandle;
use crate::util::IntervalLoop;
use crate::{app_metrics, CubeError};
use chrono::Utc;
use datafusion::cube_ext;
use itertools::Itertools;
use log::trace;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::RwLockWriteGuard;

#[derive(Debug)]
struct CacheLookupEvent {
    row_id: u64,
    raw_size: u32,
    key_hash: [u8; 8],
}

#[derive(Debug, Clone)]
pub struct CachePolicyData {
    pub key_hash: [u8; 8],
    pub raw_size: u32,
    pub lru: PackedDateTime,
    pub lfu: u8,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CacheEvictionPolicy {
    // Keeps most recently used keys, iterating over all keys without sampling
    AllKeysLru = 0,
    // Keeps most least frequently used, iterating over all keys without sampling
    AllKeysLfu = 1,
    // Removes shortest remaining time-to-live (TTL) values, iterating over all keys without sampling
    AllKeysTtl = 2,
    // Keeps most recently used keys, iterating over all keys by sampling & exit earlier when it's possible
    SampledLru = 3,
    // Keeps most recently used keys, iterating over all keys by sampling & exit earlier when it's possible
    SampledLfu = 4,
    // Removes shortest remaining time-to-live (TTL) values, iterating over all keys by sampling & exit earlier when it's possible
    SampledTtl = 5,
}

impl CacheEvictionPolicy {
    pub fn to_weight(&self) -> CacheEvictionWeightCriteria {
        match self {
            CacheEvictionPolicy::AllKeysLru => CacheEvictionWeightCriteria::ByLRU,
            CacheEvictionPolicy::AllKeysLfu => CacheEvictionWeightCriteria::ByLFU,
            CacheEvictionPolicy::AllKeysTtl => CacheEvictionWeightCriteria::ByTTL,
            CacheEvictionPolicy::SampledLru => CacheEvictionWeightCriteria::ByLRU,
            CacheEvictionPolicy::SampledLfu => CacheEvictionWeightCriteria::ByLFU,
            CacheEvictionPolicy::SampledTtl => CacheEvictionWeightCriteria::ByTTL,
        }
    }
}

#[derive(Debug)]
pub enum CacheEvictionWeightCriteria {
    ByLRU,
    ByTTL,
    ByLFU,
}

#[derive(Debug)]
enum EvictionState {
    Initial,
    LoadingFailed,
    Ready,
    Loading(/* finalizer */ tokio::sync::oneshot::Receiver<()>),
    EvictionStarted(/* finalizer */ tokio::sync::oneshot::Receiver<()>),
    TruncationStarted,
}

impl FromStr for CacheEvictionPolicy {
    type Err = CubeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s {
            &"allkeys-lru" => Ok(CacheEvictionPolicy::AllKeysLru),
            &"allkeys-lfu" => Ok(CacheEvictionPolicy::AllKeysLfu),
            &"allkeys-ttl" => Ok(CacheEvictionPolicy::AllKeysTtl),
            &"sampled-lru" => Ok(CacheEvictionPolicy::SampledLru),
            &"sampled-lfu" => Ok(CacheEvictionPolicy::SampledLru),
            &"sampled-ttl" => Ok(CacheEvictionPolicy::SampledTtl),
            other => Err(CubeError::user(format!(
                "Unsupported cache eviction type: {}",
                other
            ))),
        }
    }
}

#[derive(Debug)]
pub struct CacheEvictionManager {
    ttl_buffer: Arc<tokio::sync::RwLock<HashMap<u64, CachePolicyData>>>,
    ttl_lookup_tx: tokio::sync::mpsc::Sender<CacheLookupEvent>,
    pub persist_loop: Arc<IntervalLoop>,
    pub eviction_loop: Arc<IntervalLoop>,
    // eviction state
    eviction_state: tokio::sync::RwLock<EvictionState>,
    // Some stats
    stats_total_keys: AtomicU32,
    stats_total_raw_size: AtomicU64,
    // Limits from configuration
    limit_max_keys_soft: u32,
    limit_max_keys_hard: u32,
    limit_max_size_hard: u64,
    limit_max_size_soft: u64,
    eviction_policy: CacheEvictionPolicy,
    // Configuration
    persist_batch_size: usize,
    eviction_batch_size: usize,
    eviction_below_threshold: u8,
    // if ttl of a key is less then this value, key will be evicted
    // this help to delete upcoming keys for deleting
    eviction_min_ttl_threshold: u32,
    // background listener to track events
    _ttl_tl_loop_join_handle: Arc<AbortingJoinHandle<()>>,
}

#[derive(Debug)]
struct DeleteBatchResult {
    deleted_count: u32,
    deleted_size: u64,
    skipped: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EvictionResult {
    InProgress(String),
    Finished(EvictionFinishedResult),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EvictionFinishedResult {
    pub total_keys_removed: u32,
    pub total_size_removed: u64,
    pub total_delete_skipped: u32,
    pub stats_total_keys: u32,
    pub stats_total_raw_size: u64,
}

fn calc_percentage(v: u64, percentage: u8) -> u64 {
    if percentage == 0 {
        return v;
    }

    (percentage as u64 * v) / 100_u64
}

impl CacheEvictionManager {
    pub fn get_stats_total_keys(&self) -> u32 {
        self.stats_total_keys.load(Ordering::SeqCst)
    }

    pub fn get_stats_total_raw_size(&self) -> u64 {
        self.stats_total_raw_size.load(Ordering::SeqCst)
    }

    pub fn new(config: &Arc<dyn ConfigObj>) -> Self {
        let ttl_buffer: HashMap<u64, CachePolicyData> = HashMap::new();
        let ttl_buffer = Arc::new(tokio::sync::RwLock::new(ttl_buffer));
        let (ttl_lookup_tx, mut ttl_lookup_rx) = tokio::sync::mpsc::channel::<CacheLookupEvent>(
            config.cachestore_cache_ttl_notify_channel(),
        );

        let ttl_buffer_to_move = ttl_buffer.clone();
        let ttl_buffer_max_size = config.cachestore_cache_ttl_buffer_max_size();

        let join_handle = cube_ext::spawn_blocking(move || loop {
            if let Some(event) = ttl_lookup_rx.blocking_recv() {
                {
                    let mut ttl_buffer = ttl_buffer_to_move.blocking_write();
                    if let Some(mut cache_data) = ttl_buffer.get_mut(&event.row_id) {
                        let expired_lfu = if let Some(previous_lru) =
                            cache_data.lru.decode_value_as_opt_datetime().unwrap()
                        {
                            previous_lru < Utc::now() - chrono::Duration::seconds(60 * 2)
                        } else {
                            true
                        };

                        cache_data.lru = Utc::now().encode_value_as_u32().unwrap();

                        if expired_lfu {
                            cache_data.lfu = 1;
                        } else {
                            if cache_data.lfu < u8::MAX {
                                cache_data.lfu += 1;
                            }
                        }
                    } else {
                        if ttl_buffer.len() >= ttl_buffer_max_size {
                            continue;
                        }

                        ttl_buffer.insert(
                            event.row_id,
                            CachePolicyData {
                                key_hash: event.key_hash,
                                raw_size: event.raw_size,
                                lru: Utc::now().encode_value_as_u32().unwrap(),
                                lfu: 1,
                            },
                        );
                    };
                }
            } else {
                return;
            }
        });

        Self {
            ttl_buffer,
            ttl_lookup_tx,
            persist_loop: Arc::new(IntervalLoop::new(
                "Cachestore ttl persist",
                tokio::time::Duration::from_secs(
                    config.cachestore_cache_ttl_persist_loop_interval(),
                ),
            )),
            eviction_loop: Arc::new(IntervalLoop::new(
                "Cachestore eviction",
                tokio::time::Duration::from_secs(config.cachestore_cache_eviction_loop_interval()),
            )),
            eviction_state: tokio::sync::RwLock::new(EvictionState::Initial),
            stats_total_keys: AtomicU32::new(0),
            stats_total_raw_size: AtomicU64::new(0),
            // Limits & Evict
            limit_max_size_soft: config.cachestore_cache_max_size(),
            limit_max_size_hard: config.cachestore_cache_max_size()
                + calc_percentage(
                    config.cachestore_cache_max_size(),
                    config.cachestore_cache_threshold_to_force_eviction(),
                ),
            limit_max_keys_soft: config.cachestore_cache_max_keys(),
            limit_max_keys_hard: config.cachestore_cache_max_keys()
                + calc_percentage(
                    config.cachestore_cache_max_keys() as u64,
                    config.cachestore_cache_threshold_to_force_eviction(),
                ) as u32,
            eviction_policy: config.cachestore_cache_eviction_policy().clone(),
            persist_batch_size: config.cachestore_cache_persist_batch_size(),
            eviction_batch_size: config.cachestore_cache_eviction_batch_size(),
            eviction_below_threshold: config.cachestore_cache_eviction_below_threshold(),
            eviction_min_ttl_threshold: config.cachestore_cache_eviction_min_ttl_threshold(),
            //
            _ttl_tl_loop_join_handle: Arc::new(AbortingJoinHandle::new(join_handle)),
        }
    }

    pub fn stop_processing_loops(&self) {
        self.persist_loop.stop();
        self.eviction_loop.stop();
    }

    async fn delete_items(
        &self,
        to_delete: Vec<(u64, u32)>,
        store: &Arc<RocksStore>,
    ) -> Result<EvictionFinishedResult, CubeError> {
        let stats_total_keys = self.get_stats_total_keys();
        let stats_total_raw_size = self.get_stats_total_raw_size();

        let mut total_keys_removed = 0;
        let mut total_size_removed = 0;
        let mut total_delete_skipped = 0;

        let last_batch = if to_delete.len() > self.eviction_batch_size {
            let mut batch = Vec::with_capacity(self.eviction_batch_size);

            for (row_id, raw_size) in to_delete {
                batch.push((row_id, raw_size));

                if batch.len() == self.eviction_batch_size {
                    let current_batch =
                        std::mem::replace(&mut batch, Vec::with_capacity(self.eviction_batch_size));
                    let batch_result = self.delete_batch(current_batch, &store).await?;

                    total_size_removed += batch_result.deleted_size;
                    total_keys_removed += batch_result.deleted_count;
                    total_delete_skipped += batch_result.skipped;
                }
            }

            batch
        } else {
            to_delete
        };

        if last_batch.len() > 0 {
            let batch_result = self.delete_batch(last_batch, &store).await?;

            total_size_removed += batch_result.deleted_size;
            total_keys_removed += batch_result.deleted_count;
            total_delete_skipped += batch_result.skipped;
        }

        return Ok(EvictionFinishedResult {
            total_keys_removed,
            total_size_removed,
            total_delete_skipped,
            stats_total_keys,
            stats_total_raw_size,
        });
    }

    async fn delete_batch(
        &self,
        batch: Vec<(u64, u32)>,
        store: &Arc<RocksStore>,
    ) -> Result<DeleteBatchResult, CubeError> {
        let (deleted_count, deleted_size, skipped) = store
            .write_operation(move |db_ref, pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                let mut deleted_count: u32 = 0;
                let mut deleted_size: u64 = 0;
                let mut skipped: u32 = 0;

                for (id, raw_size) in batch {
                    if let Some(_) = cache_schema.try_delete(id, pipe)? {
                        deleted_count += 1;
                        deleted_size += raw_size as u64;
                    } else {
                        skipped += 1;
                    };
                }

                Ok((deleted_count, deleted_size, skipped))
            })
            .await?;

        self.stats_total_keys
            .fetch_sub(deleted_count, Ordering::Relaxed);
        self.stats_total_raw_size
            .fetch_sub(deleted_size, Ordering::Relaxed);

        app_metrics::CACHESTORE_EVICTION_REMOVED_KEYS.add(deleted_count as i64);
        app_metrics::CACHESTORE_EVICTION_REMOVED_SIZE.add(deleted_size as i64);

        Ok(DeleteBatchResult {
            deleted_count,
            deleted_size,
            skipped,
        })
    }

    pub async fn run_eviction(&self, store: &Arc<RocksStore>) -> Result<EvictionResult, CubeError> {
        trace!(
            "Eviction started, total_keys: {}, total_size: {}",
            self.get_stats_total_keys(),
            self.get_stats_total_raw_size()
        );

        let mut state = self.eviction_state.write().await;
        match *state {
            EvictionState::Initial | EvictionState::LoadingFailed => {
                let (state_tx, state_rx) = tokio::sync::oneshot::channel::<()>();

                *state = EvictionState::Loading(state_rx);
                drop(state);

                let load_result = self.do_load(&store).await;

                let mut state = self.eviction_state.write().await;
                *state = if load_result.is_ok() {
                    EvictionState::Ready
                } else {
                    EvictionState::LoadingFailed
                };
                drop(state);

                let _ = state_tx.send(());

                load_result
            }
            EvictionState::Ready => {
                let (state_tx, state_rx) = tokio::sync::oneshot::channel::<()>();

                *state = EvictionState::EvictionStarted(state_rx);
                drop(state);

                let eviction_result = self.do_eviction(&store).await;

                let mut state = self.eviction_state.write().await;
                *state = EvictionState::Ready;
                drop(state);

                let _ = state_tx.send(());

                eviction_result
            }
            EvictionState::Loading(_) => Ok(EvictionResult::InProgress(
                "loading is in progress".to_string(),
            )),
            EvictionState::EvictionStarted(_) => Ok(EvictionResult::InProgress(
                "eviction is in progress".to_string(),
            )),
            EvictionState::TruncationStarted => Ok(EvictionResult::InProgress(
                "truncation is in progress".to_string(),
            )),
        }
    }

    async fn do_eviction(&self, store: &Arc<RocksStore>) -> Result<EvictionResult, CubeError> {
        let eviction_fut = if self.get_stats_total_keys() > self.limit_max_keys_soft {
            let need_to_evict = (self.get_stats_total_keys() - self.limit_max_keys_soft) as u64;
            let target =
                need_to_evict + calc_percentage(need_to_evict, self.eviction_below_threshold);

            trace!(
                "Max keys limit eviction: {} > {}, need to evict: {}, threshold: {}, target: {}",
                self.get_stats_total_keys(),
                self.limit_max_keys_soft,
                need_to_evict,
                self.eviction_below_threshold,
                target
            );

            self.do_eviction_by(&store, target, false)
        } else if self.get_stats_total_raw_size() > self.limit_max_size_soft {
            let need_to_evict = (self.get_stats_total_raw_size() - self.limit_max_size_soft) as u64;
            let target =
                need_to_evict + calc_percentage(need_to_evict, self.eviction_below_threshold);

            trace!(
                "Max size limit eviction: {} > {}, need to evict: {}, threshold: {}, target: {}",
                self.get_stats_total_raw_size(),
                self.limit_max_size_soft,
                need_to_evict,
                self.eviction_below_threshold,
                target
            );

            self.do_eviction_by(&store, target, true)
        } else {
            trace!("Nothing to evict");

            return Ok(EvictionResult::Finished(EvictionFinishedResult {
                total_keys_removed: 0,
                total_size_removed: 0,
                total_delete_skipped: 0,
                stats_total_keys: self.get_stats_total_keys(),
                stats_total_raw_size: self.get_stats_total_raw_size(),
            }));
        };

        let result = eviction_fut.await?;

        trace!(
            "Eviction finished, total_keys: {}, total_size: {}",
            self.get_stats_total_keys(),
            self.get_stats_total_raw_size()
        );

        Ok(result)
    }

    async fn do_load(&self, store: &Arc<RocksStore>) -> Result<EvictionResult, CubeError> {
        let mut total_keys_removed = 0;
        let mut total_size_removed = 0;
        let mut total_delete_skipped = 0;

        let absolute_items = self.collect_stats_and_candidates_to_evict(&store).await?;
        let absolute_items_len = absolute_items.len();

        if absolute_items_len > 0 {
            let batch_result = self.delete_items(absolute_items, &store).await?;
            total_keys_removed = batch_result.total_delete_skipped;
            total_size_removed = batch_result.total_size_removed;
            total_delete_skipped = batch_result.total_delete_skipped;
        }

        trace!(
            "Eviction loaded stats, total_keys: {}, total_size: {}, absolute: {}",
            self.get_stats_total_keys(),
            self.get_stats_total_raw_size(),
            absolute_items_len
        );

        Ok(EvictionResult::Finished(EvictionFinishedResult {
            total_keys_removed,
            total_size_removed,
            total_delete_skipped,
            stats_total_keys: self.get_stats_total_keys(),
            stats_total_raw_size: self.get_stats_total_raw_size(),
        }))
    }

    async fn collect_stats_and_candidates_to_evict(
        &self,
        store: &Arc<RocksStore>,
    ) -> Result<Vec<(u64, u32)>, CubeError> {
        let eviction_min_ttl_threshold = self.eviction_min_ttl_threshold as i64;

        let (to_delete, stats_total_keys, stats_total_raw_size) = store
            .read_operation_out_of_queue(move |db_ref| {
                let mut stats_total_keys: u32 = 0;
                let mut stats_total_raw_size: u64 = 0;

                let mut result = Vec::with_capacity(16);
                let started_date =
                    Utc::now() + chrono::Duration::seconds(eviction_min_ttl_threshold);
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                for item in cache_schema.scan_index_values(&CacheItemRocksIndex::ByPath)? {
                    let item = item?;

                    stats_total_keys += 1;

                    let row_size = if let Some(extended) = item.extended {
                        stats_total_raw_size += extended.raw_size as u64;
                        extended.raw_size
                    } else {
                        0
                    };

                    if let Some(ttl) = item.ttl {
                        if ttl < started_date {
                            result.push((item.row_id, row_size));
                        }
                    }
                }

                Ok((result, stats_total_keys, stats_total_raw_size))
            })
            .await?;

        self.stats_total_keys
            .store(stats_total_keys, Ordering::Release);
        self.stats_total_raw_size
            .store(stats_total_raw_size, Ordering::Release);

        Ok(to_delete)
    }

    async fn collect_allkeys_to_evict(
        &self,
        criteria: CacheEvictionWeightCriteria,
        store: &Arc<RocksStore>,
    ) -> Result<Vec<(u64, u32)>, CubeError> {
        let eviction_min_ttl_threshold = self.eviction_min_ttl_threshold as i64;

        let batch_to_delete = store
            .read_operation_out_of_queue(move |db_ref| {
                let started_date =
                    Utc::now() + chrono::Duration::seconds(eviction_min_ttl_threshold);

                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let mut result = Vec::new();

                for item in cache_schema.scan_index_values(&CacheItemRocksIndex::ByPath)? {
                    let item = item?;

                    let (weight, raw_size) = if let Some(extended) = item.extended {
                        let weight = match criteria {
                            CacheEvictionWeightCriteria::ByLRU => {
                                extended.lru.encode_value_as_u32()?
                            }
                            CacheEvictionWeightCriteria::ByTTL => item.ttl.encode_value_as_u32()?,
                            CacheEvictionWeightCriteria::ByLFU => extended.lfu as u32,
                        };

                        (weight, extended.raw_size)
                    } else {
                        (/* height priority to delete */ 0, 0)
                    };

                    if let Some(ttl) = item.ttl {
                        if ttl < started_date {
                            result.push((item.row_id, weight, raw_size));
                            continue;
                        }
                    }

                    result.push((item.row_id, weight, raw_size))
                }

                Ok(result)
            })
            .await?;

        let ttl_sorted: Vec<(u64, /* weight */ u32, /* raw_size */ u32)> = batch_to_delete
            .into_iter()
            .sorted_by(|(_, a, _), (_, b, _)| a.cmp(b))
            .collect();

        let mapped = ttl_sorted
            .into_iter()
            .map(|(id, _weight, raw_size)| (id, raw_size))
            .collect();

        Ok(mapped)
    }

    async fn do_eviction_by_allkeys(
        &self,
        store: &Arc<RocksStore>,
        target: u64,
        target_is_size: bool,
        criteria: CacheEvictionWeightCriteria,
    ) -> Result<EvictionResult, CubeError> {
        let to_evict = self.collect_allkeys_to_evict(criteria, &store).await?;

        let stats_total_keys = self.get_stats_total_keys();
        let stats_total_raw_size = self.get_stats_total_raw_size();

        let mut total_size_removed = 0_u64;
        let mut total_keys_removed = 0_u32;
        let mut total_delete_skipped = 0_u32;

        let mut batch = Vec::with_capacity(self.eviction_batch_size);

        for (id, raw_size) in to_evict {
            batch.push((id, raw_size));

            if batch.len() >= self.eviction_batch_size {
                let current_batch =
                    std::mem::replace(&mut batch, Vec::with_capacity(self.eviction_batch_size));
                let batch_result = self.delete_batch(current_batch, &store).await?;

                total_size_removed += batch_result.deleted_size;
                total_keys_removed += batch_result.deleted_count;
                total_delete_skipped += batch_result.skipped;

                if target_is_size {
                    if total_size_removed >= target {
                        return Ok(EvictionResult::Finished(EvictionFinishedResult {
                            total_keys_removed,
                            total_size_removed,
                            total_delete_skipped,
                            stats_total_keys,
                            stats_total_raw_size,
                        }));
                    }
                } else {
                    if total_keys_removed >= target as u32 {
                        return Ok(EvictionResult::Finished(EvictionFinishedResult {
                            total_keys_removed,
                            total_size_removed,
                            total_delete_skipped,
                            stats_total_keys,
                            stats_total_raw_size,
                        }));
                    }
                }
            }
        }

        return Ok(EvictionResult::Finished(EvictionFinishedResult {
            total_keys_removed,
            total_size_removed,
            total_delete_skipped,
            stats_total_keys,
            stats_total_raw_size,
        }));
    }

    async fn do_eviction_by_sampling(
        &self,
        store: &Arc<RocksStore>,
        target: u64,
        target_is_size: bool,
        criteria: CacheEvictionWeightCriteria,
    ) -> Result<EvictionResult, CubeError> {
        // move
        let eviction_batch_size = self.eviction_batch_size;

        let to_delete: Vec<(u64, u32)> = store
            .read_operation_out_of_queue(move |db_ref| {
                let mut pending_volume_remove: u64 = 0;

                let mut to_delete = Vec::with_capacity(eviction_batch_size);
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                let mut sampling_count = 0;
                let mut sampling_min: Option<(
                    /* row_id */ u64,
                    /* lru */ u32,
                    /* raw_size */ u32,
                )> = None;

                for item in cache_schema.scan_index_values(&CacheItemRocksIndex::ByPath)? {
                    let item = item?;

                    let (weight, raw_size) = if let Some(extended) = item.extended {
                        let weight = match criteria {
                            CacheEvictionWeightCriteria::ByLRU => {
                                extended.lru.encode_value_as_u32()?
                            }
                            CacheEvictionWeightCriteria::ByTTL => item.ttl.encode_value_as_u32()?,
                            CacheEvictionWeightCriteria::ByLFU => extended.lfu as u32,
                        };

                        (weight, extended.raw_size)
                    } else {
                        (/* height priority to delete */ 0, 0)
                    };

                    if let Some((_, min_weight, _)) = sampling_min {
                        if min_weight > weight {
                            sampling_min = Some((item.row_id, weight, raw_size));
                        }
                    } else {
                        sampling_min = Some((item.row_id, weight, raw_size));
                    }

                    sampling_count += 1;

                    if sampling_count == 6 {
                        let (min_id, _, min_raw_size) =
                            sampling_min.take().expect("must contain sample");
                        sampling_count = 0;

                        if target_is_size {
                            pending_volume_remove += min_raw_size as u64;
                        } else {
                            pending_volume_remove += 1;
                        }

                        to_delete.push((min_id, min_raw_size));

                        if pending_volume_remove >= target {
                            return Ok(to_delete);
                        }
                    }
                }

                Ok(to_delete)
            })
            .await?;

        Ok(EvictionResult::Finished(
            self.delete_items(to_delete, &store).await?,
        ))
    }

    async fn do_eviction_by(
        &self,
        store: &Arc<RocksStore>,
        target: u64,
        target_is_size: bool,
    ) -> Result<EvictionResult, CubeError> {
        return match self.eviction_policy {
            CacheEvictionPolicy::AllKeysLru => {
                self.do_eviction_by_allkeys(
                    store,
                    target,
                    target_is_size,
                    self.eviction_policy.to_weight(),
                )
                .await
            }
            CacheEvictionPolicy::AllKeysLfu => {
                self.do_eviction_by_allkeys(
                    store,
                    target,
                    target_is_size,
                    self.eviction_policy.to_weight(),
                )
                .await
            }
            CacheEvictionPolicy::AllKeysTtl => {
                self.do_eviction_by_allkeys(
                    store,
                    target,
                    target_is_size,
                    self.eviction_policy.to_weight(),
                )
                .await
            }
            CacheEvictionPolicy::SampledLru => {
                self.do_eviction_by_sampling(
                    store,
                    target,
                    target_is_size,
                    self.eviction_policy.to_weight(),
                )
                .await
            }
            CacheEvictionPolicy::SampledLfu => {
                self.do_eviction_by_sampling(
                    store,
                    target,
                    target_is_size,
                    self.eviction_policy.to_weight(),
                )
                .await
            }
            CacheEvictionPolicy::SampledTtl => {
                self.do_eviction_by_sampling(
                    store,
                    target,
                    target_is_size,
                    self.eviction_policy.to_weight(),
                )
                .await
            }
        };
    }

    pub async fn run_persist(&self, store: &Arc<RocksStore>) -> Result<(), CubeError> {
        let (to_persist, buffer_len) = {
            let mut ttl_buffer = self.ttl_buffer.write().await;
            trace!("TTL persisting, buffer len: {}", ttl_buffer.len());

            if ttl_buffer.len() >= self.persist_batch_size {
                let mut to_persist = HashMap::with_capacity(self.persist_batch_size);
                let all_keys: Vec<u64> = ttl_buffer
                    .keys()
                    .into_iter()
                    .take(self.persist_batch_size)
                    .map(|k| k.clone())
                    .collect();

                for key in all_keys.into_iter() {
                    if let Some(item) = ttl_buffer.remove(&key) {
                        to_persist.insert(key, item);
                    }
                }

                (to_persist.into_iter(), ttl_buffer.len())
            } else {
                let old_state = std::mem::replace(&mut *ttl_buffer, HashMap::new());
                (old_state.into_iter(), 0)
            }
        };

        app_metrics::CACHESTORE_TTL_PERSIST.add(to_persist.len() as i64);
        app_metrics::CACHESTORE_TTL_BUFFER.report(buffer_len as i64);

        store
            .write_operation(move |db_ref, pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                for (row_id, item) in to_persist.into_iter() {
                    cache_schema.update_extended_ttl_secondary_index(
                        row_id,
                        &CacheItemRocksIndex::ByPath,
                        item.key_hash.to_vec(),
                        RocksSecondaryIndexValueTTLExtended {
                            lfu: item.lfu,
                            lru: item.lru.decode_value_as_opt_datetime()?,
                            raw_size: item.raw_size,
                        },
                        pipe,
                    )?;
                }

                Ok(())
            })
            .await?;

        Ok(())
    }

    pub async fn notify_lookup(&self, id_row: &IdRow<CacheItem>) -> Result<(), CubeError> {
        let event = CacheLookupEvent {
            row_id: id_row.get_id(),
            raw_size: id_row.get_row().get_value().len() as u32,
            key_hash: CacheItemRocksIndex::ByPath
                .key_hash(id_row.get_row())
                .to_be_bytes(),
        };

        if let Err(ref err) = self.ttl_lookup_tx.try_send(event) {
            match &err {
                TrySendError::Full(_) => {
                    log::error!(
                        "Unable to track lookup event for eviction manager: no available capacity"
                    );

                    Ok(())
                }
                TrySendError::Closed(_) => Err(CubeError::internal(
                    "Unable to track lookup event for eviction manager: channel closed".to_string(),
                )),
            }
        } else {
            Ok(())
        }
    }

    pub fn need_to_evict(&self, row_size: u64) -> bool {
        if self.get_stats_total_keys() + 1 > self.limit_max_keys_hard {
            return true;
        }

        if self.get_stats_total_raw_size() + row_size > self.limit_max_size_hard {
            return true;
        }

        false
    }

    pub async fn before_insert(&self, row_size: u64) -> Result<(), CubeError> {
        if self.need_to_evict(row_size) {
            self.eviction_loop.trigger_process();
        }

        Ok(())
    }

    pub fn notify_insert(&self, raw_size: u64) -> Result<(), CubeError> {
        self.stats_total_keys.fetch_add(1, Ordering::Relaxed);
        self.stats_total_raw_size
            .fetch_add(raw_size, Ordering::Relaxed);

        Ok(())
    }

    pub async fn notify_delete(&self, row_id: u64, row_size: u64) -> Result<(), CubeError> {
        let mut guard = self.ttl_buffer.write().await;
        guard.remove(&row_id);

        self.stats_total_keys.fetch_sub(1, Ordering::Relaxed);
        self.stats_total_raw_size
            .fetch_sub(row_size, Ordering::Relaxed);

        Ok(())
    }

    pub async fn truncation_block<'a>(&'a self) -> Result<TruncationBlockGuard<'a>, CubeError> {
        let mut eviction_state_guard = self.eviction_state.write().await;
        *eviction_state_guard = EvictionState::TruncationStarted;
        drop(eviction_state_guard);

        let mut ttl_buffer_guard = self.ttl_buffer.write().await;
        *ttl_buffer_guard = HashMap::new();

        Ok(TruncationBlockGuard {
            _ttl_buffer_guard: ttl_buffer_guard,
        })
    }

    pub async fn notify_truncate_end(&self) {
        self.stats_total_keys.store(0, Ordering::Relaxed);
        self.stats_total_raw_size.store(0, Ordering::Relaxed);

        let mut eviction_state_guard = self.eviction_state.write().await;
        *eviction_state_guard = EvictionState::Ready;
    }
}

#[derive(Debug)]
pub struct TruncationBlockGuard<'a> {
    _ttl_buffer_guard: RwLockWriteGuard<'a, HashMap<u64, CachePolicyData>>,
}
