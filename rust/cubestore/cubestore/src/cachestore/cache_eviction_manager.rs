use crate::cachestore::cache_item::{
    CacheItemRocksIndex, CacheItemRocksTable, CACHE_ITEM_SIZE_WITHOUT_VALUE,
};
use crate::cachestore::CacheItem;
use crate::config::ConfigObj;
use crate::metastore::{
    BaseRocksSecondaryIndex, IdRow, PackedDateTime, RocksSecondaryIndexValueTTLExtended,
    RocksSecondaryIndexValueVersionDecoder, RocksSecondaryIndexValueVersionEncoder, RocksStore,
    RocksTable, SecondaryIndexValueScanIterItem,
};
use crate::util::aborting_join_handle::AbortingJoinHandle;
use crate::util::lock::acquire_lock;
use crate::util::IntervalLoop;
use crate::{app_metrics, CubeError};
use chrono::Utc;
use datafusion::cube_ext;
use deepsize::DeepSizeOf;
use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::RwLockWriteGuard;

#[derive(Debug)]
enum CacheEvent {
    Lookup {
        row_id: u64,
        raw_size: u32,
        key_hash: [u8; 8],
    },
    Delete {
        row_id: u64,
    },
}

#[derive(Debug, Clone, DeepSizeOf)]
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
    Loading,
    EvictionStarted,
    TruncationStarted,
}

impl fmt::Display for EvictionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            EvictionState::Initial => "initial",
            EvictionState::LoadingFailed => "loading failed",
            EvictionState::Ready => "ready",
            EvictionState::Loading => "loading",
            EvictionState::EvictionStarted => "eviction started",
            EvictionState::TruncationStarted => "truncation started",
        })
    }
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

type KeysVector = Vec<(/* row_id */ u64, /* raw_size */ u32)>;

#[derive(Debug)]
pub struct CacheEvictionManager {
    ttl_buffer: Arc<tokio::sync::RwLock<HashMap<u64, CachePolicyData>>>,
    ttl_event_tx: tokio::sync::mpsc::Sender<CacheEvent>,
    pub persist_loop: Arc<IntervalLoop>,
    pub eviction_loop: Arc<IntervalLoop>,
    // eviction state
    eviction_state: tokio::sync::RwLock<EvictionState>,
    eviction_state_notify: tokio::sync::Notify,
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
    /// Proactive deletion of keys with upcoming expiration in the next N seconds. + it checks size, because
    /// possible it can lead to a drop of refresh keys or touch flags
    eviction_proactive_size_threshold: u32,
    eviction_proactive_ttl_threshold: u32,
    compaction_trigger_size: u64,
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

impl EvictionFinishedResult {
    pub fn empty() -> Self {
        Self {
            total_keys_removed: 0,
            total_size_removed: 0,
            total_delete_skipped: 0,
            stats_total_keys: 0,
            stats_total_raw_size: 0,
        }
    }

    pub fn add_eviction_result(&mut self, results: EvictionFinishedResult) {
        self.total_keys_removed += results.total_keys_removed;
        self.total_size_removed += results.total_size_removed;
        self.total_delete_skipped += results.total_delete_skipped;
    }
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
        let (ttl_event_tx, mut ttl_event_rx) =
            tokio::sync::mpsc::channel::<CacheEvent>(config.cachestore_cache_ttl_notify_channel());

        let ttl_buffer_to_move = ttl_buffer.clone();
        let ttl_buffer_max_size = config.cachestore_cache_ttl_buffer_max_size();

        let join_handle = cube_ext::spawn_blocking(move || loop {
            match ttl_event_rx.blocking_recv() {
                Some(CacheEvent::Delete { row_id }) => {
                    let mut ttl_buffer = ttl_buffer_to_move.blocking_write();
                    ttl_buffer.remove(&row_id);
                }
                Some(CacheEvent::Lookup {
                    row_id,
                    key_hash,
                    raw_size,
                }) => {
                    let mut ttl_buffer = ttl_buffer_to_move.blocking_write();
                    if let Some(cache_data) = ttl_buffer.get_mut(&row_id) {
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
                            row_id,
                            CachePolicyData {
                                key_hash,
                                raw_size,
                                lru: Utc::now().encode_value_as_u32().unwrap(),
                                lfu: 1,
                            },
                        );
                    };
                }
                None => {
                    return;
                }
            }
        });

        Self {
            ttl_buffer,
            ttl_event_tx,
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
            eviction_state_notify: tokio::sync::Notify::new(),
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
            eviction_proactive_size_threshold: config
                .cachestore_cache_eviction_proactive_size_threshold(),
            eviction_proactive_ttl_threshold: config
                .cachestore_cache_eviction_proactive_ttl_threshold(),
            compaction_trigger_size: config.cachestore_cache_compaction_trigger_size(),
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
        to_delete: KeysVector,
        store: &Arc<RocksStore>,
        keys_are_expired: bool,
    ) -> Result<EvictionFinishedResult, CubeError> {
        let stats_total_keys = self.get_stats_total_keys();
        let stats_total_raw_size = self.get_stats_total_raw_size();

        let mut total_keys_removed = 0;
        let mut total_size_removed = 0;
        let mut total_delete_skipped = 0;

        let last_batch = if to_delete.len() > self.eviction_batch_size {
            let mut batch = Vec::with_capacity(self.eviction_batch_size);

            for (row_id, raw_size) in to_delete.into_iter() {
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

        if keys_are_expired {
            app_metrics::CACHESTORE_EVICTION_REMOVED_EXPIRED_KEYS.add(total_keys_removed as i64);
            app_metrics::CACHESTORE_EVICTION_REMOVED_EXPIRED_SIZE.add(total_size_removed as i64);
        } else {
            app_metrics::CACHESTORE_EVICTION_REMOVED_KEYS.add(total_keys_removed as i64);
            app_metrics::CACHESTORE_EVICTION_REMOVED_SIZE.add(total_size_removed as i64);
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
            .write_operation("delete_batch", move |db_ref, pipe| {
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

        Ok(DeleteBatchResult {
            deleted_count,
            deleted_size,
            skipped,
        })
    }

    pub async fn run_eviction(&self, store: &Arc<RocksStore>) -> Result<EvictionResult, CubeError> {
        log::debug!(
            "Eviction loop, total_keys: {}, total_size: {}",
            self.get_stats_total_keys(),
            humansize::format_size(self.get_stats_total_raw_size(), humansize::DECIMAL)
        );

        let mut state = self.eviction_state.write().await;
        match *state {
            EvictionState::Initial | EvictionState::LoadingFailed => {
                *state = EvictionState::Loading;
                drop(state);

                let load_result = self.do_load(&store).await;

                let mut state = self.eviction_state.write().await;
                *state = if load_result.is_ok() {
                    EvictionState::Ready
                } else {
                    EvictionState::LoadingFailed
                };
                drop(state);

                self.eviction_state_notify.notify_waiters();

                load_result
            }
            EvictionState::Ready => {
                *state = EvictionState::EvictionStarted;
                drop(state);

                let eviction_result = self.do_eviction(&store).await;

                let mut state = self.eviction_state.write().await;
                *state = EvictionState::Ready;
                drop(state);

                self.eviction_state_notify.notify_waiters();

                eviction_result
            }
            EvictionState::Loading => Ok(EvictionResult::InProgress(
                "loading is in progress".to_string(),
            )),
            EvictionState::EvictionStarted => Ok(EvictionResult::InProgress(
                "eviction is in progress".to_string(),
            )),
            EvictionState::TruncationStarted => Ok(EvictionResult::InProgress(
                "truncation is in progress".to_string(),
            )),
        }
    }

    async fn do_eviction(&self, store: &Arc<RocksStore>) -> Result<EvictionResult, CubeError> {
        let stats_total_keys = self.get_stats_total_keys();
        let eviction_fut = if stats_total_keys > self.limit_max_keys_soft {
            let need_to_evict = (stats_total_keys - self.limit_max_keys_soft) as u64;
            let target =
                need_to_evict + calc_percentage(need_to_evict, self.eviction_below_threshold);

            log::debug!(
                "Max keys limit eviction: {} > {}, need to evict: {}, threshold: {}, target: {}",
                stats_total_keys,
                self.limit_max_keys_soft,
                need_to_evict,
                self.eviction_below_threshold,
                target
            );

            self.do_eviction_by(&store, target, false)
        } else {
            let stats_total_raw_siz = self.get_stats_total_raw_size();
            if stats_total_raw_siz > self.limit_max_size_soft {
                let need_to_evict = (stats_total_raw_siz - self.limit_max_size_soft) as u64;
                let target =
                    need_to_evict + calc_percentage(need_to_evict, self.eviction_below_threshold);

                log::debug!(
                    "Max size limit eviction: {} > {}, need to evict: {}, threshold: {}, target: {}",
                    humansize::format_size(stats_total_raw_siz, humansize::DECIMAL),
                    humansize::format_size(self.limit_max_size_soft, humansize::DECIMAL),
                    humansize::format_size(need_to_evict, humansize::DECIMAL),
                    self.eviction_below_threshold,
                    humansize::format_size(target, humansize::DECIMAL)
                );

                self.do_eviction_by(&store, target, true)
            } else {
                log::trace!("Nothing to evict");

                self.check_compaction_trigger(&store).await;

                return Ok(EvictionResult::Finished(EvictionFinishedResult {
                    total_keys_removed: 0,
                    total_size_removed: 0,
                    total_delete_skipped: 0,
                    stats_total_keys: self.get_stats_total_keys(),
                    stats_total_raw_size: self.get_stats_total_raw_size(),
                }));
            }
        };

        let result = eviction_fut.await?;

        self.check_compaction_trigger(&store).await;

        log::debug!(
            "Eviction finished, total_keys: {}, total_size: {}",
            self.get_stats_total_keys(),
            humansize::format_size(self.get_stats_total_raw_size(), humansize::DECIMAL)
        );

        Ok(result)
    }

    async fn do_load(&self, store: &Arc<RocksStore>) -> Result<EvictionResult, CubeError> {
        let mut total_keys_removed = 0;
        let mut total_size_removed = 0;
        let mut total_delete_skipped = 0;

        let expired_items = self.collect_stats_and_expired_keys(&store).await?;
        let expired_len = expired_items.len();

        if expired_len > 0 {
            let deletion_result = self.delete_items(expired_items, &store, true).await?;

            total_keys_removed = deletion_result.total_keys_removed;
            total_size_removed = deletion_result.total_size_removed;
            total_delete_skipped = deletion_result.total_delete_skipped;
        }

        log::trace!(
            "Eviction loaded stats, total_keys: {}, total_size: {}, expired: {}",
            self.get_stats_total_keys(),
            self.get_stats_total_raw_size(),
            expired_len
        );

        Ok(EvictionResult::Finished(EvictionFinishedResult {
            total_keys_removed,
            total_size_removed,
            total_delete_skipped,
            stats_total_keys: self.get_stats_total_keys(),
            stats_total_raw_size: self.get_stats_total_raw_size(),
        }))
    }

    async fn collect_stats_and_expired_keys(
        &self,
        store: &Arc<RocksStore>,
    ) -> Result<KeysVector, CubeError> {
        let (expired, stats_total_keys, stats_total_raw_size) = store
            .read_operation_out_of_queue("collect_stats_and_expired_keys", move |db_ref| {
                let mut stats_total_keys: u32 = 0;
                let mut stats_total_raw_size: u64 = 0;

                let now = Utc::now();
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                let mut expired = KeysVector::new();

                for item in cache_schema.scan_index_values(&CacheItemRocksIndex::ByPath)? {
                    let item = item?;

                    let row_size = if let Some(extended) = item.extended {
                        extended.raw_size
                    } else {
                        CACHE_ITEM_SIZE_WITHOUT_VALUE
                    };

                    stats_total_keys += 1;
                    stats_total_raw_size += row_size as u64;

                    if let Some(ttl) = item.ttl {
                        if ttl < now {
                            expired.push((item.row_id, row_size));
                        }
                    }
                }

                Ok((expired, stats_total_keys, stats_total_raw_size))
            })
            .await?;

        self.stats_total_keys
            .store(stats_total_keys, Ordering::Release);
        self.stats_total_raw_size
            .store(stats_total_raw_size, Ordering::Release);

        Ok(expired)
    }

    async fn collect_allkeys_to_evict(
        &self,
        criteria: CacheEvictionWeightCriteria,
        store: &Arc<RocksStore>,
    ) -> Result<(KeysVector, KeysVector), CubeError> {
        let eviction_proactive_ttl_threshold = self.eviction_proactive_ttl_threshold;
        let eviction_proactive_size_threshold = self.eviction_proactive_size_threshold;

        let (all_keys, stats_total_keys, stats_total_raw_size, expired_keys) = store
            .read_operation_out_of_queue("collect_allkeys_to_evict", move |db_ref| {
                let mut stats_total_keys: u32 = 0;
                let mut stats_total_raw_size: u64 = 0;

                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                let now_at_start = Utc::now();

                let mut expired_keys = KeysVector::with_capacity(64);
                let mut all_keys: Vec<(
                    /* id */ u64,
                    /* weight */ u32,
                    /* raw_size */ u32,
                )> = Vec::with_capacity(64);

                for item in cache_schema.scan_index_values(&CacheItemRocksIndex::ByPath)? {
                    let item = item?;

                    let (weight, raw_size) =
                        Self::get_weight_and_size_by_criteria(&item, &criteria)?;

                    // We need to count expired keys too for correct stats!
                    stats_total_keys += 1;
                    stats_total_raw_size += raw_size as u64;

                    if let Some(ttl) = item.ttl {
                        let ready_to_delete = if ttl <= now_at_start {
                            true
                        } else if ttl - now_at_start
                            <= chrono::Duration::seconds(eviction_proactive_ttl_threshold as i64)
                        {
                            // Checking the size of the key, because it can be problematic to delete keys with small size, because
                            // it can be a refresh key.
                            raw_size > eviction_proactive_size_threshold
                        } else {
                            false
                        };

                        if ready_to_delete {
                            expired_keys.push((item.row_id, raw_size));
                            continue;
                        }
                    }

                    all_keys.push((item.row_id, weight, raw_size))
                }

                Ok((
                    all_keys,
                    stats_total_keys,
                    stats_total_raw_size,
                    expired_keys,
                ))
            })
            .await?;

        self.stats_total_keys
            .store(stats_total_keys, Ordering::Release);
        self.stats_total_raw_size
            .store(stats_total_raw_size, Ordering::Release);

        let sorted: KeysVector = all_keys
            .into_iter()
            .sorted_by(|(_, a, _), (_, b, _)| a.cmp(b))
            .map(|(id, _weight, raw_size)| (id, raw_size))
            .collect();

        Ok((sorted, expired_keys))
    }

    async fn do_eviction_by_allkeys(
        &self,
        store: &Arc<RocksStore>,
        target: u64,
        target_is_size: bool,
        criteria: CacheEvictionWeightCriteria,
    ) -> Result<EvictionResult, CubeError> {
        let (all_keys, expired) = self.collect_allkeys_to_evict(criteria, &store).await?;

        let mut pending_keys_removed = 0_u32;
        let mut pending_size_removed = 0_u64;

        let stats_total_keys = self.get_stats_total_keys();
        let stats_total_raw_size = self.get_stats_total_raw_size();
        let mut result = EvictionFinishedResult {
            total_keys_removed: 0,
            total_size_removed: 0,
            total_delete_skipped: 0,
            stats_total_keys,
            stats_total_raw_size,
        };

        if expired.len() > 0 {
            let deletion_result = self.delete_items(expired, &store, true).await?;
            result.add_eviction_result(deletion_result);
        }

        let mut pending = Vec::with_capacity(self.eviction_batch_size);

        for (id, raw_size) in all_keys {
            pending_size_removed += raw_size as u64;
            pending_keys_removed += 1;

            pending.push((id, raw_size));

            let target_reached = if target_is_size {
                pending_size_removed >= target
            } else {
                pending_keys_removed >= (target as u32)
            };

            if target_reached {
                let deletion_result = self.delete_items(pending, &store, false).await?;
                result.add_eviction_result(deletion_result);

                return Ok(EvictionResult::Finished(result));
            }
        }

        log::error!("Inconsistency eviction. Unable to reach target on eviction with all keys policy, target: {}", if target_is_size {
            humansize::format_size(target, humansize::DECIMAL)
        } else {
            target.to_string()
        });

        let deletion_result = self.delete_items(pending, &store, false).await?;
        result.add_eviction_result(deletion_result);

        Ok(EvictionResult::Finished(result))
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
        let eviction_proactive_ttl_threshold = self.eviction_proactive_ttl_threshold;
        let eviction_proactive_size_threshold = self.eviction_proactive_size_threshold;

        let to_delete: Vec<(u64, u32)> = store
            .read_operation_out_of_queue("do_eviction_by_sampling", move |db_ref| {
                let mut pending_volume_remove: u64 = 0;

                let now_at_start = Utc::now();
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

                    let (weight, raw_size) =
                        Self::get_weight_and_size_by_criteria(&item, &criteria)?;

                    if let Some(ttl) = item.ttl {
                        let ready_to_delete = if ttl < now_at_start {
                            true
                        } else if ttl - now_at_start
                            <= chrono::Duration::seconds(eviction_proactive_ttl_threshold as i64)
                        {
                            // Checking the size of the key, because it can be problematic to delete keys with small size, because
                            // it can be a refresh key.
                            raw_size > eviction_proactive_size_threshold
                        } else {
                            false
                        };

                        if ready_to_delete {
                            if target_is_size {
                                pending_volume_remove += raw_size as u64;
                            } else {
                                pending_volume_remove += 1;
                            }

                            to_delete.push((item.row_id, raw_size));
                            continue;
                        }
                    }

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
            self.delete_items(to_delete, &store, false).await?,
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
            log::debug!(
                "TTL persisting, len: {}, size: {}",
                ttl_buffer.len(),
                humansize::format_size(ttl_buffer.deep_size_of(), humansize::DECIMAL)
            );

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
            .write_operation("persist_ttl", move |db_ref, pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                for (row_id, item) in to_persist.into_iter() {
                    cache_schema.update_extended_ttl_secondary_index(
                        row_id,
                        &CacheItemRocksIndex::ByPath,
                        item.key_hash,
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

    fn send_ttl_event(&self, event: CacheEvent) -> Result<(), CubeError> {
        if let Err(ref err) = self.ttl_event_tx.try_send(event) {
            match &err {
                TrySendError::Full(_) => {
                    log::error!(
                        "Unable to track event for eviction manager: no available capacity"
                    );

                    Ok(())
                }
                TrySendError::Closed(_) => Err(CubeError::internal(
                    "Unable to track event for eviction manager: channel closed".to_string(),
                )),
            }
        } else {
            Ok(())
        }
    }

    pub fn notify_lookup(&self, id_row: &IdRow<CacheItem>) -> Result<(), CubeError> {
        self.send_ttl_event(CacheEvent::Lookup {
            row_id: id_row.get_id(),
            raw_size: id_row.get_row().get_value().len() as u32,
            key_hash: CacheItemRocksIndex::ByPath
                .key_hash(id_row.get_row())
                .to_be_bytes(),
        })
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

    pub fn notify_delete(&self, row_id: u64, row_size: u64) -> Result<(), CubeError> {
        self.stats_total_keys.fetch_sub(1, Ordering::Relaxed);
        self.stats_total_raw_size
            .fetch_sub(row_size, Ordering::Relaxed);

        self.send_ttl_event(CacheEvent::Delete { row_id })
    }

    async fn start_eviction_state(&self, next_state: EvictionState) -> Result<(), CubeError> {
        for _ in 0..5 {
            let mut eviction_state_guard =
                acquire_lock("eviction state", self.eviction_state.write()).await?;
            match &*eviction_state_guard {
                EvictionState::Initial | EvictionState::LoadingFailed | EvictionState::Ready => {
                    *eviction_state_guard = next_state;
                    drop(eviction_state_guard);

                    return Ok(());
                }
                _ => {
                    drop(eviction_state_guard);
                    self.eviction_state_notify.notified().await;
                }
            }
        }

        Err(CubeError::internal(format!(
            "Can't start {} state",
            next_state
        )))
    }

    async fn end_eviction_state(&self, next_state: EvictionState) -> Result<(), CubeError> {
        let mut eviction_state_guard =
            acquire_lock("eviction state", self.eviction_state.write()).await?;
        *eviction_state_guard = next_state;

        Ok(())
    }

    pub async fn truncation_block<'a>(&'a self) -> Result<TruncationBlockGuard<'a>, CubeError> {
        self.start_eviction_state(EvictionState::TruncationStarted)
            .await?;

        let mut ttl_buffer_guard = match acquire_lock("ttl buffer", self.ttl_buffer.write()).await {
            Ok(r) => r,
            Err(err) => {
                self.end_eviction_state(EvictionState::Ready).await?;
                return Err(err);
            }
        };
        *ttl_buffer_guard = HashMap::new();

        Ok(TruncationBlockGuard {
            _ttl_buffer_guard: ttl_buffer_guard,
        })
    }

    pub async fn notify_truncate_end(&self) -> Result<(), CubeError> {
        self.stats_total_keys.store(0, Ordering::Relaxed);
        self.stats_total_raw_size.store(0, Ordering::Relaxed);

        self.end_eviction_state(EvictionState::Ready).await
    }

    #[inline]
    fn get_weight_and_size_by_criteria(
        item: &SecondaryIndexValueScanIterItem,
        criteria: &CacheEvictionWeightCriteria,
    ) -> Result<(u32, u32), CubeError> {
        if let Some(extended) = &item.extended {
            let weight = match criteria {
                CacheEvictionWeightCriteria::ByLRU => extended.lru.encode_value_as_u32()?,
                CacheEvictionWeightCriteria::ByTTL => item.ttl.encode_value_as_u32()?,
                CacheEvictionWeightCriteria::ByLFU => extended.lfu as u32,
            };

            Ok((weight, extended.raw_size))
        } else {
            let weight = match criteria {
                CacheEvictionWeightCriteria::ByLRU =>
                /* height priority to delete */
                {
                    0
                }
                CacheEvictionWeightCriteria::ByTTL => item.ttl.encode_value_as_u32()?,
                CacheEvictionWeightCriteria::ByLFU =>
                /* height priority to delete */
                {
                    0
                }
            };

            Ok((weight, CACHE_ITEM_SIZE_WITHOUT_VALUE))
        }
    }

    async fn check_compaction_trigger(&self, store: &Arc<RocksStore>) {
        let default_cf_metadata = store.db.get_column_family_metadata();

        log::trace!(
            "Compaction auto trigger, CF default size: {}",
            humansize::format_size(default_cf_metadata.size, humansize::DECIMAL)
        );

        if default_cf_metadata.size > self.compaction_trigger_size {
            log::debug!(
                "Triggering compaction, CF default size: {} > {}",
                humansize::format_size(default_cf_metadata.size, humansize::DECIMAL),
                humansize::format_size(self.compaction_trigger_size, humansize::DECIMAL)
            );

            let _ = store
                .read_operation_out_of_queue_opt(
                    "check_compaction_trigger",
                    |db_ref| {
                        let start: Option<&[u8]> = None;
                        let end: Option<&[u8]> = None;

                        db_ref.db.compact_range(start, end);

                        Ok(())
                    },
                    Duration::from_secs(60),
                )
                .await;
        }
    }
}

#[derive(Debug)]
pub struct TruncationBlockGuard<'a> {
    _ttl_buffer_guard: RwLockWriteGuard<'a, HashMap<u64, CachePolicyData>>,
}
