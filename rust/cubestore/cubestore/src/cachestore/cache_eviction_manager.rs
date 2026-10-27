use crate::cachestore::cache_item::{
    CacheItemRocksIndex, CacheItemRocksTable, CACHE_ITEM_SIZE_WITHOUT_VALUE,
};
use crate::cachestore::ttl_buffer::TTLBuffer;
use crate::cachestore::CacheItem;
use crate::config::ConfigObj;
use crate::metastore::{
    BaseRocksSecondaryIndex, IdRow, RocksSecondaryIndex, RocksSecondaryIndexValueTTLExtended,
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
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
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
    DeleteBatch {
        row_ids: Vec<u64>,
    },
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
            &"sampled-lfu" => Ok(CacheEvictionPolicy::SampledLfu),
            &"sampled-ttl" => Ok(CacheEvictionPolicy::SampledTtl),
            other => Err(CubeError::user(format!(
                "Unsupported cache eviction type: {}",
                other
            ))),
        }
    }
}

/// Starting at 10 instead of 0 protects newly inserted keys from immediate eviction,
/// giving them a grace period to accumulate real access frequency before they can be
/// considered low-frequency candidates.
/// TODO: Implement such protection
pub const LFU_INIT_VAL: u8 = 10;

/// Decay the LFU counter based on elapsed time.
/// Subtracts `elapsed_minutes / decay_time` from counter, saturating at 0.
fn lfu_decay_counter(counter: u8, elapsed_minutes: u32, decay_time: u32) -> u8 {
    if decay_time == 0 || elapsed_minutes == 0 {
        return counter;
    }

    let decrement = elapsed_minutes / decay_time;
    counter.saturating_sub(u8::try_from(decrement).unwrap_or(u8::MAX))
}

/// Applies `hits` logarithmic probabilistic increments to the LFU counter.
/// Higher counters have exponentially a lower probability of incrementing.
///
/// Expected hits needed to increment (with default log_factor=10, LFU_INIT_VAL=10):
///
/// | counter | base_val | p = 1/(base_val*lf+1) | avg hits to incr | cumulative hits to reach |
/// |---------|----------|-----------------------|-------------------|--------------------------|
/// |      10 |        0 | 1.000                 |                 1 |                       10 |
/// |      11 |        1 | 0.0909                |                11 |                       11 |
/// |      15 |        5 | 0.0196                |                51 |                      115 |
/// |      20 |       10 | 0.0099                |               101 |                      470 |
/// |      50 |       40 | 0.00249               |               401 |                    7_850 |
/// |     100 |       90 | 0.00111               |               901 |                   40_150 |
/// |     200 |      190 | 0.000526              |              1901 |                  179_750 |
/// |     255 |        - | 0 (capped)            |                 - |                        - |
///
/// Cumulative formula: `LFU_INIT_VAL + lf * n*(n-1)/2 + n` where `n = counter - LFU_INIT_VAL`.
/// For counter <= LFU_INIT_VAL, cumulative = counter (each step is guaranteed).
///
/// Instead of rolling once per hit, it draws the number of hits needed for the next increment from
/// a geometric distribution, so the cost is bounded by the counter range, not by `hits`: a hot key
/// can collect 10^5 hits between persists, and this runs inside a write operation.
fn lfu_apply_hits(mut counter: u8, mut hits: u32, log_factor: u8, rng: &mut impl Rng) -> u8 {
    while hits > 0 && counter < 255 {
        let base_val = counter.saturating_sub(LFU_INIT_VAL) as f64;
        let p = 1.0 / (base_val * log_factor as f64 + 1.0);

        let needed = if p >= 1.0 {
            1
        } else {
            // u in (0, 1], so ln(u) <= 0 and needed >= 1
            let u: f64 = 1.0 - rng.gen::<f64>();
            (u.ln() / (1.0 - p).ln()).ceil().max(1.0) as u64
        };

        if needed > hits as u64 {
            break;
        }

        hits -= needed as u32;
        counter += 1;
    }

    counter
}

#[derive(Debug, Clone, Copy)]
struct EvictionKey {
    row_id: u64,
    raw_size: u32,
}

type KeysVector = Vec<EvictionKey>;

#[derive(Debug, Clone, Copy)]
struct WeightedEvictionKey {
    key: EvictionKey,
    size_class_rank: u8,
    /// Lower weight is evicted first
    weight: u32,
}

impl WeightedEvictionKey {
    /// Lower is evicted first
    fn eviction_order(&self) -> (u8, u32) {
        (self.size_class_rank, self.weight)
    }
}

/// Size-limit eviction drains the largest size class first. Small keys free almost no memory
/// but are often state (pre-aggregation touch marks, refresh keys, locks), so plain LRU would
/// drop them for nothing. `size_classes` are strictly increasing boundaries; empty = one class.
fn size_class_rank(size_classes: &[u32], raw_size: u32) -> u8 {
    let class = size_classes
        .iter()
        .take_while(|boundary| raw_size >= **boundary)
        .count();

    (size_classes.len() - class) as u8
}

#[derive(Debug, Default)]
struct KeysStats {
    total_keys: u32,
    total_raw_size: u64,
}

impl KeysStats {
    fn add(&mut self, raw_size: u32) {
        self.total_keys += 1;
        self.total_raw_size += raw_size as u64;
    }
}

#[derive(Debug)]
struct ExpiredKeysScan {
    expired: KeysVector,
    stats: KeysStats,
}

#[derive(Debug)]
struct AllKeysScan {
    candidates: Vec<WeightedEvictionKey>,
    expired: KeysVector,
    stats: KeysStats,
}

#[derive(Debug)]
struct AllKeysToEvict {
    /// Sorted by weight, the first key is evicted first
    sorted: KeysVector,
    expired: KeysVector,
}

#[derive(Debug)]
pub struct CacheEvictionManager {
    ttl_buffer: Arc<tokio::sync::RwLock<TTLBuffer>>,
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
    eviction_size_classes: Vec<u32>,
    compaction_trigger_size: u64,
    // LFU configuration
    lfu_decay_time: u32,
    lfu_log_factor: u8,
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
        let ttl_buffer = Arc::new(tokio::sync::RwLock::new(TTLBuffer::new()));
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
                Some(CacheEvent::DeleteBatch { row_ids }) => {
                    let mut ttl_buffer = ttl_buffer_to_move.blocking_write();
                    for row_id in row_ids {
                        ttl_buffer.remove(&row_id);
                    }
                }
                Some(CacheEvent::Lookup {
                    row_id,
                    key_hash,
                    raw_size,
                }) => {
                    let mut ttl_buffer = ttl_buffer_to_move.blocking_write();
                    ttl_buffer.on_lookup(ttl_buffer_max_size, row_id, key_hash, raw_size);
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
            eviction_size_classes: config.cachestore_cache_eviction_size_classes().to_vec(),
            compaction_trigger_size: config.cachestore_cache_compaction_trigger_size(),
            lfu_decay_time: config.cachestore_cache_lfu_decay_time(),
            lfu_log_factor: config.cachestore_cache_lfu_log_factor(),
            //
            _ttl_tl_loop_join_handle: Arc::new(AbortingJoinHandle::new(join_handle)),
        }
    }

    #[cfg(test)]
    pub async fn get_ttl_buffer_hits(&self, row_id: u64) -> Option<u32> {
        self.ttl_buffer.read().await.get(&row_id).map(|d| d.hits)
    }

    #[cfg(test)]
    pub async fn set_ttl_buffer_last_access(
        &self,
        row_id: u64,
        last_access: crate::metastore::PackedDateTime,
    ) {
        if let Some(data) = self.ttl_buffer.write().await.get_mut(&row_id) {
            data.last_access = last_access;
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

            for key in to_delete.into_iter() {
                batch.push(key);

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
        batch: KeysVector,
        store: &Arc<RocksStore>,
    ) -> Result<DeleteBatchResult, CubeError> {
        let (result, deleted_row_ids) = store
            .write_operation("delete_batch", move |db_ref, pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                let mut deleted_row_ids = Vec::with_capacity(batch.len());
                let mut result = DeleteBatchResult {
                    deleted_count: 0,
                    deleted_size: 0,
                    skipped: 0,
                };

                for key in batch {
                    if let Some(_) = cache_schema.try_delete(key.row_id, pipe)? {
                        deleted_row_ids.push(key.row_id);
                        result.deleted_count += 1;
                        result.deleted_size += key.raw_size as u64;
                    } else {
                        result.skipped += 1;
                    };
                }

                Ok((result, deleted_row_ids))
            })
            .await?;

        self.stats_total_keys
            .fetch_sub(result.deleted_count, Ordering::Relaxed);
        self.stats_total_raw_size
            .fetch_sub(result.deleted_size, Ordering::Relaxed);

        if !deleted_row_ids.is_empty() {
            self.send_ttl_event(CacheEvent::DeleteBatch {
                row_ids: deleted_row_ids,
            })?;
        }

        Ok(result)
    }

    fn store_stats(&self, stats: &KeysStats) {
        self.stats_total_keys
            .store(stats.total_keys, Ordering::Release);
        self.stats_total_raw_size
            .store(stats.total_raw_size, Ordering::Release);
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
                    "Max size limit eviction: {} > {}, need to evict: {}, threshold: {}, target: {}, size classes: {:?}",
                    humansize::format_size(stats_total_raw_siz, humansize::DECIMAL),
                    humansize::format_size(self.limit_max_size_soft, humansize::DECIMAL),
                    humansize::format_size(need_to_evict, humansize::DECIMAL),
                    self.eviction_below_threshold,
                    humansize::format_size(target, humansize::DECIMAL),
                    self.eviction_size_classes
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
        let scan = store
            .read_operation_out_of_queue("collect_stats_and_expired_keys", move |db_ref| {
                let now = Utc::now();
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                let mut scan = ExpiredKeysScan {
                    expired: KeysVector::new(),
                    stats: KeysStats::default(),
                };

                for item in cache_schema.scan_index_values(&CacheItemRocksIndex::ByPath)? {
                    let item = item?;

                    let raw_size = if let Some(extended) = item.extended {
                        extended.raw_size
                    } else {
                        CACHE_ITEM_SIZE_WITHOUT_VALUE
                    };

                    scan.stats.add(raw_size);

                    if let Some(ttl) = item.ttl {
                        if ttl < now {
                            scan.expired.push(EvictionKey {
                                row_id: item.row_id,
                                raw_size,
                            });
                        }
                    }
                }

                Ok(scan)
            })
            .await?;

        self.store_stats(&scan.stats);

        Ok(scan.expired)
    }

    /// Size classes only make sense for the size limit: under the keys limit
    /// a small key counts the same as a large one.
    fn size_classes_for(&self, target_is_size: bool) -> Vec<u32> {
        if target_is_size {
            self.eviction_size_classes.clone()
        } else {
            vec![]
        }
    }

    async fn collect_allkeys_to_evict(
        &self,
        criteria: CacheEvictionWeightCriteria,
        target_is_size: bool,
        store: &Arc<RocksStore>,
    ) -> Result<AllKeysToEvict, CubeError> {
        let eviction_proactive_ttl_threshold = self.eviction_proactive_ttl_threshold;
        let eviction_proactive_size_threshold = self.eviction_proactive_size_threshold;
        let lfu_decay_time = self.lfu_decay_time;
        let size_classes = self.size_classes_for(target_is_size);

        let scan = store
            .read_operation_out_of_queue("collect_allkeys_to_evict", move |db_ref| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                let now_at_start = Utc::now();

                let mut scan = AllKeysScan {
                    candidates: Vec::with_capacity(64),
                    expired: KeysVector::with_capacity(64),
                    stats: KeysStats::default(),
                };

                for item in cache_schema.scan_index_values(&CacheItemRocksIndex::ByPath)? {
                    let item = item?;

                    let candidate = Self::weighted_key_by_criteria(
                        &item,
                        &criteria,
                        lfu_decay_time,
                        &size_classes,
                    )?;
                    let raw_size = candidate.key.raw_size;

                    // We need to count expired keys too for correct stats!
                    scan.stats.add(raw_size);

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
                            scan.expired.push(candidate.key);
                            continue;
                        }
                    }

                    scan.candidates.push(candidate)
                }

                Ok(scan)
            })
            .await?;

        self.store_stats(&scan.stats);

        let sorted: KeysVector = scan
            .candidates
            .into_iter()
            .sorted_by_key(|candidate| candidate.eviction_order())
            .map(|candidate| candidate.key)
            .collect();

        Ok(AllKeysToEvict {
            sorted,
            expired: scan.expired,
        })
    }

    async fn do_eviction_by_allkeys(
        &self,
        store: &Arc<RocksStore>,
        target: u64,
        target_is_size: bool,
        criteria: CacheEvictionWeightCriteria,
    ) -> Result<EvictionResult, CubeError> {
        let AllKeysToEvict { sorted, expired } = self
            .collect_allkeys_to_evict(criteria, target_is_size, &store)
            .await?;

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

        for key in sorted {
            pending_size_removed += key.raw_size as u64;
            pending_keys_removed += 1;

            pending.push(key);

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
        let lfu_decay_time = self.lfu_decay_time;
        let size_classes = self.size_classes_for(target_is_size);

        let to_delete: KeysVector = store
            .read_operation_out_of_queue("do_eviction_by_sampling", move |db_ref| {
                let mut pending_volume_remove: u64 = 0;

                let now_at_start = Utc::now();
                let mut to_delete = KeysVector::with_capacity(eviction_batch_size);

                let cache_schema = CacheItemRocksTable::new(db_ref.clone());

                let mut sampling_count = 0;
                let mut sampling_min: Option<WeightedEvictionKey> = None;

                for item in cache_schema.scan_index_values(&CacheItemRocksIndex::ByPath)? {
                    let item = item?;

                    let candidate = Self::weighted_key_by_criteria(
                        &item,
                        &criteria,
                        lfu_decay_time,
                        &size_classes,
                    )?;
                    let raw_size = candidate.key.raw_size;

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

                            to_delete.push(candidate.key);
                            continue;
                        }
                    }

                    if let Some(min) = &sampling_min {
                        if min.eviction_order() > candidate.eviction_order() {
                            sampling_min = Some(candidate);
                        }
                    } else {
                        sampling_min = Some(candidate);
                    }

                    sampling_count += 1;

                    if sampling_count == 6 {
                        let min = sampling_min.take().expect("must contain sample");
                        sampling_count = 0;

                        if target_is_size {
                            pending_volume_remove += min.key.raw_size as u64;
                        } else {
                            pending_volume_remove += 1;
                        }

                        to_delete.push(min.key);

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

            let to_persist = ttl_buffer.take_batch(self.persist_batch_size);
            (to_persist, ttl_buffer.len())
        };

        app_metrics::CACHESTORE_TTL_PERSIST.add(to_persist.len() as i64);
        app_metrics::CACHESTORE_TTL_BUFFER.report(buffer_len as i64);

        let lfu_decay_time = self.lfu_decay_time;
        let lfu_log_factor = self.lfu_log_factor;

        store
            .write_operation("persist_ttl", move |db_ref, pipe| {
                let cache_schema = CacheItemRocksTable::new(db_ref.clone());
                let mut rng = rand::thread_rng();

                for (row_id, item) in to_persist.into_iter() {
                    let first_access = item.first_access.decode_value_as_opt_datetime()?;
                    let last_access = item.last_access.decode_value_as_opt_datetime()?;

                    cache_schema.update_extended_ttl_secondary_index(
                        row_id,
                        &CacheItemRocksIndex::ByPath,
                        item.key_hash,
                        |stored| {
                            let base = match (stored, first_access) {
                                (Some(stored), Some(first_access)) => {
                                    let elapsed_minutes = stored.lru.map_or(0, |lru| {
                                        first_access.signed_duration_since(lru).num_minutes().max(0)
                                            as u32
                                    });
                                    lfu_decay_counter(stored.lfu, elapsed_minutes, lfu_decay_time)
                                }
                                (Some(stored), None) => stored.lfu,
                                (None, _) => LFU_INIT_VAL,
                            };

                            // cache_set can rewrite the row (same row_id) after the lookup was
                            // buffered: the stored lru/raw_size are then newer than the buffered ones
                            let lru = match (stored.and_then(|s| s.lru), last_access) {
                                (Some(stored_lru), Some(last_access)) => {
                                    Some(stored_lru.max(last_access))
                                }
                                (stored_lru, last_access) => last_access.or(stored_lru),
                            };

                            Ok(RocksSecondaryIndexValueTTLExtended {
                                lfu: lfu_apply_hits(base, item.hits, lfu_log_factor, &mut rng),
                                lru,
                                raw_size: stored.map_or(item.raw_size, |s| s.raw_size),
                            })
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
            raw_size: CacheItemRocksIndex::ByPath.raw_value_size(id_row.get_row()),
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
        *ttl_buffer_guard = TTLBuffer::new();

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
    fn weighted_key_by_criteria(
        item: &SecondaryIndexValueScanIterItem,
        criteria: &CacheEvictionWeightCriteria,
        lfu_decay_time: u32,
        size_classes: &[u32],
    ) -> Result<WeightedEvictionKey, CubeError> {
        if let Some(extended) = &item.extended {
            let weight = match criteria {
                CacheEvictionWeightCriteria::ByLRU => extended.lru.encode_value_as_u32()?,
                CacheEvictionWeightCriteria::ByTTL => item.ttl.encode_value_as_u32()?,
                CacheEvictionWeightCriteria::ByLFU => {
                    let elapsed_minutes = if let Some(lru_dt) = extended.lru {
                        Utc::now()
                            .signed_duration_since(lru_dt)
                            .num_minutes()
                            .max(0) as u32
                    } else {
                        0
                    };
                    lfu_decay_counter(extended.lfu, elapsed_minutes, lfu_decay_time) as u32
                }
            };

            Ok(WeightedEvictionKey {
                key: EvictionKey {
                    row_id: item.row_id,
                    raw_size: extended.raw_size,
                },
                size_class_rank: size_class_rank(size_classes, extended.raw_size),
                weight,
            })
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

            Ok(WeightedEvictionKey {
                key: EvictionKey {
                    row_id: item.row_id,
                    raw_size: CACHE_ITEM_SIZE_WITHOUT_VALUE,
                },
                size_class_rank: size_class_rank(size_classes, CACHE_ITEM_SIZE_WITHOUT_VALUE),
                weight,
            })
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
    _ttl_buffer_guard: RwLockWriteGuard<'a, TTLBuffer>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lfu_decay_no_time() {
        assert_eq!(lfu_decay_counter(50, 0, 1), 50);
    }

    #[test]
    fn test_lfu_decay_basic() {
        // 10 elapsed minutes / 1 decay_time = subtract 10
        assert_eq!(lfu_decay_counter(100, 10, 1), 90);
        // 30 elapsed / 10 decay = subtract 3
        assert_eq!(lfu_decay_counter(50, 30, 10), 47);
    }

    #[test]
    fn test_lfu_decay_clamps() {
        // Saturating subtraction to 0
        assert_eq!(lfu_decay_counter(5, 100, 1), 0);
        assert_eq!(lfu_decay_counter(0, 10, 1), 0);
    }

    #[test]
    fn test_lfu_decay_large_decrement_does_not_wrap() {
        // 256 decay periods would wrap to 0 with an `as u8` cast
        assert_eq!(lfu_decay_counter(255, 256 * 60, 60), 0);
        assert_eq!(lfu_decay_counter(255, u32::MAX, 1), 0);
    }

    #[test]
    fn test_lfu_decay_disabled() {
        // decay_time=0 means decay is disabled
        assert_eq!(lfu_decay_counter(100, 999, 0), 100);
    }

    #[test]
    fn test_lfu_apply_hits_no_hits() {
        assert_eq!(lfu_apply_hits(50, 0, 10, &mut rand::thread_rng()), 50);
    }

    #[test]
    fn test_lfu_apply_hits_at_max() {
        assert_eq!(lfu_apply_hits(255, 1, 10, &mut rand::thread_rng()), 255);
        assert_eq!(
            lfu_apply_hits(255, u32::MAX, 10, &mut rand::thread_rng()),
            255
        );
    }

    #[test]
    fn test_lfu_apply_hits_low_counter() {
        // Counters at or below LFU_INIT_VAL have base_val=0, so p=1.0 -> every hit increments
        let mut rng = rand::thread_rng();
        for counter in 0..=LFU_INIT_VAL {
            assert_eq!(
                lfu_apply_hits(counter, 1, 10, &mut rng),
                counter + 1,
                "counter {} should always increment",
                counter
            );
        }

        assert_eq!(lfu_apply_hits(0, 3, 10, &mut rng), 3);
    }

    #[test]
    fn test_lfu_apply_hits_without_log_factor() {
        // log_factor=0 -> p=1.0 for every counter
        let mut rng = rand::thread_rng();
        assert_eq!(lfu_apply_hits(50, 7, 0, &mut rng), 57);
        assert_eq!(lfu_apply_hits(250, 100, 0, &mut rng), 255);
    }

    #[test]
    fn test_lfu_apply_hits_single_hit_distribution() {
        // Statistical test: counter=50, log_factor=10
        // base_val = 50 - 10 = 40, p = 1/(40*10+1) = 1/401 ≈ 0.00249
        // Over 10000 trials, expect ~25 increments
        let mut increments = 0;
        let mut rng = rand::thread_rng();

        let trials = 10_000;
        for _ in 0..trials {
            if lfu_apply_hits(50, 1, 10, &mut rng) == 51 {
                increments += 1;
            }
        }

        assert!(
            increments > 0 && increments < 100,
            "Expected ~25 increments out of {} trials, got {}",
            trials,
            increments
        );
    }

    #[test]
    fn test_lfu_apply_hits_cumulative() {
        // From the table: reaching 20 takes ~470 hits in total, 10 of them to get to LFU_INIT_VAL
        let mut rng = rand::thread_rng();

        let trials = 1_000;
        let sum: u64 = (0..trials)
            .map(|_| lfu_apply_hits(LFU_INIT_VAL, 460, 10, &mut rng) as u64)
            .sum();
        let avg = sum as f64 / trials as f64;

        assert!(
            avg > 18.0 && avg < 22.0,
            "Expected ~20 on average, got {}",
            avg
        );
    }

    #[test]
    fn test_size_class_rank() {
        let classes = [1 << 10, 256 << 10];

        assert_eq!(size_class_rank(&classes, 300 << 10), 0);
        assert_eq!(size_class_rank(&classes, 2 << 10), 1);
        assert_eq!(size_class_rank(&classes, 13), 2);
        // boundaries belong to the upper class
        assert_eq!(size_class_rank(&classes, 1 << 10), 1);
        assert_eq!(size_class_rank(&classes, 256 << 10), 0);
        assert_eq!(size_class_rank(&[], 300 << 10), 0);
        assert_eq!(size_class_rank(&[], 13), 0);
    }
}
