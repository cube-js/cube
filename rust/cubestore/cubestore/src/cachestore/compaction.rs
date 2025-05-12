use crate::metastore::{
    IndexId, RocksSecondaryIndexValue, RocksSecondaryIndexValueVersion, RowKey, SecondaryIndexInfo,
};
use crate::TableId;

use chrono::{DateTime, Utc};
use cuberockstore::rocksdb::compaction_filter::CompactionFilter;
use cuberockstore::rocksdb::compaction_filter_factory::{
    CompactionFilterContext, CompactionFilterFactory,
};
use cuberockstore::rocksdb::CompactionDecision;
use log::{error, trace, warn};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::sync::{Arc, Mutex};

pub struct MetaStoreCacheCompactionFilter {
    name: CString,
    current: DateTime<Utc>,
    scanned: u64,
    removed: u64,
    orphaned: u64,
    no_ttl: u64,
    not_expired: u64,
    state: CompactionSharedState,
    context: CompactionFilterContext,
}

impl MetaStoreCacheCompactionFilter {
    pub fn new(state: CompactionSharedState, context: CompactionFilterContext) -> Self {
        Self {
            name: CString::new("cache-expire-check").unwrap(),
            current: Utc::now(),
            scanned: 0,
            removed: 0,
            orphaned: 0,
            no_ttl: 0,
            not_expired: 0,
            state,
            context,
        }
    }
}

impl Drop for MetaStoreCacheCompactionFilter {
    fn drop(&mut self) {
        let elapsed = Utc::now() - self.current;

        trace!(
            "Compaction finished in {}.{} secs (is_full: {}), scanned: {}, removed: {}, orphaned: {}, no_ttl: {}, not_expired: {})",
            elapsed.num_seconds(),
            elapsed.num_milliseconds(),
            self.context.is_full_compaction,
            self.scanned,
            self.removed,
            self.orphaned,
            self.no_ttl,
            self.not_expired
        );
    }
}

impl MetaStoreCacheCompactionFilter {
    fn filter_table_row_key(&mut self, table_id: TableId, value: &[u8]) -> CompactionDecision {
        if !table_id.has_ttl() {
            return CompactionDecision::Keep;
        }

        let reader = match flexbuffers::Reader::get_root(&value) {
            Ok(r) => r,
            Err(err) => {
                warn!(r#"Unable to deserialize row, error: {}"#, err);

                self.orphaned += 1;

                return CompactionDecision::Remove;
            }
        };

        let root = reader.as_map();
        let expire_key_id = match root.index_key(&table_id.get_ttl_field()) {
            None => {
                if cfg!(debug_assertions) {
                    warn!(
                        "There is no {} field in row specified with TTL for {:?}",
                        table_id.get_ttl_field(),
                        table_id
                    );
                }

                self.orphaned += 1;

                return CompactionDecision::Keep;
            }
            Some(idx) => idx,
        };

        let expire = root.idx(expire_key_id);

        if expire.flexbuffer_type() == flexbuffers::FlexBufferType::Null {
            self.no_ttl += 1;

            return CompactionDecision::Keep;
        }

        match DateTime::from_timestamp(expire.as_i64(), 0) {
            Some(expire) => {
                if expire <= self.current {
                    self.removed += 1;

                    CompactionDecision::Remove
                } else {
                    self.not_expired += 1;

                    CompactionDecision::Keep
                }
            }
            None => {
                warn!(
                    r#"Unable to parser date from expire field with value "{}""#,
                    expire
                );

                self.orphaned += 1;

                CompactionDecision::Remove
            }
        }
    }

    fn filter_secondary_row_key(&mut self, index_id: IndexId, value: &[u8]) -> CompactionDecision {
        let index_info = if let Some(state) = &self.state {
            if let Some(index) = state.indexes.get(&index_id) {
                index
            } else {
                warn!("Unknown index {:?}, skipping compaction", index_id);

                // In the future, we need to change this to CompactionDecision::Remove
                // This may happens when we drop index from the table
                return CompactionDecision::Keep;
            }
        } else {
            return CompactionDecision::Keep;
        };

        match RocksSecondaryIndexValue::from_bytes(value, index_info.value_version) {
            Err(err) => {
                // Index was migrated from old format to the new version, but compaction didnt remove it
                if index_info.value_version == RocksSecondaryIndexValueVersion::WithTTLSupport
                    && RocksSecondaryIndexValue::from_bytes(
                        value,
                        RocksSecondaryIndexValueVersion::OnlyHash,
                    )
                    .is_ok()
                {
                    CompactionDecision::Remove
                } else {
                    error!(
                        "Unable to read index value on compaction with error: {}",
                        err.message
                    );

                    CompactionDecision::Keep
                }
            }
            Ok(RocksSecondaryIndexValue::Hash(_)) => CompactionDecision::Keep,
            Ok(
                RocksSecondaryIndexValue::HashAndTTL(_, expire)
                | RocksSecondaryIndexValue::HashAndTTLExtended(_, expire, _),
            ) => {
                if let Some(expire) = expire {
                    if expire <= self.current {
                        self.removed += 1;

                        CompactionDecision::Remove
                    } else {
                        CompactionDecision::Keep
                    }
                } else {
                    CompactionDecision::Keep
                }
            }
        }
    }
}

impl CompactionFilter for MetaStoreCacheCompactionFilter {
    fn filter(&mut self, _level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        self.scanned += 1;

        match RowKey::try_from_bytes(key) {
            Ok(row_key) => match row_key {
                RowKey::Table(table_id, _) => self.filter_table_row_key(table_id, value),
                RowKey::SecondaryIndex(index_id, _, _) => {
                    self.filter_secondary_row_key(index_id, value)
                }
                RowKey::Sequence(_) => CompactionDecision::Keep,
                RowKey::SecondaryIndexInfo { .. } => CompactionDecision::Keep,
                RowKey::TableInfo { .. } => CompactionDecision::Keep,
            },
            Err(err) => {
                error!("Unable to read key on compaction, error: {}", err.message);

                CompactionDecision::Keep
            }
        }
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

pub type CompactionSharedState = Option<CompactionPreloadedState>;

#[derive(Debug, Clone)]
pub struct CompactionPreloadedState {
    indexes: HashMap<IndexId, SecondaryIndexInfo>,
}

impl CompactionPreloadedState {
    pub fn new(indexes: HashMap<IndexId, SecondaryIndexInfo>) -> Self {
        Self { indexes }
    }
}

#[derive(Debug)]
pub struct MetaStoreCacheCompactionFactory {
    name: CString,
    state: Arc<Mutex<CompactionSharedState>>,
}

impl MetaStoreCacheCompactionFactory {
    pub fn new(state: Arc<Mutex<CompactionSharedState>>) -> Self {
        Self {
            name: CString::new("cache-expire-check").unwrap(),
            state,
        }
    }
}

impl CompactionFilterFactory for MetaStoreCacheCompactionFactory {
    type Filter = MetaStoreCacheCompactionFilter;

    fn create(&mut self, context: CompactionFilterContext) -> Self::Filter {
        let state = if let Ok(guard) = self.state.lock() {
            // It's better to clone full state instead of using lock on each filter call
            guard.as_ref().map(|s| s.clone())
        } else {
            println!("Unable to unlock compaction state");

            None
        };

        MetaStoreCacheCompactionFilter::new(state, context)
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cachestore::cache_item::{CacheItemRocksIndex, CacheItemRocksTable};
    use crate::cachestore::cache_rocksstore::RocksCacheStoreDetails;
    use crate::cachestore::CacheItem;
    use crate::config::init_test_logger;
    use crate::metastore::{BaseRocksSecondaryIndex, RocksTable};
    use crate::TableId;
    use chrono::Duration;
    use cuberockstore::rocksdb::compaction_filter::Decision;
    use serde::Serialize;

    fn get_test_filter_context() -> CompactionFilterContext {
        CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        }
    }

    #[tokio::test]
    async fn test_compaction_table_no_ttl_keep() {
        init_test_logger().await;

        let mut filter = MetaStoreCacheCompactionFilter::new(
            Some(RocksCacheStoreDetails::get_compaction_state()),
            get_test_filter_context(),
        );

        let key = RowKey::Table(TableId::CacheItems, 6);
        let row = CacheItem::new("key1".to_string(), None, "value".to_string());

        let mut ser = flexbuffers::FlexbufferSerializer::new();
        row.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();

        match filter.filter(1, key.to_bytes().as_slice(), serialized_row.as_slice()) {
            Decision::Keep => (),
            _ => panic!("must be keep"),
        }
    }

    #[tokio::test]
    async fn test_compaction_table_tll_not_expired_keep() {
        init_test_logger().await;

        let mut filter = MetaStoreCacheCompactionFilter::new(
            Some(RocksCacheStoreDetails::get_compaction_state()),
            get_test_filter_context(),
        );

        let key = RowKey::Table(TableId::CacheItems, 6);
        let row = CacheItem::new("key1".to_string(), Some(10), "value".to_string());

        let mut ser = flexbuffers::FlexbufferSerializer::new();
        row.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();

        match filter.filter(1, key.to_bytes().as_slice(), serialized_row.as_slice()) {
            Decision::Keep => (),
            _ => panic!("must be keep"),
        }
    }

    #[tokio::test]
    async fn test_compaction_table_tll_expired_remove() {
        init_test_logger().await;

        let mut filter = MetaStoreCacheCompactionFilter::new(
            Some(RocksCacheStoreDetails::get_compaction_state()),
            get_test_filter_context(),
        );

        let key = RowKey::Table(TableId::CacheItems, 6);
        let mut row = CacheItem::new("key1".to_string(), None, "value".to_string());
        row.expire = Some(Utc::now() - Duration::seconds(10));

        let mut ser = flexbuffers::FlexbufferSerializer::new();
        row.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();

        match filter.filter(1, key.to_bytes().as_slice(), serialized_row.as_slice()) {
            Decision::Remove => (),
            _ => panic!("must be remove"),
        }
    }

    #[tokio::test]
    async fn test_compaction_index_ttl_expired_remove() {
        init_test_logger().await;

        let mut filter = MetaStoreCacheCompactionFilter::new(
            Some(RocksCacheStoreDetails::get_compaction_state()),
            get_test_filter_context(),
        );

        let mut row = CacheItem::new("key1".to_string(), None, "value".to_string());
        row.expire = Some(Utc::now() - Duration::seconds(10));

        let index = CacheItemRocksIndex::ByPath;
        let key = RowKey::SecondaryIndex(
            CacheItemRocksTable::index_id(index.get_id()),
            index.key_hash(&row).to_be_bytes().to_vec(),
            1,
        );

        match filter.filter(1, &key.to_bytes(), &index.index_value(&row)) {
            Decision::Remove => (),
            _ => panic!("must be remove"),
        }
    }

    #[tokio::test]
    async fn test_compaction_index_ttl_not_expired_remove() {
        init_test_logger().await;

        let mut filter = MetaStoreCacheCompactionFilter::new(
            Some(RocksCacheStoreDetails::get_compaction_state()),
            get_test_filter_context(),
        );

        let row = CacheItem::new("key1".to_string(), Some(10), "value".to_string());

        let index = CacheItemRocksIndex::ByPath;
        let key = RowKey::SecondaryIndex(
            CacheItemRocksTable::index_id(index.get_id()),
            index.key_hash(&row).to_be_bytes().to_vec(),
            1,
        );

        match filter.filter(1, &key.to_bytes(), &index.index_value(&row)) {
            Decision::Keep => (),
            _ => panic!("must be keep"),
        }
    }

    #[tokio::test]
    async fn test_compaction_index_old_format() {
        init_test_logger().await;

        let mut filter = MetaStoreCacheCompactionFilter::new(
            Some(RocksCacheStoreDetails::get_compaction_state()),
            get_test_filter_context(),
        );

        let row = CacheItem::new("key1".to_string(), Some(10), "value".to_string());

        let index = CacheItemRocksIndex::ByPath;
        let key = RowKey::SecondaryIndex(
            CacheItemRocksTable::index_id(index.get_id()),
            index.key_hash(&row).to_be_bytes().to_vec(),
            1,
        );

        // Indexes with TTL use new format (v2) for indexes, but index migration doesnt skip
        // compaction for old rows
        let index_value = RocksSecondaryIndexValue::Hash("kek".as_bytes())
            .to_bytes(RocksSecondaryIndexValueVersion::OnlyHash)
            .unwrap();

        match filter.filter(1, &key.to_bytes(), &index_value) {
            Decision::Remove => (),
            _ => panic!("must be remove"),
        }
    }
}
