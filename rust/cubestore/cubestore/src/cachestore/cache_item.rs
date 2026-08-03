use crate::metastore::{
    BaseRocksTable, IndexId, RocksEntity, RocksSecondaryIndex, RocksTable, TableId, TableInfo,
};
use crate::{base_rocks_secondary_index, rocks_table_new, CubeError};
use chrono::serde::ts_seconds_option;
use chrono::{DateTime, Duration, Utc};
use cuberockstore::rocksdb::WriteBatch;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct CacheItem {
    pub(crate) prefix: Option<String>,
    pub(crate) key: String,
    pub(crate) value: String,
    #[serde(with = "ts_seconds_option")]
    pub(crate) expire: Option<DateTime<Utc>>,
}

// Every RowKey uses 15 bytes
// Table 58 (flex format)
// SecondaryIndex::ByPrefix 8 + hash (let's take 18)
// SecondaryIndex::ByPath 13 + hash (let's take 18)
pub const CACHE_ITEM_SIZE_WITHOUT_VALUE: u32 = (15 * 3) + 58 + (8 + 18) + (13 + 18);

impl RocksEntity for CacheItem {}

impl CacheItem {
    pub fn parse_path_to_prefix(mut path: String) -> String {
        if path.ends_with(":*") {
            path.pop();
            path.pop();

            return path;
        }

        if path.ends_with(":") {
            path.pop();

            return path;
        }

        path
    }

    pub fn new(path: String, ttl: Option<u32>, value: String) -> CacheItem {
        let parts: Vec<&str> = path.rsplitn(2, ":").collect();

        let (prefix, key) = match parts.len() {
            2 => (Some(parts[1].to_string()), parts[0].to_string()),
            _ => (None, path),
        };

        CacheItem {
            prefix,
            key,
            value,
            expire: ttl.map(|ttl| Utc::now() + Duration::seconds(ttl as i64)),
        }
    }

    pub fn get_path(&self) -> String {
        if let Some(prefix) = &self.prefix {
            format!("{}:{}", prefix, self.key)
        } else {
            self.key.clone()
        }
    }

    pub fn get_prefix(&self) -> &Option<String> {
        &self.prefix
    }

    pub fn get_key(&self) -> &String {
        &self.key
    }

    pub fn get_expire(&self) -> &Option<DateTime<Utc>> {
        &self.expire
    }

    pub fn get_value(&self) -> &String {
        &self.value
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum CacheItemRocksIndex {
    ByPath = 1,
    ByPrefix = 2,
}
pub struct CacheItemRocksTable<'a> {
    db: crate::metastore::DbTableRef<'a>,
}

impl<'a> CacheItemRocksTable<'a> {
    pub fn new(db: crate::metastore::DbTableRef<'a>) -> Self {
        Self { db }
    }
}

impl<'a> BaseRocksTable for CacheItemRocksTable<'a> {
    fn enable_delete_event(&self) -> bool {
        false
    }

    fn enable_update_event(&self) -> bool {
        false
    }

    fn migrate_table(
        &self,
        batch: &mut WriteBatch,
        _table_info: TableInfo,
    ) -> Result<(), CubeError> {
        self.migrate_table_by_truncate(batch)
    }
}

rocks_table_new!(CacheItem, CacheItemRocksTable, TableId::CacheItems, {
    vec![
        Box::new(CacheItemRocksIndex::ByPath),
        Box::new(CacheItemRocksIndex::ByPrefix),
    ]
});

#[derive(Hash, Clone, Debug)]
pub enum CacheItemIndexKey {
    // prefix + key
    ByPath(String),
    ByPrefix(String),
}

base_rocks_secondary_index!(CacheItem, CacheItemRocksIndex);

impl RocksSecondaryIndex<CacheItem, CacheItemIndexKey> for CacheItemRocksIndex {
    fn typed_key_by(&self, row: &CacheItem) -> CacheItemIndexKey {
        match self {
            CacheItemRocksIndex::ByPath => CacheItemIndexKey::ByPath(row.get_path()),
            CacheItemRocksIndex::ByPrefix => {
                CacheItemIndexKey::ByPrefix(if let Some(prefix) = row.get_prefix() {
                    prefix.clone()
                } else {
                    "".to_string()
                })
            }
        }
    }

    fn raw_value_size(&self, row: &CacheItem) -> u32 {
        let size: usize = CACHE_ITEM_SIZE_WITHOUT_VALUE as usize + row.value.len();

        if let Ok(size) = u32::try_from(size) {
            size
        } else {
            u32::MAX
        }
    }

    fn key_to_bytes(&self, key: &CacheItemIndexKey) -> Vec<u8> {
        match key {
            CacheItemIndexKey::ByPrefix(s) | CacheItemIndexKey::ByPath(s) => s.as_bytes().to_vec(),
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            CacheItemRocksIndex::ByPath => true,
            CacheItemRocksIndex::ByPrefix => false,
        }
    }

    fn is_ttl(&self) -> bool {
        true
    }

    fn store_ttl_extended_info(&self) -> bool {
        match self {
            CacheItemRocksIndex::ByPath => true,
            CacheItemRocksIndex::ByPrefix => false,
        }
    }

    fn get_expire(&self, row: &CacheItem) -> Option<DateTime<Utc>> {
        row.get_expire().clone()
    }

    fn version(&self) -> u32 {
        match self {
            CacheItemRocksIndex::ByPath => 1,
            CacheItemRocksIndex::ByPrefix => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_split() {
        let row = CacheItem::new("lock:1".to_string(), None, "value".to_string());
        assert_eq!(row.prefix, Some("lock".to_string()));
        assert_eq!(row.key, "1".to_string());
        assert_eq!(row.get_path(), "lock:1".to_string());
    }
}
