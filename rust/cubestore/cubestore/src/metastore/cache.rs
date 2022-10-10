use super::{
    BaseRocksSecondaryIndex, CacheItem, ColumnFamilyName, IdRow, IndexId, MetaStoreEvent,
    RocksSecondaryIndex, RocksTable, TableId,
};
use crate::{base_rocks_secondary_index, rocks_table_impl};

use chrono::{DateTime, Duration, Utc};
use rocksdb::DB;
use serde::{Deserialize, Deserializer};

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

rocks_table_impl!(
    CacheItem,
    CacheItemRocksTable,
    TableId::CacheItems,
    {
        vec![
            Box::new(CacheItemRocksIndex::ByPath),
            Box::new(CacheItemRocksIndex::ByPrefix),
        ]
    },
    ColumnFamilyName::Cache
);

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

    fn version(&self) -> u32 {
        match self {
            CacheItemRocksIndex::ByPath => 1,
            CacheItemRocksIndex::ByPrefix => 1,
        }
    }

    fn is_ttl(&self) -> bool {
        true
    }

    fn get_expire<'a>(&self, row: &'a CacheItem) -> &'a Option<DateTime<Utc>> {
        row.get_expire()
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
