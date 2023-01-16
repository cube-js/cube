use crate::metastore::{IndexId, RocksSecondaryIndex, TableId};
use crate::{base_rocks_secondary_index, rocks_table_impl};
use chrono::serde::ts_seconds;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueResult {
    path: String,
    pub(crate) value: String,
    #[serde(with = "ts_seconds")]
    pub(crate) expire: DateTime<Utc>,
}

impl QueueResult {
    pub fn new(path: String, value: String) -> Self {
        QueueResult {
            path,
            value,
            expire: Utc::now() + Duration::minutes(10),
        }
    }

    pub fn get_path(&self) -> &String {
        &self.path
    }

    pub fn get_value(&self) -> &String {
        &self.value
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum QueueResultRocksIndex {
    ByPath = 1,
}

rocks_table_impl!(QueueResult, QueueResultRocksTable, TableId::QueueResults, {
    vec![Box::new(QueueResultRocksIndex::ByPath)]
});

#[derive(Hash, Clone, Debug)]
pub enum QueueResultIndexKey {
    ByPath(String),
}

base_rocks_secondary_index!(QueueResult, QueueResultRocksIndex);

impl RocksSecondaryIndex<QueueResult, QueueResultIndexKey> for QueueResultRocksIndex {
    fn typed_key_by(&self, row: &QueueResult) -> QueueResultIndexKey {
        match self {
            QueueResultRocksIndex::ByPath => QueueResultIndexKey::ByPath(row.get_path().clone()),
        }
    }

    fn key_to_bytes(&self, key: &QueueResultIndexKey) -> Vec<u8> {
        match key {
            QueueResultIndexKey::ByPath(s) => s.as_bytes().to_vec(),
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            QueueResultRocksIndex::ByPath => true,
        }
    }

    fn version(&self) -> u32 {
        match self {
            QueueResultRocksIndex::ByPath => 1,
        }
    }

    fn is_ttl(&self) -> bool {
        true
    }

    fn get_expire(&self, row: &QueueResult) -> Option<DateTime<Utc>> {
        Some(row.expire.clone())
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
