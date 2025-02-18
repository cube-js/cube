use crate::cachestore::QueueKey;
use crate::metastore::{
    BaseRocksTable, IdRow, IndexId, RocksEntity, RocksSecondaryIndex, RocksTable, TableId,
    TableInfo,
};
use crate::{base_rocks_secondary_index, rocks_table_new, CubeError};
use chrono::serde::ts_seconds;
use chrono::{DateTime, Duration, Utc};
use cuberockstore::rocksdb::WriteBatch;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueResult {
    path: String,
    pub(crate) value: String,
    pub(crate) deleted: bool,
    #[serde(with = "ts_seconds")]
    pub(crate) expire: DateTime<Utc>,
}

impl RocksEntity for QueueResult {
    fn version() -> u32 {
        3
    }
}

impl QueueResult {
    pub fn new(path: String, value: String) -> Self {
        QueueResult {
            path,
            value,
            deleted: false,
            expire: Utc::now() + Duration::minutes(5),
        }
    }

    pub fn get_path(&self) -> &String {
        &self.path
    }

    pub fn get_value(&self) -> &String {
        &self.value
    }

    pub fn get_expire(&self) -> &DateTime<Utc> {
        &self.expire
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum QueueResultRocksIndex {
    ByPath = 1,
}
pub struct QueueResultRocksTable<'a> {
    db: crate::metastore::DbTableRef<'a>,
}

impl<'a> QueueResultRocksTable<'a> {
    pub fn new(db: crate::metastore::DbTableRef<'a>) -> Self {
        Self { db }
    }

    pub fn get_row_by_key(&self, key: QueueKey) -> Result<Option<IdRow<QueueResult>>, CubeError> {
        match key {
            QueueKey::ByPath(path) => {
                let index_key = QueueResultIndexKey::ByPath(path);
                self.get_single_opt_row_by_index_reverse(&index_key, &QueueResultRocksIndex::ByPath)
            }
            QueueKey::ById(id) => self.get_row(id),
        }
    }
}

impl<'a> BaseRocksTable for QueueResultRocksTable<'a> {
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

rocks_table_new!(QueueResult, QueueResultRocksTable, TableId::QueueResults, {
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
            QueueResultRocksIndex::ByPath => false,
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
