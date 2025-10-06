use crate::metastore::{
    BaseRocksTable, IndexId, RocksEntity, RocksSecondaryIndex, RocksTable, TableId, TableInfo,
};
use crate::{base_rocks_secondary_index, rocks_table_new, CubeError};
use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use cuberockstore::rocksdb::WriteBatch;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueItemPayload {
    // Immutable field
    pub(crate) value: String,
    #[serde(with = "ts_seconds")]
    created: DateTime<Utc>,
    #[serde(with = "ts_seconds")]
    expire: DateTime<Utc>,
}

impl RocksEntity for QueueItemPayload {
    fn version() -> u32 {
        2
    }
}

impl QueueItemPayload {
    pub fn new(value: String, created: DateTime<Utc>, expire: DateTime<Utc>) -> Self {
        Self {
            value,
            created,
            expire,
        }
    }

    pub fn get_value(&self) -> &String {
        &self.value
    }

    pub fn get_created(&self) -> &DateTime<Utc> {
        &self.created
    }
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub(crate) enum QueueItemPayloadRocksIndex {}

pub struct QueueItemPayloadRocksTable<'a> {
    db: crate::metastore::DbTableRef<'a>,
}

impl<'a> QueueItemPayloadRocksTable<'a> {
    pub fn new(db: crate::metastore::DbTableRef<'a>) -> Self {
        Self { db }
    }
}

impl<'a> BaseRocksTable for QueueItemPayloadRocksTable<'a> {
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

rocks_table_new!(
    QueueItemPayload,
    QueueItemPayloadRocksTable,
    TableId::QueueItemPayload,
    { vec![] }
);

#[derive(Hash, Clone, Debug)]
#[allow(dead_code)]
pub enum QueueItemPayloadIndexKey {}

base_rocks_secondary_index!(QueueItemPayload, QueueItemPayloadRocksIndex);

impl RocksSecondaryIndex<QueueItemPayload, QueueItemPayloadIndexKey>
    for QueueItemPayloadRocksIndex
{
    fn typed_key_by(&self, _row: &QueueItemPayload) -> QueueItemPayloadIndexKey {
        unimplemented!();
    }

    fn key_to_bytes(&self, _key: &QueueItemPayloadIndexKey) -> Vec<u8> {
        unimplemented!();
    }

    fn is_unique(&self) -> bool {
        unimplemented!();
    }

    fn version(&self) -> u32 {
        1
    }

    fn is_ttl(&self) -> bool {
        true
    }

    fn get_expire(&self, row: &QueueItemPayload) -> Option<DateTime<Utc>> {
        Some(row.expire.clone())
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
