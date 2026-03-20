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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) external_id: Option<String>,
}

impl RocksEntity for QueueResult {
    fn version() -> u32 {
        3
    }
}

impl QueueResult {
    pub fn new(path: String, value: String, external_id: Option<String>) -> Self {
        QueueResult {
            path,
            value,
            deleted: false,
            expire: Utc::now() + Duration::minutes(5),
            external_id,
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

    pub fn get_external_id(&self) -> &Option<String> {
        &self.external_id
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum QueueResultRocksIndex {
    ByPath = 1,
    ByExternalId = 2,
}
pub struct QueueResultRocksTable<'a> {
    db: crate::metastore::DbTableRef<'a>,
}

impl<'a> QueueResultRocksTable<'a> {
    pub fn new(db: crate::metastore::DbTableRef<'a>) -> Self {
        Self { db }
    }

    pub fn get_row_by_key(&self, key: QueueKey) -> Result<Option<IdRow<QueueResult>>, CubeError> {
        let row = match key {
            QueueKey::ByPath(path) => {
                let index_key = QueueResultIndexKey::ByPath(path);
                self.get_single_opt_row_by_index_reverse(
                    &index_key,
                    &QueueResultRocksIndex::ByPath,
                )?
            }
            QueueKey::ById(id) => self.get_row(id)?,
        };

        Ok(row.filter(|r| r.get_row().get_expire() >= &Utc::now()))
    }

    pub fn get_row_by_external_id(
        &self,
        external_id: String,
    ) -> Result<Option<IdRow<QueueResult>>, CubeError> {
        let index_key = QueueResultIndexKey::ByExternalId(Some(external_id));
        self.get_single_opt_row_by_index(&index_key, &QueueResultRocksIndex::ByExternalId)
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
    vec![
        Box::new(QueueResultRocksIndex::ByPath),
        Box::new(QueueResultRocksIndex::ByExternalId),
    ]
});

#[derive(Hash, Clone, Debug)]
pub enum QueueResultIndexKey {
    ByPath(String),
    ByExternalId(Option<String>),
}

base_rocks_secondary_index!(QueueResult, QueueResultRocksIndex);

impl RocksSecondaryIndex<QueueResult, QueueResultIndexKey> for QueueResultRocksIndex {
    fn typed_key_by(&self, row: &QueueResult) -> QueueResultIndexKey {
        match self {
            QueueResultRocksIndex::ByPath => QueueResultIndexKey::ByPath(row.get_path().clone()),
            QueueResultRocksIndex::ByExternalId => {
                QueueResultIndexKey::ByExternalId(row.get_external_id().clone())
            }
        }
    }

    fn key_to_bytes(&self, key: &QueueResultIndexKey) -> Vec<u8> {
        match key {
            QueueResultIndexKey::ByPath(s) => s.as_bytes().to_vec(),
            QueueResultIndexKey::ByExternalId(s) => {
                s.as_deref().unwrap_or("__null__").as_bytes().to_vec()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            QueueResultRocksIndex::ByPath => false,
            QueueResultRocksIndex::ByExternalId => true,
        }
    }

    fn version(&self) -> u32 {
        match self {
            QueueResultRocksIndex::ByPath => 1,
            QueueResultRocksIndex::ByExternalId => 1,
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

    fn should_index_row(&self, row: &QueueResult) -> bool {
        match self {
            Self::ByExternalId => row.external_id.is_some(),
            _ => true,
        }
    }
}
