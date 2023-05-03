use crate::metastore::{
    BaseRocksTable, IndexId, RocksEntity, RocksSecondaryIndex, RocksTable, TableId, TableInfo,
};
use crate::table::{Row, TableValue};
use crate::{base_rocks_secondary_index, rocks_table_new, CubeError};
use chrono::serde::{ts_seconds, ts_seconds_option};
use chrono::{DateTime, Duration, Utc};
use rocksdb::WriteBatch;
use std::cmp::Ordering;

use serde::{Deserialize, Deserializer, Serialize};

fn merge(a: serde_json::Value, b: serde_json::Value) -> Option<serde_json::Value> {
    match (a, b) {
        (mut root @ serde_json::Value::Object(_), serde_json::Value::Object(b)) => {
            let r = root.as_object_mut().unwrap();
            for (k, v) in b {
                if r.contains_key(&k) {
                    r.remove(&k);
                }

                r.insert(k, v);
            }

            Some(root)
        }
        // Special case to truncate extra
        (_a, serde_json::Value::Null) => None,
        (_a, b) => Some(b),
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum QueueResultAckEventResult {
    Empty,
    WithResult { row_id: u64, result: String },
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct QueueResultAckEvent {
    pub path: String,
    pub result: QueueResultAckEventResult,
}

#[repr(u8)]
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum QueueItemStatus {
    Pending = 0,
    Active = 1,
    Finished = 2,
}

impl ToString for QueueItemStatus {
    fn to_string(&self) -> String {
        match self {
            QueueItemStatus::Pending => "pending".to_string(),
            QueueItemStatus::Active => "active".to_string(),
            QueueItemStatus::Finished => "finished".to_string(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct QueueItem {
    prefix: Option<String>,
    pub(crate) key: String,
    // Immutable field
    value: String,
    extra: Option<String>,
    #[serde(default = "QueueItem::status_default")]
    pub(crate) status: QueueItemStatus,
    #[serde(default)]
    priority: i64,
    #[serde(with = "ts_seconds")]
    created: DateTime<Utc>,
    #[serde(with = "ts_seconds_option")]
    pub(crate) heartbeat: Option<DateTime<Utc>>,
    #[serde(with = "ts_seconds_option")]
    orphaned: Option<DateTime<Utc>>,
}

impl RocksEntity for QueueItem {
    fn version() -> u32 {
        2
    }
}

impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.priority == other.priority {
            other.created.cmp(&self.created)
        } else {
            self.priority.cmp(&other.priority)
        }
    }
}

impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl QueueItem {
    pub fn parse_path(path: String) -> (Option<String>, String) {
        let parts: Vec<&str> = path.rsplitn(2, ":").collect();

        match parts.len() {
            2 => (Some(parts[1].to_string()), parts[0].to_string()),
            _ => (None, path),
        }
    }

    pub fn new(
        path: String,
        value: String,
        status: QueueItemStatus,
        priority: i64,
        orphaned: Option<u32>,
    ) -> Self {
        let (prefix, key) = QueueItem::parse_path(path);
        let created = Utc::now();

        QueueItem {
            prefix,
            key,
            value,
            status,
            priority,
            extra: None,
            heartbeat: None,
            orphaned: if let Some(orphaned) = orphaned {
                Some(created + Duration::seconds(orphaned as i64))
            } else {
                None
            },
            created,
        }
    }

    pub fn into_queue_cancel_row(self) -> Row {
        let res = vec![
            TableValue::String(self.value),
            if let Some(extra) = self.extra {
                TableValue::String(extra)
            } else {
                TableValue::Null
            },
        ];

        Row::new(res)
    }

    pub fn into_queue_retrieve_row(self) -> Row {
        let res = vec![
            TableValue::String(self.value),
            if let Some(extra) = self.extra {
                TableValue::String(extra)
            } else {
                TableValue::Null
            },
        ];

        Row::new(res)
    }

    pub fn into_queue_get_row(self) -> Row {
        let res = vec![
            TableValue::String(self.value),
            if let Some(extra) = self.extra {
                TableValue::String(extra)
            } else {
                TableValue::Null
            },
        ];

        Row::new(res)
    }

    pub fn into_queue_list_row(self, with_payload: bool) -> Row {
        let mut res = vec![
            TableValue::String(self.key),
            TableValue::String(self.status.to_string()),
            if let Some(extra) = self.extra {
                TableValue::String(extra)
            } else {
                TableValue::Null
            },
        ];

        if with_payload {
            res.push(TableValue::String(self.value));
        }

        Row::new(res)
    }

    pub fn get_key(&self) -> &String {
        &self.key
    }

    pub fn get_prefix(&self) -> &Option<String> {
        &self.prefix
    }

    pub fn get_path(&self) -> String {
        if let Some(prefix) = &self.prefix {
            format!("{}:{}", prefix, self.key)
        } else {
            self.key.clone()
        }
    }

    pub fn get_value(&self) -> &String {
        &self.value
    }

    pub fn get_priority(&self) -> &i64 {
        &self.priority
    }

    pub fn get_extra(&self) -> &Option<String> {
        &self.extra
    }

    pub fn get_status(&self) -> &QueueItemStatus {
        &self.status
    }

    pub fn get_heartbeat(&self) -> &Option<DateTime<Utc>> {
        &self.heartbeat
    }

    pub fn get_created(&self) -> &DateTime<Utc> {
        &self.created
    }

    pub fn get_orphaned(&self) -> &Option<DateTime<Utc>> {
        &self.orphaned
    }

    pub fn status_default() -> QueueItemStatus {
        QueueItemStatus::Pending
    }

    pub fn update_heartbeat(&mut self) {
        self.heartbeat = Some(Utc::now());
    }

    pub fn merge_extra(&self, payload: String) -> Result<Self, CubeError> {
        let mut new = self.clone();

        if let Some(extra) = &self.extra {
            let prev = serde_json::from_str(&extra)?;
            let next = serde_json::from_str(&payload)?;

            let extra = merge(prev, next);

            new.extra = extra.map(|v| v.to_string())
        } else {
            new.extra = Some(payload);
        }

        Ok(new)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum QueueRetrieveResponse {
    Success {
        id: u64,
        item: QueueItem,
        pending: u64,
        active: Vec<String>,
    },
    LockFailed {
        pending: u64,
        active: Vec<String>,
    },
    NotEnoughConcurrency {
        pending: u64,
        active: Vec<String>,
    },
    NotFound {
        pending: u64,
        active: Vec<String>,
    },
}

impl QueueRetrieveResponse {
    pub fn into_queue_retrieve_rows(self, extended: bool) -> Vec<Row> {
        match self {
            QueueRetrieveResponse::Success {
                id,
                item,
                pending,
                active,
            } => vec![Row::new(vec![
                TableValue::String(item.value),
                if let Some(extra) = item.extra {
                    TableValue::String(extra)
                } else {
                    TableValue::Null
                },
                TableValue::Int(pending as i64),
                if active.len() > 0 {
                    TableValue::String(active.join(","))
                } else {
                    TableValue::Null
                },
                TableValue::String(id.to_string()),
            ])],
            QueueRetrieveResponse::LockFailed { pending, active }
            | QueueRetrieveResponse::NotEnoughConcurrency { pending, active }
            | QueueRetrieveResponse::NotFound { pending, active } => {
                if extended {
                    vec![Row::new(vec![
                        TableValue::Null,
                        TableValue::Null,
                        TableValue::Int(pending as i64),
                        if active.len() > 0 {
                            TableValue::String(active.join(","))
                        } else {
                            TableValue::Null
                        },
                        TableValue::Null,
                    ])]
                } else {
                    vec![]
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum QueueItemRocksIndex {
    ByPath = 1,
    ByPrefixAndStatus = 2,
    ByPrefix = 3,
}

pub struct QueueItemRocksTable<'a> {
    db: crate::metastore::DbTableRef<'a>,
}

impl<'a> QueueItemRocksTable<'a> {
    pub fn new(db: crate::metastore::DbTableRef<'a>) -> Self {
        Self { db }
    }
}

impl<'a> BaseRocksTable for QueueItemRocksTable<'a> {
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

rocks_table_new!(QueueItem, QueueItemRocksTable, TableId::QueueItems, {
    vec![
        Box::new(QueueItemRocksIndex::ByPath),
        Box::new(QueueItemRocksIndex::ByPrefixAndStatus),
        Box::new(QueueItemRocksIndex::ByPrefix),
    ]
});

#[derive(Hash, Clone, Debug)]
pub enum QueueItemIndexKey {
    ByPath(String),
    ByPrefixAndStatus(String, QueueItemStatus),
    ByPrefix(String),
}

base_rocks_secondary_index!(QueueItem, QueueItemRocksIndex);

impl RocksSecondaryIndex<QueueItem, QueueItemIndexKey> for QueueItemRocksIndex {
    fn typed_key_by(&self, row: &QueueItem) -> QueueItemIndexKey {
        match self {
            QueueItemRocksIndex::ByPath => QueueItemIndexKey::ByPath(row.get_path()),
            QueueItemRocksIndex::ByPrefixAndStatus => QueueItemIndexKey::ByPrefixAndStatus(
                row.get_prefix().clone().unwrap_or("".to_string()),
                row.get_status().clone(),
            ),
            QueueItemRocksIndex::ByPrefix => {
                QueueItemIndexKey::ByPrefix(row.get_prefix().clone().unwrap_or("".to_string()))
            }
        }
    }

    fn key_to_bytes(&self, key: &QueueItemIndexKey) -> Vec<u8> {
        match key {
            QueueItemIndexKey::ByPath(s) => s.as_bytes().to_vec(),
            QueueItemIndexKey::ByPrefix(s) => s.as_bytes().to_vec(),
            QueueItemIndexKey::ByPrefixAndStatus(prefix, s) => {
                let mut r = Vec::with_capacity(prefix.len() + 1);
                r.extend_from_slice(&prefix.as_bytes());

                match s {
                    QueueItemStatus::Pending => r.push(0_u8),
                    QueueItemStatus::Active => r.push(1_u8),
                    QueueItemStatus::Finished => r.push(2_u8),
                }

                r
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            QueueItemRocksIndex::ByPath => true,
            QueueItemRocksIndex::ByPrefixAndStatus => false,
            QueueItemRocksIndex::ByPrefix => false,
        }
    }

    fn version(&self) -> u32 {
        match self {
            QueueItemRocksIndex::ByPath => 1,
            QueueItemRocksIndex::ByPrefixAndStatus => 2,
            QueueItemRocksIndex::ByPrefix => 1,
        }
    }

    fn is_ttl(&self) -> bool {
        true
    }

    fn get_expire(&self, row: &QueueItem) -> Option<DateTime<Utc>> {
        if let Some(orphaned) = row.orphaned {
            Some(orphaned.clone() + Duration::hours(1))
        } else {
            Some(row.get_created().clone() + Duration::hours(2))
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CubeError;
    use itertools::Itertools;

    #[test]
    fn test_queue_item_sort() -> Result<(), CubeError> {
        let priority0_1 = QueueItem::new(
            "1".to_string(),
            "1".to_string(),
            QueueItemStatus::Active,
            0,
            None,
        );
        let priority0_2 = QueueItem::new(
            "2".to_string(),
            "2".to_string(),
            QueueItemStatus::Active,
            0,
            None,
        );
        let priority0_3 = QueueItem::new(
            "3".to_string(),
            "3".to_string(),
            QueueItemStatus::Active,
            0,
            None,
        );
        let priority10_4 = QueueItem::new(
            "4".to_string(),
            "4".to_string(),
            QueueItemStatus::Active,
            10,
            None,
        );
        let priority0_5 = QueueItem::new(
            "5".to_string(),
            "5".to_string(),
            QueueItemStatus::Active,
            0,
            None,
        );
        let priority_n5_6 = QueueItem::new(
            "6".to_string(),
            "6".to_string(),
            QueueItemStatus::Active,
            -5,
            None,
        );

        assert_eq!(
            vec![
                priority0_1.clone(),
                priority0_2.clone(),
                priority0_3.clone(),
                priority10_4.clone(),
                priority_n5_6.clone(),
                priority0_5.clone()
            ]
            .into_iter()
            .sorted_by(|a, b| b.cmp(&a))
            .map(|item| item.get_key().clone())
            .collect::<Vec<String>>(),
            vec![
                "4".to_string(),
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "5".to_string(),
                "6".to_string()
            ]
        );

        assert_eq!(
            vec![
                priority10_4,
                priority0_1,
                priority0_5,
                priority0_2,
                priority0_3,
                priority_n5_6
            ]
            .into_iter()
            .sorted_by(|a, b| b.cmp(&a))
            .map(|item| item.get_key().clone())
            .collect::<Vec<String>>(),
            vec![
                "4".to_string(),
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "5".to_string(),
                "6".to_string()
            ]
        );

        Ok(())
    }
}
