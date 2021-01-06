use super::{
    BaseRocksSecondaryIndex, IndexId, Partition, RocksSecondaryIndex, RocksTable, TableId,
};
use crate::base_rocks_secondary_index;
use crate::metastore::{IdRow, MetaStoreEvent};
use crate::rocks_table_impl;
use crate::table::Row;
use byteorder::{BigEndian, WriteBytesExt};
use chrono::Utc;
use rocksdb::DB;
use serde::{Deserialize, Deserializer};
use std::ops::Sub;

impl Partition {
    pub fn new(index_id: u64, min_value: Option<Row>, max_value: Option<Row>) -> Partition {
        Partition {
            index_id,
            min_value,
            max_value,
            parent_partition_id: None,
            active: true,
            main_table_row_count: 0,
            last_used: None,
        }
    }

    pub fn child(&self, id: u64) -> Partition {
        Partition {
            index_id: self.index_id,
            min_value: None,
            max_value: None,
            parent_partition_id: Some(id),
            active: false,
            main_table_row_count: 0,
            last_used: None,
        }
    }

    pub fn get_min_val(&self) -> &Option<Row> {
        &self.min_value
    }

    pub fn get_max_val(&self) -> &Option<Row> {
        &self.max_value
    }

    pub fn get_full_name(&self, partition_id: u64) -> Option<String> {
        self.parent_partition_id
            .and(Some(format!("{}.parquet", partition_id)))
    }

    pub fn to_active(&self, active: bool) -> Partition {
        Partition {
            index_id: self.index_id,
            min_value: self.min_value.clone(),
            max_value: self.max_value.clone(),
            parent_partition_id: self.parent_partition_id,
            active,
            main_table_row_count: self.main_table_row_count,
            last_used: self.last_used.clone(),
        }
    }

    pub fn update_min_max_and_row_count(
        &self,
        min_value: Option<Row>,
        max_value: Option<Row>,
        main_table_row_count: u64,
    ) -> Partition {
        Partition {
            index_id: self.index_id,
            min_value,
            max_value,
            parent_partition_id: self.parent_partition_id,
            active: self.active,
            main_table_row_count,
            last_used: self.last_used.clone(),
        }
    }

    pub fn update_last_used(&self) -> Self {
        let mut new = self.clone();
        new.last_used = Some(Utc::now());
        new
    }

    pub fn get_index_id(&self) -> u64 {
        self.index_id
    }

    pub fn parent_partition_id(&self) -> &Option<u64> {
        &self.parent_partition_id
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn main_table_row_count(&self) -> u64 {
        self.main_table_row_count
    }

    pub fn is_used(&self, timeout: u64) -> bool {
        self.last_used
            .map(|time| Utc::now().sub(time.clone()).num_seconds() < timeout as i64)
            .unwrap_or(false)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PartitionRocksIndex {
    IndexId = 1,
}

rocks_table_impl!(
    Partition,
    PartitionRocksTable,
    TableId::Partitions,
    { vec![Box::new(PartitionRocksIndex::IndexId)] },
    DeletePartition
);

#[derive(Hash, Clone, Debug)]
pub enum PartitionIndexKey {
    ByIndexId(u64),
}

base_rocks_secondary_index!(Partition, PartitionRocksIndex);

impl RocksSecondaryIndex<Partition, PartitionIndexKey> for PartitionRocksIndex {
    fn typed_key_by(&self, row: &Partition) -> PartitionIndexKey {
        match self {
            PartitionRocksIndex::IndexId => PartitionIndexKey::ByIndexId(row.index_id),
        }
    }

    fn key_to_bytes(&self, key: &PartitionIndexKey) -> Vec<u8> {
        match key {
            PartitionIndexKey::ByIndexId(index_id) => {
                let mut buf = Vec::with_capacity(8);
                buf.write_u64::<BigEndian>(*index_id).unwrap();
                buf
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            PartitionRocksIndex::IndexId => false,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
