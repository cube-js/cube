//! Multi indexes split data inside **multiple** tables into ranges with the same key ranges. This
//! allows efficient implementation of combining data by key, e.g. JOIN by column equality.
//!
//! Each multi-index owns a tree of multi-partitions. Each multi-partition defines a range of keys
//! and guarantees that ordinary partitions that store the data have the same key range.
//! Multi-partitioned are compacted and repartitioned by applying the same operation to ordinary
//! partitions they own.
use super::RocksTable;
use crate::data_frame_from;
use crate::metastore::{
    BaseRocksSecondaryIndex, Column, ColumnFamilyName, IdRow, IndexId, MetaStoreEvent,
    RocksSecondaryIndex, TableId,
};
use crate::rocks_table_impl;
use crate::table::Row;
use byteorder::{BigEndian, WriteBytesExt};
use rocksdb::DB;
use serde::{Deserialize, Deserializer, Serialize};
use std::io::Cursor;
use std::io::Write;

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct MultiIndex {
    schema_id: u64,
    name: String,
    key_columns: Vec<Column>
}
}

impl MultiIndex {
    pub fn new(schema_id: u64, name: String, key_columns: Vec<Column>) -> MultiIndex {
        MultiIndex {
            schema_id,
            name,
            key_columns,
        }
    }

    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_columns(&self) -> &[Column] {
        &self.key_columns
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MultiIndexRocksIndex {
    ByName = 1,
}

crate::base_rocks_secondary_index!(MultiIndex, MultiIndexRocksIndex);

rocks_table_impl!(
    MultiIndex,
    MultiIndexRocksTable,
    TableId::MultiIndexes,
    { vec![Box::new(MultiIndexRocksIndex::ByName)] },
    ColumnFamilyName::Default
);

#[derive(Hash, Clone, Debug)]
pub enum MultiIndexIndexKey {
    ByName(u64, String),
}

impl RocksSecondaryIndex<MultiIndex, MultiIndexIndexKey> for MultiIndexRocksIndex {
    fn typed_key_by(&self, i: &MultiIndex) -> MultiIndexIndexKey {
        match self {
            MultiIndexRocksIndex::ByName => {
                MultiIndexIndexKey::ByName(i.schema_id, i.name.to_string())
            }
        }
    }

    fn key_to_bytes(&self, key: &MultiIndexIndexKey) -> Vec<u8> {
        match key {
            MultiIndexIndexKey::ByName(schema, name) => {
                let mut w = Cursor::new(Vec::with_capacity(8 + name.len()));
                w.write_u64::<BigEndian>(*schema).unwrap();
                w.write_all(name.as_bytes()).unwrap();
                w.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            MultiIndexRocksIndex::ByName => true,
        }
    }

    fn version(&self) -> u32 {
        match self {
            MultiIndexRocksIndex::ByName => 1,
        }
    }

    fn is_ttl(&self) -> bool {
        false
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct MultiPartition {
   multi_index_id: u64,
   parent_multi_partition_id: Option<u64>,
   min_row: Option<Row>,
   max_row: Option<Row>,
   active: bool,
   /// When this is true, compactions must not run to avoid concurrent modifications with split.
   prepared_for_split: bool,
   /// All active rows, including main tables and chunks of all partitions.
   total_row_count: u64
}
}

impl MultiPartition {
    // Note that roots are active by default.
    pub fn new_root(multi_index_id: u64) -> MultiPartition {
        MultiPartition {
            multi_index_id,
            parent_multi_partition_id: None,
            min_row: None,
            max_row: None,
            total_row_count: 0,
            prepared_for_split: false,
            active: true,
        }
    }

    // Note that children are inactive by default.
    pub fn new_child(
        parent: &IdRow<MultiPartition>,
        min_row: Option<Row>,
        max_row: Option<Row>,
    ) -> MultiPartition {
        MultiPartition {
            multi_index_id: parent.row.multi_index_id,
            parent_multi_partition_id: Some(parent.id),
            min_row,
            max_row,
            total_row_count: 0,
            prepared_for_split: false,
            active: false,
        }
    }

    pub fn total_row_count(&self) -> u64 {
        self.total_row_count
    }
    pub fn add_rows(&self, rows: u64) -> MultiPartition {
        let mut s = self.clone();
        s.total_row_count += rows;
        s
    }
    pub fn subtract_rows(&self, rows: u64) -> MultiPartition {
        let mut s = self.clone();
        assert!(
            rows <= s.total_row_count,
            "{} and {}",
            rows,
            self.total_row_count,
        );
        s.total_row_count -= rows;
        s
    }
    pub fn active(&self) -> bool {
        self.active
    }
    pub fn set_active(&self, v: bool) -> MultiPartition {
        let mut s = self.clone();
        s.active = v;
        s
    }
    pub fn mark_prepared_for_split(&self) -> MultiPartition {
        let mut s = self.clone();
        s.prepared_for_split = true;
        s
    }
    pub fn prepared_for_split(&self) -> bool {
        self.prepared_for_split
    }
    pub fn was_activated(&self) -> bool {
        self.active || self.prepared_for_split
    }
    pub fn multi_index_id(&self) -> u64 {
        self.multi_index_id
    }
    pub fn min_row(&self) -> Option<&Row> {
        self.min_row.as_ref()
    }
    pub fn max_row(&self) -> Option<&Row> {
        self.max_row.as_ref()
    }
    pub fn parent_multi_partition_id(&self) -> Option<u64> {
        self.parent_multi_partition_id
    }
}

rocks_table_impl!(
    MultiPartition,
    MultiPartitionRocksTable,
    TableId::MultiPartitions,
    {
        vec![
            Box::new(MultiPartitionRocksIndex::ByMultiIndexId),
            Box::new(MultiPartitionRocksIndex::ByParentId),
        ]
    },
    ColumnFamilyName::Default
);

#[derive(Hash, Clone, Debug)]
pub enum MultiPartitionIndexKey {
    ByMultiIndexId(u64),
    ByParentId(Option<u64>),
}

crate::base_rocks_secondary_index!(MultiPartition, MultiPartitionRocksIndex);

#[derive(Clone, Copy, Debug)]
pub enum MultiPartitionRocksIndex {
    ByMultiIndexId = 1,
    ByParentId,
}

impl RocksSecondaryIndex<MultiPartition, MultiPartitionIndexKey> for MultiPartitionRocksIndex {
    fn typed_key_by(&self, p: &MultiPartition) -> MultiPartitionIndexKey {
        match self {
            MultiPartitionRocksIndex::ByMultiIndexId => {
                MultiPartitionIndexKey::ByMultiIndexId(p.multi_index_id)
            }
            MultiPartitionRocksIndex::ByParentId => {
                MultiPartitionIndexKey::ByParentId(p.parent_multi_partition_id)
            }
        }
    }

    fn key_to_bytes(&self, key: &MultiPartitionIndexKey) -> Vec<u8> {
        match key {
            MultiPartitionIndexKey::ByMultiIndexId(id) => {
                let mut w = Cursor::new(Vec::with_capacity(8));
                w.write_u64::<BigEndian>(*id).unwrap();
                w.into_inner()
            }
            MultiPartitionIndexKey::ByParentId(id) => {
                let mut w = Cursor::new(Vec::with_capacity(9));
                match id {
                    None => w.write_u8(0).unwrap(),
                    Some(id) => {
                        w.write_u8(1).unwrap();
                        w.write_u64::<BigEndian>(*id).unwrap();
                    }
                }
                w.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            MultiPartitionRocksIndex::ByMultiIndexId => false,
            MultiPartitionRocksIndex::ByParentId => false,
        }
    }

    fn version(&self) -> u32 {
        match self {
            MultiPartitionRocksIndex::ByMultiIndexId => 1,
            MultiPartitionRocksIndex::ByParentId => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
