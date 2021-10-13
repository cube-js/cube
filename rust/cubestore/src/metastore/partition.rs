use super::{
    BaseRocksSecondaryIndex, IndexId, Partition, RocksSecondaryIndex, RocksTable, TableId,
};
use crate::base_rocks_secondary_index;
use crate::metastore::{IdRow, MetaStoreEvent};
use crate::rocks_table_impl;
use crate::table::Row;
use byteorder::{BigEndian, WriteBytesExt};
use rocksdb::DB;
use serde::{Deserialize, Deserializer};

impl Partition {
    pub fn new(index_id: u64, min_value: Option<Row>, max_value: Option<Row>) -> Partition {
        Partition {
            index_id,
            min_value,
            max_value,
            parent_partition_id: None,
            active: true,
            warmed_up: false,
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
            warmed_up: false,
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
        partition_file_name(self.parent_partition_id, partition_id)
    }

    pub fn to_active(&self, active: bool) -> Partition {
        let mut p = self.clone();
        p.active = active;
        p
    }

    pub fn to_warmed_up(&self) -> Partition {
        let mut p = self.clone();
        p.warmed_up = true;
        p
    }

    pub fn update_min_max_and_row_count(
        &self,
        min_value: Option<Row>,
        max_value: Option<Row>,
        main_table_row_count: u64,
    ) -> Partition {
        let mut p = self.clone();
        p.min_value = min_value;
        p.max_value = max_value;
        p.main_table_row_count = main_table_row_count;
        p
    }

    pub fn get_index_id(&self) -> u64 {
        self.index_id
    }

    pub fn parent_partition_id(&self) -> &Option<u64> {
        &self.parent_partition_id
    }

    pub fn update_parent_partition_id(&self, parent_partition_id: Option<u64>) -> Partition {
        let mut p = self.clone();
        p.parent_partition_id = parent_partition_id;
        p
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn is_warmed_up(&self) -> bool {
        self.warmed_up
    }

    pub fn main_table_row_count(&self) -> u64 {
        self.main_table_row_count
    }
}

pub fn partition_file_name(parent_partition_id: Option<u64>, partition_id: u64) -> Option<String> {
    parent_partition_id.and(Some(format!("{}.parquet", partition_id)))
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PartitionRocksIndex {
    IndexId = 1,
}

rocks_table_impl!(Partition, PartitionRocksTable, TableId::Partitions, {
    vec![Box::new(PartitionRocksIndex::IndexId)]
});

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
