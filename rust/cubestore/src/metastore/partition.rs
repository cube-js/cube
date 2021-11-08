use super::{
    BaseRocksSecondaryIndex, IndexId, Partition, RocksSecondaryIndex, RocksTable, TableId,
};
use crate::base_rocks_secondary_index;
use crate::metastore::{IdRow, MetaStoreEvent};
use crate::rocks_table_impl;
use crate::table::Row;
use byteorder::{BigEndian, WriteBytesExt};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rocksdb::DB;
use serde::{Deserialize, Deserializer};

impl Partition {
    pub fn new(
        index_id: u64,
        multi_partition_id: Option<u64>,
        min_value: Option<Row>,
        max_value: Option<Row>,
    ) -> Partition {
        Partition {
            index_id,
            multi_partition_id,
            min_value,
            max_value,
            parent_partition_id: None,
            active: true,
            warmed_up: false,
            main_table_row_count: 0,
            last_used: None,
            suffix: Some(
                String::from_utf8(thread_rng().sample_iter(&Alphanumeric).take(8).collect())
                    .unwrap()
                    .to_lowercase(),
            ),
        }
    }

    pub fn new_child(parent: &IdRow<Partition>, multi_partition_id: Option<u64>) -> Partition {
        Partition {
            index_id: parent.row.index_id,
            min_value: None,
            max_value: None,
            parent_partition_id: Some(parent.id),
            multi_partition_id,
            active: false,
            warmed_up: false,
            main_table_row_count: 0,
            last_used: None,
            suffix: Some(
                String::from_utf8(thread_rng().sample_iter(&Alphanumeric).take(8).collect())
                    .unwrap()
                    .to_lowercase(),
            ),
        }
    }
    pub fn get_min_val(&self) -> &Option<Row> {
        &self.min_value
    }

    pub fn get_max_val(&self) -> &Option<Row> {
        &self.max_value
    }

    pub fn get_full_name(&self, partition_id: u64) -> Option<String> {
        match self.has_main_table_file() {
            false => None,
            true => Some(partition_file_name(partition_id, self.suffix())),
        }
    }

    pub fn has_main_table_file(&self) -> bool {
        self.main_table_row_count != 0
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

    pub fn update_row_count(&self, main_table_row_count: u64) -> Partition {
        let mut p = self.clone();
        p.main_table_row_count = main_table_row_count;
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

    pub fn multi_partition_id(&self) -> Option<u64> {
        self.multi_partition_id
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

    pub fn suffix(&self) -> &Option<String> {
        &self.suffix
    }
}

pub fn partition_file_name(partition_id: u64, suffix: &Option<String>) -> String {
    format!(
        "{}{}.parquet",
        partition_id,
        suffix
            .as_ref()
            .map(|h| format!("-{}", h))
            .unwrap_or("".to_string())
    )
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PartitionRocksIndex {
    IndexId = 1,
    MultiPartitionId = 2,
}

rocks_table_impl!(Partition, PartitionRocksTable, TableId::Partitions, {
    vec![
        Box::new(PartitionRocksIndex::IndexId),
        Box::new(PartitionRocksIndex::MultiPartitionId),
    ]
});

#[derive(Hash, Clone, Debug)]
pub enum PartitionIndexKey {
    ByIndexId(u64),
    ByMultiPartitionId(Option<u64>),
}

base_rocks_secondary_index!(Partition, PartitionRocksIndex);

impl RocksSecondaryIndex<Partition, PartitionIndexKey> for PartitionRocksIndex {
    fn typed_key_by(&self, row: &Partition) -> PartitionIndexKey {
        match self {
            PartitionRocksIndex::IndexId => PartitionIndexKey::ByIndexId(row.index_id),
            PartitionRocksIndex::MultiPartitionId => {
                PartitionIndexKey::ByMultiPartitionId(row.multi_partition_id)
            }
        }
    }

    fn key_to_bytes(&self, key: &PartitionIndexKey) -> Vec<u8> {
        match key {
            PartitionIndexKey::ByIndexId(index_id) => {
                let mut buf = Vec::with_capacity(8);
                buf.write_u64::<BigEndian>(*index_id).unwrap();
                buf
            }
            PartitionIndexKey::ByMultiPartitionId(id) => match id {
                None => return vec![0],
                Some(id) => {
                    let mut buf = Vec::with_capacity(9);
                    buf.write_u8(1).unwrap();
                    buf.write_u64::<BigEndian>(*id).unwrap();
                    buf
                }
            },
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            PartitionRocksIndex::IndexId => false,
            PartitionRocksIndex::MultiPartitionId => false,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
