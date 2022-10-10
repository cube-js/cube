use super::{BaseRocksSecondaryIndex, IndexId, RocksSecondaryIndex, RocksTable, TableId, WAL};
use crate::base_rocks_secondary_index;
use crate::metastore::{ColumnFamilyName, IdRow, MetaStoreEvent};
use crate::rocks_table_impl;
use byteorder::{BigEndian, WriteBytesExt};
use rocksdb::DB;
use serde::{Deserialize, Deserializer};

impl WAL {
    pub fn new(table_id: u64, row_count: usize) -> WAL {
        WAL {
            table_id,
            row_count: row_count as u64,
            uploaded: false,
        }
    }

    pub fn get_row_count(&self) -> u64 {
        self.row_count
    }

    pub fn get_table_id(&self) -> u64 {
        self.table_id
    }

    pub fn set_uploaded(&self, uploaded: bool) -> WAL {
        WAL {
            table_id: self.table_id,
            row_count: self.row_count,
            uploaded,
        }
    }

    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    pub fn uploaded(&self) -> bool {
        self.uploaded
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum WALRocksIndex {
    TableID = 1,
}

rocks_table_impl!(
    WAL,
    WALRocksTable,
    TableId::WALs,
    { vec![Box::new(WALRocksIndex::TableID)] },
    ColumnFamilyName::Default
);

#[derive(Hash, Clone, Debug)]
pub enum WALIndexKey {
    ByTable(u64),
}

base_rocks_secondary_index!(WAL, WALRocksIndex);

impl RocksSecondaryIndex<WAL, WALIndexKey> for WALRocksIndex {
    fn typed_key_by(&self, row: &WAL) -> WALIndexKey {
        match self {
            WALRocksIndex::TableID => WALIndexKey::ByTable(row.table_id),
        }
    }

    fn key_to_bytes(&self, key: &WALIndexKey) -> Vec<u8> {
        match key {
            WALIndexKey::ByTable(table_id) => {
                let mut buf = Vec::new();
                buf.write_u64::<BigEndian>(*table_id).unwrap();
                buf
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            WALRocksIndex::TableID => false,
        }
    }

    fn version(&self) -> u32 {
        match self {
            WALRocksIndex::TableID => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
