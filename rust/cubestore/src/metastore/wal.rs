use rocksdb::DB;
use std::sync::Arc;
use serde::{Deserialize, Deserializer};
use super::{BaseRocksSecondaryIndex, RocksTable, IndexId, RocksSecondaryIndex, WAL, TableId};
use crate::metastore::{MetaStoreEvent, IdRow};
use crate::rocks_table_impl;

impl WAL {
    pub fn new(table_id: u64, row_count: usize) -> WAL {
        WAL { table_id, row_count: row_count as u64, uploaded: false }
    }

    pub fn get_row_count(&self) -> u64 {
        self.row_count
    }

    pub fn get_table_id(&self) -> u64 {
        self.table_id
    }

    pub fn set_uploaded(&self, uploaded: bool) -> WAL {
        WAL { table_id: self.table_id, row_count: self.row_count, uploaded }
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
    TableID = 1
}

rocks_table_impl!(
    WAL,
    WALRocksTable,
    TableId::WALs,
    { vec![Box::new(WALRocksIndex::TableID)] },
    DeleteWal
);

impl RocksSecondaryIndex<WAL, String> for WALRocksIndex {
    fn typed_key_by(&self, row: &WAL) -> String {
        match self {
            WALRocksIndex::TableID => row.table_id.to_string()
        }
    }

    fn key_to_bytes(&self, key: &String) -> Vec<u8> {
        key.as_bytes().to_vec()
    }

    fn is_unique(&self) -> bool {
        match self {
            WALRocksIndex::TableID => false
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
