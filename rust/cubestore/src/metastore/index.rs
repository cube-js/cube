use super::{
    BaseRocksSecondaryIndex, Column, Index, IndexId, RocksSecondaryIndex, RocksTable, TableId,
};
use crate::metastore::{IdRow, MetaStoreEvent};
use crate::{rocks_table_impl, CubeError};
use byteorder::{BigEndian, WriteBytesExt};
use rocksdb::DB;
use serde::{Deserialize, Deserializer};
use std::io::{Cursor, Write};

impl Index {
    pub fn try_new(
        name: String,
        table_id: u64,
        columns: Vec<Column>,
        sort_key_size: u64,
    ) -> Result<Index, CubeError> {
        if sort_key_size == 0 {
            return Err(CubeError::user(format!(
                "Sort key size can't be 0 for {}, columns: {:?}",
                name, columns
            )));
        }
        Ok(Index {
            name,
            table_id,
            columns,
            sort_key_size,
        })
    }

    pub fn table_id(&self) -> u64 {
        return self.table_id;
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    // TODO remove
    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn sort_key_size(&self) -> u64 {
        self.sort_key_size
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum IndexRocksIndex {
    Name = 1,
    TableID,
}

impl BaseRocksSecondaryIndex<Index> for IndexRocksIndex {
    fn index_key_by(&self, row: &Index) -> Vec<u8> {
        self.key_to_bytes(&self.typed_key_by(row))
    }

    fn get_id(&self) -> u32 {
        RocksSecondaryIndex::get_id(self)
    }

    fn is_unique(&self) -> bool {
        RocksSecondaryIndex::is_unique(self)
    }
}

rocks_table_impl!(Index, IndexRocksTable, TableId::Indexes, {
    vec![
        Box::new(IndexRocksIndex::TableID),
        Box::new(IndexRocksIndex::Name),
    ]
});

#[derive(Hash, Clone, Debug)]
pub enum IndexIndexKey {
    TableId(u64),
    Name(u64, String),
}

impl RocksSecondaryIndex<Index, IndexIndexKey> for IndexRocksIndex {
    fn typed_key_by(&self, row: &Index) -> IndexIndexKey {
        match self {
            IndexRocksIndex::TableID => IndexIndexKey::TableId(row.table_id),
            IndexRocksIndex::Name => IndexIndexKey::Name(row.table_id, row.name.to_string()),
        }
    }

    fn key_to_bytes(&self, key: &IndexIndexKey) -> Vec<u8> {
        match key {
            IndexIndexKey::TableId(table_id) => {
                let mut buf = Vec::with_capacity(8);
                buf.write_u64::<BigEndian>(*table_id).unwrap();
                buf
            }
            IndexIndexKey::Name(table_id, name) => {
                let bytes = name.as_bytes();
                let mut buf = Cursor::new(Vec::with_capacity(8 + bytes.len()));
                buf.write_u64::<BigEndian>(*table_id).unwrap();
                buf.write_all(bytes).unwrap();
                buf.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            IndexRocksIndex::TableID => false,
            IndexRocksIndex::Name => true,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
