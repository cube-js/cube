use super::{
    BaseRocksSecondaryIndex, Column, Index, IndexId, IndexType, RocksSecondaryIndex, RocksTable,
    TableId,
};
use crate::metastore::{ColumnFamilyName, IdRow, MetaStoreEvent};
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
        partition_split_key_size: Option<u64>,
        multi_index_id: Option<u64>,
        index_type: IndexType,
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
            partition_split_key_size,
            multi_index_id,
            index_type,
        })
    }

    pub fn table_id(&self) -> u64 {
        return self.table_id;
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_type(&self) -> IndexType {
        self.index_type.clone()
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

    pub fn partition_split_key_size(&self) -> &Option<u64> {
        &self.partition_split_key_size
    }

    pub fn multi_index_id(&self) -> Option<u64> {
        self.multi_index_id
    }

    pub fn index_type_default() -> IndexType {
        IndexType::Regular
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum IndexRocksIndex {
    Name = 1,
    TableID,
    MultiIndexId,
}

crate::base_rocks_secondary_index!(Index, IndexRocksIndex);

rocks_table_impl!(
    Index,
    IndexRocksTable,
    TableId::Indexes,
    {
        vec![
            Box::new(IndexRocksIndex::TableID),
            Box::new(IndexRocksIndex::Name),
            Box::new(IndexRocksIndex::MultiIndexId),
        ]
    },
    ColumnFamilyName::Default
);

#[derive(Hash, Clone, Debug)]
pub enum IndexIndexKey {
    TableId(u64),
    Name(u64, String),
    MultiIndexId(Option<u64>),
}

impl RocksSecondaryIndex<Index, IndexIndexKey> for IndexRocksIndex {
    fn typed_key_by(&self, row: &Index) -> IndexIndexKey {
        match self {
            IndexRocksIndex::TableID => IndexIndexKey::TableId(row.table_id),
            IndexRocksIndex::Name => IndexIndexKey::Name(row.table_id, row.name.to_string()),
            IndexRocksIndex::MultiIndexId => IndexIndexKey::MultiIndexId(row.multi_index_id),
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
            IndexIndexKey::MultiIndexId(multi_index_id) => {
                let mut buf = Cursor::new(Vec::with_capacity(9));
                match multi_index_id {
                    None => buf.write_u8(0).unwrap(),
                    Some(id) => {
                        buf.write_u8(1).unwrap();
                        buf.write_u64::<BigEndian>(*id).unwrap();
                    }
                }
                buf.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            IndexRocksIndex::TableID => false,
            IndexRocksIndex::Name => true,
            IndexRocksIndex::MultiIndexId => false,
        }
    }

    fn version(&self) -> u32 {
        match self {
            IndexRocksIndex::TableID => 1,
            IndexRocksIndex::Name => 1,
            IndexRocksIndex::MultiIndexId => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
