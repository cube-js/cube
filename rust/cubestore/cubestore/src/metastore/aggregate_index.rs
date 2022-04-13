use super::{
    AggregateIndex, AggregateIndexFun, BaseRocksSecondaryIndex, Column, IndexId,
    RocksSecondaryIndex, RocksTable, TableId,
};
use crate::metastore::{IdRow, MetaStoreEvent};
use crate::{rocks_table_impl, CubeError};
use byteorder::{BigEndian, WriteBytesExt};
use rocksdb::DB;
use serde::{Deserialize, Deserializer};
use std::io::{Cursor, Write};

impl AggregateIndex {
    pub fn try_new(
        name: String,
        table_id: u64,
        columns: Vec<Column>,
        aggregate_columns: Vec<Column>,
        aggregate_functions: Vec<AggregateIndexFun>,
        partition_split_key_size: Option<u64>,
    ) -> Result<Self, CubeError> {
        if columns.len() == 0 {
            return Err(CubeError::user(format!(
                "Aggregate index {} must contain at least one sort column",
                name
            )));
        }
        Ok(Self {
            name,
            table_id,
            columns,
            aggregate_columns,
            aggregate_functions,
            partition_split_key_size,
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
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum AggregateIndexRocksIndex {
    Name = 1,
    TableID,
}

crate::base_rocks_secondary_index!(AggregateIndex, AggregateIndexRocksIndex);

rocks_table_impl!(
    AggregateIndex,
    AggregateIndexRocksTable,
    TableId::AggregateIndexes,
    {
        vec![
            Box::new(AggregateIndexRocksIndex::TableID),
            Box::new(AggregateIndexRocksIndex::Name),
        ]
    }
);

#[derive(Hash, Clone, Debug)]
pub enum AggregateIndexIndexKey {
    TableId(u64),
    Name(u64, String),
}

impl RocksSecondaryIndex<AggregateIndex, AggregateIndexIndexKey> for AggregateIndexRocksIndex {
    fn typed_key_by(&self, row: &AggregateIndex) -> AggregateIndexIndexKey {
        match self {
            AggregateIndexRocksIndex::TableID => AggregateIndexIndexKey::TableId(row.table_id),
            AggregateIndexRocksIndex::Name => {
                AggregateIndexIndexKey::Name(row.table_id, row.name.to_string())
            }
        }
    }

    fn key_to_bytes(&self, key: &AggregateIndexIndexKey) -> Vec<u8> {
        match key {
            AggregateIndexIndexKey::TableId(table_id) => {
                let mut buf = Vec::with_capacity(8);
                buf.write_u64::<BigEndian>(*table_id).unwrap();
                buf
            }
            AggregateIndexIndexKey::Name(table_id, name) => {
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
            AggregateIndexRocksIndex::TableID => false,
            AggregateIndexRocksIndex::Name => true,
        }
    }

    fn version(&self) -> u32 {
        match self {
            AggregateIndexRocksIndex::TableID => 1,
            AggregateIndexRocksIndex::Name => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
