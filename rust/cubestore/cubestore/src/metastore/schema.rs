use super::{BaseRocksSecondaryIndex, IndexId, RocksSecondaryIndex, RocksTable, Schema, TableId};
use crate::metastore::{ColumnFamilyName, IdRow, MetaStoreEvent};
use crate::rocks_table_impl;
use rocksdb::DB;
use serde::{Deserialize, Deserializer};

impl Schema {
    pub fn new(name: String) -> Schema {
        Schema { name }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn set_name(&mut self, name: &String) {
        self.name = name.clone();
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum SchemaRocksIndex {
    Name = 1,
}

rocks_table_impl!(
    Schema,
    SchemaRocksTable,
    TableId::Schemas,
    { vec![Box::new(SchemaRocksIndex::Name)] },
    ColumnFamilyName::Default
);

impl RocksSecondaryIndex<Schema, String> for SchemaRocksIndex {
    fn typed_key_by(&self, row: &Schema) -> String {
        match self {
            SchemaRocksIndex::Name => row.name.to_string(),
        }
    }

    fn key_to_bytes(&self, key: &String) -> Vec<u8> {
        key.as_bytes().to_vec()
    }

    fn is_unique(&self) -> bool {
        match self {
            SchemaRocksIndex::Name => true,
        }
    }

    fn version(&self) -> u32 {
        match self {
            SchemaRocksIndex::Name => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
