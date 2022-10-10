use super::{BaseRocksSecondaryIndex, IndexId, RocksSecondaryIndex, RocksTable, TableId};
use crate::base_rocks_secondary_index;
use crate::metastore::{ColumnFamilyName, DataFrameValue, IdRow, MetaStoreEvent};
use crate::rocks_table_impl;
use byteorder::{BigEndian, WriteBytesExt};
use rocksdb::DB;
use serde::{Deserialize, Deserializer, Serialize};
use std::io::{Cursor, Write};

#[derive(Clone, Serialize, Deserialize, Debug, Hash)]
pub enum SourceCredentials {
    KSql {
        user: Option<String>,
        password: Option<String>,
        url: String,
    },
}

impl DataFrameValue<String> for SourceCredentials {
    fn value(v: &Self) -> String {
        format!("{:?}", v)
    }
}

crate::data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Hash)]
pub struct Source {
    name: String,
    source_credentials: SourceCredentials
}
}

impl Source {
    pub fn new(name: String, source_credentials: SourceCredentials) -> Self {
        Self {
            name,
            source_credentials,
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn source_type(&self) -> &SourceCredentials {
        &self.source_credentials
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SourceRocksIndex {
    Name = 1,
}

base_rocks_secondary_index!(Source, SourceRocksIndex);

rocks_table_impl!(
    Source,
    SourceRocksTable,
    TableId::Sources,
    { vec![Box::new(SourceRocksIndex::Name)] },
    ColumnFamilyName::Default
);

#[derive(Hash, Clone, Debug)]
pub enum SourceIndexKey {
    Name(String),
}

impl RocksSecondaryIndex<Source, SourceIndexKey> for SourceRocksIndex {
    fn typed_key_by(&self, row: &Source) -> SourceIndexKey {
        match self {
            SourceRocksIndex::Name => SourceIndexKey::Name(row.name.to_string()),
        }
    }

    fn key_to_bytes(&self, key: &SourceIndexKey) -> Vec<u8> {
        match key {
            SourceIndexKey::Name(name) => {
                let mut buf = Cursor::new(Vec::new());
                buf.write_u32::<BigEndian>(name.len() as u32).unwrap();
                buf.write_all(name.as_bytes()).unwrap();
                buf.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            SourceRocksIndex::Name => true,
        }
    }

    fn version(&self) -> u32 {
        match self {
            SourceRocksIndex::Name => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
