use super::{IndexId, RocksSecondaryIndex, TableId};
use crate::base_rocks_secondary_index;
use crate::metastore::RocksEntity;
use crate::rocks_table_impl;
use byteorder::{BigEndian, WriteBytesExt};

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::Debug;

crate::data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Hash)]
pub struct TraceObject {
    table_id: u64,
    trace_obj: String
}
}

impl TraceObject {
    pub fn new(table_id: u64, trace_obj: String) -> Self {
        Self {
            table_id,
            trace_obj,
        }
    }

    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    pub fn trace_obj(&self) -> &String {
        &self.trace_obj
    }
}

impl RocksEntity for TraceObject {}

#[derive(Clone, Copy, Debug)]
pub enum TraceObjectRocksIndex {
    ByTableId = 1,
}

base_rocks_secondary_index!(TraceObject, TraceObjectRocksIndex);

rocks_table_impl!(TraceObject, TraceObjectRocksTable, TableId::TraceObjects, {
    vec![Box::new(TraceObjectRocksIndex::ByTableId)]
});

#[derive(Hash, Clone, Debug)]
pub enum TraceObjectIndexKey {
    ByTableId(u64),
}

impl RocksSecondaryIndex<TraceObject, TraceObjectIndexKey> for TraceObjectRocksIndex {
    fn typed_key_by(&self, row: &TraceObject) -> TraceObjectIndexKey {
        match self {
            TraceObjectRocksIndex::ByTableId => TraceObjectIndexKey::ByTableId(row.table_id),
        }
    }

    fn key_to_bytes(&self, key: &TraceObjectIndexKey) -> Vec<u8> {
        match key {
            TraceObjectIndexKey::ByTableId(table_id) => {
                let mut buf = Vec::with_capacity(8);
                buf.write_u64::<BigEndian>(*table_id).unwrap();
                buf
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            TraceObjectRocksIndex::ByTableId => true,
        }
    }

    fn version(&self) -> u32 {
        match self {
            TraceObjectRocksIndex::ByTableId => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
