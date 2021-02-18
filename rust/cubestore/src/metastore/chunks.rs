use super::{BaseRocksSecondaryIndex, Chunk, IndexId, RocksSecondaryIndex, RocksTable, TableId};
use crate::base_rocks_secondary_index;
use crate::metastore::{IdRow, MetaStoreEvent};
use crate::rocks_table_impl;
use byteorder::{BigEndian, WriteBytesExt};
use chrono::Utc;
use rocksdb::DB;
use serde::{Deserialize, Deserializer};
use std::io::Cursor;
use std::ops::Sub;

impl Chunk {
    pub fn new(partition_id: u64, row_count: usize) -> Chunk {
        Chunk {
            partition_id,
            row_count: row_count as u64,
            uploaded: false,
            active: false,
            last_used: None,
        }
    }

    pub fn get_row_count(&self) -> u64 {
        self.row_count
    }

    pub fn get_full_name(&self, chunk_id: u64) -> String {
        format!("{}.chunk.parquet", chunk_id)
    }

    pub fn get_partition_id(&self) -> u64 {
        self.partition_id
    }

    pub fn set_uploaded(&self, uploaded: bool) -> Chunk {
        Chunk {
            partition_id: self.partition_id,
            row_count: self.row_count,
            uploaded,
            active: uploaded,
            last_used: self.last_used.clone(),
        }
    }

    pub fn deactivate(&self) -> Chunk {
        Chunk {
            partition_id: self.partition_id,
            row_count: self.row_count,
            uploaded: self.uploaded,
            active: false,
            last_used: self.last_used.clone(),
        }
    }

    pub fn update_last_used(&self) -> Self {
        let mut new = self.clone();
        new.last_used = Some(Utc::now());
        new
    }

    pub fn uploaded(&self) -> bool {
        self.uploaded
    }

    pub fn active(&self) -> bool {
        self.active
    }

    pub fn is_used(&self, timeout: u64) -> bool {
        self.last_used
            .map(|time| Utc::now().sub(time.clone()).num_seconds() < timeout as i64)
            .unwrap_or(false)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ChunkRocksIndex {
    PartitionId = 1,
}

rocks_table_impl!(Chunk, ChunkRocksTable, TableId::Chunks, {
    vec![Box::new(ChunkRocksIndex::PartitionId)]
});

base_rocks_secondary_index!(Chunk, ChunkRocksIndex);

#[derive(Hash, Clone, Debug)]
pub enum ChunkIndexKey {
    ByPartitionId(u64),
}

impl RocksSecondaryIndex<Chunk, ChunkIndexKey> for ChunkRocksIndex {
    fn typed_key_by(&self, row: &Chunk) -> ChunkIndexKey {
        match self {
            ChunkRocksIndex::PartitionId => ChunkIndexKey::ByPartitionId(row.partition_id),
        }
    }

    fn key_to_bytes(&self, key: &ChunkIndexKey) -> Vec<u8> {
        match key {
            ChunkIndexKey::ByPartitionId(partition_id) => {
                let mut buf = Cursor::new(Vec::new());
                buf.write_u64::<BigEndian>(*partition_id).unwrap();
                buf.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            ChunkRocksIndex::PartitionId => false,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
