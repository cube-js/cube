use super::{BaseRocksSecondaryIndex, Chunk, IndexId, RocksSecondaryIndex, RocksTable, TableId};
use crate::base_rocks_secondary_index;
use crate::metastore::{IdRow, MetaStoreEvent};
use crate::rocks_table_impl;
use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rocksdb::DB;
use serde::{Deserialize, Deserializer};
use std::io::Cursor;

impl Chunk {
    pub fn new(partition_id: u64, row_count: usize, in_memory: bool) -> Chunk {
        Chunk {
            partition_id,
            row_count: row_count as u64,
            uploaded: false,
            active: false,
            last_used: None,
            in_memory,
            created_at: Some(Utc::now()),
            suffix: Some(
                String::from_utf8(thread_rng().sample_iter(&Alphanumeric).take(8).collect())
                    .unwrap()
                    .to_lowercase(),
            ),
        }
    }

    pub fn get_row_count(&self) -> u64 {
        self.row_count
    }

    pub fn get_full_name(&self, chunk_id: u64) -> String {
        chunk_file_name(chunk_id, self.suffix())
    }

    pub fn get_partition_id(&self) -> u64 {
        self.partition_id
    }

    pub fn set_partition_id(&self, partition_id: u64) -> Self {
        let mut to_update = self.clone();
        to_update.partition_id = partition_id;
        to_update
    }

    pub fn set_uploaded(&self, uploaded: bool) -> Chunk {
        let mut to_update = self.clone();
        to_update.uploaded = uploaded;
        to_update.active = uploaded;
        to_update
    }

    pub fn deactivate(&self) -> Chunk {
        let mut to_update = self.clone();
        to_update.active = false;
        to_update
    }

    pub fn uploaded(&self) -> bool {
        self.uploaded
    }

    pub fn active(&self) -> bool {
        self.active
    }

    pub fn in_memory(&self) -> bool {
        self.in_memory
    }

    pub fn created_at(&self) -> &Option<DateTime<Utc>> {
        &self.created_at
    }

    pub fn suffix(&self) -> &Option<String> {
        &self.suffix
    }
}

pub fn chunk_file_name(chunk_id: u64, suffix: &Option<String>) -> String {
    format!(
        "{}{}.chunk.parquet",
        chunk_id,
        suffix
            .as_ref()
            .map(|h| format!("-{}", h))
            .unwrap_or("".to_string())
    )
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

    fn version(&self) -> u32 {
        match self {
            ChunkRocksIndex::PartitionId => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
