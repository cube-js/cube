use super::{Chunk, IndexId, RocksSecondaryIndex, TableId};

use crate::rocks_table_impl;
use crate::table::Row;
use crate::{base_rocks_secondary_index, CubeError};
use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use serde::{Deserialize, Deserializer};
use std::io::Cursor;

impl Chunk {
    pub fn new(
        partition_id: u64,
        row_count: usize,
        min: Option<Row>,
        max: Option<Row>,
        in_memory: bool,
    ) -> Chunk {
        Chunk {
            partition_id,
            row_count: row_count as u64,
            uploaded: false,
            active: false,
            last_used: None,
            in_memory,
            created_at: Some(Utc::now()),
            oldest_insert_at: Some(Utc::now()),
            deactivated_at: None,
            suffix: Some(
                String::from_utf8(thread_rng().sample_iter(&Alphanumeric).take(8).collect())
                    .unwrap()
                    .to_lowercase(),
            ),
            file_size: None,
            replay_handle_id: None,
            min,
            max,
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

    pub fn set_oldest_insert_at(&self, oldest_insert_at: Option<DateTime<Utc>>) -> Chunk {
        let mut to_update = self.clone();
        to_update.oldest_insert_at = oldest_insert_at;
        to_update
    }

    pub fn file_size(&self) -> Option<u64> {
        self.file_size
    }

    pub fn set_file_size(&self, file_size: u64) -> Result<Self, CubeError> {
        let mut c = self.clone();
        if file_size == 0 {
            return Err(CubeError::internal(format!(
                "Received zero file size for chunk"
            )));
        }
        c.file_size = Some(file_size);
        Ok(c)
    }

    pub fn deactivate(&self) -> Chunk {
        let mut to_update = self.clone();
        to_update.active = false;
        to_update.deactivated_at = Some(Utc::now());
        to_update.replay_handle_id = None;
        to_update
    }

    pub fn replay_handle_id(&self) -> &Option<u64> {
        &self.replay_handle_id
    }

    pub fn set_replay_handle_id(&self, replay_handle_id: Option<u64>) -> Chunk {
        let mut to_update = self.clone();
        to_update.replay_handle_id = replay_handle_id;
        to_update
    }

    pub fn min(&self) -> &Option<Row> {
        &self.min
    }

    pub fn max(&self) -> &Option<Row> {
        &self.max
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

    pub fn oldest_insert_at(&self) -> &Option<DateTime<Utc>> {
        &self.oldest_insert_at
    }

    pub fn deactivated_at(&self) -> &Option<DateTime<Utc>> {
        &self.deactivated_at
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
    ReplayHandleId = 2,
}

rocks_table_impl!(Chunk, ChunkRocksTable, TableId::Chunks, {
    vec![
        Box::new(ChunkRocksIndex::PartitionId),
        Box::new(ChunkRocksIndex::ReplayHandleId),
    ]
});

base_rocks_secondary_index!(Chunk, ChunkRocksIndex);

#[derive(Hash, Clone, Debug)]
pub enum ChunkIndexKey {
    ByPartitionId(u64),
    ByReplayHandleId(Option<u64>),
}

impl RocksSecondaryIndex<Chunk, ChunkIndexKey> for ChunkRocksIndex {
    fn typed_key_by(&self, row: &Chunk) -> ChunkIndexKey {
        match self {
            ChunkRocksIndex::PartitionId => ChunkIndexKey::ByPartitionId(row.partition_id),
            ChunkRocksIndex::ReplayHandleId => {
                ChunkIndexKey::ByReplayHandleId(row.replay_handle_id.clone())
            }
        }
    }

    fn key_to_bytes(&self, key: &ChunkIndexKey) -> Vec<u8> {
        match key {
            ChunkIndexKey::ByPartitionId(partition_id) => {
                let mut buf = Cursor::new(Vec::new());
                buf.write_u64::<BigEndian>(*partition_id).unwrap();
                buf.into_inner()
            }
            ChunkIndexKey::ByReplayHandleId(handle_id) => {
                let mut buf = Cursor::new(Vec::new());
                buf.write_u64::<BigEndian>(*handle_id.as_ref().unwrap_or(&0))
                    .unwrap();
                buf.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            ChunkRocksIndex::PartitionId => false,
            ChunkRocksIndex::ReplayHandleId => false,
        }
    }

    fn version(&self) -> u32 {
        match self {
            ChunkRocksIndex::PartitionId => 1,
            ChunkRocksIndex::ReplayHandleId => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
