use super::{BaseRocksSecondaryIndex, IndexId, RocksSecondaryIndex, RocksTable, TableId};
use crate::base_rocks_secondary_index;
use crate::metastore::{IdRow, MetaStoreEvent, RowKey};
use crate::rocks_table_impl;
use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};
use rocksdb::DB;
use serde::{Deserialize, Deserializer, Serialize};
use std::io::{Cursor, Write};

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum JobType {
    WalPartitioning,
    PartitionCompaction,
    TableImport,
    Repartition,
    TableImportCSV(/*location*/ String),
    MultiPartitionSplit,
    FinishMultiSplit,
}

fn get_job_type_index(j: &JobType) -> u32 {
    match j {
        JobType::WalPartitioning => 1,
        JobType::PartitionCompaction => 2,
        JobType::TableImport => 3,
        JobType::Repartition => 4,
        JobType::TableImportCSV(_) => 5,
        JobType::MultiPartitionSplit => 6,
        JobType::FinishMultiSplit => 7,
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
pub enum JobStatus {
    Scheduled(String),
    ProcessingBy(String),
    Completed,
    Timeout,
    Error(String),
}

#[derive(Clone, Serialize, Deserialize, Debug, Hash)]
pub struct Job {
    row_reference: RowKey,
    job_type: JobType,
    last_heart_beat: DateTime<Utc>,
    status: JobStatus,
}

impl Job {
    pub fn new(row_reference: RowKey, job_type: JobType, shard: String) -> Job {
        Job {
            row_reference,
            job_type,
            last_heart_beat: Utc::now(),
            status: JobStatus::Scheduled(shard),
        }
    }

    pub fn job_type(&self) -> &JobType {
        &self.job_type
    }

    pub fn row_reference(&self) -> &RowKey {
        &self.row_reference
    }

    pub fn last_heart_beat(&self) -> &DateTime<Utc> {
        &self.last_heart_beat
    }

    pub fn status(&self) -> &JobStatus {
        &self.status
    }

    pub fn update_status(&self, status: JobStatus) -> Job {
        Job {
            row_reference: self.row_reference.clone(),
            job_type: self.job_type.clone(),
            last_heart_beat: Utc::now(),
            status,
        }
    }

    pub fn start_processing(&self, node_name: String) -> Job {
        self.update_status(JobStatus::ProcessingBy(node_name))
    }

    pub fn update_heart_beat(&self) -> Job {
        self.update_status(self.status.clone())
    }

    pub fn completed(&self) -> Job {
        self.update_status(JobStatus::Completed)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum JobRocksIndex {
    RowReference = 1,
    ByShard,
}

base_rocks_secondary_index!(Job, JobRocksIndex);

rocks_table_impl!(Job, JobRocksTable, TableId::Jobs, {
    vec![
        Box::new(JobRocksIndex::RowReference),
        Box::new(JobRocksIndex::ByShard),
    ]
});

#[derive(Hash, Clone, Debug)]
pub enum JobIndexKey {
    RowReference(RowKey, JobType),
    ScheduledByShard(Option<String>),
}

impl RocksSecondaryIndex<Job, JobIndexKey> for JobRocksIndex {
    fn typed_key_by(&self, row: &Job) -> JobIndexKey {
        match self {
            JobRocksIndex::RowReference => {
                JobIndexKey::RowReference(row.row_reference.clone(), row.job_type.clone())
            }
            JobRocksIndex::ByShard => match &row.status {
                JobStatus::Scheduled(shard) => {
                    JobIndexKey::ScheduledByShard(Some(shard.to_string()))
                }
                _ => JobIndexKey::ScheduledByShard(None),
            },
        }
    }

    fn key_to_bytes(&self, key: &JobIndexKey) -> Vec<u8> {
        match key {
            JobIndexKey::RowReference(row_key, job_type) => {
                let mut buf = Cursor::new(Vec::new());
                buf.write_all(row_key.to_bytes().as_slice()).unwrap();
                buf.write_u32::<BigEndian>(get_job_type_index(job_type))
                    .unwrap();
                match job_type {
                    JobType::TableImportCSV(l) => {
                        buf.write_u64::<BigEndian>(l.len() as u64).unwrap();
                        buf.write(l.as_bytes()).unwrap();
                    }
                    _ => {}
                }
                buf.into_inner()
            }
            JobIndexKey::ScheduledByShard(shard) => {
                let mut buf = Cursor::new(Vec::new());
                buf.write_u32::<BigEndian>(shard.as_ref().map(|s| s.len() as u32).unwrap_or(0))
                    .unwrap();
                if let Some(v) = shard {
                    buf.write_all(v.as_bytes()).unwrap();
                }
                buf.into_inner()
            }
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            JobRocksIndex::RowReference => true,
            JobRocksIndex::ByShard => false,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}
