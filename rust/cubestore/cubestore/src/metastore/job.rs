use super::{IndexId, RocksSecondaryIndex, TableId};
use crate::base_rocks_secondary_index;
use crate::metastore::table::Table;
use crate::metastore::{RocksEntity, RowKey};
use crate::rocks_table_impl;
use crate::util::redact::redact_url_credentials;
use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::io::{Cursor, Write};

/// `Debug` is hand-written so that logs never carry an import location's credentials.
#[derive(Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum JobType {
    WalPartitioning,
    PartitionCompaction,
    TableImport,
    Repartition,
    TableImportCSV(/*location*/ String),
    MultiPartitionSplit,
    FinishMultiSplit,
    RepartitionChunk,
    InMemoryChunksCompaction,
    NodeInMemoryChunksCompaction(/*node*/ String),
    // Repartition an inclusive [start, end] chunk-id range of an inactive parent in
    // one merge+swap. row_reference carries the start chunk; the end is data only and
    // is deliberately excluded from the job index key (see key_to_bytes) so a tail
    // that extends the trailing range dedups on the start instead of spawning a
    // second job for the same start.
    RepartitionRange(/*end_chunk_id*/ u64),
    /// Fallback for job types written by a newer binary that this binary does
    /// not know about. Lets the read path decode such rows instead of failing
    /// the whole job scan; the worker ignores `Unknown` jobs.
    #[serde(other)]
    Unknown,
}

impl fmt::Debug for JobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobType::WalPartitioning => f.write_str("WalPartitioning"),
            JobType::PartitionCompaction => f.write_str("PartitionCompaction"),
            JobType::TableImport => f.write_str("TableImport"),
            JobType::Repartition => f.write_str("Repartition"),
            JobType::TableImportCSV(location) => f
                .debug_tuple("TableImportCSV")
                .field(&redact_url_credentials(location))
                .finish(),
            JobType::MultiPartitionSplit => f.write_str("MultiPartitionSplit"),
            JobType::FinishMultiSplit => f.write_str("FinishMultiSplit"),
            JobType::RepartitionChunk => f.write_str("RepartitionChunk"),
            JobType::InMemoryChunksCompaction => f.write_str("InMemoryChunksCompaction"),
            JobType::NodeInMemoryChunksCompaction(node) => f
                .debug_tuple("NodeInMemoryChunksCompaction")
                .field(node)
                .finish(),
            JobType::RepartitionRange(end_chunk_id) => f
                .debug_tuple("RepartitionRange")
                .field(end_chunk_id)
                .finish(),
            JobType::Unknown => f.write_str("Unknown"),
        }
    }
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
        JobType::RepartitionChunk => 8,
        JobType::InMemoryChunksCompaction => 9,
        JobType::NodeInMemoryChunksCompaction(_) => 10,
        JobType::Unknown => 11,
        JobType::RepartitionRange(_) => 12,
    }
}

/// Get the priority of a job type. Higher numbers are higher priority.
fn get_job_type_priority(j: &JobType) -> u32 {
    match j {
        JobType::WalPartitioning => 1000,
        JobType::PartitionCompaction => 1000,
        JobType::TableImport => 1000,
        JobType::Repartition => 1000,
        JobType::TableImportCSV(_) => 1000,
        JobType::MultiPartitionSplit => 1000,
        JobType::FinishMultiSplit => 1000,
        JobType::RepartitionChunk => 1000,
        JobType::RepartitionRange(_) => 1000,
        JobType::InMemoryChunksCompaction => 10000,
        JobType::NodeInMemoryChunksCompaction(_) => 10000,
        JobType::Unknown => 0,
    }
}

/// A job runner pool. Each runner serves exactly one pool and only picks up jobs that
/// belong to it. `CsvImport` overlaps with `Regular`: non-stream CSV imports stay eligible
/// for regular runners and additionally get a reserved pool so they progress even when all
/// regular runners are busy with compaction/repartition.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobRunnerPool {
    Regular,
    LongTerm,
    CsvImport,
}

#[derive(Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum JobStatus {
    Scheduled(String),
    ProcessingBy(String),
    Completed,
    Timeout,
    Error(String),
    Orphaned,
}

impl fmt::Debug for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobStatus::Scheduled(shard) => f.debug_tuple("Scheduled").field(shard).finish(),
            JobStatus::ProcessingBy(node) => f.debug_tuple("ProcessingBy").field(node).finish(),
            JobStatus::Completed => f.write_str("Completed"),
            JobStatus::Timeout => f.write_str("Timeout"),
            // Rows written before errors were redacted at their source still hold the raw text.
            JobStatus::Error(error) => f
                .debug_tuple("Error")
                .field(&redact_url_credentials(error))
                .finish(),
            JobStatus::Orphaned => f.write_str("Orphaned"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Hash)]
pub struct Job {
    row_reference: RowKey,
    job_type: JobType,
    last_heart_beat: DateTime<Utc>,
    status: JobStatus,
}

impl RocksEntity for Job {}

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

    pub fn is_long_term(&self) -> bool {
        match &self.job_type {
            JobType::TableImportCSV(location) if Table::is_stream_location(location) => true,
            _ => false,
        }
    }

    pub fn is_csv_import(&self) -> bool {
        matches!(
            &self.job_type,
            JobType::TableImportCSV(location) if !Table::is_stream_location(location)
        )
    }

    pub fn matches_pool(&self, pool: JobRunnerPool) -> bool {
        match pool {
            JobRunnerPool::Regular => !self.is_long_term(),
            JobRunnerPool::LongTerm => self.is_long_term(),
            JobRunnerPool::CsvImport => self.is_csv_import(),
        }
    }

    pub fn priority(&self) -> u32 {
        get_job_type_priority(&self.job_type)
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
                    JobType::TableImportCSV(l) | JobType::NodeInMemoryChunksCompaction(l) => {
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

    fn version(&self) -> u32 {
        match self {
            JobRocksIndex::RowReference => 1,
            JobRocksIndex::ByShard => 1,
        }
    }

    fn get_id(&self) -> IndexId {
        *self as IndexId
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    fn job(job_type: JobType) -> Job {
        Job::new(
            RowKey::Table(TableId::Tables, 1),
            job_type,
            "node".to_string(),
        )
    }

    #[test]
    fn matches_pool_routing() {
        let file_import = job(JobType::TableImportCSV("file.csv".to_string()));
        let stream_import = job(JobType::TableImportCSV("stream:topic".to_string()));
        let compaction = job(JobType::PartitionCompaction);

        // Non-stream CSV import stays eligible for the regular pool and additionally
        // for the reserved CSV pool, but is never long-term.
        assert!(file_import.matches_pool(JobRunnerPool::Regular));
        assert!(file_import.matches_pool(JobRunnerPool::CsvImport));
        assert!(!file_import.matches_pool(JobRunnerPool::LongTerm));

        // Streaming CSV import is long-term only.
        assert!(!stream_import.matches_pool(JobRunnerPool::Regular));
        assert!(!stream_import.matches_pool(JobRunnerPool::CsvImport));
        assert!(stream_import.matches_pool(JobRunnerPool::LongTerm));

        // Everything else belongs to the regular pool only.
        assert!(compaction.matches_pool(JobRunnerPool::Regular));
        assert!(!compaction.matches_pool(JobRunnerPool::CsvImport));
        assert!(!compaction.matches_pool(JobRunnerPool::LongTerm));
    }

    /// Mirrors `JobType` as it might look in a newer binary: it carries extra
    /// variants this binary does not know about (one unit, one data-carrying).
    /// We serialize with this enum and deserialize with the real `JobType` to
    /// emulate a forward version skew across `latest`/`release` channels.
    #[derive(Serialize)]
    enum NewerJobType {
        #[allow(dead_code)]
        WalPartitioning,
        #[allow(dead_code)]
        NodeInMemoryChunksCompaction(String),
        BrandNewUnitVariant,
        BrandNewDataVariant(String),
    }

    fn flex_roundtrip<S: Serialize, D: for<'de> Deserialize<'de>>(value: &S) -> Result<D, String> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        value.serialize(&mut ser).map_err(|e| e.to_string())?;
        let buffer = ser.take_buffer();
        let reader = flexbuffers::Reader::get_root(buffer.as_slice()).map_err(|e| e.to_string())?;
        D::deserialize(reader).map_err(|e| e.to_string())
    }

    #[test]
    fn unknown_unit_variant_decodes_to_unknown() {
        let decoded: JobType =
            flex_roundtrip(&NewerJobType::BrandNewUnitVariant).expect("unit variant must decode");
        assert_eq!(decoded, JobType::Unknown);
    }

    #[test]
    fn unknown_data_variant_decodes_to_unknown() {
        // A data-carrying unknown variant. flexbuffers must skip the payload and
        // land on `Unknown` rather than erroring; if this fails, `#[serde(other)]`
        // is insufficient for the job read path and Option B is required.
        let decoded: JobType =
            flex_roundtrip(&NewerJobType::BrandNewDataVariant("loc".to_string()))
                .expect("data variant must decode");
        assert_eq!(decoded, JobType::Unknown);
    }

    #[test]
    fn known_variants_still_roundtrip() {
        for jt in [
            JobType::WalPartitioning,
            JobType::TableImportCSV("s3://x".to_string()),
            JobType::NodeInMemoryChunksCompaction("node-1".to_string()),
        ] {
            let decoded: JobType = flex_roundtrip(&jt).expect("known variant must decode");
            assert_eq!(decoded, jt);
        }
    }

    const PRESIGNED_LOCATIONS: [&str; 3] = [
        "https://bucket.s3.eu-west-1.amazonaws.com/x/data.csv.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261029%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Expires=3600&X-Amz-Signature=deadbeef",
        "https://storage.googleapis.com/bucket/x/data.csv.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=example%40project.iam.gserviceaccount.com&X-Goog-Signature=deadbeef",
        "https://account.blob.core.windows.net/container/x/data.csv.gz?sv=2022-11-02&sp=r&se=2026-10-30T00%3A00%3A00Z&skoid=skoidexample&sig=deadbeef",
    ];

    #[test]
    fn debug_redacts_presigned_import_locations() {
        for location in PRESIGNED_LOCATIONS {
            let (host_and_path, _) = location.split_once('?').unwrap();
            let debug = format!(
                "{:?}",
                job(JobType::TableImportCSV(location.to_string()))
                    .update_status(JobStatus::Error(format!("Internal: failed {}", location)))
            );
            for secret in [
                "deadbeef",
                "AKIAEXAMPLE",
                "example%40project",
                "skoidexample",
            ] {
                assert!(!debug.contains(secret), "{secret} leaked: {debug}");
            }
            assert!(
                debug.contains(&format!("TableImportCSV(\"{host_and_path}?<redacted>\")")),
                "{debug}"
            );
            assert!(
                debug.contains(&format!(
                    "Error(\"Internal: failed {host_and_path}?<redacted>\")"
                )),
                "{debug}"
            );
        }
    }

    #[test]
    fn debug_of_other_job_types_is_unchanged() {
        for (job_type, expected) in [
            (
                JobType::TableImportCSV("stream://kafka/topic/0".to_string()),
                r#"TableImportCSV("stream://kafka/topic/0")"#,
            ),
            (
                JobType::TableImportCSV("temp://upload.csv".to_string()),
                r#"TableImportCSV("temp://upload.csv")"#,
            ),
            (
                JobType::TableImportCSV("/data/file.csv.gz".to_string()),
                r#"TableImportCSV("/data/file.csv.gz")"#,
            ),
            (
                JobType::TableImportCSV("https://host.example.com/file.csv".to_string()),
                r#"TableImportCSV("https://host.example.com/file.csv")"#,
            ),
            (JobType::WalPartitioning, "WalPartitioning"),
            (JobType::PartitionCompaction, "PartitionCompaction"),
            (JobType::TableImport, "TableImport"),
            (JobType::Repartition, "Repartition"),
            (JobType::MultiPartitionSplit, "MultiPartitionSplit"),
            (JobType::FinishMultiSplit, "FinishMultiSplit"),
            (JobType::RepartitionChunk, "RepartitionChunk"),
            (
                JobType::InMemoryChunksCompaction,
                "InMemoryChunksCompaction",
            ),
            (
                JobType::NodeInMemoryChunksCompaction("node-1".to_string()),
                r#"NodeInMemoryChunksCompaction("node-1")"#,
            ),
            (JobType::RepartitionRange(7), "RepartitionRange(7)"),
            (JobType::Unknown, "Unknown"),
        ] {
            assert_eq!(format!("{:?}", job_type), expected);
        }
        for (status, expected) in [
            (
                JobStatus::Scheduled("shard".to_string()),
                r#"Scheduled("shard")"#,
            ),
            (
                JobStatus::ProcessingBy("node".to_string()),
                r#"ProcessingBy("node")"#,
            ),
            (JobStatus::Completed, "Completed"),
            (JobStatus::Timeout, "Timeout"),
            (
                JobStatus::Error("CorruptData: x".to_string()),
                r#"Error("CorruptData: x")"#,
            ),
            (JobStatus::Orphaned, "Orphaned"),
        ] {
            assert_eq!(format!("{:?}", status), expected);
        }
    }

    #[test]
    fn stored_and_indexed_location_keeps_credentials() {
        for location in PRESIGNED_LOCATIONS {
            let job_type = JobType::TableImportCSV(location.to_string());
            let decoded: JobType = flex_roundtrip(&job_type).expect("must decode");
            assert_eq!(decoded, job_type);

            let index = JobRocksIndex::RowReference;
            let key = index.key_to_bytes(&index.typed_key_by(&job(job_type)));
            let mut expected = RowKey::Table(TableId::Tables, 1).to_bytes();
            expected.extend_from_slice(&5u32.to_be_bytes());
            expected.extend_from_slice(&(location.len() as u64).to_be_bytes());
            expected.extend_from_slice(location.as_bytes());
            assert_eq!(key, expected);
        }
    }

    #[test]
    fn unknown_variant_inside_job_struct_decodes() {
        // The real read path deserializes the whole `Job` struct, so verify the
        // fallback also works when the unknown enum is a nested field.
        #[derive(Serialize)]
        struct NewerJob {
            row_reference: RowKey,
            job_type: NewerJobType,
            last_heart_beat: DateTime<Utc>,
            status: JobStatus,
        }

        let newer = NewerJob {
            row_reference: RowKey::Table(TableId::Jobs, 1),
            job_type: NewerJobType::BrandNewDataVariant("payload".to_string()),
            last_heart_beat: Utc::now(),
            status: JobStatus::Scheduled("shard-1".to_string()),
        };
        let decoded: Job = flex_roundtrip(&newer).expect("job with unknown type must decode");
        assert_eq!(decoded.job_type(), &JobType::Unknown);
        assert!(matches!(decoded.status(), JobStatus::Scheduled(s) if s == "shard-1"));
    }
}
