use crate::config::ConfigObj;
use crate::metastore::table::TablePath;
use crate::metastore::{MetaStoreEvent, MetaStoreFs};
use crate::util::time_span::warn_long;

use crate::CubeError;
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use datafusion::cube_ext;

use cuberockstore::rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};
use cuberockstore::rocksdb::checkpoint::Checkpoint;
use cuberockstore::rocksdb::{
    DBCompressionType, Env, Snapshot, WriteBatch, WriteBatchIterator, DB,
};
use log::{info, trace};
use serde::{Deserialize, Serialize};
use serde_repr::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Cursor, Read, Write};

use crate::metastore::snapshot_info::SnapshotInfo;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use cuberockstore::rocksdb;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{env, mem, time};
use tokio::fs;
use tokio::fs::File;
use tokio::sync::broadcast::Sender;
use tokio::sync::{oneshot, Mutex as AsyncMutex, Notify, RwLock};

macro_rules! enum_from_primitive_impl {
    ($name:ident, $( $variant:ident )*) => {
        impl TryFrom<u32> for $name {
            type Error = crate::CubeError;

            fn try_from(n: u32) -> Result<Self, Self::Error> {
                $( if n == $name::$variant as u32 {
                    Ok($name::$variant)
                } else )* {
                    Err(crate::CubeError::internal(
                        format!("Unknown {}: {}", stringify!($name), n)
                    ))
                }
            }
        }
    };
}

macro_rules! enum_from_primitive {
    (
        $( #[$enum_attr:meta] )*
        pub enum $name:ident {
            $( $( $( #[$variant_attr:meta] )* $variant:ident ),+ = $discriminator:expr ),*
        }
    ) => {
        $( #[$enum_attr] )*
        pub enum $name {
            $( $( $( #[$variant_attr] )* $variant ),+ = $discriminator ),*
        }
        enum_from_primitive_impl! { $name, $( $( $variant )+ )* }
    };
}

enum_from_primitive! {
    #[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize, Hash)]
    pub enum TableId {
        Schemas = 0x0100,
        Tables = 0x0200,
        Indexes = 0x0300,
        Partitions = 0x0400,
        Chunks = 0x0500,
        WALs = 0x0600,
        Jobs = 0x0700,
        Sources = 0x0800,
        MultiIndexes = 0x0900,
        MultiPartitions = 0x0A00,
        ReplayHandles = 0x0B00,
        CacheItems = 0x0C00,
        QueueItems = 0x0D00,
        QueueResults = 0x0E00,
        TraceObjects = 0x0F00,
        QueueItemPayload = 0x1000

    }
}

impl TableId {
    #[inline]
    pub fn has_ttl(&self) -> bool {
        match self {
            TableId::Schemas => false,
            TableId::Tables => false,
            TableId::Indexes => false,
            TableId::Partitions => false,
            TableId::Chunks => false,
            TableId::WALs => false,
            TableId::Jobs => false,
            TableId::Sources => false,
            TableId::MultiIndexes => false,
            TableId::MultiPartitions => false,
            TableId::ReplayHandles => false,
            TableId::CacheItems => true,
            TableId::QueueItems => true,
            TableId::QueueResults => true,
            TableId::TraceObjects => false,
            TableId::QueueItemPayload => true,
        }
    }

    #[inline]
    pub fn get_ttl_field(&self) -> &'static str {
        "expire"
    }
}

pub fn get_fixed_prefix() -> usize {
    13
}

pub type SecondaryKeyHash = [u8; 8];
pub type IndexId = u32;

#[derive(Clone)]
pub struct MemorySequence {
    seq_store: Arc<Mutex<HashMap<TableId, u64>>>,
}

impl MemorySequence {
    pub fn new(seq_store: Arc<Mutex<HashMap<TableId, u64>>>) -> Self {
        Self { seq_store }
    }

    pub fn next_seq(&self, table_id: TableId, snapshot_value: u64) -> Result<u64, CubeError> {
        let mut store = self.seq_store.lock()?;
        let mut current = *store.entry(table_id).or_insert(snapshot_value);
        current += 1;
        store.insert(table_id, current);
        Ok(current)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct RocksSecondaryIndexValueTTLExtended {
    // Approximate counter of usage
    pub lfu: u8,
    // Last access time
    pub lru: Option<DateTime<Utc>>,
    // Raw size of table's value without compression
    pub raw_size: u32,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct RocksTableStats {
    pub table_name: String,
    pub keys_total: u32,
    pub size_total: u64,
    pub expired_keys_total: u32,
    pub expired_size_total: u64,
    pub min_row_size: u64,
    pub avg_row_size: u64,
    pub max_row_size: u64,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct RocksPropertyRow {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug)]
pub enum RocksSecondaryIndexValue<'a> {
    Hash(&'a [u8]),
    HashAndTTL(&'a [u8], Option<DateTime<Utc>>),
    HashAndTTLExtended(
        &'a [u8],
        Option<DateTime<Utc>>,
        RocksSecondaryIndexValueTTLExtended,
    ),
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
pub enum RocksSecondaryIndexValueVersion {
    OnlyHash = 1,
    WithTTLSupport = 2,
}

pub type PackedDateTime = u32;

fn base_date_epoch() -> NaiveDateTime {
    NaiveDate::from_ymd(2022, 1, 1).and_hms(0, 0, 0)
}

pub trait RocksSecondaryIndexValueVersionEncoder {
    fn encode_value_as_u32(&self) -> Result<u32, CubeError>;
}

pub trait RocksSecondaryIndexValueVersionDecoder {
    fn decode_value_as_opt_datetime(self) -> Result<Option<DateTime<Utc>>, CubeError>;
}

impl RocksSecondaryIndexValueVersionDecoder for u32 {
    fn decode_value_as_opt_datetime(self) -> Result<Option<DateTime<Utc>>, CubeError> {
        if self == 0 {
            return Ok(None);
        }

        let timestamp = DateTime::<Utc>::from_utc(base_date_epoch(), Utc)
            + chrono::Duration::seconds(self as i64);

        Ok(Some(timestamp))
    }
}

impl RocksSecondaryIndexValueVersionEncoder for DateTime<Utc> {
    fn encode_value_as_u32(&self) -> Result<u32, CubeError> {
        let seconds = self
            .naive_local()
            .signed_duration_since(base_date_epoch())
            .num_seconds();

        u32::try_from(seconds).map_err(|err| {
            CubeError::internal(format!("Unable to represent datetime as u32: {}", err))
        })
    }
}

impl RocksSecondaryIndexValueVersionEncoder for Option<DateTime<Utc>> {
    fn encode_value_as_u32(&self) -> Result<u32, CubeError> {
        match self {
            None => Ok(0),
            Some(v) => v.encode_value_as_u32(),
        }
    }
}

/// RocksSecondaryIndexValue represent a value for secondary index keys
///
/// | hash | - hash only
/// | hash | ttl - 8 | - with ttl
/// | hash | ttl - 4 | raw_size - 4 | lru - 4 | lfu - 1 | - extended ttl
///
/// Where:
///
/// Hash is a set of character data of indeterminate length.
///
/// LRU/TTl (HashAndTTLExtended) is encoded as u32 (seconds since our epoch),
/// u32 = 4294967295 / (60 * 60 * 24 * 365) = 136 years from 2022/1/1.
impl<'a> RocksSecondaryIndexValue<'a> {
    pub fn from_bytes(
        bytes: &'a [u8],
        value_version: RocksSecondaryIndexValueVersion,
    ) -> Result<RocksSecondaryIndexValue<'a>, CubeError> {
        match value_version {
            RocksSecondaryIndexValueVersion::OnlyHash => Ok(RocksSecondaryIndexValue::Hash(bytes)),
            RocksSecondaryIndexValueVersion::WithTTLSupport => match bytes[0] {
                0 => Ok(RocksSecondaryIndexValue::Hash(bytes)),
                1 => {
                    let hash_size = bytes.len() - 8;
                    let (hash, mut expire_buf) = (&bytes[1..hash_size], &bytes[hash_size..]);
                    let expire_timestamp = expire_buf.read_i64::<BigEndian>()?;

                    let expire = if expire_timestamp == 0 {
                        None
                    } else {
                        Some(DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp(expire_timestamp, 0),
                            Utc,
                        ))
                    };

                    Ok(RocksSecondaryIndexValue::HashAndTTL(&hash, expire))
                }
                2 => {
                    let hash_size = bytes.len() - (4 + 4 + 4 + 1);
                    let (hash, mut expire_buf, extended_buf) = (
                        &bytes[1..hash_size],
                        &bytes[hash_size..hash_size + 4],
                        &bytes[hash_size + 4..],
                    );

                    let expire = expire_buf
                        .read_u32::<BigEndian>()?
                        .decode_value_as_opt_datetime()?;

                    let (mut size_buf, mut lru_buf, mut lfu_buf) = (
                        &extended_buf[0..4],
                        &extended_buf[4..8],
                        &extended_buf[8..9],
                    );

                    let raw_size = size_buf.read_u32::<BigEndian>()?;
                    let lru = lru_buf
                        .read_u32::<BigEndian>()?
                        .decode_value_as_opt_datetime()?;
                    let lfu = lfu_buf.read_u8()?;

                    Ok(RocksSecondaryIndexValue::HashAndTTLExtended(
                        &hash,
                        expire,
                        RocksSecondaryIndexValueTTLExtended { raw_size, lru, lfu },
                    ))
                }
                tid => Err(CubeError::internal(format!(
                    "Unsupported type \"{}\" of value for index",
                    tid
                ))),
            },
            #[allow(unreachable_patterns)]
            version => Err(CubeError::internal(format!(
                "Unsupported value_version {:?}",
                version
            ))),
        }
    }

    pub fn to_bytes(
        &self,
        value_version: RocksSecondaryIndexValueVersion,
    ) -> Result<Vec<u8>, CubeError> {
        match value_version {
            RocksSecondaryIndexValueVersion::OnlyHash => match *self {
                RocksSecondaryIndexValue::Hash(hash) => Ok(hash.to_vec()),
                RocksSecondaryIndexValue::HashAndTTL(_, _) => panic!(
                    "RocksSecondaryIndexValue::HashAndTTL is not supported for value_version = 1"
                ),
                RocksSecondaryIndexValue::HashAndTTLExtended(_, _, _) => panic!(
                    "RocksSecondaryIndexValue::HashAndTTLWithUsageInfo is not supported for value_version = 1"
                ),
            },
            RocksSecondaryIndexValueVersion::WithTTLSupport => match self {
                RocksSecondaryIndexValue::Hash(hash) => {
                    let mut buf = Cursor::new(Vec::with_capacity(hash.len() + 1));

                    buf.write_u8(0)?;
                    buf.write_all(&hash)?;

                    Ok(buf.into_inner())
                }
                RocksSecondaryIndexValue::HashAndTTL(hash, expire) => {
                    let mut buf = Cursor::new(Vec::with_capacity(hash.len() + 1 + 8));

                    buf.write_u8(1)?;
                    buf.write_all(&hash)?;

                    if let Some(ex) = expire {
                        buf.write_i64::<BigEndian>(ex.timestamp())?
                    } else {
                        buf.write_i64::<BigEndian>(0)?
                    }

                    Ok(buf.into_inner())
                },
                RocksSecondaryIndexValue::HashAndTTLExtended(hash, expire, info) => {
                    let mut buf = Cursor::new(Vec::with_capacity(hash.len() + 1 + 4 + 4 + 8 + 1));

                    buf.write_u8(2)?;
                    buf.write_all(&hash)?;
                    buf.write_u32::<BigEndian>(expire.encode_value_as_u32()?)?;

                    buf.write_u32::<BigEndian>(info.raw_size)?;
                    buf.write_u32::<BigEndian>(info.lru.encode_value_as_u32()?)?;
                    buf.write_u8(info.lfu)?;

                    Ok(buf.into_inner())
                }
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum RowKey {
    Table(TableId, /** row_id */ u64),
    Sequence(TableId),
    SecondaryIndex(IndexId, SecondaryKeyHash, /** row_id */ u64),
    SecondaryIndexInfo { index_id: IndexId },
    TableInfo { table_id: TableId },
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct SecondaryIndexInfo {
    // user specific version
    pub version: u32,
    #[serde(default = "secondary_index_info_default_value_version")]
    // serialization/deserialization version
    pub value_version: RocksSecondaryIndexValueVersion,
}

fn secondary_index_info_default_value_version() -> RocksSecondaryIndexValueVersion {
    RocksSecondaryIndexValueVersion::OnlyHash
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct TableInfo {
    // user specific version
    pub version: u32,
    // serialization/deserialization version, reserved, not used
    pub value_version: u32,
}

impl RowKey {
    pub fn try_from_bytes(bytes: &[u8]) -> Result<RowKey, CubeError> {
        let mut reader = Cursor::new(bytes);
        match reader.read_u8()? {
            1 => Ok(RowKey::Table(
                TableId::try_from(reader.read_u32::<BigEndian>()?)?,
                {
                    // skip zero for fixed key padding
                    reader.read_u64::<BigEndian>()?;
                    reader.read_u64::<BigEndian>()?
                },
            )),
            2 => Ok(RowKey::Sequence(TableId::try_from(
                reader.read_u32::<BigEndian>()?,
            )?)),
            3 => {
                let table_id = IndexId::from(reader.read_u32::<BigEndian>()?);

                let mut secondary_key: SecondaryKeyHash = [0_u8; 8];
                reader.read_exact(&mut secondary_key)?;

                let row_id = reader.read_u64::<BigEndian>()?;

                Ok(RowKey::SecondaryIndex(table_id, secondary_key, row_id))
            }
            4 => {
                let index_id = IndexId::from(reader.read_u32::<BigEndian>()?);

                Ok(RowKey::SecondaryIndexInfo { index_id })
            }
            5 => {
                let table_id = TableId::try_from(reader.read_u32::<BigEndian>()?)?;

                Ok(RowKey::TableInfo { table_id })
            }
            v => Err(CubeError::internal(format!("Unknown key prefix: {}", v))),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> RowKey {
        RowKey::try_from_bytes(bytes).unwrap()
    }

    pub fn to_iterate_range(&self) -> std::ops::Range<Vec<u8>> {
        let mut min = Vec::with_capacity(5);
        let mut max = Vec::with_capacity(5);

        match self {
            RowKey::Table(table_id, _) => {
                min.write_u8(1).unwrap();
                max.write_u8(1).unwrap();

                min.write_u32::<BigEndian>(*table_id as u32).unwrap();
                max.write_u32::<BigEndian>((*table_id as u32) + 1).unwrap();
            }
            RowKey::Sequence(table_id) => {
                min.write_u8(2).unwrap();
                max.write_u8(2).unwrap();

                min.write_u32::<BigEndian>(*table_id as u32).unwrap();
                max.write_u32::<BigEndian>((*table_id as u32) + 1).unwrap();
            }
            RowKey::SecondaryIndex(index_id, _, _) => {
                min.write_u8(3).unwrap();
                max.write_u8(3).unwrap();

                min.write_u32::<BigEndian>(*index_id as IndexId).unwrap();
                max.write_u32::<BigEndian>((*index_id as IndexId) + 1)
                    .unwrap();
            }
            RowKey::SecondaryIndexInfo { index_id } => {
                min.write_u8(4).unwrap();
                max.write_u8(4).unwrap();

                min.write_u32::<BigEndian>(*index_id as IndexId).unwrap();
                max.write_u32::<BigEndian>((*index_id as IndexId) + 1)
                    .unwrap();
            }
            RowKey::TableInfo { table_id } => {
                min.write_u8(5).unwrap();
                max.write_u8(5).unwrap();

                min.write_u32::<BigEndian>(*table_id as u32).unwrap();
                max.write_u32::<BigEndian>((*table_id as u32) + 1).unwrap();
            }
        }

        min..max
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut wtr = vec![];
        match self {
            RowKey::Table(table_id, row_id) => {
                wtr.write_u8(1).unwrap();
                wtr.write_u32::<BigEndian>(*table_id as u32).unwrap();
                wtr.write_u64::<BigEndian>(0).unwrap();
                wtr.write_u64::<BigEndian>(row_id.clone()).unwrap();
            }
            RowKey::Sequence(table_id) => {
                wtr.write_u8(2).unwrap();
                wtr.write_u32::<BigEndian>(*table_id as u32).unwrap();
            }
            RowKey::SecondaryIndex(index_id, secondary_key, row_id) => {
                wtr.write_u8(3).unwrap();
                wtr.write_u32::<BigEndian>(*index_id as IndexId).unwrap();
                for &n in secondary_key {
                    wtr.write_u8(n).unwrap();
                }
                wtr.write_u64::<BigEndian>(row_id.clone()).unwrap();
            }
            RowKey::SecondaryIndexInfo { index_id } => {
                wtr.write_u8(4).unwrap();
                wtr.write_u32::<BigEndian>(*index_id as IndexId).unwrap();
            }
            RowKey::TableInfo { table_id } => {
                wtr.write_u8(5).unwrap();
                wtr.write_u32::<BigEndian>(*table_id as u32).unwrap();
            }
        }
        wtr
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum WriteBatchEntry {
    Put { key: Box<[u8]>, value: Box<[u8]> },
    Delete { key: Box<[u8]> },
}

impl WriteBatchEntry {
    pub fn size(&self) -> usize {
        match self {
            Self::Put { key, value } => key.len() + value.len(),
            Self::Delete { key } => key.len(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WriteBatchContainer {
    entries: Vec<WriteBatchEntry>,
}

impl WriteBatchContainer {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.entries.iter().fold(0, |acc, i| acc + i.size())
    }

    pub fn write_batch(&self) -> WriteBatch {
        let mut batch = WriteBatch::default();
        for entry in self.entries.iter() {
            match entry {
                WriteBatchEntry::Put { key, value } => batch.put(key, value),
                WriteBatchEntry::Delete { key } => batch.delete(key),
            }
        }
        batch
    }

    pub async fn write_to_file(self, file_name: &str) -> Result<(), CubeError> {
        let serialized = flexbuffers::to_vec(self)?;

        let mut file = File::create(file_name).await?;
        Ok(tokio::io::AsyncWriteExt::write_all(&mut file, &serialized).await?)
    }

    pub async fn read_from_file(file_name: &str) -> Result<Self, CubeError> {
        let mut file = File::open(file_name).await?;

        let mut buffer = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut file, &mut buffer).await?;
        let r = flexbuffers::Reader::get_root(&buffer)?;
        Ok(Self::deserialize(r)?)
    }
}

impl WriteBatchIterator for WriteBatchContainer {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.entries.push(WriteBatchEntry::Put { key, value });
    }

    fn delete(&mut self, key: Box<[u8]>) {
        self.entries.push(WriteBatchEntry::Delete { key });
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct IdRow<T: Clone> {
    pub(crate) id: u64,
    pub(crate) row: T,
}

impl<T: Clone> IdRow<T> {
    pub fn new(id: u64, row: T) -> IdRow<T> {
        IdRow { id, row }
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_row(&self) -> &T {
        &self.row
    }

    pub fn into_row(self) -> T {
        self.row
    }
}

pub struct KeyVal {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

pub struct BatchPipe<'a> {
    db: &'a DB,
    write_batch: WriteBatch,
    events: Vec<MetaStoreEvent>,
    pub invalidate_tables_cache: bool,
}

impl<'a> BatchPipe<'a> {
    pub fn new(db: &'a DB) -> BatchPipe<'a> {
        BatchPipe {
            db,
            write_batch: WriteBatch::default(),
            events: Vec::new(),
            invalidate_tables_cache: false,
        }
    }

    pub fn batch(&mut self) -> &mut WriteBatch {
        &mut self.write_batch
    }

    pub fn add_event(&mut self, event: MetaStoreEvent) {
        self.events.push(event);
    }

    pub fn batch_write_rows(self) -> Result<Vec<MetaStoreEvent>, CubeError> {
        let db = self.db;
        db.write(self.write_batch)?;
        Ok(self.events)
    }

    pub fn invalidate_tables_cache(&mut self) {
        self.invalidate_tables_cache = true;
    }
}

#[derive(Clone)]
pub struct DbTableRef<'a> {
    pub db: &'a DB,
    pub snapshot: &'a Snapshot<'a>,
    pub mem_seq: MemorySequence,
    pub start_time: DateTime<Utc>,
}

#[async_trait]
pub trait MetaStoreTable: Send + Sync {
    type T: Serialize + Clone + Debug + 'static;

    async fn all_rows(&self) -> Result<Vec<IdRow<Self::T>>, CubeError>;

    async fn row_by_id_or_not_found(&self, id: u64) -> Result<IdRow<Self::T>, CubeError>;

    async fn delete(&self, id: u64) -> Result<IdRow<Self::T>, CubeError>;
}

#[macro_export]
macro_rules! meta_store_table_impl {
    ($name: ident, $table: ty, $rocks_table: ident) => {
        pub struct $name {
            rocks_meta_store: Arc<RocksStore>,
        }

        impl $name {
            fn table<'a>(db: DbTableRef<'a>) -> $rocks_table<'a> {
                <$rocks_table>::new(db)
            }
        }

        #[async_trait]
        impl MetaStoreTable for $name {
            type T = $table;

            async fn all_rows(&self) -> Result<Vec<IdRow<Self::T>>, CubeError> {
                self.rocks_meta_store
                    .read_operation_out_of_queue("all_rows", move |db_ref| {
                        Ok(Self::table(db_ref).all_rows()?)
                    })
                    .await
            }

            async fn row_by_id_or_not_found(&self, id: u64) -> Result<IdRow<Self::T>, CubeError> {
                self.rocks_meta_store
                    .read_operation("row_by_id_or_not_found", move |db_ref| {
                        Ok(Self::table(db_ref).get_row_or_not_found(id)?)
                    })
                    .await
            }

            async fn delete(&self, id: u64) -> Result<IdRow<Self::T>, CubeError> {
                self.rocks_meta_store
                    .write_operation("delete", move |db_ref, batch| {
                        Ok(Self::table(db_ref).delete(id, batch)?)
                    })
                    .await
            }
        }
    };
}

#[derive(Debug, Clone)]
pub enum RocksStoreChecksumType {
    NoChecksum = 0,
    CRC32c = 1,
    XXHash = 2,
    XXHash64 = 3,
    XXH3 = 4, // Supported since RocksDB 6.27
}

impl RocksStoreChecksumType {
    pub fn as_rocksdb_enum(&self) -> rocksdb::ChecksumType {
        match &self {
            RocksStoreChecksumType::NoChecksum => rocksdb::ChecksumType::NoChecksum,
            RocksStoreChecksumType::CRC32c => rocksdb::ChecksumType::CRC32c,
            RocksStoreChecksumType::XXHash => rocksdb::ChecksumType::XXHash,
            RocksStoreChecksumType::XXHash64 => rocksdb::ChecksumType::XXHash64,
            RocksStoreChecksumType::XXH3 => rocksdb::ChecksumType::XXH3,
        }
    }
}

pub type RocksStoreCompressionType = DBCompressionType;

#[derive(Debug, Clone)]
pub struct RocksStoreConfig {
    pub checksum_type: RocksStoreChecksumType,
    pub cache_capacity: usize,
    pub compression_type: RocksStoreCompressionType,
    pub bottommost_compression_type: RocksStoreCompressionType,
    // Sets maximum number of concurrent background jobs (compactions and flushes).
    pub max_background_jobs: u32,
    // Sets maximum number of threads that will concurrently perform a compaction job by breaking
    // it into multiple, smaller ones that are run simultaneously.
    pub max_subcompactions: u32,
    // By default, RocksDB uses only one background thread for flush and compaction.
    pub parallelism: u32,
}

impl RocksStoreConfig {
    pub fn metastore_default() -> Self {
        Self {
            // Supported since RocksDB 6.27
            checksum_type: RocksStoreChecksumType::XXH3,
            cache_capacity: 8 * 1024 * 1024,
            compression_type: RocksStoreCompressionType::None,
            bottommost_compression_type: RocksStoreCompressionType::None,
            max_background_jobs: 2,
            // Default: 1 (i.e. no subcompactions)
            max_subcompactions: 1,
            // Default: 1
            parallelism: 2,
        }
    }

    pub fn cachestore_default() -> Self {
        Self {
            // Supported since RocksDB 6.27
            checksum_type: RocksStoreChecksumType::XXH3,
            cache_capacity: 8 * 1024 * 1024,
            compression_type: RocksStoreCompressionType::None,
            bottommost_compression_type: RocksStoreCompressionType::None,
            max_background_jobs: 2,
            // Default: 1 (i.e. no subcompactions)
            max_subcompactions: 1,
            // Default: 1
            parallelism: 2,
        }
    }
}

pub trait RocksStoreDetails: Send + Sync {
    fn open_db(&self, path: &Path, config: &Arc<dyn ConfigObj>) -> Result<DB, CubeError>;

    fn open_readonly_db(&self, path: &Path, config: &Arc<dyn ConfigObj>) -> Result<DB, CubeError>;

    fn migrate(&self, table_ref: DbTableRef) -> Result<(), CubeError>;

    fn get_name(&self) -> &'static str;

    fn log_enabled(&self) -> bool;
}

pub type RocksStoreRWLoopFn = Box<dyn FnOnce() -> Result<(), CubeError> + Send + 'static>;

#[derive(Debug, Clone)]
pub struct RocksStoreRWLoop {
    name: &'static str,
    tx: tokio::sync::mpsc::Sender<RocksStoreRWLoopFn>,
}

impl RocksStoreRWLoop {
    pub fn new(store_name: &'static str, name: &'static str) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<RocksStoreRWLoopFn>(32_768);

        let thread_name = format!("{}-{}-rwloop", store_name, name);
        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || loop {
                if let Some(fun) = rx.blocking_recv() {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(fun)) {
                        Err(panic_payload) => {
                            let restore_error = CubeError::from_panic_payload(panic_payload);
                            log::error!(
                                "Panic during read write loop execution: {}",
                                restore_error
                            );
                        }
                        Ok(res) => {
                            if let Err(e) = res {
                                log::error!("Error during read write loop execution: {}", e);
                            }
                        }
                    }
                } else {
                    return;
                }
            })
            .expect(&format!(
                "Failed to spawn RWLoop thread for store '{}', name '{}'",
                store_name, name
            ));

        // Thread handle is intentionally dropped - thread will exit when tx is dropped
        Self { name, tx }
    }

    pub async fn schedule(&self, fun: RocksStoreRWLoopFn) -> Result<(), CubeError> {
        self.tx.send(fun).await.map_err(|err| {
            CubeError::user(format!(
                "Failed to schedule task to RWLoop ({}), error: {}",
                self.name, err
            ))
        })
    }

    pub fn get_name(&self) -> &'static str {
        self.name
    }
}

#[derive(Clone)]
pub struct RocksStore {
    pub db: Arc<DB>,
    pub config: Arc<dyn ConfigObj>,
    seq_store: Arc<Mutex<HashMap<TableId, u64>>>,
    pub listeners: Arc<RwLock<Vec<Sender<MetaStoreEvent>>>>,
    metastore_fs: Arc<dyn MetaStoreFs>,
    last_checkpoint_time: Arc<RwLock<SystemTime>>,
    write_notify: Arc<Notify>,
    pub(crate) write_completed_notify: Arc<Notify>,
    last_upload_seq: Arc<RwLock<u64>>,
    last_check_seq: Arc<RwLock<u64>>,
    snapshot_uploaded: Arc<RwLock<bool>>,
    snapshots_upload_stopped: Arc<AsyncMutex<bool>>,
    pub(crate) cached_tables: Arc<Mutex<Option<Arc<Vec<TablePath>>>>>,
    rw_loop_default_cf: RocksStoreRWLoop,
    details: Arc<dyn RocksStoreDetails>,
}

pub fn check_if_exists(name: &String, existing_keys_len: usize) -> Result<(), CubeError> {
    if existing_keys_len > 1 {
        let e = CubeError::user(format!(
            "Schema with name '{}' has more than one id. Something went wrong.",
            name
        ));
        return Err(e);
    } else if existing_keys_len == 0 {
        let e = CubeError::user(format!("Schema with name '{}' does not exist.", name));
        return Err(e);
    }
    Ok(())
}

impl RocksStore {
    pub fn with_listener(
        path: &Path,
        listeners: Vec<Sender<MetaStoreEvent>>,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        details: Arc<dyn RocksStoreDetails>,
    ) -> Result<Arc<Self>, CubeError> {
        let meta_store = Self::with_listener_impl(path, listeners, metastore_fs, config, details)?;
        Ok(Arc::new(meta_store))
    }

    pub fn with_listener_impl(
        path: &Path,
        listeners: Vec<Sender<MetaStoreEvent>>,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        details: Arc<dyn RocksStoreDetails>,
    ) -> Result<Self, CubeError> {
        let db = details.open_db(path, &config)?;
        let db_arc = Arc::new(db);

        let meta_store = RocksStore {
            db: db_arc.clone(),
            seq_store: Arc::new(Mutex::new(HashMap::new())),
            listeners: Arc::new(RwLock::new(listeners)),
            metastore_fs,
            last_checkpoint_time: Arc::new(RwLock::new(SystemTime::now())),
            snapshot_uploaded: Arc::new(RwLock::new(false)),
            write_notify: Arc::new(Notify::new()),
            write_completed_notify: Arc::new(Notify::new()),
            last_upload_seq: Arc::new(RwLock::new(db_arc.latest_sequence_number())),
            last_check_seq: Arc::new(RwLock::new(db_arc.latest_sequence_number())),
            snapshots_upload_stopped: Arc::new(AsyncMutex::new(false)),
            config,
            cached_tables: Arc::new(Mutex::new(None)),
            rw_loop_default_cf: RocksStoreRWLoop::new("metastore", "default"),
            details,
        };

        Ok(meta_store)
    }

    pub fn new(
        path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        details: Arc<dyn RocksStoreDetails>,
    ) -> Result<Arc<Self>, CubeError> {
        Self::with_listener(path, vec![], metastore_fs, config, details)
    }

    pub async fn load_from_dump(
        path: &Path,
        dump_path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        details: Arc<dyn RocksStoreDetails>,
    ) -> Result<Arc<Self>, CubeError> {
        if !fs::metadata(path).await.is_ok() {
            let opts = BackupEngineOptions::new(dump_path)?;
            let mut backup = BackupEngine::open(&opts, &Env::new()?)?;
            backup.restore_from_latest_backup(&path, &path, &RestoreOptions::default())?;
        } else {
            info!(
                "Using existing {} in {}",
                details.get_name(),
                path.as_os_str().to_string_lossy()
            );
        }

        let meta_store = Self::new(path, metastore_fs, config, details)?;
        Self::check_all_indexes(&meta_store).await?;

        Ok(meta_store)
    }

    pub async fn check_all_indexes(meta_store: &Arc<Self>) -> Result<(), CubeError> {
        let meta_store_to_move = meta_store.clone();

        cube_ext::spawn_blocking(move || {
            trace!(
                "Migration for {}: started",
                meta_store_to_move.details.get_name()
            );

            let table_ref = DbTableRef {
                db: &meta_store_to_move.db,
                snapshot: &meta_store_to_move.db.snapshot(),
                mem_seq: MemorySequence::new(meta_store_to_move.seq_store.clone()),
                start_time: Utc::now(),
            };

            if let Err(e) = meta_store_to_move.details.migrate(table_ref) {
                log::error!(
                    "Error during {} migration: {}",
                    meta_store_to_move.details.get_name(),
                    e
                );
            } else {
                trace!(
                    "Migration for {}: done",
                    meta_store_to_move.details.get_name()
                );
            }
        })
        .await?;

        Ok(())
    }

    pub async fn add_listener(&self, listener: Sender<MetaStoreEvent>) {
        self.listeners.write().await.push(listener);
    }

    #[inline(always)]
    pub async fn write_operation<F, R>(&self, op_name: &'static str, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>, &'a mut BatchPipe) -> Result<R, CubeError>
            + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
    {
        self.write_operation_impl::<F, R>(&self.rw_loop_default_cf, op_name, f)
            .await
    }

    pub async fn write_operation_impl<F, R>(
        &self,
        rw_loop: &RocksStoreRWLoop,
        op_name: &'static str,
        f: F,
    ) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>, &'a mut BatchPipe) -> Result<R, CubeError>
            + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
    {
        let db = self.db.clone();
        let mem_seq = MemorySequence::new(self.seq_store.clone());
        let db_to_send = db.clone();
        let cached_tables = self.cached_tables.clone();

        let loop_name = rw_loop.get_name();
        let store_name = self.details.get_name();
        let span_name = format!("{}({}) write operation: {}", store_name, loop_name, op_name);

        let (tx, rx) = oneshot::channel::<Result<(R, Vec<MetaStoreEvent>), CubeError>>();

        let res = rw_loop
            .schedule(Box::new(move || {
                let db_span = warn_long(&span_name, Duration::from_millis(100));

                let mut batch = BatchPipe::new(db_to_send.as_ref());
                let snapshot = db_to_send.snapshot();
                let res = f(
                    DbTableRef {
                        db: db_to_send.as_ref(),
                        snapshot: &snapshot,
                        mem_seq,
                        start_time: Utc::now(),
                    },
                    &mut batch,
                );
                match res {
                    Ok(res) => {
                        if batch.invalidate_tables_cache {
                            *cached_tables.lock().unwrap() = None;
                        }
                        let write_result = batch.batch_write_rows()?;
                        tx.send(Ok((res, write_result))).map_err(|_| {
                            CubeError::internal(format!(
                                "[{}-{}] Write operation result receiver has been dropped",
                                store_name, loop_name
                            ))
                        })?;
                    }
                    Err(e) => {
                        tx.send(Err(e)).map_err(|_| {
                            CubeError::internal(format!(
                                "[{}-{}] Write operation result receiver has been dropped",
                                store_name, loop_name
                            ))
                        })?;
                    }
                }

                mem::drop(db_span);

                Ok(())
            }))
            .await;
        if let Err(e) = res {
            log::error!(
                "[{}] Error during scheduling write task in loop: {}",
                store_name,
                e
            );

            return Err(CubeError::internal(format!(
                "Error during scheduling write task in loop: {}",
                e
            )));
        }

        let res = rx.await.map_err(|err| {
            CubeError::internal(format!("Unable to receive result for write task: {}", err))
        })?;
        let (spawn_res, events) = res?;

        self.write_notify.notify_waiters();

        if events.len() > 0 {
            for listener in self.listeners.read().await.clone().iter_mut() {
                for event in events.iter() {
                    listener.send(event.clone())?;
                }
            }
        }
        Ok(spawn_res)
    }

    pub async fn run_upload(&self) -> Result<(), CubeError> {
        let time = SystemTime::now();
        trace!("Persisting {}", self.details.get_name());

        let last_check_seq = self.last_check_seq().await;
        let last_db_seq = self.db.latest_sequence_number();
        if last_check_seq == last_db_seq {
            trace!("Persisting {}: nothing to update", self.details.get_name());
            return Ok(());
        }

        if self.details.log_enabled() {
            let last_upload_seq = self.last_upload_seq().await;
            let (serializer, min, max) = {
                let updates = self.db.get_updates_since(last_upload_seq)?;
                let mut serializer = WriteBatchContainer::new();

                let mut seq_numbers = Vec::new();
                let size_limit = self.config.meta_store_log_upload_size_limit() as usize;
                for update in updates.into_iter() {
                    let (n, write_batch) = update?;
                    seq_numbers.push(n);
                    write_batch.iterate(&mut serializer);
                    if serializer.size() > size_limit {
                        break;
                    }
                }

                (
                    serializer,
                    seq_numbers.iter().min().map(|v| *v),
                    seq_numbers.iter().max().map(|v| *v),
                )
            };
            if max.is_some() {
                let snapshot_uploaded = self.snapshot_uploaded.read().await;
                if *snapshot_uploaded {
                    let checkpoint_time = self.last_checkpoint_time.read().await;
                    let dir_name = format!("{}-logs", self.get_store_path(&checkpoint_time));
                    self.metastore_fs
                        .upload_log(&dir_name, min.unwrap(), serializer)
                        .await?;
                }
                let mut seq = self.last_upload_seq.write().await;
                *seq = max.unwrap();
                self.write_completed_notify.notify_waiters();
            }
        } else {
            trace!("Persisting {}: logs are disabled", self.details.get_name());
        }

        let last_checkpoint_time: SystemTime = self.last_checkpoint_time.read().await.clone();
        if last_checkpoint_time
            + time::Duration::from_secs(self.config.meta_store_snapshot_interval())
            < SystemTime::now()
        {
            self.upload_check_point().await?;
        }

        info!(
            "Persisting {} snapshot: done ({:?})",
            self.details.get_name(),
            time.elapsed()?
        );

        Ok(())
    }

    pub fn rocksdb_properties(&self) -> Result<Vec<RocksPropertyRow>, CubeError> {
        let to_collect = [
            rocksdb::properties::BLOCK_CACHE_CAPACITY,
            rocksdb::properties::BLOCK_CACHE_USAGE,
            rocksdb::properties::BLOCK_CACHE_PINNED_USAGE,
            rocksdb::properties::LEVELSTATS,
            &rocksdb::properties::compression_ratio_at_level(0),
            &rocksdb::properties::compression_ratio_at_level(1),
            &rocksdb::properties::compression_ratio_at_level(2),
            &rocksdb::properties::compression_ratio_at_level(3),
            &rocksdb::properties::compression_ratio_at_level(4),
            &rocksdb::properties::compression_ratio_at_level(6),
            rocksdb::properties::DBSTATS,
            // rocksdb::properties::SSTABLES,
            rocksdb::properties::NUM_RUNNING_FLUSHES,
            rocksdb::properties::COMPACTION_PENDING,
            rocksdb::properties::NUM_RUNNING_COMPACTIONS,
            rocksdb::properties::BACKGROUND_ERRORS,
            rocksdb::properties::CUR_SIZE_ACTIVE_MEM_TABLE,
            rocksdb::properties::CUR_SIZE_ALL_MEM_TABLES,
            rocksdb::properties::SIZE_ALL_MEM_TABLES,
            rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE,
            rocksdb::properties::NUM_ENTRIES_IMM_MEM_TABLES,
            rocksdb::properties::NUM_DELETES_ACTIVE_MEM_TABLE,
            rocksdb::properties::NUM_DELETES_IMM_MEM_TABLES,
            rocksdb::properties::ESTIMATE_NUM_KEYS,
            rocksdb::properties::NUM_SNAPSHOTS,
            rocksdb::properties::OLDEST_SNAPSHOT_TIME,
            rocksdb::properties::NUM_LIVE_VERSIONS,
            rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE,
            rocksdb::properties::LIVE_SST_FILES_SIZE,
            rocksdb::properties::ESTIMATE_PENDING_COMPACTION_BYTES,
            rocksdb::properties::ESTIMATE_TABLE_READERS_MEM,
            rocksdb::properties::BASE_LEVEL,
            rocksdb::properties::AGGREGATED_TABLE_PROPERTIES,
        ];

        let mut result = Vec::with_capacity(to_collect.len());

        for property_name in to_collect {
            result.push(RocksPropertyRow {
                key: property_name.to_string_lossy().to_string(),
                value: self.db.property_value(property_name)?,
            })
        }

        Ok(result)
    }

    pub async fn healthcheck(&self) -> Result<(), CubeError> {
        self.read_operation("healthcheck", move |_| {
            // read_operation will call getSnapshot, which is enough to test that RocksDB works
            Ok(())
        })
        .await?;

        let db_path = self.db.path();

        // read/write operation doesnt check fs status
        tokio::fs::metadata(db_path).await.map_err(|err| {
            CubeError::internal(format!(
                "Error while checking database for {}: {}",
                self.details.get_name(),
                err
            ))
        })?;

        for live_file in self.db.live_files()? {
            let file_name = live_file.name.trim_start_matches(std::path::MAIN_SEPARATOR);
            tokio::fs::metadata(db_path.join(file_name).as_path())
                .await
                .map_err(|err| {
                    CubeError::internal(format!(
                        "Error while checking live file \"{}\" for {}: {}",
                        file_name,
                        self.details.get_name(),
                        err
                    ))
                })?;
        }

        Ok(())
    }

    pub async fn upload_check_point(&self) -> Result<(), CubeError> {
        info!("Uploading {} check point", self.details.get_name());
        let upload_stopped = self.snapshots_upload_stopped.lock().await;
        if !*upload_stopped {
            let mut check_point_time = self.last_checkpoint_time.write().await;

            let (remote_path, checkpoint_path) = {
                let _db = self.db.clone();
                *check_point_time = SystemTime::now();
                self.prepare_checkpoint(&check_point_time).await?
            };

            let details = self.details.clone();
            let config = self.config.clone();
            let path_to_move = checkpoint_path.clone();
            let checkpoint_last_seq =
                cube_ext::spawn_blocking(move || -> Result<u64, CubeError> {
                    let snap_db = details.open_readonly_db(&path_to_move, &config)?;
                    Ok(snap_db.latest_sequence_number())
                })
                .await??;

            self.metastore_fs
                .upload_checkpoint(remote_path, checkpoint_path)
                .await?;
            let mut snapshot_uploaded = self.snapshot_uploaded.write().await;
            *snapshot_uploaded = true;
            let mut last_uploaded_check_seq = self.last_check_seq.write().await;
            *last_uploaded_check_seq = checkpoint_last_seq;
            let mut last_uploaded_seq = self.last_upload_seq.write().await;
            *last_uploaded_seq = checkpoint_last_seq;
            self.write_completed_notify.notify_waiters();
        }
        Ok(())
    }

    async fn last_upload_seq(&self) -> u64 {
        *self.last_upload_seq.read().await
    }

    async fn last_check_seq(&self) -> u64 {
        *self.last_check_seq.read().await
    }

    #[cfg(test)]
    pub fn last_seq(&self) -> u64 {
        self.db.latest_sequence_number()
    }

    fn get_store_path(&self, checkpoint_time: &SystemTime) -> String {
        format!(
            "{}-{}",
            self.details.get_name(),
            checkpoint_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        )
    }

    async fn prepare_checkpoint(
        &self,
        checkpoint_time: &SystemTime,
    ) -> Result<(String, PathBuf), CubeError> {
        let remote_path = self.get_store_path(checkpoint_time);
        let checkpoint_path = self.db.path().join("..").join(remote_path.clone());

        let path_to_move = checkpoint_path.clone();
        let db_to_move = self.db.clone();

        cube_ext::spawn_blocking(move || -> Result<(), CubeError> {
            let checkpoint = Checkpoint::new(db_to_move.as_ref())?;
            checkpoint.create_checkpoint(path_to_move.as_path())?;
            Ok(())
        })
        .await??;

        Ok((remote_path, checkpoint_path))
    }

    #[inline(always)]
    pub async fn read_operation<F, R>(&self, op_name: &'static str, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        self.read_operation_impl::<F, R>(&self.rw_loop_default_cf, op_name, f)
            .await
    }

    pub async fn read_operation_impl<F, R>(
        &self,
        rw_loop: &RocksStoreRWLoop,
        op_name: &'static str,
        f: F,
    ) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        let mem_seq = MemorySequence::new(self.seq_store.clone());
        let db_to_send = self.db.clone();
        let (tx, rx) = oneshot::channel::<Result<R, CubeError>>();

        let loop_name = rw_loop.get_name();
        let store_name = self.details.get_name();
        let span_name = format!("{}({}) read operation: {}", store_name, loop_name, op_name);

        let res = rw_loop.schedule(Box::new(move || {
            let db_span = warn_long(&span_name, Duration::from_millis(100));

            let snapshot = db_to_send.snapshot();
            let res = f(DbTableRef {
                db: db_to_send.as_ref(),
                snapshot: &snapshot,
                mem_seq,
                start_time: Utc::now(),
            });

            tx.send(res).map_err(|_| {
                CubeError::internal(format!(
                    "[{}-{}] Read operation result receiver has been dropped",
                    store_name, loop_name
                ))
            })?;

            mem::drop(db_span);

            Ok(())
        }));
        if let Err(e) = res.await {
            log::error!("Error during scheduling read task in loop: {}", e);

            return Err(CubeError::internal(format!(
                "Error during scheduling read task in loop: {}",
                e
            )));
        }

        rx.await.map_err(|err| {
            CubeError::internal(format!("Unable to receive result for read task: {}", err))
        })?
    }

    pub async fn read_operation_out_of_queue_opt<F, R>(
        &self,
        op_name: &'static str,
        f: F,
        timeout: Duration,
    ) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        let mem_seq = MemorySequence::new(self.seq_store.clone());
        let db_to_send = self.db.clone();
        let span_name = format!(
            "{} read operation out of queue: {}",
            self.details.get_name(),
            op_name
        );

        cube_ext::spawn_blocking(move || {
            let db_span = warn_long(&span_name, timeout);
            let span = tracing::trace_span!("store read operation out of queue");
            let span_holder = span.enter();

            let snapshot = db_to_send.snapshot();
            let res = f(DbTableRef {
                db: db_to_send.as_ref(),
                snapshot: &snapshot,
                mem_seq,
                start_time: Utc::now(),
            });

            mem::drop(span_holder);
            mem::drop(db_span);

            res
        })
        .await?
    }

    pub async fn read_operation_out_of_queue<F, R>(
        &self,
        op_name: &'static str,
        f: F,
    ) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        self.read_operation_out_of_queue_opt::<F, R>(op_name, f, Duration::from_millis(100))
            .await
    }

    pub fn cleanup_test_store(test_name: &str) {
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-local", test_name));
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-remote", test_name));
        let _ = std::fs::remove_dir_all(store_path.clone());
        let _ = std::fs::remove_dir_all(remote_store_path.clone());
    }

    pub async fn has_pending_changes(&self) -> Result<bool, CubeError> {
        let db = self.db.clone();
        Ok(db
            .get_updates_since(self.last_upload_seq().await)?
            .next()
            .is_some())
    }

    pub async fn get_snapshots_list(&self) -> Result<Vec<SnapshotInfo>, CubeError> {
        self.metastore_fs.get_snapshots_list().await
    }

    pub async fn set_current_snapshot(&self, snapshot_id: u128) -> Result<(), CubeError> {
        let mut upload_stopped = self.snapshots_upload_stopped.lock().await;

        let snapshots = self.get_snapshots_list().await?;
        let snapshot = snapshots.iter().find(|info| info.id == snapshot_id);
        if snapshot.is_none() {
            return Err(CubeError::user(format!(
                "Metastore snapshot with id {} don't exists",
                snapshot_id
            )));
        }
        let snapshot = snapshot.unwrap();
        if snapshot.current {
            return Err(CubeError::user(format!(
                "Metastore snapshot with id {} is already current snapshot",
                snapshot_id
            )));
        }

        let remote_path = format!("{}-{}", self.details.get_name(), snapshot_id);
        self.metastore_fs
            .write_metastore_current(&remote_path)
            .await?;

        *upload_stopped = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::metastore::rocks_table::RocksTable;
    use crate::metastore::schema::SchemaRocksTable;
    use crate::metastore::Schema;
    use crate::metastore::{BaseRocksStoreFs, RocksMetaStoreDetails};
    use crate::remotefs::LocalDirRemoteFs;
    use chrono::Timelike;
    use std::{env, fs};

    #[test]
    fn test_rocks_secondary_index_encoding_with_ttl() -> Result<(), CubeError> {
        let hash = &[1, 2, 3, 4, 5];
        let expire = Some(
            Utc::now()
                .with_nanosecond(0)
                .expect("Should truncate nanos, because we dont truncate it"),
        );

        let index_v = RocksSecondaryIndexValue::HashAndTTL(hash, expire);
        let index_v_as_bytes = index_v.to_bytes(RocksSecondaryIndexValueVersion::WithTTLSupport)?;

        let index_v_decoded = RocksSecondaryIndexValue::from_bytes(
            &index_v_as_bytes,
            RocksSecondaryIndexValueVersion::WithTTLSupport,
        )?;
        match index_v_decoded {
            RocksSecondaryIndexValue::HashAndTTL(h, ex) => {
                assert_eq!(h, hash, "decoded hash should match encoded");
                assert_eq!(ex, expire, "decoded expire should match encoded");
            }
            other => panic!("Wrong decoded value: {:?}", other),
        };

        Ok(())
    }

    #[test]
    fn test_rocks_secondary_index_encoding_with_ttl_and_info() -> Result<(), CubeError> {
        let hash = &[1, 2, 3, 4, 5];
        let expire = Some(
            Utc::now()
                .with_nanosecond(0)
                .expect("Should truncate nanos, because we dont truncate it"),
        );
        let info = RocksSecondaryIndexValueTTLExtended {
            lfu: 1,
            lru: Some(
                Utc::now()
                    .with_nanosecond(0)
                    .expect("Should truncate nanos, because we dont truncate it"),
            ),
            raw_size: 1024,
        };

        let index_v = RocksSecondaryIndexValue::HashAndTTLExtended(hash, expire, info.clone());
        let index_v_as_bytes = index_v.to_bytes(RocksSecondaryIndexValueVersion::WithTTLSupport)?;

        let index_v_decoded = RocksSecondaryIndexValue::from_bytes(
            &index_v_as_bytes,
            RocksSecondaryIndexValueVersion::WithTTLSupport,
        )?;
        match index_v_decoded {
            RocksSecondaryIndexValue::HashAndTTLExtended(h, ex, inf) => {
                assert_eq!(h, hash, "decoded hash should match encoded");
                assert_eq!(ex, expire, "decoded expire should match encoded");
                assert_eq!(inf, info, "decoded expire should match encoded");
            }
            other => panic!("Wrong decoded value: {:?}", other),
        };

        Ok(())
    }

    #[tokio::test]
    async fn test_loop_panic() -> Result<(), CubeError> {
        let config = Config::test("test_loop_panic");
        let store_path = env::current_dir().unwrap().join("test_loop_panic-local");
        let remote_store_path = env::current_dir().unwrap().join("test_loop_panic-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());

        let details = Arc::new(RocksMetaStoreDetails {});
        let rocks_store = RocksStore::new(
            store_path.join("metastore").as_path(),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
            details,
        )?;

        // read operation
        {
            let r = rocks_store
                .read_operation("unnamed", |_| -> Result<(), CubeError> {
                    panic!("panic - task 1");
                })
                .await;
            assert_eq!(
                r.err().expect("must be error").message,
                "Unable to receive result for read task: channel closed".to_string()
            );

            let r = rocks_store
                .read_operation("unnamed", |_| -> Result<(), CubeError> {
                    Err(CubeError::user("error - task 3".to_string()))
                })
                .await;
            assert_eq!(
                r.err().expect("must be error").message,
                "error - task 3".to_string()
            );
        }

        // write operation
        {
            let r = rocks_store
                .write_operation("unnamed", |_, _| -> Result<(), CubeError> {
                    panic!("panic - task 1");
                })
                .await;
            assert_eq!(
                r.err().expect("must be error").message,
                "Unable to receive result for write task: channel closed".to_string()
            );

            let r = rocks_store
                .write_operation("unnamed", |_, _| -> Result<(), CubeError> {
                    panic!("panic - task 2");
                })
                .await;
            assert_eq!(
                r.err().expect("must be error").message,
                "Unable to receive result for write task: channel closed".to_string()
            );

            let r = rocks_store
                .write_operation("unnamed", |_, _| -> Result<(), CubeError> {
                    Err(CubeError::user("error - task 3".to_string()))
                })
                .await;
            assert_eq!(
                r.err().expect("must be error").message,
                "error - task 3".to_string()
            );
        }

        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());

        Ok(())
    }

    async fn write_test_data(rocks_store: &Arc<RocksStore>, name: String) {
        rocks_store
            .write_operation("write_test_data", move |db_ref, batch_pipe| {
                let table = SchemaRocksTable::new(db_ref.clone());
                let schema = Schema { name };
                Ok(table.insert(schema, batch_pipe)?)
            })
            .await
            .unwrap();
    }

    #[test]
    fn test_row_key_to_iterate_range() -> Result<(), CubeError> {
        {
            let row_key = RowKey::Table(TableId::CacheItems, 0);
            let range = row_key.to_iterate_range();

            assert_eq!(range.start, vec![1, 0, 0, 12, 0]);

            assert_eq!(range.end, vec![1, 0, 0, 12, 1]);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_uploads() -> Result<(), CubeError> {
        let config = Config::test("test_snapshots_uploads").update_config(|mut c| {
            c.meta_store_log_upload_size_limit = 300;
            c
        });
        let store_path = env::current_dir()
            .unwrap()
            .join("test_snapshots_uploads-local");
        let remote_store_path = env::current_dir()
            .unwrap()
            .join("test_snapshots_uploads-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());

        let details = Arc::new(RocksMetaStoreDetails {});

        let rocks_store = RocksStore::new(
            store_path.join("metastore").as_path(),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
            details.clone(),
        )?;

        assert_eq!(rocks_store.last_upload_seq().await, 0);
        assert_eq!(rocks_store.last_check_seq().await, 0);

        write_test_data(&rocks_store, "test".to_string()).await;
        write_test_data(&rocks_store, "test2".to_string()).await;

        rocks_store.upload_check_point().await.unwrap();

        let last_seq = rocks_store.last_seq();

        assert_eq!(rocks_store.last_upload_seq().await, last_seq);
        assert_eq!(rocks_store.last_check_seq().await, last_seq);

        write_test_data(&rocks_store, "test3".to_string()).await;

        rocks_store.run_upload().await.unwrap();

        assert_eq!(
            rocks_store.last_upload_seq().await,
            rocks_store.last_seq() - 1
        );
        assert_eq!(rocks_store.last_check_seq().await, last_seq);

        write_test_data(&rocks_store, "test4".to_string()).await;

        rocks_store.run_upload().await.unwrap();

        assert_eq!(
            rocks_store.last_upload_seq().await,
            rocks_store.last_seq() - 1
        );
        assert_eq!(rocks_store.last_check_seq().await, last_seq);

        let last_upl = rocks_store.last_seq();

        write_test_data(&rocks_store, "a".repeat(150)).await;
        write_test_data(&rocks_store, "b".repeat(150)).await;

        rocks_store.run_upload().await.unwrap();

        assert_eq!(rocks_store.last_upload_seq().await, last_upl + 2); // +1 is seq number write and +1 first insert batch
        assert!(rocks_store.last_upload_seq().await < rocks_store.last_seq() - 1);
        assert_eq!(rocks_store.last_check_seq().await, last_seq);

        rocks_store.run_upload().await.unwrap();
        assert_eq!(
            rocks_store.last_upload_seq().await,
            rocks_store.last_seq() - 1
        );
        assert_eq!(rocks_store.last_check_seq().await, last_seq);

        write_test_data(&rocks_store, "c".repeat(150)).await;
        write_test_data(&rocks_store, "e".repeat(150)).await;

        rocks_store.run_upload().await.unwrap();
        assert_eq!(
            rocks_store.last_upload_seq().await,
            rocks_store.last_seq() - 4
        );
        assert_eq!(rocks_store.last_check_seq().await, last_seq);

        let _ = fs::remove_dir_all(store_path.clone());
        drop(rocks_store);

        let rocks_fs = BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj());
        let path = store_path.join("metastore").to_string_lossy().to_string();
        let rocks_store = rocks_fs
            .load_from_remote(&path, config.config_obj(), details)
            .await
            .unwrap();
        let all_schemas = rocks_store
            .read_operation_out_of_queue("test_snapshot_uplaods", move |db_ref| {
                SchemaRocksTable::new(db_ref).all_rows()
            })
            .await
            .unwrap();
        let expected = vec![
            "test".to_string(),
            "test2".to_string(),
            "test3".to_string(),
            "test4".to_string(),
            "a".repeat(150),
            "b".repeat(150),
            "c".repeat(150),
        ];

        assert_eq!(expected.len(), all_schemas.len());

        for (exp, row) in expected.into_iter().zip(all_schemas.into_iter()) {
            assert_eq!(&exp, row.get_row().get_name());
        }

        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());

        Ok(())
    }
}
