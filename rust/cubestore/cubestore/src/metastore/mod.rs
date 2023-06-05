pub mod chunks;
pub mod index;
pub mod job;
pub mod listener;
pub mod multi_index;
pub mod partition;
pub mod replay_handle;
mod rocks_fs;
mod rocks_store;
mod rocks_table;
pub mod schema;
pub mod snapshot_info;
pub mod source;
pub mod table;
pub mod trace_object;
pub mod wal;

pub use rocks_fs::*;
pub use rocks_store::*;
pub use rocks_table::*;

use crate::cluster::node_name_by_partition;
use async_trait::async_trait;
use log::info;
use rocksdb::{BlockBasedOptions, Cache, Env, MergeOperands, Options, DB};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::{env, io::Cursor, sync::Arc};

use crate::config::injection::DIService;
use crate::config::{Config, ConfigObj};
use crate::metastore::chunks::{ChunkIndexKey, ChunkRocksIndex};
use crate::metastore::index::IndexIndexKey;
use crate::metastore::job::{Job, JobIndexKey, JobRocksIndex, JobRocksTable, JobStatus, JobType};
use crate::metastore::multi_index::{
    MultiIndexIndexKey, MultiPartition, MultiPartitionIndexKey, MultiPartitionRocksIndex,
    MultiPartitionRocksTable,
};
use crate::metastore::partition::PartitionIndexKey;
use crate::metastore::replay_handle::{
    ReplayHandle, ReplayHandleIndexKey, ReplayHandleRocksIndex, ReplayHandleRocksTable, SeqPointer,
};
use crate::metastore::source::{
    Source, SourceCredentials, SourceIndexKey, SourceRocksIndex, SourceRocksTable,
};
use crate::metastore::table::{AggregateColumnIndex, StreamOffset, TableIndexKey, TablePath};
use crate::metastore::trace_object::{
    TraceObject, TraceObjectIndexKey, TraceObjectRocksIndex, TraceObjectRocksTable,
};
use crate::metastore::wal::{WALIndexKey, WALRocksIndex};

use crate::table::{Row, TableValue};

use crate::util::WorkerLoop;
use crate::{meta_store_table_impl, CubeError};
use arrow::datatypes::TimeUnit::Microsecond;
use arrow::datatypes::{DataType, Field};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, Utc};
use chunks::ChunkRocksTable;
use core::fmt;
use cubehll::HllSketch;
use cubezetasketch::HyperLogLogPlusPlus;
use datafusion::cube_ext;
use futures_timer::Delay;
use index::{IndexRocksIndex, IndexRocksTable};
use itertools::Itertools;
use log::trace;
use multi_index::{MultiIndex, MultiIndexRocksIndex, MultiIndexRocksTable};
use parquet::basic::{ConvertedType, Repetition};
use parquet::{basic::Type, schema::types};
use partition::{PartitionRocksIndex, PartitionRocksTable};
use regex::Regex;
use rocksdb::backup::{BackupEngine, BackupEngineOptions};

use schema::{SchemaRocksIndex, SchemaRocksTable};
use smallvec::alloc::fmt::Formatter;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::mem::take;

use std::path::Path;
use std::str::FromStr;

use crate::cachestore::{CacheItem, QueueItem, QueueItemStatus, QueueResult, QueueResultAckEvent};
use crate::remotefs::LocalDirRemoteFs;
use deepsize::DeepSizeOf;
use snapshot_info::SnapshotInfo;
use std::time::{Duration, SystemTime};
use table::Table;
use table::{TableRocksIndex, TableRocksTable};
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use wal::WALRocksTable;

#[macro_export]
macro_rules! format_table_value {
    ($row:expr, $field:ident, $tt:ty) => {
        crate::metastore::DataFrameValue::value(&$row.$field)
    };
}

#[macro_export]
macro_rules! data_frame_from {
    (
        $( #[$struct_attr:meta] )*
        pub struct $name:ident {
            $( $( #[$field_attr:meta] )* $variant:ident : $tt:ty ),+
        }
    ) => {
        $( #[$struct_attr] )*
        pub struct $name {
            $( $( #[$field_attr] )* $variant : $tt ),+
        }

        impl From<Vec<crate::metastore::IdRow<$name>>> for crate::store::DataFrame {
            fn from(rows: Vec<crate::metastore::IdRow<$name>>) -> Self {
                crate::store::DataFrame::new(
                    vec![
                        crate::metastore::Column::new("id".to_string(), crate::metastore::ColumnType::Int, 0),
                        $( crate::metastore::Column::new(std::stringify!($variant).to_string(), crate::metastore::ColumnType::String, 1) ),+
                    ],
                    rows.iter().map(|r|
                        crate::table::Row::new(vec![
                            crate::table::TableValue::Int(r.get_id() as i64),
                            $(
                                crate::table::TableValue::String(crate::format_table_value!(r.get_row(), $variant, $tt))
                            ),+
                        ])
                    ).collect()
                )
            }
        }
    }
}

#[macro_export]
macro_rules! base_rocks_secondary_index {
    ($table: ty, $index: ty) => {
        impl crate::metastore::BaseRocksSecondaryIndex<$table> for $index {
            fn index_value(&self, row: &$table) -> Vec<u8> {
                RocksSecondaryIndex::index_value(self, row)
            }

            fn index_key_by(&self, row: &$table) -> Vec<u8> {
                RocksSecondaryIndex::index_key_by(self, row)
            }

            fn get_id(&self) -> u32 {
                crate::metastore::RocksSecondaryIndex::get_id(self)
            }

            fn value_version(&self) -> u32 {
                crate::metastore::RocksSecondaryIndex::value_version(self)
            }

            fn version(&self) -> u32 {
                crate::metastore::RocksSecondaryIndex::version(self)
            }

            fn is_unique(&self) -> bool {
                crate::metastore::RocksSecondaryIndex::is_unique(self)
            }

            fn is_ttl(&self) -> bool {
                RocksSecondaryIndex::is_ttl(self)
            }

            fn get_expire(&self, row: &$table) -> Option<chrono::DateTime<chrono::Utc>> {
                RocksSecondaryIndex::get_expire(self, row)
            }
        }
    };
}

pub trait DataFrameValue<T> {
    fn value(v: &Self) -> T;
}

impl DataFrameValue<String> for String {
    fn value(v: &Self) -> String {
        v.to_string()
    }
}

impl DataFrameValue<String> for u64 {
    fn value(v: &Self) -> String {
        format!("{}", v)
    }
}

impl DataFrameValue<String> for i64 {
    fn value(v: &Self) -> String {
        format!("{}", v)
    }
}

impl DataFrameValue<String> for bool {
    fn value(v: &Self) -> String {
        format!("{}", v)
    }
}

impl DataFrameValue<String> for Vec<Column> {
    fn value(v: &Self) -> String {
        serde_json::to_string(v).unwrap()
    }
}

impl DataFrameValue<String> for Option<Vec<Column>> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| serde_json::to_string(v).unwrap())
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<String> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|s| s.to_string())
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<Vec<String>> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|s| format!("{:?}", s))
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<DateTime<Utc>> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|s| s.to_string())
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for DateTime<Utc> {
    fn value(v: &Self) -> String {
        v.to_string()
    }
}

impl DataFrameValue<String> for Option<ImportFormat> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| format!("{:?}", v))
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<Vec<Option<SeqPointer>>> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| format!("{:?}", v))
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for IndexType {
    fn value(v: &Self) -> String {
        format!("{:?}", v)
    }
}

impl DataFrameValue<String> for QueueItemStatus {
    fn value(v: &Self) -> String {
        format!("{:?}", v)
    }
}

impl DataFrameValue<String> for Option<u64> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| format!("{:?}", v))
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<i64> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| format!("{:?}", v))
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<Vec<u64>> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| format!("{:?}", v))
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<Row> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| {
                format!(
                    "({})",
                    v.values()
                        .iter()
                        .map(|tv| match tv {
                            TableValue::Null => "NULL".to_string(),
                            TableValue::String(s) => format!("\"{}\"", s),
                            TableValue::Int(i) => i.to_string(),
                            TableValue::Timestamp(t) => format!("{:?}", t),
                            TableValue::Bytes(b) => format!("{:?}", b),
                            TableValue::Boolean(b) => format!("{:?}", b),
                            TableValue::Decimal(v) => format!("{}", v.raw_value()),
                            TableValue::Float(v) => format!("{}", v),
                        })
                        .join(", ")
                )
            })
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<Vec<AggregateFunction>> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| v.iter().map(|v| format!("{}", v)).join(", "))
            .unwrap_or("NULL".to_string())
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub enum HllFlavour {
    Airlift,    // Compatible with Presto, Athena, etc.
    Snowflake,  // Same storage as Airlift, imports from Snowflake JSON.
    Postgres,   // Same storage as Airlift, imports from HLL Storage Specification.
    ZetaSketch, // Compatible with BigQuery.
}

pub fn is_valid_plain_binary_hll(data: &[u8], f: HllFlavour) -> Result<(), CubeError> {
    // TODO: do no memory allocations for better performance, this is run on hot path.
    match f {
        HllFlavour::Airlift => {
            HllSketch::read(data)?;
        }
        HllFlavour::ZetaSketch => {
            HyperLogLogPlusPlus::read(data)?;
        }
        HllFlavour::Postgres | HllFlavour::Snowflake => {
            panic!("string formats should be handled separately")
        }
    }
    return Ok(());
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub enum ColumnType {
    String,
    Int,
    Bytes,
    HyperLogLog(HllFlavour), // HLL Sketches, compatible with presto.
    Timestamp,
    Decimal { scale: i32, precision: i32 },
    Float,
    Boolean,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let s = match self {
            ColumnType::Decimal { scale, .. } => return write!(f, "decimal({})", scale),
            ColumnType::String => "text",
            ColumnType::Int => "int",
            ColumnType::Bytes => "bytes",
            ColumnType::HyperLogLog(HllFlavour::Airlift) => "hyperloglog",
            ColumnType::HyperLogLog(HllFlavour::ZetaSketch) => "hyperloglogpp",
            ColumnType::HyperLogLog(HllFlavour::Postgres) => "hll_postgres",
            ColumnType::HyperLogLog(HllFlavour::Snowflake) => "hll_snowflake",
            ColumnType::Timestamp => "timestamp",
            ColumnType::Float => "float",
            ColumnType::Boolean => "boolean",
        };
        f.write_str(s)
    }
}

impl ColumnType {
    pub fn from_string(s: &str) -> Result<ColumnType, CubeError> {
        lazy_static! {
            static ref DECIMAL_RE: Regex = Regex::new(r"decimal\((?P<scale>\d+)\)").unwrap();
        }
        if let Some(captures) = DECIMAL_RE.captures(s) {
            let scale = captures
                .name("scale")
                .ok_or(CubeError::internal("missing scale capture".to_string()))?
                .as_str()
                .parse::<i32>()?;
            Ok(ColumnType::Decimal {
                scale,
                precision: 0,
            })
        } else {
            match s {
                "text" => Ok(ColumnType::String),
                "int" => Ok(ColumnType::Int),
                "bigint" => Ok(ColumnType::Int),
                "bytes" => Ok(ColumnType::Bytes),
                "hyperloglog" => Ok(ColumnType::HyperLogLog(HllFlavour::Airlift)),
                "hyperloglogpp" => Ok(ColumnType::HyperLogLog(HllFlavour::ZetaSketch)),
                "hll_postgres" => Ok(ColumnType::HyperLogLog(HllFlavour::Postgres)),
                "hll_snowflake" => Ok(ColumnType::HyperLogLog(HllFlavour::Snowflake)),
                "timestamp" => Ok(ColumnType::Timestamp),
                "float" => Ok(ColumnType::Float),
                "boolean" => Ok(ColumnType::Boolean),
                _ => {
                    return Err(CubeError::user(format!(
                        "Column type '{}' is not supported",
                        s
                    )))
                }
            }
        }
    }

    pub fn target_scale(&self) -> i32 {
        match self {
            ColumnType::Decimal { scale, .. } => {
                if *scale > 5 {
                    10
                } else {
                    *scale
                }
            }
            x => panic!("target_scale called on {:?}", x),
        }
    }
}

impl From<&Column> for parquet::schema::types::Type {
    fn from(column: &Column) -> Self {
        match column.get_column_type() {
            ColumnType::String => {
                types::Type::primitive_type_builder(&column.get_name(), Type::BYTE_ARRAY)
                    .with_converted_type(ConvertedType::UTF8)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            ColumnType::Int => types::Type::primitive_type_builder(&column.get_name(), Type::INT64)
                .with_converted_type(ConvertedType::INT_64)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .unwrap(),
            ColumnType::Decimal { precision, .. } => {
                types::Type::primitive_type_builder(&column.get_name(), Type::INT64)
                    .with_converted_type(ConvertedType::DECIMAL)
                    .with_precision(*precision)
                    .with_scale(column.get_column_type().target_scale())
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            ColumnType::Bytes | ColumnType::HyperLogLog(_) => {
                types::Type::primitive_type_builder(&column.get_name(), Type::BYTE_ARRAY)
                    .with_converted_type(ConvertedType::NONE)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            ColumnType::Timestamp => {
                types::Type::primitive_type_builder(&column.get_name(), Type::INT64)
                    //TODO MICROS?
                    .with_converted_type(ConvertedType::TIMESTAMP_MICROS)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            ColumnType::Boolean => {
                types::Type::primitive_type_builder(&column.get_name(), Type::BOOLEAN)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            ColumnType::Float => {
                types::Type::primitive_type_builder(&column.get_name(), Type::DOUBLE)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub struct Column {
    name: String,
    column_type: ColumnType,
    column_index: usize,
}

impl Into<Field> for Column {
    fn into(self) -> Field {
        (&self).into()
    }
}

impl<'a> Into<Field> for &'a Column {
    fn into(self) -> Field {
        Field::new(
            self.name.as_str(),
            match self.column_type {
                ColumnType::String => DataType::Utf8,
                ColumnType::Int => DataType::Int64,
                ColumnType::Timestamp => DataType::Timestamp(Microsecond, None),
                ColumnType::Boolean => DataType::Boolean,
                ColumnType::Decimal { .. } => {
                    DataType::Int64Decimal(self.column_type.target_scale() as usize)
                }
                ColumnType::Bytes => DataType::Binary,
                ColumnType::HyperLogLog(_) => DataType::Binary,
                ColumnType::Float => DataType::Float64,
            },
            true,
        )
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let column_type = match &self.column_type {
            ColumnType::String => "STRING".to_string(),
            ColumnType::Int => "INT".to_string(),
            ColumnType::Timestamp => "TIMESTAMP".to_string(),
            ColumnType::Boolean => "BOOLEAN".to_string(),
            ColumnType::Decimal { scale, precision } => {
                format!("DECIMAL({}, {})", precision, scale)
            }
            ColumnType::Bytes => "BYTES".to_string(),
            ColumnType::HyperLogLog(HllFlavour::Airlift) => "HYPERLOGLOG".to_string(),
            ColumnType::HyperLogLog(HllFlavour::ZetaSketch) => "HYPERLOGLOGPP".to_string(),
            ColumnType::HyperLogLog(HllFlavour::Postgres) => "HLL_POSTGRES".to_string(),
            ColumnType::HyperLogLog(HllFlavour::Snowflake) => "HLL_SNOWFLAKE".to_string(),
            ColumnType::Float => "FLOAT".to_string(),
        };
        f.write_fmt(format_args!("{} {}", self.name, column_type))
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum ImportFormat {
    CSV,
    CSVNoHeader,
    CSVOptions {
        delimiter: Option<char>,
        escape: Option<char>,
        quote: Option<char>,
        has_header: bool,
    },
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Schema {
    name: String
}
}

impl RocksEntity for Schema {}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum IndexType {
    Regular = 1,
    Aggregate = 2,
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Index {
    name: String,
    table_id: u64,
    columns: Vec<Column>,
    sort_key_size: u64,
    #[serde(default)]
    partition_split_key_size: Option<u64>,
    #[serde(default)]
    multi_index_id: Option<u64>,
    #[serde(default = "Index::index_type_default")]
    index_type: IndexType
}
}

impl RocksEntity for Index {}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum AggregateFunction {
    SUM = 1,
    MAX = 2,
    MIN = 3,
    MERGE = 4,
}

impl FromStr for AggregateFunction {
    type Err = CubeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_ref() {
            "SUM" => Ok(AggregateFunction::SUM),
            "MAX" => Ok(AggregateFunction::MAX),
            "MIN" => Ok(AggregateFunction::MIN),
            "MERGE" => Ok(AggregateFunction::MERGE),
            _ => Err(CubeError::user(format!(
                "Function {} can't be used in aggregate index",
                s
            ))),
        }
    }
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let res = match self {
            Self::SUM => "SUM",
            Self::MAX => "MAX",
            Self::MIN => "MIN",
            Self::MERGE => "MERGE",
        };

        f.write_fmt(format_args!("{}", res))
    }
}

impl AggregateFunction {
    pub fn allowed_for_type(&self, col_type: &ColumnType) -> bool {
        match self {
            Self::MAX | Self::MIN => match col_type {
                ColumnType::HyperLogLog(_) => false,
                _ => true,
            },
            Self::SUM => match col_type {
                ColumnType::Int | ColumnType::Decimal { .. } | ColumnType::Float => true,
                _ => false,
            },
            Self::MERGE => match col_type {
                ColumnType::HyperLogLog(_) => true,
                ColumnType::Bytes => true,
                _ => false,
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    pub multi_index: Option<String>,
    pub index_type: IndexType,
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Partition {
    index_id: u64,
    parent_partition_id: Option<u64>,
    #[serde(default)]
    multi_partition_id: Option<u64>,
    min_value: Option<Row>,
    max_value: Option<Row>,
    active: bool,
    #[serde(default)]
    warmed_up: bool,
    main_table_row_count: u64,
    /// Not used or updated anymore.
    #[serde(default)]
    last_used: Option<DateTime<Utc>>,
    #[serde(default)]
    suffix: Option<String>,
    #[serde(default)]
    file_size: Option<u64>,
    #[serde(default)]
    min: Option<Row>,
    #[serde(default)]
    max: Option<Row>
}
}

impl RocksEntity for Partition {}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Chunk {
    partition_id: u64,
    row_count: u64,
    uploaded: bool,
    active: bool,
    /// Not used or updated anymore.
    #[serde(default)]
    last_used: Option<DateTime<Utc>>,
    #[serde(default)]
    in_memory: bool,
    #[serde(default)]
    created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    oldest_insert_at: Option<DateTime<Utc>>,
    #[serde(default)]
    deactivated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    suffix: Option<String>,
    #[serde(default)]
    file_size: Option<u64>,
    #[serde(default)]
    replay_handle_id: Option<u64>,
    min: Option<Row>,
    #[serde(default)]
    max: Option<Row>
}
}

impl RocksEntity for Chunk {}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct WAL {
    table_id: u64,
    row_count: u64,
    uploaded: bool,
}

impl RocksEntity for WAL {}

meta_store_table_impl!(SchemaMetaStoreTable, Schema, SchemaRocksTable);
meta_store_table_impl!(ChunkMetaStoreTable, Chunk, ChunkRocksTable);
meta_store_table_impl!(IndexMetaStoreTable, Index, IndexRocksTable);
meta_store_table_impl!(PartitionMetaStoreTable, Partition, PartitionRocksTable);
meta_store_table_impl!(TableMetaStoreTable, Table, TableRocksTable);

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionData {
    pub partition: IdRow<Partition>,
    pub index: IdRow<Index>,
    pub chunks: Vec<IdRow<Chunk>>,
}

#[cuberpc::service]
pub trait MetaStore: DIService + Send + Sync {
    async fn wait_for_current_seq_to_sync(&self) -> Result<(), CubeError>;
    fn schemas_table(&self) -> SchemaMetaStoreTable;
    async fn create_schema(
        &self,
        schema_name: String,
        if_not_exists: bool,
    ) -> Result<IdRow<Schema>, CubeError>;
    async fn get_schemas(&self) -> Result<Vec<IdRow<Schema>>, CubeError>;
    async fn get_schema_by_id(&self, schema_id: u64) -> Result<IdRow<Schema>, CubeError>;
    //TODO Option
    async fn get_schema_id(&self, schema_name: String) -> Result<u64, CubeError>;
    //TODO Option
    async fn get_schema(&self, schema_name: String) -> Result<IdRow<Schema>, CubeError>;
    async fn rename_schema(
        &self,
        old_schema_name: String,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError>;
    async fn rename_schema_by_id(
        &self,
        schema_id: u64,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError>;
    async fn delete_schema(&self, schema_name: String) -> Result<(), CubeError>;
    async fn delete_schema_by_id(&self, schema_id: u64) -> Result<(), CubeError>;

    fn tables_table(&self) -> TableMetaStoreTable;
    async fn create_table(
        &self,
        schema_name: String,
        table_name: String,
        columns: Vec<Column>,
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        indexes: Vec<IndexDef>,
        is_ready: bool,
        build_range_end: Option<DateTime<Utc>>,
        seal_at: Option<DateTime<Utc>>,
        select_statement: Option<String>,
        source_coulumns: Option<Vec<Column>>,
        stream_offset: Option<StreamOffset>,
        unique_key_column_names: Option<Vec<String>>,
        aggregates: Option<Vec<(String, String)>>,
        partition_split_threshold: Option<u64>,
        trace_obj: Option<String>,
    ) -> Result<IdRow<Table>, CubeError>;
    async fn table_ready(&self, id: u64, is_ready: bool) -> Result<IdRow<Table>, CubeError>;
    async fn seal_table(&self, id: u64) -> Result<IdRow<Table>, CubeError>;
    async fn get_trace_obj_by_table_id(&self, table_id: u64) -> Result<Option<String>, CubeError>;
    async fn update_location_download_size(
        &self,
        id: u64,
        location: String,
        download_size: u64,
    ) -> Result<IdRow<Table>, CubeError>;
    async fn get_table(
        &self,
        schema_name: String,
        table_name: String,
    ) -> Result<IdRow<Table>, CubeError>;
    async fn get_table_by_id(&self, table_id: u64) -> Result<IdRow<Table>, CubeError>;
    async fn get_tables(&self) -> Result<Vec<IdRow<Table>>, CubeError>;
    async fn get_tables_with_path(
        &self,
        include_non_ready: bool,
    ) -> Result<Arc<Vec<TablePath>>, CubeError>;
    async fn not_ready_tables(
        &self,
        created_seconds_ago: i64,
    ) -> Result<Vec<IdRow<Table>>, CubeError>;
    async fn drop_table(&self, table_id: u64) -> Result<IdRow<Table>, CubeError>;

    fn partition_table(&self) -> PartitionMetaStoreTable;
    async fn create_partition(&self, partition: Partition) -> Result<IdRow<Partition>, CubeError>;
    async fn get_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError>;
    async fn get_partition_out_of_queue(
        &self,
        partition_id: u64,
    ) -> Result<IdRow<Partition>, CubeError>;
    async fn get_partition_for_compaction(
        &self,
        partition_id: u64,
    ) -> Result<
        (
            IdRow<Partition>,
            IdRow<Index>,
            IdRow<Table>,
            Option<IdRow<MultiPartition>>,
        ),
        CubeError,
    >;
    async fn get_partition_chunk_sizes(&self, partition_id: u64) -> Result<u64, CubeError>;
    /// Swaps chunks inside the same partition.
    /// The operation will not be commited if partition_id is inside a multi-partition that started
    /// a multi-split. Returns true iff the operation is committed.
    async fn swap_compacted_chunks(
        &self,
        partition_id: u64,
        old_chunk_ids: Vec<u64>,
        new_chunk: u64,
        new_chunk_file_size: u64,
    ) -> Result<bool, CubeError>;
    async fn swap_active_partitions(
        &self,
        current_active: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
        new_active: Vec<(IdRow<Partition>, u64)>,
        new_active_min_max: Vec<(u64, (Option<Row>, Option<Row>), (Option<Row>, Option<Row>))>,
    ) -> Result<(), CubeError>;
    async fn delete_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError>;
    async fn mark_partition_warmed_up(&self, partition_id: u64) -> Result<(), CubeError>;
    async fn delete_middle_man_partition(
        &self,
        partition_id: u64,
    ) -> Result<IdRow<Partition>, CubeError>;
    async fn can_delete_partition(&self, partition_id: u64) -> Result<bool, CubeError>;
    async fn can_delete_middle_man_partition(&self, partition_id: u64) -> Result<bool, CubeError>;
    async fn all_inactive_partitions_to_repartition(
        &self,
    ) -> Result<Vec<IdRow<Partition>>, CubeError>;
    async fn all_inactive_middle_man_partitions(&self) -> Result<Vec<IdRow<Partition>>, CubeError>;
    async fn all_just_created_partitions(&self) -> Result<Vec<IdRow<Partition>>, CubeError>;
    async fn get_partitions_with_chunks_created_seconds_ago(
        &self,
        seconds_ago: i64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError>;
    async fn get_partitions_for_in_memory_compaction(
        &self,
        node: String,
    ) -> Result<
        Vec<(
            IdRow<Partition>,
            IdRow<Index>,
            IdRow<Table>,
            Vec<IdRow<Chunk>>,
        )>,
        CubeError,
    >;
    async fn get_all_node_in_memory_chunks(
        &self,
        node: String,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError>;
    async fn get_chunks_without_partition_created_seconds_ago(
        &self,
        seconds_ago: i64,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError>;

    fn index_table(&self) -> IndexMetaStoreTable;
    async fn create_index(
        &self,
        schema_name: String,
        table_name: String,
        index_def: IndexDef,
    ) -> Result<IdRow<Index>, CubeError>;
    async fn get_default_index(&self, table_id: u64) -> Result<IdRow<Index>, CubeError>;
    async fn get_table_indexes(&self, table_id: u64) -> Result<Vec<IdRow<Index>>, CubeError>;
    async fn get_table_indexes_out_of_queue(
        &self,
        table_id: u64,
    ) -> Result<Vec<IdRow<Index>>, CubeError>;
    async fn get_active_partitions_by_index_id(
        &self,
        index_id: u64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError>;
    async fn get_index(&self, index_id: u64) -> Result<IdRow<Index>, CubeError>;

    async fn get_index_with_active_partitions_out_of_queue(
        &self,
        index_id: u64,
    ) -> Result<(IdRow<Index>, Vec<IdRow<Partition>>), CubeError>;

    async fn create_partitioned_index(
        &self,
        schema: String,
        name: String,
        columns: Vec<Column>,
        if_not_exists: bool,
    ) -> Result<IdRow<MultiIndex>, CubeError>;
    async fn drop_partitioned_index(&self, schema: String, name: String) -> Result<(), CubeError>;
    async fn get_multi_partition(&self, id: u64) -> Result<IdRow<MultiPartition>, CubeError>;
    async fn get_child_multi_partitions(
        &self,
        id: u64,
    ) -> Result<Vec<IdRow<MultiPartition>>, CubeError>;
    /// Retrieve a partial subtrees that contain common parents for all [multi_part_ids]. We
    /// guarantee that all nodes on the paths to common parents are in the results. No attempt is
    /// made to retrieve extra children, however.
    async fn get_multi_partition_subtree(
        &self,
        multi_part_ids: Vec<u64>,
    ) -> Result<HashMap<u64, MultiPartition>, CubeError>;
    async fn create_multi_partition(
        &self,
        p: MultiPartition,
    ) -> Result<IdRow<MultiPartition>, CubeError>;
    async fn prepare_multi_partition_for_split(
        &self,
        multi_partition_id: u64,
    ) -> Result<(IdRow<MultiIndex>, IdRow<MultiPartition>, Vec<PartitionData>), CubeError>;
    async fn commit_multi_partition_split(
        &self,
        multi_partition_id: u64,
        new_multi_partitions: Vec<u64>,
        new_multi_partition_rows: Vec<u64>,
        old_partitions: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
        new_partitions: Vec<(IdRow<Partition>, u64)>,
        new_partition_rows: Vec<u64>,
        initial_split: bool,
    ) -> Result<(), CubeError>;
    async fn find_unsplit_partitions(&self, multi_partition_id: u64)
        -> Result<Vec<u64>, CubeError>;
    async fn prepare_multi_split_finish(
        &self,
        multi_partition_id: u64,
        partition_id: u64,
    ) -> Result<(PartitionData, Vec<IdRow<MultiPartition>>), CubeError>;

    async fn get_active_partitions_and_chunks_by_index_id_for_select(
        &self,
        index_id: Vec<u64>,
    ) -> Result<Vec<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>>, CubeError>;

    async fn get_warmup_partitions(
        &self,
    ) -> Result<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>, CubeError>;

    async fn get_all_filenames(&self) -> Result<Vec<String>, CubeError>;

    fn chunks_table(&self) -> ChunkMetaStoreTable;
    async fn create_chunk(
        &self,
        partition_id: u64,
        row_count: usize,
        min: Option<Row>,
        max: Option<Row>,
        in_memory: bool,
    ) -> Result<IdRow<Chunk>, CubeError>;
    async fn insert_chunks(&self, chunks: Vec<Chunk>) -> Result<Vec<IdRow<Chunk>>, CubeError>;
    async fn get_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError>;
    async fn get_chunks_out_of_queue(&self, ids: Vec<u64>) -> Result<Vec<IdRow<Chunk>>, CubeError>;
    async fn get_partitions_out_of_queue(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<IdRow<Partition>>, CubeError>;
    async fn get_chunks_by_partition(
        &self,
        partition_id: u64,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError>;
    async fn get_used_disk_space_out_of_queue(
        &self,
        node: Option<String>,
    ) -> Result<u64, CubeError>;
    async fn get_all_partitions_and_chunks_out_of_queue(
        &self,
    ) -> Result<(Vec<IdRow<Partition>>, Vec<IdRow<Chunk>>), CubeError>;
    async fn get_chunks_by_partition_out_of_queue(
        &self,
        partition_id: u64,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError>;
    async fn chunk_uploaded(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError>;
    async fn chunk_update_last_inserted(
        &self,
        chunk_ids: Vec<u64>,
        last_inserted_at: Option<DateTime<Utc>>,
    ) -> Result<(), CubeError>;
    async fn deactivate_chunk(&self, chunk_id: u64) -> Result<(), CubeError>;
    async fn deactivate_chunks(&self, chunk_ids: Vec<u64>) -> Result<(), CubeError>;
    async fn swap_chunks(
        &self,
        deactivate_ids: Vec<u64>,
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError>;
    async fn swap_chunks_without_check(
        &self,
        deactivate_ids: Vec<u64>,
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError>;
    async fn deactivate_chunks_without_check(
        &self,
        deactivate_ids: Vec<u64>,
    ) -> Result<(), CubeError>;
    async fn activate_chunks(
        &self,
        table_id: u64,
        uploaded_chunk_ids: Vec<(u64, Option<u64>)>,
        replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError>;
    async fn delete_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError>;
    async fn delete_chunks_without_checks(&self, chunk_ids: Vec<u64>) -> Result<(), CubeError>;
    async fn all_inactive_chunks(&self) -> Result<Vec<IdRow<Chunk>>, CubeError>;
    async fn all_inactive_not_uploaded_chunks(&self) -> Result<Vec<IdRow<Chunk>>, CubeError>;

    async fn create_wal(&self, table_id: u64, row_count: usize) -> Result<IdRow<WAL>, CubeError>;
    async fn get_wal(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError>;
    async fn delete_wal(&self, wal_id: u64) -> Result<(), CubeError>;
    async fn wal_uploaded(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError>;
    async fn get_wals_for_table(&self, table_id: u64) -> Result<Vec<IdRow<WAL>>, CubeError>;

    async fn all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError>;
    async fn add_job(&self, job: Job) -> Result<Option<IdRow<Job>>, CubeError>;
    async fn get_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError>;
    async fn get_job_by_ref(
        &self,
        row_reference: RowKey,
        job_type: JobType,
    ) -> Result<Option<IdRow<Job>>, CubeError>;
    async fn get_orphaned_jobs(
        &self,
        orphaned_timeout: Duration,
    ) -> Result<Vec<IdRow<Job>>, CubeError>;
    async fn get_jobs_on_non_exists_nodes(&self) -> Result<Vec<IdRow<Job>>, CubeError>;
    async fn delete_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError>;
    async fn start_processing_job(
        &self,
        server_name: String,
        long_term: bool,
    ) -> Result<Option<IdRow<Job>>, CubeError>;
    async fn update_status(&self, job_id: u64, status: JobStatus) -> Result<IdRow<Job>, CubeError>;
    async fn update_heart_beat(&self, job_id: u64) -> Result<IdRow<Job>, CubeError>;
    async fn delete_all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError>;

    async fn create_or_update_source(
        &self,
        name: String,
        credentials: SourceCredentials,
    ) -> Result<IdRow<Source>, CubeError>;
    async fn get_source(&self, id: u64) -> Result<IdRow<Source>, CubeError>;
    async fn get_source_by_name(&self, name: String) -> Result<IdRow<Source>, CubeError>;
    async fn delete_source(&self, id: u64) -> Result<IdRow<Source>, CubeError>;

    async fn create_replay_handle(
        &self,
        table_id: u64,
        location_index: usize,
        seq_pointer: SeqPointer,
    ) -> Result<IdRow<ReplayHandle>, CubeError>;
    async fn create_replay_handle_from_seq_pointers(
        &self,
        table_id: u64,
        seq_pointer: Option<Vec<Option<SeqPointer>>>,
    ) -> Result<IdRow<ReplayHandle>, CubeError>;
    async fn get_replay_handles_by_table(
        &self,
        table_id: u64,
    ) -> Result<Vec<IdRow<ReplayHandle>>, CubeError>;
    async fn get_replay_handles_by_ids(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<IdRow<ReplayHandle>>, CubeError>;
    async fn all_replay_handles(&self) -> Result<Vec<IdRow<ReplayHandle>>, CubeError>;
    /// Returns `(handle, has_active_chunks)` tuple for all replay handles.
    async fn all_replay_handles_to_merge(
        &self,
    ) -> Result<Vec<(IdRow<ReplayHandle>, bool)>, CubeError>;
    async fn update_replay_handle_failed_if_exists(
        &self,
        id: u64,
        failed: bool,
    ) -> Result<(), CubeError>;
    async fn replace_replay_handles(
        &self,
        old_ids: Vec<u64>,
        new_seq_pointer: Option<Vec<Option<SeqPointer>>>,
    ) -> Result<Option<IdRow<ReplayHandle>>, CubeError>;

    async fn get_tables_with_indexes(
        &self,
        table_name: Vec<(String, String)>,
    ) -> Result<Vec<(IdRow<Schema>, IdRow<Table>, Vec<IdRow<Index>>)>, CubeError>;

    async fn debug_dump(&self, out_path: String) -> Result<(), CubeError>;
    // Force compaction for the whole RocksDB
    async fn compaction(&self) -> Result<(), CubeError>;
    async fn healthcheck(&self) -> Result<(), CubeError>;

    async fn get_snapshots_list(&self) -> Result<Vec<SnapshotInfo>, CubeError>;
    async fn set_current_snapshot(&self, snapshot_id: u128) -> Result<(), CubeError>;
}

crate::di_service!(RocksMetaStore, [MetaStore]);
crate::di_service!(MetaStoreRpcClient, [MetaStore]);

#[derive(Clone, Debug)]
pub enum MetaStoreEvent {
    Insert(TableId, u64),
    Update(TableId, u64),
    Delete(TableId, u64),

    UpdateChunk(IdRow<Chunk>, IdRow<Chunk>),
    UpdateIndex(IdRow<Index>, IdRow<Index>),
    UpdateJob(IdRow<Job>, IdRow<Job>),
    UpdatePartition(IdRow<Partition>, IdRow<Partition>),
    UpdateSchema(IdRow<Schema>, IdRow<Schema>),
    UpdateTable(IdRow<Table>, IdRow<Table>),
    UpdateWAL(IdRow<WAL>, IdRow<WAL>),
    UpdateSource(IdRow<Source>, IdRow<Source>),
    UpdateReplayHandle(IdRow<ReplayHandle>, IdRow<ReplayHandle>),
    UpdateTraceObject(IdRow<TraceObject>, IdRow<TraceObject>),

    DeleteChunk(IdRow<Chunk>),
    DeleteIndex(IdRow<Index>),
    DeleteJob(IdRow<Job>),
    DeletePartition(IdRow<Partition>),
    DeleteSchema(IdRow<Schema>),
    DeleteTable(IdRow<Table>),
    DeleteWAL(IdRow<WAL>),
    DeleteSource(IdRow<Source>),
    DeleteReplayHandle(IdRow<ReplayHandle>),
    DeleteTraceObject(IdRow<TraceObject>),

    UpdateMultiIndex(IdRow<MultiIndex>, IdRow<MultiIndex>),
    DeleteMultiIndex(IdRow<MultiIndex>),

    UpdateMultiPartition(IdRow<MultiPartition>, IdRow<MultiPartition>),
    DeleteMultiPartition(IdRow<MultiPartition>),

    // TODO: Split to CacheStoreEvent
    UpdateCacheItem(IdRow<CacheItem>, IdRow<CacheItem>),
    DeleteCacheItem(IdRow<CacheItem>),

    UpdateQueueItem(IdRow<QueueItem>, IdRow<QueueItem>),
    DeleteQueueItem(IdRow<QueueItem>),
    AckQueueItem(QueueResultAckEvent),

    UpdateQueueResult(IdRow<QueueResult>, IdRow<QueueResult>),
    DeleteQueueResult(IdRow<QueueResult>),
}

fn meta_store_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(8);
    let mut counter = existing_val
        .map(|v| Cursor::new(v).read_u64::<BigEndian>().unwrap())
        .unwrap_or(0);

    for op in operands {
        counter += Cursor::new(op).read_u64::<BigEndian>().unwrap()
    }

    result.write_u64::<BigEndian>(counter).unwrap();
    Some(result)
}

struct RocksMetaStoreDetails {}

impl RocksStoreDetails for RocksMetaStoreDetails {
    fn open_db(&self, path: &Path, config: &Arc<dyn ConfigObj>) -> Result<DB, CubeError> {
        let rocksdb_config = config.metastore_rocksdb_config();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(13));
        opts.set_merge_operator_associative("meta_store merge", meta_store_merge);
        // TODO(ovr): Decrease after additional fix for get_updates_since
        opts.set_wal_ttl_seconds(
            config.meta_store_snapshot_interval() + config.meta_store_log_upload_interval(),
        );

        let block_opts = {
            let mut block_opts = BlockBasedOptions::default();
            // https://github.com/facebook/rocksdb/blob/v7.9.2/include/rocksdb/table.h#L524
            block_opts.set_format_version(5);
            block_opts.set_checksum_type(rocksdb_config.checksum_type.as_rocksdb_enum());

            let cache = Cache::new_lru_cache(rocksdb_config.cache_capacity)?;
            block_opts.set_block_cache(&cache);

            block_opts
        };

        opts.set_block_based_table_factory(&block_opts);

        DB::open(&opts, path)
            .map_err(|err| CubeError::internal(format!("DB::open error for metastore: {}", err)))
    }

    fn migrate(&self, table_ref: DbTableRef) -> Result<(), CubeError> {
        SchemaRocksTable::new(table_ref.clone()).migrate()?;
        TableRocksTable::new(table_ref.clone()).migrate()?;
        IndexRocksTable::new(table_ref.clone()).migrate()?;
        PartitionRocksTable::new(table_ref.clone()).migrate()?;
        ChunkRocksTable::new(table_ref.clone()).migrate()?;
        WALRocksTable::new(table_ref.clone()).migrate()?;
        JobRocksTable::new(table_ref.clone()).migrate()?;
        SourceRocksTable::new(table_ref.clone()).migrate()?;
        MultiIndexRocksTable::new(table_ref.clone()).migrate()?;
        MultiPartitionRocksTable::new(table_ref.clone()).migrate()?;

        Ok(())
    }

    fn get_name(&self) -> &'static str {
        &"metastore"
    }
}

pub struct RocksMetaStore {
    store: Arc<RocksStore>,
    disk_space_cache: Arc<RwLock<Option<(HashMap<String, u64>, SystemTime)>>>,
    upload_loop: Arc<WorkerLoop>,
}

impl RocksMetaStore {
    pub fn new(
        path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        Ok(Self::new_from_store(RocksStore::with_listener(
            path,
            vec![],
            metastore_fs,
            config,
            Arc::new(RocksMetaStoreDetails {}),
        )?))
    }

    fn new_from_store(store: Arc<RocksStore>) -> Arc<Self> {
        Arc::new(Self {
            store,
            disk_space_cache: Arc::new(RwLock::new(None)),
            upload_loop: Arc::new(WorkerLoop::new("Metastore upload")),
        })
    }

    pub async fn load_from_dump(
        path: &Path,
        dump_path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let store = RocksStore::load_from_dump(
            path,
            dump_path,
            metastore_fs,
            config,
            Arc::new(RocksMetaStoreDetails {}),
        )
        .await?;

        Ok(Self::new_from_store(store))
    }

    pub async fn load_from_remote(
        path: &str,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let store = metastore_fs
            .load_from_remote(&path, config, Arc::new(RocksMetaStoreDetails {}))
            .await?;

        Ok(Self::new_from_store(store))
    }

    pub async fn wait_upload_loop(self: Arc<Self>) {
        if !self.store.config.upload_to_remote() {
            log::info!("Not running metastore upload loop");
            return;
        }

        let upload_interval = self.store.config.meta_store_log_upload_interval();
        self.upload_loop
            .process(
                self.clone(),
                async move |_| Ok(Delay::new(Duration::from_secs(upload_interval)).await),
                async move |m, _| m.store.run_upload().await,
            )
            .await;
    }

    pub async fn stop_processing_loops(&self) {
        self.upload_loop.stop();
    }

    pub async fn add_listener(&self, listener: Sender<MetaStoreEvent>) {
        self.store.add_listener(listener).await;
    }

    pub fn prepare_test_metastore(test_name: &str) -> (Arc<LocalDirRemoteFs>, Arc<Self>) {
        let config = Config::test(test_name);
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-local", test_name));
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-remote", test_name));

        let _ = std::fs::remove_dir_all(store_path.clone());
        let _ = std::fs::remove_dir_all(remote_store_path.clone());

        let details = Arc::new(RocksMetaStoreDetails {});
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        let store = RocksStore::new(
            store_path.clone().join(details.get_name()).as_path(),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
            details,
        )
        .unwrap();

        (remote_fs, Self::new_from_store(store))
    }

    pub fn cleanup_test_metastore(test_name: &str) {
        RocksStore::cleanup_test_store(test_name)
    }

    pub async fn run_upload(&self) -> Result<(), CubeError> {
        self.store.run_upload().await
    }

    pub async fn upload_check_point(&self) -> Result<(), CubeError> {
        self.store.upload_check_point().await
    }

    pub async fn has_pending_changes(&self) -> Result<bool, CubeError> {
        self.store.has_pending_changes().await
    }

    pub async fn check_all_indexes(&self) -> Result<(), CubeError> {
        RocksStore::check_all_indexes(&self.store).await
    }

    pub async fn read_operation<F, R>(&self, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        self.store.read_operation(f).await
    }

    pub async fn read_operation_out_of_queue<F, R>(&self, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        self.store.read_operation_out_of_queue(f).await
    }

    pub async fn write_operation<F, R>(&self, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>, &'a mut BatchPipe) -> Result<R, CubeError>
            + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
    {
        self.store.write_operation(f).await
    }
}

impl RocksMetaStore {
    fn add_index(
        batch_pipe: &mut BatchPipe,
        rocks_index: &IndexRocksTable,
        rocks_partition: &PartitionRocksTable,
        table_cols: &Vec<Column>,
        table_id: &IdRow<Table>,
        multi_index: Option<&IdRow<MultiIndex>>,
        multi_partitions: &[IdRow<MultiPartition>],
        index_def: IndexDef,
    ) -> Result<IdRow<Index>, CubeError> {
        match index_def.index_type {
            IndexType::Regular => Self::add_regular_index(
                batch_pipe,
                rocks_index,
                rocks_partition,
                table_cols,
                table_id,
                multi_index,
                multi_partitions,
                index_def,
            ),
            IndexType::Aggregate => Self::add_aggregate_index(
                batch_pipe,
                rocks_index,
                rocks_partition,
                table_cols,
                table_id,
                index_def,
            ),
        }
    }
    fn add_regular_index(
        batch_pipe: &mut BatchPipe,
        rocks_index: &IndexRocksTable,
        rocks_partition: &PartitionRocksTable,
        table_cols: &Vec<Column>,
        table_id: &IdRow<Table>,
        multi_index: Option<&IdRow<MultiIndex>>,
        multi_partitions: &[IdRow<MultiPartition>],
        index_def: IndexDef,
    ) -> Result<IdRow<Index>, CubeError> {
        debug_assert_eq!(multi_index.is_some(), !multi_partitions.is_empty());
        if let Some(not_found) = index_def
            .columns
            .iter()
            .find(|dc| table_cols.iter().all(|c| c.name.as_str() != dc.as_str()))
        {
            return Err(CubeError::user(format!(
                "Column '{}' in index '{}' is not found in table '{}'",
                not_found,
                index_def.name,
                table_id.get_row().get_table_name()
            )));
        }
        let unique_key_columns = table_id.get_row().unique_key_columns();
        if let Some(unique_key) = &unique_key_columns {
            if let Some(not_found) = index_def
                .columns
                .iter()
                .find(|dc| unique_key.iter().all(|c| c.name.as_str() != dc.as_str()))
            {
                return Err(CubeError::user(format!(
                    "Column '{}' in index '{}' is out of unique key {:?} for table '{}'. Index columns outside of unique key are not supported.",
                    not_found,
                    index_def.name,
                    unique_key
                        .iter()
                        .map(|c| c.name.to_string())
                        .collect::<Vec<String>>(),
                    table_id.get_row().get_table_name()
                )));
            }
        }

        // First put the columns from the sort key.
        let mut taken = vec![false; table_cols.len()];
        let mut index_columns = Vec::with_capacity(table_cols.len());
        for c in index_def.columns {
            let i = table_cols.iter().position(|tc| tc.name == c).unwrap();
            if taken[i] {
                continue; // ignore duplicate columns inside the index.
            }

            taken[i] = true;
            index_columns.push(table_cols[i].clone().replace_index(index_columns.len()));
        }

        if let Some(unique_key) = &unique_key_columns {
            for c in unique_key.iter() {
                let i = c.get_index();
                if taken[i] {
                    continue;
                }

                taken[i] = true;
                index_columns.push(c.clone().replace_index(index_columns.len()));
            }

            let seq_column = table_id.get_row().seq_column().ok_or_else(|| {
                CubeError::internal(format!(
                    "Seq column is not defined for '{}'",
                    table_id.get_row().get_table_name()
                ))
            })?;
            let i = seq_column.get_index();

            if !taken[i] {
                taken[i] = true;
                index_columns.push(seq_column.replace_index(index_columns.len()));
            }
        }

        let sorted_key_size = index_columns.len() as u64;
        // Put the rest of the columns.
        for i in 0..table_cols.len() {
            if taken[i] {
                continue;
            }

            index_columns.push(table_cols[i].replace_index(index_columns.len()));
        }
        assert_eq!(index_columns.len(), table_cols.len());

        // Validate the columns match types specified in the MultiIndex.
        if let Some(mi) = multi_index {
            let mi = mi.get_row();
            if sorted_key_size as usize != mi.key_columns().len() {
                return Err(CubeError::user(format!(
                    "Partitioned index '{}' has {} columns, got {} columns in ADD TO PARTITIONED INDEX",
                    mi.name(),
                    mi.key_columns().len(),
                    sorted_key_size
                )));
            }
            for i in 0..(sorted_key_size as usize) {
                let l = &index_columns[i];
                let r = &mi.key_columns()[i];
                if l.get_column_type() != &r.column_type {
                    return Err(CubeError::user(
                        format!("Type of table column '{}'({}) is different from type of partitioned index column '{}'({}). Index name is '{}'",
                                l.name, l.column_type, r.name, r.column_type, mi.name()
                        )
                    ));
                }
            }
        }

        let index = Index::try_new(
            index_def.name,
            table_id.get_id(),
            index_columns,
            sorted_key_size,
            // Seq column shouldn't participate in partition split. Otherwise we can't do shared nothing calculations across partitions.
            table_id.get_row().seq_column().map(|_| sorted_key_size - 1),
            multi_index.map(|i| i.get_id()),
            IndexType::Regular,
        )?;
        let index_id = rocks_index.insert(index, batch_pipe)?;
        if multi_partitions.is_empty() {
            rocks_partition.insert(
                Partition::new(index_id.get_id(), None, None, None),
                batch_pipe,
            )?;
        } else {
            for p in multi_partitions {
                rocks_partition.insert(
                    Partition::new(
                        index_id.get_id(),
                        Some(p.get_id()),
                        p.get_row().min_row().cloned(),
                        p.get_row().max_row().cloned(),
                    ),
                    batch_pipe,
                )?;
            }
        }
        Ok(index_id)
    }

    fn add_aggregate_index(
        batch_pipe: &mut BatchPipe,
        rocks_index: &IndexRocksTable,
        rocks_partition: &PartitionRocksTable,
        table_cols: &Vec<Column>,
        table_id: &IdRow<Table>,
        index_def: IndexDef,
    ) -> Result<IdRow<Index>, CubeError> {
        if let Some(not_found) = index_def
            .columns
            .iter()
            .chain(index_def.columns.iter())
            .find(|dc| table_cols.iter().all(|c| c.name.as_str() != dc.as_str()))
        {
            return Err(CubeError::user(format!(
                "Column '{}' in aggregate index '{}' is not found in table '{}'",
                not_found,
                index_def.name,
                table_id.get_row().get_table_name()
            )));
        }

        let aggregate_columns = table_id.get_row().aggregate_columns();
        if aggregate_columns.is_empty() {
            return Err(CubeError::user(format!(
                "Can't create aggregate index for table '{}' because aggregate columns (`AGGREGATIONS`) not specified for the table",
                table_id.get_row().get_table_name()
            )));
        }
        if let Some(col_in_aggreations) = index_def.columns.iter().find(|dc| {
            aggregate_columns
                .iter()
                .any(|c| c.column().name == dc.as_str())
        }) {
            return Err(CubeError::user(format!(
                "Column '{}' in aggregate index '{}' is in aggregations list for table '{}'. Aggregate index columns must be outside of aggregations list.",
                col_in_aggreations,
                index_def.name,
                table_id.get_row().get_table_name()
            )));
        }
        let unique_key_columns = table_id.get_row().unique_key_columns();
        if unique_key_columns.is_some() {
            return Err(CubeError::user(format!(
                "Can't create aggregate index for table '{}' because aggregate index for the table with unique key is not supported yet",
                table_id.get_row().get_table_name())));
        }

        // First put the columns from the sort key.
        let mut taken = vec![false; table_cols.len()];
        let mut index_columns = Vec::with_capacity(index_def.columns.len());
        for c in index_def.columns {
            let i = table_cols.iter().position(|tc| tc.name == c).unwrap();
            if taken[i] {
                continue; // ignore duplicate columns inside the index.
            }
            taken[i] = true;
            index_columns.push(table_cols[i].clone().replace_index(index_columns.len()));
        }

        let sorted_key_size = index_columns.len() as u64;
        // Put the rest of the columns.
        for col in aggregate_columns {
            index_columns.push(col.column().replace_index(index_columns.len()));
        }

        let index = Index::try_new(
            index_def.name,
            table_id.get_id(),
            index_columns,
            sorted_key_size,
            // Seq column shouldn't participate in partition split. Otherwise we can't do shared nothing calculations across partitions.
            None,
            None,
            IndexType::Aggregate,
        )?;

        let index_id = rocks_index.insert(index, batch_pipe)?;
        rocks_partition.insert(
            Partition::new(index_id.get_id(), None, None, None),
            batch_pipe,
        )?;
        Ok(index_id)
    }

    fn get_table_by_name(
        schema_name: String,
        table_name: String,
        rocks_table: TableRocksTable,
        rocks_schema: SchemaRocksTable,
    ) -> Result<IdRow<Table>, CubeError> {
        let schema_id =
            rocks_schema.get_single_row_by_index(&schema_name, &SchemaRocksIndex::Name)?;
        let index_key = TableIndexKey::ByName(schema_id.get_id(), table_name.to_string());
        let table = rocks_table.get_single_row_by_index(&index_key, &TableRocksIndex::Name)?;
        Ok(table)
    }

    fn chunks_by_partition(
        partition_id: u64,
        table: &ChunkRocksTable,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        let chunks = table
            .get_rows_by_index(
                &ChunkIndexKey::ByPartitionId(partition_id),
                &ChunkRocksIndex::PartitionId,
            )?
            .into_iter()
            .filter(|c| include_inactive || c.get_row().uploaded() && c.get_row().active())
            .collect();
        Ok(chunks)
    }

    // Must be run under write_operation(). Returns activated row count.
    fn activate_chunks_impl(
        db_ref: DbTableRef,
        batch_pipe: &mut BatchPipe,
        uploaded_chunk_ids: &[(u64, Option<u64>)],
        replay_handle_id: Option<u64>,
    ) -> Result<(u64, HashMap</*partition_id*/ u64, /*rows*/ u64>), CubeError> {
        let table = ChunkRocksTable::new(db_ref.clone());
        let mut activated_row_count = 0;
        let mut partitions = HashMap::new();
        for (id, file_size) in uploaded_chunk_ids {
            let chunk = table.get_row_or_not_found(*id)?.into_row();
            *partitions.entry(chunk.get_partition_id()).or_default() += chunk.get_row_count();
            activated_row_count += chunk.get_row_count();
            table.update_with_res_fn(
                *id,
                |row| {
                    let mut chunk = row.set_uploaded(true);
                    if let Some(file_size) = file_size {
                        chunk = chunk.set_file_size(*file_size)?;
                    }
                    Ok(chunk.set_replay_handle_id(replay_handle_id.clone()))
                },
                batch_pipe,
            )?;
        }
        return Ok((activated_row_count, partitions));
    }
}

#[async_trait]
impl MetaStore for RocksMetaStore {
    async fn wait_for_current_seq_to_sync(&self) -> Result<(), CubeError> {
        if !self.store.config.upload_to_remote() {
            return Err(CubeError::internal(
                "waiting for metastore to upload in noupload mode".to_string(),
            ));
        }
        while self.store.has_pending_changes().await? {
            tokio::time::timeout(
                Duration::from_secs(30),
                self.store.write_completed_notify.notified(),
            )
            .await?;
        }
        Ok(())
    }

    fn schemas_table(&self) -> SchemaMetaStoreTable {
        SchemaMetaStoreTable {
            rocks_meta_store: self.store.clone(),
        }
    }

    async fn create_schema(
        &self,
        schema_name: String,
        if_not_exists: bool,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let table = SchemaRocksTable::new(db_ref.clone());
            if if_not_exists {
                let rows = table.get_rows_by_index(&schema_name, &SchemaRocksIndex::Name)?;
                if let Some(row) = rows.into_iter().nth(0) {
                    return Ok(row);
                }
            }
            let schema = Schema {
                name: schema_name.clone(),
            };
            Ok(table.insert(schema, batch_pipe)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_schemas(&self) -> Result<Vec<IdRow<Schema>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| SchemaRocksTable::new(db_ref).all_rows())
            .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_schema_by_id(&self, schema_id: u64) -> Result<IdRow<Schema>, CubeError> {
        self.read_operation(move |db_ref| {
            let table = SchemaRocksTable::new(db_ref);
            table.get_row_or_not_found(schema_id)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_schema_id(&self, schema_name: String) -> Result<u64, CubeError> {
        self.read_operation(move |db_ref| {
            let table = SchemaRocksTable::new(db_ref);
            let existing_keys =
                table.get_row_ids_by_index(&schema_name, &SchemaRocksIndex::Name)?;
            check_if_exists(&schema_name, existing_keys.len())?;
            Ok(existing_keys[0])
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_schema(&self, schema_name: String) -> Result<IdRow<Schema>, CubeError> {
        self.read_operation(move |db_ref| {
            let table = SchemaRocksTable::new(db_ref);
            Ok(table.get_single_row_by_index(&schema_name, &SchemaRocksIndex::Name)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn rename_schema(
        &self,
        old_schema_name: String,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let table = SchemaRocksTable::new(db_ref.clone());
            let existing_keys =
                table.get_row_ids_by_index(&old_schema_name, &SchemaRocksIndex::Name)?;
            check_if_exists(&old_schema_name, existing_keys.len())?;

            let schema_id = existing_keys[0];

            let old_schema = table.get_row(schema_id)?.unwrap();
            let mut new_schema = old_schema.clone();
            new_schema.row.set_name(&new_schema_name);
            let id_row = table.update(schema_id, new_schema.row, &old_schema.row, batch_pipe)?;
            Ok(id_row)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn rename_schema_by_id(
        &self,
        schema_id: u64,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let table = SchemaRocksTable::new(db_ref.clone());

            let old_schema = table.get_row(schema_id)?.unwrap();
            let mut new_schema = old_schema.clone();
            new_schema.row.set_name(&new_schema_name);
            let id_row = table.update(schema_id, new_schema.row, &old_schema.row, batch_pipe)?;

            Ok(id_row)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_schema(&self, schema_name: String) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let table = SchemaRocksTable::new(db_ref.clone());
            let existing_keys =
                table.get_row_ids_by_index(&schema_name, &SchemaRocksIndex::Name)?;
            check_if_exists(&schema_name, existing_keys.len())?;
            let schema_id = existing_keys[0];

            let tables = TableRocksTable::new(db_ref.clone()).all_rows()?;
            if tables
                .into_iter()
                .any(|t| t.get_row().get_schema_id() == schema_id)
            {
                return Err(CubeError::user(format!(
                    "Schema {} contains tables and cannot be deleted",
                    schema_name
                )));
            }
            table.delete(schema_id, batch_pipe)?;

            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_schema_by_id(&self, schema_id: u64) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let tables = TableRocksTable::new(db_ref.clone()).all_rows()?;
            if tables
                .into_iter()
                .any(|t| t.get_row().get_schema_id() == schema_id)
            {
                return Err(CubeError::internal(format!(
                    "Schema with id {} contains tables and cannot be deleted",
                    schema_id
                )));
            }
            let table = SchemaRocksTable::new(db_ref.clone());
            table.delete(schema_id, batch_pipe)?;

            Ok(())
        })
        .await
    }

    fn tables_table(&self) -> TableMetaStoreTable {
        TableMetaStoreTable {
            rocks_meta_store: self.store.clone(),
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip(
            self,
            columns,
            locations,
            import_format,
            indexes,
            is_ready,
            build_range_end,
            seal_at,
            select_statement,
            stream_offset,
            unique_key_column_names,
            aggregates,
            partition_split_threshold
        )
    )]
    async fn create_table(
        &self,
        schema_name: String,
        table_name: String,
        columns: Vec<Column>,
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        indexes: Vec<IndexDef>,
        is_ready: bool,
        build_range_end: Option<DateTime<Utc>>,
        seal_at: Option<DateTime<Utc>>,
        select_statement: Option<String>,
        source_coulumns: Option<Vec<Column>>,
        stream_offset: Option<StreamOffset>,
        unique_key_column_names: Option<Vec<String>>,
        aggregates: Option<Vec<(String, String)>>,
        partition_split_threshold: Option<u64>,
        trace_obj: Option<String>,
    ) -> Result<IdRow<Table>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let rocks_table = TableRocksTable::new(db_ref.clone());
            let rocks_index = IndexRocksTable::new(db_ref.clone());
            let rocks_schema = SchemaRocksTable::new(db_ref.clone());
            let rocks_partition = PartitionRocksTable::new(db_ref.clone());
            let rocks_multi_index = MultiIndexRocksTable::new(db_ref.clone());
            let rocks_multi_partition = MultiPartitionRocksTable::new(db_ref.clone());
            let trace_objects_table = TraceObjectRocksTable::new(db_ref.clone());

            let schema_id =
                rocks_schema.get_single_row_by_index(&schema_name, &SchemaRocksIndex::Name)?;
            let mut table_columns = columns.clone();
            let mut seq_column_index = None;
            let unique_key_column_indices = if let Some(column_names) = unique_key_column_names {
                let seq_column =
                    Column::new("__seq".to_string(), ColumnType::Int, table_columns.len());
                seq_column_index = Some(seq_column.column_index as u64);
                table_columns.push(seq_column);
                Some(
                    column_names
                        .iter()
                        .map(|key_column| {
                            let column = columns
                                .iter()
                                .find(|c| &c.name == key_column)
                                .ok_or_else(|| {
                                    CubeError::user(format!(
                                        "Key column {} not found among column definitions {:?}",
                                        key_column, columns
                                    ))
                                })?;
                            Ok(column.column_index as u64)
                        })
                        .collect::<Result<Vec<u64>, CubeError>>()?,
                )
            } else {
                None
            };
            let aggregate_column_indices = if let Some(aggrs) = aggregates {
                let res = aggrs.iter()
                    .map(|aggr| {
                        let aggr_column = &aggr.1;
                        let column = columns
                            .iter()
                            .find(|c| &c.name == aggr_column)
                            .ok_or_else(|| {
                                    CubeError::user(format!(
                                        "Aggregate column {} not found among column definitions {:?}",
                                        aggr_column, columns
                                    ))
                            })?;

                        let index = column.column_index as u64;
                        if let Some(unique_indices) = &unique_key_column_indices {
                            if unique_indices.iter().find(|i| i == &&index).is_some() {
                                return Err(CubeError::user(format!(
                                            "Aggregate column {} is in unique key. A column can't be in an unique key and an aggregation at the same time",
                                            aggr_column
                                            )));
                            }
                        }
                        let function = aggr.0.parse::<AggregateFunction>()?;

                        if !function.allowed_for_type(&column.column_type) {
                            return Err(CubeError::user(
                                    format!(
                                        "Aggregate function {} not allowed for column type {}",
                                        function, &column.column_type
                                        )
                            ))
                        }
                        Ok(AggregateColumnIndex::new(index, function))
                    })
                .collect::<Result<Vec<_>,_>>()?;

                res
            } else {
                vec![]
            };
            let table = Table::new(
                table_name,
                schema_id.get_id(),
                table_columns.clone(),
                locations,
                import_format,
                is_ready,
                build_range_end,
                seal_at,
                select_statement,
                source_coulumns,
                stream_offset,
                unique_key_column_indices,
                aggregate_column_indices,
                seq_column_index,
                partition_split_threshold,
            );
            let table_id = rocks_table.insert(table, batch_pipe)?;

            if let Some(trace_obj) = trace_obj {
                let trace_object = TraceObject::new(table_id.get_id(), trace_obj);
                trace_objects_table.insert(trace_object, batch_pipe)?;
            }

            for index_def in indexes.into_iter() {
                let multi_index;
                let mut multi_partitions;
                match &index_def.multi_index {
                    None => {
                        multi_index = None;
                        multi_partitions = vec![];
                    }
                    Some(mi) => {
                        let mi = rocks_multi_index.get_single_row_by_index(
                            &MultiIndexIndexKey::ByName(schema_id.get_id(), mi.clone()),
                            &MultiIndexRocksIndex::ByName,
                        )?;
                        multi_partitions = rocks_multi_partition.get_rows_by_index(
                            &MultiPartitionIndexKey::ByMultiIndexId(mi.get_id()),
                            &MultiPartitionRocksIndex::ByMultiIndexId,
                        )?;
                        multi_partitions.retain(|m| m.row.active());
                        multi_index = Some(mi);
                    }
                }
                RocksMetaStore::add_index(
                    batch_pipe,
                    &rocks_index,
                    &rocks_partition,
                    &table_columns,
                    &table_id,
                    multi_index.as_ref(),
                    &multi_partitions,
                    index_def,
                )?;
            }
            let def_index_columns = table_id
                .get_row()
                .unique_key_columns()
                .map(|c| c.into_iter().map(|c| c.clone()).collect::<Vec<Column>>())
                .unwrap_or(table_columns.clone())
                .iter()
                .filter_map(|c| match c.get_column_type() {
                    ColumnType::Bytes => None,
                    ColumnType::HyperLogLog(_) => None,
                    _ => {
                        if seq_column_index.is_none()
                            || seq_column_index.is_some()
                                && c.get_index() as u64 != seq_column_index.unwrap()
                        {
                            Some(c.get_name().clone())
                        } else {
                            None
                        }
                    }
                })
                .collect_vec();
            RocksMetaStore::add_index(
                batch_pipe,
                &rocks_index,
                &rocks_partition,
                &table_columns,
                &table_id,
                None,
                &[],
                IndexDef {
                    name: "default".to_string(),
                    multi_index: None,
                    columns: def_index_columns,
                    index_type: IndexType::Regular
                },
            )?;

            Ok(table_id)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn table_ready(&self, id: u64, is_ready: bool) -> Result<IdRow<Table>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let rocks_table = TableRocksTable::new(db_ref.clone());
            Ok(rocks_table.update_with_fn(id, |r| r.update_is_ready(is_ready), batch_pipe)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn seal_table(&self, id: u64) -> Result<IdRow<Table>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let rocks_table = TableRocksTable::new(db_ref.clone());
            Ok(rocks_table.update_with_fn(id, |r| r.update_sealed(true), batch_pipe)?)
        })
        .await
    }

    async fn get_trace_obj_by_table_id(&self, table_id: u64) -> Result<Option<String>, CubeError> {
        self.read_operation(move |db_ref| {
            let table = TraceObjectRocksTable::new(db_ref);
            let trace_object_row = table.get_single_opt_row_by_index(
                &TraceObjectIndexKey::ByTableId(table_id),
                &TraceObjectRocksIndex::ByTableId,
            )?;
            Ok(trace_object_row.map(|r| r.get_row().trace_obj().clone()))
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn update_location_download_size(
        &self,
        id: u64,
        location: String,
        download_size: u64,
    ) -> Result<IdRow<Table>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let rocks_table = TableRocksTable::new(db_ref.clone());
            Ok(rocks_table.update_with_res_fn(
                id,
                |r| r.update_location_download_size(&location, download_size),
                batch_pipe,
            )?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_table(
        &self,
        schema_name: String,
        table_name: String,
    ) -> Result<IdRow<Table>, CubeError> {
        self.read_operation(move |db_ref| get_table_impl(db_ref, schema_name, table_name))
            .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_table_by_id(&self, table_id: u64) -> Result<IdRow<Table>, CubeError> {
        self.read_operation(move |db_ref| {
            TableRocksTable::new(db_ref.clone()).get_row_or_not_found(table_id)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_tables(&self) -> Result<Vec<IdRow<Table>>, CubeError> {
        self.read_operation_out_of_queue(|db_ref| TableRocksTable::new(db_ref).all_rows())
            .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_tables_with_path(
        &self,
        include_non_ready: bool,
    ) -> Result<Arc<Vec<TablePath>>, CubeError> {
        if include_non_ready {
            self.read_operation_out_of_queue(move |db_ref| {
                let tables = TableRocksTable::new(db_ref.clone()).all_rows()?;
                let schemas = SchemaRocksTable::new(db_ref);
                let tables = Arc::new(schemas.build_path_rows(
                    tables,
                    |t| t.get_row().get_schema_id(),
                    |table, schema| TablePath { table, schema },
                )?);

                Ok(tables)
            })
            .await
        } else {
            let cache = self.store.cached_tables.clone();

            if let Some(t) = cube_ext::spawn_blocking(move || cache.lock().unwrap().clone()).await?
            {
                return Ok(t);
            }

            let cache = self.store.cached_tables.clone();
            // Can't do read_operation_out_of_queue as we need to update cache on the same thread where it's dropped
            self.read_operation(move |db_ref| {
                let cached_tables = { cache.lock().unwrap().clone() };
                if let Some(t) = cached_tables {
                    return Ok(t);
                }
                let table_rocks_table = TableRocksTable::new(db_ref.clone());
                let mut tables = Vec::new();
                for t in table_rocks_table.scan_all_rows()? {
                    let t = t?;
                    if t.get_row().is_ready() {
                        tables.push(t);
                    }
                }
                let schemas = SchemaRocksTable::new(db_ref);
                let tables = Arc::new(schemas.build_path_rows(
                    tables,
                    |t| t.get_row().get_schema_id(),
                    |table, schema| TablePath { table, schema },
                )?);

                let to_cache = tables.clone();
                *cache.lock().unwrap() = Some(to_cache);

                Ok(tables)
            })
            .await
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn not_ready_tables(
        &self,
        created_seconds_ago: i64,
    ) -> Result<Vec<IdRow<Table>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let table_rocks_table = TableRocksTable::new(db_ref);
            let tables = table_rocks_table.scan_all_rows()?;
            let mut res = Vec::new();
            let now = Utc::now();
            for table in tables {
                let table = table?;
                if !table.get_row().is_ready()
                    && table
                        .get_row()
                        .created_at()
                        .as_ref()
                        .map(|created_at| {
                            now.signed_duration_since(created_at.clone()).num_seconds()
                                >= created_seconds_ago
                        })
                        .unwrap_or(false)
                {
                    res.push(table);
                }
            }
            Ok(res)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn drop_table(&self, table_id: u64) -> Result<IdRow<Table>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            batch_pipe.invalidate_tables_cache();
            let tables_table = TableRocksTable::new(db_ref.clone());
            let indexes_table = IndexRocksTable::new(db_ref.clone());
            let replay_handles_table = ReplayHandleRocksTable::new(db_ref.clone());
            let trace_objects_table = TraceObjectRocksTable::new(db_ref.clone());
            let indexes = indexes_table.get_row_ids_by_index(
                &IndexIndexKey::TableId(table_id),
                &IndexRocksIndex::TableID,
            )?;
            let trace_objects = trace_objects_table.get_rows_by_index(
                &TraceObjectIndexKey::ByTableId(table_id),
                &TraceObjectRocksIndex::ByTableId,
            )?;
            for trace_object in trace_objects {
                trace_objects_table.delete(trace_object.get_id(), batch_pipe)?;
            }
            let replay_handles = replay_handles_table.get_rows_by_index(
                &ReplayHandleIndexKey::ByTableId(table_id),
                &ReplayHandleRocksIndex::ByTableId,
            )?;
            for replay_handle in replay_handles {
                replay_handles_table.delete(replay_handle.get_id(), batch_pipe)?;
            }
            for index in indexes {
                RocksMetaStore::drop_index(db_ref.clone(), batch_pipe, index, true)?;
            }
            Ok(tables_table.delete(table_id, batch_pipe)?)
        })
        .await
    }

    fn partition_table(&self) -> PartitionMetaStoreTable {
        PartitionMetaStoreTable {
            rocks_meta_store: self.store.clone(),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_partition(&self, partition: Partition) -> Result<IdRow<Partition>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = PartitionRocksTable::new(db_ref.clone());
            let row_id = table.insert(partition, batch_pipe)?;
            Ok(row_id)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError> {
        self.read_operation(move |db_ref| {
            PartitionRocksTable::new(db_ref).get_row_or_not_found(partition_id)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_partition_out_of_queue(
        &self,
        partition_id: u64,
    ) -> Result<IdRow<Partition>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            PartitionRocksTable::new(db_ref).get_row_or_not_found(partition_id)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_partition_for_compaction(
        &self,
        partition_id: u64,
    ) -> Result<
        (
            IdRow<Partition>,
            IdRow<Index>,
            IdRow<Table>,
            Option<IdRow<MultiPartition>>,
        ),
        CubeError,
    > {
        self.read_operation(move |db_ref| {
            let partition = PartitionRocksTable::new(db_ref.clone())
                .get_row(partition_id)?
                .ok_or(CubeError::internal(format!(
                    "Partition is not found: {}",
                    partition_id
                )))?;
            let index = IndexRocksTable::new(db_ref.clone())
                .get_row(partition.get_row().get_index_id())?
                .ok_or(CubeError::internal(format!(
                    "Index {} is not found for partition: {}",
                    partition.get_row().get_index_id(),
                    partition_id
                )))?;
            let table = TableRocksTable::new(db_ref.clone())
                .get_row_or_not_found(index.get_row().table_id())?;
            let multi_part = match partition.get_row().multi_partition_id {
                None => None,
                Some(m) => {
                    Some(MultiPartitionRocksTable::new(db_ref.clone()).get_row_or_not_found(m)?)
                }
            };
            Ok((partition, index, table, multi_part))
        })
        .await
    }

    async fn get_used_disk_space_out_of_queue(
        &self,
        node: Option<String>,
    ) -> Result<u64, CubeError> {
        let cached = if let Some((sizes, time)) = self.disk_space_cache.read().await.as_ref() {
            let cache_duration =
                Duration::from_secs(self.store.config.disk_space_cache_duration_secs());
            if time.elapsed()? < cache_duration {
                Some(sizes.clone())
            } else {
                None
            }
        } else {
            None
        };
        let sizes_map = if let Some(sizes) = cached {
            sizes
        } else {
            let (partitions, chunks) = self.get_all_partitions_and_chunks_out_of_queue().await?;
            let mut partitions_map = partitions
                .into_iter()
                .map(|p| {
                    (
                        p.get_id(),
                        (
                            p.get_row().file_size().unwrap_or(0),
                            node_name_by_partition(self.store.config.as_ref(), &p),
                        ),
                    )
                })
                .collect::<HashMap<u64, (u64, String)>>();
            for c in chunks.into_iter() {
                if let Some((ref mut size, _)) =
                    partitions_map.get_mut(&c.get_row().get_partition_id())
                {
                    *size = c.get_row().file_size().unwrap_or(0);
                }
            }

            let workers = if self.store.config.select_workers().is_empty() {
                vec![self.store.config.server_name().clone()]
            } else {
                self.store.config.select_workers().clone()
            };

            let mut map = workers
                .into_iter()
                .map(|n| (n, 0))
                .collect::<HashMap<String, u64>>();

            for (_, (size, node)) in partitions_map.into_iter() {
                map.entry(node).and_modify(|s| *s += size).or_insert(0);
            }

            let mut cache = self.disk_space_cache.write().await;
            *cache = Some((map.clone(), SystemTime::now()));

            map
        };

        let res = if let Some(node_name) = node {
            sizes_map.get(&node_name).unwrap_or(&0).clone()
        } else {
            sizes_map.values().sum::<u64>()
        };

        Ok(res)
    }

    async fn get_all_partitions_and_chunks_out_of_queue(
        &self,
    ) -> Result<(Vec<IdRow<Partition>>, Vec<IdRow<Chunk>>), CubeError> {
        self.read_operation_out_of_queue(move |db| {
            let partitions = PartitionRocksTable::new(db.clone()).all_rows()?;
            let chunks = ChunkRocksTable::new(db).all_rows()?;
            Ok((partitions, chunks))
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_partition_chunk_sizes(&self, partition_id: u64) -> Result<u64, CubeError> {
        let chunks = self
            .get_chunks_by_partition_out_of_queue(partition_id, false)
            .await?;
        Ok(chunks.iter().map(|r| r.get_row().row_count).sum())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn swap_compacted_chunks(
        &self,
        partition_id: u64,
        old_chunk_ids: Vec<u64>,
        new_chunk: u64,
        new_chunk_file_size: u64,
    ) -> Result<bool, CubeError> {
        self.write_operation(move |db, pipe| {
            let p = PartitionRocksTable::new(db.clone()).get_row_or_not_found(partition_id)?;
            if let Some(mp) = p.row.multi_partition_id {
                let mp = MultiPartitionRocksTable::new(db.clone()).get_row_or_not_found(mp)?;
                if mp.row.prepared_for_split() {
                    // When run concurrently with multi-split, the latter takes precedence.
                    return Ok(false);
                }
            }
            RocksMetaStore::swap_chunks_impl(
                old_chunk_ids,
                vec![(new_chunk, Some(new_chunk_file_size))],
                db,
                pipe,
                false,
                None,
            )?;
            Ok(true)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn swap_active_partitions(
        &self,
        current_active: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
        new_active: Vec<(IdRow<Partition>, u64)>,
        mut new_active_min_max: Vec<(u64, (Option<Row>, Option<Row>), (Option<Row>, Option<Row>))>,
    ) -> Result<(), CubeError> {
        trace!(
            "Swapping partitions: deactivating ({}), deactivating chunks ({}), activating ({})",
            current_active.iter().map(|(p, _)| p.id).join(", "),
            current_active
                .iter()
                .flat_map(|(_, cs)| cs)
                .map(|c| c.id)
                .join(", "),
            new_active.iter().map(|(p, _)| p.id).join(", ")
        );
        self.write_operation(move |db, pipe| {
            swap_active_partitions_impl(
                db,
                pipe,
                &current_active,
                &new_active,
                move |i, p| {
                    let (rows, (min_val, max_val), (min, max)) = take(&mut new_active_min_max[i]);
                    p.update_min_max_and_row_count(min_val, max_val, rows, min, max)
                },
                |current_i| {
                    Err(CubeError::internal(format!(
                        "Current partition is not found during swap active: {}",
                        current_active[current_i].0.id
                    )))
                },
                |_| panic!("error from current partition must propagate before this call"),
            )
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let partitions_table = PartitionRocksTable::new(db_ref.clone());
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let partition = partitions_table.get_row_or_not_found(partition_id)?;

            if partition.get_row().is_active() {
                return Err(CubeError::internal(format!(
                    "Can't drop active partition: {:?}",
                    partition
                )));
            }

            let chunks = chunks_table.get_rows_by_index(
                &ChunkIndexKey::ByPartitionId(partition_id),
                &ChunkRocksIndex::PartitionId,
            )?;

            if !chunks.is_empty() {
                return Err(CubeError::internal(format!(
                    "Can't drop partition {:?} with chunks: {:?}",
                    partition, chunks
                )));
            }

            let children = partitions_table.get_rows_by_index(
                &PartitionIndexKey::ByParentPartitionId(Some(partition.get_id())),
                &PartitionRocksIndex::ParentPartitionId,
            )?;

            if !children.is_empty() {
                return Err(CubeError::internal(format!(
                    "Can't drop partition {:?} with child partitions: {:?}",
                    partition, children
                )));
            }

            PartitionRocksTable::new(db_ref).delete(partition_id, batch_pipe)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn mark_partition_warmed_up(&self, partition_id: u64) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = PartitionRocksTable::new(db_ref);
            let partition = table.get_row_or_not_found(partition_id)?;
            table.update(
                partition_id,
                partition.row.to_warmed_up(),
                &partition.row,
                batch_pipe,
            )?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_middle_man_partition(
        &self,
        partition_id: u64,
    ) -> Result<IdRow<Partition>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let partitions_table = PartitionRocksTable::new(db_ref.clone());
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let partition = partitions_table.get_row_or_not_found(partition_id)?;

            if let Some(parent_partition_id) = partition.get_row().parent_partition_id() {
                let parent_partition = partitions_table.get_row_or_not_found(*parent_partition_id)?;
                // This sanity check isn't structurally used anywhere as of now however we might need
                // to preserve such hierarchy in future for special type of indexes.
                if parent_partition.get_row().get_max_val() != partition.get_row().get_max_val() ||
                    parent_partition.get_row().get_min_val() != partition.get_row().get_min_val() {
                    return Err(CubeError::internal(format!(
                        "Middle man drop should preserve partition hierarchy but trying to drop partition {:?} with parent {:?}",
                        partition,
                        parent_partition
                    )));
                }
            } else {
                return Err(CubeError::internal(format!(
                    "Can't drop root partition as middle man: {:?}",
                    partition
                )));
            }

            if partition.get_row().is_active() {
                return Err(CubeError::internal(format!(
                    "Can't drop active partition: {:?}",
                    partition
                )));
            }

            let chunks = chunks_table
                .get_rows_by_index(
                    &ChunkIndexKey::ByPartitionId(partition_id),
                    &ChunkRocksIndex::PartitionId,
                )?;

            if !chunks.is_empty() {
                return Err(CubeError::internal(format!(
                    "Can't drop partition {:?} with chunks: {:?}",
                    partition,
                    chunks
                )));
            }

            let children = partitions_table
                .get_rows_by_index(
                    &PartitionIndexKey::ByParentPartitionId(Some(partition.get_id())),
                    &PartitionRocksIndex::ParentPartitionId,
                )?;

            if !children.is_empty() {
                for child in children.into_iter() {
                    partitions_table.update_with_fn(child.get_id(), |c| c.update_parent_partition_id(partition.get_row().parent_partition_id().clone()), batch_pipe)?;
                }
            }

            partitions_table.delete(partition_id, batch_pipe)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn can_delete_partition(&self, partition_id: u64) -> Result<bool, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let partitions_table = PartitionRocksTable::new(db_ref.clone());
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let partition = partitions_table.get_row_or_not_found(partition_id)?;

            if partition.get_row().is_active() {
                return Ok(false);
            }

            let chunks = chunks_table.get_rows_by_index(
                &ChunkIndexKey::ByPartitionId(partition_id),
                &ChunkRocksIndex::PartitionId,
            )?;

            if !chunks.is_empty() {
                return Ok(false);
            }

            let children = partitions_table.get_rows_by_index(
                &PartitionIndexKey::ByParentPartitionId(Some(partition.get_id())),
                &PartitionRocksIndex::ParentPartitionId,
            )?;

            if !children.is_empty() {
                return Ok(false);
            }

            Ok(true)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn can_delete_middle_man_partition(&self, partition_id: u64) -> Result<bool, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let partitions_table = PartitionRocksTable::new(db_ref.clone());
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let partition = partitions_table.get_row_or_not_found(partition_id)?;

            if let Some(parent_partition_id) = partition.get_row().parent_partition_id() {
                let parent_partition =
                    partitions_table.get_row_or_not_found(*parent_partition_id)?;
                if parent_partition.get_row().get_max_val() != partition.get_row().get_max_val()
                    || parent_partition.get_row().get_min_val() != partition.get_row().get_min_val()
                {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }

            if partition.get_row().is_active() {
                return Ok(false);
            }

            let chunks = chunks_table.get_rows_by_index(
                &ChunkIndexKey::ByPartitionId(partition_id),
                &ChunkRocksIndex::PartitionId,
            )?;

            if !chunks.is_empty() {
                return Ok(false);
            }

            Ok(true)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn all_inactive_partitions_to_repartition(
        &self,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let partitions_table = PartitionRocksTable::new(db_ref.clone());
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let mut partitions_with_chunks = HashSet::new();

            let chunks = chunks_table.scan_all_rows()?;

            for chunk in chunks {
                let chunk = chunk?;
                if chunk.get_row().active() {
                    partitions_with_chunks.insert(chunk.get_row().get_partition_id());
                }
            }

            let mut to_repartition = Vec::new();

            for partition in partitions_table.scan_rows_by_index(
                &PartitionIndexKey::ByActive(false),
                &PartitionRocksIndex::Active,
            )? {
                let p = partition?;
                if !p.get_row().is_active() && partitions_with_chunks.contains(&p.get_id()) {
                    to_repartition.push(p);
                }
            }

            Ok(to_repartition)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn all_inactive_middle_man_partitions(&self) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let partitions_table = PartitionRocksTable::new(db_ref.clone());
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let mut partitions_with_chunks = HashSet::new();

            let chunks = chunks_table.scan_all_rows()?;

            for chunk in chunks {
                partitions_with_chunks.insert(chunk?.get_row().get_partition_id());
            }

            let orphaned_partitions = partitions_table.scan_rows_by_index(
                &PartitionIndexKey::ByActive(false),
                &PartitionRocksIndex::Active,
            )?;

            let mut result = Vec::new();

            for partition in orphaned_partitions {
                let partition = partition?;
                if !partitions_with_chunks.contains(&partition.get_id()) {
                    if let Some(parent_partition_id) = partition.get_row().parent_partition_id() {
                        // TODO it actually should fail if it isn't found but skip for now due to bug in reconciliation process which led to missing parents
                        if let Ok(parent_partition) =
                            partitions_table.get_row_or_not_found(*parent_partition_id)
                        {
                            if parent_partition.get_row().get_max_val()
                                == partition.get_row().get_max_val()
                                && parent_partition.get_row().get_min_val()
                                    == partition.get_row().get_min_val()
                            {
                                result.push(partition);
                            }
                        }
                    }
                }
            }

            Ok(result)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn all_just_created_partitions(&self) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let partitions_table = PartitionRocksTable::new(db_ref.clone());

            let orphaned_partitions = partitions_table.scan_rows_by_index(
                &PartitionIndexKey::ByJustCreated(true),
                &PartitionRocksIndex::JustCreated,
            )?;

            let mut result = Vec::new();

            for partition in orphaned_partitions {
                let partition = partition?;
                result.push(partition);
            }

            Ok(result)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_partitions_with_chunks_created_seconds_ago(
        &self,
        seconds_ago: i64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let now = Utc::now();
            let mut partition_ids = HashSet::new();
            for c in chunks_table.scan_all_rows()? {
                let c = c?;
                if c.get_row().active()
                    && c.get_row()
                        .created_at()
                        .as_ref()
                        .map(|created_at| {
                            now.signed_duration_since(created_at.clone()).num_seconds()
                                >= seconds_ago
                        })
                        .unwrap_or(false)
                {
                    partition_ids.insert(c.get_row().get_partition_id());
                }
            }

            let partitions_table = PartitionRocksTable::new(db_ref.clone());
            let mut partitions = Vec::new();
            for id in partition_ids {
                if let Some(partition) = partitions_table.get_row(id)? {
                    partitions.push(partition);
                }
            }

            Ok(partitions)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_partitions_for_in_memory_compaction(
        &self,
        node: String,
    ) -> Result<
        Vec<(
            IdRow<Partition>,
            IdRow<Index>,
            IdRow<Table>,
            Vec<IdRow<Chunk>>,
        )>,
        CubeError,
    > {
        let config = self.store.config.clone();
        self.read_operation_out_of_queue(move |db_ref| {
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let mut partitions_map = HashMap::new();
            for c in chunks_table.scan_all_rows()? {
                let c = c?;
                if c.get_row().active() && c.get_row().in_memory() {
                    partitions_map
                        .entry(c.get_row().get_partition_id())
                        .or_insert(Vec::new())
                        .push(c);
                }
            }

            let partitions_table = PartitionRocksTable::new(db_ref.clone());

            let mut result = Vec::with_capacity(partitions_map.len());
            let index_table = IndexRocksTable::new(db_ref.clone());
            let table_table = TableRocksTable::new(db_ref.clone());

            for (id, chunks) in partitions_map.into_iter() {
                if let Some(partition) = partitions_table.get_row(id)? {
                    if partition.get_row().is_active()
                        && partition.get_row().multi_partition_id.is_none()
                        && node_name_by_partition(config.as_ref(), &partition) == node
                    {
                        let index = index_table
                            .get_row(partition.get_row().get_index_id())?
                            .ok_or(CubeError::internal(format!(
                                "Index {} is not found for partition: {}",
                                partition.get_row().get_index_id(),
                                id
                            )))?;
                        let table = table_table.get_row_or_not_found(index.get_row().table_id())?;

                        result.push((partition, index, table, chunks));
                    }
                }
            }

            Ok(result)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_all_node_in_memory_chunks(
        &self,
        node: String,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        let config = self.store.config.clone();
        self.read_operation_out_of_queue(move |db_ref| {
            let chunks_table = ChunkRocksTable::new(db_ref.clone());
            let partitions_table = PartitionRocksTable::new(db_ref.clone());

            let mut partitions_map = HashMap::new();
            for c in chunks_table.scan_all_rows()? {
                let c = c?;
                if c.get_row().in_memory() {
                    partitions_map
                        .entry(c.get_row().get_partition_id())
                        .or_insert(Vec::new())
                        .push(c);
                }
            }

            let mut result = Vec::new();

            for (id, mut chunks) in partitions_map.into_iter() {
                if let Some(partition) = partitions_table.get_row(id)? {
                    if node_name_by_partition(config.as_ref(), &partition) == node {
                        result.append(&mut chunks);
                    }
                }
            }

            Ok(result)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_chunks_without_partition_created_seconds_ago(
        &self,
        seconds_ago: i64,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let chunks_table = ChunkRocksTable::new(db_ref.clone());

            let now = Utc::now();
            let mut partitions = HashMap::new();
            for c in chunks_table.scan_all_rows()? {
                let c = c?;
                if c.get_row().active()
                    && c.get_row()
                        .created_at()
                        .as_ref()
                        .map(|created_at| {
                            now.signed_duration_since(created_at.clone()).num_seconds()
                                >= seconds_ago
                        })
                        .unwrap_or(false)
                {
                    partitions
                        .entry(c.get_row().get_partition_id())
                        .or_insert(vec![])
                        .push(c);
                }
            }

            let partitions_table = PartitionRocksTable::new(db_ref.clone());

            let mut result = Vec::new();
            for (id, mut chunks) in partitions {
                if partitions_table.get_row(id)?.is_none() {
                    result.append(&mut chunks);
                }
            }

            Ok(result)
        })
        .await
    }

    fn index_table(&self) -> IndexMetaStoreTable {
        IndexMetaStoreTable {
            rocks_meta_store: self.store.clone(),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_index(
        &self,
        schema_name: String,
        table_name: String,
        index_def: IndexDef,
    ) -> Result<IdRow<Index>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let rocks_index = IndexRocksTable::new(db_ref.clone());
            let rocks_partition = PartitionRocksTable::new(db_ref.clone());
            let rocks_table = TableRocksTable::new(db_ref.clone());
            let rocks_schema = SchemaRocksTable::new(db_ref.clone());

            let table = RocksMetaStore::get_table_by_name(
                schema_name,
                table_name,
                rocks_table,
                rocks_schema,
            )?;

            if *table.get_row().has_data() {
                return Err(CubeError::user(format!(
                    "Can't create '{}' index because '{}' table already has data",
                    index_def.name,
                    table.get_row().get_table_name()
                )));
            }

            Ok(RocksMetaStore::add_index(
                batch_pipe,
                &rocks_index,
                &rocks_partition,
                table.get_row().get_columns(),
                &table,
                None,
                &[],
                index_def,
            )?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_default_index(&self, table_id: u64) -> Result<IdRow<Index>, CubeError> {
        self.read_operation(move |db_ref| get_default_index_impl(db_ref, table_id))
            .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_table_indexes(&self, table_id: u64) -> Result<Vec<IdRow<Index>>, CubeError> {
        self.read_operation(move |db_ref| {
            let index_table = IndexRocksTable::new(db_ref);
            Ok(index_table
                .get_rows_by_index(&IndexIndexKey::TableId(table_id), &IndexRocksIndex::TableID)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_table_indexes_out_of_queue(
        &self,
        table_id: u64,
    ) -> Result<Vec<IdRow<Index>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let index_table = IndexRocksTable::new(db_ref);
            Ok(index_table
                .get_rows_by_index(&IndexIndexKey::TableId(table_id), &IndexRocksIndex::TableID)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_active_partitions_by_index_id(
        &self,
        index_id: u64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let rocks_partition = PartitionRocksTable::new(db_ref);
            // TODO iterate over range
            Ok(rocks_partition
                .get_rows_by_index(
                    &PartitionIndexKey::ByIndexId(index_id),
                    &PartitionRocksIndex::IndexId,
                )?
                .into_iter()
                .filter(|r| r.get_row().active)
                .collect::<Vec<_>>())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_index(&self, index_id: u64) -> Result<IdRow<Index>, CubeError> {
        self.read_operation(move |db_ref| {
            IndexRocksTable::new(db_ref).get_row_or_not_found(index_id)
        })
        .await
    }

    async fn get_index_with_active_partitions_out_of_queue(
        &self,
        index_id: u64,
    ) -> Result<(IdRow<Index>, Vec<IdRow<Partition>>), CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let index = IndexRocksTable::new(db_ref.clone()).get_row_or_not_found(index_id)?;
            let rocks_partition = PartitionRocksTable::new(db_ref);

            let partitions = rocks_partition
                .get_rows_by_index(
                    &PartitionIndexKey::ByIndexId(index.get_id()),
                    &PartitionRocksIndex::IndexId,
                )?
                .into_iter()
                .filter(|r| r.get_row().active)
                .collect::<Vec<_>>();

            Ok((index, partitions))
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self, key_columns))]
    async fn create_partitioned_index(
        &self,
        schema: String,
        name: String,
        key_columns: Vec<Column>,
        if_not_exists: bool,
    ) -> Result<IdRow<MultiIndex>, CubeError> {
        self.write_operation(move |db, pipe| {
            let mindexes = MultiIndexRocksTable::new(db.clone());
            let mpartitions = MultiPartitionRocksTable::new(db.clone());
            let schemas = SchemaRocksTable::new(db.clone());
            let schema_id = schemas
                .get_single_row_by_index(&schema, &SchemaRocksIndex::Name)?
                .id;
            if if_not_exists {
                let mut existing = mindexes.get_rows_by_index(
                    &MultiIndexIndexKey::ByName(schema_id, name.clone()),
                    &MultiIndexRocksIndex::ByName,
                )?;
                if !existing.is_empty() {
                    return Ok(existing.remove(0));
                }
            }
            let r = mindexes.insert(MultiIndex::new(schema_id, name, key_columns), pipe)?;
            mpartitions.insert(MultiPartition::new_root(r.id), pipe)?;
            Ok(r)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn drop_partitioned_index(&self, schema: String, name: String) -> Result<(), CubeError> {
        self.write_operation(move |db, pipe| {
            let schema_id = SchemaRocksTable::new(db.clone())
                .get_single_row_by_index(&schema, &SchemaRocksIndex::Name)?
                .id;

            let multi_index_t = MultiIndexRocksTable::new(db.clone());
            let multi_index_id = multi_index_t
                .get_single_row_by_index(
                    &MultiIndexIndexKey::ByName(schema_id, name),
                    &MultiIndexRocksIndex::ByName,
                )?
                .id;
            multi_index_t.delete(multi_index_id, pipe)?;

            let index_t = IndexRocksTable::new(db.clone());
            let indexes = index_t.get_row_ids_by_index(
                &IndexIndexKey::MultiIndexId(Some(multi_index_id)),
                &IndexRocksIndex::MultiIndexId,
            )?;
            for index in indexes {
                RocksMetaStore::drop_index(db.clone(), pipe, index, false)?;
            }

            let multi_part_t = MultiPartitionRocksTable::new(db.clone());
            let multi_partitions = multi_part_t.get_row_ids_by_index(
                &MultiPartitionIndexKey::ByMultiIndexId(multi_index_id),
                &MultiPartitionRocksIndex::ByMultiIndexId,
            )?;
            for mp in multi_partitions {
                multi_part_t.delete(mp, pipe)?;
            }

            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_active_partitions_and_chunks_by_index_id_for_select(
        &self,
        index_id: Vec<u64>,
    ) -> Result<Vec<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let rocks_chunk = ChunkRocksTable::new(db_ref.clone());
            let rocks_partition = PartitionRocksTable::new(db_ref);

            let mut results = Vec::with_capacity(index_id.len());
            for index_id in index_id {
                let mut processed = HashSet::new();
                let mut partitions = Vec::new();
                let mut add_with_parents = |mut p: u64| -> Result<(), CubeError> {
                    loop {
                        if !processed.insert(p) {
                            break;
                        }
                        let r = rocks_partition.get_row_or_not_found(p)?;
                        let parent = r.row.parent_partition_id().clone();
                        partitions.push((r, Vec::new()));
                        match parent {
                            None => break,
                            Some(parent) => p = parent,
                        }
                    }
                    Ok(())
                };
                // TODO iterate over range.
                for p in rocks_partition.get_row_ids_by_index(
                    &PartitionIndexKey::ByIndexId(index_id),
                    &PartitionRocksIndex::IndexId,
                )? {
                    add_with_parents(p)?;
                }

                for (p, chunks) in &mut partitions {
                    *chunks = Self::chunks_by_partition(p.id, &rocks_chunk, false)?;
                }
                results.push(partitions)
            }
            Ok(results)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_warmup_partitions(
        &self,
    ) -> Result<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>, CubeError> {
        self.read_operation_out_of_queue(|db| {
            // Do full scan, likely only a small number chunks and partitions are inactive.
            let mut partition_to_chunks = HashMap::new();
            for c in ChunkRocksTable::new(db.clone()).table_scan(db.snapshot)? {
                let c = c?;
                if !c.row.active() {
                    continue;
                }
                partition_to_chunks
                    .entry(c.row.partition_id)
                    .or_insert(Vec::new())
                    .push(c.clone())
            }

            let mut partitions = Vec::new();
            for p in PartitionRocksTable::new(db.clone()).table_scan(db.snapshot)? {
                let p = p?;
                if p.row.is_active() {
                    let mut chunks = Vec::new();
                    chunks.extend(partition_to_chunks.entry(p.id).or_default().iter().cloned());
                    if let Some(parent_id) = p.row.parent_partition_id {
                        chunks.extend(
                            partition_to_chunks
                                .entry(parent_id)
                                .or_default()
                                .iter()
                                .cloned(),
                        );
                    }
                    partitions.push((p, chunks));
                }
            }
            Ok(partitions)
        })
        .await
    }
    async fn get_all_filenames(&self) -> Result<Vec<String>, CubeError> {
        self.read_operation_out_of_queue(|db| {
            let mut filenames = Vec::new();
            for c in ChunkRocksTable::new(db.clone()).table_scan(db.snapshot)? {
                let c = c?;
                if !c.row.in_memory {
                    filenames.push(c.row.get_full_name(c.id));
                }
            }

            for p in PartitionRocksTable::new(db.clone()).table_scan(db.snapshot)? {
                let p = p?;
                if let Some(filename) = p.row.get_full_name(p.id) {
                    filenames.push(filename);
                }
            }
            Ok(filenames)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_chunk(
        &self,
        partition_id: u64,
        row_count: usize,
        min: Option<Row>,
        max: Option<Row>,
        in_memory: bool,
    ) -> Result<IdRow<Chunk>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let rocks_chunk = ChunkRocksTable::new(db_ref.clone());

            let chunk = Chunk::new(partition_id, row_count, min, max, in_memory);
            let id_row = rocks_chunk.insert(chunk, batch_pipe)?;

            Ok(id_row)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self, chunks))]
    async fn insert_chunks(&self, chunks: Vec<Chunk>) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let rocks_chunk = ChunkRocksTable::new(db_ref.clone());
            let mut result = Vec::with_capacity(chunks.len());

            for chunk in chunks.into_iter() {
                let id_row = rocks_chunk.insert(chunk, batch_pipe)?;
                result.push(id_row);
            }

            Ok(result)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        self.read_operation(move |db_ref| {
            ChunkRocksTable::new(db_ref).get_row_or_not_found(chunk_id)
        })
        .await
    }
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_chunks_out_of_queue(&self, ids: Vec<u64>) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.read_operation_out_of_queue(move |db| {
            let db = ChunkRocksTable::new(db.clone());
            let mut res = Vec::with_capacity(ids.len());
            for id in ids.into_iter() {
                if let Some(chunk) = db.get_row(id)? {
                    res.push(chunk);
                }
            }
            Ok(res)
        })
        .await
    }
    async fn get_partitions_out_of_queue(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.read_operation_out_of_queue(move |db| {
            let db = PartitionRocksTable::new(db.clone());
            let mut res = Vec::with_capacity(ids.len());
            for id in ids.into_iter() {
                if let Some(partition) = db.get_row(id)? {
                    res.push(partition);
                }
            }
            Ok(res)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_chunks_by_partition(
        &self,
        partition_id: u64,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.read_operation(move |db| {
            Self::chunks_by_partition(partition_id, &ChunkRocksTable::new(db), include_inactive)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_chunks_by_partition_out_of_queue(
        &self,
        partition_id: u64,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.read_operation_out_of_queue(move |db| {
            Self::chunks_by_partition(partition_id, &ChunkRocksTable::new(db), include_inactive)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn chunk_uploaded(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = ChunkRocksTable::new(db_ref.clone());
            let row = table.get_row_or_not_found(chunk_id)?;
            let id_row = table.update(
                chunk_id,
                row.get_row().set_uploaded(true),
                row.get_row(),
                batch_pipe,
            )?;

            Ok(id_row)
        })
        .await
    }
    #[tracing::instrument(level = "trace", skip(self))]
    async fn chunk_update_last_inserted(
        &self,
        chunk_ids: Vec<u64>,
        last_inserted_at: Option<DateTime<Utc>>,
    ) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = ChunkRocksTable::new(db_ref.clone());
            for chunk_id in chunk_ids {
                let row = table.get_row_or_not_found(chunk_id)?;
                table.update(
                    chunk_id,
                    row.get_row().set_oldest_insert_at(last_inserted_at),
                    row.get_row(),
                    batch_pipe,
                )?;
            }

            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn deactivate_chunk(&self, chunk_id: u64) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            ChunkRocksTable::new(db_ref.clone()).update_with_fn(
                chunk_id,
                |row| row.deactivate(),
                batch_pipe,
            )?;
            Ok(())
        })
        .await
    }
    #[tracing::instrument(level = "trace", skip(self))]
    async fn deactivate_chunks(&self, chunk_ids: Vec<u64>) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = ChunkRocksTable::new(db_ref.clone());
            for chunk_id in chunk_ids {
                table.update_with_fn(chunk_id, |row| row.deactivate(), batch_pipe)?;
            }
            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn activate_chunks(
        &self,
        table_id: u64,
        uploaded_chunk_ids: Vec<(u64, Option<u64>)>,
        replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        trace!(
            "Activating chunks ({})",
            uploaded_chunk_ids.iter().map(|(id, _)| id).join(", ")
        );
        self.write_operation(move |db, pipe| {
            TableRocksTable::new(db.clone()).update_with_fn(
                table_id,
                |t| t.update_has_data(true),
                pipe,
            )?;
            let (_, partition_rows) = Self::activate_chunks_impl(
                db.clone(),
                pipe,
                &uploaded_chunk_ids,
                replay_handle_id,
            )?;
            let partition = PartitionRocksTable::new(db.clone());
            let mut mpartition_rows = HashMap::new();
            for (p, rows) in partition_rows {
                if let Some(mp) = partition.get_row_or_not_found(p)?.row.multi_partition_id {
                    *mpartition_rows.entry(mp).or_default() += rows;
                }
            }
            let mpartition = MultiPartitionRocksTable::new(db.clone());
            for (mp, rows) in mpartition_rows {
                mpartition.update_with_fn(mp, |p| p.add_rows(rows), pipe)?;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn swap_chunks(
        &self,
        deactivate_ids: Vec<u64>,
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        if uploaded_ids_and_sizes.is_empty() {
            return Err(CubeError::internal(format!(
                "Can't swap chunks: {:?} to {:?} empty",
                deactivate_ids, uploaded_ids_and_sizes
            )));
        }
        self.write_operation(move |db_ref, batch_pipe| {
            RocksMetaStore::swap_chunks_impl(
                deactivate_ids,
                uploaded_ids_and_sizes,
                db_ref,
                batch_pipe,
                true,
                new_replay_handle_id,
            )
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn swap_chunks_without_check(
        &self,
        deactivate_ids: Vec<u64>,
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        if uploaded_ids_and_sizes.is_empty() {
            return Err(CubeError::internal(format!(
                "Can't swap chunks: {:?} to {:?} empty",
                deactivate_ids, uploaded_ids_and_sizes
            )));
        }
        self.write_operation(move |db_ref, batch_pipe| {
            RocksMetaStore::swap_chunks_impl(
                deactivate_ids,
                uploaded_ids_and_sizes,
                db_ref,
                batch_pipe,
                false,
                new_replay_handle_id,
            )
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn deactivate_chunks_without_check(
        &self,
        deactivate_ids: Vec<u64>,
    ) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            RocksMetaStore::swap_chunks_impl(
                deactivate_ids,
                Vec::new(),
                db_ref,
                batch_pipe,
                false,
                None,
            )
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let chunks = ChunkRocksTable::new(db_ref.clone());
            let chunk = chunks.get_row_or_not_found(chunk_id)?;

            if chunk.get_row().active() {
                return Err(CubeError::internal(format!(
                    "Can't remove active chunk #{}. It should be deactivated first",
                    chunk_id
                )));
            }
            Ok(chunks.delete(chunk_id, batch_pipe)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_chunks_without_checks(&self, chunk_ids: Vec<u64>) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let chunks = ChunkRocksTable::new(db_ref.clone());
            for id in chunk_ids {
                chunks.delete(id, batch_pipe)?;
            }

            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn all_inactive_chunks(&self) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let table = ChunkRocksTable::new(db_ref);
            let mut res = Vec::new();
            for c in table.scan_all_rows()? {
                let c = c?;
                if !c.get_row().active() && c.get_row().uploaded() {
                    res.push(c);
                }
            }
            Ok(res)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn all_inactive_not_uploaded_chunks(&self) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let table = ChunkRocksTable::new(db_ref);

            let mut res = Vec::new();

            for c in table.scan_all_rows()? {
                let c = c?;
                if !c.get_row().active() && !c.get_row().uploaded() {
                    res.push(c);
                }
            }

            Ok(res)
        })
        .await
    }

    fn chunks_table(&self) -> ChunkMetaStoreTable {
        ChunkMetaStoreTable {
            rocks_meta_store: self.store.clone(),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_wal(&self, table_id: u64, row_count: usize) -> Result<IdRow<WAL>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let rocks_wal = WALRocksTable::new(db_ref.clone());
            TableRocksTable::new(db_ref.clone()).update_with_fn(
                table_id,
                |t| t.update_has_data(true),
                batch_pipe,
            )?;

            let wal = WAL::new(table_id, row_count);
            let id_row = rocks_wal.insert(wal, batch_pipe)?;

            Ok(id_row)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_wal(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError> {
        self.read_operation(move |db_ref| WALRocksTable::new(db_ref).get_row_or_not_found(wal_id))
            .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_wals_for_table(&self, table_id: u64) -> Result<Vec<IdRow<WAL>>, CubeError> {
        self.read_operation(move |db_ref| {
            WALRocksTable::new(db_ref)
                .get_rows_by_index(&WALIndexKey::ByTable(table_id), &WALRocksIndex::TableID)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_wal(&self, wal_id: u64) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            WALRocksTable::new(db_ref.clone()).delete(wal_id, batch_pipe)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn wal_uploaded(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = WALRocksTable::new(db_ref.clone());
            let row = table.get_row_or_not_found(wal_id)?;
            let id_row = table.update(
                wal_id,
                row.get_row().set_uploaded(true),
                row.get_row(),
                batch_pipe,
            )?;

            Ok(id_row)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| Ok(JobRocksTable::new(db_ref).all_rows()?))
            .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn add_job(&self, job: Job) -> Result<Option<IdRow<Job>>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = JobRocksTable::new(db_ref.clone());

            let result = table.get_row_ids_by_index(
                &JobIndexKey::RowReference(job.row_reference().clone(), job.job_type().clone()),
                &JobRocksIndex::RowReference,
            )?;
            if result.len() > 0 {
                return Ok(None);
            }

            let id_row = table.insert(job, batch_pipe)?;

            Ok(Some(id_row))
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        self.read_operation(move |db_ref| {
            Ok(JobRocksTable::new(db_ref).get_row_or_not_found(job_id)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_job_by_ref(
        &self,
        row_reference: RowKey,
        job_type: JobType,
    ) -> Result<Option<IdRow<Job>>, CubeError> {
        self.read_operation(move |db_ref| {
            let jobs_table = JobRocksTable::new(db_ref);
            let result = jobs_table.get_rows_by_index(
                &JobIndexKey::RowReference(row_reference, job_type),
                &JobRocksIndex::RowReference,
            )?;
            Ok(result.into_iter().next())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_orphaned_jobs(
        &self,
        orphaned_timeout: Duration,
    ) -> Result<Vec<IdRow<Job>>, CubeError> {
        let duration = chrono::Duration::from_std(orphaned_timeout).unwrap();
        self.read_operation_out_of_queue(move |db_ref| {
            let jobs_table = JobRocksTable::new(db_ref);
            let time = Utc::now();
            let all_jobs = jobs_table
                .all_rows()?
                .into_iter()
                .filter(|j| {
                    if let JobStatus::Scheduled(_) = j.get_row().status() {
                        return false;
                    }
                    let duration1 =
                        time.signed_duration_since(j.get_row().last_heart_beat().clone());
                    duration1 > duration
                })
                .collect::<Vec<_>>();
            Ok(all_jobs)
        })
        .await
    }

    async fn get_jobs_on_non_exists_nodes(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        let workers = if self.store.config.select_workers().is_empty() {
            vec![self.store.config.server_name().clone()]
        } else {
            self.store.config.select_workers().clone()
        };
        let nodes = workers
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>();
        self.read_operation_out_of_queue(move |db_ref| {
            let jobs_table = JobRocksTable::new(db_ref);
            let all_jobs = jobs_table
                .all_rows()?
                .into_iter()
                .filter(|j| match j.get_row().status() {
                    JobStatus::Scheduled(node) | JobStatus::ProcessingBy(node) => {
                        !nodes.contains(node)
                    }
                    _ => false,
                })
                .collect::<Vec<_>>();
            Ok(all_jobs)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            Ok(JobRocksTable::new(db_ref.clone()).delete(job_id, batch_pipe)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn start_processing_job(
        &self,
        server_name: String,
        long_term: bool,
    ) -> Result<Option<IdRow<Job>>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = JobRocksTable::new(db_ref);
            let next_job = table
                .get_rows_by_index(
                    &JobIndexKey::ScheduledByShard(Some(server_name.to_string())),
                    &JobRocksIndex::ByShard,
                )?
                .into_iter()
                .filter(|j| j.get_row().is_long_term() == long_term)
                //We use min_by instead of the max_by because of min_by returns the first element
                //if priority is equal while max_by returns the last element
                .min_by(|a, b| b.get_row().priority().cmp(&a.get_row().priority()));

            if let Some(job) = next_job {
                if let JobStatus::ProcessingBy(node) = job.get_row().status() {
                    return Err(CubeError::internal(format!(
                        "Job {:?} is already processing by {}",
                        job, node
                    )));
                }
                Ok(Some(table.update_with_fn(
                    job.get_id(),
                    |row| row.start_processing(server_name),
                    batch_pipe,
                )?))
            } else {
                Ok(None)
            }
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn update_heart_beat(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            Ok(JobRocksTable::new(db_ref).update_with_fn(
                job_id,
                |row| row.update_heart_beat(),
                batch_pipe,
            )?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn update_status(&self, job_id: u64, status: JobStatus) -> Result<IdRow<Job>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            Ok(JobRocksTable::new(db_ref).update_with_fn(
                job_id,
                |row| row.update_status(status),
                batch_pipe,
            )?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let jobs_table = JobRocksTable::new(db_ref);
            let all_jobs = jobs_table.all_rows()?;
            for job in all_jobs.iter() {
                jobs_table.delete(job.get_id(), batch_pipe)?;
            }
            Ok(all_jobs)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self, credentials))]
    async fn create_or_update_source(
        &self,
        name: String,
        credentials: SourceCredentials,
    ) -> Result<IdRow<Source>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = SourceRocksTable::new(db_ref.clone());

            let source = Source::new(name.to_string(), credentials);

            let row = table.get_single_row_by_index(
                &SourceIndexKey::Name(name.to_string()),
                &SourceRocksIndex::Name,
            );

            if row.is_err() {
                let id_row = table.insert(source, batch_pipe)?;
                Ok(id_row)
            } else {
                let updated = table.update_with_fn(row?.get_id(), |_| source, batch_pipe)?;
                Ok(updated)
            }
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_source(&self, id: u64) -> Result<IdRow<Source>, CubeError> {
        self.read_operation(move |db_ref| {
            Ok(SourceRocksTable::new(db_ref).get_row_or_not_found(id)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_source_by_name(&self, name: String) -> Result<IdRow<Source>, CubeError> {
        self.read_operation(move |db_ref| {
            Ok(SourceRocksTable::new(db_ref)
                .get_single_row_by_index(&SourceIndexKey::Name(name), &SourceRocksIndex::Name)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_source(&self, id: u64) -> Result<IdRow<Source>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            Ok(SourceRocksTable::new(db_ref.clone()).delete(id, batch_pipe)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_replay_handle(
        &self,
        table_id: u64,
        location_index: usize,
        seq_pointer: SeqPointer,
    ) -> Result<IdRow<ReplayHandle>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let tables_table = TableRocksTable::new(db_ref.clone());
            let table = tables_table.get_row_or_not_found(table_id)?;
            let handle = ReplayHandle::new(&table, location_index, seq_pointer)?;
            Ok(ReplayHandleRocksTable::new(db_ref.clone()).insert(handle, batch_pipe)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self, seq_pointers))]
    async fn create_replay_handle_from_seq_pointers(
        &self,
        table_id: u64,
        seq_pointers: Option<Vec<Option<SeqPointer>>>,
    ) -> Result<IdRow<ReplayHandle>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let handle = ReplayHandle::new_from_seq_pointers(table_id, seq_pointers);
            Ok(ReplayHandleRocksTable::new(db_ref.clone()).insert(handle, batch_pipe)?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_replay_handles_by_table(
        &self,
        table_id: u64,
    ) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            Ok(ReplayHandleRocksTable::new(db_ref).get_rows_by_index(
                &ReplayHandleIndexKey::ByTableId(table_id),
                &ReplayHandleRocksIndex::ByTableId,
            )?)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_replay_handles_by_ids(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        self.read_operation(move |db_ref| {
            let table = ReplayHandleRocksTable::new(db_ref);
            let rows = ids
                .iter()
                .map(|id| table.get_row(*id))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter_map(|v| v)
                .collect::<Vec<_>>();
            Ok(rows)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn update_replay_handle_failed_if_exists(
        &self,
        id: u64,
        failed: bool,
    ) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = ReplayHandleRocksTable::new(db_ref.clone());
            if table.get_row(id)?.is_some() {
                table.update_with_fn(id, |h| h.set_failed_to_persist_chunks(failed), batch_pipe)?;
            }

            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn replace_replay_handles(
        &self,
        old_ids: Vec<u64>,
        new_seq_pointer: Option<Vec<Option<SeqPointer>>>,
    ) -> Result<Option<IdRow<ReplayHandle>>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            if old_ids.is_empty() {
                return Err(CubeError::internal("Can't merge empty replay handles list".to_string()));
            }
            let table = ReplayHandleRocksTable::new(db_ref.clone());
            let chunks_table = ChunkRocksTable::new(db_ref);
            let mut replay_handles: Vec<IdRow<ReplayHandle>> = Vec::new();
            for id in old_ids.into_iter() {
                let replay_handle = table.get_row_or_not_found(id)?;

                let chunks = chunks_table.get_rows_by_index(
                    &ChunkIndexKey::ByReplayHandleId(Some(replay_handle.get_id())),
                    &ChunkRocksIndex::ReplayHandleId,
                )?;

                let active_chunks = chunks.iter().filter(|c| c.get_row().active() || !c.get_row().uploaded()).collect::<Vec<_>>();
                if !active_chunks.is_empty() {
                    return Err(CubeError::internal(format!(
                        "Can't merge replay handle with chunks: {:?}, {}",
                        replay_handle, active_chunks[0].get_id()
                    )))
                }

                if !replay_handles.is_empty()
                    && replay_handles[0].get_row().table_id()
                        != replay_handle
                            .get_row()
                            .table_id()
                {
                    return Err(CubeError::internal(format!(
                        "All replay handles should reference one table but two found: {:?} and {:?}",
                        replay_handles[0].get_row(), replay_handle
                    )))
                }
                replay_handles.push(replay_handle);
            }
            let new_handle = if let Some(_) = new_seq_pointer {
                let new_replay_handle = ReplayHandle::new_from_seq_pointers(replay_handles[0].get_row().table_id(), new_seq_pointer);
                Some(table.insert(new_replay_handle, batch_pipe)?)

            } else {
                None
            };

            for handle in replay_handles.iter() {
                table.delete(handle.get_id(), batch_pipe)?;
            }

            Ok(new_handle)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn all_replay_handles(&self) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        self.read_operation(move |db_ref| ReplayHandleRocksTable::new(db_ref).all_rows())
            .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn all_replay_handles_to_merge(
        &self,
    ) -> Result<Vec<(IdRow<ReplayHandle>, bool)>, CubeError> {
        self.read_operation_out_of_queue(move |db_ref| {
            let all_replay_handles = ReplayHandleRocksTable::new(db_ref.clone()).all_rows()?;
            let chunks_table = ChunkRocksTable::new(db_ref);
            let mut result = Vec::new();
            for replay_handle in all_replay_handles.into_iter() {
                let chunks = chunks_table.get_rows_by_index(
                    &ChunkIndexKey::ByReplayHandleId(Some(replay_handle.get_id())),
                    &ChunkRocksIndex::ReplayHandleId,
                )?;
                result.push((
                    replay_handle,
                    chunks
                        .iter()
                        .all(|c| !c.get_row().active() && c.get_row().uploaded()),
                ));
            }
            Ok(result)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_tables_with_indexes(
        &self,
        table_name: Vec<(String, String)>,
    ) -> Result<Vec<(IdRow<Schema>, IdRow<Table>, Vec<IdRow<Index>>)>, CubeError> {
        self.read_operation_out_of_queue(|db| {
            let mut r = Vec::with_capacity(table_name.len());
            for (schema, table) in table_name {
                let table = get_table_impl(db.clone(), schema, table)?;
                let schema = SchemaRocksTable::new(db.clone())
                    .get_row_or_not_found(table.get_row().get_schema_id())?;

                let mut indexes;
                indexes = IndexRocksTable::new(db.clone()).get_rows_by_index(
                    &IndexIndexKey::TableId(table.get_id()),
                    &IndexRocksIndex::TableID,
                )?;
                indexes.insert(0, get_default_index_impl(db.clone(), table.get_id())?);

                r.push((schema, table, indexes))
            }
            Ok(r)
        })
        .await
    }

    async fn debug_dump(&self, out_path: String) -> Result<(), CubeError> {
        self.read_operation(|db| {
            let opts = BackupEngineOptions::new(out_path)?;
            let mut e = BackupEngine::open(&opts, &Env::new()?)?;
            Ok(e.create_new_backup_flush(db.db, true)?)
        })
        .await
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, _batch_pipe| {
            let start: Option<&[u8]> = None;
            let end: Option<&[u8]> = None;

            db_ref.db.compact_range(start, end);

            Ok(())
        })
        .await?;

        Ok(())
    }

    async fn healthcheck(&self) -> Result<(), CubeError> {
        self.store.healthcheck().await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_multi_partition(&self, id: u64) -> Result<IdRow<MultiPartition>, CubeError> {
        self.read_operation(move |db| MultiPartitionRocksTable::new(db).get_row_or_not_found(id))
            .await
    }
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_child_multi_partitions(
        &self,
        id: u64,
    ) -> Result<Vec<IdRow<MultiPartition>>, CubeError> {
        self.read_operation(move |db| {
            MultiPartitionRocksTable::new(db).get_rows_by_index(
                &MultiPartitionIndexKey::ByParentId(Some(id)),
                &MultiPartitionRocksIndex::ByParentId,
            )
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_multi_partition_subtree(
        &self,
        multi_part_ids: Vec<u64>,
    ) -> Result<HashMap<u64, MultiPartition>, CubeError> {
        self.read_operation_out_of_queue(move |db| {
            let table = MultiPartitionRocksTable::new(db);
            let mut r = HashMap::new();
            for m in multi_part_ids {
                let mut curr = m;
                loop {
                    let e = match r.entry(m) {
                        Entry::Occupied(_) => break,
                        Entry::Vacant(e) => e,
                    };

                    let row = table.get_row_or_not_found(curr)?;
                    let parent = row.row.parent_multi_partition_id();

                    e.insert(row.row);
                    curr = match parent {
                        Some(parent) => parent,
                        None => break,
                    };
                }
            }
            Ok(r)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_multi_partition(
        &self,
        p: MultiPartition,
    ) -> Result<IdRow<MultiPartition>, CubeError> {
        self.write_operation(move |db, pipe| MultiPartitionRocksTable::new(db).insert(p, pipe))
            .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn prepare_multi_partition_for_split(
        &self,
        multi_partition_id: u64,
    ) -> Result<(IdRow<MultiIndex>, IdRow<MultiPartition>, Vec<PartitionData>), CubeError> {
        let (mi, mp, pds) = self
            .read_operation(move |db| {
                let mindex = MultiIndexRocksTable::new(db.clone());
                let mpartition = MultiPartitionRocksTable::new(db.clone());
                let index = IndexRocksTable::new(db.clone());
                let partition = PartitionRocksTable::new(db.clone());
                let chunk = ChunkRocksTable::new(db.clone());

                let mp = mpartition.get_row_or_not_found(multi_partition_id)?;
                if !mp.row.active() {
                    return Err(CubeError::internal(
                        "refusing to split inactive multi-partition".to_string(),
                    ));
                }
                let mi = mindex.get_row_or_not_found(mp.row.multi_index_id())?;
                let ps = partition.get_rows_by_index(
                    &PartitionIndexKey::ByMultiPartitionId(Some(multi_partition_id)),
                    &PartitionRocksIndex::MultiPartitionId,
                )?;

                let mut pds = Vec::with_capacity(ps.len());
                for partition in ps {
                    let index = index.get_row_or_not_found(partition.row.get_index_id())?;
                    let mut chunks = chunk.get_rows_by_index(
                        &ChunkIndexKey::ByPartitionId(partition.get_id()),
                        &ChunkRocksIndex::PartitionId,
                    )?;
                    chunks.retain(|c| c.row.active);
                    pds.push(PartitionData {
                        partition,
                        index,
                        chunks,
                    })
                }
                Ok((mi, mp, pds))
            })
            .await?;

        // We try to keep the write operation small.
        self.write_operation(move |db, pipe| {
            MultiPartitionRocksTable::new(db).update_with_fn(
                mp.get_id(),
                |p| p.mark_prepared_for_split(),
                pipe,
            )?;
            Ok((mi, mp, pds))
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn commit_multi_partition_split(
        &self,
        multi_partition_id: u64,
        new_multi_partitions: Vec<u64>,
        mut new_multi_partition_rows: Vec<u64>,
        old_partitions: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
        new_partitions: Vec<(IdRow<Partition>, u64)>,
        new_partition_rows: Vec<u64>,
        initial_split: bool,
    ) -> Result<(), CubeError> {
        assert_eq!(new_multi_partitions.len(), new_multi_partition_rows.len());
        assert_eq!(new_partition_rows.len(), new_partitions.len());
        assert!(new_multi_partitions.is_sorted());
        self.write_operation(move |db, pipe| {
            log::trace!(
                "Committing {} split of multi-partition {} to {:?}. (preliminary counts) {} rows split into {:?}",
                if initial_split { "initial" }  else {"postponed"},
                multi_partition_id,
                new_multi_partitions,
                new_multi_partition_rows.iter().sum::<u64>(),
                new_multi_partition_rows
            );
            swap_active_partitions_impl(
                db.clone(),
                pipe,
                &old_partitions,
                &new_partitions,
                |i, p| p.update_row_count(new_partition_rows[i]),
                |_| Ok(()), // Concurrent 'DROP TABLE' might remove some partitions. That's ok.
                |new_i| {
                    let mi = new_multi_partitions.binary_search(
                        &new_partitions[new_i].0.row.multi_partition_id.unwrap())
                        .expect("could not find multi-partition");
                    assert!(new_partition_rows[new_i] <= new_multi_partition_rows[mi] ,
                            "{} <= {}", new_partition_rows[new_i], new_multi_partition_rows[mi]);
                    new_multi_partition_rows[mi] -= new_partition_rows[new_i];
                    Ok(())
                }
            )?;

            let total_new_rows = new_multi_partition_rows.iter().sum();
            log::trace!(
                "Committing {} split of multi-partition {} to {:?}. (final counts) {} rows split into {:?}",
                if initial_split { "initial" }  else {"postponed"},
                multi_partition_id,
                new_multi_partitions,
                total_new_rows,
                new_multi_partition_rows
            );
            let mpartitions = MultiPartitionRocksTable::new(db);
            mpartitions.update_with_fn(
                multi_partition_id,
                |p| {
                    if initial_split {
                        assert!(p.active(), "refusing to commit split of inactive multi-partition");
                        p.set_active(false).subtract_rows(total_new_rows)
                    } else {
                        assert!(!p.active(), "active multi-partition during postponed split");
                        p.subtract_rows(total_new_rows)
                    }
                },
                pipe,
            )?;
            for i in 0..new_multi_partitions.len() {
                mpartitions.update_with_fn(
                    new_multi_partitions[i],
                    |p| {
                        assert_eq!(p.parent_multi_partition_id(), Some(multi_partition_id));
                        if initial_split {
                            assert!(!p.active(), "new multi-partition active on initial split");
                            p.set_active(true).add_rows(new_multi_partition_rows[i])
                        } else {
                            p.add_rows(new_multi_partition_rows[i])
                        }
                    },
                    pipe,
                )?;
            }
            Ok(())
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn find_unsplit_partitions(
        &self,
        multi_partition_id: u64,
    ) -> Result<Vec<u64>, CubeError> {
        self.read_operation(move |db| {
            let mparts = MultiPartitionRocksTable::new(db.clone());
            if mparts
                .get_row_or_not_found(multi_partition_id)?
                .row
                .active()
            {
                return Ok(Vec::new());
            }
            let mut mchildren = mparts.get_rows_by_index(
                &MultiPartitionIndexKey::ByParentId(Some(multi_partition_id)),
                &MultiPartitionRocksIndex::ByParentId,
            )?;
            // Some children might be leftovers from errors.
            mchildren.retain(|m| m.row.was_activated());

            let parts = PartitionRocksTable::new(db.clone());
            let mut with_children = HashSet::new();
            for c in mchildren {
                let c = c.id;
                let new_parts = parts.get_rows_by_index(
                    &PartitionIndexKey::ByMultiPartitionId(Some(c)),
                    &PartitionRocksIndex::MultiPartitionId,
                )?;
                for p in new_parts {
                    with_children.insert(p.row.parent_partition_id.unwrap());
                }
            }
            let mut ps = parts.get_row_ids_by_index(
                &PartitionIndexKey::ByMultiPartitionId(Some(multi_partition_id)),
                &PartitionRocksIndex::MultiPartitionId,
            )?;
            ps.retain(|p| !with_children.contains(p));
            Ok(ps)
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn prepare_multi_split_finish(
        &self,
        multi_partition_id: u64,
        partition_id: u64,
    ) -> Result<(PartitionData, Vec<IdRow<MultiPartition>>), CubeError> {
        self.read_operation(move |db| {
            log::trace!(
                "Preparing to finish split of {} (partition {})",
                multi_partition_id,
                partition_id
            );
            let mpartitions = MultiPartitionRocksTable::new(db.clone());
            assert!(
                !mpartitions
                    .get_row_or_not_found(multi_partition_id)?
                    .row
                    .active(),
                "attempting to split active multi-partition"
            );
            let mut children = mpartitions.get_rows_by_index(
                &MultiPartitionIndexKey::ByParentId(Some(multi_partition_id)),
                &MultiPartitionRocksIndex::ByParentId,
            )?;
            children.retain(|c| c.row.was_activated());

            let partitions = PartitionRocksTable::new(db.clone());
            let partition = partitions.get_row_or_not_found(partition_id)?;

            let indexes = IndexRocksTable::new(db.clone());
            let index = indexes.get_row_or_not_found(partition.row.index_id)?;

            let chunks = ChunkRocksTable::new(db.clone());
            let mut chunks = chunks.get_rows_by_index(
                &ChunkIndexKey::ByPartitionId(partition_id),
                &ChunkRocksIndex::PartitionId,
            )?;
            chunks.retain(|c| c.row.active);

            let d = PartitionData {
                partition,
                index,
                chunks,
            };
            Ok((d, children))
        })
        .await
    }
    async fn get_snapshots_list(&self) -> Result<Vec<SnapshotInfo>, CubeError> {
        self.store.get_snapshots_list().await
    }
    async fn set_current_snapshot(&self, snapshot_id: u128) -> Result<(), CubeError> {
        self.store.set_current_snapshot(snapshot_id).await
    }
}

pub async fn deactivate_table_on_corrupt_data<'a, T: 'static>(
    meta_store: Arc<dyn MetaStore>,
    e: &'a Result<T, CubeError>,
    partition: &'a IdRow<Partition>,
    chunk_id: Option<u64>,
) {
    if let Err(e) =
        deactivate_table_on_corrupt_data_res::<T>(meta_store, e, partition, chunk_id).await
    {
        log::error!("Error during deactivation of table on corrupt data: {}", e);
    }
}

pub async fn deactivate_table_on_corrupt_data_res<'a, T: 'static>(
    meta_store: Arc<dyn MetaStore>,
    result: &'a Result<T, CubeError>,
    partition: &'a IdRow<Partition>,
    chunk_id: Option<u64>,
) -> Result<(), CubeError> {
    if let Err(e) = &result {
        if e.is_corrupt_data() {
            //Firstly check if chunk and partition exists in metastore now, because they could have been deleted due to compaction and similar things
            if let Some(chunk_id) = chunk_id {
                match meta_store.get_chunk(chunk_id).await {
                    Ok(_) => {}
                    Err(_) => {
                        log::info!(
                            "Chunk {} is no longer in metastore so deactivation is not required",
                            chunk_id
                        );
                        return Ok(());
                    }
                };
            } else {
                match meta_store.get_partition(partition.get_id()).await {
                    Ok(_) => {}
                    Err(_) => {
                        log::info!(
                            "Partition {} is no longer in metastore so deactivation is not required",
                            partition.get_id()
                        );
                        return Ok(());
                    }
                };
            }
            let table_id = meta_store
                .get_index(partition.get_row().get_index_id())
                .await?
                .get_row()
                .table_id();
            let message = e.message.to_string();
            deactivate_table_due_to_corrupt_data(meta_store, table_id, message).await?;
        }
    }
    Ok(())
}

pub async fn deactivate_table_due_to_corrupt_data(
    meta_store: Arc<dyn MetaStore>,
    table_id: u64,
    message: String,
) -> Result<(), CubeError> {
    let table = meta_store.get_table_by_id(table_id).await?;
    let schema = meta_store
        .get_schema_by_id(table.get_row().get_schema_id())
        .await?;
    info!(
        "Deactivating table {}.{} (#{}) due to corrupt data error: {}",
        schema.get_row().get_name(),
        table.get_row().get_table_name(),
        table_id,
        message,
    );
    meta_store.table_ready(table_id, false).await?;
    Ok(())
}

fn get_table_impl(
    db_ref: DbTableRef,
    schema_name: String,
    table_name: String,
) -> Result<IdRow<Table>, CubeError> {
    let rocks_table = TableRocksTable::new(db_ref.clone());
    let rocks_schema = SchemaRocksTable::new(db_ref);
    let table =
        RocksMetaStore::get_table_by_name(schema_name, table_name, rocks_table, rocks_schema)?;
    Ok(table)
}

fn get_default_index_impl(db_ref: DbTableRef, table_id: u64) -> Result<IdRow<Index>, CubeError> {
    let index = IndexRocksTable::new(db_ref);
    let indexes = index.get_rows_by_index(
        &IndexIndexKey::Name(table_id, "default".to_string()),
        &IndexRocksIndex::Name,
    )?;
    indexes
        .into_iter()
        .nth(0)
        .ok_or(CubeError::internal(format!(
            "Missing default index for table {}",
            table_id
        )))
}

/// Note that [current_active] and [new_active] are snapshots at some older point in time. The
/// relevant partitions might be dropped or changed by the time this function runs. Implementation
/// must take great care to avoid inconsistencies caused by this.
fn swap_active_partitions_impl(
    db_ref: DbTableRef,
    batch_pipe: &mut BatchPipe,
    current_active: &[(IdRow<Partition>, Vec<IdRow<Chunk>>)],
    new_active: &[(IdRow<Partition>, u64)],
    mut update_new_partition_stats: impl FnMut(/*index*/ usize, &Partition) -> Partition,
    mut on_dropped_current_partition: impl FnMut(/*index*/ usize) -> Result<(), CubeError>,
    mut on_dropped_new_partition: impl FnMut(/*index*/ usize) -> Result<(), CubeError>,
) -> Result<(), CubeError> {
    let index_table = IndexRocksTable::new(db_ref.clone());
    let table_table = TableRocksTable::new(db_ref.clone());
    let table = PartitionRocksTable::new(db_ref.clone());
    let chunk_table = ChunkRocksTable::new(db_ref.clone());

    // Rows are compacted using unique key columns or aggregating index and totals don't match
    let skip_row_count_sanity_check = if let Some(current) = current_active.first() {
        let current_partition = table
            .get_row(current.0.get_id())?
            .ok_or(CubeError::internal(format!(
                "Current partition is not found during swap active: {}",
                current.0.get_id()
            )))?;
        let index = index_table.get_row_or_not_found(current_partition.get_row().get_index_id())?;
        let table = table_table.get_row_or_not_found(index.get_row().table_id())?;
        index.get_row().get_type() == IndexType::Aggregate
            || table.get_row().unique_key_columns().is_some()
    } else {
        false
    };

    let mut deactivated_row_count = 0;
    let mut activated_row_count = 0;

    let mut dropped_current = HashSet::new();
    for (current_i, (current, chunks)) in current_active.iter().enumerate() {
        let current_partition = match table.get_row(current.get_id())? {
            None => {
                on_dropped_current_partition(current_i)?;
                dropped_current.insert(current.get_id());
                continue;
            }
            Some(p) => p,
        };
        if !current_partition.get_row().is_active() {
            return Err(CubeError::internal(format!(
                "Current partition is not active: {:?}",
                current_partition.get_row()
            )));
        }
        table.update(
            current_partition.get_id(),
            current_partition.get_row().to_active(false),
            current_partition.get_row(),
            batch_pipe,
        )?;
        deactivated_row_count += current_partition.get_row().main_table_row_count();

        for chunk in chunks.iter() {
            let current_chunk = chunk_table.get_row_or_not_found(chunk.get_id())?;
            if !current_chunk.get_row().active() {
                return Err(CubeError::internal(format!(
                    "Current chunk is not active: {:?}",
                    chunk.get_row()
                )));
            }
            deactivated_row_count += current_chunk.get_row().get_row_count();
            chunk_table.update_with_fn(
                current_chunk.get_id(),
                |row| row.deactivate(),
                batch_pipe,
            )?;
        }
    }

    for i in 0..new_active.len() {
        let (new, new_file_size) = &new_active[i];
        if dropped_current.contains(&new.get_row().parent_partition_id.unwrap()) {
            on_dropped_new_partition(i)?;
            if let Err(e) = table.delete(new.get_id(), batch_pipe) {
                // This might happen during DROP TABLE.
                log::trace!(
                    "Failure when removing new partition, likely not an error: {}",
                    e.display_with_backtrace()
                );
            }
            continue;
        }
        let new_partition = table
            .get_row(new.get_id())?
            .ok_or(CubeError::internal(format!(
                "New partition is not found during swap active: {}",
                new.get_id()
            )))?;
        if new_partition.get_row().is_active() {
            return Err(CubeError::internal(format!(
                "New partition is already active: {:?}",
                new_partition.get_row()
            )));
        }
        let updated = update_new_partition_stats(i, new_partition.get_row())
            .to_active(true)
            .set_file_size(*new_file_size)?;
        activated_row_count += updated.main_table_row_count;
        table.update(
            new_partition.get_id(),
            updated,
            new_partition.get_row(),
            batch_pipe,
        )?;
    }

    // if it's chunk compaction only without split then just re-parent all chunks without going through repartition process
    if current_active.len() == 1
        && new_active.len() == 1
        && current_active[0].0.get_row().get_min_val() == new_active[0].0.get_row().get_min_val()
        && current_active[0].0.get_row().get_max_val() == new_active[0].0.get_row().get_max_val()
    {
        let chunks_to_repartition = chunk_table
            .get_rows_by_index(
                &ChunkIndexKey::ByPartitionId(current_active[0].0.get_id()),
                &ChunkRocksIndex::PartitionId,
            )?
            .into_iter()
            .filter(|c| {
                current_active[0]
                    .1
                    .iter()
                    .find(|oc| oc.get_id() == c.get_id())
                    .is_none()
            });
        for chunk in chunks_to_repartition {
            chunk_table.update_with_fn(
                chunk.get_id(),
                |c| c.set_partition_id(new_active[0].0.get_id()),
                batch_pipe,
            )?;
        }
    }

    if !skip_row_count_sanity_check && activated_row_count != deactivated_row_count {
        return Err(CubeError::internal(format!(
            "Deactivated row count ({}) doesn't match activated row count ({}) during swap of partition ({}) and ({}) chunks to new partitions ({})",
            deactivated_row_count,
            activated_row_count,
            current_active.iter().map(|(p,_)| p.get_id()).join(", "),
            current_active.iter().flat_map(|(_, cs)| cs).map(|c| c.get_id()).join(", "),
            new_active.iter().map(|p| p.0.get_id()).join(", ")
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::table::AggregateColumn;
    use super::*;
    use crate::config::Config;
    use crate::remotefs::{LocalDirRemoteFs, RemoteFs};
    use futures_timer::Delay;
    use rocksdb::IteratorMode;
    use std::thread::sleep;
    use std::time::Duration;
    use std::{env, fs};

    #[test]
    fn macro_test() {
        let s = Schema {
            name: "foo".to_string(),
        };
        assert_eq!(format_table_value!(s, name, String), "foo");
    }

    #[tokio::test]
    async fn schema_test() {
        let config = Config::test("schema_test");
        let store_path = env::current_dir().unwrap().join("test-local");
        let remote_store_path = env::current_dir().unwrap().join("test-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());

        {
            let meta_store = RocksMetaStore::new(
                store_path.join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();

            let schema_1 = meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            println!("New id: {}", schema_1.id);
            let schema_2 = meta_store
                .create_schema("bar".to_string(), false)
                .await
                .unwrap();
            println!("New id: {}", schema_2.id);
            let schema_3 = meta_store
                .create_schema("boo".to_string(), false)
                .await
                .unwrap();
            println!("New id: {}", schema_3.id);

            let schema_1_id = schema_1.id;
            let schema_2_id = schema_2.id;
            let schema_3_id = schema_3.id;

            assert!(meta_store
                .create_schema("foo".to_string(), false)
                .await
                .is_err());

            assert_eq!(
                meta_store.get_schema("foo".to_string()).await.unwrap(),
                schema_1
            );
            assert_eq!(
                meta_store.get_schema("bar".to_string()).await.unwrap(),
                schema_2
            );
            assert_eq!(
                meta_store.get_schema("boo".to_string()).await.unwrap(),
                schema_3
            );

            assert_eq!(
                meta_store.get_schema_by_id(schema_1_id).await.unwrap(),
                schema_1
            );
            assert_eq!(
                meta_store.get_schema_by_id(schema_2_id).await.unwrap(),
                schema_2
            );
            assert_eq!(
                meta_store.get_schema_by_id(schema_3_id).await.unwrap(),
                schema_3
            );

            assert_eq!(
                meta_store.get_schemas().await.unwrap(),
                vec![
                    IdRow::new(
                        1,
                        Schema {
                            name: "foo".to_string()
                        }
                    ),
                    IdRow::new(
                        2,
                        Schema {
                            name: "bar".to_string()
                        }
                    ),
                    IdRow::new(
                        3,
                        Schema {
                            name: "boo".to_string()
                        }
                    ),
                ]
            );

            assert_eq!(
                meta_store
                    .rename_schema("foo".to_string(), "foo1".to_string())
                    .await
                    .unwrap(),
                IdRow::new(
                    schema_1_id,
                    Schema {
                        name: "foo1".to_string()
                    }
                )
            );
            assert!(meta_store.get_schema("foo".to_string()).await.is_err());
            assert_eq!(
                meta_store.get_schema("foo1".to_string()).await.unwrap(),
                IdRow::new(
                    schema_1_id,
                    Schema {
                        name: "foo1".to_string()
                    }
                )
            );
            assert_eq!(
                meta_store.get_schema_by_id(schema_1_id).await.unwrap(),
                IdRow::new(
                    schema_1_id,
                    Schema {
                        name: "foo1".to_string()
                    }
                )
            );

            assert!(meta_store
                .rename_schema("boo1".to_string(), "foo1".to_string())
                .await
                .is_err());

            assert_eq!(
                meta_store
                    .rename_schema_by_id(schema_2_id, "bar1".to_string())
                    .await
                    .unwrap(),
                IdRow::new(
                    schema_2_id,
                    Schema {
                        name: "bar1".to_string()
                    }
                )
            );
            assert!(meta_store.get_schema("bar".to_string()).await.is_err());
            assert_eq!(
                meta_store.get_schema("bar1".to_string()).await.unwrap(),
                IdRow::new(
                    schema_2_id,
                    Schema {
                        name: "bar1".to_string()
                    }
                )
            );
            assert_eq!(
                meta_store.get_schema_by_id(schema_2_id).await.unwrap(),
                IdRow::new(
                    schema_2_id,
                    Schema {
                        name: "bar1".to_string()
                    }
                )
            );

            assert_eq!(
                meta_store.delete_schema("bar1".to_string()).await.unwrap(),
                ()
            );
            assert!(meta_store.delete_schema("bar1".to_string()).await.is_err());
            assert!(meta_store.delete_schema("bar".to_string()).await.is_err());

            assert!(meta_store.get_schema("bar1".to_string()).await.is_err());
            assert!(meta_store.get_schema("bar".to_string()).await.is_err());

            assert_eq!(
                meta_store.delete_schema_by_id(schema_3_id).await.unwrap(),
                ()
            );
            assert!(meta_store.delete_schema_by_id(schema_2_id).await.is_err());
            assert_eq!(
                meta_store.delete_schema_by_id(schema_1_id).await.unwrap(),
                ()
            );
            assert!(meta_store.delete_schema_by_id(schema_1_id).await.is_err());
            assert!(meta_store.get_schema("foo".to_string()).await.is_err());
            assert!(meta_store.get_schema("foo1".to_string()).await.is_err());
            assert!(meta_store.get_schema("boo".to_string()).await.is_err());
        }
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn non_empty_schema_test() {
        let config = Config::test("non_empty_schema_test");
        let store_path = env::current_dir().unwrap().join("test-local-ne-schema");
        let remote_store_path = env::current_dir().unwrap().join("test-remote-ne-schema");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());

        let meta_store = RocksMetaStore::new(
            store_path.join("metastore").as_path(),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
        )
        .unwrap();

        let schema1 = meta_store
            .create_schema("foo".to_string(), false)
            .await
            .unwrap();

        let _schema2 = meta_store
            .create_schema("foo2".to_string(), false)
            .await
            .unwrap();
        let mut columns = Vec::new();
        columns.push(Column::new("col1".to_string(), ColumnType::Int, 0));

        let _table1 = meta_store
            .create_table(
                "foo".to_string(),
                "boo".to_string(),
                columns.clone(),
                None,
                None,
                vec![],
                true,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let _table2 = meta_store
            .create_table(
                "foo2".to_string(),
                "boo".to_string(),
                columns.clone(),
                None,
                None,
                vec![],
                true,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(meta_store
            .delete_schema_by_id(schema1.get_id())
            .await
            .is_err());
        assert!(meta_store.delete_schema("foo2".to_string()).await.is_err());

        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn index_repair_test() {
        let config = Config::test("index_repair_test");
        let store_path = env::current_dir().unwrap().join("index_repair_test-local");
        let remote_store_path = env::current_dir().unwrap().join("index_repair_test-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());

        {
            let meta_store = RocksMetaStore::new(
                store_path.join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();

            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();

            meta_store
                .store
                .db
                .delete(RowKey::Table(TableId::Schemas, 1).to_bytes())
                .unwrap();

            let result = meta_store.get_schema("foo".to_string()).await;
            println!("{:?}", result);
            assert_eq!(result.is_err(), true);

            let iterator = meta_store.store.db.iterator(IteratorMode::Start);

            println!("Keys in db");
            for kv_res in iterator {
                let (key, _) = kv_res.unwrap();
                println!("Key {:?}", RowKey::from_bytes(&key));
            }

            sleep(Duration::from_millis(300));

            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
        }
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn table_test() {
        let config = Config::test("table_test");
        let store_path = env::current_dir().unwrap().join("test-table-local");
        let remote_store_path = env::current_dir().unwrap().join("test-table-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        {
            let meta_store = RocksMetaStore::new(
                store_path.clone().join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();

            let schema_1 = meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let mut columns = Vec::new();
            columns.push(Column::new("col1".to_string(), ColumnType::Int, 0));
            columns.push(Column::new("col2".to_string(), ColumnType::String, 1));
            columns.push(Column::new(
                "col3".to_string(),
                ColumnType::Decimal {
                    scale: 2,
                    precision: 18,
                },
                2,
            ));
            columns.push(Column::new("col4".to_string(), ColumnType::Bytes, 3));
            columns.push(Column::new(
                "col5".to_string(),
                ColumnType::HyperLogLog(HllFlavour::Airlift),
                4,
            ));

            let table1 = meta_store
                .create_table(
                    "foo".to_string(),
                    "boo".to_string(),
                    columns.clone(),
                    None,
                    None,
                    vec![],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let table1_id = table1.id;

            assert!(schema_1.id == table1.get_row().get_schema_id());
            assert!(meta_store
                .create_table(
                    "foo".to_string(),
                    "boo".to_string(),
                    columns.clone(),
                    None,
                    None,
                    vec![],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .is_err());

            assert_eq!(
                meta_store
                    .get_table("foo".to_string(), "boo".to_string())
                    .await
                    .unwrap(),
                table1
            );

            let expected_index = Index::try_new(
                "default".to_string(),
                table1_id,
                columns.clone(),
                columns.len() as u64 - 2,
                None,
                None,
                Index::index_type_default(),
            )
            .unwrap();
            let expected_res = vec![IdRow::new(1, expected_index)];
            assert_eq!(meta_store.get_table_indexes(1).await.unwrap(), expected_res);
        }
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }
    #[tokio::test]
    async fn default_index_field_positions_test() {
        let config = Config::test("default_index_field_positions_test");
        let store_path = env::current_dir()
            .unwrap()
            .join("test-default-index-positions-local");
        let remote_store_path = env::current_dir()
            .unwrap()
            .join("test-default-index-positions-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        {
            let meta_store = RocksMetaStore::new(
                store_path.clone().join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();

            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let mut columns = Vec::new();
            columns.push(Column::new("col1".to_string(), ColumnType::Int, 0));
            columns.push(Column::new("col2".to_string(), ColumnType::Bytes, 1));
            columns.push(Column::new(
                "col3".to_string(),
                ColumnType::HyperLogLog(HllFlavour::Airlift),
                2,
            ));
            columns.push(Column::new("col4".to_string(), ColumnType::String, 3));
            columns.push(Column::new(
                "col5".to_string(),
                ColumnType::Decimal {
                    scale: 2,
                    precision: 18,
                },
                4,
            ));

            let table1 = meta_store
                .create_table(
                    "foo".to_string(),
                    "boo".to_string(),
                    columns.clone(),
                    None,
                    None,
                    vec![],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let table1_id = table1.id;

            let expected_columns = vec![
                columns[0].clone(),
                columns[3].replace_index(1),
                columns[4].replace_index(2),
                columns[1].replace_index(3),
                columns[2].replace_index(4),
            ];

            let expected_index = Index::try_new(
                "default".to_string(),
                table1_id,
                expected_columns,
                columns.len() as u64 - 2,
                None,
                None,
                Index::index_type_default(),
            )
            .unwrap();
            let expected_res = vec![IdRow::new(1, expected_index)];
            assert_eq!(meta_store.get_table_indexes(1).await.unwrap(), expected_res);
        }
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn table_with_aggregate_index_test() {
        let config = Config::test("table_with_aggregate_index_test");
        let store_path = env::current_dir()
            .unwrap()
            .join("test-table-aggregate-local");
        let remote_store_path = env::current_dir()
            .unwrap()
            .join("test-table-aggregate-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        {
            let meta_store = RocksMetaStore::new(
                store_path.clone().join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();

            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let mut columns = Vec::new();
            columns.push(Column::new("col1".to_string(), ColumnType::Int, 0));
            columns.push(Column::new("col2".to_string(), ColumnType::String, 1));
            columns.push(Column::new("col3".to_string(), ColumnType::Int, 2));
            columns.push(Column::new("aggr_col1".to_string(), ColumnType::Int, 3));
            columns.push(Column::new("aggr_col2".to_string(), ColumnType::Int, 4));

            let aggr_index_def = IndexDef {
                name: "aggr_index".to_string(),
                columns: vec!["col2".to_string(), "col1".to_string()],
                multi_index: None,
                index_type: IndexType::Aggregate,
            };

            let table1 = meta_store
                .create_table(
                    "foo".to_string(),
                    "boo".to_string(),
                    columns.clone(),
                    None,
                    None,
                    vec![aggr_index_def.clone()],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(vec![
                        ("sum".to_string(), "aggr_col2".to_string()),
                        ("max".to_string(), "aggr_col1".to_string()),
                    ]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let table_id = table1.get_id();

            assert_eq!(
                meta_store
                    .get_table("foo".to_string(), "boo".to_string())
                    .await
                    .unwrap(),
                table1
            );

            let aggr_columns = table1.get_row().aggregate_columns();
            assert_eq!(
                aggr_columns[0],
                AggregateColumn::new(
                    Column::new("aggr_col2".to_string(), ColumnType::Int, 4),
                    AggregateFunction::SUM
                )
            );
            assert_eq!(
                aggr_columns[1],
                AggregateColumn::new(
                    Column::new("aggr_col1".to_string(), ColumnType::Int, 3),
                    AggregateFunction::MAX
                )
            );

            let indexes = meta_store.get_table_indexes(table_id).await.unwrap();
            assert_eq!(indexes.len(), 2);
            let ind = indexes
                .into_iter()
                .find(|ind| ind.get_row().get_name() == &aggr_index_def.name)
                .unwrap();

            let index = ind.get_row();
            assert!(match index.get_type() {
                IndexType::Aggregate => true,
                _ => false,
            });

            let expected_columns = vec![
                Column::new("col2".to_string(), ColumnType::String, 0),
                Column::new("col1".to_string(), ColumnType::Int, 1),
                Column::new("aggr_col2".to_string(), ColumnType::Int, 2),
                Column::new("aggr_col1".to_string(), ColumnType::Int, 3),
            ];
            assert_eq!(index.get_columns(), &expected_columns);

            assert!(meta_store
                .create_table(
                    "foo".to_string(),
                    "boo2".to_string(),
                    columns.clone(),
                    None,
                    None,
                    vec![aggr_index_def.clone()],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(vec!["col2".to_string(), "col1".to_string()]),
                    Some(vec![
                        ("sum".to_string(), "aggr_col2".to_string()),
                        ("max".to_string(), "col1".to_string()),
                    ]),
                    None,
                    None,
                )
                .await
                .is_err());

            assert!(meta_store
                .create_table(
                    "foo".to_string(),
                    "boo3".to_string(),
                    columns.clone(),
                    None,
                    None,
                    vec![aggr_index_def.clone()],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(vec!["col1".to_string()]),
                    None,
                    None,
                    None,
                )
                .await
                .is_err());

            assert!(meta_store
                .create_table(
                    "foo".to_string(),
                    "boo4".to_string(),
                    columns.clone(),
                    None,
                    None,
                    vec![aggr_index_def.clone()],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(vec!["col1".to_string()]),
                    Some(vec![
                        ("sum".to_string(), "aggr_col2".to_string()),
                        ("max".to_string(), "aggr_col1".to_string()),
                    ]),
                    None,
                    None,
                )
                .await
                .is_err());
        }
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn cold_start_test() {
        {
            let config = Config::test("cold_start_test");

            let _ = fs::remove_dir_all(config.local_dir());
            let _ = fs::remove_dir_all(config.remote_dir());

            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();
            services
                .meta_store
                .create_schema("foo1".to_string(), false)
                .await
                .unwrap();
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .run_upload()
                .await
                .unwrap();
            services
                .meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .upload_check_point()
                .await
                .unwrap();
            services
                .meta_store
                .create_schema("bar".to_string(), false)
                .await
                .unwrap();
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .run_upload()
                .await
                .unwrap();
            services.stop_processing_loops().await.unwrap();

            Delay::new(Duration::from_millis(1000)).await; // TODO logger init conflict
            fs::remove_dir_all(config.local_dir()).unwrap();
        }

        {
            let config = Config::test("cold_start_test");

            let services2 = config.configure().await;
            services2
                .meta_store
                .get_schema("foo1".to_string())
                .await
                .unwrap();
            services2
                .meta_store
                .get_schema("foo".to_string())
                .await
                .unwrap();
            services2
                .meta_store
                .get_schema("bar".to_string())
                .await
                .unwrap();
            fs::remove_dir_all(config.local_dir()).unwrap();
            fs::remove_dir_all(config.remote_dir()).unwrap();
        }
    }

    #[tokio::test]
    async fn get_snapshots_list() {
        {
            let config = Config::test("get_snapshots_list");

            let _ = fs::remove_dir_all(config.local_dir());
            let _ = fs::remove_dir_all(config.remote_dir());

            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();
            let snapshots = services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .get_snapshots_list()
                .await
                .unwrap();
            assert_eq!(snapshots.len(), 0);
            services
                .meta_store
                .create_schema("foo1".to_string(), false)
                .await
                .unwrap();
            assert_eq!(snapshots.len(), 0);
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .upload_check_point()
                .await
                .unwrap();
            let snapshots = services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .get_snapshots_list()
                .await
                .unwrap();
            assert_eq!(snapshots.len(), 1);
            assert!(snapshots[0].current);
            services
                .meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .upload_check_point()
                .await
                .unwrap();
            let snapshots = services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .get_snapshots_list()
                .await
                .unwrap();
            assert_eq!(snapshots.len(), 2);
            assert!(!snapshots[0].current);
            assert!(snapshots[1].current);
            services
                .meta_store
                .create_schema("bar".to_string(), false)
                .await
                .unwrap();
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .upload_check_point()
                .await
                .unwrap();
            let snapshots = services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .get_snapshots_list()
                .await
                .unwrap();
            assert_eq!(snapshots.len(), 3);
            assert!(!snapshots[0].current);
            assert!(!snapshots[1].current);
            assert!(snapshots[2].current);
            services.stop_processing_loops().await.unwrap();

            Delay::new(Duration::from_millis(1000)).await; // TODO logger init conflict
            fs::remove_dir_all(config.local_dir()).unwrap();
        }

        {
            let config = Config::test("get_snapshots_list");

            let services2 = config.configure().await;
            let snapshots = services2
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .get_snapshots_list()
                .await
                .unwrap();
            assert_eq!(snapshots.len(), 3);
            assert!(!snapshots[0].current);
            assert!(!snapshots[1].current);
            assert!(snapshots[2].current);
            fs::remove_dir_all(config.local_dir()).unwrap();
            fs::remove_dir_all(config.remote_dir()).unwrap();
        }
    }
    #[tokio::test]
    async fn set_current_snapshot() {
        {
            let config = Config::test("set_current_snapshot");

            let _ = fs::remove_dir_all(config.local_dir());
            let _ = fs::remove_dir_all(config.remote_dir());

            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();
            let rocks_meta_store = services.rocks_meta_store.as_ref().unwrap();
            services
                .meta_store
                .create_schema("foo1".to_string(), false)
                .await
                .unwrap();
            rocks_meta_store.upload_check_point().await.unwrap();
            services
                .meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            rocks_meta_store.upload_check_point().await.unwrap();
            services
                .meta_store
                .create_schema("bar".to_string(), false)
                .await
                .unwrap();
            rocks_meta_store.upload_check_point().await.unwrap();
            let snapshots = services.meta_store.get_snapshots_list().await.unwrap();
            assert_eq!(snapshots.len(), 3);
            assert!(!snapshots[0].current);
            assert!(!snapshots[1].current);
            assert!(snapshots[2].current);

            let res = services.meta_store.set_current_snapshot(111).await;
            assert_eq!(
                res.unwrap_err().to_string(),
                "Metastore snapshot with id 111 don't exists".to_string()
            );

            let res = services
                .meta_store
                .set_current_snapshot(snapshots[2].id)
                .await;
            assert_eq!(
                res.unwrap_err().to_string(),
                format!(
                    "Metastore snapshot with id {} is already current snapshot",
                    snapshots[2].id
                )
            );

            let res = services
                .meta_store
                .set_current_snapshot(snapshots[1].id)
                .await;
            assert!(res.is_ok());

            services
                .meta_store
                .create_schema("bar_after".to_string(), false)
                .await
                .unwrap();
            rocks_meta_store.upload_check_point().await.unwrap();
            rocks_meta_store.run_upload().await.unwrap();

            let snapshots = services.meta_store.get_snapshots_list().await.unwrap();
            assert_eq!(snapshots.len(), 3);
            assert!(!snapshots[0].current);
            assert!(snapshots[1].current);
            assert!(!snapshots[2].current);

            services.stop_processing_loops().await.unwrap();

            Delay::new(Duration::from_millis(1000)).await; // TODO logger init conflict
            fs::remove_dir_all(config.local_dir()).unwrap();
        }

        {
            let config = Config::test("set_current_snapshot");

            let services2 = config.configure().await;
            let snapshots = services2.meta_store.get_snapshots_list().await.unwrap();
            assert_eq!(snapshots.len(), 3);
            assert!(!snapshots[0].current);
            assert!(snapshots[1].current);
            assert!(!snapshots[2].current);
            services2
                .meta_store
                .get_schema("foo1".to_string())
                .await
                .unwrap();
            services2
                .meta_store
                .get_schema("foo".to_string())
                .await
                .unwrap();
            assert!(services2
                .meta_store
                .get_schema("bar".to_string())
                .await
                .is_err());
            assert!(services2
                .meta_store
                .get_schema("bar_after".to_string())
                .await
                .is_err());

            let res = services2
                .meta_store
                .set_current_snapshot(snapshots[2].id)
                .await;
            assert!(res.is_ok());
            Delay::new(Duration::from_millis(1000)).await; // TODO logger init conflict
            fs::remove_dir_all(config.local_dir()).unwrap();
        }

        {
            let config = Config::test("set_current_snapshot");

            let services3 = config.configure().await;
            let snapshots = services3.meta_store.get_snapshots_list().await.unwrap();
            assert_eq!(snapshots.len(), 3);
            assert!(!snapshots[0].current);
            assert!(!snapshots[1].current);
            assert!(snapshots[2].current);
            services3
                .meta_store
                .get_schema("foo1".to_string())
                .await
                .unwrap();
            services3
                .meta_store
                .get_schema("foo".to_string())
                .await
                .unwrap();
            services3
                .meta_store
                .get_schema("bar".to_string())
                .await
                .unwrap();
            services3
                .meta_store
                .get_schema("bar_after".to_string())
                .await
                .unwrap();
            fs::remove_dir_all(config.local_dir()).unwrap();
            fs::remove_dir_all(config.remote_dir()).unwrap();
        }
    }

    #[tokio::test]
    async fn upload_logs_without_snapshots() {
        let config = Config::test("upload_logs_without_snapshots");

        let _ = fs::remove_dir_all(config.local_dir());
        let _ = fs::remove_dir_all(config.remote_dir());

        let services = config.configure().await;

        services.start_processing_loops().await.unwrap();
        let rocks_meta_store = services.rocks_meta_store.as_ref().unwrap();
        let remote_fs = services
            .injector
            .get_service::<dyn RemoteFs>("original_remote_fs")
            .await;
        services
            .meta_store
            .create_schema("foo1".to_string(), false)
            .await
            .unwrap();
        rocks_meta_store.run_upload().await.unwrap();
        services
            .meta_store
            .create_schema("foo".to_string(), false)
            .await
            .unwrap();
        rocks_meta_store.run_upload().await.unwrap();
        let uploaded = remote_fs.list("metastore-").await.unwrap();
        assert!(uploaded.is_empty());

        rocks_meta_store.upload_check_point().await.unwrap();

        services
            .meta_store
            .create_schema("bar".to_string(), false)
            .await
            .unwrap();

        rocks_meta_store.run_upload().await.unwrap();

        let uploaded = remote_fs.list("metastore-").await.unwrap();

        let logs_uploaded = uploaded
            .into_iter()
            .filter(|n| n.contains("-logs"))
            .collect::<Vec<_>>();

        assert_eq!(logs_uploaded.len(), 1);

        rocks_meta_store.run_upload().await.unwrap();

        let uploaded = remote_fs.list("metastore-").await.unwrap();

        let logs_uploaded = uploaded
            .into_iter()
            .filter(|n| n.contains("-logs"))
            .collect::<Vec<_>>();

        assert_eq!(logs_uploaded.len(), 1);

        services
            .meta_store
            .create_schema("bar2".to_string(), false)
            .await
            .unwrap();

        rocks_meta_store.run_upload().await.unwrap();

        let uploaded = remote_fs.list("metastore-").await.unwrap();

        let logs_uploaded = uploaded
            .into_iter()
            .filter(|n| n.contains("-logs"))
            .collect::<Vec<_>>();

        assert_eq!(logs_uploaded.len(), 2);

        let _ = fs::remove_dir_all(config.local_dir());
        let _ = fs::remove_dir_all(config.remote_dir());
    }

    #[tokio::test]
    async fn log_replay_ordering() {
        {
            let config = Config::test("log_replay_ordering");

            let _ = fs::remove_dir_all(config.local_dir());
            let _ = fs::remove_dir_all(config.remote_dir());

            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .upload_check_point()
                .await
                .unwrap();
            for i in 0..100 {
                let schema = services
                    .meta_store
                    .create_schema(format!("foo{}", i), false)
                    .await
                    .unwrap();
                services
                    .rocks_meta_store
                    .as_ref()
                    .unwrap()
                    .run_upload()
                    .await
                    .unwrap();
                let table = services
                    .meta_store
                    .create_table(
                        format!("foo{}", i),
                        format!("table{}", i),
                        vec![Column::new("foo".to_string(), ColumnType::String, 0)],
                        None,
                        None,
                        Vec::new(),
                        false,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .await
                    .unwrap();
                services
                    .rocks_meta_store
                    .as_ref()
                    .unwrap()
                    .run_upload()
                    .await
                    .unwrap();
                services
                    .meta_store
                    .drop_table(table.get_id())
                    .await
                    .unwrap();
                services
                    .rocks_meta_store
                    .as_ref()
                    .unwrap()
                    .run_upload()
                    .await
                    .unwrap();
                services
                    .meta_store
                    .delete_schema_by_id(schema.get_id())
                    .await
                    .unwrap();
                services
                    .rocks_meta_store
                    .as_ref()
                    .unwrap()
                    .run_upload()
                    .await
                    .unwrap();
            }
            services.stop_processing_loops().await.unwrap();

            Delay::new(Duration::from_millis(1000)).await; // TODO logger init conflict
            fs::remove_dir_all(config.local_dir()).unwrap();
        }

        {
            let config = Config::test("log_replay_ordering");

            let services2 = config.configure().await;
            let tables = services2
                .meta_store
                .get_tables_with_path(true)
                .await
                .unwrap();
            assert_eq!(tables.len(), 0);
            fs::remove_dir_all(config.local_dir()).unwrap();
            fs::remove_dir_all(config.remote_dir()).unwrap();
        }
    }

    #[tokio::test]
    async fn discard_logs() {
        {
            let config = Config::test("discard_logs");

            let _ = fs::remove_dir_all(config.local_dir());
            let _ = fs::remove_dir_all(config.remote_dir());

            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();
            services
                .meta_store
                .create_schema("foo1".to_string(), false)
                .await
                .unwrap();
            while !services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .has_pending_changes()
                .await
                .unwrap()
            {
                futures_timer::Delay::new(Duration::from_millis(100)).await;
            }
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .run_upload()
                .await
                .unwrap();
            services
                .meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            while !services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .has_pending_changes()
                .await
                .unwrap()
            {
                futures_timer::Delay::new(Duration::from_millis(100)).await;
            }
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .upload_check_point()
                .await
                .unwrap();
            services
                .meta_store
                .create_schema("bar".to_string(), false)
                .await
                .unwrap();
            while !services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .has_pending_changes()
                .await
                .unwrap()
            {
                futures_timer::Delay::new(Duration::from_millis(100)).await;
            }
            services
                .rocks_meta_store
                .as_ref()
                .unwrap()
                .run_upload()
                .await
                .unwrap();
            services.stop_processing_loops().await.unwrap();

            Delay::new(Duration::from_millis(1000)).await; // TODO logger init conflict
            fs::remove_dir_all(config.local_dir()).unwrap();
            let list = LocalDirRemoteFs::list_recursive(
                config.remote_dir().clone(),
                "metastore-".to_string(),
                config.remote_dir().clone(),
            )
            .await
            .unwrap();
            let re = Regex::new(r"(\d+).flex").unwrap();
            let last_log = list
                .iter()
                .filter(|f| re.captures(f.remote_path()).is_some())
                .max_by_key(|f| {
                    re.captures(f.remote_path())
                        .unwrap()
                        .get(1)
                        .map(|m| m.as_str().parse::<u64>().unwrap())
                })
                .unwrap();
            let file_path = config.remote_dir().join(last_log.remote_path());
            println!("Truncating {:?}", file_path);
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(file_path.clone())
                .unwrap();
            println!("Size {}", file.metadata().unwrap().len());
            file.set_len(50).unwrap();
        }

        {
            let config = Config::test("discard_logs");

            let services2 = config.configure().await;
            services2
                .meta_store
                .get_schema("foo1".to_string())
                .await
                .unwrap();
            services2
                .meta_store
                .get_schema("foo".to_string())
                .await
                .unwrap();

            fs::remove_dir_all(config.local_dir()).unwrap();
            fs::remove_dir_all(config.remote_dir()).unwrap();
        }
    }

    #[tokio::test]
    async fn swap_chunks() {
        let config = Config::test("swap_chunks");
        let store_path = env::current_dir().unwrap().join("swap_chunks_test-local");
        let remote_store_path = env::current_dir().unwrap().join("swap_chunks_test-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        {
            let meta_store = RocksMetaStore::new(
                store_path.join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let cols = vec![Column::new("name".to_string(), ColumnType::String, 0)];
            meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    cols.clone(),
                    None,
                    None,
                    vec![],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let partition = meta_store.get_partition(1).await.unwrap();

            //============= trying to swap same source chunks twice ==============

            let mut source_ids: Vec<u64> = Vec::new();
            let ch = meta_store
                .create_chunk(partition.get_id(), 10, None, None, true)
                .await
                .unwrap();
            source_ids.push(ch.get_id());
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();

            let ch = meta_store
                .create_chunk(partition.get_id(), 16, None, None, true)
                .await
                .unwrap();
            source_ids.push(ch.get_id());
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();

            let dest_chunk = meta_store
                .create_chunk(partition.get_id(), 26, None, None, true)
                .await
                .unwrap();
            assert_eq!(dest_chunk.get_row().active(), false);

            let dest_chunk2 = meta_store
                .create_chunk(partition.get_id(), 26, None, None, true)
                .await
                .unwrap();
            assert_eq!(dest_chunk2.get_row().active(), false);

            meta_store
                .swap_chunks(
                    source_ids.clone(),
                    vec![(dest_chunk.get_id(), Some(26))],
                    None,
                )
                .await
                .unwrap();

            for id in source_ids.iter() {
                let ch = meta_store.get_chunk(id.to_owned()).await.unwrap();
                assert_eq!(ch.get_row().active(), false);
            }

            let ch = meta_store.get_chunk(dest_chunk.get_id()).await.unwrap();
            assert_eq!(ch.get_row().active(), true);

            meta_store
                .swap_chunks(
                    source_ids.clone(),
                    vec![(dest_chunk2.get_id(), Some(26))],
                    None,
                )
                .await
                .expect_err("Source chunk 1 is not active when swapping of (1, 2) to (3) chunks");

            //============= trying to use already active chunk as destination of swap ==============
            let mut source_ids: Vec<u64> = Vec::new();
            let ch = meta_store
                .create_chunk(partition.get_id(), 10, None, None, true)
                .await
                .unwrap();
            source_ids.push(ch.get_id());
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();

            let ch = meta_store
                .create_chunk(partition.get_id(), 16, None, None, true)
                .await
                .unwrap();
            source_ids.push(ch.get_id());
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();

            meta_store
                .swap_chunks(
                    source_ids.clone(),
                    vec![(dest_chunk.get_id(), Some(26))],
                    None,
                )
                .await
                .expect_err(
                    "Destination chunk 3 is already active when swapping of (5, 6) to (3) chunks",
                );

            for id in source_ids.iter() {
                let ch = meta_store.get_chunk(id.to_owned()).await.unwrap();
                assert_eq!(ch.get_row().active(), true);
            }
        }

        assert!(true);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn swap_active_partitions() {
        let config = Config::test("swap_active_partitions");
        let store_path = env::current_dir()
            .unwrap()
            .join("swap_active_partitions_test-local");
        let remote_store_path = env::current_dir()
            .unwrap()
            .join("swap_active_partitions_test-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        {
            let meta_store = RocksMetaStore::new(
                store_path.join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let cols = vec![Column::new("name".to_string(), ColumnType::String, 0)];
            meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    cols.clone(),
                    None,
                    None,
                    vec![],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let partition = meta_store.get_partition(1).await.unwrap();

            let mut source_chunks: Vec<IdRow<Chunk>> = Vec::new();
            let ch = meta_store
                .create_chunk(partition.get_id(), 10, None, None, true)
                .await
                .unwrap();
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();
            source_chunks.push(ch);

            let ch = meta_store
                .create_chunk(partition.get_id(), 16, None, None, true)
                .await
                .unwrap();
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();
            source_chunks.push(ch);

            let dest_partition = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();

            meta_store
                .swap_active_partitions(
                    vec![(partition.clone(), source_chunks.clone())],
                    vec![(dest_partition.clone(), 10)],
                    vec![(26, (None, None), (None, None))],
                )
                .await
                .unwrap();
            assert_eq!(
                meta_store
                    .get_partition(1)
                    .await
                    .unwrap()
                    .get_row()
                    .is_active(),
                false
            );
            assert_eq!(
                meta_store
                    .get_partition(dest_partition.get_id())
                    .await
                    .unwrap()
                    .get_row()
                    .is_active(),
                true
            );
            for c in source_chunks.iter() {
                assert_eq!(
                    meta_store
                        .get_chunk(c.get_id())
                        .await
                        .unwrap()
                        .get_row()
                        .active(),
                    false
                );
            }

            //==================  Source partition is not active ===============

            let mut source_chunks: Vec<IdRow<Chunk>> = Vec::new();
            let ch = meta_store
                .create_chunk(partition.clone().get_id(), 10, None, None, true)
                .await
                .unwrap();
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();
            source_chunks.push(ch);

            let ch = meta_store
                .create_chunk(partition.get_id(), 16, None, None, true)
                .await
                .unwrap();
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();
            source_chunks.push(ch);

            let dest_partition = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();

            match meta_store
                .swap_active_partitions(
                    vec![(partition, source_chunks.clone())],
                    vec![(dest_partition.clone(), 10)],
                    vec![(26, (None, None), (None, None))],
                )
                .await
            {
                Ok(_) => assert!(false),
                Err(CubeError { message, .. }) => {
                    assert!(message.starts_with("Current partition is not active"))
                }
            };

            //==================  Source chunks is not active ===============

            let mut source_chunks: Vec<IdRow<Chunk>> = Vec::new();

            let partition = meta_store
                .get_active_partitions_by_index_id(1)
                .await
                .unwrap()
                .first()
                .unwrap()
                .to_owned();
            let ch = meta_store
                .create_chunk(partition.clone().get_id(), 10, None, None, true)
                .await
                .unwrap();
            source_chunks.push(ch);

            let ch = meta_store
                .create_chunk(partition.get_id(), 16, None, None, true)
                .await
                .unwrap();
            source_chunks.push(ch);

            let dest_partition = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();

            let dest_row_count = partition.get_row().main_table_row_count() + 26;

            match meta_store
                .swap_active_partitions(
                    vec![(partition, source_chunks.clone())],
                    vec![(dest_partition.clone(), 10)],
                    vec![(dest_row_count, (None, None), (None, None))],
                )
                .await
            {
                Ok(_) => assert!(false),
                Err(CubeError { message, .. }) => {
                    assert!(message.starts_with("Current chunk is not active"))
                }
            };

            //===================== Destination partition is active ================
            let mut source_chunks: Vec<IdRow<Chunk>> = Vec::new();

            let partition = meta_store
                .get_active_partitions_by_index_id(1)
                .await
                .unwrap()
                .first()
                .unwrap()
                .to_owned();
            let ch = meta_store
                .create_chunk(partition.clone().get_id(), 10, None, None, true)
                .await
                .unwrap();
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();
            source_chunks.push(ch);

            let ch = meta_store
                .create_chunk(partition.get_id(), 16, None, None, true)
                .await
                .unwrap();
            meta_store.chunk_uploaded(ch.get_id()).await.unwrap();
            source_chunks.push(ch);

            let dest_row_count = partition.get_row().main_table_row_count() + 26;

            match meta_store
                .swap_active_partitions(
                    vec![(partition.clone(), source_chunks.clone())],
                    vec![(partition.clone(), 10)],
                    vec![(dest_row_count, (None, None), (None, None))],
                )
                .await
            {
                Ok(_) => assert!(false),
                Err(CubeError { message, .. }) => {
                    assert!(message.starts_with("New partition is already active"))
                }
            };
        }

        assert!(true);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn job_priority_test() {
        let config = Config::test("job_priority_test");
        let store_path = env::current_dir().unwrap().join("test-job-priority-local");
        let remote_store_path = env::current_dir().unwrap().join("test-job-priority-remote");
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        {
            let meta_store = RocksMetaStore::new(
                store_path.clone().join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 1),
                    JobType::InMemoryChunksCompaction,
                    "node1".to_string(),
                ))
                .await
                .unwrap();
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 1),
                    JobType::PartitionCompaction,
                    "node1".to_string(),
                ))
                .await
                .unwrap();
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 2),
                    JobType::PartitionCompaction,
                    "node1".to_string(),
                ))
                .await
                .unwrap();
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 3),
                    JobType::InMemoryChunksCompaction,
                    "node1".to_string(),
                ))
                .await
                .unwrap();

            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 11),
                    JobType::PartitionCompaction,
                    "node2".to_string(),
                ))
                .await
                .unwrap();
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 12),
                    JobType::PartitionCompaction,
                    "node2".to_string(),
                ))
                .await
                .unwrap();
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 13),
                    JobType::InMemoryChunksCompaction,
                    "node2".to_string(),
                ))
                .await
                .unwrap();
            meta_store
                .add_job(Job::new(
                    RowKey::Table(TableId::Partitions, 11),
                    JobType::InMemoryChunksCompaction,
                    "node2".to_string(),
                ))
                .await
                .unwrap();

            let job = meta_store
                .start_processing_job("node1".to_string(), false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.get_row().job_type(), &JobType::InMemoryChunksCompaction);
            assert_eq!(
                job.get_row().row_reference(),
                &RowKey::Table(TableId::Partitions, 1)
            );

            let job = meta_store
                .start_processing_job("node1".to_string(), false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.get_row().job_type(), &JobType::InMemoryChunksCompaction);
            assert_eq!(
                job.get_row().row_reference(),
                &RowKey::Table(TableId::Partitions, 3)
            );

            let job = meta_store
                .start_processing_job("node1".to_string(), false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.get_row().job_type(), &JobType::PartitionCompaction);
            assert_eq!(
                job.get_row().row_reference(),
                &RowKey::Table(TableId::Partitions, 1)
            );

            let job = meta_store
                .start_processing_job("node1".to_string(), false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.get_row().job_type(), &JobType::PartitionCompaction);
            assert_eq!(
                job.get_row().row_reference(),
                &RowKey::Table(TableId::Partitions, 2)
            );

            let job = meta_store
                .start_processing_job("node2".to_string(), false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.get_row().job_type(), &JobType::InMemoryChunksCompaction);
            assert_eq!(
                job.get_row().row_reference(),
                &RowKey::Table(TableId::Partitions, 13)
            );

            let job = meta_store
                .start_processing_job("node2".to_string(), false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.get_row().job_type(), &JobType::InMemoryChunksCompaction);
            assert_eq!(
                job.get_row().row_reference(),
                &RowKey::Table(TableId::Partitions, 11)
            );

            let job = meta_store
                .start_processing_job("node2".to_string(), false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.get_row().job_type(), &JobType::PartitionCompaction);
            assert_eq!(
                job.get_row().row_reference(),
                &RowKey::Table(TableId::Partitions, 11)
            );

            let job = meta_store
                .start_processing_job("node2".to_string(), false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.get_row().job_type(), &JobType::PartitionCompaction);
            assert_eq!(
                job.get_row().row_reference(),
                &RowKey::Table(TableId::Partitions, 12)
            );
        }
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }
}

impl RocksMetaStore {
    fn swap_chunks_impl(
        deactivate_ids: Vec<u64>,
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        db_ref: DbTableRef,
        batch_pipe: &mut BatchPipe,
        check_rows: bool,
        new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        trace!(
            "Swapping chunks: deactivating ({}), activating ({})",
            deactivate_ids.iter().join(", "),
            uploaded_ids_and_sizes.iter().map(|(id, _)| id).join(", ")
        );
        let chunks = ChunkRocksTable::new(db_ref.clone());
        let mut partition_to_row_diffs = HashMap::<u64, i64>::new();
        let mut deactivated_row_count = 0;
        let mut activated_row_count = 0;
        for id in deactivate_ids.iter() {
            let chunk = chunks.get_row_or_not_found(*id)?;
            if !chunk.get_row().active() {
                return Err(CubeError::internal(format!(
                    "Source chunk {} is not active when swapping of ({}) to ({}) chunks",
                    id,
                    deactivate_ids.iter().join(", "),
                    uploaded_ids_and_sizes.iter().map(|(id, _)| id).join(", ")
                )));
            }
            deactivated_row_count += chunk.get_row().row_count;
            *partition_to_row_diffs
                .entry(chunk.get_row().partition_id)
                .or_default() -= chunk.get_row().row_count as i64;
            chunks.update_with_fn(*id, |row| row.deactivate(), batch_pipe)?;
        }
        for (id, file_size) in uploaded_ids_and_sizes.iter() {
            let chunk = chunks.get_row_or_not_found(*id)?;
            if chunk.get_row().active() {
                return Err(CubeError::internal(format!(
                    "Destination chunk {} is already active when swapiping of ({}) to ({}) chunks",
                    id,
                    deactivate_ids.iter().join(", "),
                    uploaded_ids_and_sizes.iter().map(|(id, _)| id).join(", ")
                )));
            }
            activated_row_count += chunk.get_row().row_count;
            *partition_to_row_diffs
                .entry(chunk.get_row().partition_id)
                .or_default() += chunk.get_row().row_count as i64;
            chunks.update_with_res_fn(
                *id,
                |row| {
                    let mut updated = row.set_uploaded(true);
                    if let Some(file_size) = file_size {
                        updated = updated.set_file_size(*file_size)?;
                    }
                    updated = updated.set_replay_handle_id(new_replay_handle_id);
                    Ok(updated)
                },
                batch_pipe,
            )?;
        }
        if check_rows && deactivate_ids.len() > 0 && activated_row_count != deactivated_row_count {
            return Err(CubeError::internal(format!(
                "Deactivated row count ({}) doesn't match activated row count ({}) during swap of ({}) to ({}) chunks",
                deactivated_row_count,
                activated_row_count,
                deactivate_ids.iter().join(", "),
                uploaded_ids_and_sizes.iter().map(|(id, _)| id).join(", ")
            )));
        }
        // Update row counts of multi partitions.
        let partitions = PartitionRocksTable::new(db_ref.clone());
        let mut multipart_to_row_diffs = HashMap::<u64, i64>::new();
        for (p, diff) in partition_to_row_diffs {
            let p = partitions.get_row_or_not_found(p)?;
            let m = match p.get_row().multi_partition_id {
                None => continue,
                Some(m) => m,
            };
            *multipart_to_row_diffs.entry(m).or_default() += diff;
        }
        let multi_partitions = MultiPartitionRocksTable::new(db_ref.clone());
        for (m, diff) in multipart_to_row_diffs {
            if diff == 0 {
                continue;
            }
            multi_partitions.update_with_fn(
                m,
                |m| {
                    if 0 < diff {
                        m.add_rows(diff as u64)
                    } else {
                        m.subtract_rows((-diff) as u64)
                    }
                },
                batch_pipe,
            )?;
        }
        Ok(())
    }
}

impl RocksMetaStore {
    fn drop_index(
        db: DbTableRef,
        pipe: &mut BatchPipe,
        index_id: u64,
        update_multi_partitions: bool,
    ) -> Result<(), CubeError> {
        let partitions_table = PartitionRocksTable::new(db.clone());
        let partitions = partitions_table.get_rows_by_index(
            &PartitionIndexKey::ByIndexId(index_id),
            &PartitionRocksIndex::IndexId,
        )?;

        let chunks_table = ChunkRocksTable::new(db.clone());
        let multi_partitions_table = MultiPartitionRocksTable::new(db.clone());
        for partition in partitions.into_iter() {
            let mut removed_rows = 0;
            if partition.get_row().is_active() {
                removed_rows += partition.get_row().main_table_row_count;
            }
            let chunks = chunks_table.get_rows_by_index(
                &ChunkIndexKey::ByPartitionId(partition.get_id()),
                &ChunkRocksIndex::PartitionId,
            )?;
            for chunk in chunks.into_iter() {
                if chunk.get_row().active {
                    removed_rows += chunk.get_row().row_count;
                }
                chunks_table.delete(chunk.get_id(), pipe)?;
            }
            partitions_table.delete(partition.get_id(), pipe)?;

            if update_multi_partitions {
                if let Some(m) = partition.get_row().multi_partition_id {
                    multi_partitions_table.update_with_fn(
                        m,
                        |r| r.subtract_rows(removed_rows),
                        pipe,
                    )?;
                }
            }
        }
        IndexRocksTable::new(db.clone()).delete(index_id, pipe)?;
        Ok(())
    }
}
