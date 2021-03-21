pub mod chunks;
pub mod index;
pub mod job;
pub mod listener;
pub mod partition;
pub mod schema;
pub mod table;
pub mod wal;

use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::{error, info};
use rocksdb::{
    DBIterator, Direction, IteratorMode, MergeOperands, Options, ReadOptions, Snapshot, WriteBatch,
    WriteBatchIterator, DB,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::hash::{Hash, Hasher};
use std::{collections::hash_map::DefaultHasher, env, io::Cursor, sync::Arc, time};
use tokio::fs;
use tokio::sync::{Notify, RwLock};

use crate::config::injection::DIService;
use crate::config::{Config, ConfigObj};
use crate::metastore::chunks::{ChunkIndexKey, ChunkRocksIndex};
use crate::metastore::index::IndexIndexKey;
use crate::metastore::job::{Job, JobIndexKey, JobRocksIndex, JobRocksTable, JobStatus};
use crate::metastore::partition::PartitionIndexKey;
use crate::metastore::table::{TableIndexKey, TablePath};
use crate::metastore::wal::{WALIndexKey, WALRocksIndex};
use crate::remotefs::{LocalDirRemoteFs, RemoteFs};
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::util::time_span::{warn_long, warn_long_fut};
use crate::util::WorkerLoop;
use crate::CubeError;
use arrow::datatypes::TimeUnit::Microsecond;
use arrow::datatypes::{DataType, Field};
use chrono::{DateTime, Utc};
use chunks::ChunkRocksTable;
use core::{fmt, mem};
use cubehll::HllSketch;
use cubezetasketch::HyperLogLogPlusPlus;
use futures::future::join_all;
use futures::TryFutureExt;
use futures_timer::Delay;
use index::{IndexRocksIndex, IndexRocksTable};
use itertools::Itertools;
use log::trace;
use parquet::basic::Repetition;
use parquet::{
    basic::{LogicalType, Type},
    schema::types,
};
use partition::{PartitionRocksIndex, PartitionRocksTable};
use regex::Regex;
use rocksdb::checkpoint::Checkpoint;
use schema::{SchemaRocksIndex, SchemaRocksTable};
use smallvec::alloc::fmt::Formatter;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use table::Table;
use table::{TableRocksIndex, TableRocksTable};
use tokio::fs::File;
use tokio::sync::broadcast::Sender;
use wal::WALRocksTable;

#[macro_export]
macro_rules! format_table_value {
    ($row:expr, $field:ident, $tt:ty) => {
        DataFrameValue::value(&$row.$field)
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

        impl From<Vec<IdRow<$name>>> for DataFrame {
            fn from(rows: Vec<IdRow<$name>>) -> Self {
                DataFrame::new(
                    vec![
                        Column::new("id".to_string(), ColumnType::Int, 0),
                        $( Column::new(std::stringify!($variant).to_string(), ColumnType::String, 1) ),+
                    ],
                    rows.iter().map(|r|
                        Row::new(vec![
                            TableValue::Int(r.id as i64),
                            $(
                                TableValue::String(format_table_value!(r.row, $variant, $tt))
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
        impl BaseRocksSecondaryIndex<$table> for $index {
            fn index_key_by(&self, row: &$table) -> Vec<u8> {
                self.key_to_bytes(&self.typed_key_by(row))
            }

            fn get_id(&self) -> u32 {
                RocksSecondaryIndex::get_id(self)
            }

            fn is_unique(&self) -> bool {
                RocksSecondaryIndex::is_unique(self)
            }
        }
    };
}

#[macro_export]
macro_rules! rocks_table_impl {
    ($table: ty, $rocks_table: ident, $table_id: expr, $indexes: block) => {
        pub(crate) struct $rocks_table<'a> {
            db: crate::metastore::DbTableRef<'a>,
        }

        impl<'a> $rocks_table<'a> {
            pub fn new(db: crate::metastore::DbTableRef<'a>) -> $rocks_table {
                $rocks_table { db }
            }
        }

        impl<'a> RocksTable for $rocks_table<'a> {
            type T = $table;

            fn db(&self) -> &DB {
                self.db.db
            }

            fn snapshot(&self) -> &rocksdb::Snapshot {
                self.db.snapshot
            }

            fn mem_seq(&self) -> &crate::metastore::MemorySequence {
                &self.db.mem_seq
            }

            fn table_id(&self) -> TableId {
                $table_id
            }

            fn index_id(&self, index_num: IndexId) -> IndexId {
                if index_num > 99 {
                    panic!("Too big index id: {}", index_num);
                }
                $table_id as IndexId + index_num
            }

            fn deserialize_row<'de, D>(
                &self,
                deserializer: D,
            ) -> Result<$table, <D as Deserializer<'de>>::Error>
            where
                D: Deserializer<'de>,
            {
                <$table>::deserialize(deserializer)
            }

            fn indexes() -> Vec<Box<dyn BaseRocksSecondaryIndex<$table>>> {
                $indexes
            }

            fn update_event(
                &self,
                old_row: IdRow<Self::T>,
                new_row: IdRow<Self::T>,
            ) -> MetaStoreEvent {
                paste::expr! { MetaStoreEvent::[<Update $table>](old_row, new_row) }
            }

            fn delete_event(&self, row: IdRow<Self::T>) -> MetaStoreEvent {
                paste::expr! { MetaStoreEvent::[<Delete $table>](row) }
            }
        }

        impl<'a> core::fmt::Debug for $rocks_table<'a> {
            fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                f.write_fmt(format_args!("{}", stringify!($rocks_table)))?;
                Ok(())
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

impl DataFrameValue<String> for Option<ImportFormat> {
    fn value(v: &Self) -> String {
        v.as_ref()
            .map(|v| format!("{:?}", v))
            .unwrap_or("NULL".to_string())
    }
}

impl DataFrameValue<String> for Option<u64> {
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
                            TableValue::Decimal(v) => format!("{}", v),
                            TableValue::Float(v) => format!("{}", v),
                        })
                        .join(", ")
                )
            })
            .unwrap_or("NULL".to_string())
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum HllFlavour {
    Airlift,    // Compatible with Presto, Athena, etc.
    ZetaSketch, // Compatible with BigQuery.
}

pub fn is_valid_hll(data: &[u8], f: HllFlavour) -> Result<(), CubeError> {
    // TODO: do no memory allocations for better performance, this is run on hot path.
    match f {
        HllFlavour::Airlift => {
            HllSketch::read(data)?;
        }
        HllFlavour::ZetaSketch => {
            HyperLogLogPlusPlus::read(data)?;
        }
    }
    return Ok(());
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
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

impl ColumnType {
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
            crate::metastore::ColumnType::String => {
                types::Type::primitive_type_builder(&column.get_name(), Type::BYTE_ARRAY)
                    .with_logical_type(LogicalType::UTF8)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            crate::metastore::ColumnType::Int => {
                types::Type::primitive_type_builder(&column.get_name(), Type::INT64)
                    .with_logical_type(LogicalType::INT_64)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            crate::metastore::ColumnType::Decimal { precision, .. } => {
                types::Type::primitive_type_builder(&column.get_name(), Type::INT64)
                    .with_logical_type(LogicalType::DECIMAL)
                    .with_precision(*precision)
                    .with_scale(column.get_column_type().target_scale())
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            crate::metastore::ColumnType::Bytes | ColumnType::HyperLogLog(_) => {
                types::Type::primitive_type_builder(&column.get_name(), Type::BYTE_ARRAY)
                    .with_logical_type(LogicalType::NONE)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            crate::metastore::ColumnType::Timestamp => {
                types::Type::primitive_type_builder(&column.get_name(), Type::INT64)
                    //TODO MICROS?
                    .with_logical_type(LogicalType::TIMESTAMP_MICROS)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            }
            crate::metastore::ColumnType::Boolean => {
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

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Column {
    name: String,
    column_type: ColumnType,
    column_index: usize,
}

impl Into<Field> for Column {
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
            false,
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
            ColumnType::Float => "FLOAT".to_string(),
        };
        f.write_fmt(format_args!("{} {}", self.name, column_type))
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum ImportFormat {
    CSV,
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Schema {
    name: String
}
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Index {
    name: String,
    table_id: u64,
    columns: Vec<Column>,
    sort_key_size: u64
}
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Partition {
    index_id: u64,
    parent_partition_id: Option<u64>,
    min_value: Option<Row>,
    max_value: Option<Row>,
    active: bool,
    #[serde(default)]
    warmed_up: bool,
    main_table_row_count: u64,
    #[serde(default)]
    last_used: Option<DateTime<Utc>>
}
}

data_frame_from! {
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Chunk {
    partition_id: u64,
    row_count: u64,
    uploaded: bool,
    active: bool,
    #[serde(default)]
    last_used: Option<DateTime<Utc>>
}
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct WAL {
    table_id: u64,
    row_count: u64,
    uploaded: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct IdRow<T: Clone> {
    id: u64,
    row: T,
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

struct KeyVal {
    key: Vec<u8>,
    val: Vec<u8>,
}

struct BatchPipe<'a> {
    db: &'a DB,
    write_batch: WriteBatch,
    events: Vec<MetaStoreEvent>,
}

impl<'a> BatchPipe<'a> {
    fn new(db: &'a DB) -> BatchPipe<'a> {
        BatchPipe {
            db,
            write_batch: WriteBatch::default(),
            events: Vec::new(),
        }
    }

    fn batch(&mut self) -> &mut WriteBatch {
        &mut self.write_batch
    }

    fn add_event(&mut self, event: MetaStoreEvent) {
        self.events.push(event);
    }

    fn batch_write_rows(self) -> Result<Vec<MetaStoreEvent>, CubeError> {
        let db = self.db;
        db.write(self.write_batch)?;
        Ok(self.events)
    }
}

#[derive(Clone)]
pub struct DbTableRef<'a> {
    pub db: &'a DB,
    pub snapshot: &'a Snapshot<'a>,
    pub mem_seq: MemorySequence,
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
            rocks_meta_store: RocksMetaStore,
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
                    .read_operation(move |db_ref| Ok(Self::table(db_ref).all_rows()?))
                    .await
            }

            async fn row_by_id_or_not_found(&self, id: u64) -> Result<IdRow<Self::T>, CubeError> {
                self.rocks_meta_store
                    .read_operation(move |db_ref| Ok(Self::table(db_ref).get_row_or_not_found(id)?))
                    .await
            }

            async fn delete(&self, id: u64) -> Result<IdRow<Self::T>, CubeError> {
                self.rocks_meta_store
                    .write_operation(
                        move |db_ref, batch| Ok(Self::table(db_ref).delete(id, batch)?),
                    )
                    .await
            }
        }
    };
}

meta_store_table_impl!(SchemaMetaStoreTable, Schema, SchemaRocksTable);
meta_store_table_impl!(ChunkMetaStoreTable, Chunk, ChunkRocksTable);
meta_store_table_impl!(IndexMetaStoreTable, Index, IndexRocksTable);
meta_store_table_impl!(PartitionMetaStoreTable, Partition, PartitionRocksTable);
meta_store_table_impl!(TableMetaStoreTable, Table, TableRocksTable);

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
    ) -> Result<IdRow<Table>, CubeError>;
    async fn get_table(
        &self,
        schema_name: String,
        table_name: String,
    ) -> Result<IdRow<Table>, CubeError>;
    async fn get_table_by_id(&self, table_id: u64) -> Result<IdRow<Table>, CubeError>;
    async fn get_tables(&self) -> Result<Vec<IdRow<Table>>, CubeError>;
    async fn get_tables_with_path(&self) -> Result<Vec<TablePath>, CubeError>;
    async fn drop_table(&self, table_id: u64) -> Result<IdRow<Table>, CubeError>;

    fn partition_table(&self) -> PartitionMetaStoreTable;
    async fn create_partition(&self, partition: Partition) -> Result<IdRow<Partition>, CubeError>;
    async fn get_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError>;
    async fn get_partition_for_compaction(
        &self,
        partition_id: u64,
    ) -> Result<(IdRow<Partition>, IdRow<Index>), CubeError>;
    async fn get_partition_chunk_sizes(&self, partition_id: u64) -> Result<u64, CubeError>;
    async fn swap_active_partitions(
        &self,
        current_active: Vec<u64>,
        new_active: Vec<u64>,
        compacted_chunk_ids: Vec<u64>,
        new_active_min_max: Vec<(u64, (Option<Row>, Option<Row>))>,
    ) -> Result<(), CubeError>;
    async fn is_partition_used(&self, partition_id: u64) -> Result<bool, CubeError>;
    async fn delete_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError>;
    async fn mark_partition_warmed_up(&self, partition_id: u64) -> Result<(), CubeError>;

    fn index_table(&self) -> IndexMetaStoreTable;
    async fn create_index(
        &self,
        schema_name: String,
        table_name: String,
        index_def: IndexDef,
    ) -> Result<IdRow<Index>, CubeError>;
    async fn get_default_index(&self, table_id: u64) -> Result<IdRow<Index>, CubeError>;
    async fn get_table_indexes(&self, table_id: u64) -> Result<Vec<IdRow<Index>>, CubeError>;
    async fn get_active_partitions_by_index_id(
        &self,
        index_id: u64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError>;
    async fn get_index(&self, index_id: u64) -> Result<IdRow<Index>, CubeError>;

    async fn get_active_partitions_and_chunks_by_index_id_for_select(
        &self,
        index_id: u64,
    ) -> Result<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>, CubeError>;

    fn chunks_table(&self) -> ChunkMetaStoreTable;
    async fn create_chunk(
        &self,
        partition_id: u64,
        row_count: usize,
    ) -> Result<IdRow<Chunk>, CubeError>;
    async fn get_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError>;
    async fn get_chunks_by_partition(
        &self,
        partition_id: u64,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError>;
    async fn chunk_uploaded(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError>;
    async fn deactivate_chunk(&self, chunk_id: u64) -> Result<(), CubeError>;
    async fn swap_chunks(
        &self,
        deactivate_ids: Vec<u64>,
        uploaded_ids: Vec<u64>,
    ) -> Result<(), CubeError>;
    async fn activate_wal(
        &self,
        wal_id_to_delete: u64,
        uploaded_ids: Vec<u64>,
        index_count: u64,
    ) -> Result<(), CubeError>;
    async fn activate_chunks(
        &self,
        table_id: u64,
        uploaded_chunk_ids: Vec<u64>,
    ) -> Result<(), CubeError>;
    async fn is_chunk_used(&self, chunk_id: u64) -> Result<bool, CubeError>;
    async fn delete_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError>;

    async fn create_wal(&self, table_id: u64, row_count: usize) -> Result<IdRow<WAL>, CubeError>;
    async fn get_wal(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError>;
    async fn delete_wal(&self, wal_id: u64) -> Result<(), CubeError>;
    async fn wal_uploaded(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError>;
    async fn get_wals_for_table(&self, table_id: u64) -> Result<Vec<IdRow<WAL>>, CubeError>;

    async fn add_job(&self, job: Job) -> Result<Option<IdRow<Job>>, CubeError>;
    async fn get_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError>;
    async fn delete_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError>;
    async fn start_processing_job(
        &self,
        server_name: String,
    ) -> Result<Option<IdRow<Job>>, CubeError>;
    async fn update_status(&self, job_id: u64, status: JobStatus) -> Result<IdRow<Job>, CubeError>;
    async fn update_heart_beat(&self, job_id: u64) -> Result<IdRow<Job>, CubeError>;
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

    DeleteChunk(IdRow<Chunk>),
    DeleteIndex(IdRow<Index>),
    DeleteJob(IdRow<Job>),
    DeletePartition(IdRow<Partition>),
    DeleteSchema(IdRow<Schema>),
    DeleteTable(IdRow<Table>),
    DeleteWAL(IdRow<WAL>),
}

type SecondaryKey = Vec<u8>;
type IndexId = u32;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum RowKey {
    Table(TableId, u64),
    Sequence(TableId),
    SecondaryIndex(IndexId, SecondaryKey, u64),
}

pub fn get_fixed_prefix() -> usize {
    13
}

impl RowKey {
    fn from_bytes(bytes: &[u8]) -> RowKey {
        let mut reader = Cursor::new(bytes);
        match reader.read_u8().unwrap() {
            1 => RowKey::Table(TableId::from(reader.read_u32::<BigEndian>().unwrap()), {
                // skip zero for fixed key padding
                reader.read_u64::<BigEndian>().unwrap();
                reader.read_u64::<BigEndian>().unwrap()
            }),
            2 => RowKey::Sequence(TableId::from(reader.read_u32::<BigEndian>().unwrap())),
            3 => {
                let table_id = IndexId::from(reader.read_u32::<BigEndian>().unwrap());
                let mut secondary_key: SecondaryKey = SecondaryKey::new();
                let sc_length = bytes.len() - 13;
                for _i in 0..sc_length {
                    secondary_key.push(reader.read_u8().unwrap());
                }
                let row_id = reader.read_u64::<BigEndian>().unwrap();

                RowKey::SecondaryIndex(table_id, secondary_key, row_id)
            }
            v => panic!("Unknown key prefix: {}", v),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
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
        }
        wtr
    }
}

macro_rules! enum_from_primitive_impl {
    ($name:ident, $( $variant:ident )*) => {
        impl From<u32> for $name {
            fn from(n: u32) -> Self {
                $( if n == $name::$variant as u32 {
                    $name::$variant
                } else )* {
                    panic!("Unknown {}: {}", stringify!($name), n);
                }
            }
        }
    };
}

#[macro_use(enum_from_primitive_impl)]
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
        Jobs = 0x0700
    }
}

#[derive(Clone)]
pub struct MemorySequence {
    seq_store: Arc<Mutex<HashMap<TableId, u64>>>,
}

impl MemorySequence {
    pub fn next_seq(&self, table_id: TableId, snapshot_value: u64) -> Result<u64, CubeError> {
        let mut store = self.seq_store.lock()?;
        let mut current = *store.entry(table_id).or_insert(snapshot_value);
        current += 1;
        store.insert(table_id, current);
        Ok(current)
    }
}

#[derive(Clone)]
pub struct RocksMetaStore {
    pub db: Arc<RwLock<Arc<DB>>>,
    seq_store: Arc<Mutex<HashMap<TableId, u64>>>,
    listeners: Arc<RwLock<Vec<Sender<MetaStoreEvent>>>>,
    remote_fs: Arc<dyn RemoteFs>,
    last_checkpoint_time: Arc<RwLock<SystemTime>>,
    write_notify: Arc<Notify>,
    write_completed_notify: Arc<Notify>,
    last_upload_seq: Arc<RwLock<u64>>,
    last_check_seq: Arc<RwLock<u64>>,
    upload_loop: Arc<WorkerLoop>,
    config: Arc<dyn ConfigObj>,
}

trait BaseRocksSecondaryIndex<T>: Debug {
    fn index_key_by(&self, row: &T) -> Vec<u8>;

    fn get_id(&self) -> u32;

    fn key_hash(&self, row: &T) -> u64 {
        let key_bytes = self.index_key_by(row);
        self.hash_bytes(&key_bytes)
    }

    fn hash_bytes(&self, key_bytes: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        key_bytes.hash(&mut hasher);
        hasher.finish()
    }

    fn is_unique(&self) -> bool;
}

trait RocksSecondaryIndex<T, K: Hash>: BaseRocksSecondaryIndex<T> {
    fn typed_key_by(&self, row: &T) -> K;

    fn key_to_bytes(&self, key: &K) -> Vec<u8>;

    fn typed_key_hash(&self, row_key: &K) -> u64 {
        let key_bytes = self.key_to_bytes(row_key);
        self.hash_bytes(&key_bytes)
    }

    fn index_key_by(&self, row: &T) -> Vec<u8> {
        self.key_to_bytes(&self.typed_key_by(row))
    }

    fn get_id(&self) -> u32;

    fn is_unique(&self) -> bool;
}

impl<T, I> BaseRocksSecondaryIndex<T> for I
where
    I: RocksSecondaryIndex<T, String>,
{
    fn index_key_by(&self, row: &T) -> Vec<u8> {
        self.key_to_bytes(&self.typed_key_by(row))
    }

    fn get_id(&self) -> u32 {
        RocksSecondaryIndex::get_id(self)
    }

    fn is_unique(&self) -> bool {
        RocksSecondaryIndex::is_unique(self)
    }
}

struct TableScanIter<'a, RT: RocksTable + ?Sized> {
    table_id: TableId,
    table: &'a RT,
    iter: DBIterator<'a>,
}

impl<'a, RT: RocksTable<T = T> + ?Sized, T> Iterator for TableScanIter<'a, RT>
where
    T: Serialize + Clone + Debug + Send,
{
    type Item = Result<IdRow<T>, CubeError>;

    fn next(&mut self) -> Option<Self::Item> {
        let option = self.iter.next();
        if let Some((key, value)) = option {
            if let RowKey::Table(table_id, row_id) = RowKey::from_bytes(&key) {
                if table_id != self.table_id {
                    return None;
                }
                Some(self.table.deserialize_id_row(row_id, &value))
            } else {
                None
            }
        } else {
            None
        }
    }
}

trait RocksTable: Debug + Send + Sync {
    type T: Serialize + Clone + Debug + Send;
    fn delete_event(&self, row: IdRow<Self::T>) -> MetaStoreEvent;
    fn update_event(&self, old_row: IdRow<Self::T>, new_row: IdRow<Self::T>) -> MetaStoreEvent;
    fn db(&self) -> &DB;
    fn snapshot(&self) -> &Snapshot;
    fn mem_seq(&self) -> &MemorySequence;
    fn index_id(&self, index_num: IndexId) -> IndexId;
    fn table_id(&self) -> TableId;
    fn deserialize_row<'de, D>(&self, deserializer: D) -> Result<Self::T, D::Error>
    where
        D: Deserializer<'de>;
    fn indexes() -> Vec<Box<dyn BaseRocksSecondaryIndex<Self::T>>>;

    fn insert(
        &self,
        row: Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        row.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();

        for index in Self::indexes().iter() {
            let hash = index.key_hash(&row);
            let index_val = index.index_key_by(&row);
            let existing_keys =
                self.get_row_from_index(index.get_id(), &index_val, &hash.to_be_bytes().to_vec())?;
            if index.is_unique() && existing_keys.len() > 0 {
                return Err(CubeError::user(
                    format!(
                        "Unique constraint violation: row {:?} has a key that already exists in {:?} index",
                        &row,
                        index
                    )
                ));
            }
        }

        let (row_id, inserted_row) = self.insert_row(serialized_row)?;
        batch_pipe.add_event(MetaStoreEvent::Insert(self.table_id(), row_id));
        batch_pipe.batch().put(inserted_row.key, inserted_row.val);

        let index_row = self.insert_index_row(&row, row_id)?;
        for to_insert in index_row {
            if self.snapshot().get(&to_insert.key)?.is_some() {
                return Err(CubeError::internal(format!("Primary key constraint violation. Primary key already exists for a row id {}: {:?}", row_id, &row)));
            }
            batch_pipe.batch().put(to_insert.key, to_insert.val);
        }

        Ok(IdRow::new(row_id, row))
    }

    fn get_row_ids_by_index<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<Vec<u64>, CubeError>
    where
        K: Hash,
    {
        let hash = secondary_index.typed_key_hash(&row_key);
        let index_val = secondary_index.key_to_bytes(&row_key);
        let existing_keys = self.get_row_from_index(
            RocksSecondaryIndex::get_id(secondary_index),
            &index_val,
            &hash.to_be_bytes().to_vec(),
        )?;

        Ok(existing_keys)
    }

    fn get_rows_by_index<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<Vec<IdRow<Self::T>>, CubeError>
    where
        K: Hash,
    {
        let row_ids = self.get_row_ids_by_index(row_key, secondary_index)?;

        let mut res = Vec::new();

        for id in row_ids {
            if let Some(row) = self.get_row(id)? {
                res.push(row);
            } else {
                let secondary_index_key = RowKey::SecondaryIndex(
                    self.index_id(RocksSecondaryIndex::get_id(secondary_index)),
                    secondary_index
                        .typed_key_hash(row_key)
                        .to_be_bytes()
                        .to_vec(),
                    id,
                );
                self.db().delete(secondary_index_key.to_bytes())?;
                return Err(CubeError::internal(format!(
                    "Row exists in secondary index however missing in {:?} table: {}. Repairing index.",
                    self, id
                )));
            }
        }

        if RocksSecondaryIndex::is_unique(secondary_index) && res.len() > 1 {
            return Err(CubeError::internal(format!(
                "Unique index expected but found multiple values in {:?} table: {:?}",
                self, res
            )));
        }

        Ok(res)
    }

    fn get_single_row_by_index<K: Debug>(
        &self,
        row_key: &K,
        secondary_index: &impl RocksSecondaryIndex<Self::T, K>,
    ) -> Result<IdRow<Self::T>, CubeError>
    where
        K: Hash,
    {
        let rows = self.get_rows_by_index(row_key, secondary_index)?;
        Ok(rows.into_iter().nth(0).ok_or(CubeError::internal(format!(
            "One value expected in {:?} for {:?} but nothing found",
            self, row_key
        )))?)
    }

    fn update_with_fn(
        &self,
        row_id: u64,
        update_fn: impl FnOnce(&Self::T) -> Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let row = self.get_row_or_not_found(row_id)?;
        let new_row = update_fn(&row.get_row());
        self.update(row_id, new_row, &row.get_row(), batch_pipe)
    }

    fn update(
        &self,
        row_id: u64,
        new_row: Self::T,
        old_row: &Self::T,
        batch_pipe: &mut BatchPipe,
    ) -> Result<IdRow<Self::T>, CubeError> {
        let deleted_row = self.delete_index_row(&old_row, row_id)?;
        for row in deleted_row {
            batch_pipe.batch().delete(row.key);
        }

        let mut ser = flexbuffers::FlexbufferSerializer::new();
        new_row.serialize(&mut ser).unwrap();
        let serialized_row = ser.take_buffer();

        let updated_row = self.update_row(row_id, serialized_row)?;
        batch_pipe.add_event(MetaStoreEvent::Update(self.table_id(), row_id));
        batch_pipe.add_event(self.update_event(
            IdRow::new(row_id, old_row.clone()),
            IdRow::new(row_id, new_row.clone()),
        ));
        batch_pipe.batch().put(updated_row.key, updated_row.val);

        let index_row = self.insert_index_row(&new_row, row_id)?;
        for row in index_row {
            batch_pipe.batch().put(row.key, row.val);
        }
        Ok(IdRow::new(row_id, new_row))
    }

    fn delete(&self, row_id: u64, batch_pipe: &mut BatchPipe) -> Result<IdRow<Self::T>, CubeError> {
        let row = self.get_row_or_not_found(row_id)?;
        let deleted_row = self.delete_index_row(row.get_row(), row_id)?;
        batch_pipe.add_event(MetaStoreEvent::Delete(self.table_id(), row_id));
        batch_pipe.add_event(self.delete_event(row.clone()));
        for row in deleted_row {
            batch_pipe.batch().delete(row.key);
        }

        batch_pipe.batch().delete(self.delete_row(row_id)?.key);

        Ok(row)
    }

    fn next_table_seq(&self) -> Result<u64, CubeError> {
        let ref db = self.db();
        let seq_key = RowKey::Sequence(self.table_id());
        let before_merge = self
            .snapshot()
            .get(seq_key.to_bytes())?
            .map(|v| Cursor::new(v).read_u64::<BigEndian>().unwrap());

        // TODO revert back merge operator if locking works
        let next_seq = self
            .mem_seq()
            .next_seq(self.table_id(), before_merge.unwrap_or(0))?;

        let mut to_write = vec![];
        to_write.write_u64::<BigEndian>(next_seq)?;
        db.put(seq_key.to_bytes(), to_write)?;

        Ok(next_seq)
    }

    fn insert_row(&self, row: Vec<u8>) -> Result<(u64, KeyVal), CubeError> {
        let next_seq = self.next_table_seq()?;
        let t = RowKey::Table(self.table_id(), next_seq);
        let res = KeyVal {
            key: t.to_bytes(),
            val: row,
        };
        Ok((next_seq, res))
    }

    fn update_row(&self, row_id: u64, row: Vec<u8>) -> Result<KeyVal, CubeError> {
        let t = RowKey::Table(self.table_id(), row_id);
        let res = KeyVal {
            key: t.to_bytes(),
            val: row,
        };
        Ok(res)
    }

    fn delete_row(&self, row_id: u64) -> Result<KeyVal, CubeError> {
        let t = RowKey::Table(self.table_id(), row_id);
        let res = KeyVal {
            key: t.to_bytes(),
            val: vec![],
        };
        Ok(res)
    }

    fn get_row_or_not_found(&self, row_id: u64) -> Result<IdRow<Self::T>, CubeError> {
        self.get_row(row_id)?.ok_or(CubeError::user(format!(
            "Row with id {} is not found for {:?}",
            row_id, self
        )))
    }

    fn get_row(&self, row_id: u64) -> Result<Option<IdRow<Self::T>>, CubeError> {
        let ref db = self.snapshot();
        let res = db.get(RowKey::Table(self.table_id(), row_id).to_bytes())?;

        if let Some(buffer) = res {
            let row = self.deserialize_id_row(row_id, buffer.as_slice())?;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn deserialize_id_row(&self, row_id: u64, buffer: &[u8]) -> Result<IdRow<Self::T>, CubeError> {
        let r = flexbuffers::Reader::get_root(&buffer).unwrap();
        let row = self.deserialize_row(r)?;
        return Ok(IdRow::new(row_id, row));
    }

    fn insert_index_row(&self, row: &Self::T, row_id: u64) -> Result<Vec<KeyVal>, CubeError> {
        let mut res = Vec::new();
        for index in Self::indexes().iter() {
            let hash = index.key_hash(&row);
            let index_val = index.index_key_by(&row);
            let key = RowKey::SecondaryIndex(
                self.index_id(index.get_id()),
                hash.to_be_bytes().to_vec(),
                row_id,
            );
            res.push(KeyVal {
                key: key.to_bytes(),
                val: index_val,
            });
        }
        Ok(res)
    }

    fn delete_index_row(&self, row: &Self::T, row_id: u64) -> Result<Vec<KeyVal>, CubeError> {
        let mut res = Vec::new();
        for index in Self::indexes().iter() {
            let hash = index.key_hash(&row);
            let key = RowKey::SecondaryIndex(
                self.index_id(index.get_id()),
                hash.to_be_bytes().to_vec(),
                row_id,
            );
            res.push(KeyVal {
                key: key.to_bytes(),
                val: vec![],
            });
        }

        Ok(res)
    }

    fn get_row_from_index(
        &self,
        secondary_id: u32,
        secondary_key_val: &Vec<u8>,
        secondary_key_hash: &Vec<u8>,
    ) -> Result<Vec<u64>, CubeError> {
        let ref db = self.snapshot();
        let key_len = secondary_key_hash.len();
        let key_min =
            RowKey::SecondaryIndex(self.index_id(secondary_id), secondary_key_hash.clone(), 0);

        let mut res: Vec<u64> = Vec::new();

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let iter = db.iterator_opt(
            IteratorMode::From(&key_min.to_bytes()[0..(key_len + 5)], Direction::Forward),
            opts,
        );

        for (key, value) in iter {
            if let RowKey::SecondaryIndex(_, secondary_index_hash, row_id) =
                RowKey::from_bytes(&key)
            {
                if !secondary_index_hash
                    .iter()
                    .zip(secondary_key_hash)
                    .all(|(a, b)| a == b)
                {
                    break;
                }

                if secondary_key_val.len() != value.len()
                    || !value.iter().zip(secondary_key_val).all(|(a, b)| a == b)
                {
                    continue;
                }
                res.push(row_id);
            };
        }
        Ok(res)
    }

    fn all_rows(&self) -> Result<Vec<IdRow<Self::T>>, CubeError> {
        let mut res = Vec::new();
        for row in self.table_scan(self.snapshot())? {
            res.push(row?);
        }
        Ok(res)
    }

    fn table_scan<'a>(&'a self, db: &'a Snapshot) -> Result<TableScanIter<'a, Self>, CubeError> {
        let my_table_id = self.table_id();
        let key_min = RowKey::Table(my_table_id, 0);

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let iterator = db.iterator_opt(
            IteratorMode::From(
                &key_min.to_bytes()[0..get_fixed_prefix()],
                Direction::Forward,
            ),
            opts,
        );

        Ok(TableScanIter {
            table_id: my_table_id,
            iter: iterator,
            table: self,
        })
    }

    fn build_path_rows<C: Clone, P>(
        &self,
        children: Vec<IdRow<C>>,
        mut parent_id_fn: impl FnMut(&IdRow<C>) -> u64,
        mut path_fn: impl FnMut(IdRow<C>, Arc<IdRow<Self::T>>) -> P,
    ) -> Result<Vec<P>, CubeError> {
        let id_to_child = children
            .into_iter()
            .map(|c| (parent_id_fn(&c), c))
            .collect::<Vec<_>>();
        let ids = id_to_child
            .iter()
            .map(|(id, _)| *id)
            .unique()
            .collect::<Vec<_>>();
        let rows = ids
            .into_iter()
            .map(|id| -> Result<(u64, Arc<IdRow<Self::T>>), CubeError> {
                Ok((id, Arc::new(self.get_row_or_not_found(id)?)))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(id_to_child
            .into_iter()
            .map(|(id, c)| path_fn(c, rows.get(&id).unwrap().clone()))
            .collect::<Vec<_>>())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum WriteBatchEntry {
    Put { key: Box<[u8]>, value: Box<[u8]> },
    Delete { key: Box<[u8]> },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct WriteBatchContainer {
    entries: Vec<WriteBatchEntry>,
}

impl WriteBatchContainer {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn write_batch(&self) -> WriteBatch {
        let mut batch = WriteBatch::default();
        for entry in self.entries.iter() {
            match entry {
                WriteBatchEntry::Put { key, value } => batch.put(key, value),
                WriteBatchEntry::Delete { key } => batch.delete(key),
            }
        }
        batch
    }

    async fn write_to_file(&self, file_name: &str) -> Result<(), CubeError> {
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut ser)?;
        let mut file = File::create(file_name).await?;
        Ok(tokio::io::AsyncWriteExt::write_all(&mut file, ser.view()).await?)
    }

    async fn read_from_file(file_name: &str) -> Result<Self, CubeError> {
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

fn meta_store_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
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

impl RocksMetaStore {
    pub fn with_listener(
        path: impl AsRef<Path>,
        listeners: Vec<Sender<MetaStoreEvent>>,
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Arc<RocksMetaStore> {
        let meta_store = RocksMetaStore::with_listener_impl(path, listeners, remote_fs, config);
        Arc::new(meta_store)
    }

    pub fn with_listener_impl(
        path: impl AsRef<Path>,
        listeners: Vec<Sender<MetaStoreEvent>>,
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
    ) -> RocksMetaStore {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(13));
        opts.set_merge_operator("meta_store merge", meta_store_merge, None);

        let db = DB::open(&opts, path).unwrap();
        let db_arc = Arc::new(db);

        let meta_store = RocksMetaStore {
            db: Arc::new(RwLock::new(db_arc.clone())),
            seq_store: Arc::new(Mutex::new(HashMap::new())),
            listeners: Arc::new(RwLock::new(listeners)),
            remote_fs,
            last_checkpoint_time: Arc::new(RwLock::new(SystemTime::now())),
            write_notify: Arc::new(Notify::new()),
            write_completed_notify: Arc::new(Notify::new()),
            last_upload_seq: Arc::new(RwLock::new(db_arc.latest_sequence_number())),
            last_check_seq: Arc::new(RwLock::new(db_arc.latest_sequence_number())),
            upload_loop: Arc::new(WorkerLoop::new("Meta Store Upload")),
            config,
        };
        meta_store
    }

    pub fn new(
        path: impl AsRef<Path>,
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Arc<RocksMetaStore> {
        Self::with_listener(path, vec![], remote_fs, config)
    }

    pub async fn load_from_remote(
        path: impl AsRef<Path>,
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<RocksMetaStore>, CubeError> {
        if !fs::metadata(path.as_ref()).await.is_ok() {
            let re = Regex::new(r"^metastore-(\d+)").unwrap();

            if remote_fs.list("metastore-current").await?.iter().len() > 0 {
                info!("Downloading remote metastore");
                let current_metastore_file = remote_fs.local_file("metastore-current").await?;
                if fs::metadata(current_metastore_file.as_str()).await.is_ok() {
                    fs::remove_file(current_metastore_file.as_str()).await?;
                }
                remote_fs.download_file("metastore-current").await?;

                let mut file = File::open(current_metastore_file.as_str()).await?;
                let mut buffer = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut file, &mut buffer).await?;
                let last_metastore_snapshot = {
                    let parse_result = re
                        .captures(&String::from_utf8(buffer)?)
                        .map(|c| c.get(1).unwrap().as_str())
                        .map(|p| u128::from_str(p));
                    if let Some(Ok(millis)) = parse_result {
                        Some(millis)
                    } else {
                        None
                    }
                };

                if let Some(snapshot) = last_metastore_snapshot {
                    let to_load = remote_fs.list(&format!("metastore-{}", snapshot)).await?;
                    let meta_store_path = remote_fs.local_file("metastore").await?;
                    fs::create_dir_all(meta_store_path.to_string()).await?;
                    for file in to_load.iter() {
                        remote_fs.download_file(file).await?;
                        let local = remote_fs.local_file(file).await?;
                        let path = Path::new(&local);
                        fs::copy(
                            path,
                            PathBuf::from(&meta_store_path)
                                .join(path.file_name().unwrap().to_str().unwrap()),
                        )
                        .await?;
                    }

                    let meta_store = Self::new(path.as_ref(), remote_fs.clone(), config);

                    let logs_to_batch = remote_fs
                        .list(&format!("metastore-{}-logs", snapshot))
                        .await?;
                    for log_file in logs_to_batch.iter() {
                        let path_to_log = remote_fs.local_file(log_file).await?;
                        let batch = WriteBatchContainer::read_from_file(&path_to_log).await;
                        if let Ok(batch) = batch {
                            let db = tokio::time::timeout(
                                Duration::from_secs(10),
                                meta_store.db.write(),
                            )
                            .map_err(|e| {
                                CubeError::internal(format!("Meta store load from remote: {}", e))
                            })
                            .await?;
                            db.write(batch.write_batch())?;
                        } else if let Err(e) = batch {
                            error!(
                                "Corrupted metastore WAL file. Discarding: {:?} {}",
                                log_file, e
                            );
                            break;
                        }
                    }

                    return Ok(meta_store);
                }
            } else {
                trace!("Can't find metastore-current in {:?}", remote_fs);
            }
            info!(
                "Creating metastore from scratch in {}",
                path.as_ref().as_os_str().to_string_lossy()
            );
        } else {
            info!(
                "Using existing metastore in {}",
                path.as_ref().as_os_str().to_string_lossy()
            );
        }

        Ok(Self::new(path, remote_fs, config))
    }

    pub async fn add_listener(&self, listener: Sender<MetaStoreEvent>) {
        self.listeners.write().await.push(listener);
    }

    async fn write_operation<F, R>(&self, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>, &'a mut BatchPipe) -> Result<R, CubeError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let db = tokio::time::timeout(Duration::from_secs(10), self.db.write())
            .map_err(|e| CubeError::internal(format!("Meta store write: {}", e)))
            .await?;
        let db_span = warn_long("metastore write operation", Duration::from_millis(100));
        let mem_seq = MemorySequence {
            seq_store: self.seq_store.clone(),
        };
        let db_to_send = db.clone();
        let (spawn_res, events) =
            tokio::task::spawn_blocking(move || -> Result<(R, Vec<MetaStoreEvent>), CubeError> {
                let mut batch = BatchPipe::new(db_to_send.as_ref());
                let snapshot = db_to_send.snapshot();
                let res = f(
                    DbTableRef {
                        db: db_to_send.as_ref(),
                        snapshot: &snapshot,
                        mem_seq,
                    },
                    &mut batch,
                )?;
                let write_result = batch.batch_write_rows()?;
                Ok((res, write_result))
            })
            .await??;

        mem::drop(db);
        mem::drop(db_span);

        self.write_notify.notify_waiters();

        for listener in self.listeners.read().await.clone().iter_mut() {
            for event in events.iter() {
                listener.send(event.clone())?;
            }
        }

        Ok(spawn_res)
    }

    pub async fn wait_upload_loop(meta_store: Arc<Self>) {
        meta_store
            .upload_loop
            .process(
                meta_store.clone(),
                async move |_| Ok(Delay::new(Duration::from_secs(60)).await),
                async move |m, _| m.run_upload().await,
            )
            .await;
    }

    pub async fn stop_processing_loops(&self) {
        self.upload_loop.stop();
    }

    pub async fn run_upload(&self) -> Result<(), CubeError> {
        let time = SystemTime::now();
        info!("Persisting meta store snapshot");
        let last_check_seq = self.last_check_seq().await;
        let last_db_seq = tokio::time::timeout(Duration::from_secs(10), self.db.read())
            .map_err(|e| CubeError::internal(format!("Meta store upload: {}", e)))
            .await?
            .latest_sequence_number();
        if last_check_seq == last_db_seq {
            return Ok(());
        }
        let last_upload_seq = self.last_upload_seq().await;
        let (serializer, min, max) = {
            let updates = tokio::time::timeout(Duration::from_secs(10), self.db.write())
                .map_err(|e| CubeError::internal(format!("Meta store upload: {}", e)))
                .await?
                .get_updates_since(last_upload_seq)?;
            let mut serializer = WriteBatchContainer::new();

            let mut seq_numbers = Vec::new();

            updates.into_iter().for_each(|(n, write_batch)| {
                seq_numbers.push(n);
                write_batch.iterate(&mut serializer);
            });
            (
                serializer,
                seq_numbers.iter().min().map(|v| *v),
                seq_numbers.iter().max().map(|v| *v),
            )
        };

        if max.is_some() {
            let checkpoint_time = self.last_checkpoint_time.read().await;
            let log_name = format!(
                "{}-logs/{}.flex",
                RocksMetaStore::meta_store_path(&checkpoint_time),
                min.unwrap()
            );
            let file_name = self.remote_fs.local_file(&log_name).await?;
            serializer.write_to_file(&file_name).await?;
            self.remote_fs.upload_file(&file_name, &log_name).await?;
            let mut seq = self.last_upload_seq.write().await;
            *seq = max.unwrap();
            self.write_completed_notify.notify_waiters();
        }

        let last_checkpoint_time: SystemTime = self.last_checkpoint_time.read().await.clone();
        if last_checkpoint_time + time::Duration::from_secs(300) < SystemTime::now() {
            info!("Uploading meta store check point");
            self.upload_check_point().await?;
        }

        let mut check_seq = self.last_check_seq.write().await;
        *check_seq = last_db_seq;

        info!(
            "Persisting meta store snapshot: done ({:?})",
            time.elapsed()?
        );

        Ok(())
    }

    async fn upload_check_point(&self) -> Result<(), CubeError> {
        let mut check_point_time = self.last_checkpoint_time.write().await;
        let remote_fs = self.remote_fs.clone();

        let (remote_path, checkpoint_path) = {
            let db = tokio::time::timeout(Duration::from_secs(10), self.db.write())
                .map_err(|e| CubeError::internal(format!("Meta store upload checkpoint: {}", e)))
                .await?
                .clone();
            *check_point_time = SystemTime::now();
            RocksMetaStore::prepare_checkpoint(db, &check_point_time).await?
        };

        RocksMetaStore::upload_checkpoint(remote_fs, remote_path, checkpoint_path).await?;
        self.write_completed_notify.notify_waiters();
        Ok(())
    }

    async fn last_upload_seq(&self) -> u64 {
        *self.last_upload_seq.read().await
    }

    async fn last_check_seq(&self) -> u64 {
        *self.last_check_seq.read().await
    }

    async fn upload_checkpoint(
        remote_fs: Arc<dyn RemoteFs>,
        remote_path: String,
        checkpoint_path: PathBuf,
    ) -> Result<(), CubeError> {
        let mut dir = fs::read_dir(checkpoint_path).await?;

        let mut files_to_upload = Vec::new();
        while let Some(file) = dir.next_entry().await? {
            let file = file.file_name();
            files_to_upload.push(format!("{}/{}", remote_path, file.to_string_lossy()));
        }
        for v in join_all(
            files_to_upload
                .into_iter()
                .map(|f| {
                    let remote_fs = remote_fs.clone();
                    return async move {
                        let local = remote_fs.local_file(&f).await?;
                        remote_fs.upload_file(&local, &f).await
                    };
                })
                .collect::<Vec<_>>(),
        )
        .await
        .into_iter()
        {
            v?;
        }

        let existing_metastore_files = remote_fs.list("metastore-").await?;
        let to_delete = existing_metastore_files
            .into_iter()
            .filter_map(|existing| {
                let path = existing
                    .split("/")
                    .nth(0)
                    .map(|p| u128::from_str(&p.replace("metastore-", "").replace("-logs", "")));
                if let Some(Ok(millis)) = path {
                    if SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        - millis
                        > 3 * 60 * 1000
                    {
                        return Some(existing);
                    }
                }
                None
            })
            .collect::<Vec<_>>();
        for v in join_all(
            to_delete
                .iter()
                .map(|f| remote_fs.delete_file(&f))
                .collect::<Vec<_>>(),
        )
        .await
        .into_iter()
        {
            v?;
        }

        let current_metastore_file = remote_fs.local_file("metastore-current").await?;

        {
            let mut file = File::create(&current_metastore_file).await?;
            tokio::io::AsyncWriteExt::write_all(&mut file, remote_path.as_bytes()).await?;
        }

        remote_fs
            .upload_file(&current_metastore_file, "metastore-current")
            .await?;

        Ok(())
    }

    async fn prepare_checkpoint(
        db: Arc<DB>,
        checkpoint_time: &SystemTime,
    ) -> Result<(String, PathBuf), CubeError> {
        let remote_path = RocksMetaStore::meta_store_path(checkpoint_time);
        let checkpoint_path = db.path().join("..").join(remote_path.clone());
        let path_to_move = checkpoint_path.clone();
        tokio::task::spawn_blocking(move || -> Result<(), CubeError> {
            let checkpoint = Checkpoint::new(db.as_ref())?;
            checkpoint.create_checkpoint(path_to_move.as_path())?;
            Ok(())
        })
        .await??;
        Ok((remote_path, checkpoint_path))
    }

    fn meta_store_path(checkpoint_time: &SystemTime) -> String {
        format!(
            "metastore-{}",
            checkpoint_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        )
    }

    async fn read_operation<F, R>(&self, f: F) -> Result<R, CubeError>
    where
        F: for<'a> FnOnce(DbTableRef<'a>) -> Result<R, CubeError> + Send + 'static,
        R: Send + 'static,
    {
        let db = tokio::time::timeout(
            Duration::from_secs(10),
            warn_long_fut(
                "metastore acquire read lock",
                Duration::from_millis(50),
                self.db.read(),
            ),
        )
        .map_err(|e| CubeError::internal(format!("Meta store read: {}", e)))
        .await?;
        let mem_seq = MemorySequence {
            seq_store: self.seq_store.clone(),
        };
        let db_to_send = db.clone();
        let res = tokio::task::spawn_blocking(move || {
            let snapshot = db_to_send.snapshot();
            f(DbTableRef {
                db: db_to_send.as_ref(),
                snapshot: &snapshot,
                mem_seq,
            })
        })
        .await?;

        mem::drop(db);

        res
    }

    fn check_if_exists(name: &String, existing_keys_len: usize) -> Result<(), CubeError> {
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

    pub fn prepare_test_metastore(test_name: &str) -> (Arc<LocalDirRemoteFs>, Arc<RocksMetaStore>) {
        let config = Config::test(test_name);
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-local", test_name));
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-remote", test_name));
        let _ = std::fs::remove_dir_all(store_path.clone());
        let _ = std::fs::remove_dir_all(remote_store_path.clone());
        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        let meta_store = RocksMetaStore::new(
            store_path.clone().join("metastore").as_path(),
            remote_fs.clone(),
            config.config_obj(),
        );
        (remote_fs, meta_store)
    }

    pub fn cleanup_test_metastore(test_name: &str) {
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-local", test_name));
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("test-{}-remote", test_name));
        let _ = std::fs::remove_dir_all(store_path.clone());
        let _ = std::fs::remove_dir_all(remote_store_path.clone());
    }

    async fn has_pending_changes(&self) -> Result<bool, CubeError> {
        let db = tokio::time::timeout(Duration::from_secs(10), self.db.read())
            .map_err(|e| CubeError::internal(format!("Meta store has pending changes: {}", e)))
            .await?;
        Ok(db
            .get_updates_since(self.last_upload_seq().await)?
            .next()
            .is_some())
    }
}

impl RocksMetaStore {
    fn add_index(
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
            .find(|dc| table_cols.iter().all(|c| c.name.as_str() != dc.as_str()))
        {
            return Err(CubeError::user(format!(
                "Column {} in index {} not found in table {}",
                not_found,
                index_def.name,
                table_id.get_row().get_table_name()
            )));
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

        let sorted_key_size = index_columns.len() as u64;
        // Put the rest of the columns.
        for i in 0..table_cols.len() {
            if taken[i] {
                continue;
            }

            index_columns.push(table_cols[i].clone().replace_index(index_columns.len()));
        }
        assert_eq!(index_columns.len(), table_cols.len());

        let index = Index::try_new(
            index_def.name,
            table_id.get_id(),
            index_columns,
            sorted_key_size,
        )?;
        let index_id = rocks_index.insert(index, batch_pipe)?;
        let partition = Partition::new(index_id.id, None, None);
        let _ = rocks_partition.insert(partition, batch_pipe)?;
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

    fn chunks_by_partitioned_with_non_repartitioned(
        partition_id: u64,
        table: &ChunkRocksTable,
        partition_table: &PartitionRocksTable,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        let mut partitions_up_to_root = Vec::new();
        let mut current_partition = partition_table.get_row_or_not_found(partition_id)?;
        partitions_up_to_root.push(current_partition.get_id());
        while let Some(parent_id) = current_partition.get_row().parent_partition_id() {
            let parent = partition_table.get_row_or_not_found(*parent_id)?;
            partitions_up_to_root.push(parent.get_id());
            current_partition = parent;
        }

        let mut chunks = Vec::new();

        for partition_id in partitions_up_to_root.into_iter() {
            chunks.extend(
                table
                    .get_rows_by_index(
                        &ChunkIndexKey::ByPartitionId(partition_id),
                        &ChunkRocksIndex::PartitionId,
                    )?
                    .into_iter()
                    .filter(|c| c.get_row().uploaded() && c.get_row().active()),
            );
        }

        Ok(chunks)
    }

    // Must be run under write_operation(). Returns activated row count.
    fn activate_chunks_impl(
        db_ref: DbTableRef,
        batch_pipe: &mut BatchPipe,
        uploaded_chunk_ids: &[u64],
    ) -> Result<u64, CubeError> {
        let table = ChunkRocksTable::new(db_ref.clone());
        let mut activated_row_count = 0;
        for id in uploaded_chunk_ids {
            activated_row_count += table.get_row_or_not_found(*id)?.get_row().get_row_count();
            table.update_with_fn(*id, |row| row.set_uploaded(true), batch_pipe)?;
        }
        return Ok(activated_row_count);
    }
}

#[async_trait]
impl MetaStore for RocksMetaStore {
    async fn wait_for_current_seq_to_sync(&self) -> Result<(), CubeError> {
        while self.has_pending_changes().await? {
            tokio::time::timeout(
                Duration::from_secs(30),
                self.write_completed_notify.notified(),
            )
            .await?;
        }
        Ok(())
    }

    fn schemas_table(&self) -> SchemaMetaStoreTable {
        SchemaMetaStoreTable {
            rocks_meta_store: self.clone(),
        }
    }

    async fn create_schema(
        &self,
        schema_name: String,
        if_not_exists: bool,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
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

    async fn get_schemas(&self) -> Result<Vec<IdRow<Schema>>, CubeError> {
        self.read_operation(move |db_ref| SchemaRocksTable::new(db_ref).all_rows())
            .await
    }

    async fn get_schema_by_id(&self, schema_id: u64) -> Result<IdRow<Schema>, CubeError> {
        self.read_operation(move |db_ref| {
            let table = SchemaRocksTable::new(db_ref);
            table.get_row_or_not_found(schema_id)
        })
        .await
    }

    async fn get_schema_id(&self, schema_name: String) -> Result<u64, CubeError> {
        self.read_operation(move |db_ref| {
            let table = SchemaRocksTable::new(db_ref);
            let existing_keys =
                table.get_row_ids_by_index(&schema_name, &SchemaRocksIndex::Name)?;
            RocksMetaStore::check_if_exists(&schema_name, existing_keys.len())?;
            Ok(existing_keys[0])
        })
        .await
    }

    async fn get_schema(&self, schema_name: String) -> Result<IdRow<Schema>, CubeError> {
        self.read_operation(move |db_ref| {
            let table = SchemaRocksTable::new(db_ref);
            Ok(table.get_single_row_by_index(&schema_name, &SchemaRocksIndex::Name)?)
        })
        .await
    }

    async fn rename_schema(
        &self,
        old_schema_name: String,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = SchemaRocksTable::new(db_ref.clone());
            let existing_keys =
                table.get_row_ids_by_index(&old_schema_name, &SchemaRocksIndex::Name)?;
            RocksMetaStore::check_if_exists(&old_schema_name, existing_keys.len())?;

            let schema_id = existing_keys[0];

            let old_schema = table.get_row(schema_id)?.unwrap();
            let mut new_schema = old_schema.clone();
            new_schema.row.set_name(&new_schema_name);
            let id_row = table.update(schema_id, new_schema.row, &old_schema.row, batch_pipe)?;
            Ok(id_row)
        })
        .await
    }

    async fn rename_schema_by_id(
        &self,
        schema_id: u64,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = SchemaRocksTable::new(db_ref.clone());

            let old_schema = table.get_row(schema_id)?.unwrap();
            let mut new_schema = old_schema.clone();
            new_schema.row.set_name(&new_schema_name);
            let id_row = table.update(schema_id, new_schema.row, &old_schema.row, batch_pipe)?;

            Ok(id_row)
        })
        .await
    }

    async fn delete_schema(&self, schema_name: String) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = SchemaRocksTable::new(db_ref.clone());
            let existing_keys =
                table.get_row_ids_by_index(&schema_name, &SchemaRocksIndex::Name)?;
            RocksMetaStore::check_if_exists(&schema_name, existing_keys.len())?;
            let schema_id = existing_keys[0];

            table.delete(schema_id, batch_pipe)?;

            Ok(())
        })
        .await
    }

    async fn delete_schema_by_id(&self, schema_id: u64) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = SchemaRocksTable::new(db_ref.clone());
            table.delete(schema_id, batch_pipe)?;

            Ok(())
        })
        .await
    }

    fn tables_table(&self) -> TableMetaStoreTable {
        TableMetaStoreTable {
            rocks_meta_store: self.clone(),
        }
    }

    async fn create_table(
        &self,
        schema_name: String,
        table_name: String,
        columns: Vec<Column>,
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        indexes: Vec<IndexDef>,
    ) -> Result<IdRow<Table>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let rocks_table = TableRocksTable::new(db_ref.clone());
            let rocks_index = IndexRocksTable::new(db_ref.clone());
            let rocks_schema = SchemaRocksTable::new(db_ref.clone());
            let rocks_partition = PartitionRocksTable::new(db_ref.clone());

            let schema_id =
                rocks_schema.get_single_row_by_index(&schema_name, &SchemaRocksIndex::Name)?;
            let index_cols = columns.clone();
            let table = Table::new(
                table_name,
                schema_id.get_id(),
                columns,
                locations,
                import_format,
            );
            let table_id = rocks_table.insert(table, batch_pipe)?;
            for index_def in indexes.into_iter() {
                RocksMetaStore::add_index(
                    batch_pipe,
                    &rocks_index,
                    &rocks_partition,
                    &index_cols,
                    &table_id,
                    index_def,
                )?;
            }

            let (mut sorted, mut unsorted) =
                index_cols.clone().into_iter().partition::<Vec<_>, _>(|c| {
                    match c.get_column_type() {
                        ColumnType::Decimal { .. } | ColumnType::Bytes | ColumnType::Float => false,
                        _ => true,
                    }
                });

            let sorted_key_size = sorted.len() as u64;
            sorted.append(&mut unsorted);

            let index = Index::try_new(
                "default".to_string(),
                table_id.get_id(),
                sorted
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| c.replace_index(i))
                    .collect::<Vec<_>>(),
                sorted_key_size,
            )?;
            let index_id = rocks_index.insert(index, batch_pipe)?;
            let partition = Partition::new(index_id.id, None, None);
            let _ = rocks_partition.insert(partition, batch_pipe)?;

            Ok(table_id)
        })
        .await
    }

    async fn get_table(
        &self,
        schema_name: String,
        table_name: String,
    ) -> Result<IdRow<Table>, CubeError> {
        self.read_operation(move |db_ref| {
            let rocks_table = TableRocksTable::new(db_ref.clone());
            let rocks_schema = SchemaRocksTable::new(db_ref);
            let table = RocksMetaStore::get_table_by_name(
                schema_name,
                table_name,
                rocks_table,
                rocks_schema,
            )?;
            Ok(table)
        })
        .await
    }

    async fn get_table_by_id(&self, table_id: u64) -> Result<IdRow<Table>, CubeError> {
        self.read_operation(move |db_ref| {
            TableRocksTable::new(db_ref.clone()).get_row_or_not_found(table_id)
        })
        .await
    }

    async fn get_tables(&self) -> Result<Vec<IdRow<Table>>, CubeError> {
        self.read_operation(|db_ref| TableRocksTable::new(db_ref).all_rows())
            .await
    }

    async fn get_tables_with_path(&self) -> Result<Vec<TablePath>, CubeError> {
        self.read_operation(|db_ref| {
            let tables = TableRocksTable::new(db_ref.clone()).all_rows()?;
            let schemas = SchemaRocksTable::new(db_ref);
            Ok(schemas.build_path_rows(
                tables,
                |t| t.get_row().get_schema_id(),
                |table, schema| TablePath { table, schema },
            )?)
        })
        .await
    }

    async fn drop_table(&self, table_id: u64) -> Result<IdRow<Table>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let tables_table = TableRocksTable::new(db_ref.clone());
            let indexes_table = IndexRocksTable::new(db_ref.clone());
            let partitions_table = PartitionRocksTable::new(db_ref.clone());
            let chunks_table = ChunkRocksTable::new(db_ref);

            let indexes = indexes_table
                .get_rows_by_index(&IndexIndexKey::TableId(table_id), &IndexRocksIndex::TableID)?;
            for index in indexes.into_iter() {
                let partitions = partitions_table.get_rows_by_index(
                    &PartitionIndexKey::ByIndexId(index.get_id()),
                    &PartitionRocksIndex::IndexId,
                )?;
                for partition in partitions.into_iter() {
                    let chunks = chunks_table.get_rows_by_index(
                        &ChunkIndexKey::ByPartitionId(partition.get_id()),
                        &ChunkRocksIndex::PartitionId,
                    )?;
                    for chunk in chunks.into_iter() {
                        chunks_table.delete(chunk.get_id(), batch_pipe)?;
                    }
                    partitions_table.delete(partition.get_id(), batch_pipe)?;
                }
                indexes_table.delete(index.get_id(), batch_pipe)?;
            }
            Ok(tables_table.delete(table_id, batch_pipe)?)
        })
        .await
    }

    fn partition_table(&self) -> PartitionMetaStoreTable {
        PartitionMetaStoreTable {
            rocks_meta_store: self.clone(),
        }
    }

    async fn create_partition(&self, partition: Partition) -> Result<IdRow<Partition>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = PartitionRocksTable::new(db_ref.clone());
            let row_id = table.insert(partition, batch_pipe)?;
            Ok(row_id)
        })
        .await
    }

    async fn get_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError> {
        self.read_operation(move |db_ref| {
            PartitionRocksTable::new(db_ref).get_row_or_not_found(partition_id)
        })
        .await
    }

    async fn get_partition_for_compaction(
        &self,
        partition_id: u64,
    ) -> Result<(IdRow<Partition>, IdRow<Index>), CubeError> {
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
            if !partition.get_row().is_active() {
                return Err(CubeError::internal(format!(
                    "Cannot compact inactive partition: {:?}",
                    partition.get_row()
                )));
            }
            Ok((partition, index))
        })
        .await
    }

    async fn get_partition_chunk_sizes(&self, partition_id: u64) -> Result<u64, CubeError> {
        let chunks = self.get_chunks_by_partition(partition_id, false).await?;
        Ok(chunks.iter().map(|r| r.get_row().row_count).sum())
    }

    async fn swap_active_partitions(
        &self,
        current_active: Vec<u64>,
        new_active: Vec<u64>,
        compacted_chunk_ids: Vec<u64>,
        new_active_min_max: Vec<(u64, (Option<Row>, Option<Row>))>,
    ) -> Result<(), CubeError> {
        trace!(
            "Swapping partitions: deactivating ({}), deactivating chunks ({}), activating ({})",
            current_active.iter().join(", "),
            compacted_chunk_ids.iter().join(", "),
            new_active.iter().join(", ")
        );
        self.write_operation(move |db_ref, batch_pipe| {
            let table = PartitionRocksTable::new(db_ref.clone());
            let chunk_table = ChunkRocksTable::new(db_ref.clone());

            let mut deactivated_row_count = 0;
            let mut activated_row_count = 0;

            for current in current_active.iter() {
                let current_partition =
                    table.get_row(*current)?.ok_or(CubeError::internal(format!(
                        "Current partition is not found during swap active: {}",
                        current
                    )))?;
                // TODO this check is not atomic
                // TODO Swapping partitions: deactivating (34), deactivating chunks (404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414), activating (35)
                // TODO Swapping partitions: deactivating (34), deactivating chunks (404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414), activating (36)
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
                deactivated_row_count += current_partition.get_row().main_table_row_count()
            }

            for (new, (count, (min_value, max_value))) in
                new_active.iter().zip(new_active_min_max.into_iter())
            {
                let new_partition = table.get_row(*new)?.ok_or(CubeError::internal(format!(
                    "New partition is not found during swap active: {}",
                    new
                )))?;
                if new_partition.get_row().is_active() {
                    return Err(CubeError::internal(format!(
                        "New partition is already active: {:?}",
                        new_partition.get_row()
                    )));
                }
                table.update(
                    new_partition.get_id(),
                    new_partition
                        .get_row()
                        .to_active(true)
                        .update_min_max_and_row_count(min_value, max_value, count),
                    new_partition.get_row(),
                    batch_pipe,
                )?;
                activated_row_count += count;
            }

            for chunk_id in compacted_chunk_ids.iter() {
                deactivated_row_count += chunk_table.get_row_or_not_found(*chunk_id)?.get_row().get_row_count();
                chunk_table.update_with_fn(*chunk_id, |row| row.deactivate(), batch_pipe)?;
            }

            if activated_row_count != deactivated_row_count {
                return Err(CubeError::internal(format!(
                    "Deactivated row count ({}) doesn't match activated row count ({}) during swap of partition ({}) and ({}) chunks to new partitions ({})",
                    deactivated_row_count,
                    activated_row_count,
                    current_active.iter().join(", "),
                    compacted_chunk_ids.iter().join(", "),
                    new_active.iter().join(", ")
                )))
            }

            Ok(())
        })
        .await
    }

    async fn is_partition_used(&self, partition_id: u64) -> Result<bool, CubeError> {
        let timeout = self.config.not_used_timeout();
        self.read_operation(move |db_ref| {
            let table = PartitionRocksTable::new(db_ref);
            let partition = table.get_row_or_not_found(partition_id)?;
            Ok(partition.get_row().is_used(timeout))
        })
        .await
    }

    async fn delete_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            PartitionRocksTable::new(db_ref).delete(partition_id, batch_pipe)
        })
        .await
    }

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

    fn index_table(&self) -> IndexMetaStoreTable {
        IndexMetaStoreTable {
            rocks_meta_store: self.clone(),
        }
    }

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
                index_def,
            )?)
        })
        .await
    }

    async fn get_default_index(&self, table_id: u64) -> Result<IdRow<Index>, CubeError> {
        self.read_operation(move |db_ref| {
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
        })
        .await
    }

    async fn get_table_indexes(&self, table_id: u64) -> Result<Vec<IdRow<Index>>, CubeError> {
        self.read_operation(move |db_ref| {
            let index_table = IndexRocksTable::new(db_ref);
            Ok(index_table
                .get_rows_by_index(&IndexIndexKey::TableId(table_id), &IndexRocksIndex::TableID)?)
        })
        .await
    }

    async fn get_active_partitions_by_index_id(
        &self,
        index_id: u64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.read_operation(move |db_ref| {
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

    async fn get_index(&self, index_id: u64) -> Result<IdRow<Index>, CubeError> {
        self.read_operation(move |db_ref| {
            IndexRocksTable::new(db_ref).get_row_or_not_found(index_id)
        })
        .await
    }

    async fn get_active_partitions_and_chunks_by_index_id_for_select(
        &self,
        index_id: u64,
    ) -> Result<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let rocks_chunk = ChunkRocksTable::new(db_ref.clone());
            let rocks_partition = PartitionRocksTable::new(db_ref);
            // TODO iterate over range
            let result = rocks_partition
                .get_rows_by_index(
                    &PartitionIndexKey::ByIndexId(index_id),
                    &PartitionRocksIndex::IndexId,
                )?
                .into_iter()
                .filter(|r| r.get_row().active)
                .map(|p| -> Result<_, CubeError> {
                    let chunks = Self::chunks_by_partitioned_with_non_repartitioned(
                        p.get_id(),
                        &rocks_chunk,
                        &rocks_partition,
                    )?;
                    Ok((p, chunks))
                })
                .collect::<Result<Vec<_>, _>>()?;

            // update last used
            for (partition, chunks) in result.iter() {
                rocks_partition.update_with_fn(
                    partition.get_id(),
                    |p| p.update_last_used(),
                    batch_pipe,
                )?;
                for chunk in chunks.iter() {
                    rocks_chunk.update_with_fn(
                        chunk.get_id(),
                        |c| c.update_last_used(),
                        batch_pipe,
                    )?;
                }
            }

            Ok(result)
        })
        .await
    }

    async fn create_chunk(
        &self,
        partition_id: u64,
        row_count: usize,
    ) -> Result<IdRow<Chunk>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let rocks_chunk = ChunkRocksTable::new(db_ref.clone());

            let chunk = Chunk::new(partition_id, row_count);
            let id_row = rocks_chunk.insert(chunk, batch_pipe)?;

            Ok(id_row)
        })
        .await
    }

    async fn get_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        self.read_operation(move |db_ref| {
            ChunkRocksTable::new(db_ref).get_row_or_not_found(chunk_id)
        })
        .await
    }

    async fn get_chunks_by_partition(
        &self,
        partition_id: u64,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.read_operation(move |db_ref| {
            let table = ChunkRocksTable::new(db_ref);
            Ok(table
                .get_rows_by_index(
                    &ChunkIndexKey::ByPartitionId(partition_id),
                    &ChunkRocksIndex::PartitionId,
                )?
                .into_iter()
                .filter(|c| include_inactive || c.get_row().uploaded() && c.get_row().active())
                .collect::<Vec<_>>())
        })
        .await
    }

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

    async fn activate_wal(
        &self,
        wal_id_to_delete: u64,
        uploaded_ids: Vec<u64>,
        index_count: u64,
    ) -> Result<(), CubeError> {
        trace!(
            "Swapping chunks: deleting WAL ({}), activating chunks ({})",
            wal_id_to_delete,
            uploaded_ids.iter().join(", ")
        );
        self.write_operation(move |db_ref, batch_pipe| {
            let wal_table = WALRocksTable::new(db_ref.clone());

            let deactivated_row_count = wal_table.get_row_or_not_found(wal_id_to_delete)?.get_row().get_row_count();
            wal_table.delete(wal_id_to_delete, batch_pipe)?;

            let activated_row_count = Self::activate_chunks_impl(db_ref, batch_pipe, &uploaded_ids)?;

            if activated_row_count != deactivated_row_count * index_count {
                return Err(CubeError::internal(format!(
                    "Deactivated WAL row count ({}) doesn't match activated row count ({}) during swap of ({}) to ({}) chunks",
                    deactivated_row_count,
                    activated_row_count,
                    wal_id_to_delete,
                    uploaded_ids.iter().join(", ")
                )))
            }
            Ok(())
        })
        .await
    }

    async fn activate_chunks(
        &self,
        table_id: u64,
        uploaded_chunk_ids: Vec<u64>,
    ) -> Result<(), CubeError> {
        trace!(
            "Activating chunks ({})",
            uploaded_chunk_ids.iter().join(", ")
        );
        self.write_operation(move |db_ref, batch_pipe| {
            TableRocksTable::new(db_ref.clone()).update_with_fn(
                table_id,
                |t| t.update_has_data(true),
                batch_pipe,
            )?;
            Self::activate_chunks_impl(db_ref, batch_pipe, &uploaded_chunk_ids)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn swap_chunks(
        &self,
        deactivate_ids: Vec<u64>,
        uploaded_ids: Vec<u64>,
    ) -> Result<(), CubeError> {
        trace!(
            "Swapping chunks: deactivating ({}), activating ({})",
            deactivate_ids.iter().join(", "),
            uploaded_ids.iter().join(", ")
        );
        self.write_operation(move |db_ref, batch_pipe| {
            let table = ChunkRocksTable::new(db_ref.clone());
            let mut deactivated_row_count = 0;
            let mut activated_row_count = 0;
            for id in deactivate_ids.iter() {
                deactivated_row_count += table.get_row_or_not_found(*id)?.get_row().get_row_count();
                table.update_with_fn(*id, |row| row.deactivate(), batch_pipe)?;
            }
            for id in uploaded_ids.iter() {
                activated_row_count += table.get_row_or_not_found(*id)?.get_row().get_row_count();
                table.update_with_fn(*id, |row| row.set_uploaded(true), batch_pipe)?;
            }
            if deactivate_ids.len() > 0 && activated_row_count != deactivated_row_count {
                return Err(CubeError::internal(format!(
                    "Deactivated row count ({}) doesn't match activated row count ({}) during swap of ({}) to ({}) chunks",
                    deactivated_row_count,
                    activated_row_count,
                    deactivate_ids.iter().join(", "),
                    uploaded_ids.iter().join(", ")
                )))
            }
            Ok(())
        })
            .await
    }

    async fn is_chunk_used(&self, chunk_id: u64) -> Result<bool, CubeError> {
        let timeout = self.config.not_used_timeout();
        self.read_operation(move |db_ref| {
            let table = ChunkRocksTable::new(db_ref);
            let chunk = table.get_row_or_not_found(chunk_id)?;
            Ok(chunk.get_row().is_used(timeout))
        })
        .await
    }

    async fn delete_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        let timeout = self.config.not_used_timeout();
        self.write_operation(move |db_ref, batch_pipe| {
            let chunks = ChunkRocksTable::new(db_ref.clone());
            let chunk = chunks.get_row_or_not_found(chunk_id)?;

            if chunk.get_row().is_used(timeout) {
                return Err(CubeError::internal(format!(
                    "Can't remove used in select chunk #{}",
                    chunk_id
                )));
            }

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

    fn chunks_table(&self) -> ChunkMetaStoreTable {
        ChunkMetaStoreTable {
            rocks_meta_store: self.clone(),
        }
    }

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

    async fn get_wal(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError> {
        self.read_operation(move |db_ref| WALRocksTable::new(db_ref).get_row_or_not_found(wal_id))
            .await
    }

    async fn get_wals_for_table(&self, table_id: u64) -> Result<Vec<IdRow<WAL>>, CubeError> {
        self.read_operation(move |db_ref| {
            WALRocksTable::new(db_ref)
                .get_rows_by_index(&WALIndexKey::ByTable(table_id), &WALRocksIndex::TableID)
        })
        .await
    }

    async fn delete_wal(&self, wal_id: u64) -> Result<(), CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            WALRocksTable::new(db_ref.clone()).delete(wal_id, batch_pipe)?;
            Ok(())
        })
        .await
    }

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

    async fn get_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        self.read_operation(move |db_ref| {
            Ok(JobRocksTable::new(db_ref).get_row_or_not_found(job_id)?)
        })
        .await
    }

    async fn delete_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            Ok(JobRocksTable::new(db_ref.clone()).delete(job_id, batch_pipe)?)
        })
        .await
    }

    async fn start_processing_job(
        &self,
        server_name: String,
    ) -> Result<Option<IdRow<Job>>, CubeError> {
        self.write_operation(move |db_ref, batch_pipe| {
            let table = JobRocksTable::new(db_ref);
            let next_job = table
                .get_rows_by_index(
                    &JobIndexKey::ScheduledByShard(Some(server_name.to_string())),
                    &JobRocksIndex::ByShard,
                )?
                .into_iter()
                .nth(0);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::remotefs::LocalDirRemoteFs;
    use futures_timer::Delay;
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
                remote_fs,
                config.config_obj(),
            );

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
                remote_fs,
                config.config_obj(),
            );

            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();

            meta_store
                .db
                .write()
                .await
                .delete(RowKey::Table(TableId::Schemas, 1).to_bytes())
                .unwrap();

            let result = meta_store.get_schema("foo".to_string()).await;
            println!("{:?}", result);
            assert_eq!(result.is_err(), true);

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
                remote_fs,
                config.config_obj(),
            );

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

            let table1 = meta_store
                .create_table(
                    "foo".to_string(),
                    "boo".to_string(),
                    columns.clone(),
                    None,
                    None,
                    vec![],
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
                    vec![]
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
                columns.len() as u64 - 1,
            )
            .unwrap();
            let expected_res = vec![IdRow::new(1, expected_index)];
            assert_eq!(meta_store.get_table_indexes(1).await.unwrap(), expected_res);
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
}
