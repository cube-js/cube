pub mod compaction;

use async_trait::async_trait;
use datafusion::arrow::compute::{concat_batches, SortOptions};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::get_reader_options_customizer;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::common::collect as common_collect;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column as FusionColumn;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use serde::{de, Deserialize, Serialize};
extern crate bincode;

use bincode::{deserialize_from, serialize_into};

use crate::metastore::{
    deactivate_table_due_to_corrupt_data, deactivate_table_on_corrupt_data, table::Table, Chunk,
    Column, ColumnType, IdRow, Index, IndexType, MetaStore, Partition, WAL,
};
use crate::queryplanner::{try_make_memory_data_source, QueryPlannerImpl};
use crate::remotefs::{ensure_temp_file_is_dropped, RemoteFs};
use crate::table::{Row, TableValue};
use crate::util::batch_memory::columns_vec_buffer_size;
use crate::CubeError;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    sync::Arc,
};

use crate::app_metrics;
use crate::cluster::{node_name_by_partition, Cluster};
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::chunks::chunk_file_name;
use crate::queryplanner::trace_data_loaded::{DataLoadedSize, TraceDataLoadedExec};
use crate::table::data::{cmp_min_rows, cmp_partition_key};
use crate::table::parquet::{arrow_schema, CubestoreMetadataCacheFactory, ParquetTableStore};
use compaction::{merge_chunks, merge_replay_handles, write_chunks_split_into_children};
use datafusion::arrow::array::{Array, ArrayRef, Int64Builder, StringBuilder, UInt64Array};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::cube_ext;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use deepsize::DeepSizeOf;
use futures::future::join_all;
use itertools::Itertools;
use log::trace;
use mockall::automock;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

pub const ROW_GROUP_SIZE: usize = 16384; // TODO config

#[derive(Serialize, Deserialize, Hash, Eq, PartialEq, Debug, DeepSizeOf)]
pub struct DataFrame {
    columns: Vec<Column>,
    data: Vec<Row>,
}

impl DataFrame {
    pub fn new(columns: Vec<Column>, data: Vec<Row>) -> DataFrame {
        DataFrame { columns, data }
    }

    pub fn empty() -> DataFrame {
        DataFrame {
            columns: vec![],
            data: vec![],
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<Field>>(),
        ))
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_rows(&self) -> &Vec<Row> {
        &self.data
    }

    pub fn print(&self) -> String {
        use comfy_table::{Cell, Table};

        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        table.set_header(self.columns.iter().map(|c| Cell::new(c.get_name())));

        for row in &self.data {
            table.add_row(
                self.columns
                    .iter()
                    .zip(row.values().iter())
                    .map(|(col, value)| value.format_with(col.get_column_type())),
            );
        }

        table.trim_fmt()
    }

    pub fn to_execution_plan(
        &self,
        columns: &Vec<Column>,
    ) -> Result<Arc<dyn ExecutionPlan + Send + Sync>, CubeError> {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<Field>>(),
        ));

        let mut column_values: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

        for c in columns.iter() {
            match c.get_column_type() {
                ColumnType::String => {
                    let mut column = StringBuilder::new();
                    for i in 0..self.data.len() {
                        let value = &self.data[i].values()[c.get_index()];
                        if let TableValue::String(v) = value {
                            column.append_value(v.as_str());
                        } else {
                            panic!("Unexpected value: {:?}", value);
                        }
                    }
                    column_values.push(Arc::new(column.finish()));
                }
                ColumnType::Int => {
                    let mut column = Int64Builder::new();
                    for i in 0..self.data.len() {
                        let value = &self.data[i].values()[c.get_index()];
                        if let TableValue::Int(v) = value {
                            column.append_value(*v);
                        } else {
                            panic!("Unexpected value: {:?}", value);
                        }
                    }
                    column_values.push(Arc::new(column.finish()));
                }
                _ => unimplemented!(),
            }
        }

        let batch = RecordBatch::try_new(schema.clone(), column_values)?;

        Ok(try_make_memory_data_source(
            &vec![vec![batch]],
            schema,
            None,
        )?)
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ChunkData {
    data_frame: DataFrame,
    columns: Vec<Column>,
}

impl ChunkData {
    pub fn new(data_frame: DataFrame, columns: Vec<Column>) -> ChunkData {
        ChunkData {
            data_frame,
            columns,
        }
    }

    pub fn len(&self) -> usize {
        self.data_frame.len()
    }
}

pub struct WALStore {
    meta_store: Arc<dyn MetaStore>,
    remote_fs: Arc<dyn RemoteFs>,
    wal_chunk_size: usize,
}

crate::di_service!(WALStore, [WALDataStore]);

pub struct ChunkStore {
    meta_store: Arc<dyn MetaStore>,
    remote_fs: Arc<dyn RemoteFs>,
    cluster: Arc<dyn Cluster>,
    config: Arc<dyn ConfigObj>,
    metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory>,
    memory_chunks: RwLock<HashMap<String, RecordBatch>>,
    chunk_size: usize,
}

crate::di_service!(ChunkStore, [ChunkDataStore]);

fn save<T: Serialize>(path: String, data: T) -> Result<(), CubeError> {
    let file = File::create(path)?;
    let mut f = BufWriter::new(file);
    serialize_into(&mut f, &data).unwrap();
    f.flush()?;
    Ok(())
}

fn load<T: de::DeserializeOwned>(path: String) -> Result<T, CubeError> {
    let f = File::open(path)?;
    let f = BufReader::new(f);
    let res: T = deserialize_from(f).unwrap();

    Ok(res)
}

#[async_trait]
pub trait WALDataStore: DIService + Send + Sync {
    async fn add_wal(&self, table: IdRow<Table>, data: DataFrame) -> Result<IdRow<WAL>, CubeError>;
    async fn get_wal(&self, wal_id: u64) -> Result<DataFrame, CubeError>;
    fn get_wal_chunk_size(&self) -> usize;
}

#[automock]
#[async_trait]
pub trait ChunkDataStore: DIService + Send + Sync {
    async fn partition(&self, wal_id: u64) -> Result<(), CubeError>;
    /// Returns ids of uploaded chunks. Uploaded chunks are **not** activated.
    async fn partition_data(
        &self,
        table_id: u64,
        rows: Vec<ArrayRef>,
        columns: &[Column],
        in_memory: bool,
    ) -> Result<Vec<ChunkUploadJob>, CubeError>;
    async fn repartition(&self, partition_id: u64) -> Result<(), CubeError>;
    async fn repartition_chunk(
        &self,
        chunk_id: u64,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError>;
    async fn repartition_partition_chunks(
        &self,
        partition_id: u64,
        anchor_chunk_id: u64,
        time_budget: std::time::Duration,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError>;
    async fn repartition_chunk_range(
        &self,
        start_chunk_id: u64,
        end_chunk_id: u64,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError>;
    async fn get_chunk_columns(&self, chunk: IdRow<Chunk>) -> Result<Vec<RecordBatch>, CubeError>;
    async fn has_in_memory_chunk(
        &self,
        chunk: IdRow<Chunk>,
        partition: IdRow<Partition>,
    ) -> Result<bool, CubeError>;
    async fn get_chunk_columns_with_preloaded_meta(
        &self,
        chunk: IdRow<Chunk>,
        partition: IdRow<Partition>,
        index: IdRow<Index>,
    ) -> Result<Vec<RecordBatch>, CubeError>;
    /// Reads each chunk as a separate already-sorted run of record batches and returns them
    /// together with the ids of the non-empty chunks. Empty chunks are deactivated.
    async fn load_sorted_chunks_data(
        &self,
        chunks: &[IdRow<Chunk>],
        partition: IdRow<Partition>,
        index: IdRow<Index>,
    ) -> Result<(Vec<Vec<RecordBatch>>, Vec<u64>), CubeError>;
    /// Returns a single-partition ExecutionPlan over one chunk's data, sorted by the index sort
    /// key. Persisted chunks are scanned from parquet (streamed); in-memory chunks are served
    /// from memory.
    async fn chunk_exec(
        &self,
        chunk: IdRow<Chunk>,
        partition: IdRow<Partition>,
        index: IdRow<Index>,
    ) -> Result<Arc<dyn ExecutionPlan>, CubeError>;
    async fn add_memory_chunk(
        &self,
        chunk_name: String,
        batch: RecordBatch,
    ) -> Result<(), CubeError>;
    async fn free_memory_chunk(&self, chunk_name: String) -> Result<(), CubeError>;
    async fn free_deleted_memory_chunks(&self, chunk_names: Vec<String>) -> Result<(), CubeError>;
    async fn add_persistent_chunk(
        &self,
        index: IdRow<Index>,
        partition: IdRow<Partition>,
        batch: RecordBatch,
    ) -> Result<(IdRow<Chunk>, Option<u64>), CubeError>;
}

crate::di_service!(MockChunkDataStore, [ChunkDataStore]);

impl WALStore {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        remote_fs: Arc<dyn RemoteFs>,
        wal_chunk_size: usize,
    ) -> Arc<WALStore> {
        let store = WALStore {
            meta_store,
            remote_fs,
            wal_chunk_size,
        };

        Arc::new(store)
    }

    pub fn wal_remote_path(wal_id: u64) -> String {
        format!("{}.wal", wal_id)
    }
}

// TODO remove as it isn't used anymore
#[async_trait]
impl WALDataStore for WALStore {
    async fn add_wal(&self, table: IdRow<Table>, data: DataFrame) -> Result<IdRow<WAL>, CubeError> {
        let wal = self
            .meta_store
            .create_wal(table.get_id(), data.len())
            .await?;
        let remote_path = WALStore::wal_remote_path(wal.get_id()).clone();
        let local_file = self.remote_fs.local_file(remote_path).await?;
        cube_ext::spawn_blocking(move || -> Result<(), CubeError> {
            save(local_file, data)?;
            Ok(())
        })
        .await??;
        // TODO do not upload WAL to speed up S3 uploads
        // self.remote_fs
        //     .upload_file(&WALStore::wal_remote_path(wal.get_id()))
        //     .await?;
        self.meta_store.wal_uploaded(wal.get_id()).await?;
        Ok(wal)
    }

    async fn get_wal(&self, wal_id: u64) -> Result<DataFrame, CubeError> {
        let wal = self.meta_store.get_wal(wal_id).await?;
        if !wal.get_row().uploaded() {
            return Err(CubeError::internal(format!(
                "Trying to get not uploaded WAL: {:?}",
                wal
            )));
        }
        let remote_path = WALStore::wal_remote_path(wal_id);
        self.remote_fs
            .download_file(remote_path.clone(), None)
            .await?;
        let local_file = self.remote_fs.local_file(remote_path.clone()).await?;
        Ok(
            cube_ext::spawn_blocking(move || -> Result<DataFrame, CubeError> {
                Ok(load::<DataFrame>(local_file)?)
            })
            .await??,
        )
    }

    fn get_wal_chunk_size(&self) -> usize {
        self.wal_chunk_size
    }
}

impl ChunkStore {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        remote_fs: Arc<dyn RemoteFs>,
        cluster: Arc<dyn Cluster>,
        config: Arc<dyn ConfigObj>,
        metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory>,
        chunk_size: usize,
    ) -> Arc<ChunkStore> {
        let store = ChunkStore {
            meta_store,
            remote_fs,
            cluster,
            config,
            metadata_cache_factory,
            memory_chunks: RwLock::new(HashMap::new()),
            chunk_size,
        };

        Arc::new(store)
    }

    pub fn get_chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn chunk_file_name(chunk: IdRow<Chunk>) -> String {
        Self::chunk_remote_path(chunk.get_id(), chunk.get_row().suffix())
    }

    pub fn chunk_remote_path(chunk_id: u64, suffix: &Option<String>) -> String {
        chunk_file_name(chunk_id, suffix)
    }
}

#[async_trait]
impl ChunkDataStore for ChunkStore {
    async fn partition_data(
        &self,
        table_id: u64,
        rows: Vec<ArrayRef>,
        columns: &[Column],
        in_memory: bool,
    ) -> Result<Vec<ChunkUploadJob>, CubeError> {
        let indexes = self
            .meta_store
            .get_table_indexes_out_of_queue(table_id)
            .await?;
        self.build_index_chunks(table_id, &indexes, rows.into(), columns, in_memory)
            .await
    }

    async fn partition(&self, _wal_id: u64) -> Result<(), CubeError> {
        panic!("not used");
    }

    async fn repartition(&self, partition_id: u64) -> Result<(), CubeError> {
        let (partition, index, table, _) = self
            .meta_store
            .get_partition_for_compaction(partition_id)
            .await?;
        if partition.get_row().is_active() {
            return Err(CubeError::internal(format!(
                "Tried to repartition active partition: {:?}",
                partition
            )));
        }

        let chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?
            .into_iter()
            .filter(|c| c.get_row().in_memory() && c.get_row().active())
            .collect::<Vec<_>>();
        if chunks.is_empty() {
            return Ok(());
        }

        //Merge all partition in memory chunk into one
        let key_size = index.get_row().sort_key_size() as usize;
        let schema = Arc::new(arrow_schema(index.get_row()));
        let main_table: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let aggregate_columns = match index.get_row().get_type() {
            IndexType::Regular => None,
            IndexType::Aggregate => Some(table.get_row().aggregate_columns()),
        };

        let unique_key = table.get_row().unique_key_columns();
        let (chunk_runs, old_chunk_ids) = self
            .load_sorted_chunks_data(&chunks[..], partition.clone(), index.clone())
            .await?;

        if old_chunk_ids.is_empty() {
            return Ok(());
        }
        let task_context = QueryPlannerImpl::make_execution_context(
            self.metadata_cache_factory
                .cache_factory()
                .make_session_config(),
        )
        .task_ctx();

        let chunk_inputs = chunk_runs
            .into_iter()
            .map(|run| try_make_memory_data_source(&[run], schema.clone(), None))
            .collect::<Result<Vec<_>, _>>()?;

        let batches_stream = merge_chunks(
            key_size,
            main_table.clone(),
            chunk_inputs,
            unique_key.clone(),
            aggregate_columns.clone(),
            task_context,
        )
        .await?;
        let batches = common_collect(batches_stream).await?;

        if batches.is_empty() {
            self.meta_store.deactivate_chunks(old_chunk_ids).await?;
            return Ok(());
        }

        let mut columns = Vec::new();
        for i in 0..batches[0].num_columns() {
            columns.push(datafusion::arrow::compute::concat(
                &batches.iter().map(|b| b.column(i).as_ref()).collect_vec(),
            )?)
        }
        let new_chunks = &mut self
            .partition_rows(partition.get_row().get_index_id(), columns, true)
            .await?;

        if new_chunks.len() == 0 {
            return Ok(());
        }

        let new_chunk_ids: Result<Vec<(u64, Option<u64>)>, CubeError> = join_all(new_chunks)
            .await
            .into_iter()
            .map(|c| {
                let (c, file_size) = c??;
                Ok((c.get_id(), file_size))
            })
            .collect();

        let replay_handle_id =
            merge_replay_handles(self.meta_store.clone(), &chunks, table.get_id()).await?;

        self.meta_store
            .swap_chunks_without_check(old_chunk_ids, new_chunk_ids?, replay_handle_id)
            .await?;

        Ok(())
    }

    async fn repartition_chunk(
        &self,
        chunk_id: u64,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError> {
        let chunk = self.meta_store.get_chunk(chunk_id).await?;
        if !chunk.get_row().active() {
            log::debug!("Skipping repartition of inactive chunk: {:?}", chunk);
            return Ok(());
        }
        let in_memory = chunk.get_row().in_memory();
        let partition = self
            .meta_store
            .get_partition(chunk.get_row().get_partition_id())
            .await?;
        if partition.get_row().is_active() {
            return Err(CubeError::internal(format!(
                "Tried to repartition active partition: {:?}",
                partition
            )));
        }
        let mut new_chunks = Vec::new();
        let mut old_chunks = Vec::new();
        let chunk_id = chunk.get_id();
        let oldest_insert_at = chunk.get_row().oldest_insert_at().clone();
        old_chunks.push(chunk_id);
        let replay_handle_id = chunk.get_row().replay_handle_id();
        let batches = self.get_chunk_columns(chunk.clone()).await?;
        if batches.is_empty() {
            self.meta_store.deactivate_chunk(chunk_id).await?;
            return Ok(());
        }
        let mut columns = Vec::new();
        for i in 0..batches[0].num_columns() {
            columns.push(datafusion::arrow::compute::concat(
                &batches.iter().map(|b| b.column(i).as_ref()).collect_vec(),
            )?)
        }

        data_loaded_size.add(columns_vec_buffer_size(&columns));

        //There is no data in the chunk, so we just deactivate it
        if columns.len() == 0 || columns[0].len() == 0 {
            self.meta_store.deactivate_chunk(chunk_id).await?;
            return Ok(());
        }

        new_chunks.append(
            &mut self
                .partition_rows(partition.get_row().get_index_id(), columns, in_memory)
                .await?,
        );

        let new_chunk_ids: Vec<(u64, Option<u64>)> = join_all(new_chunks)
            .await
            .into_iter()
            .map(|c| {
                let (c, file_size) = c??;
                Ok((c.get_id(), file_size))
            })
            .collect::<Result<Vec<_>, CubeError>>()?;

        self.meta_store
            .chunk_update_last_inserted(
                new_chunk_ids.iter().map(|c| c.0).collect(),
                oldest_insert_at,
            )
            .await?;

        self.meta_store
            .swap_chunks(old_chunks, new_chunk_ids, replay_handle_id.clone())
            .await?;

        Ok(())
    }

    // PerPartition repartition: this single job (keyed on the anchor chunk) k-way merges
    // the parent's persisted chunks in groups and splits them into the active children at
    // the wal-split limit. anchor_chunk_id is the dedup key (smallest persisted chunk);
    // chunks are processed anchor-last so it stays active until the run finishes/yields.
    async fn repartition_partition_chunks(
        &self,
        partition_id: u64,
        anchor_chunk_id: u64,
        time_budget: std::time::Duration,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError> {
        let start = std::time::Instant::now();
        self.repartition_partition_chunks_merged(
            partition_id,
            anchor_chunk_id,
            self.config.repartition_merge_max_input_files().max(1),
            start,
            time_budget,
            data_loaded_size,
        )
        .await
    }

    // Repartition the active persisted chunks of one inclusive [start, end] chunk-id
    // range into the parent's children, as a single RepartitionRange job. The range is
    // resolved through the start chunk's partition; the boundary chunks must share that
    // partition (Error::internal otherwise) and may be inactive (read for the partition
    // only). Only active persisted chunks of the range are merged, in one atomic swap.
    async fn repartition_chunk_range(
        &self,
        start_chunk_id: u64,
        end_chunk_id: u64,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError> {
        let start_chunk = self.meta_store.get_chunk(start_chunk_id).await?;
        let end_chunk = self.meta_store.get_chunk(end_chunk_id).await?;
        let partition_id = start_chunk.get_row().get_partition_id();
        if end_chunk.get_row().get_partition_id() != partition_id {
            return Err(CubeError::internal(format!(
                "RepartitionRange boundary chunks {} and {} belong to different partitions",
                start_chunk_id, end_chunk_id
            )));
        }
        let partition = self.meta_store.get_partition(partition_id).await?;
        if partition.get_row().is_active() {
            return Err(CubeError::internal(format!(
                "Tried to repartition active partition: {:?}",
                partition
            )));
        }
        let index = self
            .meta_store
            .get_index(partition.get_row().get_index_id())
            .await?;
        let table = self
            .meta_store
            .get_table_by_id(index.get_row().table_id())
            .await?;
        let (children, boundaries) = self
            .compute_repartition_children(&partition, &index)
            .await?;

        let group = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?
            .into_iter()
            .filter(|c| {
                !c.get_row().in_memory()
                    && c.get_id() >= start_chunk_id
                    && c.get_id() <= end_chunk_id
            })
            .collect::<Vec<_>>();
        if group.is_empty() {
            return Ok(());
        }
        self.merge_chunk_group_into_children(
            &partition,
            &index,
            &table,
            &children,
            &boundaries,
            &group,
            data_loaded_size,
        )
        .await
    }

    async fn get_chunk_columns(&self, chunk: IdRow<Chunk>) -> Result<Vec<RecordBatch>, CubeError> {
        let partition = self
            .meta_store
            .get_partition(chunk.get_row().get_partition_id())
            .await?;
        let index = self
            .meta_store
            .get_index(partition.get_row().get_index_id())
            .await?;
        self.get_chunk_columns_with_preloaded_meta(chunk, partition, index)
            .await
    }
    async fn get_chunk_columns_with_preloaded_meta(
        &self,
        chunk: IdRow<Chunk>,
        partition: IdRow<Partition>,
        index: IdRow<Index>,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        if chunk.get_row().in_memory() {
            let node_name = self.cluster.node_name_by_partition(&partition);
            let server_name = self.cluster.server_name();
            if node_name != server_name {
                return Err(CubeError::internal(format!("In memory chunk {:?} with owner node '{}' is trying to be repartitioned or compacted on non owner node '{}'", chunk, node_name, server_name)));
            }
            let memory_chunks = self.memory_chunks.read().await;
            let chunk_name = chunk_file_name(chunk.get_id(), chunk.get_row().suffix());
            Ok(vec![memory_chunks
                .get(&chunk_name)
                .map(|b| b.clone())
                .unwrap_or(RecordBatch::new_empty(Arc::new(
                    arrow_schema(&index.get_row()),
                )))])
        } else {
            let (local_file, index) = self.download_chunk(chunk, partition, index).await?;
            let metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory> =
                self.metadata_cache_factory.clone();
            Ok(cube_ext::spawn_blocking(move || -> Result<_, CubeError> {
                let parquet = ParquetTableStore::new(index, ROW_GROUP_SIZE, metadata_cache_factory);
                Ok(parquet.read_columns(&local_file)?)
            })
            .await??)
        }
    }

    async fn load_sorted_chunks_data(
        &self,
        chunks: &[IdRow<Chunk>],
        partition: IdRow<Partition>,
        index: IdRow<Index>,
    ) -> Result<(Vec<Vec<RecordBatch>>, Vec<u64>), CubeError> {
        let mut runs: Vec<Vec<RecordBatch>> = Vec::new();
        let mut empty_chunk_ids = Vec::new();
        let mut non_empty_chunk_ids = Vec::new();

        // Each chunk is written sorted by the index sort key, so its batches form a single
        // sorted run that can be merged downstream without re-sorting.
        for chunk in chunks.iter() {
            let run = self
                .get_chunk_columns_with_preloaded_meta(
                    chunk.clone(),
                    partition.clone(),
                    index.clone(),
                )
                .await?
                .into_iter()
                .filter(|b| b.num_rows() > 0)
                .collect::<Vec<_>>();
            if run.is_empty() {
                empty_chunk_ids.push(chunk.get_id());
            } else {
                non_empty_chunk_ids.push(chunk.get_id());
                runs.push(run);
            }
        }
        if !empty_chunk_ids.is_empty() {
            self.meta_store.deactivate_chunks(empty_chunk_ids).await?;
        }

        Ok((runs, non_empty_chunk_ids))
    }

    async fn chunk_exec(
        &self,
        chunk: IdRow<Chunk>,
        partition: IdRow<Partition>,
        index: IdRow<Index>,
    ) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        let schema = Arc::new(arrow_schema(index.get_row()));
        if chunk.get_row().in_memory() {
            let batches = self
                .get_chunk_columns_with_preloaded_meta(chunk, partition, index)
                .await?;
            Ok(try_make_memory_data_source(&[batches], schema, None)?)
        } else {
            let (local_file, _) = self.download_chunk(chunk, partition, index).await?;
            let session_config = self
                .metadata_cache_factory
                .cache_factory()
                .make_session_config();
            let parquet_source = ParquetSource::new(
                TableParquetOptions::default(),
                get_reader_options_customizer(&session_config),
            )
            .with_parquet_file_reader_factory(
                self.metadata_cache_factory
                    .cache_factory()
                    .make_noop_cache(),
            );
            let file_scan = FileScanConfig::new(
                ObjectStoreUrl::local_filesystem(),
                schema,
                Arc::new(parquet_source),
            )
            .with_file(PartitionedFile::from_path(local_file)?);
            Ok(Arc::new(DataSourceExec::new(Arc::new(file_scan))))
        }
    }

    async fn has_in_memory_chunk(
        &self,
        chunk: IdRow<Chunk>,
        partition: IdRow<Partition>,
    ) -> Result<bool, CubeError> {
        if chunk.get_row().in_memory() {
            let node_name = self.cluster.node_name_by_partition(&partition);
            let server_name = self.cluster.server_name();
            if node_name != server_name {
                return Err(CubeError::internal(format!("In memory chunk {:?} with owner node '{}' is trying to be repartitioned or compacted on non owner node '{}'", chunk, node_name, server_name)));
            }
            let chunk_name = chunk_file_name(chunk.get_id(), chunk.get_row().suffix());
            let memory_chunks = self.memory_chunks.read().await;
            Ok(memory_chunks.contains_key(&chunk_name))
        } else {
            return Err(CubeError::internal(format!(
                "Chunk {:?} is not in memory",
                chunk
            )));
        }
    }

    async fn add_memory_chunk(
        &self,
        chunk_name: String,
        batch: RecordBatch,
    ) -> Result<(), CubeError> {
        self.report_in_memory_metrics().await?;
        let mut memory_chunks = self.memory_chunks.write().await;
        memory_chunks.insert(chunk_name, batch);
        Ok(())
    }
    async fn add_persistent_chunk(
        &self,
        index: IdRow<Index>,
        partition: IdRow<Partition>,
        batch: RecordBatch,
    ) -> Result<(IdRow<Chunk>, Option<u64>), CubeError> {
        let table = self
            .meta_store
            .get_table_by_id(index.get_row().table_id())
            .await?;
        self.add_chunk_columns(
            index.clone(),
            &table,
            partition.clone(),
            batch.columns().to_vec(),
            false,
        )
        .await?
        .await?
    }

    async fn free_memory_chunk(&self, chunk_name: String) -> Result<(), CubeError> {
        self.report_in_memory_metrics().await?;
        let mut memory_chunks = self.memory_chunks.write().await;
        memory_chunks.remove(&chunk_name);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn free_deleted_memory_chunks(&self, chunk_names: Vec<String>) -> Result<(), CubeError> {
        let names_set = chunk_names.into_iter().collect::<HashSet<_>>();
        {
            let mut memory_chunks = self.memory_chunks.write().await;
            memory_chunks.retain(|name, _| !names_set.contains(name));
        }

        self.report_in_memory_metrics().await?;
        Ok(())
    }
}

impl ChunkStore {
    // Streaming merge variant of repartition. The parent's persisted chunks are
    // merged k-way (in groups of <= max_group input files) and the sorted stream is
    // split directly into the parent's already-active children at the wal-split
    // limit, producing full-size chunks in one streaming pass. Each group commits
    // with a single atomic swap_chunks; correctness against a concurrent job racing
    // for the same chunks rests on that swap rejecting an already-inactive source
    // (the loser's new chunks stay inactive and are GC'd). The anchor sits in the
    // last group so it stays active (holding the job dedup key) until the run
    // finishes or yields on the time budget.
    async fn repartition_partition_chunks_merged(
        &self,
        partition_id: u64,
        anchor_chunk_id: u64,
        max_group: usize,
        start: std::time::Instant,
        time_budget: std::time::Duration,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError> {
        let partition = self.meta_store.get_partition(partition_id).await?;
        if partition.get_row().is_active() {
            return Err(CubeError::internal(format!(
                "Tried to repartition active partition: {:?}",
                partition
            )));
        }
        let index = self
            .meta_store
            .get_index(partition.get_row().get_index_id())
            .await?;
        let table = self
            .meta_store
            .get_table_by_id(index.get_row().table_id())
            .await?;
        let (children, boundaries) = self
            .compute_repartition_children(&partition, &index)
            .await?;

        let mut chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?
            .into_iter()
            .filter(|c| !c.get_row().in_memory())
            .collect::<Vec<_>>();
        chunks.sort_by_key(|c| (c.get_id() == anchor_chunk_id, c.get_id()));

        // Group the chunks the same way the range strategy slices its jobs: cut a group
        // once it reaches max_rows rows or max_group chunks, whichever comes first. The
        // anchor is last, so it lands in the final group and stays active until the run
        // finishes or yields on the time budget.
        let max_rows = self.config.repartition_merge_max_rows().max(1);
        let mut i = 0;
        while i < chunks.len() {
            let mut group = Vec::new();
            let mut rows = 0u64;
            while i < chunks.len() {
                rows += chunks[i].get_row().get_row_count();
                group.push(chunks[i].clone());
                i += 1;
                if rows >= max_rows || group.len() >= max_group {
                    break;
                }
            }
            self.merge_chunk_group_into_children(
                &partition,
                &index,
                &table,
                &children,
                &boundaries,
                &group,
                data_loaded_size.clone(),
            )
            .await?;
            if start.elapsed() >= time_budget {
                break;
            }
        }
        Ok(())
    }

    // Resolve the parent's children for repartition: active partitions of the index
    // within the parent's range, ordered by min bound (the bound the writer routes on,
    // kept monotonic), plus the interior split boundaries. Children must be non-empty
    // and non-overlapping; the merge stream-splits across them with the first child as
    // the low catch-all and the last as the high catch-all, so no row is ever dropped.
    async fn compute_repartition_children(
        &self,
        partition: &IdRow<Partition>,
        index: &IdRow<Index>,
    ) -> Result<(Vec<IdRow<Partition>>, Vec<Row>), CubeError> {
        let key_size = index.get_row().sort_key_size() as usize;
        let mut children = self
            .meta_store
            .get_active_partitions_by_index_id(index.get_id())
            .await?
            .into_iter()
            .filter(|c| Self::partition_within(partition, c, key_size))
            .collect::<Vec<_>>();
        children.sort_by(|a, b| {
            cmp_min_rows(
                key_size,
                a.get_row().get_min_val().as_ref(),
                b.get_row().get_min_val().as_ref(),
            )
        });

        // No active children in the parent's range is treated as a transient topology
        // read (e.g. a concurrent split swapping children to grandchildren between our
        // separate metastore reads), NOT corruption: return an error so the scheduler
        // retries instead of deactivating a possibly-healthy table.
        if children.is_empty() {
            return Err(CubeError::internal(format!(
                "Repartition of {} found no active children in its range; will retry",
                partition.get_id()
            )));
        }

        // Optional metadata sanity check (off by default): the active children must not
        // overlap. Gaps between adjacent children are expected and benign — a partition's
        // min/max are its data extent, and a split sets a child's lower bound to the first
        // segment's data min rather than the parent's lower bound, so adjacent partitions
        // legitimately leave empty key ranges between them. Only an overlap (a child whose
        // range starts before the previous child's range ends) is genuine metadata
        // corruption — a row could then belong to two children. The streaming split never
        // drops rows regardless, so this is a defensive check, not a correctness
        // requirement, and the legacy per-chunk path performed no such check.
        if self.config.repartition_check_overlapping_children() {
            let overlapping = children.windows(2).find(|w| {
                match (
                    w[0].get_row().get_max_val().as_ref(),
                    w[1].get_row().get_min_val().as_ref(),
                ) {
                    // An interior child must be right-bounded and the next left-bounded;
                    // an open bound in the middle of the ordering is itself corruption.
                    (Some(prev_max), Some(next_min)) => {
                        cmp_min_rows(key_size, Some(prev_max), Some(next_min)) == Ordering::Greater
                    }
                    _ => true,
                }
            });
            if overlapping.is_some() {
                let message = format!(
                    "Repartition of {} found overlapping children: {:?}",
                    partition.get_id(),
                    children.iter().map(|c| c.get_id()).collect::<Vec<_>>()
                );
                deactivate_table_due_to_corrupt_data(
                    self.meta_store.clone(),
                    index.get_row().table_id(),
                    message.clone(),
                )
                .await?;
                return Err(CubeError::internal(message));
            }
        }

        let boundaries: Vec<Row> = children
            .iter()
            .skip(1)
            .map(|c| c.get_row().get_min_val().clone())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                CubeError::internal("Child partition without a min bound during repartition".into())
            })?;
        Ok((children, boundaries))
    }

    // Merge one group of (active) persisted chunks into the parent's children: k-way
    // merge the chunks, stream-split the sorted output across the children at the
    // wal-split limit, then commit a single atomic swap. An empty group (all sources
    // had no rows) deactivates the sources directly. Losing the swap to a concurrent
    // job that already repartitioned a source is tolerated (its new chunks are GC'd).
    async fn merge_chunk_group_into_children(
        &self,
        partition: &IdRow<Partition>,
        index: &IdRow<Index>,
        table: &IdRow<Table>,
        children: &[IdRow<Partition>],
        boundaries: &[Row],
        group: &[IdRow<Chunk>],
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError> {
        if group.is_empty() {
            return Ok(());
        }
        let key_size = index.get_row().sort_key_size() as usize;
        let wal_split = table
            .get_row()
            .partition_split_threshold_or_default(self.config.partition_split_threshold())
            as usize;
        let unique_key = table.get_row().unique_key_columns();
        let aggregate_columns = match index.get_row().get_type() {
            IndexType::Regular => None,
            IndexType::Aggregate => Some(table.get_row().aggregate_columns()),
        };
        // merge_chunks collapses rows only for aggregate indexes (group-by) and unique-key
        // tables (last-row dedup). For a plain regular index it is a pure sort-merge that
        // conserves the row count, so we keep the checked swap there as an integrity guard.
        let merge_collapses_rows = unique_key.is_some() || aggregate_columns.is_some();
        let schema = Arc::new(arrow_schema(index.get_row()));

        // Download the group's chunk parquets into the local cache concurrently before
        // building the merge inputs, so downloads overlap instead of running one-at-a-time
        // in chunk_exec below. The group is bounded by max_input_files and the download
        // pool by download_concurrency. download_file is idempotent so chunk_exec then hits
        // the cache.
        if self.config.repartition_concurrent_download() {
            let mut downloads = Vec::with_capacity(group.len());
            for chunk in group.iter() {
                let file_size = chunk.get_row().file_size();
                let remote_path = ChunkStore::chunk_file_name(chunk.clone());
                let remote_fs = self.remote_fs.clone();
                downloads.push(cube_ext::spawn(async move {
                    // Warm the cache; a genuine error surfaces later in chunk_exec.
                    let _ = remote_fs.download_file(remote_path, file_size).await;
                }));
            }
            for d in downloads {
                let _ = d.await;
            }
        }

        let mut chunk_inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(group.len());
        for chunk in group.iter() {
            let exec = self
                .chunk_exec(chunk.clone(), partition.clone(), index.clone())
                .await?;
            chunk_inputs.push(Arc::new(TraceDataLoadedExec::new(
                exec,
                data_loaded_size.clone(),
            )));
        }
        let task_context = QueryPlannerImpl::make_execution_context(
            self.metadata_cache_factory
                .cache_factory()
                .make_session_config(),
        )
        .task_ctx();
        // merge_chunks aggregates / last-row-dedups the group, the same way compaction
        // does. For unique-key tables the full sort key ends with the seq column, so the
        // surviving row is the one with the max seq (latest); group order only decides
        // ties on the full sort key, i.e. rows with identical (unique key, seq) that are
        // duplicates anyway. Dedup is group-local and re-applied at query/compaction time
        // over the child's chunks, so the final result matches the per-chunk path.
        let records = merge_chunks(
            key_size,
            Arc::new(EmptyExec::new(schema.clone())),
            chunk_inputs,
            unique_key,
            aggregate_columns,
            task_context,
        )
        .await?;

        for child in children.iter() {
            self.check_node_disk_space(child).await?;
        }

        let group_rows: u64 = group.iter().map(|c| c.get_row().get_row_count()).sum();
        let max_files = children.len() + (group_rows as usize).div_ceil(wal_split.max(1)) + 1;
        // A random salt keeps temp paths unique even if two repartition jobs ever run
        // against the same partition concurrently (e.g. across a channel switch).
        let salt = rand::random::<u64>();
        let mut files = Vec::with_capacity(max_files);
        for i in 0..max_files {
            files.push(
                self.remote_fs
                    .temp_upload_path(format!(
                        "repartition/{}-{:x}-{}.chunk.parquet",
                        partition.get_id(),
                        salt,
                        i
                    ))
                    .await?,
            );
        }
        let store = ParquetTableStore::new(
            index.get_row().clone(),
            ROW_GROUP_SIZE,
            self.metadata_cache_factory.clone(),
        );
        let written = write_chunks_split_into_children(
            records,
            store,
            table,
            files,
            boundaries.to_vec(),
            wal_split.max(1),
        )
        .await?;

        let mut new_chunk_ids: Vec<(u64, Option<u64>)> = Vec::new();
        if self.config.metastore_batch_rpc() {
            // Create all child chunks in one metastore write, then upload their files. The
            // chunks are inactive until the swap below, so creating them before the uploads
            // matches the per-item path's visibility.
            let mut specs = Vec::new();
            let mut spec_files = Vec::new();
            for w in written {
                if w.num_rows == 0 {
                    let _ = tokio::fs::remove_file(&w.file).await;
                    continue;
                }
                let child = &children[w.child_index];
                specs.push(Chunk::new(
                    child.get_id(),
                    w.num_rows,
                    Some(Row::new(w.min)),
                    Some(Row::new(w.max)),
                    false,
                ));
                spec_files.push(w.file);
            }
            if !specs.is_empty() {
                let chunks = self.meta_store.insert_chunks(specs).await?;
                for (file, chunk) in spec_files.into_iter().zip(chunks) {
                    let remote = ChunkStore::chunk_file_name(chunk.clone());
                    let file_size = self.remote_fs.upload_file(file, remote).await?;
                    new_chunk_ids.push((chunk.get_id(), Some(file_size)));
                }
            }
        } else {
            for w in written {
                if w.num_rows == 0 {
                    let _ = tokio::fs::remove_file(&w.file).await;
                    continue;
                }
                let child = &children[w.child_index];
                let chunk = self
                    .meta_store
                    .create_chunk(
                        child.get_id(),
                        w.num_rows,
                        Some(Row::new(w.min)),
                        Some(Row::new(w.max)),
                        false,
                    )
                    .await?;
                let remote = ChunkStore::chunk_file_name(chunk.clone());
                let file_size = self.remote_fs.upload_file(w.file, remote).await?;
                new_chunk_ids.push((chunk.get_id(), Some(file_size)));
            }
        }

        let group_ids: Vec<u64> = group.iter().map(|c| c.get_id()).collect();

        // A fully empty group produces no new chunks; swap_chunks rejects an empty
        // activation list, so deactivate the sources directly.
        if new_chunk_ids.is_empty() {
            self.meta_store.deactivate_chunks(group_ids).await?;
            return Ok(());
        }

        // Carry the oldest insert time across the merge (the min) so repartitioned
        // chunks keep their real age instead of looking freshly inserted.
        let oldest_insert_at = group
            .iter()
            .filter_map(|c| c.get_row().oldest_insert_at().clone())
            .min();
        self.meta_store
            .chunk_update_last_inserted(
                new_chunk_ids.iter().map(|c| c.0).collect(),
                oldest_insert_at,
            )
            .await?;

        let replay_handle_id =
            merge_replay_handles(self.meta_store.clone(), &group.to_vec(), table.get_id()).await?;
        // For aggregate indexes and unique-key tables merge_chunks aggregates / last-row-dedups
        // the group, so the activated row count is legitimately smaller than the deactivated one
        // and the checked swap would reject it as a mismatch (like compaction commits its own
        // dedup'd merges unchecked). A plain regular index keeps every row, so commit it through
        // the checked swap to preserve the row-count integrity guard.
        let swap_result = if merge_collapses_rows {
            self.meta_store
                .swap_chunks_without_check(group_ids.clone(), new_chunk_ids, replay_handle_id)
                .await
        } else {
            self.meta_store
                .swap_chunks(group_ids.clone(), new_chunk_ids, replay_handle_id)
                .await
        };
        if let Err(e) = swap_result {
            let mut all_active = true;
            for id in &group_ids {
                let active = self
                    .meta_store
                    .get_chunk(*id)
                    .await
                    .map_or(false, |c| c.get_row().active());
                if !active {
                    all_active = false;
                    break;
                }
            }
            if all_active {
                return Err(e);
            }
            log::debug!(
                "Skipping repartition group lost to a concurrent swap: {}",
                e
            );
        }
        Ok(())
    }

    // A child partition lies within the parent when its lower bound is >= the
    // parent's (None = -inf) and its upper bound is <= the parent's (None = +inf),
    // compared on the partition-key prefix.
    fn partition_within(
        parent: &IdRow<Partition>,
        child: &IdRow<Partition>,
        key_size: usize,
    ) -> bool {
        let lower_ok = cmp_min_rows(
            key_size,
            child.get_row().get_min_val().as_ref(),
            parent.get_row().get_min_val().as_ref(),
        ) != Ordering::Less;
        let upper_ok = match (
            child.get_row().get_max_val().as_ref(),
            parent.get_row().get_max_val().as_ref(),
        ) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(c), Some(p)) => cmp_min_rows(key_size, Some(c), Some(p)) != Ordering::Greater,
        };
        lower_ok && upper_ok
    }

    async fn download_chunk(
        &self,
        chunk: IdRow<Chunk>,
        partition: IdRow<Partition>,
        index: IdRow<Index>,
    ) -> Result<(String, Index), CubeError> {
        if !chunk.get_row().uploaded() {
            return Err(CubeError::internal(format!(
                "Trying to get not uploaded chunk: {:?}",
                chunk
            )));
        }
        let file_size = chunk.get_row().file_size();
        let chunk_id = chunk.get_id();
        let remote_path = ChunkStore::chunk_file_name(chunk);
        let result = self
            .remote_fs
            .download_file(remote_path.clone(), file_size)
            .await;

        deactivate_table_on_corrupt_data(
            self.meta_store.clone(),
            &result,
            &partition,
            Some(chunk_id),
        )
        .await;

        Ok((
            self.remote_fs.local_file(remote_path.clone()).await?,
            index.into_row(),
        ))
    }
    async fn report_in_memory_metrics(&self) -> Result<(), CubeError> {
        let memory_chunks = self.memory_chunks.read().await;
        let chunks_len = memory_chunks.len();
        let chunks_rows = memory_chunks
            .values()
            .map(|b| b.num_rows() as i64)
            .sum::<i64>();
        let chunks_memory = memory_chunks
            .values()
            .map(|b| {
                b.columns()
                    .iter()
                    .map(|c| c.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum::<usize>();
        app_metrics::IN_MEMORY_CHUNKS_COUNT.report(chunks_len as i64);
        app_metrics::IN_MEMORY_CHUNKS_ROWS.report(chunks_rows);
        app_metrics::IN_MEMORY_CHUNKS_MEMORY.report(chunks_memory as i64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_eq_columns;
    use crate::cluster::MockCluster;
    use crate::config::Config;
    use crate::metastore::{BaseRocksStoreFs, IndexDef, IndexType, RocksMetaStore};
    use crate::queryplanner::metadata_cache::BasicMetadataCacheFactory;
    use crate::remotefs::LocalDirRemoteFs;
    use crate::table::data::{concat_record_batches, rows_to_columns};
    use crate::table::parquet::CubestoreMetadataCacheFactoryImpl;
    use crate::{metastore::ColumnType, table::TableValue};
    use cuberockstore::rocksdb::{Options, DB};
    use datafusion::arrow::array::{Int64Array, StringArray};
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn dataframe_deep_size_of() {
        for (v, expected_size) in [(
            DataFrame::new(
                vec![Column::new("payload".to_string(), ColumnType::String, 0)],
                vec![Row::new(vec![TableValue::String("foo".to_string())])],
            ),
            162_usize,
        )] {
            assert_eq!(v.deep_size_of(), expected_size, "size for {:?}", v);
        }
    }

    #[tokio::test]
    async fn create_wal_test() {
        let config = Config::test("create_chunk_test");
        let path = "/tmp/test_create_wal";
        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());

        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(remote_store_path.clone())),
                PathBuf::from(store_path.clone()),
            );
            let store = WALStore::new(
                RocksMetaStore::new(
                    Path::new(path),
                    BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                    config.config_obj(),
                )
                .unwrap(),
                remote_fs.clone(),
                10,
            );

            let col = vec![
                Column::new("foo_int".to_string(), ColumnType::Int, 0),
                Column::new("foo".to_string(), ColumnType::String, 1),
                Column::new("boo".to_string(), ColumnType::String, 2),
            ];
            let first_rows = (0..35)
                .map(|i| {
                    Row::new(vec![
                        TableValue::Int(i),
                        TableValue::String(format!("Foo {}", i)),
                        TableValue::String(format!("Boo {}", i)),
                    ])
                })
                .collect::<Vec<_>>();

            let data_frame = DataFrame::new(col.clone(), first_rows);

            store
                .meta_store
                .create_schema("s".to_string(), false)
                .await
                .unwrap();
            let table = store
                .meta_store
                .create_table(
                    "s".to_string(),
                    "foo".to_string(),
                    col.clone(),
                    None,
                    None,
                    Vec::new(),
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
                    false,
                    None,
                )
                .await
                .unwrap();
            store.add_wal(table.clone(), data_frame).await.unwrap();
            let wal = IdRow::new(1, WAL::new(1, 10));
            let restored_wal: DataFrame = store.get_wal(wal.get_id()).await.unwrap();

            let first_rows = (0..35)
                .map(|i| {
                    Row::new(vec![
                        TableValue::Int(i),
                        TableValue::String(format!("Foo {}", i)),
                        TableValue::String(format!("Boo {}", i)),
                    ])
                })
                .collect::<Vec<_>>();
            let origin_data = DataFrame::new(col.clone(), first_rows);
            assert!(restored_wal == origin_data);
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn create_chunk_test() {
        let config = Config::test("create_chunk_test");
        let path = "/tmp/test_create_chunk";
        let wal_store_path = path.to_string() + &"_store_wal".to_string();
        let wal_remote_store_path = path.to_string() + &"_remote_store_wal".to_string();
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(wal_store_path.clone());
        let _ = fs::remove_dir_all(wal_remote_store_path.clone());
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(chunk_remote_store_path.clone())),
                PathBuf::from(chunk_store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                Path::new(path),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                10,
            );

            let col = vec![
                Column::new("foo_int".to_string(), ColumnType::Int, 0),
                Column::new("foo".to_string(), ColumnType::String, 1),
                Column::new("boo".to_string(), ColumnType::String, 2),
            ];
            let first_rows = (0..35)
                .map(|i| {
                    Row::new(vec![
                        TableValue::Int(34 - i),
                        TableValue::String(format!("Foo {}", 34 - i)),
                        TableValue::String(format!("Boo {}", 34 - i)),
                    ])
                })
                .collect::<Vec<_>>();

            let data_frame = DataFrame::new(col.clone(), first_rows);
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let table = meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    col.clone(),
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
                    false,
                    None,
                )
                .await
                .unwrap();

            let index = meta_store.get_default_index(table.get_id()).await.unwrap();
            let partitions = meta_store
                .get_active_partitions_by_index_id(index.get_id())
                .await
                .unwrap();
            let partition = partitions[0].clone();

            let data = rows_to_columns(&col, data_frame.get_rows().as_slice());
            let (chunk, file_size) = chunk_store
                .add_chunk_columns(index, &table, partition, data.clone(), false)
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            meta_store
                .swap_chunks(Vec::new(), vec![(chunk.get_id(), file_size)], None)
                .await
                .unwrap();
            let chunk = meta_store.get_chunk(1).await.unwrap();
            let restored_chunk =
                concat_record_batches(&chunk_store.get_chunk_columns(chunk).await.unwrap());
            assert_eq_columns!(restored_chunk.columns(), &data);
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(wal_store_path.clone());
        let _ = fs::remove_dir_all(wal_remote_store_path.clone());
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
    }

    #[tokio::test]
    async fn partition_data_with_batch_rpc() {
        // Exercises the CUBESTORE_METASTORE_BATCH_RPC path: build_index_chunks fetches active
        // partitions for all indexes in one get_active_partitions_for_indexes call and routes
        // them per index. The produced chunks must still cover every input row.
        let config = Config::test("partition_data_with_batch_rpc").update_config(|mut c| {
            c.metastore_batch_rpc = true;
            c
        });
        let path = "/tmp/test_partition_data_batch";
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(chunk_remote_store_path.clone())),
                PathBuf::from(chunk_store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                Path::new(path),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                10,
            );

            let col = vec![
                Column::new("foo_int".to_string(), ColumnType::Int, 0),
                Column::new("foo".to_string(), ColumnType::String, 1),
                Column::new("boo".to_string(), ColumnType::String, 2),
            ];
            let rows = (0..35)
                .map(|i| {
                    Row::new(vec![
                        TableValue::Int(34 - i),
                        TableValue::String(format!("Foo {}", 34 - i)),
                        TableValue::String(format!("Boo {}", 34 - i)),
                    ])
                })
                .collect::<Vec<_>>();
            let data_frame = DataFrame::new(col.clone(), rows);
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            // A secondary regular index so build_index_chunks loops over >1 index on the batch
            // path and must route each index its own partitions.
            let secondary = IndexDef {
                name: "by_foo".to_string(),
                columns: vec!["foo".to_string(), "foo_int".to_string()],
                multi_index: None,
                index_type: IndexType::Regular,
            };
            let table = meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    col.clone(),
                    None,
                    None,
                    vec![secondary],
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
                    false,
                    None,
                )
                .await
                .unwrap();
            let index_count = meta_store
                .get_table_indexes(table.get_id())
                .await
                .unwrap()
                .len();
            assert_eq!(index_count, 2, "default + secondary index expected");

            let data = rows_to_columns(&col, data_frame.get_rows().as_slice());
            let jobs = chunk_store
                .partition_data(table.get_id(), data, &col, false)
                .await
                .unwrap();
            assert!(!jobs.is_empty());
            let mut total_rows = 0u64;
            for job in jobs {
                let (chunk, _file_size) = job.await.unwrap().unwrap();
                total_rows += chunk.get_row().get_row_count();
            }
            // Each index receives all 35 rows, routed to its own partition.
            assert_eq!(total_rows, 35 * index_count as u64);
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
    }

    #[tokio::test]
    async fn create_aggr_chunk_test() {
        let config = Config::test("create_aggr_chunk_test");
        let path = "/tmp/test_create_aggr_chunk";
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(chunk_remote_store_path.clone())),
                PathBuf::from(chunk_store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                Path::new(path),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                10,
            );

            let col = vec![
                Column::new("foo".to_string(), ColumnType::String, 0),
                Column::new("boo".to_string(), ColumnType::Int, 1),
                Column::new("sum_int".to_string(), ColumnType::Int, 2),
            ];

            let foos = Arc::new(StringArray::from(vec![
                "a".to_string(),
                "b".to_string(),
                "a".to_string(),
                "b".to_string(),
                "a".to_string(),
            ]));
            let boos = Arc::new(Int64Array::from(vec![10, 20, 10, 20, 20]));

            let sums = Arc::new(Int64Array::from(vec![1, 2, 10, 20, 5]));

            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();

            let ind = IndexDef {
                name: "aggr".to_string(),
                columns: vec!["foo".to_string(), "boo".to_string()],
                multi_index: None,
                index_type: IndexType::Aggregate,
            };
            let table = meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    col.clone(),
                    None,
                    None,
                    vec![ind],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(vec![("sum".to_string(), "sum_int".to_string())]),
                    None,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap();

            let data: Vec<ArrayRef> = vec![foos, boos, sums];

            let indices = meta_store.get_table_indexes(table.get_id()).await.unwrap();

            let aggr_index = indices
                .iter()
                .find(|i| i.get_row().get_name() == "aggr")
                .unwrap();
            let chunk_feats = join_all(
                chunk_store
                    .partition_rows(aggr_index.get_id(), data, false)
                    .await
                    .unwrap(),
            )
            .await
            .into_iter()
            .map(|c| {
                let (c, _) = c.unwrap().unwrap();
                let cstore = chunk_store.clone();
                let mstore = meta_store.clone();
                async move {
                    let c = mstore.chunk_uploaded(c.get_id()).await.unwrap();
                    let batches = cstore.get_chunk_columns(c).await.unwrap();
                    concat_batches(&batches[0].schema(), &batches).unwrap()
                }
            })
            .collect::<Vec<_>>();

            let chunks = join_all(chunk_feats).await;

            let res = concat_batches(&chunks[0].schema(), &chunks).unwrap();

            let foos = Arc::new(StringArray::from(vec![
                "a".to_string(),
                "a".to_string(),
                "b".to_string(),
            ]));
            let boos = Arc::new(Int64Array::from(vec![10, 20, 20]));

            let sums = Arc::new(Int64Array::from(vec![11, 5, 22]));
            let expected: Vec<ArrayRef> = vec![foos, boos, sums];
            assert_eq!(res.columns(), &expected);
        }
    }

    #[tokio::test]
    async fn repartition_merge_drains_and_yields() {
        // Merge path with a group cap of 2: the parent's 3 persisted chunks are
        // merged in groups and split into two children. A zero budget yields after
        // the first group (anchor, processed last, stays active); a large budget
        // drains the rest. Row counts must be conserved across the children.
        let config = Config::test("repartition_merge_drains_and_yields").update_config(|mut c| {
            c.repartition_strategy = crate::config::RepartitionStrategy::PerPartition;
            c.repartition_merge_max_input_files = 2;
            c
        });
        let path = "/tmp/test_repartition_merge";
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(chunk_remote_store_path.clone())),
                PathBuf::from(chunk_store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                Path::new(path),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                10,
            );

            let col = vec![Column::new("n".to_string(), ColumnType::Int, 0)];
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let table = meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    col.clone(),
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
                    false,
                    None,
                )
                .await
                .unwrap();
            let index = meta_store.get_default_index(table.get_id()).await.unwrap();
            let partition = meta_store
                .get_active_partitions_by_index_id(index.get_id())
                .await
                .unwrap()[0]
                .clone();

            let mut chunk_ids = Vec::new();
            for range in [0..10i64, 10..20i64, 20..30i64] {
                let rows = range
                    .map(|i| Row::new(vec![TableValue::Int(i)]))
                    .collect::<Vec<_>>();
                let data = rows_to_columns(&col, &rows);
                let (chunk, file_size) = chunk_store
                    .add_chunk_columns(index.clone(), &table, partition.clone(), data, false)
                    .await
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
                meta_store
                    .swap_chunks(Vec::new(), vec![(chunk.get_id(), file_size)], None)
                    .await
                    .unwrap();
                chunk_ids.push(chunk.get_id());
            }

            let dest1 = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();
            let dest2 = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();
            let mid = Row::new(vec![TableValue::Int(15)]);
            meta_store
                .swap_active_partitions(
                    vec![(partition.clone(), vec![])],
                    vec![(dest1.clone(), 1), (dest2.clone(), 1)],
                    vec![
                        (0, (None, Some(mid.clone())), (None, Some(mid.clone()))),
                        (0, (Some(mid.clone()), None), (Some(mid.clone()), None)),
                    ],
                )
                .await
                .unwrap();

            let anchor = *chunk_ids.iter().min().unwrap();

            let child_rows = |dest_id: u64| {
                let meta_store = meta_store.clone();
                async move {
                    meta_store
                        .get_chunks_by_partition(dest_id, false)
                        .await
                        .unwrap()
                        .iter()
                        .filter(|c| c.get_row().active())
                        .map(|c| c.get_row().get_row_count())
                        .sum::<u64>()
                }
            };

            // Zero budget: only the first group (the two non-anchor chunks) is
            // processed; the anchor stays active on the parent.
            chunk_store
                .repartition_partition_chunks(
                    partition.get_id(),
                    anchor,
                    std::time::Duration::from_secs(0),
                    DataLoadedSize::new(),
                )
                .await
                .unwrap();
            let remaining = meta_store
                .get_chunks_by_partition(partition.get_id(), false)
                .await
                .unwrap()
                .into_iter()
                .filter(|c| c.get_row().active())
                .collect::<Vec<_>>();
            assert_eq!(
                remaining.len(),
                1,
                "zero-budget merge run must yield after the first group"
            );
            assert_eq!(
                remaining[0].get_id(),
                anchor,
                "anchor must be processed last and remain active after a yield"
            );

            // Large budget drains the rest; all 30 rows must land in the children.
            chunk_store
                .repartition_partition_chunks(
                    partition.get_id(),
                    anchor,
                    std::time::Duration::from_secs(600),
                    DataLoadedSize::new(),
                )
                .await
                .unwrap();
            let remaining = meta_store
                .get_chunks_by_partition(partition.get_id(), false)
                .await
                .unwrap()
                .into_iter()
                .filter(|c| c.get_row().active())
                .collect::<Vec<_>>();
            assert!(remaining.is_empty(), "parent chunks must drain under merge");

            let total = child_rows(dest1.get_id()).await + child_rows(dest2.get_id()).await;
            assert_eq!(total, 30, "all rows must be conserved across children");
            assert_eq!(
                child_rows(dest1.get_id()).await,
                15,
                "rows below the split go to the first child"
            );
            assert_eq!(
                child_rows(dest2.get_id()).await,
                15,
                "rows at/above the split go to the second child"
            );
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path);
        let _ = fs::remove_dir_all(chunk_remote_store_path);
    }

    #[tokio::test]
    async fn repartition_chunk_range_merges_only_range() {
        // repartition_chunk_range must merge only the active persisted chunks within
        // [start, end], leaving the rest active, and conserve rows into the children.
        // Run with batched metastore RPC on so the batched insert_chunks path in
        // merge_chunk_group_into_children is exercised (per-item path covered by the other
        // repartition tests, which default the flag off).
        let config =
            Config::test("repartition_chunk_range_merges_only_range").update_config(|mut c| {
                c.metastore_batch_rpc = true;
                c
            });
        let path = "/tmp/test_repartition_range";
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(chunk_remote_store_path.clone())),
                PathBuf::from(chunk_store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                Path::new(path),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                10,
            );

            let col = vec![Column::new("n".to_string(), ColumnType::Int, 0)];
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let table = meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    col.clone(),
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
                    false,
                    None,
                )
                .await
                .unwrap();
            let index = meta_store.get_default_index(table.get_id()).await.unwrap();
            let partition = meta_store
                .get_active_partitions_by_index_id(index.get_id())
                .await
                .unwrap()[0]
                .clone();

            let mut chunk_ids = Vec::new();
            for range in [0..10i64, 10..20i64, 20..30i64] {
                let rows = range
                    .map(|i| Row::new(vec![TableValue::Int(i)]))
                    .collect::<Vec<_>>();
                let data = rows_to_columns(&col, &rows);
                let (chunk, file_size) = chunk_store
                    .add_chunk_columns(index.clone(), &table, partition.clone(), data, false)
                    .await
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
                meta_store
                    .swap_chunks(Vec::new(), vec![(chunk.get_id(), file_size)], None)
                    .await
                    .unwrap();
                chunk_ids.push(chunk.get_id());
            }

            let dest1 = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();
            let dest2 = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();
            let mid = Row::new(vec![TableValue::Int(15)]);
            meta_store
                .swap_active_partitions(
                    vec![(partition.clone(), vec![])],
                    vec![(dest1.clone(), 1), (dest2.clone(), 1)],
                    vec![
                        (0, (None, Some(mid.clone())), (None, Some(mid.clone()))),
                        (0, (Some(mid.clone()), None), (Some(mid.clone()), None)),
                    ],
                )
                .await
                .unwrap();

            let active_count = |partition_id: u64| {
                let meta_store = meta_store.clone();
                async move {
                    meta_store
                        .get_chunks_by_partition(partition_id, false)
                        .await
                        .unwrap()
                        .into_iter()
                        .filter(|c| c.get_row().active())
                        .count()
                }
            };
            let child_rows = |dest_id: u64| {
                let meta_store = meta_store.clone();
                async move {
                    meta_store
                        .get_chunks_by_partition(dest_id, false)
                        .await
                        .unwrap()
                        .iter()
                        .filter(|c| c.get_row().active())
                        .map(|c| c.get_row().get_row_count())
                        .sum::<u64>()
                }
            };

            // Merge only [c1, c2]; c3 must stay active on the parent.
            chunk_store
                .repartition_chunk_range(chunk_ids[0], chunk_ids[1], DataLoadedSize::new())
                .await
                .unwrap();
            assert_eq!(
                active_count(partition.get_id()).await,
                1,
                "only the chunk outside the range stays active"
            );
            assert_eq!(
                child_rows(dest1.get_id()).await + child_rows(dest2.get_id()).await,
                20,
                "the range's rows land in the children"
            );

            // Now merge the remaining chunk; the parent drains.
            chunk_store
                .repartition_chunk_range(chunk_ids[2], chunk_ids[2], DataLoadedSize::new())
                .await
                .unwrap();
            assert_eq!(
                active_count(partition.get_id()).await,
                0,
                "parent drains after the last range"
            );
            assert_eq!(child_rows(dest1.get_id()).await, 15);
            assert_eq!(child_rows(dest2.get_id()).await, 15);
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path);
        let _ = fs::remove_dir_all(chunk_remote_store_path);
    }

    #[tokio::test]
    async fn repartition_merge_drains_empty_group() {
        // A group whose chunks are all empty produces no new chunks; the merge path
        // must deactivate the sources directly instead of failing on an empty swap.
        let config = Config::test("repartition_merge_drains_empty_group").update_config(|mut c| {
            c.repartition_strategy = crate::config::RepartitionStrategy::PerPartition;
            c.repartition_merge_max_input_files = 4;
            c
        });
        let path = "/tmp/test_repartition_merge_empty";
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(chunk_remote_store_path.clone())),
                PathBuf::from(chunk_store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                Path::new(path),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                10,
            );

            let col = vec![Column::new("n".to_string(), ColumnType::Int, 0)];
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let table = meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    col.clone(),
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
                    false,
                    None,
                )
                .await
                .unwrap();
            let index = meta_store.get_default_index(table.get_id()).await.unwrap();
            let partition = meta_store
                .get_active_partitions_by_index_id(index.get_id())
                .await
                .unwrap()[0]
                .clone();

            // Two empty persisted chunks on the parent.
            for _ in 0..2 {
                let data = rows_to_columns(&col, &[]);
                let (chunk, file_size) = chunk_store
                    .add_chunk_columns(index.clone(), &table, partition.clone(), data, false)
                    .await
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
                meta_store
                    .swap_chunks(Vec::new(), vec![(chunk.get_id(), file_size)], None)
                    .await
                    .unwrap();
            }

            let dest1 = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();
            let dest2 = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();
            let mid = Row::new(vec![TableValue::Int(15)]);
            meta_store
                .swap_active_partitions(
                    vec![(partition.clone(), vec![])],
                    vec![(dest1.clone(), 1), (dest2.clone(), 1)],
                    vec![
                        (0, (None, Some(mid.clone())), (None, Some(mid.clone()))),
                        (0, (Some(mid.clone()), None), (Some(mid.clone()), None)),
                    ],
                )
                .await
                .unwrap();

            let anchor = meta_store
                .get_chunks_by_partition(partition.get_id(), false)
                .await
                .unwrap()
                .iter()
                .map(|c| c.get_id())
                .min()
                .unwrap();

            chunk_store
                .repartition_partition_chunks(
                    partition.get_id(),
                    anchor,
                    std::time::Duration::from_secs(600),
                    DataLoadedSize::new(),
                )
                .await
                .unwrap();

            let remaining = meta_store
                .get_chunks_by_partition(partition.get_id(), false)
                .await
                .unwrap()
                .into_iter()
                .filter(|c| c.get_row().active())
                .count();
            assert_eq!(remaining, 0, "empty group must drain via deactivation");
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path);
        let _ = fs::remove_dir_all(chunk_remote_store_path);
    }

    #[tokio::test]
    async fn repartition_merge_aggregate_index_collapses_rows() {
        // Regression guard for the merge-based repartition path (per_partition / range
        // strategies). merge_chunk_group_into_children k-way merges a group of source
        // chunks through merge_chunks, which for an aggregate index groups rows by the
        // sort key and so emits FEWER rows than it consumed. The swap that commits the
        // new chunks must therefore not enforce activated_row_count == deactivated_row_count
        // (the same reason compaction commits with swap_chunks_without_check). Before the
        // fix this raised "Deactivated row count (20) doesn't match activated row count (10)
        // during swap" and the repartition job failed.
        let config = Config::test("repartition_merge_aggregate_index_collapses_rows");
        let path = "/tmp/test_repartition_merge_aggr";
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(chunk_remote_store_path.clone())),
                PathBuf::from(chunk_store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                Path::new(path),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                10,
            );

            // Aggregate index keyed on `g` with a sum over `m`. The index sort key is `g`,
            // so the merge groups by `g` and sums `m`.
            let col = vec![
                Column::new("g".to_string(), ColumnType::Int, 0),
                Column::new("m".to_string(), ColumnType::Int, 1),
            ];
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let ind = IndexDef {
                name: "agg".to_string(),
                columns: vec!["g".to_string()],
                multi_index: None,
                index_type: IndexType::Aggregate,
            };
            let table = meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    col.clone(),
                    None,
                    None,
                    vec![ind],
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(vec![("sum".to_string(), "m".to_string())]),
                    None,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap();
            let index = meta_store
                .get_table_indexes(table.get_id())
                .await
                .unwrap()
                .into_iter()
                .find(|i| i.get_row().get_name() == "agg")
                .unwrap();
            let partition = meta_store
                .get_active_partitions_by_index_id(index.get_id())
                .await
                .unwrap()[0]
                .clone();

            // Two persisted chunks that share every `g` value (0..10), so the aggregate
            // merge collapses 20 source rows into 10 grouped rows.
            let mut chunk_ids = Vec::new();
            for _ in 0..2 {
                let rows = (0..10i64)
                    .map(|g| Row::new(vec![TableValue::Int(g), TableValue::Int(1)]))
                    .collect::<Vec<_>>();
                let data = rows_to_columns(&col, &rows);
                let (chunk, file_size) = chunk_store
                    .add_chunk_columns(index.clone(), &table, partition.clone(), data, false)
                    .await
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
                meta_store
                    .swap_chunks(Vec::new(), vec![(chunk.get_id(), file_size)], None)
                    .await
                    .unwrap();
                chunk_ids.push(chunk.get_id());
            }

            let dest1 = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();
            let dest2 = meta_store
                .create_partition(Partition::new_child(&partition, None))
                .await
                .unwrap();
            let mid = Row::new(vec![TableValue::Int(5)]);
            meta_store
                .swap_active_partitions(
                    vec![(partition.clone(), vec![])],
                    vec![(dest1.clone(), 1), (dest2.clone(), 1)],
                    vec![
                        (0, (None, Some(mid.clone())), (None, Some(mid.clone()))),
                        (0, (Some(mid.clone()), None), (Some(mid.clone()), None)),
                    ],
                )
                .await
                .unwrap();

            let active_count = |partition_id: u64| {
                let meta_store = meta_store.clone();
                async move {
                    meta_store
                        .get_chunks_by_partition(partition_id, false)
                        .await
                        .unwrap()
                        .into_iter()
                        .filter(|c| c.get_row().active())
                        .count()
                }
            };
            let child_rows = |dest_id: u64| {
                let meta_store = meta_store.clone();
                async move {
                    meta_store
                        .get_chunks_by_partition(dest_id, false)
                        .await
                        .unwrap()
                        .iter()
                        .filter(|c| c.get_row().active())
                        .map(|c| c.get_row().get_row_count())
                        .sum::<u64>()
                }
            };

            // Merge the whole [first, last] range in one swap. The aggregate merge reduces
            // the 20 source rows to 10, so the swap activates fewer rows than it deactivates.
            let start = *chunk_ids.iter().min().unwrap();
            let end = *chunk_ids.iter().max().unwrap();
            chunk_store
                .repartition_chunk_range(start, end, DataLoadedSize::new())
                .await
                .unwrap();

            assert_eq!(
                active_count(partition.get_id()).await,
                0,
                "parent drains after the merge swap"
            );
            assert_eq!(
                child_rows(dest1.get_id()).await + child_rows(dest2.get_id()).await,
                10,
                "aggregate merge collapses the 20 source rows into 10 grouped rows"
            );
            assert_eq!(
                child_rows(dest1.get_id()).await,
                5,
                "groups below the split go to the first child"
            );
            assert_eq!(
                child_rows(dest2.get_id()).await,
                5,
                "groups at/above the split go to the second child"
            );
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path);
        let _ = fs::remove_dir_all(chunk_remote_store_path);
    }

    #[tokio::test]
    async fn partition_rows_deactivates_table_on_column_count_mismatch() {
        let config = Config::test("partition_rows_column_count_mismatch");
        let path = "/tmp/test_partition_rows_mismatch";
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(chunk_remote_store_path.clone())),
                PathBuf::from(chunk_store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                Path::new(path),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                10,
            );

            let col = vec![Column::new("n".to_string(), ColumnType::Int, 0)];
            meta_store
                .create_schema("foo".to_string(), false)
                .await
                .unwrap();
            let table = meta_store
                .create_table(
                    "foo".to_string(),
                    "bar".to_string(),
                    col.clone(),
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
                    false,
                    None,
                )
                .await
                .unwrap();
            let index = meta_store.get_default_index(table.get_id()).await.unwrap();

            // The index has 1 column; feed 2 columns to simulate a chunk written under a
            // different (wider) schema. The mismatch must be treated as corrupt data, not
            // retried forever via failing RepartitionChunk jobs.
            let mismatched: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 5, 6])),
            ];
            let err = chunk_store
                .partition_rows(index.get_id(), mismatched, false)
                .await
                .unwrap_err();
            assert!(
                err.message
                    .contains("expects 1 columns but incoming chunk data has 2 columns"),
                "unexpected error: {}",
                err.message
            );

            // The table is deactivated (marked not ready) instead of looping.
            let table = meta_store.get_table_by_id(table.get_id()).await.unwrap();
            assert!(!table.get_row().is_ready());
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path);
        let _ = fs::remove_dir_all(chunk_remote_store_path);
    }
}

pub type ChunkUploadJob = JoinHandle<Result<(IdRow<Chunk>, Option<u64>), CubeError>>;

impl ChunkStore {
    async fn partition_rows(
        &self,
        index_id: u64,
        columns: Vec<ArrayRef>,
        in_memory: bool,
    ) -> Result<Vec<JoinHandle<Result<(IdRow<Chunk>, Option<u64>), CubeError>>>, CubeError> {
        let index = self.meta_store.get_index(index_id).await?;
        let table = self
            .meta_store
            .get_table_by_id(index.get_row().table_id())
            .await?;
        self.partition_rows_for_index(&index, &table, None, columns, in_memory)
            .await
    }
    #[tracing::instrument(level = "trace", skip(self, columns))]
    async fn partition_rows_for_index(
        &self,
        index: &IdRow<Index>,
        table: &IdRow<Table>,
        preset_partitions: Option<Vec<IdRow<Partition>>>,
        mut columns: Vec<ArrayRef>,
        in_memory: bool,
    ) -> Result<Vec<JoinHandle<Result<(IdRow<Chunk>, Option<u64>), CubeError>>>, CubeError> {
        let index_id = index.get_id();
        let partitions = match preset_partitions {
            Some(partitions) => partitions,
            None => {
                self.meta_store
                    .get_active_partitions_by_index_id(index_id)
                    .await?
            }
        };
        let sort_key_size = index.get_row().sort_key_size() as usize;

        let expected_columns = index.get_row().get_columns().len();
        if columns.len() != expected_columns {
            // The chunk data column count doesn't match the index schema (schema/version
            // mismatch, e.g. an id collision after restoring a stale metastore copy). Treat as
            // corrupt data and deactivate the table rather than panicking on the
            // `columns[0..sort_key_size]` slice below or failing RecordBatch::try_new in
            // post_process_columns.
            let error_message = format!(
                "Index {:?} expects {} columns but incoming chunk data has {} columns",
                index,
                expected_columns,
                columns.len()
            );
            deactivate_table_due_to_corrupt_data(
                self.meta_store.clone(),
                index.get_row().table_id(),
                error_message.clone(),
            )
            .await?;
            return Err(CubeError::corrupt_data(error_message));
        }

        let mut remaining_rows: Vec<u64> = (0..columns[0].len() as u64).collect_vec();
        {
            let (columns_again, remaining_rows_again) =
                cube_ext::spawn_blocking(move || -> Result<_, ArrowError> {
                    let sort_key = &columns[0..sort_key_size];
                    let converter = RowConverter::new(
                        (0..sort_key_size)
                            .map(|i| SortField::new(columns[i].data_type().clone()))
                            .into_iter()
                            .collect(),
                    )?;
                    let rows = converter.convert_columns(sort_key)?;
                    remaining_rows
                        .sort_unstable_by(|a, b| rows.row(*a as usize).cmp(&rows.row(*b as usize)));
                    Ok((columns, remaining_rows))
                })
                .await??;

            columns = columns_again;
            remaining_rows = remaining_rows_again;
        }

        let mut futures = Vec::new();
        // The disk-space check resolves to a per-node total, so checking each distinct target
        // node once is enough; this avoids one metastore RPC per partition written.
        let mut checked_nodes: HashSet<String> = HashSet::new();
        for partition in partitions.into_iter() {
            let min = partition.get_row().get_min_val().as_ref();
            let max = partition.get_row().get_max_val().as_ref();
            let (to_write, next) = remaining_rows.into_iter().partition::<Vec<_>, _>(|&r| {
                let r = r as usize;
                (min.is_none()
                    || cmp_partition_key(
                        sort_key_size,
                        min.unwrap().values().as_slice(),
                        columns.as_slice(),
                        r,
                    ) <= Ordering::Equal)
                    && (max.is_none()
                        || cmp_partition_key(
                            sort_key_size,
                            max.unwrap().values().as_slice(),
                            columns.as_slice(),
                            r,
                        ) > Ordering::Equal)
            });
            if to_write.len() > 0 {
                if !in_memory
                    && checked_nodes
                        .insert(node_name_by_partition(self.config.as_ref(), &partition))
                {
                    self.check_node_disk_space(&partition).await?;
                }
                let to_write = UInt64Array::from(to_write);
                let columns = columns
                    .iter()
                    .map(|c| datafusion::arrow::compute::take(c.as_ref(), &to_write, None))
                    .collect::<Result<Vec<_>, _>>()?;
                let columns = self
                    .post_process_columns(index.clone(), table, columns)
                    .await?;

                futures.push(self.add_chunk_columns(
                    index.clone(),
                    table,
                    partition.clone(),
                    columns,
                    in_memory,
                ));
            }
            remaining_rows = next;
        }

        if !remaining_rows.is_empty() {
            let error_message = format!("Error while insert data into index {:?}. {} rows of incoming data can't be assigned to any partitions. Probably paritition metadata is lost", index, remaining_rows.len());
            deactivate_table_due_to_corrupt_data(
                self.meta_store.clone(),
                index.get_row().table_id(),
                error_message.clone(),
            )
            .await?;
            return Err(CubeError::internal(error_message));
        }

        let new_chunks = join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(new_chunks)
    }

    async fn check_node_disk_space(&self, partition: &IdRow<Partition>) -> Result<(), CubeError> {
        let max_disk_space = self.config.max_disk_space_per_worker();
        if max_disk_space == 0 {
            return Ok(());
        }

        let node_name = node_name_by_partition(self.config.as_ref(), partition);
        let used_space = self
            .meta_store
            .get_used_disk_space_out_of_queue(Some(node_name.clone()))
            .await?;

        if max_disk_space < used_space {
            return Err(CubeError::user(format!(
                "Exceeded available storage space on worker {}: {:.3} GB out of {} GB allowed. Please consider changing pre-aggregations build range, reducing index count or pre-aggregations granularity.",
                node_name,
                used_space as f64 / 1024. / 1024. / 1024.,
                max_disk_space as f64 / 1024. / 1024. / 1024.
            )));
        }
        Ok(())
    }

    ///Post-processing of index columns chunk data before saving to parqet files.
    ///Suitable for pre-aggregaions and similar things
    ///`data` must be sorted in order of index columns
    async fn post_process_columns(
        &self,
        index: IdRow<Index>,
        table: &IdRow<Table>,
        data: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>, CubeError> {
        match index.get_row().get_type() {
            IndexType::Regular => Ok(data),
            IndexType::Aggregate => {
                let schema = Arc::new(arrow_schema(&index.get_row()));

                let batch = RecordBatch::try_new(schema.clone(), data)?;

                let memory_source_config =
                    MemorySourceConfig::try_new(&[vec![batch]], schema.clone(), None)?;

                let key_size = index.get_row().sort_key_size() as usize;
                let mut groups = Vec::with_capacity(key_size);
                let mut lex_ordering = Vec::<PhysicalSortExpr>::with_capacity(key_size);
                for i in 0..key_size {
                    let f = schema.field(i);
                    let col: Arc<dyn PhysicalExpr> =
                        Arc::new(FusionColumn::new(f.name().as_str(), i));
                    groups.push((col.clone(), f.name().clone()));
                    lex_ordering.push(PhysicalSortExpr::new(col, SortOptions::default()));
                }

                let input = Arc::new(DataSourceExec::new(Arc::new(
                    memory_source_config
                        .try_with_sort_information(vec![LexOrdering::new(lex_ordering)])?,
                )));

                let aggregates = table
                    .get_row()
                    .aggregate_columns()
                    .iter()
                    .map(|aggr_col| aggr_col.aggregate_expr(&schema).map(Arc::new))
                    .collect::<Result<Vec<_>, _>>()?;

                let filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>> = vec![None; aggregates.len()];

                let aggregate = Arc::new(AggregateExec::try_new(
                    AggregateMode::Single,
                    PhysicalGroupBy::new_single(groups),
                    aggregates,
                    filter_expr,
                    input,
                    schema.clone(),
                )?);

                assert!(aggregate
                    .properties()
                    .output_ordering()
                    .is_some_and(|ordering| ordering.len() == key_size));

                let task_context = QueryPlannerImpl::make_execution_context(
                    self.metadata_cache_factory
                        .cache_factory()
                        .make_session_config(),
                )
                .task_ctx();

                let batches = collect(aggregate, task_context).await?;
                if batches.is_empty() {
                    Ok(vec![])
                } else if batches.len() == 1 {
                    Ok(batches[0].columns().to_vec())
                } else {
                    let res = concat_batches(&schema, &batches).unwrap();
                    Ok(res.columns().to_vec())
                }
            }
        }
    }

    /// Processes data intuet files in the current task and schedules an async file upload.
    /// Join the returned handle to wait for the upload to finish.
    async fn add_chunk_columns(
        &self,
        index: IdRow<Index>,
        table: &IdRow<Table>,
        partition: IdRow<Partition>,
        data: Vec<ArrayRef>,
        in_memory: bool,
    ) -> Result<ChunkUploadJob, CubeError> {
        let key_size = index.get_row().sort_key_size() as usize;
        let (min, max) = min_max_values_from_data(&data, key_size);
        let chunk = self
            .meta_store
            .create_chunk(partition.get_id(), data[0].len(), min, max, in_memory)
            .await?;
        if in_memory {
            trace!(
                "New in memory chunk allocated during partitioning: {:?}",
                chunk
            );
            let batch = RecordBatch::try_new(Arc::new(arrow_schema(&index.get_row())), data)?;
            let node_name = self.cluster.node_name_by_partition(&partition);
            let cluster = self.cluster.clone();

            let chunk_name = chunk_file_name(chunk.get_id(), chunk.get_row().suffix());
            Ok(cube_ext::spawn(async move {
                cluster
                    .add_memory_chunk(&node_name, chunk_name, batch)
                    .await?;

                Ok((chunk, None))
            }))
        } else {
            trace!("New chunk allocated during partitioning: {:?}", chunk);
            let remote_path = ChunkStore::chunk_file_name(chunk.clone()).clone();
            let local_file = self.remote_fs.temp_upload_path(remote_path.clone()).await?;
            let local_file = scopeguard::guard(local_file, ensure_temp_file_is_dropped);
            let local_file_copy = local_file.clone();
            let metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory> =
                self.metadata_cache_factory.clone();

            let parquet = ParquetTableStore::new(
                index.get_row().clone(),
                ROW_GROUP_SIZE,
                metadata_cache_factory,
            );

            let writer_props = parquet.writer_props(table).await?;
            cube_ext::spawn_blocking(move || -> Result<(), CubeError> {
                parquet.write_data_given_props(&local_file_copy, data, writer_props)
            })
            .await??;

            let fs = self.remote_fs.clone();
            Ok(cube_ext::spawn(async move {
                let file_size = fs
                    .upload_file(local_file.to_string(), remote_path.clone())
                    .await?;
                Ok((chunk, Some(file_size)))
            }))
        }
    }

    /// Returns a list of newly added chunks.
    async fn build_index_chunks(
        &self,
        table_id: u64,
        indexes: &[IdRow<Index>],
        rows: VecArrayRef,
        columns: &[Column],
        in_memory: bool,
    ) -> Result<Vec<ChunkUploadJob>, CubeError> {
        let mut rows = rows.0;
        // The table is the same for every index/chunk produced by this call, so load it once
        // here instead of re-fetching it per chunk over the metastore RPC downstream.
        let table = self.meta_store.get_table_by_id(table_id).await?;
        // When batching is enabled, fetch the active partitions of all indexes in one RPC
        // instead of one per index inside partition_rows_for_index.
        let mut partitions_by_index = if self.config.metastore_batch_rpc() {
            let index_ids = indexes.iter().map(|i| i.get_id()).collect::<Vec<_>>();
            Some(
                self.meta_store
                    .get_active_partitions_for_indexes(index_ids)
                    .await?,
            )
        } else {
            None
        };
        let mut futures = Vec::new();
        for index in indexes.iter() {
            let index_columns = index.get_row().columns();
            let index_columns_copy = index_columns.clone();
            let columns = columns.to_vec();
            let (rows_again, remapped) = cube_ext::spawn_blocking(move || {
                let remapped = remap_columns(table_id, &rows, &columns, &index_columns_copy);
                (rows, remapped)
            })
            .await?;
            let remapped = remapped?;
            rows = rows_again;
            // In batch mode the map always has an entry per requested index id (the RPC inserts
            // one for every id, empty vec included). A missing key means the requested ids and
            // the indexes iterated here diverged — an internal bug we surface rather than
            // silently passing an empty set, which would trip the corrupt-data path below.
            let preset_partitions = match partitions_by_index.as_mut() {
                Some(map) => Some(map.remove(&index.get_id()).ok_or_else(|| {
                    CubeError::internal(format!(
                        "Active partitions missing for index {} in batched fetch",
                        index.get_id()
                    ))
                })?),
                None => None,
            };
            futures.push(self.partition_rows_for_index(
                &index,
                &table,
                preset_partitions,
                remapped,
                in_memory,
            ));
        }

        let new_chunks = join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(new_chunks)
    }
}

fn min_max_values_from_data(data: &[ArrayRef], key_size: usize) -> (Option<Row>, Option<Row>) {
    if data.is_empty() || data[0].is_empty() || key_size == 0 {
        (None, None)
    } else {
        (
            Some(Row::new(TableValue::from_columns(&data[0..key_size], 0))),
            Some(Row::new(TableValue::from_columns(
                &data[0..key_size],
                data[0].len() - 1,
            ))),
        )
    }
}

fn remap_columns(
    table_id: u64,
    old: &[ArrayRef],
    old_columns: &[Column],
    new_columns: &[Column],
) -> Result<Vec<ArrayRef>, CubeError> {
    assert_eq!(old_columns.len(), old.len(), "table id: {}", table_id);
    let mut new = Vec::with_capacity(new_columns.len());
    for new_column in new_columns.iter() {
        let old_column = old_columns
            .iter()
            .find(|c| c.get_name() == new_column.get_name())
            .ok_or_else(|| {
                CubeError::internal(format!(
                    "Column '{}' not found in {:?}",
                    new_column.get_name(),
                    old_columns
                ))
            })?;
        new.push(old[old_column.get_index()].clone());
    }
    Ok(new)
}

/// A wrapper to workaround Rust compiler error when using Vec<ArrayRef> in function arguments.
/// ``error[E0700]: hidden type for `impl Trait` captures lifetime that does not appear in bounds``
pub struct VecArrayRef(Vec<ArrayRef>);
impl From<Vec<ArrayRef>> for VecArrayRef {
    fn from(v: Vec<ArrayRef>) -> Self {
        VecArrayRef(v)
    }
}

impl Into<Vec<ArrayRef>> for VecArrayRef {
    fn into(self) -> Vec<ArrayRef> {
        self.0
    }
}
