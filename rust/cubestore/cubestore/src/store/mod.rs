pub mod compaction;

use async_trait::async_trait;
use datafusion::arrow::compute::{concat_batches, lexsort_to_indices, SortColumn, SortOptions};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::common::collect as common_collect;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column as FusionColumn;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use serde::{de, Deserialize, Serialize};
extern crate bincode;

use bincode::{deserialize_from, serialize_into};

use crate::metastore::{
    deactivate_table_due_to_corrupt_data, deactivate_table_on_corrupt_data, table::Table, Chunk,
    Column, ColumnType, IdRow, Index, IndexType, MetaStore, Partition, WAL,
};
use crate::queryplanner::QueryPlannerImpl;
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
use crate::queryplanner::trace_data_loaded::DataLoadedSize;
use crate::table::data::cmp_partition_key;
use crate::table::parquet::{arrow_schema, CubestoreMetadataCacheFactory, ParquetTableStore};
use compaction::{merge_chunks, merge_replay_handles};
use datafusion::arrow::array::{Array, ArrayRef, Int64Builder, StringBuilder, UInt64Array};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::cube_ext;
use datafusion::execution::TaskContext;
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

        Ok(Arc::new(MemoryExec::try_new(
            &vec![vec![batch]],
            schema,
            None,
        )?))
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
    ///Return tuple with concated and sorted chunks data and vectore of non-empty chunks
    ///Deactiveat empty chunks
    async fn concat_and_sort_chunks_data(
        &self,
        chunks: &[IdRow<Chunk>],
        partition: IdRow<Partition>,
        index: IdRow<Index>,
        sort_key_size: usize,
    ) -> Result<(Vec<ArrayRef>, Vec<u64>), CubeError>;
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
        self.build_index_chunks(&indexes, rows.into(), columns, in_memory)
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
        let (in_memory_columns, old_chunk_ids) = self
            .concat_and_sort_chunks_data(&chunks[..], partition.clone(), index.clone(), key_size)
            .await?;

        if old_chunk_ids.is_empty() {
            return Ok(());
        }
        let task_context = QueryPlannerImpl::execution_context_helper(
            self.metadata_cache_factory
                .cache_factory()
                .make_session_config(),
        )
        .task_ctx();

        let batches_stream = merge_chunks(
            key_size,
            main_table.clone(),
            in_memory_columns,
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

    async fn concat_and_sort_chunks_data(
        &self,
        chunks: &[IdRow<Chunk>],
        partition: IdRow<Partition>,
        index: IdRow<Index>,
        sort_key_size: usize,
    ) -> Result<(Vec<ArrayRef>, Vec<u64>), CubeError> {
        let mut data: Vec<RecordBatch> = Vec::new();
        let mut empty_chunk_ids = Vec::new();
        let mut non_empty_chunk_ids = Vec::new();

        for chunk in chunks.iter() {
            for b in self
                .get_chunk_columns_with_preloaded_meta(
                    chunk.clone(),
                    partition.clone(),
                    index.clone(),
                )
                .await?
            {
                if b.num_rows() == 0 {
                    empty_chunk_ids.push(chunk.get_id());
                } else {
                    non_empty_chunk_ids.push(chunk.get_id());
                    data.push(b)
                }
            }
        }
        if !empty_chunk_ids.is_empty() {
            self.meta_store.deactivate_chunks(empty_chunk_ids).await?;
        }
        if data.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let new = cube_ext::spawn_blocking(move || -> Result<_, CubeError> {
            // Concat rows from all chunks.
            let num_columns = data[0].num_columns();
            let mut columns = Vec::with_capacity(num_columns);
            for i in 0..num_columns {
                let v = datafusion::arrow::compute::concat(
                    &data.iter().map(|a| a.column(i).as_ref()).collect_vec(),
                )?;
                columns.push(v);
            }
            // Sort rows from all chunks.
            let mut sort_key = Vec::with_capacity(sort_key_size);
            for i in 0..sort_key_size {
                sort_key.push(SortColumn {
                    values: columns[i].clone(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
                });
            }
            let indices = lexsort_to_indices(&sort_key, None)?;
            let mut new = Vec::with_capacity(num_columns);
            for c in columns {
                new.push(datafusion::arrow::compute::take(
                    c.as_ref(),
                    &indices,
                    None,
                )?)
            }
            Ok(new)
        })
        .await??;

        Ok((new, non_empty_chunk_ids))
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
        self.add_chunk_columns(
            index.clone(),
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
                .add_chunk_columns(index, partition, data.clone(), false)
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
        self.partition_rows_for_index(&index, columns, in_memory)
            .await
    }
    #[tracing::instrument(level = "trace", skip(self, columns))]
    async fn partition_rows_for_index(
        &self,
        index: &IdRow<Index>,
        mut columns: Vec<ArrayRef>,
        in_memory: bool,
    ) -> Result<Vec<JoinHandle<Result<(IdRow<Chunk>, Option<u64>), CubeError>>>, CubeError> {
        let index_id = index.get_id();
        let partitions = self
            .meta_store
            .get_active_partitions_by_index_id(index_id)
            .await?;
        let sort_key_size = index.get_row().sort_key_size() as usize;

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
                if !in_memory {
                    self.check_node_disk_space(&partition).await?;
                }
                let to_write = UInt64Array::from(to_write);
                let columns = columns
                    .iter()
                    .map(|c| datafusion::arrow::compute::take(c.as_ref(), &to_write, None))
                    .collect::<Result<Vec<_>, _>>()?;
                let columns = self.post_process_columns(index.clone(), columns).await?;

                futures.push(self.add_chunk_columns(
                    index.clone(),
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
        data: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>, CubeError> {
        match index.get_row().get_type() {
            IndexType::Regular => Ok(data),
            IndexType::Aggregate => {
                let table = self
                    .meta_store
                    .get_table_by_id(index.get_row().table_id())
                    .await?;
                let schema = Arc::new(arrow_schema(&index.get_row()));

                let batch = RecordBatch::try_new(schema.clone(), data)?;

                let memory_exec = MemoryExec::try_new(&[vec![batch]], schema.clone(), None)?;

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

                let input = Arc::new(memory_exec.with_sort_information(vec![lex_ordering]));

                let aggregates = table
                    .get_row()
                    .aggregate_columns()
                    .iter()
                    .map(|aggr_col| aggr_col.aggregate_expr(&schema))
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

                let task_context = QueryPlannerImpl::execution_context_helper(
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

            let table = self
                .meta_store
                .get_table_by_id(index.get_row().table_id())
                .await?;

            let parquet = ParquetTableStore::new(
                index.get_row().clone(),
                ROW_GROUP_SIZE,
                metadata_cache_factory,
            );

            let writer_props = parquet.writer_props(&table).await?;
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
        indexes: &[IdRow<Index>],
        rows: VecArrayRef,
        columns: &[Column],
        in_memory: bool,
    ) -> Result<Vec<ChunkUploadJob>, CubeError> {
        let mut rows = rows.0;
        let mut futures = Vec::new();
        for index in indexes.iter() {
            let index_columns = index.get_row().columns();
            let index_columns_copy = index_columns.clone();
            let columns = columns.to_vec();
            let (rows_again, remapped) = cube_ext::spawn_blocking(move || {
                let remapped = remap_columns(&rows, &columns, &index_columns_copy);
                (rows, remapped)
            })
            .await?;
            let remapped = remapped?;
            rows = rows_again;
            futures.push(self.partition_rows_for_index(&index, remapped, in_memory));
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
    old: &[ArrayRef],
    old_columns: &[Column],
    new_columns: &[Column],
) -> Result<Vec<ArrayRef>, CubeError> {
    assert_eq!(old_columns.len(), old.len());
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
