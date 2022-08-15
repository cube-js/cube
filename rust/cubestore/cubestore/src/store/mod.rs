pub mod compaction;

use async_trait::async_trait;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::expressions::Column as FusionColumn;
use datafusion::physical_plan::hash_aggregate::{
    AggregateMode, AggregateStrategy, HashAggregateExec,
};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use serde::{de, Deserialize, Serialize};
extern crate bincode;

use bincode::{deserialize_from, serialize_into};

use crate::metastore::{
    deactivate_table_on_corrupt_data, table::Table, Chunk, Column, ColumnType, IdRow, Index,
    IndexType, MetaStore, Partition, WAL,
};
use crate::remotefs::{ensure_temp_file_is_dropped, RemoteFs};
use crate::table::{Row, TableValue};
use crate::CubeError;
use arrow::datatypes::{Schema, SchemaRef};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    sync::Arc,
};

use crate::cluster::Cluster;
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::chunks::chunk_file_name;
use crate::table::data::cmp_partition_key;
use crate::table::parquet::{arrow_schema, ParquetTableStore};
use arrow::array::{Array, ArrayRef, Int64Builder, StringBuilder, UInt64Array};
use arrow::record_batch::RecordBatch;
use datafusion::cube_ext;
use datafusion::cube_ext::util::lexcmp_array_rows;
use futures::future::join_all;
use itertools::Itertools;
use log::trace;
use mockall::automock;
use std::cmp::Ordering;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

pub const ROW_GROUP_SIZE: usize = 16384; // TODO config

#[derive(Serialize, Deserialize, Hash, Eq, PartialEq, Debug)]
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
                .collect::<Vec<_>>(),
        ))
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_rows(&self) -> &Vec<Row> {
        &self.data
    }

    pub fn mut_rows(&mut self) -> &mut Vec<Row> {
        &mut self.data
    }

    pub fn into_rows(self) -> Vec<Row> {
        self.data
    }

    pub fn to_execution_plan(
        &self,
        columns: &Vec<Column>,
    ) -> Result<Arc<dyn ExecutionPlan + Send + Sync>, CubeError> {
        let schema = Arc::new(Schema::new(
            columns.iter().map(|c| c.clone().into()).collect::<Vec<_>>(),
        ));

        let mut column_values: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

        for c in columns.iter() {
            match c.get_column_type() {
                ColumnType::String => {
                    let mut column = StringBuilder::new(self.data.len());
                    for i in 0..self.data.len() {
                        let value = &self.data[i].values()[c.get_index()];
                        if let TableValue::String(v) = value {
                            column.append_value(v.as_str())?;
                        } else {
                            panic!("Unexpected value: {:?}", value);
                        }
                    }
                    column_values.push(Arc::new(column.finish()));
                }
                ColumnType::Int => {
                    let mut column = Int64Builder::new(self.data.len());
                    for i in 0..self.data.len() {
                        let value = &self.data[i].values()[c.get_index()];
                        if let TableValue::Int(v) = value {
                            column.append_value(*v)?;
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

    pub fn mut_rows(&mut self) -> &mut Vec<Row> {
        &mut self.data_frame.data
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
    memory_chunks: RwLock<HashMap<u64, RecordBatch>>,
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
    async fn repartition_chunk(&self, chunk_id: u64) -> Result<(), CubeError>;
    async fn get_chunk_columns(&self, chunk: IdRow<Chunk>) -> Result<Vec<RecordBatch>, CubeError>;
    async fn add_memory_chunk(&self, chunk_id: u64, batch: RecordBatch) -> Result<(), CubeError>;
    async fn free_memory_chunk(&self, chunk_id: u64) -> Result<(), CubeError>;
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
        let local_file = self.remote_fs.local_file(&remote_path).await?;
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
        self.remote_fs.download_file(&remote_path, None).await?;
        let local_file = self.remote_fs.local_file(&remote_path).await?;
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
        chunk_size: usize,
    ) -> Arc<ChunkStore> {
        let store = ChunkStore {
            meta_store,
            remote_fs,
            cluster,
            config,
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
        let indexes = self.meta_store.get_table_indexes(table_id).await?;
        self.build_index_chunks(&indexes, rows.into(), columns, in_memory)
            .await
    }

    async fn partition(&self, _wal_id: u64) -> Result<(), CubeError> {
        panic!("not used");
    }

    // TODO shouldn't be used anymore. Deprecate and remove
    async fn repartition(&self, partition_id: u64) -> Result<(), CubeError> {
        let partition = self.meta_store.get_partition(partition_id).await?;
        if partition.get_row().is_active() {
            return Err(CubeError::internal(format!(
                "Tried to repartition active partition: {:?}",
                partition
            )));
        }
        let mut size = 0;
        let mut count = 0;
        let chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?
            .into_iter()
            .take_while(|c| {
                if size == 0 {
                    size += c.get_row().get_row_count();
                    count += 1;
                    true
                } else {
                    size += c.get_row().get_row_count();
                    count += 1;
                    // TODO config
                    size <= self.config.compaction_chunks_total_size_threshold() && count < 32
                }
            })
            .collect::<Vec<_>>();
        if chunks.is_empty() {
            return Ok(());
        }
        let mut new_chunks = Vec::new();
        let mut old_chunks = Vec::new();
        for chunk in chunks.into_iter() {
            let chunk_id = chunk.get_id();
            old_chunks.push(chunk_id);
            let batches = self.get_chunk_columns(chunk).await?;
            let mut columns = Vec::new();
            for i in 0..batches[0].num_columns() {
                columns.push(arrow::compute::concat(
                    &batches.iter().map(|b| b.column(i).as_ref()).collect_vec(),
                )?)
            }
            new_chunks.append(
                &mut self
                    .partition_rows(partition.get_row().get_index_id(), columns, false)
                    .await?,
            );
        }

        let new_chunk_ids: Result<Vec<(u64, Option<u64>)>, CubeError> = join_all(new_chunks)
            .await
            .into_iter()
            .map(|c| {
                let (c, file_size) = c??;
                Ok((c.get_id(), file_size))
            })
            .collect();

        self.meta_store
            .swap_chunks(old_chunks, new_chunk_ids?)
            .await?;

        Ok(())
    }

    async fn repartition_chunk(&self, chunk_id: u64) -> Result<(), CubeError> {
        let chunk = self.meta_store.get_chunk(chunk_id).await?;
        if !chunk.get_row().active() {
            log::debug!("Skipping repartition of inactive chunk: {:?}", chunk);
            return Ok(());
        }
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
        old_chunks.push(chunk_id);
        let batches = self.get_chunk_columns(chunk).await?;
        let mut columns = Vec::new();
        for i in 0..batches[0].num_columns() {
            columns.push(arrow::compute::concat(
                &batches.iter().map(|b| b.column(i).as_ref()).collect_vec(),
            )?)
        }
        new_chunks.append(
            &mut self
                .partition_rows(partition.get_row().get_index_id(), columns, false)
                .await?,
        );

        let new_chunk_ids: Result<Vec<(u64, Option<u64>)>, CubeError> = join_all(new_chunks)
            .await
            .into_iter()
            .map(|c| {
                let (c, file_size) = c??;
                Ok((c.get_id(), file_size))
            })
            .collect();

        self.meta_store
            .swap_chunks(old_chunks, new_chunk_ids?)
            .await?;

        Ok(())
    }

    async fn get_chunk_columns(&self, chunk: IdRow<Chunk>) -> Result<Vec<RecordBatch>, CubeError> {
        if chunk.get_row().in_memory() {
            let partition = self
                .meta_store
                .get_partition(chunk.get_row().get_partition_id())
                .await?;
            let node_name = self.cluster.node_name_by_partition(&partition);
            let server_name = self.cluster.server_name();
            if node_name != server_name {
                return Err(CubeError::internal(format!("In memory chunk {:?} with owner node '{}' is trying to be repartitioned or compacted on non owner node '{}'", chunk, node_name, server_name)));
            }
            let index = self
                .meta_store
                .get_index(partition.get_row().get_index_id())
                .await?;
            let memory_chunks = self.memory_chunks.read().await;
            Ok(vec![memory_chunks
                .get(&chunk.get_id())
                .map(|b| b.clone())
                .unwrap_or(RecordBatch::new_empty(Arc::new(
                    arrow_schema(&index.get_row()),
                )))])
        } else {
            let (local_file, index) = self.download_chunk(chunk).await?;
            Ok(cube_ext::spawn_blocking(move || -> Result<_, CubeError> {
                let parquet = ParquetTableStore::new(index, ROW_GROUP_SIZE);
                Ok(parquet.read_columns(&local_file)?)
            })
            .await??)
        }
    }

    async fn add_memory_chunk(&self, chunk_id: u64, batch: RecordBatch) -> Result<(), CubeError> {
        let mut memory_chunks = self.memory_chunks.write().await;
        memory_chunks.insert(chunk_id, batch);
        Ok(())
    }

    async fn free_memory_chunk(&self, chunk_id: u64) -> Result<(), CubeError> {
        let mut memory_chunks = self.memory_chunks.write().await;
        memory_chunks.remove(&chunk_id);
        Ok(())
    }
}

impl ChunkStore {
    async fn download_chunk(&self, chunk: IdRow<Chunk>) -> Result<(String, Index), CubeError> {
        if !chunk.get_row().uploaded() {
            return Err(CubeError::internal(format!(
                "Trying to get not uploaded chunk: {:?}",
                chunk
            )));
        }
        let partition = self
            .meta_store
            .get_partition(chunk.get_row().get_partition_id())
            .await?;
        let index = self
            .meta_store
            .get_index(partition.get_row().get_index_id())
            .await?;
        let file_size = chunk.get_row().file_size();
        let remote_path = ChunkStore::chunk_file_name(chunk);
        let result = self.remote_fs.download_file(&remote_path, file_size).await;

        deactivate_table_on_corrupt_data(self.meta_store.clone(), &result, &partition).await;

        Ok((
            self.remote_fs.local_file(&remote_path).await?,
            index.into_row(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_eq_columns;
    use crate::cluster::MockCluster;
    use crate::config::Config;
    use crate::metastore::{IndexDef, IndexType, RocksMetaStore};
    use crate::remotefs::LocalDirRemoteFs;
    use crate::table::data::{concat_record_batches, rows_to_columns};
    use crate::{metastore::ColumnType, table::TableValue};
    use arrow::array::{Int64Array, StringArray};
    use rocksdb::{Options, DB};
    use std::fs;
    use std::path::PathBuf;

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
                RocksMetaStore::new(path, remote_fs.clone(), config.config_obj()),
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
            let meta_store = RocksMetaStore::new(path, remote_fs.clone(), config.config_obj());
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
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
                .swap_chunks(Vec::new(), vec![(chunk.get_id(), file_size)])
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
            let meta_store = RocksMetaStore::new(path, remote_fs.clone(), config.config_obj());
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
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
                    Some(vec![("sum".to_string(), "sum_int".to_string())]),
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
                    RecordBatch::concat(&batches[0].schema(), &batches).unwrap()
                }
            })
            .collect::<Vec<_>>();

            let chunks = join_all(chunk_feats).await;

            let res = RecordBatch::concat(&chunks[0].schema(), &chunks).unwrap();

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
        mut columns: Vec<ArrayRef>,
        in_memory: bool,
    ) -> Result<Vec<JoinHandle<Result<(IdRow<Chunk>, Option<u64>), CubeError>>>, CubeError> {
        let index = self.meta_store.get_index(index_id).await?;
        let partitions = self
            .meta_store
            .get_active_partitions_by_index_id(index_id)
            .await?;
        let sort_key_size = index.get_row().sort_key_size() as usize;

        let mut remaining_rows: Vec<u64> = (0..columns[0].len() as u64).collect_vec();
        {
            let (columns_again, remaining_rows_again) = cube_ext::spawn_blocking(move || {
                let sort_key = &columns[0..sort_key_size];
                remaining_rows.sort_unstable_by(|&a, &b| {
                    lexcmp_array_rows(sort_key.iter(), a as usize, b as usize)
                });
                (columns, remaining_rows)
            })
            .await?;

            columns = columns_again;
            remaining_rows = remaining_rows_again;
        }

        let mut new_chunks = Vec::new();

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
                let to_write = UInt64Array::from(to_write);
                let columns = columns
                    .iter()
                    .map(|c| arrow::compute::take(c.as_ref(), &to_write, None))
                    .collect::<Result<Vec<_>, _>>()?;
                let columns = self.post_process_columns(index.clone(), columns).await?;
                new_chunks.push(
                    self.add_chunk_columns(index.clone(), partition, columns, in_memory)
                        .await?,
                );
            }
            remaining_rows = next;
        }

        assert_eq!(remaining_rows.len(), 0);

        Ok(new_chunks)
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

                let input = Arc::new(MemoryExec::try_new(&[vec![batch]], schema.clone(), None)?);

                let key_size = index.get_row().sort_key_size() as usize;
                let mut groups = Vec::with_capacity(key_size);
                for i in 0..key_size {
                    let f = schema.field(i);
                    let col: Arc<dyn PhysicalExpr> =
                        Arc::new(FusionColumn::new(f.name().as_str(), i));
                    groups.push((col, f.name().clone()));
                }

                let aggregates = table
                    .get_row()
                    .aggregate_columns()
                    .iter()
                    .map(|aggr_col| aggr_col.aggregate_expr(&schema))
                    .collect::<Result<Vec<_>, _>>()?;

                let output_sort_order = (0..index.get_row().sort_key_size())
                    .map(|x| x as usize)
                    .collect();

                let aggregate = Arc::new(HashAggregateExec::try_new(
                    AggregateStrategy::InplaceSorted,
                    Some(output_sort_order),
                    AggregateMode::Final,
                    groups,
                    aggregates,
                    input,
                    schema.clone(),
                )?);

                let batches = collect(aggregate).await?;
                if batches.is_empty() {
                    Ok(vec![])
                } else if batches.len() == 1 {
                    Ok(batches[0].columns().to_vec())
                } else {
                    let res = RecordBatch::concat(&schema, &batches).unwrap();
                    Ok(res.columns().to_vec())
                }
            }
        }
    }

    /// Processes data into parquet files in the current task and schedules an async file upload.
    /// Join the returned handle to wait for the upload to finish.
    async fn add_chunk_columns(
        &self,
        index: IdRow<Index>,
        partition: IdRow<Partition>,
        data: Vec<ArrayRef>,
        in_memory: bool,
    ) -> Result<ChunkUploadJob, CubeError> {
        let chunk = self
            .meta_store
            .create_chunk(partition.get_id(), data[0].len(), in_memory)
            .await?;
        if in_memory {
            trace!(
                "New in memory chunk allocated during partitioning: {:?}",
                chunk
            );
            let batch = RecordBatch::try_new(Arc::new(arrow_schema(&index.get_row())), data)?;
            let node_name = self.cluster.node_name_by_partition(&partition);
            let cluster = self.cluster.clone();

            Ok(cube_ext::spawn(async move {
                cluster
                    .add_memory_chunk(&node_name, chunk.get_id(), batch)
                    .await?;

                Ok((chunk, None))
            }))
        } else {
            trace!("New chunk allocated during partitioning: {:?}", chunk);
            let remote_path = ChunkStore::chunk_file_name(chunk.clone()).clone();
            let local_file = self.remote_fs.temp_upload_path(&remote_path).await?;
            let local_file = scopeguard::guard(local_file, ensure_temp_file_is_dropped);
            let local_file_copy = local_file.clone();
            cube_ext::spawn_blocking(move || -> Result<(), CubeError> {
                let parquet = ParquetTableStore::new(index.get_row().clone(), ROW_GROUP_SIZE);
                parquet.write_data(&local_file_copy, data)?;
                Ok(())
            })
            .await??;

            let fs = self.remote_fs.clone();
            Ok(cube_ext::spawn(async move {
                let file_size = fs.upload_file(&local_file, &remote_path).await?;
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
        let mut new_chunks = Vec::new();
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
            new_chunks.append(
                &mut self
                    .partition_rows(index.get_id(), remapped, in_memory)
                    .await?,
            );
        }

        Ok(new_chunks)
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
