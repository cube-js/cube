pub mod compaction;

use async_trait::async_trait;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use serde::{de, Deserialize, Serialize};
extern crate bincode;

use bincode::{deserialize_from, serialize_into};

use crate::metastore::{
    table::Table, Chunk, Column, ColumnType, IdRow, Index, MetaStore, Partition, WAL,
};
use crate::remotefs::RemoteFs;
use crate::table::{Row, TableStore, TableValue};
use crate::CubeError;
use arrow::datatypes::Schema;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    sync::Arc,
};

use crate::config::injection::DIService;
use crate::table::data::{cmp_row_key, cmp_row_key_heap, MutRows, Rows};
use crate::table::parquet::ParquetTableStore;
use arrow::array::{Array, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;
use datafusion::cube_ext;
use futures::future::join_all;
use itertools::Itertools;
use log::trace;
use mockall::automock;
use std::cmp::Ordering;
use tokio::task::JoinHandle;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
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
    wal_store: Arc<dyn WALDataStore>,
    remote_fs: Arc<dyn RemoteFs>,
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
        rows: Rows,
        columns: &[Column],
    ) -> Result<Vec<ChunkUploadJob>, CubeError>;
    async fn repartition(&self, partition_id: u64) -> Result<(), CubeError>;
    async fn get_chunk(&self, chunk: IdRow<Chunk>) -> Result<Rows, CubeError>;
    async fn download_chunk(&self, chunk: IdRow<Chunk>) -> Result<String, CubeError>;
    async fn delete_remote_chunk(&self, chunk: IdRow<Chunk>) -> Result<(), CubeError>;
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
        self.remote_fs.download_file(&remote_path).await?;
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
        wal_store: Arc<dyn WALDataStore>,
        chunk_size: usize,
    ) -> Arc<ChunkStore> {
        let store = ChunkStore {
            meta_store,
            remote_fs,
            wal_store,
            chunk_size,
        };

        Arc::new(store)
    }

    pub fn get_chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn chunk_file_name(chunk: IdRow<Chunk>) -> String {
        Self::chunk_remote_path(chunk.get_id())
    }

    pub fn chunk_remote_path(chunk_id: u64) -> String {
        format!("{}.chunk.parquet", chunk_id)
    }
}

#[async_trait]
impl ChunkDataStore for ChunkStore {
    async fn partition_data(
        &self,
        table_id: u64,
        rows: Rows,
        columns: &[Column],
    ) -> Result<Vec<ChunkUploadJob>, CubeError> {
        let indexes = self.meta_store.get_table_indexes(table_id).await?;
        self.build_index_chunks(&indexes, rows, columns).await
    }

    async fn partition(&self, wal_id: u64) -> Result<(), CubeError> {
        let wal = self.meta_store.get_wal(wal_id).await?;
        let table_id = wal.get_row().table_id();
        let data = self.wal_store.get_wal(wal_id).await?;
        let indexes = self.meta_store.get_table_indexes(table_id).await?;
        let rows = MutRows::from_heap_allocated(data.columns.len(), data.get_rows()).freeze();
        let new_chunks: Result<Vec<u64>, CubeError> = join_all(
            self.build_index_chunks(&indexes, rows, &data.columns)
                .await?,
        )
        .await
        .into_iter()
        .map(|c| Ok(c??.get_id()))
        .collect();

        self.meta_store
            .activate_wal(wal_id, new_chunks?, indexes.len() as u64)
            .await?;

        Ok(())
    }

    async fn repartition(&self, partition_id: u64) -> Result<(), CubeError> {
        let partition = self.meta_store.get_partition(partition_id).await?;
        if partition.get_row().is_active() {
            return Err(CubeError::internal(format!(
                "Tried to repartition active partition: {:?}",
                partition
            )));
        }
        let chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?;
        let mut new_chunks = Vec::new();
        let mut old_chunks = Vec::new();
        for chunk in chunks.into_iter() {
            let chunk_id = chunk.get_id();
            old_chunks.push(chunk_id);
            let rows = self.get_chunk(chunk).await?;
            new_chunks.append(
                &mut self
                    .partition_data_frame(partition.get_row().get_index_id(), rows)
                    .await?,
            )
        }

        let new_chunk_ids: Result<Vec<u64>, CubeError> = join_all(new_chunks)
            .await
            .into_iter()
            .map(|c| Ok(c??.get_id()))
            .collect();

        self.meta_store
            .swap_chunks(old_chunks, new_chunk_ids?)
            .await?;

        Ok(())
    }

    async fn get_chunk(&self, chunk: IdRow<Chunk>) -> Result<Rows, CubeError> {
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
        let remote_path = ChunkStore::chunk_file_name(chunk);
        self.remote_fs.download_file(&remote_path).await?;
        let local_file = self.remote_fs.local_file(&remote_path).await?;
        Ok(
            cube_ext::spawn_blocking(move || -> Result<Rows, CubeError> {
                let parquet = ParquetTableStore::new(index.get_row().clone(), 16384); // TODO config
                Ok(parquet.read_rows(&local_file)?)
            })
            .await??,
        )
    }

    async fn download_chunk(&self, chunk: IdRow<Chunk>) -> Result<String, CubeError> {
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
        self.meta_store
            .get_index(partition.get_row().get_index_id())
            .await?;
        let remote_path = ChunkStore::chunk_file_name(chunk);
        self.remote_fs.download_file(&remote_path).await?;
        Ok(self.remote_fs.local_file(&remote_path).await?)
    }

    async fn delete_remote_chunk(&self, chunk: IdRow<Chunk>) -> Result<(), CubeError> {
        let remote_path = ChunkStore::chunk_file_name(chunk);
        self.remote_fs.delete_file(&remote_path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::metastore::RocksMetaStore;
    use crate::remotefs::LocalDirRemoteFs;
    use crate::table::data::MutRows;
    use crate::{metastore::ColumnType, table::TableValue};
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
            let wal_store = WALStore::new(meta_store.clone(), remote_fs.clone(), 10);
            let chunk_store =
                ChunkStore::new(meta_store.clone(), remote_fs.clone(), wal_store.clone(), 10);

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
                )
                .await
                .unwrap();

            let _ = wal_store.add_wal(table.clone(), data_frame).await;
            let wal = IdRow::new(1, WAL::new(1, 10));
            let mut restored_wal: DataFrame = wal_store.get_wal(wal.get_id()).await.unwrap();
            restored_wal
                .data
                .sort_by(|a, b| a.sort_key(a.len() as u64).cmp(&b.sort_key(b.len() as u64)));
            let restored_wal_not_sorted: DataFrame = wal_store.get_wal(wal.get_id()).await.unwrap();
            let mut restored_wal_sorted: DataFrame = wal_store.get_wal(wal.get_id()).await.unwrap();
            restored_wal_sorted
                .data
                .sort_by(|a, b| a.sort_key(a.len() as u64).cmp(&b.sort_key(b.len() as u64)));
            let index = meta_store.get_default_index(table.get_id()).await.unwrap();
            let partitions = meta_store
                .get_active_partitions_by_index_id(index.get_id())
                .await
                .unwrap();
            let partition = partitions[0].clone();

            let mut rows = MutRows::new(restored_wal.get_columns().len());
            for r in restored_wal.into_rows() {
                rows.add_row_heap_allocated(&r);
            }

            let chunk = chunk_store
                .add_chunk(index, partition, rows.freeze())
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            meta_store
                .swap_chunks(Vec::new(), vec![chunk.get_id()])
                .await
                .unwrap();
            let chunk = meta_store.get_chunk(1).await.unwrap();
            let restored_chunk = chunk_store.get_chunk(chunk).await.unwrap();

            let restored_data = restored_chunk.view().convert_to_heap_allocated();
            assert_eq!(restored_data, restored_wal_sorted.data);
            assert_ne!(restored_data, restored_wal_not_sorted.data);
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(wal_store_path.clone());
        let _ = fs::remove_dir_all(wal_remote_store_path.clone());
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
    }
}

pub type ChunkUploadJob = JoinHandle<Result<IdRow<Chunk>, CubeError>>;

impl ChunkStore {
    async fn partition_data_frame(
        &self,
        index_id: u64,
        mut rows: Rows,
    ) -> Result<Vec<JoinHandle<Result<IdRow<Chunk>, CubeError>>>, CubeError> {
        let index = self.meta_store.get_index(index_id).await?;
        let partitions = self
            .meta_store
            .get_active_partitions_by_index_id(index_id)
            .await?;
        let sort_key_size = index.get_row().sort_key_size() as usize;

        let mut remaining_rows: Vec<usize> = (0..rows.num_rows()).collect_vec();
        {
            let (rows_again, remaining_rows_again) = cube_ext::spawn_blocking(move || {
                remaining_rows.sort_unstable_by(|&a, &b| {
                    cmp_row_key(sort_key_size, &rows.view()[a], &rows.view()[b])
                });
                (rows, remaining_rows)
            })
            .await?;

            rows = rows_again;
            remaining_rows = remaining_rows_again;
        }

        let mut new_chunks = Vec::new();

        for partition in partitions.into_iter() {
            let (to_write, next) = remaining_rows.into_iter().partition::<Vec<_>, _>(|&r| {
                partition
                    .get_row()
                    .get_min_val()
                    .as_ref()
                    .map(|min| {
                        cmp_row_key_heap(sort_key_size, min.values(), &rows.view()[r])
                            <= Ordering::Equal
                    })
                    .unwrap_or(true)
                    && partition
                        .get_row()
                        .get_max_val()
                        .as_ref()
                        .map(|max| {
                            cmp_row_key_heap(sort_key_size, max.values(), &rows.view()[r])
                                > Ordering::Equal
                        })
                        .unwrap_or(true)
            });
            if to_write.len() > 0 {
                new_chunks.push(
                    self.add_chunk(index.clone(), partition, rows.copy_some_rows(&to_write))
                        .await?,
                );
            }
            remaining_rows = next;
        }

        assert_eq!(remaining_rows.len(), 0);

        Ok(new_chunks)
    }

    /// Processes data into parquet files in the current task and schedules an async file upload.
    /// Join the returned handle to wait for the upload to finish.
    async fn add_chunk(
        &'a self,
        index: IdRow<Index>,
        partition: IdRow<Partition>,
        data: Rows,
    ) -> Result<ChunkUploadJob, CubeError> {
        let chunk = self
            .meta_store
            .create_chunk(partition.get_id(), data.num_rows())
            .await?;
        trace!("New chunk allocated during partitioning: {:?}", chunk);
        let remote_path = ChunkStore::chunk_file_name(chunk.clone()).clone();
        let local_file = self.remote_fs.temp_upload_path(&remote_path).await?;
        let local_file_copy = local_file.clone();
        cube_ext::spawn_blocking(move || -> Result<(), CubeError> {
            let parquet = ParquetTableStore::new(index.get_row().clone(), 16384); // TODO config
            parquet.merge_rows(
                None,
                vec![local_file_copy],
                data.view(),
                index.get_row().sort_key_size() as usize,
            )?;
            Ok(())
        })
        .await??;

        let fs = self.remote_fs.clone();
        Ok(cube_ext::spawn(async move {
            fs.upload_file(&local_file, &remote_path).await?;
            Ok(chunk)
        }))
    }

    /// Returns a list of newly added chunks.
    async fn build_index_chunks(
        &self,
        indexes: &[IdRow<Index>],
        mut rows: Rows,
        columns: &[Column],
    ) -> Result<Vec<ChunkUploadJob>, CubeError> {
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
            new_chunks.append(&mut self.partition_data_frame(index.get_id(), remapped).await?);
        }

        Ok(new_chunks)
    }
}

fn remap_columns(
    old: &Rows,
    old_columns: &[Column],
    new_columns: &[Column],
) -> Result<Rows, CubeError> {
    assert_eq!(old_columns.len(), old.num_columns());
    let mut new_to_old = Vec::with_capacity(new_columns.len());
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
        new_to_old.push(old_column.get_index());
    }
    Ok(old.remap_columns(&new_to_old))
}
