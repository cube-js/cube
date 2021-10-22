use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::MetaStore;
use crate::remotefs::RemoteFs;
use crate::store::{ChunkDataStore, ROW_GROUP_SIZE};
use crate::table::data::cmp_partition_key;
use crate::table::parquet::{arrow_schema, ParquetTableStore};
use crate::table::redistribute::redistribute;
use crate::table::{Row, TableValue};
use crate::CubeError;
use arrow::array::ArrayRef;
use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::cube_ext;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::merge_sort::{LastRowByUniqueKeyExec, MergeSortExec};
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use itertools::{EitherOrBoth, Itertools};
use num::integer::div_ceil;
use num::Integer;
use parquet::arrow::ArrowWriter;
use std::cmp::Ordering;
use std::fs::File;
use std::mem::take;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

#[async_trait]
pub trait CompactionService: DIService + Send + Sync {
    async fn compact(&self, partition_id: u64) -> Result<(), CubeError>;
}

pub struct CompactionServiceImpl {
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    remote_fs: Arc<dyn RemoteFs>,
    config: Arc<dyn ConfigObj>,
}

crate::di_service!(CompactionServiceImpl, [CompactionService]);

impl CompactionServiceImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Arc<CompactionServiceImpl> {
        Arc::new(CompactionServiceImpl {
            meta_store,
            chunk_store,
            remote_fs,
            config,
        })
    }
}

#[async_trait]
impl CompactionService for CompactionServiceImpl {
    async fn compact(&self, partition_id: u64) -> Result<(), CubeError> {
        let mut chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?;
        chunks.sort_by_key(|c| c.get_row().get_row_count());
        let mut size = 0;
        let chunks = chunks
            .into_iter()
            .take_while(|c| {
                if size == 0 {
                    size += c.get_row().get_row_count();
                    true
                } else {
                    size += c.get_row().get_row_count();
                    size <= self.config.compaction_chunks_total_size_threshold()
                }
            })
            .collect::<Vec<_>>();
        let (partition, index) = self
            .meta_store
            .get_partition_for_compaction(partition_id)
            .await?;
        let partition_id = partition.get_id();
        let chunks_row_count = chunks
            .iter()
            .map(|c| c.get_row().get_row_count())
            .sum::<u64>();
        let total_rows = partition.get_row().main_table_row_count() + chunks_row_count;
        let new_partitions_count =
            div_ceil(total_rows, self.config.partition_split_threshold()) as usize;

        let mut new_partitions = Vec::new();
        for _ in 0..new_partitions_count {
            new_partitions.push(
                self.meta_store
                    .create_partition(partition.get_row().child(partition.get_id()))
                    .await?,
            );
        }

        let mut data = Vec::new();
        let num_columns = index.get_row().columns().len();
        for chunk in chunks.iter() {
            for b in self.chunk_store.get_chunk_columns(chunk.clone()).await? {
                assert_eq!(num_columns, b.num_columns());
                data.push(b)
            }
        }

        let store = ParquetTableStore::new(index.get_row().clone(), ROW_GROUP_SIZE);
        let old_partition_local =
            if let Some(f) = partition.get_row().get_full_name(partition.get_id()) {
                Some(self.remote_fs.download_file(&f).await?)
            } else {
                None
            };

        let mut new_partition_local_files = Vec::new();
        for p in new_partitions.iter() {
            let new_remote_path = p.get_row().get_full_name(p.get_id()).unwrap();
            new_partition_local_files.push(self.remote_fs.temp_upload_path(&new_remote_path).await?)
        }

        let new_partition_file_names = new_partition_local_files.clone();
        let key_size = index.get_row().sort_key_size() as usize;
        let (store, new) = cube_ext::spawn_blocking(move || -> Result<_, CubeError> {
            // Concat rows from all chunks.
            let mut columns = Vec::with_capacity(num_columns);
            for i in 0..num_columns {
                let v = arrow::compute::concat(
                    &data.iter().map(|a| a.column(i).as_ref()).collect_vec(),
                )?;
                columns.push(v);
            }
            // Sort rows from all chunks.
            let mut sort_key = Vec::with_capacity(key_size);
            for i in 0..key_size {
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
                new.push(arrow::compute::take(c.as_ref(), &indices, None)?)
            }
            Ok((store, new))
        })
        .await??;

        // Merge and write rows.
        let schema = Arc::new(arrow_schema(index.get_row()));
        let main_table: Arc<dyn ExecutionPlan> = match old_partition_local {
            Some(file) => Arc::new(ParquetExec::try_from_path(
                file.as_str(),
                None,
                None,
                ROW_GROUP_SIZE,
                1,
                None,
            )?),
            None => Arc::new(EmptyExec::new(false, schema.clone())),
        };

        let table = self
            .meta_store
            .get_table_by_id(index.get_row().table_id())
            .await?;

        let records = merge_chunks(
            key_size,
            main_table,
            new,
            table.get_row().unique_key_columns(),
        )
        .await?;
        let count_and_min = write_to_files(
            records,
            total_rows as usize,
            store,
            new_partition_file_names,
        )
        .await?;

        let mut filtered_partitions = Vec::new();

        for (i, p) in new_partitions
            .into_iter()
            .zip_longest(count_and_min.iter())
            .enumerate()
        {
            match p {
                EitherOrBoth::Both(p, _) => {
                    let new_remote_path = p.get_row().get_full_name(p.get_id()).unwrap();
                    self.remote_fs
                        .upload_file(&new_partition_local_files[i], new_remote_path.as_str())
                        .await?;
                    filtered_partitions.push(p);
                }
                EitherOrBoth::Left(p) => {
                    self.meta_store.delete_partition(p.get_id()).await?;
                    // TODO: ensure all files get removed on errors.
                    let _ = tokio::fs::remove_file(&new_partition_local_files[i]).await;
                }
                EitherOrBoth::Right(_) => {
                    return Err(CubeError::internal(format!(
                        "Unexpected state during partitioning: {:?}",
                        p
                    )))
                }
            }
        }

        self.meta_store
            .swap_active_partitions(
                vec![partition_id],
                filtered_partitions
                    .iter()
                    .map(|p| p.get_id())
                    .collect::<Vec<_>>(),
                chunks.iter().map(|c| c.get_id()).collect(),
                count_and_min
                    .iter()
                    .zip_longest(count_and_min.iter().skip(1 as usize))
                    .enumerate()
                    .map(|(i, item)| -> Result<_, CubeError> {
                        match item {
                            EitherOrBoth::Both((c, min), (_, next_min)) => {
                                if i == 0 && partition.get_row().get_min_val().is_none() {
                                    Ok((*c as u64, (None, Some(Row::new(next_min.clone())))))
                                } else if i < filtered_partitions.len() - 1 {
                                    Ok((
                                        *c as u64,
                                        (
                                            Some(Row::new(min.clone())),
                                            Some(Row::new(next_min.clone())),
                                        ),
                                    ))
                                } else {
                                    Err(CubeError::internal(format!(
                                        "Unexpected state for {} new partitions: {}, {:?}",
                                        filtered_partitions.len(),
                                        i,
                                        item
                                    )))
                                }
                            }
                            EitherOrBoth::Left((c, min)) => {
                                if i == 0 && filtered_partitions.len() == 1 {
                                    Ok((
                                        *c as u64,
                                        (
                                            partition.get_row().get_min_val().clone(),
                                            partition.get_row().get_max_val().clone(),
                                        ),
                                    ))
                                } else if i == filtered_partitions.len() - 1 {
                                    Ok((
                                        *c as u64,
                                        (
                                            Some(Row::new(min.clone())),
                                            partition.get_row().get_max_val().clone(),
                                        ),
                                    ))
                                } else {
                                    Err(CubeError::internal(format!(
                                        "Unexpected state for {} new partitions: {}, {:?}",
                                        filtered_partitions.len(),
                                        i,
                                        item
                                    )))
                                }
                            }
                            EitherOrBoth::Right(_) => Err(CubeError::internal(format!(
                                "Unexpected state for {} new partitions: {}, {:?}",
                                filtered_partitions.len(),
                                i,
                                item
                            ))),
                        }
                    })
                    .collect::<Result<Vec<_>, CubeError>>()?,
            )
            .await?;

        Ok(())
    }
}

/// Writes [records] into [files], trying to split into equally-sized rows, with an additional
/// restriction that files must have non-intersecting key ranges.
/// [records] must be sorted and have exactly [num_rows] rows.
pub(crate) async fn write_to_files(
    records: SendableRecordBatchStream,
    num_rows: usize,
    store: ParquetTableStore,
    files: Vec<String>,
) -> Result<Vec<(usize, Vec<TableValue>)>, CubeError> {
    let schema = Arc::new(store.arrow_schema());
    let key_size = store.key_size() as usize;
    let partition_split_key_size = store.partition_split_key_size() as usize;
    let rows_per_file = (num_rows as usize).div_ceil(&files.len());
    let mut writers = files.into_iter().map(move |f| -> Result<_, CubeError> {
        Ok(ArrowWriter::try_new(
            File::create(f)?,
            schema.clone(),
            Some(store.writer_props()),
        )?)
    });

    let (write_tx, mut write_rx) = tokio::sync::mpsc::channel(1);
    let io_job = cube_ext::spawn_blocking(move || -> Result<_, CubeError> {
        let mut writer = writers.next().transpose()?.unwrap();
        let mut current_writer_i = 0;
        while let Some((writer_i, batch)) = write_rx.blocking_recv() {
            debug_assert!(current_writer_i <= writer_i);
            if current_writer_i != writer_i {
                writer.close()?;

                writer = writers.next().transpose()?.unwrap();
                current_writer_i = writer_i;
            }

            writer.write(&batch)?;
        }

        writer.close()?;
        Ok(())
    });

    // Stats for the current writer.
    let mut writer_i = Ok(0); // Ok(n) marks active file, Err(n) marks finished file.
    let mut last_row = Vec::new();
    // (num_rows, first_row) for all processed writers.
    let stats = Arc::new(Mutex::new(vec![(0, Vec::new())]));
    let stats_ref = stats.clone();
    let mut process_row_group = move |b: RecordBatch| -> Result<_, CubeError> {
        let stats_ref = stats_ref.clone();
        let mut stats = stats_ref.lock().unwrap();
        if let Err(n) = writer_i {
            writer_i = Ok(n + 1);
            stats.push((0, Vec::new()));
            last_row.clear();
        }

        let (num_rows, first_row) = stats.last_mut().unwrap();
        let current_writer = writer_i.unwrap();

        if first_row.is_empty() {
            *first_row = TableValue::from_columns(&b.columns()[0..key_size], 0);
        }
        if *num_rows + b.num_rows() < rows_per_file {
            *num_rows += b.num_rows();
            return Ok(((current_writer, b), None));
        }
        let mut i;
        if last_row.is_empty() {
            i = rows_per_file - *num_rows - 1;
            last_row = TableValue::from_columns(&b.columns()[0..key_size], i);
            i += 1;
        } else {
            i = 0;
        }
        // Keep writing into the same file until we see a different key.
        while i < b.num_rows()
            && cmp_partition_key(partition_split_key_size, &last_row, b.columns(), i)
                == Ordering::Equal
        {
            i += 1;
        }
        if i == b.num_rows() {
            *num_rows += b.num_rows();
            return Ok(((current_writer, b), None));
        }

        *num_rows += i;
        writer_i = Err(current_writer); // Next iteration will write into a new file.
        return Ok((
            (current_writer, b.slice(0, i)),
            Some(b.slice(i, b.num_rows() - i)),
        ));
    };
    let err = redistribute(records, ROW_GROUP_SIZE, move |b| {
        let r = process_row_group(b);
        let write_tx = write_tx.clone();
        async move {
            let (to_write, to_return) = r?;
            write_tx.send(to_write).await?;
            return Ok(to_return);
        }
    })
    .await;

    // We want to report IO errors first, `err` will be unhelpful ("channel closed") when IO fails.
    io_job.await??;
    err?;

    let stats = take(stats.lock().unwrap().deref_mut());
    Ok(stats)
}

async fn merge_chunks(
    key_size: usize,
    l: Arc<dyn ExecutionPlan>,
    r: Vec<ArrayRef>,
    unique_key_columns: Option<Vec<&crate::metastore::Column>>,
) -> Result<SendableRecordBatchStream, CubeError> {
    let schema = l.schema();
    let r = RecordBatch::try_new(schema.clone(), r)?;

    let mut key = Vec::with_capacity(key_size);
    for i in 0..key_size {
        let f = schema.field(i);
        key.push(Column::new(f.name().as_str(), i));
    }

    let inputs = UnionExec::new(vec![
        l,
        Arc::new(MemoryExec::try_new(&[vec![r]], schema, None)?),
    ]);
    let mut res: Arc<dyn ExecutionPlan> = Arc::new(MergeSortExec::try_new(Arc::new(inputs), key)?);

    if let Some(key_columns) = unique_key_columns {
        res = Arc::new(LastRowByUniqueKeyExec::try_new(
            res.clone(),
            key_columns
                .iter()
                .map(|c| {
                    datafusion::physical_plan::expressions::Column::new_with_schema(
                        c.get_name().as_str(),
                        &res.schema(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        )?)
    }

    Ok(res.execute(0).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MockConfigObj;
    use crate::metastore::{Column, ColumnType, RocksMetaStore};
    use crate::store::MockChunkDataStore;
    use crate::table::{Row, TableValue};
    use arrow::array::StringArray;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn compaction() {
        let (remote_fs, metastore) = RocksMetaStore::prepare_test_metastore("compaction");
        let mut chunk_store = MockChunkDataStore::new();
        let mut config = MockConfigObj::new();
        metastore
            .create_schema("foo".to_string(), false)
            .await
            .unwrap();
        let cols = vec![Column::new("name".to_string(), ColumnType::String, 0)];
        metastore
            .create_table(
                "foo".to_string(),
                "bar".to_string(),
                cols.clone(),
                None,
                None,
                vec![],
                true,
                None,
            )
            .await
            .unwrap();
        metastore.get_default_index(1).await.unwrap();
        let partition = metastore.get_partition(1).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 10, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(1).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 16, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(2).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 20, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(3).await.unwrap();

        chunk_store.expect_get_chunk_columns().returning(move |i| {
            let limit = match i.get_id() {
                1 => 10,
                2 => 16,
                3 => 20,
                4 => 2,
                _ => unimplemented!(),
            };

            let mut strings = Vec::with_capacity(limit);
            for i in 0..limit {
                strings.push(format!("foo{}", i));
            }
            let schema = Arc::new(Schema::new(vec![(&cols[0]).into()]));
            Ok(vec![RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(strings))],
            )?])
        });

        config.expect_partition_split_threshold().returning(|| 20);

        config
            .expect_compaction_chunks_total_size_threshold()
            .returning(|| 30);

        let compaction_service = CompactionServiceImpl::new(
            metastore.clone(),
            Arc::new(chunk_store),
            remote_fs,
            Arc::new(config),
        );
        compaction_service.compact(1).await.unwrap();
        let partition_1 = metastore.get_partition(2).await.unwrap();
        let partition_2 = metastore.get_partition(3).await.unwrap();
        let mut result = vec![
            (
                partition_1.get_row().main_table_row_count(),
                partition_1.get_row().get_min_val().as_ref().cloned(),
                partition_1.get_row().get_max_val().as_ref().cloned(),
            ),
            (
                partition_2.get_row().main_table_row_count(),
                partition_2.get_row().get_min_val().as_ref().cloned(),
                partition_2.get_row().get_max_val().as_ref().cloned(),
            ),
        ];
        result.sort_by_key(|(s, _, _)| *s);
        let mut expected = vec![
            (
                12,
                //  4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9
                Some(Row::new(vec![TableValue::String("foo4".to_string())])),
                None,
            ),
            (
                14,
                None,
                // 0, 0, 1, 1, 10, 11, 12, 13, 14, 15, 2, 2, 3, 3
                Some(Row::new(vec![TableValue::String("foo4".to_string())])),
            ),
        ];
        expected.sort_by_key(|(s, _, _)| *s);
        assert_eq!(result, expected);

        let next_partition_id = vec![partition_1, partition_2]
            .iter()
            .find(|p| p.get_row().main_table_row_count() == 14)
            .unwrap()
            .get_id();
        metastore
            .create_chunk(next_partition_id, 2, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(4).await.unwrap();

        compaction_service.compact(next_partition_id).await.unwrap();

        let partition = metastore.get_partition(4).await.unwrap();

        assert_eq!(
            (
                partition.get_row().main_table_row_count(),
                partition.get_row().get_min_val().as_ref().cloned(),
                partition.get_row().get_max_val().as_ref().cloned(),
            ),
            (
                16,
                None,
                Some(Row::new(vec![TableValue::String("foo4".to_string())])),
            ),
        );

        RocksMetaStore::cleanup_test_metastore("compaction");
    }
}
