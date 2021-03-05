use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::MetaStore;
use crate::remotefs::RemoteFs;
use crate::store::ChunkDataStore;
use crate::sys::malloc::trim_allocs;
use crate::table::data::{cmp_row_key, RowsView, TableValueR};
use crate::table::parquet::ParquetTableStore;
use crate::table::TableStore;
use crate::CubeError;
use async_trait::async_trait;
use itertools::{EitherOrBoth, Itertools};
use num::integer::div_ceil;
use scopeguard::defer;
use std::mem::swap;
use std::sync::Arc;

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
        defer!(trim_allocs());
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
        let total_count = partition.get_row().main_table_row_count() + chunks_row_count;
        let new_partitions_count =
            div_ceil(total_count, self.config.partition_split_threshold()) as usize;

        let mut new_partitions = Vec::new();
        for _ in 0..new_partitions_count {
            new_partitions.push(
                self.meta_store
                    .create_partition(partition.get_row().child(partition.get_id()))
                    .await?,
            );
        }

        let mut data = Vec::new();
        let mut total_data_rows = 0;
        let num_columns = index.get_row().columns().len();
        for chunk in chunks.iter() {
            let d = self.chunk_store.get_chunk(chunk.clone()).await?;
            assert_eq!(num_columns, d.num_columns());
            total_data_rows += d.num_rows();
            data.push(d);
        }

        let store = ParquetTableStore::new(index.get_row().clone(), 16384); // TODO config
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
        let count_and_min_max = tokio::task::spawn_blocking(move || {
            let mut merge_buffer = Vec::with_capacity(total_data_rows * num_columns);
            for d in &data {
                merge_buffer.extend_from_slice(d.all_values());
            }
            let sort_key_size = index.get_row().sort_key_size() as usize;
            sort_rows(
                &mut merge_buffer,
                total_data_rows,
                num_columns,
                sort_key_size,
            );

            let rows = RowsView::new(&merge_buffer, num_columns);
            store.merge_rows(
                old_partition_local.as_ref().map(|s| s.as_str()),
                new_partition_file_names,
                rows,
                sort_key_size,
            )
        })
        .await??;

        let mut filtered_partitions = Vec::new();

        for (i, p) in new_partitions
            .into_iter()
            .zip_longest(count_and_min_max.iter())
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
                count_and_min_max
                    .iter()
                    .zip_longest(count_and_min_max.iter().skip(1 as usize))
                    .enumerate()
                    .map(|(i, item)| -> Result<_, CubeError> {
                        match item {
                            EitherOrBoth::Both((c, (min, _)), (_, (next_min, _))) => {
                                if i == 0 && partition.get_row().get_min_val().is_none() {
                                    Ok((*c, (None, Some(next_min.clone()))))
                                } else if i < filtered_partitions.len() - 1 {
                                    Ok((*c, (Some(min.clone()), Some(next_min.clone()))))
                                } else {
                                    Err(CubeError::internal(format!(
                                        "Unexpected state for {} new partitions: {}, {:?}",
                                        filtered_partitions.len(),
                                        i,
                                        item
                                    )))
                                }
                            }
                            EitherOrBoth::Left((c, (min, _))) => {
                                if i == 0 && filtered_partitions.len() == 1 {
                                    Ok((
                                        *c,
                                        (
                                            partition.get_row().get_min_val().clone(),
                                            partition.get_row().get_max_val().clone(),
                                        ),
                                    ))
                                } else if i == filtered_partitions.len() - 1 {
                                    Ok((
                                        *c,
                                        (
                                            Some(min.clone()),
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

fn sort_rows(
    values: &mut Vec<TableValueR>,
    num_rows: usize,
    num_columns: usize,
    sort_key_size: usize,
) {
    assert_eq!(values.len(), num_rows * num_columns);
    let mut rows = (0..num_rows).collect_vec();
    rows.sort_unstable_by(|l, r| {
        cmp_row_key(
            sort_key_size,
            &values[l * num_columns..l * num_columns + num_columns],
            &values[r * num_columns..r * num_columns + num_columns],
        )
    });

    // TODO: apply permutation without extra memory.
    let mut result = Vec::with_capacity(values.len());
    for i in rows {
        result.extend_from_slice(&values[i * num_columns..i * num_columns + num_columns]);
    }

    swap(&mut *values, &mut result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MockConfigObj;
    use crate::metastore::{Column, ColumnType, RocksMetaStore};
    use crate::store::MockChunkDataStore;
    use crate::table::data::MutRows;
    use crate::table::{Row, TableValue};

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
            )
            .await
            .unwrap();
        metastore.get_default_index(1).await.unwrap();
        let partition = metastore.get_partition(1).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 10)
            .await
            .unwrap();
        metastore.chunk_uploaded(1).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 16)
            .await
            .unwrap();
        metastore.chunk_uploaded(2).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 20)
            .await
            .unwrap();
        metastore.chunk_uploaded(3).await.unwrap();

        chunk_store.expect_get_chunk().returning(move |i| {
            let limit = match i.get_id() {
                1 => 10,
                2 => 16,
                3 => 20,
                4 => 2,
                _ => unimplemented!(),
            };

            let mut rows = MutRows::new(cols.len());
            for i in 0..limit {
                rows.add_row()
                    .set_interned(0, TableValueR::String(&format!("foo{}", i)));
            }
            Ok(rows.freeze())
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
        metastore.create_chunk(next_partition_id, 2).await.unwrap();
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
