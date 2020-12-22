use crate::config::ConfigObj;
use crate::metastore::MetaStore;
use crate::remotefs::RemoteFs;
use crate::store::ChunkDataStore;
use crate::table::parquet::ParquetTableStore;
use crate::table::TableStore;
use crate::CubeError;
use async_trait::async_trait;
use itertools::{EitherOrBoth, Itertools};
use num::integer::div_ceil;
use std::sync::Arc;

#[async_trait]
pub trait CompactionService: Send + Sync {
    async fn compact(&self, partition_id: u64) -> Result<(), CubeError>;
}

pub struct CompactionServiceImpl {
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    remote_fs: Arc<dyn RemoteFs>,
    config: Arc<dyn ConfigObj>,
}

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
        let chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id)
            .await?;
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

        let mut rows = Vec::new();
        for chunk in chunks.iter() {
            let mut data = self.chunk_store.get_chunk(chunk.clone()).await?;
            rows.append(data.mut_rows());
        }
        let sort_key_size = index.get_row().sort_key_size();
        rows.sort_by(|a, b| a.sort_key(sort_key_size).cmp(&b.sort_key(sort_key_size)));

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
            new_partition_local_files.push(self.remote_fs.local_file(&new_remote_path).await?)
        }

        let new_partition_file_names = new_partition_local_files.clone();
        let count_and_min_max = tokio::task::spawn_blocking(move || {
            store.merge_rows(
                old_partition_local.as_ref().map(|s| s.as_str()),
                new_partition_file_names,
                rows,
                sort_key_size,
            )
        })
        .await??;

        let mut filtered_partitions = Vec::new();

        for p in new_partitions
            .into_iter()
            .zip_longest(count_and_min_max.iter())
        {
            match p {
                EitherOrBoth::Both(p, _) => {
                    let new_remote_path = p.get_row().get_full_name(p.get_id()).unwrap();
                    self.remote_fs.upload_file(new_remote_path.as_str()).await?;
                    filtered_partitions.push(p);
                }
                EitherOrBoth::Left(p) => {
                    self.meta_store.partition_table().delete(p.get_id()).await?;
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
                    .zip_longest(count_and_min_max.iter().skip(1))
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
                                    assert_eq!(partition.get_row().get_min_val().is_none(), true);
                                    assert_eq!(partition.get_row().get_max_val().is_none(), true);
                                    Ok((*c, (None, None)))
                                } else if i == filtered_partitions.len() - 1 {
                                    Ok(((
                                        *c,
                                        (
                                            Some(min.clone()),
                                            partition.get_row().get_max_val().clone(),
                                        ),
                                    )))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MockConfigObj;
    use crate::metastore::{Column, ColumnType, RocksMetaStore};
    use crate::store::{DataFrame, MockChunkDataStore};
    use crate::table::{Row, TableValue};

    #[actix_rt::test]
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
            .create_chunk(partition.get_id(), 15)
            .await
            .unwrap();
        metastore.chunk_uploaded(2).await.unwrap();

        chunk_store.expect_get_chunk().times(2).returning(move |i| {
            Ok(DataFrame::new(
                cols.clone(),
                (0..{
                    if i.get_id() == 1 {
                        10
                    } else {
                        16
                    }
                })
                    .map(|i| Row::new(vec![TableValue::String(format!("foo{}", i))]))
                    .collect::<Vec<_>>(),
            ))
        });

        config
            .expect_partition_split_threshold()
            .times(1)
            .returning(|| 20);

        let compaction_service = CompactionServiceImpl::new(
            metastore.clone(),
            Arc::new(chunk_store),
            remote_fs,
            Arc::new(config),
        );
        compaction_service.compact(1).await.unwrap();
        let partition_1 = metastore.get_partition(2).await.unwrap();
        assert_eq!(partition_1.get_row().get_min_val(), &None);
        assert_eq!(partition_1.get_row().main_table_row_count(), 14);
        assert_eq!(
            partition_1.get_row().get_max_val(),
            // 0, 0, 1, 1, 10, 11, 12, 13, 14, 15, 2, 2, 3, 3
            &Some(Row::new(vec![TableValue::String("foo4".to_string())]))
        );
        let partition_2 = metastore.get_partition(3).await.unwrap();
        assert_eq!(partition_2.get_row().main_table_row_count(), 12);
        assert_eq!(
            partition_2.get_row().get_min_val(),
            //  4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9
            &Some(Row::new(vec![TableValue::String("foo4".to_string())]))
        );
        assert_eq!(partition_2.get_row().get_max_val(), &None);
        RocksMetaStore::cleanup_test_metastore("compaction");
    }
}
