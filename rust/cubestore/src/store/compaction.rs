use async_trait::async_trait;
use crate::CubeError;
use crate::metastore::{MetaStore};
use std::sync::Arc;
use crate::table::parquet::ParquetTableStore;
use crate::store::ChunkDataStore;
use crate::table::TableStore;
use crate::remotefs::RemoteFs;
use crate::config::ConfigObj;
use num::integer::div_ceil;

#[async_trait]
pub trait CompactionService: Send + Sync {
    async fn compact(&self, partition_id: u64) -> Result<(), CubeError>;
}

pub struct CompactionServiceImpl {
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    remote_fs: Arc<dyn RemoteFs>,
    config: Arc<dyn ConfigObj>
}

impl CompactionServiceImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>
    ) -> Arc<CompactionServiceImpl> {
        Arc::new(CompactionServiceImpl { meta_store, chunk_store, remote_fs, config })
    }
}

#[async_trait]
impl CompactionService for CompactionServiceImpl {
    async fn compact(&self, partition_id: u64) -> Result<(), CubeError> {
        let chunks = self.meta_store.get_chunks_by_partition(partition_id).await?;
        let (partition, index) = self.meta_store.get_partition_for_compaction(partition_id).await?;
        let partition_id = partition.get_id();
        let chunks_row_count = chunks.iter().map(|c| c.get_row().get_row_count()).sum::<u64>();
        let total_count = partition.get_row().main_table_row_count() + chunks_row_count;
        let new_partitions_count = div_ceil(total_count, self.config.partition_split_threshold()) as usize;

        let mut new_partitions = Vec::new();
        for _ in 0..new_partitions_count {
            new_partitions.push(self.meta_store.create_partition(partition.get_row().child(partition.get_id())).await?);
        }

        let mut rows = Vec::new();
        for chunk in chunks.iter() {
            let mut data = self.chunk_store.get_chunk(chunk.clone()).await?;
            rows.append(data.mut_rows());
        }
        let sort_key_size = index.get_row().sort_key_size();
        rows.sort_by(|a, b| a.sort_key(sort_key_size).cmp(&b.sort_key(sort_key_size)));

        let store = ParquetTableStore::new(index.get_row().clone(), 16384); // TODO config
        let old_partition_local = if let Some(f) = partition.get_row().get_full_name(partition.get_id()) {
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
                sort_key_size
            )
        }).await??;

        for p in new_partitions.iter() {
            let new_remote_path = p.get_row().get_full_name(p.get_id()).unwrap();
            self.remote_fs.upload_file(new_remote_path.as_str()).await?;
        }

        self.meta_store.swap_active_partitions(
            vec![partition_id],
            new_partitions.iter().map(|p| p.get_id()).collect::<Vec<_>>(),
            chunks.iter().map(|c| c.get_id()).collect(),
            count_and_min_max.into_iter().enumerate().map(|(i, (c, (min, max)))| {
                if i == 0 && new_partitions_count == 1 {
                    (c, (partition.get_row().get_min_val().as_ref().map(|_| min), partition.get_row().get_max_val().as_ref().map(|_| max)))
                } else if i == 0 && partition.get_row().get_min_val().is_none() {
                    (c, (None, Some(max)))
                } else if i == new_partitions_count - 1 && partition.get_row().get_max_val().is_none() {
                    (c, (Some(min), None))
                } else {
                    (c, (Some(min), Some(max)))
                }
            }).collect::<Vec<_>>()
        ).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{RocksMetaStore, Column, ColumnType};
    use crate::store::{MockChunkDataStore, DataFrame};
    use crate::table::{Row, TableValue};
    use crate::config::MockConfigObj;

    #[actix_rt::test]
    async fn compaction() {
        let (remote_fs, metastore) = RocksMetaStore::prepare_test_metastore("compaction");
        let mut chunk_store = MockChunkDataStore::new();
        let mut config = MockConfigObj::new();
        metastore.create_schema("foo".to_string(), false).await.unwrap();
        let cols = vec![Column::new("name".to_string(), ColumnType::String, 0)];
        metastore.create_table(
            "foo".to_string(),
            "bar".to_string(),
            cols.clone(),
            None,
            None,
            vec![]
        ).await.unwrap();
        metastore.get_default_index(1).await.unwrap();
        let partition = metastore.get_partition(1).await.unwrap();
        metastore.create_chunk(partition.get_id(), 10).await.unwrap();
        metastore.chunk_uploaded(1).await.unwrap();
        metastore.create_chunk(partition.get_id(), 15).await.unwrap();
        metastore.chunk_uploaded(2).await.unwrap();

        chunk_store.expect_get_chunk()
            .times(2)
            .returning(move |i| Ok(
                DataFrame::new(
                    cols.clone(),
                    (0..{ if i.get_id() == 1 { 10 } else { 15 } })
                        .map(|i| Row::new(vec![TableValue::String(format!("foo{}", i))]))
                        .collect::<Vec<_>>()
                )));

        config.expect_partition_split_threshold().times(1).returning(|| 20);

        let compaction_service = CompactionServiceImpl::new(
            metastore.clone(),
            Arc::new(chunk_store),
            remote_fs,
            Arc::new(config)
        );
        compaction_service.compact(1).await.unwrap();
        let partition_1 = metastore.get_partition(2).await.unwrap();
        assert_eq!(partition_1.get_row().get_min_val(), &None);
        assert_eq!(partition_1.get_row().get_max_val(), &Some(Row::new(vec![TableValue::String("foo3".to_string())])));
        let partition_2 = metastore.get_partition(3).await.unwrap();
        assert_eq!(partition_2.get_row().get_min_val(), &Some(Row::new(vec![TableValue::String("foo4".to_string())])));
        assert_eq!(partition_2.get_row().get_max_val(), &None);
        RocksMetaStore::cleanup_test_metastore("compaction");
    }
}