use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::chunks::chunk_file_name;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::partition::partition_file_name;
use crate::metastore::replay_handle::{union_seq_pointer_by_location, SeqPointerForLocation};
use crate::metastore::table::AggregateColumn;
use crate::metastore::{
    deactivate_table_on_corrupt_data, table::Table, Chunk, IdRow, Index, IndexType, MetaStore,
    Partition, PartitionData,
};
use crate::queryplanner::merge_sort::LastRowByUniqueKeyExec;
use crate::queryplanner::metadata_cache::MetadataCacheFactory;
use crate::queryplanner::trace_data_loaded::{DataLoadedSize, TraceDataLoadedExec};
use crate::remotefs::{ensure_temp_file_is_dropped, RemoteFs};
use crate::store::{min_max_values_from_data, ChunkDataStore, ChunkStore, ROW_GROUP_SIZE};
use crate::table::data::{cmp_min_rows, cmp_partition_key};
use crate::table::parquet::{arrow_schema, CubestoreMetadataCacheFactory, ParquetTableStore};
use crate::table::redistribute::redistribute;
use crate::table::{Row, TableValue};
use crate::util::batch_memory::record_batch_buffer_size;
use crate::CubeError;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::compute::{concat_batches, lexsort_to_indices, SortColumn, SortOptions};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::cube_ext;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetExecBuilder;
use datafusion::datasource::physical_plan::{
    FileScanConfig, ParquetExec, ParquetFileReaderFactory,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::count::{count_udaf, Count};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::lit;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{Column, Literal};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, SendableRecordBatchStream};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use futures_util::future::join_all;
use itertools::{EitherOrBoth, Itertools};
use num::integer::div_ceil;
use std::cmp::Ordering;
use std::fs::File;
use std::mem::take;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

#[async_trait]
pub trait CompactionService: DIService + Send + Sync {
    async fn compact(
        &self,
        partition_id: u64,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError>;
    async fn compact_in_memory_chunks(&self, partition_id: u64) -> Result<(), CubeError>;
    async fn compact_node_in_memory_chunks(&self, node: String) -> Result<(), CubeError>;
    /// Split multi-partition that has too many rows. Figures out the keys based on stored data.
    async fn split_multi_partition(&self, multi_partition_id: u64) -> Result<(), CubeError>;
    /// Process partitions that were added concurrently with multi-split.
    async fn finish_multi_split(
        &self,
        multi_partition_id: u64,
        partition_id: u64,
    ) -> Result<(), CubeError>;
}

pub struct CompactionServiceImpl {
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    remote_fs: Arc<dyn RemoteFs>,
    config: Arc<dyn ConfigObj>,
    metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory>,
}

crate::di_service!(CompactionServiceImpl, [CompactionService]);

impl CompactionServiceImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
        remote_fs: Arc<dyn RemoteFs>,
        config: Arc<dyn ConfigObj>,
        metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory>,
    ) -> Arc<CompactionServiceImpl> {
        Arc::new(CompactionServiceImpl {
            meta_store,
            chunk_store,
            remote_fs,
            config,
            metadata_cache_factory,
        })
    }

    fn is_compaction_needed(&self, chunks: &Vec<IdRow<Chunk>>) -> bool {
        let compaction_in_memory_chunks_count_threshold =
            self.config.compaction_in_memory_chunks_count_threshold();

        let oldest_insert_at = chunks
            .iter()
            .filter_map(|c| c.get_row().oldest_insert_at().clone())
            .min();

        chunks.len() > compaction_in_memory_chunks_count_threshold
            || oldest_insert_at
                .map(|min| {
                    Utc::now().signed_duration_since(min).num_seconds()
                        > self
                            .config
                            .compaction_in_memory_chunks_max_lifetime_threshold()
                            as i64
                })
                .unwrap_or(false)
    }

    async fn compact_prepared_in_memory_chunks(
        &self,
        partition: IdRow<Partition>,
        index: IdRow<Index>,
        table: IdRow<Table>,
        chunks: Vec<IdRow<Chunk>>,
    ) -> Result<(), CubeError> {
        // Test invariants
        if !partition.get_row().is_active() && partition.get_row().multi_partition_id().is_some() {
            log::trace!(
                "Cannot compact inactive partition: {:?}",
                partition.get_row()
            );
            return Ok(());
        }

        let compaction_in_memory_chunks_size_limit =
            self.config.compaction_in_memory_chunks_size_limit();

        let active_in_memory = chunks
            .into_iter()
            .filter(|c| c.get_row().in_memory() && c.get_row().active())
            .collect::<Vec<_>>();
        let chunk_and_inmemory = active_in_memory
            .into_iter()
            .map(|c| {
                let chunk_store = self.chunk_store.clone();
                let partition = partition.clone();
                cube_ext::spawn(async move {
                    let has_in_memory_chunk = chunk_store
                        .has_in_memory_chunk(c.clone(), partition)
                        .await?;
                    Result::<_, CubeError>::Ok((c, has_in_memory_chunk))
                })
            })
            .collect::<Vec<_>>();
        let chunk_and_inmemory = join_all(chunk_and_inmemory)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let (in_memory, failed) = chunk_and_inmemory
            .into_iter()
            .partition::<Vec<_>, _>(|(_, has_in_memory_chunk)| *has_in_memory_chunk);
        let (mem_chunks, persistent_chunks) =
            in_memory.into_iter().map(|(c, _)| c).partition(|c| {
                c.get_row().get_row_count() <= compaction_in_memory_chunks_size_limit
                    && c.get_row()
                        .oldest_insert_at()
                        .map(|m| {
                            Utc::now().signed_duration_since(m).num_seconds()
                                <= self
                                    .config
                                    .compaction_in_memory_chunks_max_lifetime_threshold()
                                    as i64
                        })
                        .unwrap_or(false)
            });

        let deactivate_res = self
            .deactivate_and_mark_failed_chunks_for_replay(failed)
            .await;
        let in_memory_res = self
            .compact_chunks_to_memory(mem_chunks, &partition, &index, &table)
            .await;
        let persistent_res = self
            .compact_chunks_to_persistent(persistent_chunks, &partition, &index, &table)
            .await;
        deactivate_res?;
        in_memory_res?;
        persistent_res?;

        Ok(())
    }

    async fn compact_chunks_to_memory(
        &self,
        mut chunks: Vec<IdRow<Chunk>>,
        partition: &IdRow<Partition>,
        index: &IdRow<Index>,
        table: &IdRow<Table>,
    ) -> Result<(), CubeError> {
        if chunks.is_empty() {
            return Ok(());
        }
        let compaction_in_memory_chunks_size_limit =
            self.config.compaction_in_memory_chunks_size_limit();

        chunks.sort_by(|a, b| {
            a.get_row()
                .get_row_count()
                .partial_cmp(&b.get_row().get_row_count())
                .unwrap()
        });

        let mut compact_groups = Vec::new();

        let mut size = 0;
        let mut count = 0;
        let mut start = 0;

        for chunk in chunks.iter() {
            if count > 0 {
                if size >= compaction_in_memory_chunks_size_limit {
                    if count > 1 {
                        compact_groups.push((start, start + count));
                        start = start + count;
                        size = 0;
                        count = 0;
                        continue;
                    }
                }
            }
            size += chunk.get_row().get_row_count();
            count += 1;
        }
        if count > 1 {
            compact_groups.push((start, start + count));
        }

        if compact_groups.is_empty() {
            return Ok(());
        }

        // Prepare merge params
        let unique_key = table.get_row().unique_key_columns();
        let key_size = index.get_row().sort_key_size() as usize;
        let schema = Arc::new(arrow_schema(index.get_row()));
        // Use empty execution plan for main_table, read only from memory chunks
        let main_table: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));

        let aggregate_columns = match index.get_row().get_type() {
            IndexType::Regular => None,
            IndexType::Aggregate => Some(table.get_row().aggregate_columns()),
        };

        let mut old_chunk_ids = Vec::new();
        let mut new_chunk_ids = Vec::new();

        for group in compact_groups.iter() {
            let group_chunks = &chunks[group.0..group.1];
            let (in_memory_columns, mut old_ids) = self
                .chunk_store
                .concat_and_sort_chunks_data(
                    &chunks[group.0..group.1],
                    partition.clone(),
                    index.clone(),
                    key_size,
                )
                .await?;

            if old_ids.is_empty() {
                continue;
            }

            // Get merged RecordBatch
            let batches_stream = merge_chunks(
                key_size,
                main_table.clone(),
                in_memory_columns,
                unique_key.clone(),
                aggregate_columns.clone(),
            )
            .await?;
            let batches = collect(batches_stream).await?;
            let batch = concat_batches(&schema, &batches).unwrap();

            let oldest_insert_at = group_chunks
                .iter()
                .filter_map(|c| {
                    Some(
                        c.get_row()
                            .oldest_insert_at()
                            .clone()
                            .unwrap_or(c.get_row().created_at().clone().unwrap()),
                    )
                })
                .min();

            let (min, max) = min_max_values_from_data(batch.columns(), key_size);
            let chunk = self
                .meta_store
                .create_chunk(partition.get_id(), batch.num_rows(), min, max, true)
                .await?;

            self.meta_store
                .chunk_update_last_inserted(vec![chunk.get_id()], oldest_insert_at)
                .await?;
            let chunk_name = chunk_file_name(chunk.get_id(), chunk.get_row().suffix());
            self.chunk_store.add_memory_chunk(chunk_name, batch).await?;

            old_chunk_ids.append(&mut old_ids);
            new_chunk_ids.push((chunk.get_id(), None));
        }

        let replay_handle_id =
            merge_replay_handles(self.meta_store.clone(), &chunks, table.get_id()).await?;
        self.meta_store
            .swap_chunks_without_check(old_chunk_ids, new_chunk_ids, replay_handle_id)
            .await?;

        Ok(())
    }
    async fn compact_chunks_to_persistent(
        &self,
        chunks: Vec<IdRow<Chunk>>,
        partition: &IdRow<Partition>,
        index: &IdRow<Index>,
        table: &IdRow<Table>,
    ) -> Result<(), CubeError> {
        if chunks.is_empty() {
            return Ok(());
        }

        // Prepare merge params
        let unique_key = table.get_row().unique_key_columns();
        let key_size = index.get_row().sort_key_size() as usize;
        let schema = Arc::new(arrow_schema(index.get_row()));
        // Use empty execution plan for main_table, read only from memory chunks
        let main_table: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));

        let aggregate_columns = match index.get_row().get_type() {
            IndexType::Regular => None,
            IndexType::Aggregate => Some(table.get_row().aggregate_columns()),
        };

        let oldest_insert_at = chunks
            .iter()
            .filter_map(|c| {
                Some(
                    c.get_row()
                        .oldest_insert_at()
                        .clone()
                        .unwrap_or(c.get_row().created_at().clone().unwrap()),
                )
            })
            .min();

        let (in_memory_columns, old_chunk_ids) = self
            .chunk_store
            .concat_and_sort_chunks_data(&chunks[..], partition.clone(), index.clone(), key_size)
            .await?;

        if old_chunk_ids.is_empty() {
            return Ok(());
        }

        let batches_stream = merge_chunks(
            key_size,
            main_table.clone(),
            in_memory_columns,
            unique_key.clone(),
            aggregate_columns.clone(),
        )
        .await?;

        let batches = collect(batches_stream).await?;
        if batches.is_empty() {
            self.meta_store.deactivate_chunks(old_chunk_ids).await?;
            return Ok(());
        }
        let batch = concat_batches(&schema, &batches).unwrap();

        let (chunk, file_size) = self
            .chunk_store
            .add_persistent_chunk(index.clone(), partition.clone(), batch)
            .await?;

        self.meta_store
            .chunk_update_last_inserted(vec![chunk.get_id()], oldest_insert_at)
            .await?;

        self.meta_store
            .swap_chunks_without_check(old_chunk_ids, vec![(chunk.get_id(), file_size)], None)
            .await?;

        Ok(())
    }

    async fn deactivate_and_mark_failed_chunks_for_replay(
        &self,
        failed: Vec<(IdRow<Chunk>, bool)>,
    ) -> Result<(), CubeError> {
        if failed.is_empty() {
            return Ok(());
        }
        let mut deactivate_failed_chunk_ids = Vec::new();
        for (failed_chunk, _) in failed {
            if let Some(handle_id) = failed_chunk.get_row().replay_handle_id() {
                self.meta_store
                    .update_replay_handle_failed_if_exists(*handle_id, true)
                    .await?;
            }
            deactivate_failed_chunk_ids.push(failed_chunk.get_id());
        }
        self.meta_store
            .deactivate_chunks_without_check(deactivate_failed_chunk_ids)
            .await?;

        Ok(())
    }
}
#[async_trait]
impl CompactionService for CompactionServiceImpl {
    async fn compact(
        &self,
        partition_id: u64,
        data_loaded_size: Arc<DataLoadedSize>,
    ) -> Result<(), CubeError> {
        let (partition, index, table, multi_part) = self
            .meta_store
            .get_partition_for_compaction(partition_id)
            .await?;

        if !partition.get_row().is_active() && !multi_part.is_some() {
            log::trace!(
                "Cannot compact inactive partition: {:?}",
                partition.get_row()
            );
            return Ok(());
        }
        if let Some(mp) = &multi_part {
            if mp.get_row().prepared_for_split() {
                log::debug!(
                    "Cancelled compaction of {}. It runs concurrently with multi-split",
                    partition_id
                );
                return Ok(());
            }
        }
        let mut all_pending_chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?;
        all_pending_chunks.sort_by_key(|c| c.get_row().get_row_count());
        let mut size = 0;
        let chunks = all_pending_chunks
            .iter()
            .filter(|c| !c.get_row().in_memory())
            .take_while(|c| {
                if size == 0 {
                    size += c.get_row().get_row_count();
                    true
                } else {
                    size += c.get_row().get_row_count();
                    size <= self.config.compaction_chunks_total_size_threshold()
                }
            })
            .map(|c| c.clone())
            .collect::<Vec<_>>();

        if chunks.is_empty() {
            return Ok(());
        }

        let partition_id = partition.get_id();

        let mut data = Vec::new();
        let mut chunks_to_use = Vec::new();
        let mut chunks_total_size = 0;
        let num_columns = index.get_row().columns().len();

        for chunk in chunks.iter() {
            for b in self
                .chunk_store
                .get_chunk_columns_with_preloaded_meta(
                    chunk.clone(),
                    partition.clone(),
                    index.clone(),
                )
                .await?
            {
                assert_eq!(
                    num_columns,
                    b.num_columns(),
                    "Column len mismatch for {:?} and {:?}",
                    index,
                    chunk
                );
                chunks_total_size += record_batch_buffer_size(&b);
                data.push(b);
            }
            chunks_to_use.push(chunk.clone());
            if chunks_total_size > self.config.compaction_chunks_in_memory_size_threshold() as usize
            {
                break;
            }
        }

        data_loaded_size.add(chunks_total_size);

        let chunks = chunks_to_use;

        let chunks_row_count = chunks
            .iter()
            .map(|c| c.get_row().get_row_count())
            .sum::<u64>();
        // For multi-partitions, we only compact chunks and never change the main table.
        // And we never split, multi-partitions have a different process for that.
        let new_chunk = match &multi_part {
            None => None,
            Some(_) => {
                if chunks.len() < 2 {
                    return Ok(());
                }

                //We don't track min/max chunk values for multi-parititons
                Some(
                    self.meta_store
                        .create_chunk(partition_id, chunks_row_count as usize, None, None, false)
                        .await?,
                )
            }
        };

        let mut total_rows = chunks_row_count;
        if new_chunk.is_none() {
            total_rows += partition.get_row().main_table_row_count();
        }
        let mut new_partitions = Vec::new();
        if new_chunk.is_none() {
            let pending_rows = all_pending_chunks
                .iter()
                .map(|c| c.get_row().get_row_count())
                .sum::<u64>()
                + partition.get_row().main_table_row_count();
            // Split partitions ahead for more than actual compaction size. The trade off here is partition accuracy vs write amplification
            let new_partitions_count_by_rows = (div_ceil(
                pending_rows,
                table
                    .get_row()
                    .partition_split_threshold_or_default(self.config.partition_split_threshold()),
            ) as usize)
                // Do not allow to much of new partitions to limit partition accuracy trade off
                .min(16);
            let new_partitions_count_by_file_size =
                if let Some(partition_file_size) = partition.get_row().file_size() {
                    let threshold = self.config.partition_size_split_threshold_bytes();
                    (div_ceil(partition_file_size, threshold) as usize).min(16)
                } else {
                    1
                };

            let new_partitions_count =
                new_partitions_count_by_rows.max(new_partitions_count_by_file_size);

            for _ in 0..new_partitions_count {
                new_partitions.push(
                    self.meta_store
                        .create_partition(Partition::new_child(&partition, None))
                        .await?,
                );
            }
        }

        let store = ParquetTableStore::new(
            index.get_row().clone(),
            ROW_GROUP_SIZE,
            self.metadata_cache_factory.clone(),
        );
        let old_partition_remote = match &new_chunk {
            Some(_) => None,
            None => partition.get_row().get_full_name(partition.get_id()),
        };
        let old_partition_local = if let Some(f) = old_partition_remote {
            let result = self
                .remote_fs
                .download_file(f, partition.get_row().file_size())
                .await;
            deactivate_table_on_corrupt_data(self.meta_store.clone(), &result, &partition, None)
                .await;
            Some(result?)
        } else {
            None
        };
        let mut new_local_files = Vec::new();
        if let Some(c) = &new_chunk {
            let remote = ChunkStore::chunk_remote_path(c.get_id(), c.get_row().suffix());
            new_local_files.push(self.remote_fs.temp_upload_path(remote).await?);
        } else {
            for p in new_partitions.iter() {
                let new_remote_path = partition_file_name(p.get_id(), p.get_row().suffix());
                new_local_files.push(self.remote_fs.temp_upload_path(new_remote_path).await?);
            }
        }

        let new_local_files2 = new_local_files.clone();

        let new_local_files = scopeguard::guard(new_local_files, |files| {
            for f in files {
                ensure_temp_file_is_dropped(f);
            }
        });

        let key_size = index.get_row().sort_key_size() as usize;
        let (store, new) = cube_ext::spawn_blocking(move || -> Result<_, CubeError> {
            // Concat rows from all chunks.
            let mut columns = Vec::with_capacity(num_columns);
            for i in 0..num_columns {
                let v = datafusion::arrow::compute::concat(
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
                new.push(datafusion::arrow::compute::take(
                    c.as_ref(),
                    &indices,
                    None,
                )?)
            }
            Ok((store, new))
        })
        .await??;

        // Merge and write rows.
        let schema = Arc::new(arrow_schema(index.get_row()));
        let main_table: Arc<dyn ExecutionPlan> = match old_partition_local {
            Some(file) => {
                let file_scan = FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema)
                    .with_file(PartitionedFile::from_path(file.to_string())?);
                let parquet_exec = ParquetExecBuilder::new(file_scan)
                    .with_parquet_file_reader_factory(
                        self.metadata_cache_factory
                            .cache_factory()
                            .make_noop_cache(),
                    )
                    .build();

                Arc::new(TraceDataLoadedExec::new(
                    Arc::new(parquet_exec),
                    data_loaded_size.clone(),
                ))
            }
            None => Arc::new(EmptyExec::new(schema.clone())),
        };

        let table = self
            .meta_store
            .get_table_by_id(index.get_row().table_id())
            .await?;
        let unique_key = table.get_row().unique_key_columns();
        let aggregate_columns = match index.get_row().get_type() {
            IndexType::Regular => None,
            IndexType::Aggregate => Some(table.get_row().aggregate_columns()),
        };
        let records =
            merge_chunks(key_size, main_table, new, unique_key, aggregate_columns).await?;
        let count_and_min = write_to_files(
            records,
            total_rows as usize,
            store,
            &table,
            new_local_files2,
        )
        .await?;

        if let Some(c) = &new_chunk {
            assert_eq!(new_local_files.len(), 1);
            let remote = ChunkStore::chunk_remote_path(c.get_id(), c.get_row().suffix());
            let file_size = self
                .remote_fs
                .upload_file(new_local_files[0].clone(), remote.clone())
                .await?;
            let chunk_ids = chunks.iter().map(|c| c.get_id()).collect_vec();
            // In memory chunks shouldn't ever get here. Otherwise replay handle should be defined.
            let swapped = self
                .meta_store
                .swap_compacted_chunks(partition_id, chunk_ids, c.get_id(), file_size)
                .await?;
            if !swapped {
                log::debug!(
                    "Cancelled compaction of {}. It runs concurrently with multi-split",
                    partition_id
                );
                self.remote_fs.delete_file(remote).await?;
            }
            return Ok(());
        }

        let mut filtered_partitions = Vec::new();
        for (i, p) in new_partitions
            .into_iter()
            .zip_longest(count_and_min.iter())
            .enumerate()
        {
            match p {
                EitherOrBoth::Both(p, _) => {
                    let new_remote_path = partition_file_name(p.get_id(), p.get_row().suffix());
                    let file_size = self
                        .remote_fs
                        .upload_file(new_local_files[i].clone(), new_remote_path.to_string())
                        .await?;
                    filtered_partitions.push((p, file_size));
                }
                EitherOrBoth::Left(p) => {
                    self.meta_store.delete_partition(p.get_id()).await?;
                    // TODO: ensure all files get removed on errors.
                    let _ = tokio::fs::remove_file(&new_local_files[i]).await;
                }
                EitherOrBoth::Right(_) => {
                    return Err(CubeError::internal(format!(
                        "Unexpected state during partitioning: {:?}",
                        p
                    )))
                }
            }
        }

        let num_filtered = filtered_partitions.len();

        let partition_min = partition.get_row().get_min_val().clone();
        let partition_max = partition.get_row().get_max_val().clone();
        self.meta_store
            .swap_active_partitions(
                vec![(partition, chunks)],
                filtered_partitions,
                count_and_min
                    .iter()
                    .zip_longest(count_and_min.iter().skip(1 as usize))
                    .enumerate()
                    .map(|(i, item)| -> Result<_, CubeError> {
                        match item {
                            EitherOrBoth::Both((c, min, max), (_, next_min, _)) => {
                                if i == 0 && partition_min.is_none() {
                                    Ok((
                                        *c as u64,
                                        (None, Some(Row::new(next_min.clone()))),
                                        (Some(Row::new(min.clone())), Some(Row::new(max.clone()))),
                                    ))
                                } else if i < num_filtered - 1 {
                                    Ok((
                                        *c as u64,
                                        (
                                            Some(Row::new(min.clone())),
                                            Some(Row::new(next_min.clone())),
                                        ),
                                        (Some(Row::new(min.clone())), Some(Row::new(max.clone()))),
                                    ))
                                } else {
                                    Err(CubeError::internal(format!(
                                        "Unexpected state for {} new partitions: {}, {:?}",
                                        num_filtered, i, item
                                    )))
                                }
                            }
                            EitherOrBoth::Left((c, min, max)) => {
                                if i == 0 && num_filtered == 1 {
                                    Ok((
                                        *c as u64,
                                        (partition_min.clone(), partition_max.clone()),
                                        (Some(Row::new(min.clone())), Some(Row::new(max.clone()))),
                                    ))
                                } else if i == num_filtered - 1 {
                                    Ok((
                                        *c as u64,
                                        (Some(Row::new(min.clone())), partition_max.clone()),
                                        (Some(Row::new(min.clone())), Some(Row::new(max.clone()))),
                                    ))
                                } else {
                                    Err(CubeError::internal(format!(
                                        "Unexpected state for {} new partitions: {}, {:?}",
                                        num_filtered, i, item
                                    )))
                                }
                            }
                            EitherOrBoth::Right(_) => Err(CubeError::internal(format!(
                                "Unexpected state for {} new partitions: {}, {:?}",
                                num_filtered, i, item
                            ))),
                        }
                    })
                    .collect::<Result<Vec<_>, CubeError>>()?,
            )
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn compact_node_in_memory_chunks(&self, node: String) -> Result<(), CubeError> {
        let candidates = self
            .meta_store
            .get_partitions_for_in_memory_compaction(node)
            .await?;
        let mut futures = Vec::new();

        for (partition, index, table, chunks) in candidates.into_iter() {
            if self.is_compaction_needed(&chunks) {
                futures
                    .push(self.compact_prepared_in_memory_chunks(partition, index, table, chunks));
            }
        }

        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    async fn compact_in_memory_chunks(&self, partition_id: u64) -> Result<(), CubeError> {
        let (partition, index, table, _) = self
            .meta_store
            .get_partition_for_compaction(partition_id)
            .await?;

        let chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?
            .into_iter()
            .filter(|c| c.get_row().in_memory() && c.get_row().active())
            .collect::<Vec<_>>();

        self.compact_prepared_in_memory_chunks(partition, index, table, chunks)
            .await
    }

    async fn split_multi_partition(&self, multi_partition_id: u64) -> Result<(), CubeError> {
        let (multi_index, multi_partition, partitions) = self
            .meta_store
            .prepare_multi_partition_for_split(multi_partition_id)
            .await?;
        log::trace!(
            "Preparing to split multi-partition {}:{:?} with partitions {:?}",
            multi_partition_id,
            multi_partition.get_row(),
            partitions
        );
        let key_len = multi_index.get_row().key_columns().len();

        // Find key ranges for new partitions.
        // TODO deactivate corrupt tables
        let files = download_files(&partitions, self.remote_fs.clone()).await?;
        let keys = find_partition_keys(
            keys_with_counts(
                &files,
                self.metadata_cache_factory.cache_factory().as_ref(),
                key_len,
                // TODO
                Arc::new(arrow_schema(
                    partitions.iter().next().unwrap().index.get_row(),
                )),
            )
            .await?,
            key_len,
            // TODO should it respect table partition_split_threshold?
            self.config.partition_split_threshold() as usize,
        )
        .await?;
        // There is no point if we cannot split the partition.
        log::trace!(
            "Split keys for multi-partition {}:{:?}",
            multi_partition_id,
            keys
        );
        if keys.len() == 0 {
            return Ok(());
        }

        // Split multi-partition.
        let create_child = |min, max| {
            self.meta_store
                .create_multi_partition(MultiPartition::new_child(&multi_partition, min, max))
        };
        let mut mchildren = Vec::with_capacity(1 + keys.len());
        mchildren.push(
            create_child(
                multi_partition.get_row().min_row().cloned(),
                Some(keys[0].clone()),
            )
            .await?,
        );
        for i in 0..keys.len() - 1 {
            mchildren.push(create_child(Some(keys[i].clone()), Some(keys[i + 1].clone())).await?);
        }
        mchildren.push(
            create_child(
                Some(keys.last().unwrap().clone()),
                multi_partition.get_row().max_row().cloned(),
            )
            .await?,
        );

        let mut s = MultiSplit::new(
            self.meta_store.clone(),
            self.remote_fs.clone(),
            self.metadata_cache_factory.clone(),
            keys,
            key_len,
            multi_partition_id,
            mchildren,
        );
        for p in partitions {
            s.split_single_partition(p).await?;
        }
        s.finish(true).await
    }

    async fn finish_multi_split(
        &self,
        multi_partition_id: u64,
        partition_id: u64,
    ) -> Result<(), CubeError> {
        let (data, mut children) = self
            .meta_store
            .prepare_multi_split_finish(multi_partition_id, partition_id)
            .await?;
        log::trace!(
            "Preparing to finish split of {} with partition {:?} and multi-part children {:?}",
            multi_partition_id,
            data,
            children
        );

        let key_len = data.index.get_row().sort_key_size() as usize;
        children.sort_unstable_by(|l, r| {
            cmp_min_rows(key_len, l.get_row().min_row(), r.get_row().min_row())
        });
        assert!(2 <= children.len(), "2 <= {}", children.len());

        let keys = children
            .iter()
            .skip(1)
            .map(|c| c.get_row().min_row().cloned().unwrap())
            .collect_vec();

        let mut s = MultiSplit::new(
            self.meta_store.clone(),
            self.remote_fs.clone(),
            self.metadata_cache_factory.clone(),
            keys,
            key_len,
            multi_partition_id,
            children,
        );
        s.split_single_partition(data).await?;
        s.finish(false).await
    }
}

/// Compute keys that partitions must be split by.
async fn find_partition_keys(
    p: AggregateExec,
    key_len: usize,
    rows_per_partition: usize,
) -> Result<Vec<Row>, CubeError> {
    let mut s = p.execute(0, Arc::new(TaskContext::default()))?;
    let mut points = Vec::new();
    let mut row_count = 0;
    while let Some(b) = s.next().await.transpose()? {
        let counts = b
            .column(key_len)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values();
        for i in 0..b.num_rows() {
            let c = counts[i] as usize;
            if rows_per_partition < row_count + c {
                points.push(Row::new(TableValue::from_columns(
                    &b.columns()[0..key_len],
                    i,
                )));
                row_count = 0;
            }
            row_count += c;
        }
    }

    Ok(points)
}

async fn read_files(
    files: &[String],
    metadata_cache_factory: &dyn MetadataCacheFactory,
    key_len: usize,
    projection: Option<Vec<usize>>,
    schema: Arc<Schema>,
) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
    assert!(!files.is_empty());
    // let mut inputs = Vec::<Arc<dyn ExecutionPlan>>::with_capacity(files.len());
    let file_scan = FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema)
        .with_file_group(
            files
                .iter()
                .map(|f| PartitionedFile::from_path(f.to_string()))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .with_projection(projection);
    let plan = ParquetExecBuilder::new(file_scan)
        .with_parquet_file_reader_factory(metadata_cache_factory.make_noop_cache())
        .build();
    // TODO upgrade DF
    // for f in files {
    //     inputs.push(Arc::new(ParquetExec::try_from_files_with_cache(
    //         &[f.as_str()],
    //         projection.clone(),
    //         None,
    //         ROW_GROUP_SIZE,
    //         1,
    //         None,
    //         metadata_cache_factory.make_noop_cache(),
    //     )?));
    // }
    // let plan = Arc::new(UnionExec::new(inputs));
    let fields = plan.schema();
    let fields = fields.fields();
    let mut columns = Vec::with_capacity(fields.len());
    for i in 0..key_len {
        columns.push(PhysicalSortExpr::new(
            Arc::new(Column::new(fields[i].name().as_str(), i)),
            SortOptions::default(),
        ));
    }
    Ok(Arc::new(SortPreservingMergeExec::new(
        columns.clone(),
        Arc::new(plan),
    )))
}

/// The returned execution plan computes all keys in sorted order and the count of rows that have
/// this key in the input files.
async fn keys_with_counts(
    files: &[String],
    metadata_cache_factory: &dyn MetadataCacheFactory,
    key_len: usize,
    schema: Arc<Schema>,
) -> Result<AggregateExec, CubeError> {
    let projection = (0..key_len).collect_vec();
    let plan = read_files(
        files,
        metadata_cache_factory,
        key_len,
        Some(projection.clone()),
        schema,
    )
    .await?;

    let fields = plan.schema();
    let fields = fields.fields();
    let mut key = Vec::<(Arc<dyn PhysicalExpr>, String)>::with_capacity(key_len);
    for i in 0..key_len {
        let name = fields[i].name().clone();
        let col = Column::new(fields[i].name().as_str(), i);
        key.push((Arc::new(col), name));
    }
    let agg: Vec<AggregateFunctionExpr> = vec![AggregateExprBuilder::new(
        count_udaf(),
        vec![Arc::new(Literal::new(ScalarValue::Int64(Some(1))))],
    )
    .build()?];
    let plan_schema = plan.schema();
    let plan = AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(key),
        agg,
        Vec::new(),
        plan,
        plan_schema,
    )?;
    Ok(plan)
}

fn collect_remote_files(p: &PartitionData, out: &mut Vec<(String, Option<u64>)>) {
    if p.partition.get_row().is_active() {
        if let Some(f) = p.partition.get_row().get_full_name(p.partition.get_id()) {
            out.push((f, p.partition.get_row().file_size()))
        }
    }
    for c in &p.chunks {
        out.push((
            ChunkStore::chunk_remote_path(c.get_id(), c.get_row().suffix()),
            c.get_row().file_size(),
        ));
    }
}

async fn download_files(
    ps: &[PartitionData],
    fs: Arc<dyn RemoteFs>,
) -> Result<Vec<String>, CubeError> {
    let mut tasks = Vec::new();
    let mut remote_files = Vec::new();
    for p in ps {
        collect_remote_files(p, &mut remote_files);
        for f in &mut remote_files {
            let (f, size) = take(f);
            let fs = fs.clone();
            tasks.push(cube_ext::spawn(
                async move { fs.download_file(f, size).await },
            ))
        }
        remote_files.clear();
    }

    let mut results = Vec::new();
    for t in tasks {
        results.push(t.await??)
    }
    Ok(results)
}

/// Writes [records] into [files], trying to split into equally-sized rows, with an additional
/// restriction that files must have non-intersecting key ranges.
/// [records] must be sorted and have exactly [num_rows] rows.
pub(crate) async fn write_to_files(
    records: SendableRecordBatchStream,
    num_rows: usize,
    store: ParquetTableStore,
    table: &IdRow<Table>,
    files: Vec<String>,
) -> Result<Vec<(usize, Vec<TableValue>, Vec<TableValue>)>, CubeError> {
    let rows_per_file = div_ceil(num_rows as usize, files.len());
    let key_size = store.key_size() as usize;
    let partition_split_key_size = store.partition_split_key_size() as usize;

    let mut last_row = Vec::new();
    // (num_rows, first_row) for all processed writers.
    let stats = Arc::new(Mutex::new(vec![(0, Vec::new(), Vec::new())]));
    let stats_ref = stats.clone();

    let pick_writer = |b: &RecordBatch| -> WriteBatchTo {
        let stats_ref = stats_ref.clone();
        let mut stats = stats_ref.lock().unwrap();

        let (num_rows, first_row, max_row) = stats.last_mut().unwrap();
        if first_row.is_empty() {
            *first_row = TableValue::from_columns(&b.columns()[0..key_size], 0);
        }
        if *num_rows + b.num_rows() < rows_per_file {
            *num_rows += b.num_rows();
            if b.num_rows() > 0 {
                *max_row = TableValue::from_columns(&b.columns()[0..key_size], b.num_rows() - 1);
            }

            return WriteBatchTo::Current;
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
        *max_row = last_row.clone();
        if i == b.num_rows() {
            *num_rows += b.num_rows();
            return WriteBatchTo::Current;
        }

        *num_rows += i;
        stats.push((0, Vec::new(), Vec::new()));
        last_row.clear();
        return WriteBatchTo::Next {
            rows_for_current: i,
        };
    };

    write_to_files_impl(records, store, files, table, pick_writer).await?;

    let mut stats = take(stats.lock().unwrap().deref_mut());
    if stats.last().unwrap().0 == 0 {
        stats.pop();
    }
    Ok(stats)
}

enum WriteBatchTo {
    /// Current batch must be fully written into the current file.
    Current,
    /// Current batch must be written (partially or fully) into the next file.
    Next { rows_for_current: usize },
}

async fn write_to_files_impl(
    records: SendableRecordBatchStream,
    store: ParquetTableStore,
    files: Vec<String>,
    table: &IdRow<Table>,
    mut pick_writer: impl FnMut(&RecordBatch) -> WriteBatchTo,
) -> Result<(), CubeError> {
    let schema = Arc::new(store.arrow_schema());
    let writer_props = store.writer_props(table).await?;
    let mut writers = files.into_iter().map(move |f| -> Result<_, CubeError> {
        Ok(ArrowWriter::try_new(
            File::create(f)?,
            schema.clone(),
            Some(writer_props.clone()),
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

    let mut writer_i = 0;
    let mut process_row_group = move |b: RecordBatch| -> Result<_, CubeError> {
        match pick_writer(&b) {
            WriteBatchTo::Current => Ok(((writer_i, b), None)),
            WriteBatchTo::Next {
                rows_for_current: n,
            } => {
                let current_writer = writer_i;
                writer_i += 1; // Next iteration will write into the next file.
                Ok((
                    (current_writer, b.slice(0, n)),
                    Some(b.slice(n, b.num_rows() - n)),
                ))
            }
        }
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

    Ok(())
}

async fn write_to_files_by_keys(
    records: SendableRecordBatchStream,
    store: ParquetTableStore,
    table: &IdRow<Table>,
    files: Vec<String>,
    keys: Vec<Row>,
) -> Result<Vec<usize>, CubeError> {
    assert_eq!(files.len(), 1 + keys.len());
    let mut row_counts = Vec::with_capacity(files.len());
    row_counts.push(0);
    let row_counts = Arc::new(Mutex::new(row_counts));
    let key_size = store.key_size() as usize;
    let mut next_key = 0;
    let row_counts_ref = row_counts.clone();
    let pick_writer = move |b: &RecordBatch| {
        assert_ne!(b.num_rows(), 0);
        let mut row_counts = row_counts_ref.lock().unwrap();
        if next_key == keys.len() {
            *row_counts.last_mut().unwrap() += b.num_rows();
            return WriteBatchTo::Current;
        }
        if cmp_partition_key(
            key_size,
            keys[next_key].values().as_slice(),
            b.columns(),
            b.num_rows() - 1,
        ) > Ordering::Equal
        {
            *row_counts.last_mut().unwrap() += b.num_rows();
            return WriteBatchTo::Current;
        }
        for i in 0..b.num_rows() {
            if cmp_partition_key(key_size, keys[next_key].values().as_slice(), b.columns(), i)
                <= Ordering::Equal
            {
                *row_counts.last_mut().unwrap() += i;
                row_counts.push(0);

                next_key += 1;
                return WriteBatchTo::Next {
                    rows_for_current: i,
                };
            }
        }
        panic!("impossible")
    };
    let num_files = files.len();
    write_to_files_impl(records, store, files, table, pick_writer).await?;

    let mut row_counts: Vec<usize> = take(row_counts.lock().unwrap().as_mut());
    assert!(
        row_counts.len() <= num_files,
        "{} <= {}",
        row_counts.len(),
        num_files
    );
    row_counts.resize(num_files, 0);
    Ok(row_counts)
}

///Builds a `SendableRecordBatchStream` containing the result of merging a persistent chunk `l` with an in-memory chunk `r`
pub async fn merge_chunks(
    key_size: usize,
    l: Arc<dyn ExecutionPlan>,
    r: Vec<ArrayRef>,
    unique_key_columns: Option<Vec<&crate::metastore::Column>>,
    aggregate_columns: Option<Vec<AggregateColumn>>,
) -> Result<SendableRecordBatchStream, CubeError> {
    let schema = l.schema();
    let r = RecordBatch::try_new(schema.clone(), r)?;

    let mut key = Vec::with_capacity(key_size);
    for i in 0..key_size {
        let f = schema.field(i);
        key.push(PhysicalSortExpr::new(
            Arc::new(Column::new(f.name().as_str(), i)),
            SortOptions::default(),
        ));
    }

    let inputs = UnionExec::new(vec![
        l,
        Arc::new(MemoryExec::try_new(&[vec![r]], schema, None)?),
    ]);
    let mut res: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(key, Arc::new(inputs)));

    if let Some(aggregate_columns) = aggregate_columns {
        let mut groups = Vec::with_capacity(key_size);
        let schema = res.schema();
        for i in 0..key_size {
            let f = schema.field(i);
            let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(f.name().as_str(), i));
            groups.push((col, f.name().clone()));
        }
        let aggregates = aggregate_columns
            .iter()
            .map(|aggr_col| aggr_col.aggregate_expr(&res.schema()))
            .collect::<Result<Vec<_>, _>>()?;
        let aggregates_len = aggregates.len();

        res = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(groups),
            aggregates,
            vec![None; aggregates_len],
            res.clone(),
            schema,
        )?);
    } else if let Some(key_columns) = unique_key_columns {
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
        )?);
    }

    Ok(res.execute(0, Arc::new(TaskContext::default()))?)
}

pub async fn merge_replay_handles(
    meta_store: Arc<dyn MetaStore>,
    chunks: &Vec<IdRow<Chunk>>,
    table_id: u64,
) -> Result<Option<u64>, CubeError> {
    let handles = meta_store
        .get_replay_handles_by_ids(
            chunks
                .iter()
                .filter_map(|c| c.get_row().replay_handle_id().clone())
                .collect(),
        )
        .await?;
    let mut seq_pointer_by_location = None;
    for handle in handles.iter() {
        union_seq_pointer_by_location(
            &mut seq_pointer_by_location,
            handle.get_row().seq_pointers_by_location(),
        )?;
    }
    if let Some(_) = seq_pointer_by_location {
        let replay_handle = meta_store
            .create_replay_handle_from_seq_pointers(table_id, seq_pointer_by_location)
            .await?;

        Ok(Some(replay_handle.get_id()))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::MockCluster;
    use crate::config::Config;
    use crate::config::MockConfigObj;
    use crate::metastore::{
        BaseRocksStoreFs, Column, ColumnType, IndexDef, IndexType, RocksMetaStore,
    };
    use crate::queryplanner::metadata_cache::BasicMetadataCacheFactory;
    use crate::remotefs::LocalDirRemoteFs;
    use crate::store::MockChunkDataStore;
    use crate::table::data::rows_to_columns;
    use crate::table::parquet::CubestoreMetadataCacheFactoryImpl;
    use crate::table::{cmp_same_types, Row, TableValue};
    use cuberockstore::rocksdb::{Options, DB};
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::collect;
    use std::fs;
    use std::path::{Path, PathBuf};

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
        metastore.get_default_index(1).await.unwrap();
        let partition = metastore.get_partition(1).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 10, None, None, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(1).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 16, None, None, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(2).await.unwrap();
        metastore
            .create_chunk(partition.get_id(), 20, None, None, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(3).await.unwrap();

        let cols_to_move = cols.clone();
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
            let schema = Arc::new(Schema::new(vec![<&Column as Into<Field>>::into(
                &cols_to_move[0],
            )]));
            Ok(vec![RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(strings))],
            )?])
        });
        let cols_to_move = cols.clone();
        chunk_store
            .expect_get_chunk_columns_with_preloaded_meta()
            .returning(move |c, _i, _p| {
                let limit = match c.get_id() {
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
                let schema = Arc::new(Schema::new(vec![<&Column as Into<Field>>::into(
                    &cols_to_move[0],
                )]));
                Ok(vec![RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(strings))],
                )?])
            });

        config.expect_partition_split_threshold().returning(|| 20);
        config
            .expect_compaction_chunks_in_memory_size_threshold()
            .returning(|| 3 * 1024 * 1024 * 1024);

        config
            .expect_partition_size_split_threshold_bytes()
            .returning(|| 100 * 1024 * 1024);

        config
            .expect_compaction_chunks_total_size_threshold()
            .returning(|| 30);

        let compaction_service = CompactionServiceImpl::new(
            metastore.clone(),
            Arc::new(chunk_store),
            remote_fs,
            Arc::new(config),
            CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
        );
        compaction_service
            .compact(1, DataLoadedSize::new())
            .await
            .unwrap();

        fn sort_fn(
            a: &(u64, Option<Row>, Option<Row>),
            b: &(u64, Option<Row>, Option<Row>),
        ) -> Ordering {
            if a.1.is_none() && b.1.is_some() {
                Ordering::Less
            } else if a.1.is_some() && b.1.is_none() {
                Ordering::Greater
            } else if a.1.is_none() && b.1.is_none() {
                Ordering::Equal
            } else {
                cmp_same_types(
                    &a.1.as_ref().unwrap().values()[0],
                    &b.1.as_ref().unwrap().values()[0],
                )
            }
        }

        let active_partitions = metastore
            .get_active_partitions_by_index_id(1)
            .await
            .unwrap();
        let mut result = active_partitions
            .iter()
            .map(|p| {
                (
                    p.get_row().main_table_row_count(),
                    p.get_row().get_min_val().as_ref().cloned(),
                    p.get_row().get_max_val().as_ref().cloned(),
                )
            })
            .collect::<Vec<_>>();

        result.sort_by(sort_fn);
        let mut expected = vec![
            (
                9,
                // 0, 0, 1, 1, 10, 11, 12, 13, 14,
                None,
                Some(Row::new(vec![TableValue::String("foo15".to_string())])),
            ),
            (
                9,
                Some(Row::new(vec![TableValue::String("foo15".to_string())])),
                // 15, 2, 2, 3, 3, 4, 4, 5, 5,
                Some(Row::new(vec![TableValue::String("foo6".to_string())])),
            ),
            (
                8,
                //  6, 6, 7, 7, 8, 8, 9, 9
                Some(Row::new(vec![TableValue::String("foo6".to_string())])),
                None,
            ),
        ];
        expected.sort_by(sort_fn);
        assert_eq!(result, expected);

        let next_partition_id = active_partitions
            .iter()
            .find(|p| p.get_row().get_min_val().is_none())
            .unwrap()
            .get_id();
        metastore
            .create_chunk(next_partition_id, 2, None, None, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(4).await.unwrap();

        compaction_service
            .compact(next_partition_id, DataLoadedSize::new())
            .await
            .unwrap();

        let active_partitions = metastore
            .get_active_partitions_by_index_id(1)
            .await
            .unwrap();
        let mut result = active_partitions
            .iter()
            .map(|p| {
                (
                    p.get_row().main_table_row_count(),
                    p.get_row().get_min_val().as_ref().cloned(),
                    p.get_row().get_max_val().as_ref().cloned(),
                )
            })
            .collect::<Vec<_>>();

        result.sort_by(sort_fn);
        let mut expected = vec![
            (
                11,
                // 0, 0, 0, 1, 1, 1, 10, 11, 12, 13, 14,
                None,
                Some(Row::new(vec![TableValue::String("foo15".to_string())])),
            ),
            (
                9,
                Some(Row::new(vec![TableValue::String("foo15".to_string())])),
                // 15, 2, 2, 3, 3, 4, 4, 5, 5,
                Some(Row::new(vec![TableValue::String("foo6".to_string())])),
            ),
            (
                8,
                //  6, 6, 7, 7, 8, 8, 9, 9
                Some(Row::new(vec![TableValue::String("foo6".to_string())])),
                None,
            ),
        ];
        expected.sort_by(sort_fn);
        assert_eq!(result, expected);

        RocksMetaStore::cleanup_test_metastore("compaction");
    }

    #[tokio::test]
    async fn compact_in_memory_chunks() {
        // arrange
        let (remote_fs, metastore) =
            RocksMetaStore::prepare_test_metastore("compact_in_memory_chunks");
        let config = Config::test("compact_in_memory_chunks");
        let mut cluster = MockCluster::new();
        cluster
            .expect_server_name()
            .return_const("test".to_string());
        cluster
            .expect_node_name_by_partition()
            .returning(move |_i| "test".to_string());
        let chunk_store = ChunkStore::new(
            metastore.clone(),
            remote_fs.clone(),
            Arc::new(cluster),
            config.config_obj(),
            CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
            10,
        );
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
        metastore.get_default_index(1).await.unwrap();
        let partition = metastore.get_partition(1).await.unwrap();

        let rows = (0..5)
            .map(|i| Row::new(vec![TableValue::String(format!("Foo {}", i))]))
            .collect::<Vec<_>>();
        let rows2 = (3..7)
            .map(|i| Row::new(vec![TableValue::String(format!("Foo {}", i))]))
            .collect::<Vec<_>>();
        let data = rows_to_columns(&cols, &rows);
        let data2 = rows_to_columns(&cols, &rows2);
        let index = metastore
            .get_index(partition.get_row().get_index_id())
            .await
            .unwrap();
        let schema = Arc::new(arrow_schema(index.get_row()));
        let batch = RecordBatch::try_new(schema.clone(), data).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), data2).unwrap();
        let chunk_first = metastore
            .create_chunk(partition.get_id(), 5, None, None, true)
            .await
            .unwrap();
        let chunk_second = metastore
            .create_chunk(partition.get_id(), 4, None, None, true)
            .await
            .unwrap();

        metastore
            .chunk_uploaded(chunk_first.get_id())
            .await
            .unwrap();
        metastore
            .chunk_uploaded(chunk_second.get_id())
            .await
            .unwrap();

        chunk_store
            .add_memory_chunk(
                chunk_file_name(chunk_first.get_id(), chunk_first.get_row().suffix()),
                batch.clone(),
            )
            .await
            .unwrap();
        chunk_store
            .add_memory_chunk(
                chunk_file_name(chunk_second.get_id(), chunk_second.get_row().suffix()),
                batch2.clone(),
            )
            .await
            .unwrap();

        // act
        let compaction_service = CompactionServiceImpl::new(
            metastore.clone(),
            chunk_store.clone(),
            remote_fs,
            config.config_obj(),
            CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
        );
        compaction_service
            .compact_in_memory_chunks(partition.get_id())
            .await
            .unwrap();

        // assert
        let chunks = metastore
            .get_chunks_by_partition(partition.get_id(), false)
            .await
            .unwrap();

        let chunks_row_count = chunks
            .iter()
            .map(|c| c.get_row().get_row_count())
            .sum::<u64>();

        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0].get_row().min(),
            &Some(Row::new(vec![TableValue::String("Foo 0".to_string())]))
        );
        assert_eq!(
            chunks[0].get_row().max(),
            &Some(Row::new(vec![TableValue::String("Foo 6".to_string())]))
        );

        let mut data = Vec::new();
        for chunk in chunks.iter() {
            for b in chunk_store.get_chunk_columns(chunk.clone()).await.unwrap() {
                data.push(b)
            }
        }

        let batch = data[0].clone();
        assert_eq!(9, chunks_row_count);

        let rows = (0..9)
            .map(|i| Row::new(TableValue::from_columns(&batch.columns(), i)))
            .collect::<Vec<_>>();

        let expected = vec![
            Row::new(vec![TableValue::String("Foo 0".to_string())]),
            Row::new(vec![TableValue::String("Foo 1".to_string())]),
            Row::new(vec![TableValue::String("Foo 2".to_string())]),
            Row::new(vec![TableValue::String("Foo 3".to_string())]),
            Row::new(vec![TableValue::String("Foo 3".to_string())]),
            Row::new(vec![TableValue::String("Foo 4".to_string())]),
            Row::new(vec![TableValue::String("Foo 4".to_string())]),
            Row::new(vec![TableValue::String("Foo 5".to_string())]),
            Row::new(vec![TableValue::String("Foo 6".to_string())]),
        ];

        assert_eq!(expected, rows);

        RocksMetaStore::cleanup_test_metastore("compact_in_memory_chunks");
    }

    #[tokio::test]
    async fn aggr_index_compaction() {
        let config = Config::test("create_aggr_chunk_test").update_config(|mut c| {
            c.compaction_chunks_total_size_threshold = 50;
            c
        });
        let path = "/tmp/test_create_aggr_chunk";
        let chunk_store_path = path.to_string() + &"_store_chunk".to_string();
        let chunk_remote_store_path = path.to_string() + &"_remote_store_chunk".to_string();

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());

        let remote_fs = LocalDirRemoteFs::new(
            Some(PathBuf::from(chunk_remote_store_path.clone())),
            PathBuf::from(chunk_store_path.clone()),
        );
        let metastore = RocksMetaStore::new(
            Path::new(path),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
        )
        .unwrap();
        let chunk_store = ChunkStore::new(
            metastore.clone(),
            remote_fs.clone(),
            Arc::new(MockCluster::new()),
            config.config_obj(),
            CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
            50,
        );

        metastore
            .create_schema("foo".to_string(), false)
            .await
            .unwrap();

        let ind = IndexDef {
            name: "aggr".to_string(),
            columns: vec!["foo".to_string(), "boo".to_string()],
            multi_index: None,
            index_type: IndexType::Aggregate,
        };
        let cols = vec![
            Column::new("foo".to_string(), ColumnType::String, 0),
            Column::new("boo".to_string(), ColumnType::Int, 1),
            Column::new("sum_int".to_string(), ColumnType::Int, 2),
        ];
        let table = metastore
            .create_table(
                "foo".to_string(),
                "bar".to_string(),
                cols.clone(),
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

        let indices = metastore.get_table_indexes(table.get_id()).await.unwrap();

        let aggr_index = indices
            .iter()
            .find(|i| i.get_row().get_name() == "aggr")
            .unwrap();

        let partition = &metastore
            .get_active_partitions_by_index_id(aggr_index.get_id())
            .await
            .unwrap()[0];

        let data1: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                "a".to_string(),
                "a".to_string(),
                "b".to_string(),
                "b".to_string(),
                "c".to_string(),
            ])),
            Arc::new(Int64Array::from(vec![1, 10, 2, 20, 10])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ];
        let data2: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                "a".to_string(),
                "a".to_string(),
                "b".to_string(),
                "b".to_string(),
                "c".to_string(),
                "c".to_string(),
            ])),
            Arc::new(Int64Array::from(vec![1, 10, 2, 20, 10, 30])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
        ];

        let (chunk, _) = chunk_store
            .add_chunk_columns(aggr_index.clone(), partition.clone(), data1.clone(), false)
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        metastore.chunk_uploaded(chunk.get_id()).await.unwrap();

        let (chunk, _) = chunk_store
            .add_chunk_columns(aggr_index.clone(), partition.clone(), data2.clone(), false)
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        metastore.chunk_uploaded(chunk.get_id()).await.unwrap();

        let compaction_service = CompactionServiceImpl::new(
            metastore.clone(),
            chunk_store.clone(),
            remote_fs.clone(),
            config.config_obj(),
            CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
        );
        compaction_service
            .compact(partition.get_id(), DataLoadedSize::new())
            .await
            .unwrap();

        let partitions = metastore
            .get_active_partitions_by_index_id(aggr_index.get_id())
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        let partition = &partitions[0];
        assert_eq!(partition.get_row().main_table_row_count(), 6);

        let remote = partition
            .get_row()
            .get_full_name(partition.get_id())
            .unwrap();
        let local = remote_fs
            .download_file(remote.clone(), partition.get_row().file_size())
            .await
            .unwrap();

        let file_scan = FileScanConfig::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(arrow_schema(aggr_index.get_row())),
        )
        .with_file(PartitionedFile::from_path(local.to_string()).unwrap());
        let parquet_exec = ParquetExecBuilder::new(file_scan).build();

        let reader = Arc::new(parquet_exec);
        let res_data = &collect(reader, Arc::new(TaskContext::default()))
            .await
            .unwrap()[0];

        let foos = Arc::new(StringArray::from(vec![
            "a".to_string(),
            "a".to_string(),
            "b".to_string(),
            "b".to_string(),
            "c".to_string(),
            "c".to_string(),
        ]));
        let boos = Arc::new(Int64Array::from(vec![1, 10, 2, 20, 10, 30]));

        let sums = Arc::new(Int64Array::from(vec![11, 22, 33, 44, 55, 60]));
        let expected: Vec<ArrayRef> = vec![foos, boos, sums];

        assert_eq!(res_data.columns(), &expected);

        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(chunk_store_path.clone());
        let _ = fs::remove_dir_all(chunk_remote_store_path.clone());
    }

    #[tokio::test]
    async fn partition_compaction_int96() {
        Config::test("partition_compaction_int96")
            .update_config(|mut c| {
                c.partition_split_threshold = 20;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;
                let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();
                let compaction_service = services
                    .injector
                    .get_service_typed::<dyn CompactionService>()
                    .await;
                service
                    .exec_query("create table test.a (a int, b int96)")
                    .await
                    .unwrap();
                let values = (0..15)
                    .map(|i| format!("({}, {})", i, i))
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!("insert into test.a (a, b) values {}", values);
                service.exec_query(&query).await.unwrap();
                compaction_service
                    .compact(1, DataLoadedSize::new())
                    .await
                    .unwrap();
                let partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert_eq!(partitions.len(), 1);
                let values = (0..30)
                    .map(|i| format!("({}, {})", i, i))
                    .collect::<Vec<_>>()
                    .join(", ");

                let query = format!("insert into test.a (a, b) values {}", values);

                service.exec_query(&query).await.unwrap();
                compaction_service
                    .compact(partitions[0].get_id(), DataLoadedSize::new())
                    .await
                    .unwrap();
                let partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert_eq!(partitions.len(), 3);
            })
            .await;
    }

    #[tokio::test]
    async fn partition_compaction_decimal96() {
        Config::test("partition_compaction_decimal96")
            .update_config(|mut c| {
                c.partition_split_threshold = 20;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;
                let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();
                let compaction_service = services
                    .injector
                    .get_service_typed::<dyn CompactionService>()
                    .await;
                service
                    .exec_query("create table test.a (a int, d0 decimal(20,0), d1 decimal(20, 1), d2 decimal(20, 2), d3 decimal(20, 3), d4 decimal(20, 4), d5 decimal(20, 5), d10 decimal(20, 10))")
                    .await
                    .unwrap();
                let values = (0..15)
                    .map(|i| format!("({}, {}, {}, {}, {}, {}, {}, {})", i, i, i, i, i, i, i, i))
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!("insert into test.a (a, d0, d1, d2, d3, d4, d5, d10) values {}", values);
                service.exec_query(&query).await.unwrap();
                compaction_service
                    .compact(1, DataLoadedSize::new())
                    .await
                    .unwrap();
                let partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert_eq!(partitions.len(), 1);
                let values = (0..30)
                    .map(|i| format!("({}, {}, {}, {}, {}, {}, {}, {})", i, i, i, i, i, i, i, i))
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!("insert into test.a (a, d0, d1, d2, d3, d4, d5, d10) values {}", values);

                service.exec_query(&query).await.unwrap();
                compaction_service
                    .compact(partitions[0].get_id(), DataLoadedSize::new())
                    .await
                    .unwrap();
                let partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert_eq!(partitions.len(), 3);
            })
            .await;
    }

    #[tokio::test]
    async fn partition_split_by_file_size() {
        Config::test("partition_split_by_file_size")
            .update_config(|mut c| {
                c.partition_split_threshold = 2000;
                c.partition_size_split_threshold_bytes = 10000;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;
                let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();
                let compaction_service = services
                    .injector
                    .get_service_typed::<dyn CompactionService>()
                    .await;
                service
                    .exec_query("create table test.a (a varchar(255), b varchar(255))")
                    .await
                    .unwrap();
                let values = (0..1000)
                    .map(|i| format!("('{}{}', '{}{}')", i, "a".repeat(10), i, "b".repeat(10)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!("insert into test.a (a, b) values {}", values);
                service.exec_query(&query).await.unwrap();
                compaction_service
                    .compact(1, DataLoadedSize::new())
                    .await
                    .unwrap();
                let partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert_eq!(partitions.len(), 1);
                let values = (0..10)
                    .map(|_| format!("('{}', '{}')", "a".repeat(10), "b".repeat(10)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!("insert into test.a (a, b) values {}", values);

                service.exec_query(&query).await.unwrap();
                compaction_service
                    .compact(partitions[0].get_id(), DataLoadedSize::new())
                    .await
                    .unwrap();
                let partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert!(partitions.len() > 1);
                for p in partitions.iter() {
                    assert!(p.get_row().file_size().unwrap() <= 10000);
                }
            })
            .await;
    }
}

struct MultiSplit {
    meta: Arc<dyn MetaStore>,
    fs: Arc<dyn RemoteFs>,
    metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory>,
    keys: Vec<Row>,
    key_len: usize,
    multi_partition_id: u64,
    new_multi_parts: Vec<IdRow<MultiPartition>>,
    new_multi_rows: Vec<u64>,
    old_partitions: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
    new_partitions: Vec<IdRow<Partition>>,
    new_partition_rows: Vec<u64>,
    uploads: Vec<JoinHandle<Result<u64, CubeError>>>,
}

impl MultiSplit {
    fn new(
        meta: Arc<dyn MetaStore>,
        fs: Arc<dyn RemoteFs>,
        metadata_cache_factory: Arc<dyn CubestoreMetadataCacheFactory>,
        keys: Vec<Row>,
        key_len: usize,
        multi_partition_id: u64,
        new_multi_parts: Vec<IdRow<MultiPartition>>,
    ) -> MultiSplit {
        let new_multi_rows = vec![0; new_multi_parts.len()];
        MultiSplit {
            meta,
            fs,
            metadata_cache_factory,
            keys,
            key_len,
            multi_partition_id,
            new_multi_parts,
            new_multi_rows,
            old_partitions: Vec::new(),
            new_partitions: Vec::new(),
            new_partition_rows: Vec::new(),
            uploads: Vec::new(),
        }
    }

    async fn split_single_partition(&mut self, p: PartitionData) -> Result<(), CubeError> {
        let mchildren = &self.new_multi_parts;
        let mrow_counts = &mut self.new_multi_rows;
        let old_partitions = &mut self.old_partitions;
        let new_partitions = &mut self.new_partitions;
        let new_partition_rows = &mut self.new_partition_rows;
        let uploads = &mut self.uploads;

        let mut children = Vec::with_capacity(mchildren.len());
        for mc in mchildren.iter() {
            let c = Partition::new_child(&p.partition, Some(mc.get_id()));
            let c = c.update_min_max_and_row_count(
                mc.get_row().min_row().cloned(),
                mc.get_row().max_row().cloned(),
                0,
                None,
                None,
            );
            children.push(self.meta.create_partition(c).await?)
        }

        let mut in_files = Vec::new();
        collect_remote_files(&p, &mut in_files);
        for (f, _) in &mut in_files {
            *f = self.fs.local_file(f.clone()).await?;
        }

        let mut out_files = Vec::with_capacity(children.len());
        let mut out_remote_paths = Vec::with_capacity(children.len());
        for c in &children {
            let remote_path = partition_file_name(c.get_id(), c.get_row().suffix());
            out_files.push(self.fs.temp_upload_path(remote_path.clone()).await?);
            out_remote_paths.push(remote_path);
        }

        let out_files = scopeguard::guard(out_files, |files| {
            for f in files {
                ensure_temp_file_is_dropped(f);
            }
        });

        let table = self
            .meta
            .get_table_by_id(p.index.get_row().table_id())
            .await?;
        let store = ParquetTableStore::new(
            p.index.get_row().clone(),
            ROW_GROUP_SIZE,
            self.metadata_cache_factory.clone(),
        );
        let records = if !in_files.is_empty() {
            read_files(
                &in_files.into_iter().map(|(f, _)| f).collect::<Vec<_>>(),
                self.metadata_cache_factory.cache_factory().as_ref(),
                self.key_len,
                None,
                Arc::new(store.arrow_schema()),
            )
            .await?
            .execute(0, Arc::new(TaskContext::default()))?
        } else {
            EmptyExec::new(Arc::new(store.arrow_schema()))
                .execute(0, Arc::new(TaskContext::default()))?
        };
        let row_counts = write_to_files_by_keys(
            records,
            store,
            &table,
            out_files.to_vec(),
            self.keys.clone(),
        )
        .await?;

        for i in 0..row_counts.len() {
            mrow_counts[i] += row_counts[i] as u64;
        }
        old_partitions.push((p.partition, p.chunks));
        assert_eq!(children.len(), row_counts.len());
        new_partitions.extend(children);
        new_partition_rows.extend(row_counts.iter().map(|n| *n as u64));
        for i in 0..row_counts.len() {
            if row_counts[i] == 0 {
                continue;
            }
            let fs = self.fs.clone();
            let local_path = out_files[i].to_string();
            let remote_path = out_files[i].to_string();
            uploads.push(cube_ext::spawn(async move {
                fs.upload_file(local_path, remote_path).await
            }));
        }
        Ok(())
    }

    async fn finish(self, initial_split: bool) -> Result<(), CubeError> {
        let mut upload_res = Vec::new();
        for u in self.uploads {
            upload_res.push(u.await??);
        }

        let mids = self
            .new_multi_parts
            .into_iter()
            .map(|p| p.get_id())
            .collect_vec();
        self.meta
            .commit_multi_partition_split(
                self.multi_partition_id,
                mids,
                self.new_multi_rows,
                self.old_partitions,
                self.new_partitions
                    .clone()
                    .into_iter()
                    .zip_eq(upload_res.into_iter())
                    .collect(),
                self.new_partition_rows,
                initial_split,
            )
            .await
    }
}
