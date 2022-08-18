use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::partition::partition_file_name;
use crate::metastore::table::AggregateColumn;
use crate::metastore::{
    deactivate_table_on_corrupt_data, Chunk, IdRow, IndexType, MetaStore, Partition, PartitionData,
};
use crate::remotefs::{ensure_temp_file_is_dropped, RemoteFs};
use crate::store::{ChunkDataStore, ChunkStore, ROW_GROUP_SIZE};
use crate::table::data::{cmp_min_rows, cmp_partition_key};
use crate::table::parquet::{arrow_schema, ParquetTableStore};
use crate::table::redistribute::redistribute;
use crate::table::{Row, TableValue};
use crate::CubeError;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::cube_ext;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{Column, Count, Literal};
use datafusion::physical_plan::hash_aggregate::{
    AggregateMode, AggregateStrategy, HashAggregateExec,
};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::merge_sort::{LastRowByUniqueKeyExec, MergeSortExec};
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    AggregateExpr, ExecutionPlan, PhysicalExpr, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use itertools::{EitherOrBoth, Itertools};
use num::integer::div_ceil;
use parquet::arrow::ArrowWriter;
use std::cmp::Ordering;
use std::fs::File;
use std::mem::take;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

#[async_trait]
pub trait CompactionService: DIService + Send + Sync {
    async fn compact(&self, partition_id: u64) -> Result<(), CubeError>;
    async fn compact_in_memory_chunks(&self, partition_id: u64) -> Result<(), CubeError>;
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
            .filter(|c| {
                !c.get_row().in_memory()
                    || c.get_row().get_row_count()
                        > self.config.compaction_in_memory_chunks_size_limit()
                    || c.get_row()
                        .oldest_insert_at()
                        .map(|m| {
                            Utc::now().signed_duration_since(m).num_seconds()
                                > self
                                    .config
                                    .compaction_in_memory_chunks_max_lifetime_threshold()
                                    as i64
                        })
                        .unwrap_or(false)
            })
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
                Some(
                    self.meta_store
                        .create_chunk(partition_id, chunks_row_count as usize, false)
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
            let new_partitions_count = (div_ceil(
                pending_rows,
                table
                    .get_row()
                    .partition_split_threshold_or_default(self.config.partition_split_threshold()),
            ) as usize)
                // Do not allow to much of new partitions to limit partition accuracy trade off
                // TODO config
                .min(16);
            for _ in 0..new_partitions_count {
                new_partitions.push(
                    self.meta_store
                        .create_partition(Partition::new_child(&partition, None))
                        .await?,
                );
            }
        }

        let mut data = Vec::new();
        let num_columns = index.get_row().columns().len();
        for chunk in chunks.iter() {
            for b in self.chunk_store.get_chunk_columns(chunk.clone()).await? {
                assert_eq!(
                    num_columns,
                    b.num_columns(),
                    "Column len mismatch for {:?} and {:?}",
                    index,
                    chunk
                );
                data.push(b)
            }
        }

        let store = ParquetTableStore::new(index.get_row().clone(), ROW_GROUP_SIZE);
        let old_partition_remote = match &new_chunk {
            Some(_) => None,
            None => partition.get_row().get_full_name(partition.get_id()),
        };
        let old_partition_local = if let Some(f) = old_partition_remote {
            let result = self
                .remote_fs
                .download_file(&f, partition.get_row().file_size())
                .await;
            deactivate_table_on_corrupt_data(self.meta_store.clone(), &result, &partition).await;
            Some(result?)
        } else {
            None
        };
        let mut new_local_files = Vec::new();
        if let Some(c) = &new_chunk {
            let remote = ChunkStore::chunk_remote_path(c.get_id(), c.get_row().suffix());
            new_local_files.push(self.remote_fs.temp_upload_path(&remote).await?);
        } else {
            for p in new_partitions.iter() {
                let new_remote_path = partition_file_name(p.get_id(), p.get_row().suffix());
                new_local_files.push(self.remote_fs.temp_upload_path(&new_remote_path).await?);
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
        let unique_key = table.get_row().unique_key_columns();
        let aggregate_columns = match index.get_row().get_type() {
            IndexType::Regular => None,
            IndexType::Aggregate => Some(table.get_row().aggregate_columns()),
        };
        let records =
            merge_chunks(key_size, main_table, new, unique_key, aggregate_columns).await?;
        let count_and_min =
            write_to_files(records, total_rows as usize, store, new_local_files2).await?;

        if let Some(c) = &new_chunk {
            assert_eq!(new_local_files.len(), 1);
            let remote = ChunkStore::chunk_remote_path(c.get_id(), c.get_row().suffix());
            let file_size = self
                .remote_fs
                .upload_file(&new_local_files[0], &remote)
                .await?;
            let chunk_ids = chunks.iter().map(|c| c.get_id()).collect_vec();
            let swapped = self
                .meta_store
                .swap_compacted_chunks(partition_id, chunk_ids, c.get_id(), file_size)
                .await?;
            if !swapped {
                log::debug!(
                    "Cancelled compaction of {}. It runs concurrently with multi-split",
                    partition_id
                );
                self.remote_fs.delete_file(&remote).await?;
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
                        .upload_file(&new_local_files[i], new_remote_path.as_str())
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
                            EitherOrBoth::Both((c, min), (_, next_min)) => {
                                if i == 0 && partition_min.is_none() {
                                    Ok((*c as u64, (None, Some(Row::new(next_min.clone())))))
                                } else if i < num_filtered - 1 {
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
                                        num_filtered, i, item
                                    )))
                                }
                            }
                            EitherOrBoth::Left((c, min)) => {
                                if i == 0 && num_filtered == 1 {
                                    Ok((*c as u64, (partition_min.clone(), partition_max.clone())))
                                } else if i == num_filtered - 1 {
                                    Ok((
                                        *c as u64,
                                        (Some(Row::new(min.clone())), partition_max.clone()),
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

    async fn compact_in_memory_chunks(&self, partition_id: u64) -> Result<(), CubeError> {
        let (partition, index, table, multi_part) = self
            .meta_store
            .get_partition_for_compaction(partition_id)
            .await?;

        // Test invariants
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

        let compaction_in_memory_chunks_size_limit =
            self.config.compaction_in_memory_chunks_size_limit();

        let mut size = 0;
        let mut count = 0;
        // Get all in_memory and active chunks
        let chunks = self
            .meta_store
            .get_chunks_by_partition(partition_id, false)
            .await?
            .into_iter()
            .filter(|c| {
                c.get_row().in_memory()
                    && c.get_row().active()
                    && c.get_row().get_row_count() <= compaction_in_memory_chunks_size_limit
                    && c.get_row()
                        .oldest_insert_at()
                        .map(|m| {
                            Utc::now().signed_duration_since(m).num_seconds()
                                <= self
                                    .config
                                    .compaction_in_memory_chunks_max_lifetime_threshold()
                                    as i64
                        })
                        .unwrap_or(true)
            })
            .take_while(|c| {
                if count < 2 {
                    size += c.get_row().get_row_count();
                    count += 1;
                    true
                } else {
                    size += c.get_row().get_row_count();
                    count += 1;
                    size <= self.config.compaction_in_memory_chunks_total_size_limit()
                }
            })
            .collect::<Vec<_>>();
        if count < 2 {
            return Ok(()); //We don't need to compact single chunk
        }
        // Prepare merge params
        let unique_key = table.get_row().unique_key_columns();
        let num_columns = index.get_row().columns().len();
        let key_size = index.get_row().sort_key_size() as usize;
        let schema = Arc::new(arrow_schema(index.get_row()));
        // Use empty execution plan for main_table, read only from memory chunks
        let main_table: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(false, schema.clone()));
        let in_memory_columns =
            prepare_in_memory_columns(&self.chunk_store, num_columns, key_size, &chunks).await?;

        let aggregate_columns = match index.get_row().get_type() {
            IndexType::Regular => None,
            IndexType::Aggregate => Some(table.get_row().aggregate_columns()),
        };

        // Get merged RecordBatch
        let batches_stream = merge_chunks(
            key_size,
            main_table,
            in_memory_columns,
            unique_key,
            aggregate_columns,
        )
        .await?;
        let batches = collect(batches_stream).await?;
        let batch = RecordBatch::concat(&schema, &batches).unwrap();

        // Create chunk, writer RecordBatch into memory, swap chunks
        let old_chunks_ids: Vec<u64> = chunks.iter().map(|c| c.get_id()).collect::<Vec<u64>>();
        let old_chunks_size: u64 = chunks
            .iter()
            .map(|c| c.get_row().get_row_count())
            .sum::<u64>();
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
        let new_chunk = self
            .meta_store
            .create_chunk(partition_id, old_chunks_size as usize, true)
            .await?;

        // oldest_insert_at will be used to force compaction
        let chunk = IdRow::new(
            new_chunk.get_id(),
            new_chunk.get_row().set_oldest_insert_at(oldest_insert_at),
        );

        self.chunk_store
            .add_memory_chunk(chunk.get_id(), batch)
            .await?;
        self.meta_store
            .swap_chunks(
                old_chunks_ids,
                vec![(chunk.get_id(), Some(chunk.get_row().get_row_count()))],
            )
            .await?;

        Ok(())
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
            keys_with_counts(&files, key_len).await?,
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
            keys,
            key_len,
            multi_partition_id,
            children,
        );
        s.split_single_partition(data).await?;
        s.finish(false).await
    }
}

// TODO: re-use it in the compact function?
async fn prepare_in_memory_columns(
    chunk_store: &Arc<dyn ChunkDataStore>,
    num_columns: usize,
    key_size: usize,
    chunks: &Vec<IdRow<Chunk>>,
) -> Result<Vec<ArrayRef>, CubeError> {
    let mut data: Vec<RecordBatch> = Vec::new();
    for chunk in chunks.iter() {
        for b in chunk_store.get_chunk_columns(chunk.clone()).await? {
            data.push(b)
        }
    }

    let new = cube_ext::spawn_blocking(move || -> Result<_, CubeError> {
        // Concat rows from all chunks.
        let mut columns = Vec::with_capacity(num_columns);
        for i in 0..num_columns {
            let v =
                arrow::compute::concat(&data.iter().map(|a| a.column(i).as_ref()).collect_vec())?;
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
        Ok(new)
    })
    .await??;

    Ok(new)
}

/// Compute keys that partitions must be split by.
async fn find_partition_keys(
    p: HashAggregateExec,
    key_len: usize,
    rows_per_partition: usize,
) -> Result<Vec<Row>, CubeError> {
    let mut s = p.execute(0).await?;
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
    key_len: usize,
    projection: Option<Vec<usize>>,
) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
    assert!(!files.is_empty());
    let mut inputs = Vec::<Arc<dyn ExecutionPlan>>::with_capacity(files.len());
    for f in files {
        inputs.push(Arc::new(ParquetExec::try_from_files(
            &[f.as_str()],
            projection.clone(),
            None,
            ROW_GROUP_SIZE,
            1,
            None,
        )?));
    }
    let plan = Arc::new(UnionExec::new(inputs));
    let fields = plan.schema();
    let fields = fields.fields();
    let mut columns = Vec::with_capacity(fields.len());
    for i in 0..key_len {
        columns.push(Column::new(fields[i].name().as_str(), i));
    }
    Ok(Arc::new(MergeSortExec::try_new(plan, columns.clone())?))
}

/// The returned execution plan computes all keys in sorted order and the count of rows that have
/// this key in the input files.
async fn keys_with_counts(
    files: &[String],
    key_len: usize,
) -> Result<HashAggregateExec, CubeError> {
    let projection = (0..key_len).collect_vec();
    let plan = read_files(files, key_len, Some(projection.clone())).await?;

    let fields = plan.schema();
    let fields = fields.fields();
    let mut key = Vec::<(Arc<dyn PhysicalExpr>, String)>::with_capacity(key_len);
    for i in 0..key_len {
        let name = fields[i].name().clone();
        let col = Column::new(fields[i].name().as_str(), i);
        key.push((Arc::new(col), name));
    }
    let agg: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Count::new(
        Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
        "#mi_row_count",
        DataType::UInt64,
    ))];
    let plan_schema = plan.schema();
    let plan = HashAggregateExec::try_new(
        AggregateStrategy::InplaceSorted,
        Some(projection),
        AggregateMode::Full,
        key,
        agg,
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
                async move { fs.download_file(&f, size).await },
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
    files: Vec<String>,
) -> Result<Vec<(usize, Vec<TableValue>)>, CubeError> {
    let rows_per_file = div_ceil(num_rows as usize, files.len());
    let key_size = store.key_size() as usize;
    let partition_split_key_size = store.partition_split_key_size() as usize;

    let mut last_row = Vec::new();
    // (num_rows, first_row) for all processed writers.
    let stats = Arc::new(Mutex::new(vec![(0, Vec::new())]));
    let stats_ref = stats.clone();

    let pick_writer = |b: &RecordBatch| -> WriteBatchTo {
        let stats_ref = stats_ref.clone();
        let mut stats = stats_ref.lock().unwrap();

        let (num_rows, first_row) = stats.last_mut().unwrap();
        if first_row.is_empty() {
            *first_row = TableValue::from_columns(&b.columns()[0..key_size], 0);
        }
        if *num_rows + b.num_rows() < rows_per_file {
            *num_rows += b.num_rows();
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
        if i == b.num_rows() {
            *num_rows += b.num_rows();
            return WriteBatchTo::Current;
        }

        *num_rows += i;
        stats.push((0, Vec::new()));
        last_row.clear();
        return WriteBatchTo::Next {
            rows_for_current: i,
        };
    };

    write_to_files_impl(records, store, files, pick_writer).await?;

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
    mut pick_writer: impl FnMut(&RecordBatch) -> WriteBatchTo,
) -> Result<(), CubeError> {
    let schema = Arc::new(store.arrow_schema());
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
    write_to_files_impl(records, store, files, pick_writer).await?;

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
async fn merge_chunks(
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
        key.push(Column::new(f.name().as_str(), i));
    }

    let inputs = UnionExec::new(vec![
        l,
        Arc::new(MemoryExec::try_new(&[vec![r]], schema, None)?),
    ]);
    let mut res: Arc<dyn ExecutionPlan> = Arc::new(MergeSortExec::try_new(Arc::new(inputs), key)?);

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

        let output_sort_order = (0..key_size).map(|x| x as usize).collect();

        res = Arc::new(HashAggregateExec::try_new(
            AggregateStrategy::InplaceSorted,
            Some(output_sort_order),
            AggregateMode::Final,
            groups,
            aggregates,
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

    Ok(res.execute(0).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::MockCluster;
    use crate::config::Config;
    use crate::config::MockConfigObj;
    use crate::metastore::{Column, ColumnType, IndexDef, IndexType, RocksMetaStore};
    use crate::remotefs::LocalDirRemoteFs;
    use crate::store::MockChunkDataStore;
    use crate::table::data::rows_to_columns;
    use crate::table::{cmp_same_types, Row, TableValue};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::collect;
    use rocksdb::{Options, DB};
    use std::fs;
    use std::path::PathBuf;

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
            .create_chunk(next_partition_id, 2, false)
            .await
            .unwrap();
        metastore.chunk_uploaded(4).await.unwrap();

        compaction_service.compact(next_partition_id).await.unwrap();

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
            )
            .await
            .unwrap();
        metastore.get_default_index(1).await.unwrap();
        let partition = metastore.get_partition(1).await.unwrap();

        let rows = (0..5)
            .map(|i| Row::new(vec![TableValue::String(format!("Foo {}", 4 - i))]))
            .collect::<Vec<_>>();
        let data = rows_to_columns(&cols, &rows);
        let index = metastore
            .get_index(partition.get_row().get_index_id())
            .await
            .unwrap();
        let schema = Arc::new(arrow_schema(index.get_row()));
        let batch = RecordBatch::try_new(schema.clone(), data).unwrap();
        let chunk_first = metastore
            .create_chunk(partition.get_id(), 5, true)
            .await
            .unwrap();
        let chunk_second = metastore
            .create_chunk(partition.get_id(), 5, true)
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
            .add_memory_chunk(chunk_first.get_id(), batch.clone())
            .await
            .unwrap();
        chunk_store
            .add_memory_chunk(chunk_second.get_id(), batch.clone())
            .await
            .unwrap();

        // act
        let compaction_service = CompactionServiceImpl::new(
            metastore.clone(),
            chunk_store.clone(),
            remote_fs,
            config.config_obj(),
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

        let mut data = Vec::new();
        for chunk in chunks.iter() {
            for b in chunk_store.get_chunk_columns(chunk.clone()).await.unwrap() {
                data.push(b)
            }
        }

        let batch = data[0].clone();

        let rows = (0..10)
            .map(|i| Row::new(TableValue::from_columns(&batch.columns().clone(), i)))
            .collect::<Vec<_>>();

        let expected = vec![
            Row::new(vec![TableValue::String("Foo 0".to_string())]),
            Row::new(vec![TableValue::String("Foo 0".to_string())]),
            Row::new(vec![TableValue::String("Foo 1".to_string())]),
            Row::new(vec![TableValue::String("Foo 1".to_string())]),
            Row::new(vec![TableValue::String("Foo 2".to_string())]),
            Row::new(vec![TableValue::String("Foo 2".to_string())]),
            Row::new(vec![TableValue::String("Foo 3".to_string())]),
            Row::new(vec![TableValue::String("Foo 3".to_string())]),
            Row::new(vec![TableValue::String("Foo 4".to_string())]),
            Row::new(vec![TableValue::String("Foo 4".to_string())]),
        ];

        assert_eq!(1, chunks.len());
        assert_eq!(10, chunks_row_count);
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
        let metastore = RocksMetaStore::new(path, remote_fs.clone(), config.config_obj());
        let chunk_store = ChunkStore::new(
            metastore.clone(),
            remote_fs.clone(),
            Arc::new(MockCluster::new()),
            config.config_obj(),
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
                Some(vec![("sum".to_string(), "sum_int".to_string())]),
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
        );
        compaction_service
            .compact(partition.get_id())
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
            .download_file(&remote, partition.get_row().file_size())
            .await
            .unwrap();
        let reader = Arc::new(
            ParquetExec::try_from_path(local.as_str(), None, None, ROW_GROUP_SIZE, 1, None)
                .unwrap(),
        );
        let res_data = &collect(reader).await.unwrap()[0];

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
}

struct MultiSplit {
    meta: Arc<dyn MetaStore>,
    fs: Arc<dyn RemoteFs>,
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
        keys: Vec<Row>,
        key_len: usize,
        multi_partition_id: u64,
        new_multi_parts: Vec<IdRow<MultiPartition>>,
    ) -> MultiSplit {
        let new_multi_rows = vec![0; new_multi_parts.len()];
        MultiSplit {
            meta,
            fs,
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
            );
            children.push(self.meta.create_partition(c).await?)
        }

        let mut in_files = Vec::new();
        collect_remote_files(&p, &mut in_files);
        for (f, _) in &mut in_files {
            *f = self.fs.local_file(f).await?;
        }

        let mut out_files = Vec::with_capacity(children.len());
        let mut out_remote_paths = Vec::with_capacity(children.len());
        for c in &children {
            let remote_path = partition_file_name(c.get_id(), c.get_row().suffix());
            out_files.push(self.fs.temp_upload_path(&remote_path).await?);
            out_remote_paths.push(remote_path);
        }

        let out_files = scopeguard::guard(out_files, |files| {
            for f in files {
                ensure_temp_file_is_dropped(f);
            }
        });

        let store = ParquetTableStore::new(p.index.get_row().clone(), ROW_GROUP_SIZE);
        let records = if !in_files.is_empty() {
            read_files(
                &in_files.into_iter().map(|(f, _)| f).collect::<Vec<_>>(),
                self.key_len,
                None,
            )
            .await?
            .execute(0)
            .await?
        } else {
            EmptyExec::new(false, Arc::new(store.arrow_schema()))
                .execute(0)
                .await?
        };
        let row_counts =
            write_to_files_by_keys(records, store, out_files.to_vec(), self.keys.clone()).await?;

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
                fs.upload_file(&local_path, &remote_path).await
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
