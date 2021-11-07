use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::partition::partition_file_name;
use crate::metastore::{Chunk, IdRow, MetaStore, Partition, PartitionData};
use crate::remotefs::RemoteFs;
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
use datafusion::cube_ext;
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
use num::Integer;
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
        let (partition, index, multi_part) = self
            .meta_store
            .get_partition_for_compaction(partition_id)
            .await?;
        if let Some(mp) = &multi_part {
            if mp.get_row().prepared_for_split() {
                log::debug!(
                    "Cancelled compaction of {}. It runs concurrently with multi-split",
                    partition_id
                );
                return Ok(());
            }
        }
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
        let partition_id = partition.get_id();
        let chunks_row_count = chunks
            .iter()
            .map(|c| c.get_row().get_row_count())
            .sum::<u64>();
        // For multi-partitions, we only compact chunks and never change the main table.
        // And we never split, multi-partitions have a different process for that.
        let new_chunk = match &multi_part {
            None => None,
            Some(_) => Some(
                self.meta_store
                    .create_chunk(partition_id, chunks_row_count as usize, false)
                    .await?,
            ),
        };
        let mut total_rows = chunks_row_count;
        if new_chunk.is_none() {
            total_rows += partition.get_row().main_table_row_count();
        }
        let mut new_partitions = Vec::new();
        if new_chunk.is_none() {
            let new_partitions_count =
                div_ceil(total_rows, self.config.partition_split_threshold()) as usize;
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
            Some(self.remote_fs.download_file(&f).await?)
        } else {
            None
        };
        let mut new_local_files = Vec::new();
        if let Some(c) = &new_chunk {
            let remote = ChunkStore::chunk_remote_path(c.get_id());
            new_local_files.push(self.remote_fs.temp_upload_path(&remote).await?);
        } else {
            for p in new_partitions.iter() {
                let new_remote_path = partition_file_name(p.get_id());
                new_local_files.push(self.remote_fs.temp_upload_path(&new_remote_path).await?);
            }
        }

        let new_local_files2 = new_local_files.clone();
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
        let records = merge_chunks(key_size, main_table, new, unique_key).await?;
        let count_and_min =
            write_to_files(records, total_rows as usize, store, new_local_files2).await?;

        if let Some(c) = &new_chunk {
            assert_eq!(new_local_files.len(), 1);
            let remote = ChunkStore::chunk_remote_path(c.get_id());
            self.remote_fs
                .upload_file(&new_local_files[0], &remote)
                .await?;
            let chunk_ids = chunks.iter().map(|c| c.get_id()).collect_vec();
            let swapped = self
                .meta_store
                .swap_compacted_chunks(partition_id, chunk_ids, c.get_id())
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
                    let new_remote_path = partition_file_name(p.get_id());
                    self.remote_fs
                        .upload_file(&new_local_files[i], new_remote_path.as_str())
                        .await?;
                    filtered_partitions.push(p);
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
        let files = download_files(&partitions, self.remote_fs.clone()).await?;
        let keys = find_partition_keys(
            keys_with_counts(&files, key_len).await?,
            key_len,
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

fn collect_remote_files(p: &PartitionData, out: &mut Vec<String>) {
    if p.partition.get_row().is_active() {
        if let Some(f) = p.partition.get_row().get_full_name(p.partition.get_id()) {
            out.push(f)
        }
    }
    for c in &p.chunks {
        out.push(ChunkStore::chunk_remote_path(c.get_id()));
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
            let f = take(f);
            let fs = fs.clone();
            tasks.push(cube_ext::spawn(async move { fs.download_file(&f).await }))
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
    let rows_per_file = (num_rows as usize).div_ceil(&files.len());
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
    uploads: Vec<JoinHandle<Result<(), CubeError>>>,
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
        for f in &mut in_files {
            *f = self.fs.local_file(f).await?;
        }

        let mut out_files = Vec::with_capacity(children.len());
        let mut out_remote_paths = Vec::with_capacity(children.len());
        for c in &children {
            let remote_path = partition_file_name(c.get_id());
            out_files.push(self.fs.temp_upload_path(&remote_path).await?);
            out_remote_paths.push(remote_path);
        }

        let store = ParquetTableStore::new(p.index.get_row().clone(), ROW_GROUP_SIZE);
        let records = if !in_files.is_empty() {
            read_files(&in_files, self.key_len, None)
                .await?
                .execute(0)
                .await?
        } else {
            EmptyExec::new(false, Arc::new(store.arrow_schema()))
                .execute(0)
                .await?
        };
        let row_counts =
            write_to_files_by_keys(records, store, out_files.clone(), self.keys.clone()).await?;

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
            let local_path = take(&mut out_files[i]);
            let remote_path = take(&mut out_remote_paths[i]);
            uploads.push(cube_ext::spawn(async move {
                fs.upload_file(&local_path, &remote_path).await
            }));
        }
        Ok(())
    }

    async fn finish(self, initial_split: bool) -> Result<(), CubeError> {
        for u in self.uploads {
            u.await??;
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
                self.new_partitions,
                self.new_partition_rows,
                initial_split,
            )
            .await
    }
}
