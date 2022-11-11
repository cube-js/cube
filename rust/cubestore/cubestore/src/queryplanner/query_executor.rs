use crate::cluster::{pick_worker_by_ids, pick_worker_by_partitions, Cluster};
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::table::Table;
use crate::metastore::{Column, ColumnType, IdRow, Index, Partition};
use crate::queryplanner::filter_by_key_range::FilterByKeyRangeExec;
use crate::queryplanner::optimizations::CubeQueryPlanner;
use crate::queryplanner::planning::{get_worker_plan, Snapshot, Snapshots};
use crate::queryplanner::pretty_printers::{pp_phys_plan, pp_plan};
use crate::queryplanner::serialized_plan::{IndexSnapshot, RowFilter, RowRange, SerializedPlan};
use crate::store::DataFrame;
use crate::table::data::rows_to_columns;
use crate::table::parquet::CubestoreParquetMetadataCache;
use crate::table::{Row, TableValue, TimestampValue};
use crate::{app_metrics, CubeError};
use arrow::array::{
    make_array, Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array,
    Int64Decimal0Array, Int64Decimal10Array, Int64Decimal1Array, Int64Decimal2Array,
    Int64Decimal3Array, Int64Decimal4Array, Int64Decimal5Array, MutableArrayData, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::MemStreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use core::fmt;
use datafusion::datasource::datasource::{Statistics, TableProviderFilterPushDown};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::logical_plan;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::merge_sort::{LastRowByUniqueKeyExec, MergeSortExec};
use datafusion::physical_plan::parquet::{
    NoopParquetMetadataCache, ParquetExec, ParquetMetadataCache,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    collect, ExecutionPlan, OptimizerHints, Partitioning, PhysicalExpr, SendableRecordBatchStream,
};
use itertools::Itertools;
use log::{debug, error, trace, warn};
use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::mem::take;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{instrument, Instrument};

#[automock]
#[async_trait]
pub trait QueryExecutor: DIService + Send + Sync {
    async fn execute_router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError>;

    async fn execute_worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError>;

    async fn router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError>;

    async fn worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError>;

    async fn pp_worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<String, CubeError>;
}

crate::di_service!(MockQueryExecutor, [QueryExecutor]);

pub struct QueryExecutorImpl {
    parquet_metadata_cache: Arc<dyn CubestoreParquetMetadataCache>,
}

crate::di_service!(QueryExecutorImpl, [QueryExecutor]);

#[async_trait]
impl QueryExecutor for QueryExecutorImpl {
    #[instrument(level = "trace", skip(self, plan, cluster))]
    async fn execute_router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError> {
        let collect_span = tracing::span!(tracing::Level::TRACE, "collect_physical_plan");
        let (physical_plan, logical_plan) = self.router_plan(plan, cluster).await?;
        let split_plan = physical_plan;

        trace!(
            "Router Query Physical Plan: {}",
            pp_phys_plan(split_plan.as_ref())
        );


        let execution_time = SystemTime::now();

        let results = collect(split_plan.clone()).instrument(collect_span).await;
        let execution_time = execution_time.elapsed()?;
        debug!("Query data processing time: {:?}", execution_time,);
        app_metrics::DATA_QUERY_TIME_MS.report(execution_time.as_millis() as i64);
        if execution_time.as_millis() > 200 {
            warn!(
                "Slow Query ({:?}):\n{}",
                execution_time,
                pp_plan(&logical_plan)
            );
            debug!(
                "Slow Query Physical Plan ({:?}): {}",
                execution_time,
                pp_phys_plan(split_plan.as_ref())
            );
        }
        if results.is_err() {
            error!(
                "Error Query ({:?}):\n{}",
                execution_time,
                pp_plan(&logical_plan)
            );
            error!(
                "Error Query Physical Plan ({:?}): {}",
                execution_time,
                pp_phys_plan(split_plan.as_ref())
            );
        }
        Ok((split_plan.schema(), results?))
    }

    #[instrument(level = "trace", skip(self, plan, remote_to_local_names))]
    async fn execute_worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError> {
        let (physical_plan, logical_plan) = self
            .worker_plan(plan, remote_to_local_names, chunk_id_to_record_batches)
            .await?;
        let worker_plan;
        let max_batch_rows;
        if let Some((p, s)) = get_worker_plan(&physical_plan) {
            worker_plan = p;
            max_batch_rows = s;
        } else {
            error!("No worker marker in physical plan: {:?}", physical_plan);
            return Err(CubeError::internal(
                "Invalid physical plan on worker".to_string(),
            ));
        }

        trace!(
            "Partition Query Physical Plan: {}",
            pp_phys_plan(worker_plan.as_ref())
        );

        let execution_time = SystemTime::now();
        let results = collect(worker_plan.clone())
            .instrument(tracing::span!(
                tracing::Level::TRACE,
                "collect_physical_plan"
            ))
            .await;
        debug!(
            "Partition Query data processing time: {:?}",
            execution_time.elapsed()?
        );
        if execution_time.elapsed()?.as_millis() > 200 || results.is_err() {
            warn!(
                "Slow Partition Query ({:?}):\n{}",
                execution_time.elapsed()?,
                pp_plan(&logical_plan)
            );
            debug!(
                "Slow Partition Query Physical Plan ({:?}): {}",
                execution_time.elapsed()?,
                pp_phys_plan(worker_plan.as_ref())
            );
        }
        if results.is_err() {
            error!(
                "Error Partition Query ({:?}):\n{}",
                execution_time.elapsed()?,
                pp_plan(&logical_plan)
            );
            error!(
                "Error Partition Query Physical Plan ({:?}): {}",
                execution_time.elapsed()?,
                pp_phys_plan(worker_plan.as_ref())
            );
        }
        // TODO: stream results as they become available.
        let results = regroup_batches(results?, max_batch_rows)?;
        Ok((worker_plan.schema(), results))
    }

    async fn router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError> {
        let plan_to_move = plan.logical_plan(
            HashMap::new(),
            HashMap::new(),
            NoopParquetMetadataCache::new(),
        )?;
        let serialized_plan = Arc::new(plan);
        let ctx = self.router_context(cluster.clone(), serialized_plan.clone())?;
        Ok((
            ctx.clone().create_physical_plan(&plan_to_move.clone())?,
            plan_to_move,
        ))
    }

    async fn worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError> {
        let plan_to_move = plan.logical_plan(
            remote_to_local_names,
            chunk_id_to_record_batches,
            self.parquet_metadata_cache.cache().clone(),
        )?;
        let plan = Arc::new(plan);
        let ctx = self.worker_context(plan.clone())?;
        let plan_ctx = ctx.clone();
        Ok((
            plan_ctx.create_physical_plan(&plan_to_move.clone())?,
            plan_to_move,
        ))
    }

    async fn pp_worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<String, CubeError> {
        let (physical_plan, _) = self
            .worker_plan(plan, remote_to_local_names, chunk_id_to_record_batches)
            .await?;

        let worker_plan;
        if let Some((p, _)) = get_worker_plan(&physical_plan) {
            worker_plan = p;
        } else {
            error!("No worker marker in physical plan: {:?}", physical_plan);
            return Err(CubeError::internal(
                "Invalid physical plan on worker".to_string(),
            ));
        }

        Ok(pp_phys_plan(worker_plan.as_ref()))
    }
}

impl QueryExecutorImpl {
    pub fn new(parquet_metadata_cache: Arc<dyn CubestoreParquetMetadataCache>) -> Arc<Self> {
        Arc::new(QueryExecutorImpl {
            parquet_metadata_cache,
        })
    }

    fn router_context(
        &self,
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<SerializedPlan>,
    ) -> Result<Arc<ExecutionContext>, CubeError> {
        Ok(Arc::new(ExecutionContext::with_config(
            ExecutionConfig::new()
                .with_batch_size(4096)
                .with_concurrency(1)
                .with_query_planner(Arc::new(CubeQueryPlanner::new_on_router(
                    cluster,
                    serialized_plan,
                ))),
        )))
    }

    fn worker_context(
        &self,
        serialized_plan: Arc<SerializedPlan>,
    ) -> Result<Arc<ExecutionContext>, CubeError> {
        Ok(Arc::new(ExecutionContext::with_config(
            ExecutionConfig::new()
                .with_batch_size(4096)
                .with_concurrency(1)
                .with_query_planner(Arc::new(CubeQueryPlanner::new_on_worker(serialized_plan))),
        )))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CubeTable {
    index_snapshot: IndexSnapshot,
    schema: SchemaRef,
    // Filled by workers
    remote_to_local_names: HashMap<String, String>,
    worker_partition_ids: Vec<(u64, RowFilter)>,
    #[serde(skip, default)]
    chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    #[serde(skip, default = "NoopParquetMetadataCache::new")]
    parquet_metadata_cache: Arc<dyn ParquetMetadataCache>,
}

impl Debug for CubeTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CubeTable")
            .field("index", self.index_snapshot.index())
            .field("schema", &self.schema)
            .field("worker_partition_ids", &self.worker_partition_ids)
            .finish()
    }
}

impl CubeTable {
    pub fn try_new(
        index_snapshot: IndexSnapshot,
        remote_to_local_names: HashMap<String, String>,
        worker_partition_ids: Vec<(u64, RowFilter)>,
        parquet_metadata_cache: Arc<dyn ParquetMetadataCache>,
    ) -> Result<Self, CubeError> {
        let schema = Arc::new(Schema::new(
            // Tables are always exposed only using table columns order instead of index one because
            // index isn't selected until logical optimization plan is done.
            // Projection indices would refer to these table columns
            index_snapshot
                .table_path
                .table
                .get_row()
                .get_columns()
                .iter()
                .map(|c| c.clone().into())
                .collect(),
        ));
        Ok(Self {
            index_snapshot,
            schema,
            remote_to_local_names,
            worker_partition_ids,
            chunk_id_to_record_batches: HashMap::new(),
            parquet_metadata_cache,
        })
    }

    pub fn has_partitions(&self, partition_ids: &Vec<(u64, RowFilter)>) -> bool {
        let partition_snapshots = self.index_snapshot.partitions();
        partition_snapshots.iter().any(|p| {
            partition_ids
                .binary_search_by_key(&p.partition().get_id(), |(id, _)| *id)
                .is_ok()
        })
    }

    #[must_use]
    pub fn to_worker_table(
        &self,
        remote_to_local_names: HashMap<String, String>,
        worker_partition_ids: Vec<(u64, RowFilter)>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
        parquet_metadata_cache: Arc<dyn ParquetMetadataCache>,
    ) -> CubeTable {
        debug_assert!(worker_partition_ids.iter().is_sorted_by_key(|(id, _)| id));
        let mut t = self.clone();
        t.remote_to_local_names = remote_to_local_names;
        t.worker_partition_ids = worker_partition_ids;
        t.chunk_id_to_record_batches = chunk_id_to_record_batches;
        t.parquet_metadata_cache = parquet_metadata_cache;
        t
    }

    pub fn index_snapshot(&self) -> &IndexSnapshot {
        &self.index_snapshot
    }

    fn async_scan(
        &self,
        table_projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        let partition_snapshots = self.index_snapshot.partitions();

        let mut partition_execs = Vec::<Arc<dyn ExecutionPlan>>::new();
        let table_cols = self.index_snapshot.table().get_row().get_columns();
        let index_cols = self.index_snapshot.index().get_row().get_columns();

        // We always introduce projection because index and table columns do not match in general
        // case so we can use simpler code without branching to handle it.
        let table_projection = table_projection
            .clone()
            .unwrap_or((0..self.schema.fields().len()).collect::<Vec<_>>());

        // Prepare projection
        // If it's non last row query just return projection itself
        // If it's last row query re-project it as (key1, key2, __seq, col3, col4)
        let table_projection_with_seq_column = {
            let table = self.index_snapshot.table_path.table.get_row();
            if let Some(mut key_columns) = table.unique_key_columns() {
                key_columns.push(table.seq_column().expect(&format!(
                    "Seq column is undefined for table: {}",
                    table.get_table_name()
                )));
                let mut with_seq = Vec::new();
                for column in key_columns {
                    if !with_seq.iter().any(|s| *s == column.get_index()) {
                        with_seq.push(column.get_index());
                    }
                }
                for original_projection_index in &table_projection {
                    if !with_seq.iter().any(|s| *s == *original_projection_index) {
                        with_seq.push(*original_projection_index);
                    }
                }
                with_seq
            } else {
                table_projection.clone()
            }
        };

        // Remap table column indices to index ones
        let index_projection = {
            let mut partition_projection =
                Vec::with_capacity(table_projection_with_seq_column.len());
            for table_col_i in &table_projection_with_seq_column {
                let name = table_cols[*table_col_i].get_name();
                let (part_col_i, _) = index_cols
                    .iter()
                    .find_position(|c| c.get_name() == name)
                    .unwrap();
                partition_projection.push(part_col_i);
            }
            // Parquet does not rearrange columns on projection. This looks like a bug, but until
            // this is fixed, we have to handle this ourselves.
            partition_projection.sort();
            partition_projection
        };

        // All persisted and in memory data should be stored using this schema
        let index_schema = Arc::new(Schema::new(
            index_cols
                .iter()
                .map(|i| {
                    self.schema
                        .field(
                            table_cols
                                .iter()
                                .find_position(|c| c.get_name() == i.get_name())
                                .unwrap()
                                .0,
                        )
                        .clone()
                })
                .collect(),
        ));

        let index_projection_schema = {
            Arc::new(Schema::new(
                index_projection
                    .iter()
                    .map(|i| index_schema.field(*i).clone())
                    .collect(),
            ))
        };

        // Save some cycles inside scan nodes on projection if schema matches
        let index_projection_or_none_on_schema_match = if index_projection_schema != index_schema {
            Some(index_projection.clone())
        } else {
            None
        };

        let predicate = combine_filters(filters);
        for partition_snapshot in partition_snapshots {
            let partition = partition_snapshot.partition();
            let filter = self
                .worker_partition_ids
                .binary_search_by_key(&partition.get_id(), |(id, _)| *id);
            let filter = match filter {
                Ok(i) => Arc::new(self.worker_partition_ids[i].1.clone()),
                Err(_) => continue,
            };

            let key_len = self.index_snapshot.index.get_row().sort_key_size() as usize;

            if let Some(remote_path) = partition.get_row().get_full_name(partition.get_id()) {
                let local_path = self
                    .remote_to_local_names
                    .get(remote_path.as_str())
                    .expect(format!("Missing remote path {}", remote_path).as_str());
                let arc: Arc<dyn ExecutionPlan> = Arc::new(ParquetExec::try_from_path_with_cache(
                    &local_path,
                    index_projection_or_none_on_schema_match.clone(),
                    predicate.clone(),
                    batch_size,
                    1,
                    None, // TODO: propagate limit
                    self.parquet_metadata_cache.clone(),
                )?);
                let arc = FilterByKeyRangeExec::issue_filters(arc, filter.clone(), key_len);
                partition_execs.push(arc);
            }

            let chunks = partition_snapshot.chunks();
            for chunk in chunks {
                let node: Arc<dyn ExecutionPlan> = if chunk.get_row().in_memory() {
                    let record_batches = self
                        .chunk_id_to_record_batches
                        .get(&chunk.get_id())
                        .ok_or(CubeError::internal(format!(
                            "Record batch for in memory chunk {:?} is not provided",
                            chunk
                        )))?;
                    if let Some(batch) = record_batches.iter().next() {
                        if batch.schema() != index_schema {
                            return Err(CubeError::internal(format!(
                                "Index schema {:?} and in memory chunk schema {:?} mismatch",
                                index_schema,
                                record_batches[0].schema()
                            )));
                        }
                    }
                    Arc::new(MemoryExec::try_new(
                        &[record_batches.clone()],
                        index_projection_schema.clone(),
                        index_projection_or_none_on_schema_match.clone(),
                    )?)
                } else {
                    let remote_path = chunk.get_row().get_full_name(chunk.get_id());
                    let local_path = self
                        .remote_to_local_names
                        .get(&remote_path)
                        .expect(format!("Missing remote path {}", remote_path).as_str());
                    Arc::new(ParquetExec::try_from_path_with_cache(
                        local_path,
                        index_projection_or_none_on_schema_match.clone(),
                        predicate.clone(),
                        batch_size,
                        1,
                        None, // TODO: propagate limit
                        self.parquet_metadata_cache.clone(),
                    )?)
                };

                let node = FilterByKeyRangeExec::issue_filters(node, filter.clone(), key_len);
                partition_execs.push(node);
            }
        }

        // We might need extra projection to re-order data because we used sorted indices projection version to workaround parquet bug.
        // Please note for consistency reasons in memory chunks are also re-projected the same way even if it's not required to.
        let mut final_reorder = Vec::with_capacity(table_projection_with_seq_column.len());
        for table_col_i in &table_projection_with_seq_column {
            let name = table_cols[*table_col_i].get_name();
            let index_col_i = index_cols
                .iter()
                .find_position(|c| c.get_name() == name)
                .unwrap()
                .0;
            let batch_col_i = index_projection
                .iter()
                .find_position(|c| **c == index_col_i)
                .unwrap()
                .0;
            final_reorder.push(batch_col_i);
        }
        if !final_reorder
            .iter()
            .cloned()
            .eq(0..table_projection_with_seq_column.len())
        {
            for p in &mut partition_execs {
                let s = p.schema();
                let proj_exprs = final_reorder
                    .iter()
                    .map(|c| {
                        let name = s.field(*c).name();
                        let col = datafusion::physical_plan::expressions::Column::new(name, *c);
                        let col: Arc<dyn PhysicalExpr> = Arc::new(col);
                        (col, name.clone())
                    })
                    .collect_vec();
                *p = Arc::new(ProjectionExec::try_new(proj_exprs, p.clone()).unwrap())
            }
        }

        // Schema for scan output and input to MergeSort and LastRowByUniqueKey
        let table_projected_schema = {
            Arc::new(Schema::new(
                table_projection_with_seq_column
                    .iter()
                    .map(|i| self.schema.field(*i).clone())
                    .collect(),
            ))
        };
        // TODO: 'nullable' modifiers differ, fix this and re-enable assertion.
        // for p in &partition_execs {
        //     assert_eq!(p.schema(), projected_schema);
        // }

        if partition_execs.len() == 0 {
            partition_execs.push(Arc::new(EmptyExec::new(
                false,
                table_projected_schema.clone(),
            )));
        }

        let schema = table_projected_schema;
        let read_data = Arc::new(CubeTableExec {
            schema: schema.clone(),
            partition_execs,
            index_snapshot: self.index_snapshot.clone(),
            filter: predicate,
        });
        let unique_key_columns = self
            .index_snapshot()
            .table_path
            .table
            .get_row()
            .unique_key_columns();

        let plan: Arc<dyn ExecutionPlan> = if let Some(key_columns) = unique_key_columns {
            let sort_columns = self
                .index_snapshot()
                .index
                .get_row()
                .columns()
                .iter()
                .take(self.index_snapshot.index.get_row().sort_key_size() as usize)
                .map(|c| {
                    datafusion::physical_plan::expressions::Column::new_with_schema(
                        c.get_name(),
                        &schema,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut exec: Arc<dyn ExecutionPlan> =
                Arc::new(MergeSortExec::try_new(read_data, sort_columns)?);
            exec = Arc::new(LastRowByUniqueKeyExec::try_new(
                exec,
                key_columns
                    .iter()
                    .map(|c| {
                        datafusion::physical_plan::expressions::Column::new_with_schema(
                            c.get_name().as_str(),
                            &schema,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )?);

            // At this point data is projected for last row query and we need to re-project it to what actually queried
            let s = exec.schema();
            let proj_exprs = table_projection
                .iter()
                .map(|c| {
                    let name = table_cols[*c].get_name();
                    let col = datafusion::physical_plan::expressions::Column::new(
                        name,
                        s.index_of(name)?,
                    );
                    let col: Arc<dyn PhysicalExpr> = Arc::new(col);
                    Ok((col, name.clone()))
                })
                .collect::<Result<Vec<_>, CubeError>>()?;
            Arc::new(ProjectionExec::try_new(proj_exprs, exec)?)
        } else if let Some(join_columns) = self.index_snapshot.sort_on() {
            assert!(join_columns.len() <= (self.index_snapshot().index.get_row().sort_key_size() as usize), "The number of columns to sort is greater than the number of sorted columns in the index");
            assert!(
                self.index_snapshot()
                    .index
                    .get_row()
                    .columns()
                    .iter()
                    .take(join_columns.len())
                    .zip(join_columns.iter())
                    .all(|(icol, jcol)| icol.get_name() == jcol),
                "The columns to sort don't match the sorted columns in the index"
            );

            let join_columns = join_columns
                .iter()
                .map(|c| {
                    datafusion::physical_plan::expressions::Column::new_with_schema(c, &schema)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Arc::new(MergeSortExec::try_new(read_data, join_columns)?)
        } else {
            Arc::new(MergeExec::new(read_data))
        };

        Ok(plan)
    }

    pub fn project_to_index_positions(
        projection_columns: &Vec<String>,
        i: &IdRow<Index>,
    ) -> Vec<Option<usize>> {
        projection_columns
            .iter()
            .map(|pc| {
                i.get_row()
                    .get_columns()
                    .iter()
                    .find_position(|c| c.get_name() == pc)
                    .map(|(p, _)| p)
            })
            .collect::<Vec<_>>()
    }

    pub fn project_to_table(
        table: &IdRow<Table>,
        projection_column_indices: &Vec<usize>,
    ) -> Vec<Column> {
        projection_column_indices
            .iter()
            .map(|i| table.get_row().get_columns()[*i].clone())
            .collect::<Vec<_>>()
    }
}

pub struct CubeTableExec {
    schema: SchemaRef,
    pub(crate) index_snapshot: IndexSnapshot,
    partition_execs: Vec<Arc<dyn ExecutionPlan>>,
    pub(crate) filter: Option<Expr>,
}

impl Debug for CubeTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CubeTableExec")
            .field("index", self.index_snapshot.index())
            .field("partition_execs", &self.partition_execs)
            .finish()
    }
}

#[async_trait]
impl ExecutionPlan for CubeTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition_execs.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.partition_execs.clone()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(CubeTableExec {
            schema: self.schema.clone(),
            partition_execs: children,
            index_snapshot: self.index_snapshot.clone(),
            filter: self.filter.clone(),
        }))
    }

    fn output_hints(&self) -> OptimizerHints {
        let sort_order;
        if let Some(snapshot_sort_on) = self.index_snapshot.sort_on() {
            // Note that this returns `None` if any of the columns were not found.
            // This only happens on programming errors.
            sort_order = snapshot_sort_on
                .iter()
                .map(|c| self.schema.index_of(&c).ok())
                .collect()
        } else {
            let index = self.index_snapshot.index().get_row();
            let sort_cols = index
                .get_columns()
                .iter()
                .take(index.sort_key_size() as usize)
                .map(|sort_col| self.schema.index_of(&sort_col.get_name()).ok())
                .take_while(|i| i.is_some())
                .map(|i| i.unwrap())
                .collect_vec();
            if !sort_cols.is_empty() {
                sort_order = Some(sort_cols)
            } else {
                sort_order = None
            }
        }

        OptimizerHints {
            sort_order,
            single_value_columns: Vec::new(),
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.partition_execs[partition].execute(0).await
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InlineTableProvider {
    id: u64,
    data: Arc<DataFrame>,
    // Filled in by workers
    inline_table_ids: Vec<InlineTableId>,
}

impl InlineTableProvider {
    pub fn new(
        id: u64,
        data: Arc<DataFrame>,
        inline_table_ids: Vec<InlineTableId>,
    ) -> InlineTableProvider {
        InlineTableProvider {
            id,
            data,
            inline_table_ids,
        }
    }

    pub fn get_id(self: &Self) -> u64 {
        self.id
    }

    pub fn get_data(self: &Self) -> Arc<DataFrame> {
        self.data.clone()
    }

    #[must_use]
    pub fn to_worker_table(&self, inline_table_ids: Vec<InlineTableId>) -> InlineTableProvider {
        let mut t = self.clone();
        t.inline_table_ids = inline_table_ids;
        t
    }

    pub fn has_inline_table_id(self: &Self, inline_table_ids: &Vec<InlineTableId>) -> bool {
        inline_table_ids.iter().any(|id| id == &self.id)
    }
}

impl Debug for InlineTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InlineTable").finish()
    }
}

pub struct ClusterSendExec {
    schema: SchemaRef,
    pub partitions: Vec<(
        /*node*/ String,
        (Vec<PartitionWithFilters>, Vec<InlineTableId>),
    )>,
    /// Never executed, only stored to allow consistent optimization on router and worker.
    pub input_for_optimizations: Arc<dyn ExecutionPlan>,
    pub cluster: Arc<dyn Cluster>,
    pub serialized_plan: Arc<SerializedPlan>,
    pub use_streaming: bool,
}

pub type PartitionWithFilters = (u64, RowRange);
pub type InlineTableId = u64;

/// Compound structure to assign inline tables to specific partitions so they can get to the same worker.
/// It's helpful to do so in order to avoid allocation of additional workers for inline table micro reads.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum InlineCompoundPartition {
    Partition(IdRow<Partition>),
    PartitionWithInlineTables(IdRow<Partition>, Vec<InlineTableId>),
    InlineTables(Vec<InlineTableId>),
}

impl ClusterSendExec {
    pub fn new(
        schema: SchemaRef,
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<SerializedPlan>,
        union_snapshots: &[Snapshots],
        input_for_optimizations: Arc<dyn ExecutionPlan>,
        use_streaming: bool,
    ) -> Result<Self, CubeError> {
        let partitions = Self::distribute_to_workers(
            cluster.config().as_ref(),
            union_snapshots,
            &serialized_plan.planning_meta().multi_part_subtree,
        )?;
        Ok(Self {
            schema,
            partitions,
            cluster,
            serialized_plan,
            input_for_optimizations,
            use_streaming,
        })
    }

    pub(crate) fn distribute_to_workers(
        config: &dyn ConfigObj,
        snapshots: &[Snapshots],
        tree: &HashMap<u64, MultiPartition>,
    ) -> Result<Vec<(String, (Vec<PartitionWithFilters>, Vec<InlineTableId>))>, CubeError> {
        let partitions = Self::logical_partitions(snapshots, tree)?;
        Ok(Self::assign_nodes(config, partitions))
    }

    fn logical_partitions(
        snapshots: &[Snapshots],
        tree: &HashMap<u64, MultiPartition>,
    ) -> Result<Vec<Vec<InlineCompoundPartition>>, CubeError> {
        let mut to_multiply = Vec::new();
        let mut multi_partitions = HashMap::<u64, Vec<_>>::new();
        let mut has_inline_tables = false;
        for union in snapshots.iter() {
            let mut ordinary_partitions = Vec::new();
            let mut inline_table_ids = Vec::new();
            for index in union {
                match index {
                    Snapshot::Index(index) => {
                        for p in &index.partitions {
                            match p.partition.get_row().multi_partition_id() {
                                Some(id) => multi_partitions
                                    .entry(id)
                                    .or_default()
                                    .push(p.partition.clone()),
                                None => ordinary_partitions.push(p.partition.clone()),
                            }
                        }
                    }
                    Snapshot::Inline(inline) => {
                        has_inline_tables = true;
                        inline_table_ids.push(inline.id);
                    }
                }
            }
            let partitions_merged_with_inline_tables =
                if !ordinary_partitions.is_empty() && !inline_table_ids.is_empty() {
                    let last_partition = ordinary_partitions.pop().unwrap();
                    let mut result = ordinary_partitions
                        .into_iter()
                        .map(|p| InlineCompoundPartition::Partition(p))
                        .collect::<Vec<_>>();
                    result.push(InlineCompoundPartition::PartitionWithInlineTables(
                        last_partition,
                        inline_table_ids,
                    ));
                    result
                } else if inline_table_ids.is_empty() {
                    ordinary_partitions
                        .into_iter()
                        .map(|p| InlineCompoundPartition::Partition(p))
                        .collect()
                } else if ordinary_partitions.is_empty() {
                    vec![InlineCompoundPartition::InlineTables(inline_table_ids)]
                } else {
                    Vec::new()
                };
            if !partitions_merged_with_inline_tables.is_empty() {
                to_multiply.push(partitions_merged_with_inline_tables);
            }
        }
        assert!(to_multiply.is_empty() || multi_partitions.is_empty(),
                "invalid state during partition selection. to_multiply: {:?}, multi_partitions: {:?}, snapshots: {:?}",
                to_multiply, multi_partitions, snapshots);
        // Multi partitions define how we distribute joins. They may not be present, though.
        if !multi_partitions.is_empty() {
            if has_inline_tables {
                return Err(CubeError::user(
                    "Partitioned index queries aren't supported with inline tables".to_string(),
                ));
            }
            return Ok(Self::distribute_multi_partitions(multi_partitions, tree)
                .into_iter()
                .map(|i| {
                    i.into_iter()
                        .map(|p| InlineCompoundPartition::Partition(p))
                        .collect()
                })
                .collect());
        }
        // Ordinary partitions need to be duplicated on multiple machines.
        let partitions = to_multiply
            .into_iter()
            .multi_cartesian_product()
            .collect::<Vec<Vec<_>>>();
        Ok(partitions)
    }

    fn distribute_multi_partitions(
        mut multi_partitions: HashMap<u64, Vec<IdRow<Partition>>>,
        tree: &HashMap<u64, MultiPartition>,
    ) -> Vec<Vec<IdRow<Partition>>> {
        let mut has_children = HashSet::new();
        for m in tree.values() {
            if let Some(p) = m.parent_multi_partition_id() {
                has_children.insert(p);
            }
        }
        // Ensure stable output order.
        for parts in multi_partitions.values_mut() {
            parts.sort_unstable_by_key(|p| p.get_id());
        }

        // Append partitions from ancestors to leaves.
        let mut leaves = HashMap::new();
        for (m, parts) in multi_partitions.iter_mut() {
            if !has_children.contains(m) {
                leaves.insert(*m, take(parts));
            }
        }

        for (m, parts) in leaves.iter_mut() {
            let mut curr = tree[m].parent_multi_partition_id();
            while let Some(p) = curr {
                if let Some(ps) = multi_partitions.get(&p) {
                    parts.extend_from_slice(ps)
                }
                curr = tree[&p].parent_multi_partition_id();
            }
        }

        // Ensure stable output order.
        let mut ps: Vec<_> = leaves.into_values().collect();
        ps.sort_unstable_by_key(|ps| ps[0].get_id());
        ps
    }

    fn issue_filters(ps: &[IdRow<Partition>]) -> Vec<(u64, RowRange)> {
        if ps.is_empty() {
            return Vec::new();
        }
        // [distribute_to_workers] guarantees [ps] starts with a leaf inside a multi-partition,
        // any other multi-partition is from ancestors and we should issue filters for those.
        let multi_id = ps[0].get_row().multi_partition_id();
        if multi_id.is_none() {
            return ps
                .iter()
                .map(|p| (p.get_id(), RowRange::default()))
                .collect();
        }
        let filter = RowRange {
            start: ps[0].get_row().get_min_val().clone(),
            end: ps[0].get_row().get_max_val().clone(),
        };

        let mut r = Vec::with_capacity(ps.len());
        for p in ps {
            let pf = if multi_id == p.get_row().multi_partition_id() {
                RowRange::default()
            } else {
                filter.clone()
            };
            r.push((p.get_id(), pf))
        }
        r
    }

    fn assign_nodes(
        c: &dyn ConfigObj,
        logical: Vec<Vec<InlineCompoundPartition>>,
    ) -> Vec<(String, (Vec<(u64, RowRange)>, Vec<InlineTableId>))> {
        let mut m: HashMap<_, (Vec<(u64, RowRange)>, Vec<InlineTableId>)> = HashMap::new();
        for ps in &logical {
            let inline_table_ids = ps
                .iter()
                .filter_map(|p| match p {
                    InlineCompoundPartition::PartitionWithInlineTables(_, inline_tables) => {
                        Some(inline_tables.clone())
                    }
                    InlineCompoundPartition::InlineTables(inline_tables) => {
                        Some(inline_tables.clone())
                    }
                    _ => None,
                })
                .flatten()
                .collect::<Vec<_>>();
            let partitions = ps
                .iter()
                .filter_map(|p| match p {
                    InlineCompoundPartition::PartitionWithInlineTables(p, _) => Some(p.clone()),
                    InlineCompoundPartition::Partition(p) => Some(p.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let node = match partitions
                .iter()
                .next()
                .and_then(|p| p.get_row().multi_partition_id())
            {
                Some(multi_id) => pick_worker_by_ids(c, [multi_id]),
                None => pick_worker_by_partitions(c, partitions.iter()),
            };
            let node_entry = &mut m.entry(node.to_string()).or_default();
            node_entry
                .0
                .extend(Self::issue_filters(partitions.as_slice()));
            node_entry.1.extend(inline_table_ids);
        }

        let mut r = m.into_iter().collect_vec();
        r.sort_unstable_by(|l, r| l.0.cmp(&r.0));
        r
    }

    pub fn with_changed_schema(
        &self,
        schema: SchemaRef,
        input_for_optimizations: Arc<dyn ExecutionPlan>,
    ) -> Self {
        ClusterSendExec {
            schema,
            partitions: self.partitions.clone(),
            cluster: self.cluster.clone(),
            serialized_plan: self.serialized_plan.clone(),
            input_for_optimizations,
            use_streaming: self.use_streaming,
        }
    }

    pub fn worker_plans(&self) -> Vec<(String, SerializedPlan)> {
        let mut res = Vec::new();
        for (node_name, partitions) in self.partitions.iter() {
            res.push((
                node_name.clone(),
                self.serialized_plan_for_partitions(partitions),
            ));
        }
        res
    }

    fn serialized_plan_for_partitions(
        &self,
        partitions: &(Vec<(u64, RowRange)>, Vec<InlineTableId>),
    ) -> SerializedPlan {
        let (partitions, inline_table_ids) = partitions;
        let mut ps = HashMap::<_, RowFilter>::new();
        for (id, range) in partitions {
            ps.entry(*id).or_default().append_or(range.clone())
        }
        let mut ps = ps.into_iter().collect_vec();
        ps.sort_unstable_by_key(|(id, _)| *id);

        self.serialized_plan
            .with_partition_id_to_execute(ps, inline_table_ids.clone())
    }
}

#[async_trait]
impl ExecutionPlan for ClusterSendExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input_for_optimizations.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            panic!("expected exactly one input");
        }
        let input_for_optimizations = children.into_iter().next().unwrap();
        Ok(Arc::new(ClusterSendExec {
            schema: self.schema.clone(),
            partitions: self.partitions.clone(),
            cluster: self.cluster.clone(),
            serialized_plan: self.serialized_plan.clone(),
            input_for_optimizations,
            use_streaming: self.use_streaming,
        }))
    }

    fn output_hints(&self) -> OptimizerHints {
        self.input_for_optimizations.output_hints()
    }

    #[instrument(level = "trace", skip(self))]
    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let (node_name, partitions) = &self.partitions[partition];

        let plan = self.serialized_plan_for_partitions(partitions);

        if self.use_streaming {
            Ok(self.cluster.run_select_stream(node_name, plan).await?)
        } else {
            let record_batches = self.cluster.run_select(node_name, plan).await?;
            // TODO .to_schema_ref()
            let memory_exec = MemoryExec::try_new(&vec![record_batches], self.schema(), None)?;
            memory_exec.execute(0).await
        }
    }
}

impl fmt::Debug for ClusterSendExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "ClusterSendExec: {:?}: {:?}",
            self.schema, self.partitions
        ))
    }
}

impl TableProvider for CubeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        _limit: Option<usize>, // TODO: propagate limit
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let res = self.async_scan(projection, batch_size, filters)?;
        Ok(res)
    }

    fn statistics(&self) -> Statistics {
        // TODO
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        return Ok(TableProviderFilterPushDown::Inexact);
    }
}

impl TableProvider for InlineTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.data.get_schema()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>, // TODO: propagate limit
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let projected_schema = if let Some(p) = projection {
            Arc::new(Schema::new(
                p.iter().map(|i| schema.field(*i).clone()).collect(),
            ))
        } else {
            schema
        };

        if !self.inline_table_ids.iter().any(|id| id == &self.id) {
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        let batches = dataframe_to_batches(self.data.as_ref(), batch_size)?;
        let projection = (*projection).clone();
        Ok(Arc::new(MemoryExec::try_new(
            &vec![batches],
            projected_schema,
            projection,
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        return Ok(TableProviderFilterPushDown::Unsupported);
    }
}

macro_rules! convert_array_cast_native {
    ($V: expr, (Vec<u8>)) => {{
        $V.to_vec()
    }};
    ($V: expr, (Decimal)) => {{
        crate::util::decimal::Decimal::new($V)
    }};
    ($V: expr, $T: ty) => {{
        $V as $T
    }};
}

macro_rules! convert_array {
    ($ARRAY:expr, $NUM_ROWS:expr, $ROWS:expr, $ARRAY_TYPE: ident, $TABLE_TYPE: ident, $NATIVE: tt) => {{
        let a = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        for i in 0..$NUM_ROWS {
            $ROWS[i].push(if a.is_null(i) {
                TableValue::Null
            } else {
                TableValue::$TABLE_TYPE(convert_array_cast_native!(a.value(i), $NATIVE))
            });
        }
    }};
}

pub fn batch_to_dataframe(batches: &Vec<RecordBatch>) -> Result<DataFrame, CubeError> {
    let mut cols = vec![];
    let mut all_rows = vec![];

    for batch in batches.iter() {
        if cols.len() == 0 {
            let schema = batch.schema().clone();
            for (i, field) in schema.fields().iter().enumerate() {
                cols.push(Column::new(
                    field.name().clone(),
                    arrow_to_column_type(field.data_type().clone())?,
                    i,
                ));
            }
        }
        if batch.num_rows() == 0 {
            continue;
        }
        let mut rows = vec![];

        for _ in 0..batch.num_rows() {
            rows.push(Row::new(Vec::with_capacity(batch.num_columns())));
        }

        for column_index in 0..batch.num_columns() {
            let array = batch.column(column_index);
            let num_rows = batch.num_rows();
            match array.data_type() {
                DataType::UInt64 => convert_array!(array, num_rows, rows, UInt64Array, Int, i64),
                DataType::Int64 => convert_array!(array, num_rows, rows, Int64Array, Int, i64),
                DataType::Float64 => {
                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            let decimal = a.value(i) as f64;
                            TableValue::Float(decimal.into())
                        });
                    }
                }
                DataType::Int64Decimal(0) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal0Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(1) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal1Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(2) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal2Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(3) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal3Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(4) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal4Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(5) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal5Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(10) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal10Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(a.value(i) * 1000 as i64))
                        });
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(a.value(i)))
                        });
                    }
                }
                DataType::Binary => {
                    convert_array!(array, num_rows, rows, BinaryArray, Bytes, (Vec<u8>))
                }
                DataType::Utf8 => {
                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::String(a.value(i).to_string())
                        });
                    }
                }
                DataType::Boolean => {
                    let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Boolean(a.value(i))
                        });
                    }
                }
                x => panic!("Unsupported data type: {:?}", x),
            }
        }
        all_rows.append(&mut rows);
    }
    Ok(DataFrame::new(cols, all_rows))
}

pub fn arrow_to_column_type(arrow_type: DataType) -> Result<ColumnType, CubeError> {
    match arrow_type {
        DataType::Binary => Ok(ColumnType::Bytes),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnType::String),
        DataType::Timestamp(_, _) => Ok(ColumnType::Timestamp),
        DataType::Float16 | DataType::Float64 => Ok(ColumnType::Float),
        DataType::Int64Decimal(scale) => Ok(ColumnType::Decimal {
            scale: scale as i32,
            precision: 18,
        }),
        DataType::Boolean => Ok(ColumnType::Boolean),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(ColumnType::Int),
        x => Err(CubeError::internal(format!("unsupported type {:?}", x))),
    }
}

pub fn dataframe_to_batches(
    data: &DataFrame,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, CubeError> {
    let mut batches = vec![];
    let mut b = 0;
    while b < data.len() {
        let rows = &data.get_rows()[b..min(b + batch_size, data.len())];
        let batch = rows_to_columns(&data.get_columns(), rows);
        batches.push(RecordBatch::try_new(data.get_schema(), batch)?);
        b += batch_size;
    }
    Ok(batches)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SerializedRecordBatchStream {
    #[serde(with = "serde_bytes")] // serde_bytes makes serialization efficient.
    record_batch_file: Vec<u8>,
}

impl SerializedRecordBatchStream {
    pub fn write(
        schema: &Schema,
        record_batches: Vec<RecordBatch>,
    ) -> Result<Vec<Self>, CubeError> {
        let mut results = Vec::with_capacity(record_batches.len());
        for batch in record_batches {
            let file = Vec::new();
            let mut writer = MemStreamWriter::try_new(Cursor::new(file), schema)?;
            writer.write(&batch)?;
            let cursor = writer.finish()?;
            results.push(Self {
                record_batch_file: cursor.into_inner(),
            })
        }
        Ok(results)
    }

    pub fn read(self) -> Result<RecordBatch, CubeError> {
        let cursor = Cursor::new(self.record_batch_file);
        let mut reader = StreamReader::try_new(cursor)?;
        let batch = reader.next();
        if batch.is_none() {
            return Err(CubeError::internal("zero batches deserialized".to_string()));
        }
        let batch = batch.unwrap()?;
        if !reader.next().is_none() {
            return Err(CubeError::internal(
                "more than one batch deserialized".to_string(),
            ));
        }
        Ok(batch)
    }
}

/// Note: copy of the function in 'datafusion/src/datasource/parquet.rs'.
///
/// Combines an array of filter expressions into a single filter expression
/// consisting of the input filter expressions joined with logical AND.
/// Returns None if the filters array is empty.
fn combine_filters(filters: &[Expr]) -> Option<Expr> {
    if filters.is_empty() {
        return None;
    }
    let combined_filter = filters
        .iter()
        .skip(1)
        .fold(filters[0].clone(), |acc, filter| {
            logical_plan::and(acc, filter.clone())
        });
    Some(combined_filter)
}

fn regroup_batches(
    batches: Vec<RecordBatch>,
    max_rows: usize,
) -> Result<Vec<RecordBatch>, CubeError> {
    let mut r = Vec::with_capacity(batches.len());
    for b in batches {
        let mut row = 0;
        while row != b.num_rows() {
            let slice_len = min(b.num_rows() - row, max_rows);
            r.push(RecordBatch::try_new(
                b.schema(),
                b.columns()
                    .iter()
                    .map(|c| slice_copy(c.as_ref(), row, slice_len))
                    .collect(),
            )?);
            row += slice_len
        }
    }
    Ok(r)
}

fn slice_copy(a: &dyn Array, start: usize, len: usize) -> ArrayRef {
    // If we use [Array::slice], serialization will still copy the whole contents.
    let mut a = MutableArrayData::new(vec![a.data()], false, len);
    a.extend(0, start, start + len);
    make_array(a.freeze())
}
