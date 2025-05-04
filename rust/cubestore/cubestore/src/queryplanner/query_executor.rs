use crate::cluster::{
    pick_worker_by_ids, pick_worker_by_partitions, Cluster, WorkerPlanningParams,
};
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::table::Table;
use crate::metastore::{Column, ColumnType, IdRow, Index, Partition};
use crate::queryplanner::filter_by_key_range::FilterByKeyRangeExec;
use crate::queryplanner::merge_sort::LastRowByUniqueKeyExec;
use crate::queryplanner::metadata_cache::{MetadataCacheFactory, NoopParquetMetadataCache};
use crate::queryplanner::optimizations::{CubeQueryPlanner, PreOptimizeRule};
use crate::queryplanner::physical_plan_flags::PhysicalPlanFlags;
use crate::queryplanner::planning::{get_worker_plan, Snapshot, Snapshots};
use crate::queryplanner::pretty_printers::{pp_phys_plan, pp_phys_plan_ext, pp_plan, PPOptions};
use crate::queryplanner::serialized_plan::{IndexSnapshot, RowFilter, RowRange, SerializedPlan};
use crate::queryplanner::trace_data_loaded::DataLoadedSize;
use crate::store::DataFrame;
use crate::table::data::rows_to_columns;
use crate::table::parquet::CubestoreParquetMetadataCache;
use crate::table::{Row, TableValue, TimestampValue};
use crate::telemetry::suboptimal_query_plan_event;
use crate::util::memory::MemoryHandler;
use crate::{app_metrics, CubeError};
use async_trait::async_trait;
use core::fmt;
use datafusion::arrow::array::{
    make_array, Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array,
    Int16Array, Int32Array, Int64Array, MutableArrayData, NullArray, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::ToDFSchema;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::get_reader_options_customizer;
use datafusion::datasource::physical_plan::{
    FileScanConfig, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexRequirement, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::topk_aggregation::TopKAggregation;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PhysicalExpr, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::{and, SessionConfig, SessionContext};
use datafusion_datasource::memory::MemoryExec;
use datafusion_datasource::source::DataSourceExec;
use futures_util::{stream, StreamExt, TryStreamExt};
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

use super::serialized_plan::PreSerializedPlan;
use super::udfs::{registerable_arc_aggregate_udfs, registerable_arc_scalar_udfs};
use super::QueryPlannerImpl;

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
        worker_planning_params: WorkerPlanningParams,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, usize), CubeError>;

    async fn router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError>;

    async fn worker_plan(
        &self,
        plan: SerializedPlan,
        worker_planning_params: WorkerPlanningParams,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
        data_loaded_size: Option<Arc<DataLoadedSize>>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError>;

    async fn pp_worker_plan(
        &self,
        plan: SerializedPlan,
        worker_planning_params: WorkerPlanningParams,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<String, CubeError>;
}

crate::di_service!(MockQueryExecutor, [QueryExecutor]);

pub struct QueryExecutorImpl {
    // TODO: Why do we need a MetadataCacheFactory when we have a ParquetMetadataCache?  (We use its make_session_config() now, TODO rename stuff)
    metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    parquet_metadata_cache: Arc<dyn CubestoreParquetMetadataCache>,
    memory_handler: Arc<dyn MemoryHandler>,
}

crate::di_service!(QueryExecutorImpl, [QueryExecutor]);

impl QueryExecutorImpl {
    fn execution_context(&self) -> Result<Arc<SessionContext>, CubeError> {
        // This is supposed to be identical to QueryImplImpl::execution_context.
        Ok(Arc::new(QueryPlannerImpl::execution_context_helper(
            self.metadata_cache_factory.make_session_config(),
        )))
    }
}

#[async_trait]
impl QueryExecutor for QueryExecutorImpl {
    #[instrument(level = "trace", skip(self, plan, cluster))]
    async fn execute_router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError> {
        let collect_span = tracing::span!(tracing::Level::TRACE, "collect_physical_plan");
        let trace_obj = plan.trace_obj();
        let (physical_plan, logical_plan) = self.router_plan(plan, cluster).await?;
        let split_plan = physical_plan;

        trace!(
            "Router Query Physical Plan: {}",
            pp_phys_plan(split_plan.as_ref())
        );

        let flags = PhysicalPlanFlags::with_execution_plan(split_plan.as_ref());
        if flags.is_suboptimal_query() {
            if let Some(trace_obj) = trace_obj.as_ref() {
                suboptimal_query_plan_event(trace_obj, flags.to_json())?;
            }
        }

        let execution_time = SystemTime::now();

        let session_context = self.execution_context()?;
        let results = collect(split_plan.clone(), session_context.task_ctx())
            .instrument(collect_span)
            .await;
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
                pp_phys_plan_ext(
                    split_plan.as_ref(),
                    &PPOptions {
                        show_metrics: true,
                        ..PPOptions::none()
                    }
                ),
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
        worker_planning_params: WorkerPlanningParams,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, usize), CubeError> {
        let data_loaded_size = DataLoadedSize::new();
        let (physical_plan, logical_plan) = self
            .worker_plan(
                plan,
                worker_planning_params,
                remote_to_local_names,
                chunk_id_to_record_batches,
                Some(data_loaded_size.clone()),
            )
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
        let session_context = self.execution_context()?;
        let results = collect(worker_plan.clone(), session_context.task_ctx())
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
                pp_plan(&logical_plan),
            );
            debug!(
                "Slow Partition Query Physical Plan ({:?}): {}",
                execution_time.elapsed()?,
                pp_phys_plan_ext(
                    worker_plan.as_ref(),
                    &PPOptions {
                        show_metrics: true,
                        ..PPOptions::none()
                    }
                ),
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
        Ok((worker_plan.schema(), results, data_loaded_size.get()))
    }

    async fn router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError> {
        let pre_serialized_plan = plan.to_pre_serialized(
            HashMap::new(),
            HashMap::new(),
            NoopParquetMetadataCache::new(),
        )?;
        let pre_serialized_plan = Arc::new(pre_serialized_plan);
        let ctx = self.router_context(cluster.clone(), pre_serialized_plan.clone())?;
        // We don't want to use session_state.create_physical_plan(...) because it redundantly
        // optimizes the logical plan, which has already been optimized before it was put into a
        // SerializedPlan (and that takes too much time).
        let session_state = ctx.state();
        let execution_plan = session_state
            .query_planner()
            .create_physical_plan(pre_serialized_plan.logical_plan(), &session_state)
            .await?;
        Ok((execution_plan, pre_serialized_plan.logical_plan().clone()))
    }

    async fn worker_plan(
        &self,
        plan: SerializedPlan,
        worker_planning_params: WorkerPlanningParams,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
        data_loaded_size: Option<Arc<DataLoadedSize>>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError> {
        let pre_serialized_plan = plan.to_pre_serialized(
            remote_to_local_names,
            chunk_id_to_record_batches,
            self.parquet_metadata_cache.cache().clone(),
        )?;
        let pre_serialized_plan = Arc::new(pre_serialized_plan);
        let ctx = self.worker_context(
            pre_serialized_plan.clone(),
            worker_planning_params,
            data_loaded_size,
        )?;
        // We don't want to use session_state.create_physical_plan(...); see comment in router_plan.
        let session_state = ctx.state();
        let execution_plan = session_state
            .query_planner()
            .create_physical_plan(pre_serialized_plan.logical_plan(), &session_state)
            .await?;
        Ok((execution_plan, pre_serialized_plan.logical_plan().clone()))
    }

    async fn pp_worker_plan(
        &self,
        plan: SerializedPlan,
        worker_planning_params: WorkerPlanningParams,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    ) -> Result<String, CubeError> {
        let (physical_plan, _) = self
            .worker_plan(
                plan,
                worker_planning_params,
                remote_to_local_names,
                chunk_id_to_record_batches,
                None,
            )
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
    pub fn new(
        metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
        parquet_metadata_cache: Arc<dyn CubestoreParquetMetadataCache>,
        memory_handler: Arc<dyn MemoryHandler>,
    ) -> Arc<Self> {
        Arc::new(QueryExecutorImpl {
            metadata_cache_factory,
            parquet_metadata_cache,
            memory_handler,
        })
    }

    fn router_context(
        &self,
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<PreSerializedPlan>,
    ) -> Result<Arc<SessionContext>, CubeError> {
        let runtime = Arc::new(RuntimeEnv::default());
        let config = self.session_config();
        let session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(Arc::new(CubeQueryPlanner::new_on_router(
                cluster,
                serialized_plan,
                self.memory_handler.clone(),
            )))
            .with_physical_optimizer_rules(self.optimizer_rules(None))
            .with_aggregate_functions(registerable_arc_aggregate_udfs())
            .with_scalar_functions(registerable_arc_scalar_udfs())
            .build();
        let ctx = SessionContext::new_with_state(session_state);
        Ok(Arc::new(ctx))
    }

    fn optimizer_rules(
        &self,
        data_loaded_size: Option<Arc<DataLoadedSize>>,
    ) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        vec![
            // Cube rules
            Arc::new(PreOptimizeRule::new(
                self.memory_handler.clone(),
                data_loaded_size,
            )),
            // DF rules without EnforceDistribution.  We do need to keep EnforceSorting.
            Arc::new(OutputRequirements::new_add_mode()),
            Arc::new(AggregateStatistics::new()),
            Arc::new(JoinSelection::new()),
            Arc::new(LimitedDistinctAggregation::new()),
            // Arc::new(EnforceDistribution::new()),
            Arc::new(CombinePartialFinalAggregate::new()),
            Arc::new(EnforceSorting::new()),
            Arc::new(OptimizeAggregateOrder::new()),
            Arc::new(ProjectionPushdown::new()),
            Arc::new(CoalesceBatches::new()),
            Arc::new(OutputRequirements::new_remove_mode()),
            Arc::new(TopKAggregation::new()),
            Arc::new(ProjectionPushdown::new()),
            Arc::new(LimitPushdown::new()),
            Arc::new(SanityCheckPlan::new()),
        ]
    }

    fn worker_context(
        &self,
        serialized_plan: Arc<PreSerializedPlan>,
        worker_planning_params: WorkerPlanningParams,
        data_loaded_size: Option<Arc<DataLoadedSize>>,
    ) -> Result<Arc<SessionContext>, CubeError> {
        let runtime = Arc::new(RuntimeEnv::default());
        let config = self.session_config();
        let session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(Arc::new(CubeQueryPlanner::new_on_worker(
                serialized_plan,
                worker_planning_params,
                self.memory_handler.clone(),
                data_loaded_size.clone(),
            )))
            .with_aggregate_functions(registerable_arc_aggregate_udfs())
            .with_scalar_functions(registerable_arc_scalar_udfs())
            .with_physical_optimizer_rules(self.optimizer_rules(data_loaded_size))
            .build();
        let ctx = SessionContext::new_with_state(session_state);
        Ok(Arc::new(ctx))
    }

    fn session_config(&self) -> SessionConfig {
        let mut config = self
            .metadata_cache_factory
            .make_session_config()
            .with_batch_size(4096)
            // TODO upgrade DF if less than 2 then there will be no MergeJoin. Decide on repartitioning.
            .with_target_partitions(2)
            .with_prefer_existing_sort(true)
            .with_round_robin_repartition(false);
        config.options_mut().optimizer.prefer_hash_join = false;
        config
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
    parquet_metadata_cache: Arc<dyn ParquetFileReaderFactory>,
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
        parquet_metadata_cache: Arc<dyn ParquetFileReaderFactory>,
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
                .collect::<Vec<Field>>(),
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
        parquet_metadata_cache: Arc<dyn ParquetFileReaderFactory>,
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
        state: &dyn Session,
        table_projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        let partition_snapshots = self.index_snapshot.partitions();

        let mut partition_execs = Vec::<Arc<dyn ExecutionPlan>>::new();
        let table_cols = self.index_snapshot.table().get_row().get_columns();
        let index_cols = self.index_snapshot.index().get_row().get_columns();

        // We always introduce projection because index and table columns do not match in general
        // case so we can use simpler code without branching to handle it.
        let table_projection = table_projection
            .cloned()
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
                .collect::<Vec<Field>>(),
        ));

        let index_projection_schema = {
            Arc::new(Schema::new(
                index_projection
                    .iter()
                    .map(|i| index_schema.field(*i).clone())
                    .collect::<Vec<Field>>(),
            ))
        };

        // Save some cycles inside scan nodes on projection if schema matches
        let index_projection_or_none_on_schema_match = if index_projection_schema != index_schema {
            Some(index_projection.clone())
        } else {
            None
        };

        let predicate = combine_filters(filters);
        let physical_predicate = if let Some(pred) = &predicate {
            Some(state.create_physical_expr(
                pred.clone(),
                &index_schema.as_ref().clone().to_dfschema()?,
            )?)
        } else {
            None
        };
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

                let parquet_source = ParquetSource::new(
                    TableParquetOptions::default(),
                    get_reader_options_customizer(state.config()),
                )
                .with_parquet_file_reader_factory(self.parquet_metadata_cache.clone());
                let parquet_source = if let Some(phys_pred) = &physical_predicate {
                    parquet_source.with_predicate(index_schema.clone(), phys_pred.clone())
                } else {
                    parquet_source
                };

                let file_scan = FileScanConfig::new(
                    ObjectStoreUrl::local_filesystem(),
                    index_schema.clone(),
                    Arc::new(parquet_source),
                )
                .with_file(PartitionedFile::from_path(local_path.to_string())?)
                .with_projection(index_projection_or_none_on_schema_match.clone())
                .with_output_ordering(vec![LexOrdering::new(
                    (0..key_len)
                        .map(|i| -> Result<_, DataFusionError> {
                            Ok(PhysicalSortExpr::new(
                            Arc::new(
                                datafusion::physical_expr::expressions::Column::new_with_schema(
                                    index_schema.field(i).name(),
                                    &index_schema,
                                )?,
                            ),
                            SortOptions::default(),
                        ))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )]);

                let data_source_exec = DataSourceExec::new(Arc::new(file_scan));

                let arc: Arc<dyn ExecutionPlan> = Arc::new(data_source_exec);
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
                    Arc::new(
                        MemoryExec::try_new(
                            &[record_batches.clone()],
                            index_schema.clone(),
                            index_projection_or_none_on_schema_match.clone(),
                        )?
                        .try_with_sort_information(vec![
                            LexOrdering::new(lex_ordering_for_index(
                                self.index_snapshot.index.get_row(),
                                &index_projection_schema,
                            )?),
                        ])?,
                    )
                } else {
                    let remote_path = chunk.get_row().get_full_name(chunk.get_id());
                    let local_path = self
                        .remote_to_local_names
                        .get(&remote_path)
                        .expect(format!("Missing remote path {}", remote_path).as_str());

                    let parquet_source = ParquetSource::new(
                        TableParquetOptions::default(),
                        get_reader_options_customizer(state.config()),
                    )
                    .with_parquet_file_reader_factory(self.parquet_metadata_cache.clone());
                    let parquet_source = if let Some(phys_pred) = &physical_predicate {
                        parquet_source.with_predicate(index_schema.clone(), phys_pred.clone())
                    } else {
                        parquet_source
                    };

                    let file_scan = FileScanConfig::new(ObjectStoreUrl::local_filesystem(), index_schema.clone(), Arc::new(parquet_source))
                        .with_file(PartitionedFile::from_path(local_path.to_string())?)
                        .with_projection(index_projection_or_none_on_schema_match.clone())
                        .with_output_ordering(vec![LexOrdering::new((0..key_len).map(|i| -> Result<_, DataFusionError> { Ok(PhysicalSortExpr::new(
                            Arc::new(
                                datafusion::physical_expr::expressions::Column::new_with_schema(index_schema.field(i).name(), &index_schema)?
                            ),
                            SortOptions::default(),
                        ))}).collect::<Result<Vec<_>, _>>()?)])
                        ;

                    let data_source_exec = DataSourceExec::new(Arc::new(file_scan));
                    Arc::new(data_source_exec)
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
                    .collect::<Vec<Field>>(),
            ))
        };
        // TODO: 'nullable' modifiers differ, fix this and re-enable assertion.
        // for p in &partition_execs {
        //     assert_eq!(p.schema(), projected_schema);
        // }

        if partition_execs.len() == 0 {
            partition_execs.push(Arc::new(SortExec::new(
                LexOrdering::new(lex_ordering_for_index(
                    self.index_snapshot.index.get_row(),
                    &table_projected_schema,
                )?),
                Arc::new(EmptyExec::new(table_projected_schema.clone())),
            )));
        }

        let schema = table_projected_schema;
        let partition_num = partition_execs.len();

        let read_data: Arc<dyn ExecutionPlan> = Arc::new(CubeTableExec {
            schema: schema.clone(),
            partition_execs,
            index_snapshot: self.index_snapshot.clone(),
            filter: predicate,
            properties: PlanProperties::new(
                EquivalenceProperties::new_with_orderings(
                    schema.clone(),
                    &[LexOrdering::new(lex_ordering_for_index(
                        self.index_snapshot.index.get_row(),
                        &schema,
                    )?)],
                ),
                Partitioning::UnknownPartitioning(partition_num),
                EmissionType::Both, // TODO upgrade DF
                Boundedness::Bounded,
            ),
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
                .map(|c| -> Result<_, CubeError> {
                    Ok(PhysicalSortExpr::new(
                        Arc::new(
                            datafusion::physical_plan::expressions::Column::new_with_schema(
                                c.get_name(),
                                &schema,
                            )?,
                        ),
                        SortOptions::default(),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut exec: Arc<dyn ExecutionPlan> =
                Arc::new(SortPreservingMergeExec::new(sort_columns.into(), read_data));
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
                .map(|c| -> Result<_, CubeError> {
                    Ok(PhysicalSortExpr::new(
                        Arc::new(
                            datafusion::physical_plan::expressions::Column::new_with_schema(
                                c, &schema,
                            )?,
                        ),
                        SortOptions::default(),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Arc::new(SortPreservingMergeExec::new(
                LexOrdering::new(join_columns),
                read_data,
            ))
        } else {
            Arc::new(CoalescePartitionsExec::new(read_data))
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
    properties: PlanProperties,
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

impl DisplayAs for CubeTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CubeTableExec")
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

    // TODO upgrade DF
    // fn output_partitioning(&self) -> Partitioning {
    //     Partitioning::UnknownPartitioning(self.partition_execs.len())
    // }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.partition_execs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let partition_count = children
            .iter()
            .map(|c| c.properties().partitioning.partition_count())
            .sum();
        Ok(Arc::new(CubeTableExec {
            schema: self.schema.clone(),
            partition_execs: children,
            index_snapshot: self.index_snapshot.clone(),
            filter: self.filter.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new_with_orderings(
                    self.schema.clone(),
                    &[LexOrdering::new(lex_ordering_for_index(
                        self.index_snapshot.index.get_row(),
                        &(&self.schema),
                    )?)],
                ),
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Both, // TODO upgrade DF
                Boundedness::Bounded,
            ),
        }))
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
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
        let order = sort_order.map(|order| {
            order
                .into_iter()
                .map(|col_index| {
                    PhysicalSortRequirement::from(PhysicalSortExpr::new(
                        // TODO unwrap()
                        Arc::new(
                            physical_expr::expressions::Column::new_with_schema(
                                self.schema.field(col_index).name(),
                                self.schema.as_ref(),
                            )
                            .unwrap(),
                        ),
                        SortOptions::default(),
                    ))
                })
                .collect()
        });

        (0..self.children().len()).map(|_| order.clone()).collect()
    }

    // TODO upgrade DF
    // fn output_hints(&self) -> OptimizerHints {
    //     let sort_order;
    //     if let Some(snapshot_sort_on) = self.index_snapshot.sort_on() {
    //         // Note that this returns `None` if any of the columns were not found.
    //         // This only happens on programming errors.
    //         sort_order = snapshot_sort_on
    //             .iter()
    //             .map(|c| self.schema.index_of(&c).ok())
    //             .collect()
    //     } else {
    //         let index = self.index_snapshot.index().get_row();
    //         let sort_cols = index
    //             .get_columns()
    //             .iter()
    //             .take(index.sort_key_size() as usize)
    //             .map(|sort_col| self.schema.index_of(&sort_col.get_name()).ok())
    //             .take_while(|i| i.is_some())
    //             .map(|i| i.unwrap())
    //             .collect_vec();
    //         if !sort_cols.is_empty() {
    //             sort_order = Some(sort_cols)
    //         } else {
    //             sort_order = None
    //         }
    //     }
    //
    //     OptimizerHints {
    //         sort_order,
    //         single_value_columns: Vec::new(),
    //     }
    // }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn execute(
        &self,
        mut partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let exec = self
            .partition_execs
            .iter()
            .find(|p| {
                if partition < p.properties().partitioning.partition_count() {
                    true
                } else {
                    partition -= p.properties().partitioning.partition_count();
                    false
                }
            })
            .expect(&format!(
                "CubeTableExec: Partition index is outside of partition range: {}",
                partition
            ));
        exec.execute(partition, context)
    }

    fn name(&self) -> &str {
        "CubeTableExec"
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; self.children().len()]
    }
}

// TODO upgrade DF: Make this return LexOrdering?
pub fn lex_ordering_for_index(
    index: &Index,
    schema: &SchemaRef,
) -> Result<Vec<PhysicalSortExpr>, DataFusionError> {
    (0..(index.sort_key_size() as usize))
        .map(|i| -> Result<_, _> {
            Ok(PhysicalSortExpr::new(
                Arc::new(
                    datafusion::physical_expr::expressions::Column::new_with_schema(
                        index.get_columns()[i].get_name(),
                        &schema,
                    )?,
                ),
                SortOptions::default(),
            ))
        })
        .take_while(|e| e.is_ok())
        .collect::<Result<Vec<_>, _>>()
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
    properties: PlanProperties,
    pub partitions: Vec<(
        /*node*/ String,
        (Vec<PartitionWithFilters>, Vec<InlineTableId>),
    )>,
    /// Never executed, only stored to allow consistent optimization on router and worker.
    pub input_for_optimizations: Arc<dyn ExecutionPlan>,
    pub cluster: Arc<dyn Cluster>,
    pub serialized_plan: Arc<PreSerializedPlan>,
    pub use_streaming: bool,
    // Used to prevent SortExec on workers (e.g. with ClusterAggregateTopK) from being optimized away.
    pub required_input_ordering: Option<LexRequirement>,
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
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<PreSerializedPlan>,
        union_snapshots: &[Snapshots],
        input_for_optimizations: Arc<dyn ExecutionPlan>,
        use_streaming: bool,
        required_input_ordering: Option<LexRequirement>,
    ) -> Result<Self, CubeError> {
        let partitions = Self::distribute_to_workers(
            cluster.config().as_ref(),
            union_snapshots,
            &serialized_plan.planning_meta().multi_part_subtree,
        )?;
        Ok(Self {
            properties: Self::compute_properties(
                input_for_optimizations.properties(),
                partitions.len(),
            ),
            partitions,
            cluster,
            serialized_plan,
            input_for_optimizations,
            use_streaming,
            required_input_ordering,
        })
    }

    /// Also used by WorkerExec (to produce the exact same plan properties so we get the same optimizations).
    pub fn compute_properties(
        input_properties: &PlanProperties,
        partitions_num: usize,
    ) -> PlanProperties {
        // Coalescing partitions (on the worker side) loses existing orderings:
        let mut eq_properties = input_properties.eq_properties.clone();
        if input_properties.output_partitioning().partition_count() > 1 {
            eq_properties.clear_orderings();
            eq_properties.clear_per_partition_constants();
        }
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(partitions_num),
            EmissionType::Both, // TODO upgrade DF: Actually Final, unless we implement streaming, but check if that value has implications.
            input_properties.boundedness.clone(),
        )
    }

    pub fn worker_planning_params(&self) -> WorkerPlanningParams {
        WorkerPlanningParams {
            // Or, self.partitions.len().
            worker_partition_count: self.properties().output_partitioning().partition_count(),
        }
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
        input_for_optimizations: Arc<dyn ExecutionPlan>,
        new_required_input_ordering: Option<LexRequirement>,
    ) -> Self {
        ClusterSendExec {
            properties: Self::compute_properties(
                input_for_optimizations.properties(),
                self.partitions.len(),
            ),
            partitions: self.partitions.clone(),
            cluster: self.cluster.clone(),
            serialized_plan: self.serialized_plan.clone(),
            input_for_optimizations,
            use_streaming: self.use_streaming,
            required_input_ordering: new_required_input_ordering,
        }
    }

    pub fn worker_plans(&self) -> Result<Vec<(String, PreSerializedPlan)>, CubeError> {
        let mut res = Vec::new();
        for (node_name, partitions) in self.partitions.iter() {
            res.push((
                node_name.clone(),
                self.serialized_plan_for_partitions(partitions)?,
            ));
        }
        Ok(res)
    }

    fn serialized_plan_for_partitions(
        &self,
        partitions: &(Vec<(u64, RowRange)>, Vec<InlineTableId>),
    ) -> Result<PreSerializedPlan, CubeError> {
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

impl DisplayAs for ClusterSendExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ClusterSendExec")
    }
}

#[async_trait]
impl ExecutionPlan for ClusterSendExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input_for_optimizations]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            panic!("expected exactly one input");
        }
        let input_for_optimizations = children.into_iter().next().unwrap();
        Ok(Arc::new(ClusterSendExec {
            properties: Self::compute_properties(
                input_for_optimizations.properties(),
                self.partitions.len(),
            ),
            partitions: self.partitions.clone(),
            cluster: self.cluster.clone(),
            serialized_plan: self.serialized_plan.clone(),
            input_for_optimizations,
            use_streaming: self.use_streaming,
            required_input_ordering: self.required_input_ordering.clone(),
        }))
    }

    #[instrument(level = "trace", skip(self))]
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let (node_name, partitions) = &self.partitions[partition];

        let plan = self.serialized_plan_for_partitions(partitions)?;

        let cluster = self.cluster.clone();
        let schema = self.properties.eq_properties.schema().clone();
        let node_name = node_name.to_string();
        let worker_planning_params = self.worker_planning_params();
        if self.use_streaming {
            // A future that yields a stream
            let fut = async move {
                cluster
                    .run_select_stream(
                        &node_name,
                        plan.to_serialized_plan()?,
                        worker_planning_params,
                    )
                    .await
            };
            // Use TryStreamExt::try_flatten to flatten the stream of streams
            let stream = futures::stream::once(fut).try_flatten();

            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        } else {
            let record_batches = async move {
                cluster
                    .run_select(
                        &node_name,
                        plan.to_serialized_plan()?,
                        worker_planning_params,
                    )
                    .await
            };
            let stream = futures::stream::once(record_batches).flat_map(|r| match r {
                Ok(vec) => stream::iter(vec.into_iter().map(|b| Ok(b)).collect::<Vec<_>>()),
                Err(e) => stream::iter(vec![Err(DataFusionError::Execution(e.to_string()))]),
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
    }

    fn name(&self) -> &str {
        "ClusterSendExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        vec![self.required_input_ordering.clone()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // TODO upgrade DF: If the WorkerExec has the number of partitions so it can produce the same output, we could occasionally return true.
        // vec![self.partitions.len() <= 1 && self.input_for_optimizations.output_partitioning().partition_count() <= 1]

        // For now, same as default implementation:
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // TODO:  Ensure this is obeyed... or allow worker partitions to be sent separately.
        vec![Distribution::SinglePartition; self.children().len()]
    }
}

impl fmt::Debug for ClusterSendExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "ClusterSendExec: {:?}: {:?}",
            self.properties.eq_properties.schema(),
            self.partitions
        ))
    }
}

pub fn find_topmost_cluster_send_exec(mut p: &Arc<dyn ExecutionPlan>) -> Option<&ClusterSendExec> {
    loop {
        if let Some(p) = p.as_any().downcast_ref::<ClusterSendExec>() {
            return Some(p);
        } else {
            let children = p.children();
            if children.len() != 1 {
                // There are no tree splits before ClusterSend.  (If there were, we need a new concept for this function.)
                return None;
            }
            p = children[0];
        }
    }
}

#[async_trait]
impl TableProvider for CubeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>, // TODO: propagate limit
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let res = self.async_scan(state, projection, filters)?;
        Ok(res)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[async_trait]
impl TableProvider for InlineTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.data.get_schema()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>, // TODO: propagate limit
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let projected_schema = if let Some(p) = projection {
            Arc::new(Schema::new(
                p.iter()
                    .map(|i| schema.field(*i).clone())
                    .collect::<Vec<Field>>(),
            ))
        } else {
            schema.clone()
        };

        if !self.inline_table_ids.iter().any(|id| id == &self.id) {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // TODO batch_size
        let batches = dataframe_to_batches(self.data.as_ref(), 16384)?;
        let projection = projection.cloned();
        Ok(Arc::new(MemoryExec::try_new(
            &vec![batches],
            schema.clone(),
            projection,
        )?))
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
}

macro_rules! convert_array_cast_native {
    ($V: expr, (Vec<u8>)) => {{
        $V.to_vec()
    }};
    ($V: expr, (Decimal)) => {{
        crate::util::decimal::Decimal::new($V)
    }};
    ($V: expr, (Decimal96)) => {{
        crate::util::decimal::Decimal96::new($V)
    }};
    ($V: expr, (Int96)) => {{
        crate::util::int96::Int96::new($V)
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

pub fn batches_to_dataframe(batches: Vec<RecordBatch>) -> Result<DataFrame, CubeError> {
    let mut cols = vec![];
    let mut all_rows = vec![];

    for batch in batches.into_iter() {
        if cols.len() == 0 {
            for (i, field) in batch.schema().fields().iter().enumerate() {
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

        let mut rows = Vec::with_capacity(batch.num_rows());

        for _ in 0..batch.num_rows() {
            rows.push(Row::new(Vec::with_capacity(batch.num_columns())));
        }

        for column_index in 0..batch.num_columns() {
            let array = batch.column(column_index);
            let num_rows = batch.num_rows();
            match array.data_type() {
                DataType::UInt16 => convert_array!(array, num_rows, rows, UInt16Array, Int, i64),
                DataType::UInt32 => convert_array!(array, num_rows, rows, UInt32Array, Int, i64),
                DataType::UInt64 => convert_array!(array, num_rows, rows, UInt64Array, Int, i64),
                DataType::Int16 => convert_array!(array, num_rows, rows, Int16Array, Int, i64),
                DataType::Int32 => convert_array!(array, num_rows, rows, Int32Array, Int, i64),
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
                DataType::Decimal128(_, _) => {
                    convert_array!(array, num_rows, rows, Decimal128Array, Decimal, (Decimal))
                }
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
                DataType::Timestamp(TimeUnit::Nanosecond, tz)
                    if tz.is_none() || tz.as_ref().unwrap().as_ref() == "+00:00" =>
                {
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
                DataType::Null => {
                    // Force the cast, just because.
                    let _ = array.as_any().downcast_ref::<NullArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(TableValue::Null);
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
        // TODO upgrade DF
        // DataType::Int64Decimal(scale) => Ok(ColumnType::Decimal {
        //     scale: scale as i32,
        //     precision: 18,
        // }),
        // DataType::Int96Decimal(scale) => Ok(ColumnType::Decimal {
        //     scale: scale as i32,
        //     precision: 27,
        // }),
        DataType::Decimal128(precision, scale) => Ok(ColumnType::Decimal {
            scale: scale as i32,
            precision: precision as i32,
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
        // This fn is only used for converting to DataFrame, and cubesql does this (as if that's a reason)
        DataType::Null => Ok(ColumnType::String),
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
            let mut writer = StreamWriter::try_new(Cursor::new(file), schema)?;
            writer.write(&batch)?;
            let cursor = writer.into_inner()?;
            results.push(Self {
                record_batch_file: cursor.into_inner(),
            })
        }
        Ok(results)
    }

    pub fn read(self) -> Result<RecordBatch, CubeError> {
        let cursor = Cursor::new(self.record_batch_file);
        let mut reader = StreamReader::try_new(cursor, None)?;
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
        .fold(filters[0].clone(), |acc, filter| and(acc, filter.clone()));
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
    let d = a.to_data();
    let data = vec![&d];
    let mut a = MutableArrayData::new(data, false, len);
    a.extend(0, start, start + len);
    make_array(a.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;

    #[test]
    fn test_batch_to_dataframe() -> Result<(), CubeError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("uint32", DataType::UInt32, true),
            Field::new("int32", DataType::Int32, true),
            Field::new("str32", DataType::Utf8, true),
        ]));
        let result = batches_to_dataframe(vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from_iter(vec![Some(1), None])) as ArrayRef,
                Arc::new(Int32Array::from_iter(vec![Some(1), None])) as ArrayRef,
                Arc::new(StringArray::from_iter(vec![Some("test".to_string()), None])) as ArrayRef,
            ],
        )?])?;

        assert_eq!(
            result.get_rows(),
            &vec![
                Row::new(vec![
                    TableValue::Int(1),
                    TableValue::Int(1),
                    TableValue::String("test".to_string())
                ]),
                Row::new(vec![TableValue::Null, TableValue::Null, TableValue::Null,])
            ]
        );

        Ok(())
    }
}
