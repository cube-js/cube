mod column_comparator;
mod inline_aggregate_stream;
mod sorted_group_values;
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
    UInt8Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{Statistics, ToDFSchema};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::get_reader_options_customizer;
use datafusion::datasource::physical_plan::{
    FileScanConfig, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::dfschema::{internal_err, not_impl_err};
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexRequirement, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
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
use datafusion::physical_plan::execution_plan::{Boundedness, CardinalityEffect, EmissionType};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{aggregates::*, InputOrderMode};
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::{and, SessionConfig, SessionContext};
use datafusion_datasource::memory::MemorySourceConfig;
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InlineAggregateMode {
    Partial,
    Final,
}

#[derive(Debug, Clone)]
pub struct InlineAggregateExec {
    mode: InlineAggregateMode,
    /// Group by expressions
    group_by: PhysicalGroupBy,
    /// Aggregate expressions
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    /// FILTER (WHERE clause) expression for each aggregate expression
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// Set if the output of this aggregation is truncated by a upstream sort/limit clause
    limit: Option<usize>,
    /// Input plan, could be a partial aggregate or the input to the aggregate
    pub input: Arc<dyn ExecutionPlan>,
    /// Schema after the aggregate is applied
    schema: SchemaRef,
    /// Input schema before any aggregation is applied. For partial aggregate this will be the
    /// same as input.schema() but for the final aggregate it will be the same as the input
    /// to the partial aggregate, i.e., partial and final aggregates have same `input_schema`.
    /// We need the input schema of partial aggregate to be able to deserialize aggregate
    /// expressions from protobuf for final aggregate.
    pub input_schema: SchemaRef,
    cache: PlanProperties,
    required_input_ordering: Vec<Option<LexRequirement>>,
}

impl InlineAggregateExec {
    /// Try to create an InlineAggregateExec from a standard AggregateExec.
    ///
    /// Returns None if the aggregate cannot be converted (e.g., not sorted, uses grouping sets).
    pub fn try_new_from_aggregate(aggregate: &AggregateExec) -> Option<Self> {
        // Only convert Sorted aggregates
        if !matches!(aggregate.input_order_mode(), InputOrderMode::Sorted) {
            return None;
        }

        // Only support Partial and Final modes
        let mode = match aggregate.mode() {
            AggregateMode::Partial => InlineAggregateMode::Partial,
            AggregateMode::Final => InlineAggregateMode::Final,
            _ => return None,
        };

        let group_by = aggregate.group_expr().clone();

        // InlineAggregate doesn't support grouping sets (CUBE/ROLLUP/GROUPING SETS)
        if !group_by.is_single() {
            return None;
        }

        let aggr_expr = aggregate.aggr_expr().iter().cloned().collect();
        let filter_expr = aggregate.filter_expr().iter().cloned().collect();
        let limit = aggregate.limit().clone();
        let input = aggregate.input().clone();
        let schema = aggregate.schema().clone();
        let input_schema = aggregate.input_schema().clone();
        let cache = aggregate.cache().clone();
        let required_input_ordering = aggregate.required_input_ordering().clone();

        Some(Self {
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            limit,
            input,
            schema,
            input_schema,
            cache,
            required_input_ordering,
        })
    }

    pub fn mode(&self) -> &InlineAggregateMode {
        &self.mode
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn aggr_expr(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggr_expr
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for InlineAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InlineAggregateExec: mode={:?}", self.mode)?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for InlineAggregateExec {
    fn name(&self) -> &'static str {
        "InlineAggregateExec"
    }

    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match &self.mode {
            InlineAggregateMode::Partial => {
                vec![Distribution::UnspecifiedDistribution]
            }
            InlineAggregateMode::Final => {
                vec![Distribution::SinglePartition]
            }
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        self.required_input_ordering.clone()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let result = Self {
            mode: self.mode,
            group_by: self.group_by.clone(),
            aggr_expr: self.aggr_expr.clone(),
            filter_expr: self.filter_expr.clone(),
            limit: self.limit.clone(),
            input: children[0].clone(),
            schema: self.schema.clone(),
            input_schema: self.input_schema.clone(),
            cache: self.cache.clone(),
            required_input_ordering: self.required_input_ordering.clone(),
        };
        Ok(Arc::new(result))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream = inline_aggregate_stream::InlineAggregateStream::new(self, context, partition)?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> DFResult<Statistics> {
        let column_statistics = Statistics::unknown_column(&self.schema());
        // When the input row count is 0 or 1, we can adopt that statistic keeping its reliability.
        // When it is larger than 1, we degrade the precision since it may decrease after aggregation.
        let num_rows = if let Some(value) = self.input().statistics()?.num_rows.get_value() {
            if *value > 1 {
                self.input().statistics()?.num_rows.to_inexact()
            } else if *value == 0 {
                // Aggregation on an empty table creates a null row.
                self.input()
                    .statistics()?
                    .num_rows
                    .add(&Precision::Exact(1))
            } else {
                // num_rows = 1 case
                self.input().statistics()?.num_rows
            }
        } else {
            Precision::Absent
        };
        Ok(Statistics {
            num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        })
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}
