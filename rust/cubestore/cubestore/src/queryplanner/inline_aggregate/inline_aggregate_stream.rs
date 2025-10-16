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
use datafusion::arrow::array::AsArray;
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
use datafusion::common::ToDFSchema;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::get_reader_options_customizer;
use datafusion::datasource::physical_plan::{
    FileScanConfig, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::dfschema::internal_err;
use datafusion::dfschema::not_impl_err;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, GroupsAccumulator, LogicalPlan};
use datafusion::physical_expr::expressions::Column as DFColumn;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::{self, GroupsAccumulatorAdapter};
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
use datafusion::physical_plan::aggregates::*;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::{and, SessionConfig, SessionContext};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use futures::ready;
use futures::{
    stream::{Stream, StreamExt},
    Future,
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
use std::task::{Context, Poll};
use std::time::SystemTime;
use tarpc::context::current;
use tracing::{instrument, Instrument};

use super::InlineAggregateExec;
use super::InlineAggregateMode;

#[derive(Debug, Clone)]
pub(crate) enum ExecutionState {
    ReadingInput,
    ProducingOutput(RecordBatch),
    Done,
}

pub(crate) struct InlineAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: InlineAggregateMode,

    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,

    group_by: PhysicalGroupBy,

    batch_size: usize,

    exec_state: ExecutionState,

    input_done: bool,

    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    current_group_indices: Vec<usize>,
}

impl InlineAggregateStream {
    pub fn new(
        agg: &InlineAggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> DFResult<Self> {
        let agg_schema = Arc::clone(&agg.schema);
        let agg_group_by = agg.group_by.clone();
        let agg_filter_expr = agg.filter_expr.clone();

        let batch_size = context.session_config().batch_size();
        let input = agg.input.execute(partition, Arc::clone(&context))?;

        let aggregate_exprs = agg.aggr_expr.clone();

        // arguments for each aggregate, one vec of expressions per
        // aggregate
        let aggregate_arguments =
            aggregate_expressions(&agg.aggr_expr, &agg.mode, agg_group_by.num_group_exprs())?;
        // arguments for aggregating spilled data is the same as the one for final aggregation
        let merging_aggregate_arguments = aggregate_expressions(
            &agg.aggr_expr,
            &InlineAggregateMode::Final,
            agg_group_by.num_group_exprs(),
        )?;

        let filter_expressions = match agg.mode {
            InlineAggregateMode::Partial => agg_filter_expr,
            InlineAggregateMode::Final => {
                vec![None; agg.aggr_expr.len()]
            }
        };

        let accumulators: Vec<_> = aggregate_exprs
            .iter()
            .map(create_group_accumulator)
            .collect::<DFResult<_>>()?;

        let group_schema = agg_group_by.group_schema(&agg.input().schema())?;

        let partial_agg_schema = create_schema(
            &agg.input().schema(),
            &agg_group_by,
            &aggregate_exprs,
            InlineAggregateMode::Partial,
        )?;

        let partial_agg_schema = Arc::new(partial_agg_schema);

        let exec_state = ExecutionState::ReadingInput;
        let current_group_indices = Vec::with_capacity(batch_size);

        Ok(InlineAggregateStream {
            schema: agg_schema,
            input,
            mode: agg.mode,
            accumulators,
            aggregate_arguments,
            filter_expressions,
            group_by: agg_group_by,
            exec_state,
            batch_size,
            current_group_indices,
            input_done: false,
        })
    }
}

fn create_schema(
    input_schema: &Schema,
    group_by: &PhysicalGroupBy,
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    mode: InlineAggregateMode,
) -> DFResult<Schema> {
    let mut fields = Vec::with_capacity(group_by.num_output_exprs() + aggr_expr.len());
    fields.extend(group_by.output_fields(input_schema)?);

    match mode {
        InlineAggregateMode::Partial => {
            // in partial mode, the fields of the accumulator's state
            for expr in aggr_expr {
                fields.extend(expr.state_fields()?.iter().cloned());
            }
        }
        InlineAggregateMode::Final => {
            // in final mode, the field with the final result of the accumulator
            for expr in aggr_expr {
                fields.push(expr.field())
            }
        }
    }

    Ok(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ))
}

fn aggregate_expressions(
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    mode: &InlineAggregateMode,
    col_idx_base: usize,
) -> DFResult<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    match mode {
        InlineAggregateMode::Partial => Ok(aggr_expr
            .iter()
            .map(|agg| {
                let mut result = agg.expressions();
                // Append ordering requirements to expressions' results. This
                // way order sensitive aggregators can satisfy requirement
                // themselves.
                if let Some(ordering_req) = agg.order_bys() {
                    result.extend(ordering_req.iter().map(|item| Arc::clone(&item.expr)));
                }
                result
            })
            .collect()),
        InlineAggregateMode::Final => {
            let mut col_idx_base = col_idx_base;
            aggr_expr
                .iter()
                .map(|agg| {
                    let exprs = merge_expressions(col_idx_base, agg)?;
                    col_idx_base += exprs.len();
                    Ok(exprs)
                })
                .collect()
        }
    }
}

fn merge_expressions(
    index_base: usize,
    expr: &AggregateFunctionExpr,
) -> DFResult<Vec<Arc<dyn PhysicalExpr>>> {
    expr.state_fields().map(|fields| {
        fields
            .iter()
            .enumerate()
            .map(|(idx, f)| Arc::new(DFColumn::new(f.name(), index_base + idx)) as _)
            .collect()
    })
}

pub(crate) fn create_group_accumulator(
    agg_expr: &Arc<AggregateFunctionExpr>,
) -> DFResult<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator()
    } else {
        let agg_expr_captured = Arc::clone(agg_expr);
        let factory = move || agg_expr_captured.create_accumulator();
        Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
    }
}

impl Stream for InlineAggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match &self.exec_state {
                ExecutionState::ReadingInput => 'reading_input: {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        // New batch to aggregate in partial aggregation operator
                        Some(Ok(batch)) if self.mode == InlineAggregateMode::Partial => {
                            /* let timer = elapsed_compute.timer();
                            let input_rows = batch.num_rows();

                            // Do the grouping
                            self.group_aggregate_batch(batch)?;

                            self.update_skip_aggregation_probe(input_rows);

                            // If we can begin emitting rows, do so,
                            // otherwise keep consuming input
                            assert!(!self.input_done);

                            // If the number of group values equals or exceeds the soft limit,
                            // emit all groups and switch to producing output
                            if self.hit_soft_group_limit() {
                                timer.done();
                                self.set_input_done_and_produce_output()?;
                                // make sure the exec_state just set is not overwritten below
                                break 'reading_input;
                            }

                            if let Some(to_emit) = self.group_ordering.emit_to() {
                                timer.done();
                                if let Some(batch) = self.emit(to_emit, false)? {

                                        ExecutionState::ProducingOutput(batch);
                                };
                                // make sure the exec_state just set is not overwritten below
                                break 'reading_input;
                            }

                            self.emit_early_if_necessary()?;

                            self.switch_to_skip_aggregation()?;

                            timer.done(); */
                            todo!()
                        }

                        // New batch to aggregate in terminal aggregation operator
                        // (Final/FinalPartitioned/Single/SinglePartitioned)
                        Some(Ok(batch)) => {
                            /* let timer = elapsed_compute.timer();

                            // Make sure we have enough capacity for `batch`, otherwise spill
                            self.spill_previous_if_necessary(&batch)?;

                            // Do the grouping


                            // If we can begin emitting rows, do so,
                            // otherwise keep consuming input
                            assert!(!self.input_done);

                            // If the number of group values equals or exceeds the soft limit,
                            // emit all groups and switch to producing output
                            if self.hit_soft_group_limit() {
                                timer.done();
                                self.set_input_done_and_produce_output()?;
                                // make sure the exec_state just set is not overwritten below
                                break 'reading_input;
                            }

                            if let Some(to_emit) = self.group_ordering.emit_to() {
                                timer.done();
                                if let Some(batch) = self.emit(to_emit, false)? {
                                    self.exec_state =
                                        ExecutionState::ProducingOutput(batch);
                                };
                                // make sure the exec_state just set is not overwritten below
                                break 'reading_input;
                            }

                            timer.done(); */
                            todo!()
                        }

                        // Found error from input stream
                        Some(Err(e)) => {
                            // inner had error, return to caller
                            return Poll::Ready(Some(Err(e)));
                        }

                        // Found end from input stream
                        None => {
                            // inner is done, emit all rows and switch to producing output
                            //self.set_input_done_and_produce_output()?;
                            todo!()
                        }
                    }
                }

                ExecutionState::ProducingOutput(batch) => {
                    // slice off a part of the batch, if needed
                    /* let output_batch;
                    let size = self.batch_size;
                    (self.exec_state, output_batch) = if batch.num_rows() <= size {
                        (
                            if self.input_done {
                                ExecutionState::Done
                            }
                            // In Partial aggregation, we also need to check
                            // if we should trigger partial skipping
                            else if self.mode == AggregateMode::Partial
                                && self.should_skip_aggregation()
                            {
                                ExecutionState::SkippingAggregation
                            } else {
                                ExecutionState::ReadingInput
                            },
                            batch.clone(),
                        )
                    } else {
                        // output first batch_size rows
                        let size = self.batch_size;
                        let num_remaining = batch.num_rows() - size;
                        let remaining = batch.slice(size, num_remaining);
                        let output = batch.slice(0, size);
                        (ExecutionState::ProducingOutput(remaining), output)
                    };
                    // Empty record batches should not be emitted.
                    // They need to be treated as  [`Option<RecordBatch>`]es and handled separately
                    debug_assert!(output_batch.num_rows() > 0);
                    return Poll::Ready(Some(Ok(
                        output_batch.record_output(&self.baseline_metrics)
                    ))); */
                    todo!()
                }

                ExecutionState::Done => {
                    // release the memory reservation since sending back output batch itself needs
                    // some memory reservation, so make some room for it.
                    /* self.clear_all();
                    let _ = self.update_memory_reservation(); */
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl InlineAggregateStream {
    fn group_aggregate_batch(&mut self, batch: RecordBatch) -> DFResult<()> {
        // Evaluate the grouping expressions
        /* let group_by_values = evaluate_group_by(&self.group_by, &batch)?;

        // Evaluate the aggregation expressions.
        let input_values = evaluate_many(&self.aggregate_arguments, &batch)?;

        // Evaluate the filter expressions, if any, against the inputs
        let filter_values = evaluate_optional(&self.filter_expressions, &batch)?;

        for group_values in &group_by_values {
            // calculate the group indices for each input row
            let starting_num_groups = self.group_values.len();
            self.group_values
                .intern(group_values, &mut self.current_group_indices)?;
            let group_indices = &self.current_group_indices;

            // Update ordering information if necessary
            /* let total_num_groups = self.group_values.len();
            if total_num_groups > starting_num_groups {
                self.group_ordering
                    .new_groups(group_values, group_indices, total_num_groups)?;
            } */

            // Gather the inputs to call the actual accumulator
            let t = self
                .accumulators
                .iter_mut()
                .zip(input_values.iter())
                .zip(filter_values.iter());

            for ((acc, values), opt_filter) in t {
                let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());

                // Call the appropriate method on each aggregator with
                // the entire input row and the relevant group indexes
                match self.mode {
                    InlineAggregateMode::Partial => {
                        acc.update_batch(values, group_indices, opt_filter, total_num_groups)?;
                    }
                    _ => {
                        if opt_filter.is_some() {
                            return internal_err!("aggregate filter should be applied in partial stage, there should be no filter in final stage");
                        }

                        // if aggregation is over intermediate states,
                        // use merge
                        acc.merge_batch(values, group_indices, None, total_num_groups)?;
                    }
                }
            }
        } */
        Ok(())
    }
}

/// Evaluates expressions against a record batch.
fn evaluate(expr: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> DFResult<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| {
            expr.evaluate(batch)
                .and_then(|v| v.into_array(batch.num_rows()))
        })
        .collect()
}

/// Evaluates expressions against a record batch.
fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> DFResult<Vec<Vec<ArrayRef>>> {
    expr.iter().map(|expr| evaluate(expr, batch)).collect()
}

fn evaluate_optional(
    expr: &[Option<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> DFResult<Vec<Option<ArrayRef>>> {
    expr.iter()
        .map(|expr| {
            expr.as_ref()
                .map(|expr| {
                    expr.evaluate(batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .transpose()
        })
        .collect()
}

fn group_id_array(group: &[bool], batch: &RecordBatch) -> DFResult<ArrayRef> {
    if group.len() > 64 {
        return not_impl_err!("Grouping sets with more than 64 columns are not supported");
    }
    let group_id = group.iter().fold(0u64, |acc, &is_null| {
        (acc << 1) | if is_null { 1 } else { 0 }
    });
    let num_rows = batch.num_rows();
    if group.len() <= 8 {
        Ok(Arc::new(UInt8Array::from(vec![group_id as u8; num_rows])))
    } else if group.len() <= 16 {
        Ok(Arc::new(UInt16Array::from(vec![group_id as u16; num_rows])))
    } else if group.len() <= 32 {
        Ok(Arc::new(UInt32Array::from(vec![group_id as u32; num_rows])))
    } else {
        Ok(Arc::new(UInt64Array::from(vec![group_id; num_rows])))
    }
}

/// Evaluate a group by expression against a `RecordBatch`
///
/// Arguments:
/// - `group_by`: the expression to evaluate
/// - `batch`: the `RecordBatch` to evaluate against
///
/// Returns: A Vec of Vecs of Array of results
/// The outer Vec appears to be for grouping sets
/// The inner Vec contains the results per expression
/// The inner-inner Array contains the results per row
fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> DFResult<Vec<Vec<ArrayRef>>> {
    let exprs: Vec<ArrayRef> = group_by
        .expr()
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            value.into_array(batch.num_rows())
        })
        .collect::<DFResult<Vec<_>>>()?;

    let null_exprs: Vec<ArrayRef> = group_by
        .null_expr()
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            value.into_array(batch.num_rows())
        })
        .collect::<DFResult<Vec<_>>>()?;

    group_by
        .groups()
        .iter()
        .map(|group| {
            let mut group_values = Vec::with_capacity(group_by.num_group_exprs());
            group_values.extend(group.iter().enumerate().map(|(idx, is_null)| {
                if *is_null {
                    Arc::clone(&null_exprs[idx])
                } else {
                    Arc::clone(&exprs[idx])
                }
            }));
            if !group_by.is_single() {
                group_values.push(group_id_array(group, batch)?);
            }
            Ok(group_values)
        })
        .collect()
}
