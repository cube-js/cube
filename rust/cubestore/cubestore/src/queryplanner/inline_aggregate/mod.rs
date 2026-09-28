mod column_comparator;
mod inline_aggregate_stream;
mod sorted_group_values;
mod sorted_group_values_rows;

pub use sorted_group_values::SortedGroupValues;
pub use sorted_group_values_rows::SortedGroupValuesRows;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{Distribution, LexRequirement};
use datafusion::physical_plan::aggregates::group_values::GroupValues;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{aggregates::*, InputOrderMode};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InlineAggregateMode {
    Partial,
    Final,
}

#[derive(Clone)]
pub struct InlineAggregateExec {
    mode: InlineAggregateMode,
    /// Group by expressions
    group_by: PhysicalGroupBy,
    /// Aggregate expressions
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    /// FILTER (WHERE clause) expression for each aggregate expression
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// Per-partition limit on the number of emitted groups: each partition emits at most this
    /// many first (in group order) complete groups and stops reading its input
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
    /// The aggregate this node replaced. Kept so that swapping the input can be delegated to it:
    /// DataFusion recomputes the plan properties there and preserves the output schema, which it
    /// derives from aggregate expression names that its own rules are free to rewrite.
    source: Arc<AggregateExec>,
}

/// Skips [InlineAggregateExec::source]: it carries a copy of the same input subtree the node
/// already prints, so deriving would double the output of every nested aggregate.
impl Debug for InlineAggregateExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InlineAggregateExec")
            .field("mode", &self.mode)
            .field("group_by", &self.group_by)
            .field("aggr_expr", &self.aggr_expr)
            .field("filter_expr", &self.filter_expr)
            .field("limit", &self.limit)
            .field("input", &self.input)
            .field("schema", &self.schema)
            .field("input_schema", &self.input_schema)
            .field("cache", &self.cache)
            .field("required_input_ordering", &self.required_input_ordering)
            .finish_non_exhaustive()
    }
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
            source: Arc::new(aggregate.clone()),
        })
    }

    pub fn mode(&self) -> &InlineAggregateMode {
        &self.mode
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Returns a copy of this aggregate with the per-partition group limit set. Each partition
    /// emits at most `limit` first (in group order) complete groups and stops reading its input.
    ///
    /// Plan properties and statistics are intentionally not narrowed by the limit: the limit is
    /// set after all the optimizers that could consume them have run.
    pub fn with_limit(&self, limit: Option<usize>) -> Self {
        let mut result = self.clone();
        result.limit = limit;
        result
    }

    pub fn aggr_expr(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggr_expr
    }

    pub fn filter_expr(&self) -> &[Option<Arc<dyn PhysicalExpr>>] {
        &self.filter_expr
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn group_expr(&self) -> &PhysicalGroupBy {
        &self.group_by
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
        // Swapping the input is delegated to the aggregate this node was built from, so that
        // DataFusion recomputes the plan properties -- output partitioning above all. Carrying
        // them over leaves a stale partition count, and the parent then executes only that many
        // of the input's partitions and silently drops the rows of the rest.
        let rebuilt = Arc::clone(&self.source).with_new_children(children)?;
        let Some(aggregate) = rebuilt.as_any().downcast_ref::<AggregateExec>() else {
            return Err(DataFusionError::Internal(format!(
                "AggregateExec::with_new_children returned {}",
                rebuilt.name()
            )));
        };

        // The streaming implementation is only valid while the new input stays sorted on the
        // group keys; if it no longer is, keep the plain hash aggregate. The limit stays behind
        // with it: here it counts complete groups off a sorted input, while a hash aggregate
        // reads it as a cap on distinct groups and would stop mid-way through arbitrary ones.
        // Dropping it only costs the early exit.
        Ok(match Self::try_new_from_aggregate(aggregate) {
            Some(inline) => Arc::new(inline.with_limit(self.limit)),
            None => {
                // No pass in the pipeline is known to de-sort an inline aggregate's input, and a
                // parent merge built around the old ordering would not notice the switch, so say
                // it out loud rather than degrade quietly.
                log::warn!(
                    "Input of an inline {:?} aggregate is no longer sorted on the group keys, \
                     falling back to a hash aggregate. Group by [{}], new input order mode {:?}, \
                     new input ordering {:?}",
                    self.mode,
                    self.group_by
                        .expr()
                        .iter()
                        .map(|(_, name)| name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    aggregate.input_order_mode(),
                    aggregate.input().properties().output_ordering()
                );
                Arc::new(aggregate.clone().with_limit(None))
            }
        })
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

/// Creates a new [`GroupValues`] implementation optimized for sorted input data
///
/// Chooses between:
/// - [`SortedGroupValues`]: Fast column-based implementation for supported types
/// - [`SortedGroupValuesRows`]: Row-based fallback for all other types (Boolean, Struct, List, etc.)
pub fn new_sorted_group_values(schema: SchemaRef) -> DFResult<Box<dyn GroupValues>> {
    // Check if all fields are supported by the column-based implementation
    if supported_schema(schema.as_ref()) {
        Ok(Box::new(SortedGroupValues::try_new(schema)?))
    } else {
        Ok(Box::new(SortedGroupValuesRows::try_new(schema)?))
    }
}

/// Returns true if the schema is supported by [`SortedGroupValues`] (column-based implementation)
fn supported_schema(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    schema
        .fields()
        .iter()
        .map(|f| f.data_type())
        .all(supported_type)
}

/// Returns true if the data type is supported by [`SortedGroupValues`]
///
/// Types not in this list will use the row-based [`SortedGroupValuesRows`] implementation
fn supported_type(data_type: &DataType) -> bool {
    matches!(
        *data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8View
            | DataType::BinaryView
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::arrow::compute::concat_batches;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn make_batch(schema: &SchemaRef, rows: &[(i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            ],
        )
        .unwrap()
    }

    fn sorted_source(
        schema: &SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Arc<dyn ExecutionPlan> {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("k", schema).unwrap(),
        )]);
        let source = MemorySourceConfig::try_new(&partitions, schema.clone(), None)
            .unwrap()
            .try_with_sort_information(vec![ordering])
            .unwrap();
        Arc::new(DataSourceExec::new(Arc::new(source)))
    }

    fn partial_sum_inline_aggregate(
        input: Arc<dyn ExecutionPlan>,
        limit: Option<usize>,
    ) -> Arc<InlineAggregateExec> {
        let schema = input.schema();
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("k", &schema).unwrap(), "k".to_string())]);
        let sum = AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema).unwrap()])
            .schema(schema.clone())
            .alias("sum_v")
            .build()
            .unwrap();
        let agg = AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![Arc::new(sum)],
            vec![None],
            input,
            schema,
        )
        .unwrap();
        assert!(
            matches!(agg.input_order_mode(), InputOrderMode::Sorted),
            "test setup must produce a sorted aggregate"
        );
        let inline = InlineAggregateExec::try_new_from_aggregate(&agg).unwrap();
        Arc::new(inline.with_limit(limit))
    }

    fn run(plan: Arc<dyn ExecutionPlan>, batch_size: usize) -> Vec<(i64, i64)> {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_batch_size(batch_size));
        let batches = futures::executor::block_on(collect(plan, session.task_ctx())).unwrap();
        let schema = batches[0].schema();
        let batch = concat_batches(&schema, &batches).unwrap();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        keys.iter()
            .zip(sums.iter())
            .map(|(k, s)| (k.unwrap(), s.unwrap()))
            .collect()
    }

    /// A group continuing in the next input batch must not be emitted early with a partial sum.
    /// A partial aggregate runs once per input partition, so its reported partitioning must
    /// follow a re-childed input. A stale single-partition count makes the parent coalesce
    /// execute only partition 0 and silently drop the rows of the rest -- the row loss this node
    /// was fixed for. Built over a 1-partition input so its cache says 1, then re-childed onto 3.
    #[test]
    fn output_partitioning_follows_rechilded_input() {
        let schema = test_schema();
        let one = sorted_source(
            &schema,
            vec![vec![make_batch(&schema, &[(1, 10), (2, 20)])]],
        );
        let exec = partial_sum_inline_aggregate(one, None);
        assert_eq!(exec.properties().output_partitioning().partition_count(), 1);

        let three: Vec<Vec<RecordBatch>> = (0..3)
            .map(|i| vec![make_batch(&schema, &[(i, 10), (i + 10, 20)])])
            .collect();
        let rechilded = exec
            .with_new_children(vec![sorted_source(&schema, three)])
            .unwrap();

        assert!(
            rechilded.as_any().is::<InlineAggregateExec>(),
            "re-childing a still-sorted input must keep the streaming exec"
        );
        assert_eq!(
            rechilded
                .properties()
                .output_partitioning()
                .partition_count(),
            3,
            "exec must report its re-childed input's partition count, not a stale 1"
        );
    }

    #[test]
    fn limit_emits_only_closed_groups() {
        let schema = test_schema();
        let input = sorted_source(
            &schema,
            vec![vec![
                make_batch(&schema, &[(1, 10), (2, 20), (3, 30), (3, 31)]),
                make_batch(&schema, &[(3, 32), (4, 40)]),
            ]],
        );
        let agg = partial_sum_inline_aggregate(input, Some(3));
        assert_eq!(run(agg, 4096), vec![(1, 10), (2, 20), (3, 93)]);
    }

    #[test]
    fn limit_results_match_no_limit_prefix() {
        let schema = test_schema();
        let rows: Vec<(i64, i64)> = (0..1000).map(|i| (i / 3, i)).collect();
        let batches: Vec<RecordBatch> = rows.chunks(97).map(|c| make_batch(&schema, c)).collect();
        let source = sorted_source(&schema, vec![batches]);

        let no_limit = run(partial_sum_inline_aggregate(source.clone(), None), 4096);
        let limited = run(partial_sum_inline_aggregate(source, Some(5)), 4096);
        assert_eq!(limited, no_limit[..5]);
    }

    #[test]
    fn limit_larger_than_group_count_emits_all() {
        let schema = test_schema();
        let input = sorted_source(
            &schema,
            vec![vec![make_batch(&schema, &[(1, 10), (2, 20), (3, 30)])]],
        );
        let agg = partial_sum_inline_aggregate(input, Some(100));
        assert_eq!(run(agg, 4096), vec![(1, 10), (2, 20), (3, 30)]);
    }

    /// Emitting in batch_size chunks until the limit is reached.
    #[test]
    fn limit_above_batch_size_emits_incrementally() {
        let schema = test_schema();
        let rows: Vec<(i64, i64)> = (0..16).map(|i| (i / 2, i)).collect();
        let batches: Vec<RecordBatch> = rows.chunks(3).map(|c| make_batch(&schema, c)).collect();
        let source = sorted_source(&schema, vec![batches]);

        let no_limit = run(partial_sum_inline_aggregate(source.clone(), None), 2);
        let limited = run(partial_sum_inline_aggregate(source, Some(5)), 2);
        assert_eq!(limited, no_limit[..5]);
    }

    /// A single input batch can bring more groups than the limit; the stream must still emit
    /// exactly `limit` groups, draining the backlog in emit threshold chunks.
    #[test]
    fn limit_holds_when_one_batch_overshoots_it() {
        let schema = test_schema();
        let rows: Vec<(i64, i64)> = (0..20).map(|k| (k, k + 100)).collect();
        let input = sorted_source(&schema, vec![vec![make_batch(&schema, &rows)]]);
        let agg = partial_sum_inline_aggregate(input, Some(10));
        assert_eq!(
            run(agg, 4),
            (0..10).map(|k| (k, k + 100)).collect::<Vec<_>>()
        );
    }

    /// Wraps a plan and counts batches its streams produce.
    #[derive(Debug)]
    struct CountingExec {
        inner: Arc<dyn ExecutionPlan>,
        batches_polled: Arc<AtomicUsize>,
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "CountingExec")
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            self.inner.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(CountingExec {
                inner: children[0].clone(),
                batches_polled: self.batches_polled.clone(),
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> DFResult<SendableRecordBatchStream> {
            let stream = self.inner.execute(partition, context)?;
            let counter = self.batches_polled.clone();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                stream.schema(),
                stream.inspect(move |_| {
                    counter.fetch_add(1, Ordering::SeqCst);
                }),
            )))
        }
    }

    /// Once the limit is reached the aggregate must stop reading its input, so a downstream
    /// LIMIT short-circuits the scan.
    #[test]
    fn limit_stops_reading_input() {
        let schema = test_schema();
        let batches: Vec<RecordBatch> = (0..100)
            .map(|i| {
                let rows: Vec<(i64, i64)> = (0..10).map(|j| (i * 10 + j, 1)).collect();
                make_batch(&schema, &rows)
            })
            .collect();
        let source = sorted_source(&schema, vec![batches]);
        let batches_polled = Arc::new(AtomicUsize::new(0));
        let counting = Arc::new(CountingExec {
            inner: source,
            batches_polled: batches_polled.clone(),
        });

        let result = run(partial_sum_inline_aggregate(counting, Some(5)), 4096);
        assert_eq!(result.len(), 5);
        assert!(
            batches_polled.load(Ordering::SeqCst) < 10,
            "aggregate must stop polling input after the limit is reached, polled {} batches",
            batches_polled.load(Ordering::SeqCst)
        );
    }

    /// When one batch brings enough groups to cover the limit, the stream must drain them
    /// without reading further input.
    #[test]
    fn limit_drains_backlog_without_reading_input() {
        let schema = test_schema();
        let batches: Vec<RecordBatch> = (0..100)
            .map(|i| {
                let rows: Vec<(i64, i64)> = (0..20).map(|j| (i * 20 + j, 1)).collect();
                make_batch(&schema, &rows)
            })
            .collect();
        let source = sorted_source(&schema, vec![batches]);
        let batches_polled = Arc::new(AtomicUsize::new(0));
        let counting = Arc::new(CountingExec {
            inner: source,
            batches_polled: batches_polled.clone(),
        });

        // The first batch alone brings 20 groups > limit 10
        let result = run(partial_sum_inline_aggregate(counting, Some(10)), 4);
        assert_eq!(result.len(), 10);
        assert_eq!(
            batches_polled.load(Ordering::SeqCst),
            1,
            "the first input batch covers the limit, no further reads needed"
        );
    }
}
