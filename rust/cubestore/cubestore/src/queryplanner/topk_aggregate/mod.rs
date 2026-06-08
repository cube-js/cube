mod topk_hash_aggregate_stream;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{Distribution, LexRequirement};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{aggregates::*, InputOrderMode};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Worker-side partial hash aggregate that trims its output to the top-k groups by a total order,
/// so far fewer partial-state rows cross the network to the router's Final aggregate.
///
/// This is a custom copy of DataFusion's partial hash aggregate (it reuses DF's `GroupValues` and
/// `GroupsAccumulator` building blocks but owns the consume/emit loop), so the only change required
/// in the DataFusion fork is making `new_group_values` public. The aggregation builds the whole
/// group table and, at the single final emit, keeps only the `k` smallest groups by `order` when
/// the number of groups exceeds `factor * k`; otherwise it emits all groups unchanged.
///
/// `order` is a TOTAL order over groups (ORDER BY columns followed by the remaining group-by
/// columns), expressed as `(partial-output column index, sort options)`. A total order is required
/// for correctness: the same group key can live on multiple workers, and a consistent cut across
/// workers guarantees every partial state the router selects reaches it.
#[derive(Debug, Clone)]
pub struct TopKHashAggregateExec {
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
    pub input: Arc<dyn ExecutionPlan>,
    /// Partial-aggregate output schema (group columns followed by accumulator state columns).
    schema: SchemaRef,
    input_schema: SchemaRef,
    cache: PlanProperties,
    /// Fetch count, `k = limit + offset`.
    k: usize,
    /// Only trim when the number of local groups exceeds `factor * k`.
    factor: usize,
    /// Total order over the partial output columns.
    order: Vec<(usize, SortOptions)>,
}

impl TopKHashAggregateExec {
    /// Build a `TopKHashAggregateExec` from a partial hash `AggregateExec`, or `None` if it is not a
    /// single-group-by partial aggregate (grouping sets and non-partial modes are not supported).
    pub fn try_new_from_partial(
        aggregate: &AggregateExec,
        k: usize,
        factor: usize,
        order: Vec<(usize, SortOptions)>,
    ) -> Option<Self> {
        if *aggregate.mode() != AggregateMode::Partial {
            return None;
        }
        // Sorted-prefix aggregates are handled by InlineAggregateExec; this targets the hash path.
        if matches!(aggregate.input_order_mode(), InputOrderMode::Sorted) {
            return None;
        }
        let group_by = aggregate.group_expr().clone();
        if !group_by.is_single() {
            return None;
        }
        Some(Self {
            group_by,
            aggr_expr: aggregate.aggr_expr().to_vec(),
            filter_expr: aggregate.filter_expr().to_vec(),
            input: aggregate.input().clone(),
            schema: aggregate.schema().clone(),
            input_schema: aggregate.input_schema().clone(),
            cache: aggregate.cache().clone(),
            k,
            factor,
            order,
        })
    }

    pub fn k(&self) -> usize {
        self.k
    }

    pub fn factor(&self) -> usize {
        self.factor
    }

    pub fn order(&self) -> &[(usize, SortOptions)] {
        &self.order
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

impl DisplayAs for TopKHashAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "TopKHashAggregateExec: k={}, factor={}, order={:?}",
                    self.k, self.factor, self.order
                )?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for TopKHashAggregateExec {
    fn name(&self) -> &'static str {
        "TopKHashAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        vec![None]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            group_by: self.group_by.clone(),
            aggr_expr: self.aggr_expr.clone(),
            filter_expr: self.filter_expr.clone(),
            input: children[0].clone(),
            schema: self.schema.clone(),
            input_schema: self.input_schema.clone(),
            cache: self.cache.clone(),
            k: self.k,
            factor: self.factor,
            order: self.order.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream =
            topk_hash_aggregate_stream::TopKHashAggregateStream::new(self, context, partition)?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics {
            num_rows: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.schema),
            total_byte_size: Precision::Absent,
        })
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}
