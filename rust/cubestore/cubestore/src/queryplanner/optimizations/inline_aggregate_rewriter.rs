use crate::queryplanner::inline_aggregate::InlineAggregateExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Replace sorted AggregateExec node with InlineAggregateExec if possible.
///
/// This is a single-node rewriter function designed to be used with `rewrite_physical_plan`.
/// It replaces standard hash-based aggregates with a more efficient sorted aggregation
/// implementation when:
/// - Input is sorted by grouping columns (InputOrderMode::Sorted)
/// - Mode is Partial or Final
/// - No grouping sets (CUBE/ROLLUP/GROUPING SETS)
///
/// The InlineAggregateExec takes advantage of sorted input to:
/// - Avoid hash table overhead
/// - Enable streaming aggregation with bounded memory
/// - Process groups in order without buffering
pub fn replace_with_inline_aggregate(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if let Some(inline_agg) = InlineAggregateExec::try_new_from_aggregate(agg) {
            return Ok(Arc::new(inline_agg));
        }
    }

    Ok(plan)
}
