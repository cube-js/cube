use crate::queryplanner::planning::WorkerExec;
use crate::queryplanner::query_executor::ClusterSendExec;
use datafusion::arrow::compute::SortOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

// Attempts to replace hash aggregate with sorted aggregate.

// TODO upgrade DF
// TODO: we should pick the right index.
// pub fn try_switch_to_inplace_aggregates(
//     p: Arc<dyn ExecutionPlan>,
// ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
//     let agg;
//     if let Some(a) = p.as_any().downcast_ref::<AggregateExec>() {
//         agg = a;
//     } else {
//         return Ok(p);
//     }
//     if agg.strategy() != AggregateStrategy::Hash || agg.group_expr().len() == 0 {
//         return Ok(p);
//     }
//     // Try to cheaply rearrange the plan so that it produces sorted inputs.
//     let new_input = try_regroup_columns(agg.input().clone())?;
//
//     let (strategy, order) = compute_aggregation_strategy(new_input.as_ref(), agg.group_expr());
//     if strategy != AggregateStrategy::InplaceSorted {
//         return Ok(p);
//     }
//     Ok(Arc::new(HashAggregateExec::try_new(
//         AggregateStrategy::InplaceSorted,
//         order,
//         *agg.mode(),
//         agg.group_expr().into(),
//         agg.aggr_expr().into(),
//         new_input,
//         agg.input_schema().clone(),
//     )?))
// }

// Attempts to provide **some** grouping in the results, but no particular one is guaranteed.

// TODO upgrade DF -- can we remove it?
pub fn try_regroup_columns(
    p: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    if p.as_any().is::<AggregateExec>() {
        return Ok(p);
    }
    if p.as_any().is::<UnionExec>()
        || p.as_any().is::<ProjectionExec>()
        || p.as_any().is::<FilterExec>()
        || p.as_any().is::<WorkerExec>()
        || p.as_any().is::<ClusterSendExec>()
    {
        let new_children = p
            .children()
            .into_iter()
            .map(|c| try_regroup_columns(c.clone()))
            .collect::<Result<_, DataFusionError>>()?;
        return p.with_new_children(new_children);
    }

    let merge;
    if let Some(m) = p.as_any().downcast_ref::<UnionExec>() {
        merge = m;
    } else {
        return Ok(p);
    }

    // Try to replace `MergeExec` with `MergeSortExec`.
    let sort_order;
    if let Some(o) = p.output_ordering() {
        sort_order = o;
    } else {
        return Ok(p);
    }
    if sort_order.is_empty() {
        return Ok(p);
    }

    Ok(Arc::new(SortPreservingMergeExec::new(
        LexOrdering::new(sort_order.to_vec()),
        p,
    )))
}
