use datafusion::error::DataFusionError;
use datafusion::physical_plan::expressions::AliasedSchemaExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::{AggregateStrategy, HashAggregateExec};
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::merge_sort::MergeSortExec;
use datafusion::physical_plan::planner::compute_aggregation_strategy;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Attempts to replace hash aggregate with sorted aggregate.
/// TODO: we should pick the right index.
pub fn try_switch_to_inplace_aggregates(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let agg;
    if let Some(a) = p.as_any().downcast_ref::<HashAggregateExec>() {
        agg = a;
    } else {
        return Ok(p);
    }
    if agg.strategy() != AggregateStrategy::Hash || agg.group_expr().len() == 0 {
        return Ok(p);
    }
    // Try to cheaply rearrange the plan so that it produces sorted inputs.
    let new_input = try_regroup_columns(agg.input().clone())?;

    if compute_aggregation_strategy(new_input.as_ref(), agg.group_expr())
        != AggregateStrategy::InplaceSorted
    {
        return Ok(p);
    }
    Ok(Arc::new(HashAggregateExec::try_new(
        AggregateStrategy::InplaceSorted,
        *agg.mode(),
        agg.group_expr().into(),
        agg.aggr_expr().into(),
        new_input,
    )?))
}

/// Attempts to provide **some** grouping in the results, but no particular one is guaranteed.
fn try_regroup_columns(
    p: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    if p.as_any().is::<HashAggregateExec>() {
        return Ok(p);
    }
    if p.as_any().is::<AliasedSchemaExec>() || p.as_any().is::<FilterExec>() {
        let children: datafusion::error::Result<Vec<_>> = p
            .children()
            .into_iter()
            .map(|c| try_regroup_columns(c))
            .collect();
        return p.with_new_children(children?);
    }
    let merge;
    if let Some(m) = p.as_any().downcast_ref::<MergeExec>() {
        merge = m;
    } else {
        return Ok(p);
    }

    // Try to replace `MergeExec` with `MergeSortExec`.
    let sort_order;
    if let Some(o) = merge.input().output_hints().sort_order {
        sort_order = o;
    } else {
        return Ok(p);
    }
    if sort_order.is_empty() {
        return Ok(p);
    }
    let sort_columns = sort_order
        .into_iter()
        .map(|i| merge.input().schema().field(i).qualified_name())
        .collect();
    Ok(Arc::new(MergeSortExec::try_new(
        merge.input().clone(),
        sort_columns,
    )?))
}
