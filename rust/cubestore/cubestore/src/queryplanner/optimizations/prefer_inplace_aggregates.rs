use crate::queryplanner::planning::WorkerExec;
use crate::queryplanner::query_executor::ClusterSendExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::{AggregateStrategy, HashAggregateExec};
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::merge_sort::MergeSortExec;
use datafusion::physical_plan::planner::input_sortedness_by_group_key;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
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

    let input_sortedness = input_sortedness_by_group_key(new_input.as_ref(), agg.group_expr());


    let (strategy, order): (AggregateStrategy, Option<Vec<usize>>) =
    match input_sortedness.sawtooth_levels() {
        Some(0) => {
            log::error!("try_switch_to_inplace_aggregates: Perfect match for inplace aggregation");
            let order = input_sortedness.sort_order[0].clone();  // TODO: No clone?
            (AggregateStrategy::InplaceSorted, Some(order))
        }
        Some(n) => {
            log::error!("try_switch_to_inplace_aggregates: Non-perfect match for inplace aggregation: {} clumps", n);
            // TODO: Note that this is very oversimplified
            (AggregateStrategy::InplaceSorted, None)
        },
        _ => {
            log::error!("try_switch_to_inplace_aggregates: No match for inplace aggregation");
            (AggregateStrategy::Hash, None)
        },
    };

    if strategy != AggregateStrategy::InplaceSorted {
        return Ok(p);
    }
    Ok(Arc::new(HashAggregateExec::try_new(
        AggregateStrategy::InplaceSorted,
        order,
        *agg.mode(),
        agg.group_expr().into(),
        agg.aggr_expr().into(),
        new_input,
        agg.input_schema().clone(),
    )?))
}

/// Attempts to provide **some** grouping in the results, but no particular one is guaranteed.
fn try_regroup_columns(
    p: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    if p.as_any().is::<HashAggregateExec>() {
        return Ok(p);
    }
    if p.as_any().is::<UnionExec>()
        || p.as_any().is::<ProjectionExec>()
        || p.as_any().is::<FilterExec>()
        || p.as_any().is::<WorkerExec>()
        || p.as_any().is::<ClusterSendExec>()
    {
        return p.with_new_children(
            p.children()
                .into_iter()
                .map(|c| try_regroup_columns(c))
                .collect::<Result<_, DataFusionError>>()?,
        );
    }

    let merge;
    if let Some(m) = p.as_any().downcast_ref::<MergeExec>() {
        merge = m;
    } else {
        return Ok(p);
    }

    let input = try_regroup_columns(merge.input().clone())?;

    // Try to replace `MergeExec` with `MergeSortExec`.
    let sort_order;
    if let Some(o) = input.output_hints().sort_order {
        sort_order = o;
    } else {
        return Ok(p);
    }
    if sort_order.is_empty() {
        return Ok(p);
    }

    let schema = input.schema();
    let sort_columns = sort_order
        .into_iter()
        .map(|i| Column::new(schema.field(i).name(), i))
        .collect();
    Ok(Arc::new(MergeSortExec::try_new(input, sort_columns)?))
}

