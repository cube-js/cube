use crate::queryplanner::materialized_rows_limit::MaterializedRowsLimitExec;
use crate::queryplanner::planning::WorkerExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::physical_plan::{ExecutionPlan, InputOrderMode};
use std::sync::Arc;

/// Add `MaterializedRowsLimitExec` at the points of the plan where rows accumulate in memory:
/// sort and window inputs, join build sides, aggregation outputs and worker results. Streaming
/// nodes are left as is.
pub fn add_materialized_rows_limit_exec(
    p: Arc<dyn ExecutionPlan>,
    limit: usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p_any = p.as_any();
    if let Some(sort) = p_any.downcast_ref::<SortExec>() {
        // Sort with a fetch keeps only top `fetch` rows in memory, so it stays under the limit on
        // its own when `fetch <= limit`. Otherwise its buffer holds `min(input, fetch)` rows, so
        // counting the input errors exactly when the buffer outgrows the limit.
        if sort.fetch().map_or(true, |fetch| fetch > limit) {
            return wrap_children(&p, &[(0, "sort input")], limit);
        }
    } else if p_any.is::<HashJoinExec>() {
        return wrap_children(&p, &[(0, "hash join build side")], limit);
    } else if p_any.is::<CrossJoinExec>() {
        return wrap_children(&p, &[(0, "cross join left side")], limit);
    } else if p_any.is::<WindowAggExec>() {
        return wrap_children(&p, &[(0, "window input")], limit);
    } else if let Some(agg) = p_any.downcast_ref::<AggregateExec>() {
        // A sorted aggregation streams groups out instead of accumulating a hash table.
        if agg.input_order_mode() != &InputOrderMode::Sorted {
            return Ok(wrap(p, limit, "aggregation groups"));
        }
    } else if p_any.is::<WorkerExec>() {
        return wrap_children(&p, &[(0, "worker result")], limit);
    }
    Ok(p)
}

pub fn wrap(
    p: Arc<dyn ExecutionPlan>,
    limit: usize,
    stage: &'static str,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(MaterializedRowsLimitExec::new(p, limit, stage))
}

fn wrap_children(
    p: &Arc<dyn ExecutionPlan>,
    wraps: &[(usize, &'static str)],
    limit: usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let mut children: Vec<_> = p.children().into_iter().cloned().collect();
    let mut changed = false;
    for (i, stage) in wraps {
        // The child rows may already be counted by an adjacent limit node.
        if !children[*i].as_any().is::<MaterializedRowsLimitExec>() {
            children[*i] = wrap(children[*i].clone(), limit, stage);
            changed = true;
        }
    }
    if changed {
        p.clone().with_new_children(children)
    } else {
        Ok(p.clone())
    }
}
