use crate::queryplanner::planning::WorkerExec;
use crate::queryplanner::query_executor::ClusterSendExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::limit::GlobalLimitExec;
use std::sync::Arc;

/// Transforms from:
///     AggregateFinal
///     `- AggregatePartial
///        `- ClusterSend
/// to:
///     AggregateFinal
///     `- ClusterSend
///        `- AggregatePartial
///
/// The latter gives results in more parallelism and less network.
pub fn push_aggregate_to_workers(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let agg;
    if let Some(a) = p.as_any().downcast_ref::<HashAggregateExec>() {
        agg = a;
    } else {
        return Ok(p);
    }
    if *agg.mode() != AggregateMode::Partial {
        return Ok(p);
    }

    if let Some(cs) = agg.input().as_any().downcast_ref::<ClusterSendExec>() {
        // Router plan, replace partial aggregate with cluster send.
        Ok(Arc::new(cs.with_changed_schema(
            agg.schema().clone(),
            agg.with_new_children(vec![cs.input_for_optimizations.clone()])?,
        )))
    } else if let Some(w) = agg.input().as_any().downcast_ref::<WorkerExec>() {
        let input = if let Some(limit) = w.input.as_any().downcast_ref::<GlobalLimitExec>() {
            //Check and pull up worker limit if exists
            limit.with_new_children(vec![agg.with_new_children(vec![limit.input().clone()])?])?
        } else {
            agg.with_new_children(vec![w.input.clone()])?
        };
        // Worker plan, execute partial aggregate inside the worker.
        Ok(Arc::new(WorkerExec {
            input,
            schema: agg.schema().clone(),
            max_batch_rows: w.max_batch_rows,
        }))
    } else {
        Ok(p)
    }
}
