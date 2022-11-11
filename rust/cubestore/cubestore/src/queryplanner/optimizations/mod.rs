use crate::cluster::Cluster;
use crate::queryplanner::optimizations::distributed_partial_aggregate::push_aggregate_to_workers;
use crate::queryplanner::optimizations::prefer_inplace_aggregates::try_switch_to_inplace_aggregates;
use crate::queryplanner::planning::CubeExtensionPlanner;
use crate::queryplanner::serialized_plan::SerializedPlan;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{ExecutionContextState, QueryPlanner};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use rewrite_plan::rewrite_physical_plan;
use std::sync::Arc;
use crate::queryplanner::pretty_printers::pp_phys_plan;

mod distributed_partial_aggregate;
mod prefer_inplace_aggregates;
pub mod rewrite_plan;

pub struct CubeQueryPlanner {
    cluster: Option<Arc<dyn Cluster>>,
    serialized_plan: Arc<SerializedPlan>,
}

impl CubeQueryPlanner {
    pub fn new_on_router(
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<SerializedPlan>,
    ) -> CubeQueryPlanner {
        CubeQueryPlanner {
            cluster: Some(cluster),
            serialized_plan,
        }
    }

    pub fn new_on_worker(serialized_plan: Arc<SerializedPlan>) -> CubeQueryPlanner {
        CubeQueryPlanner {
            serialized_plan,
            cluster: None,
        }
    }
}

impl QueryPlanner for CubeQueryPlanner {
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let p =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(CubeExtensionPlanner {
                cluster: self.cluster.clone(),
                serialized_plan: self.serialized_plan.clone(),
            })])
            .create_physical_plan(logical_plan, ctx_state)?;
        // TODO: assert there is only a single ClusterSendExec in the plan.
        finalize_physical_plan(p)
    }
}

fn finalize_physical_plan(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p = rewrite_physical_plan(p.as_ref(), &mut |p| try_switch_to_inplace_aggregates(p))?;
    rewrite_physical_plan(p.as_ref(), &mut |p| push_aggregate_to_workers(p))
}
