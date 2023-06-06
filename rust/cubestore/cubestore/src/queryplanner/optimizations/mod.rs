mod check_memory;
mod distributed_partial_aggregate;
mod prefer_inplace_aggregates;
pub mod rewrite_plan;

use crate::cluster::Cluster;
use crate::queryplanner::optimizations::distributed_partial_aggregate::{
    add_limit_to_workers, push_aggregate_to_workers,
};
use crate::queryplanner::optimizations::prefer_inplace_aggregates::try_switch_to_inplace_aggregates;
use crate::queryplanner::planning::CubeExtensionPlanner;
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::util::memory::MemoryHandler;
use check_memory::add_check_memory_exec;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{ExecutionContextState, QueryPlanner};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use rewrite_plan::rewrite_physical_plan;
use std::sync::Arc;

pub struct CubeQueryPlanner {
    cluster: Option<Arc<dyn Cluster>>,
    serialized_plan: Arc<SerializedPlan>,
    memory_handler: Arc<dyn MemoryHandler>,
}

impl CubeQueryPlanner {
    pub fn new_on_router(
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<SerializedPlan>,
        memory_handler: Arc<dyn MemoryHandler>,
    ) -> CubeQueryPlanner {
        CubeQueryPlanner {
            cluster: Some(cluster),
            serialized_plan,
            memory_handler,
        }
    }

    pub fn new_on_worker(
        serialized_plan: Arc<SerializedPlan>,
        memory_handler: Arc<dyn MemoryHandler>,
    ) -> CubeQueryPlanner {
        CubeQueryPlanner {
            serialized_plan,
            cluster: None,
            memory_handler,
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
        finalize_physical_plan(p, self.memory_handler.clone())
    }
}

fn finalize_physical_plan(
    p: Arc<dyn ExecutionPlan>,
    memory_handler: Arc<dyn MemoryHandler>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p = rewrite_physical_plan(p.as_ref(), &mut |p| try_switch_to_inplace_aggregates(p))?;
    let p = rewrite_physical_plan(p.as_ref(), &mut |p| push_aggregate_to_workers(p))?;
    let p = rewrite_physical_plan(p.as_ref(), &mut |p| {
        add_check_memory_exec(p, memory_handler.clone())
    })?;
    rewrite_physical_plan(p.as_ref(), &mut |p| add_limit_to_workers(p))
}
