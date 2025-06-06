mod check_memory;
mod distributed_partial_aggregate;
mod prefer_inplace_aggregates;
pub mod rewrite_plan;
pub mod rolling_optimizer;
mod trace_data_loaded;

use crate::cluster::{Cluster, WorkerPlanningParams};
use crate::queryplanner::optimizations::distributed_partial_aggregate::{
    add_limit_to_workers, ensure_partition_merge, push_aggregate_to_workers,
};
use std::fmt::{Debug, Formatter};
// use crate::queryplanner::optimizations::prefer_inplace_aggregates::try_switch_to_inplace_aggregates;
use super::serialized_plan::PreSerializedPlan;
use crate::queryplanner::planning::CubeExtensionPlanner;
use crate::queryplanner::rolling::RollingWindowPlanner;
use crate::queryplanner::trace_data_loaded::DataLoadedSize;
use crate::util::memory::MemoryHandler;
use async_trait::async_trait;
use check_memory::add_check_memory_exec;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use distributed_partial_aggregate::ensure_partition_merge_with_acceptable_parent;
use rewrite_plan::rewrite_physical_plan;
use std::sync::Arc;
use trace_data_loaded::add_trace_data_loaded_exec;

pub struct CubeQueryPlanner {
    /// Set on the router
    cluster: Option<Arc<dyn Cluster>>,
    /// Set on the worker
    worker_partition_count: Option<WorkerPlanningParams>,
    serialized_plan: Arc<PreSerializedPlan>,
    memory_handler: Arc<dyn MemoryHandler>,
    data_loaded_size: Option<Arc<DataLoadedSize>>,
}

impl CubeQueryPlanner {
    pub fn new_on_router(
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<PreSerializedPlan>,
        memory_handler: Arc<dyn MemoryHandler>,
    ) -> CubeQueryPlanner {
        CubeQueryPlanner {
            cluster: Some(cluster),
            worker_partition_count: None,
            serialized_plan,
            memory_handler,
            data_loaded_size: None,
        }
    }

    pub fn new_on_worker(
        serialized_plan: Arc<PreSerializedPlan>,
        worker_planning_params: WorkerPlanningParams,
        memory_handler: Arc<dyn MemoryHandler>,
        data_loaded_size: Option<Arc<DataLoadedSize>>,
    ) -> CubeQueryPlanner {
        CubeQueryPlanner {
            serialized_plan,
            cluster: None,
            worker_partition_count: Some(worker_planning_params),
            memory_handler,
            data_loaded_size,
        }
    }
}

impl Debug for CubeQueryPlanner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CubeQueryPlanner")
    }
}

#[async_trait]
impl QueryPlanner for CubeQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let p = DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(CubeExtensionPlanner {
                cluster: self.cluster.clone(),
                worker_planning_params: self.worker_partition_count,
                serialized_plan: self.serialized_plan.clone(),
            }),
            Arc::new(RollingWindowPlanner {}),
        ])
        .create_physical_plan(logical_plan, ctx_state)
        .await?;
        // TODO: assert there is only a single ClusterSendExec in the plan.  Update: This is no longer true.
        finalize_physical_plan(
            p,
            self.memory_handler.clone(),
            self.data_loaded_size.clone(),
        )
    }
}

#[derive(Debug)]
pub struct PreOptimizeRule {
    memory_handler: Arc<dyn MemoryHandler>,
    data_loaded_size: Option<Arc<DataLoadedSize>>,
}

impl PreOptimizeRule {
    pub fn new(
        memory_handler: Arc<dyn MemoryHandler>,
        data_loaded_size: Option<Arc<DataLoadedSize>>,
    ) -> Self {
        Self {
            memory_handler,
            data_loaded_size,
        }
    }
}

impl PhysicalOptimizerRule for PreOptimizeRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        pre_optimize_physical_plan(
            plan,
            self.memory_handler.clone(),
            self.data_loaded_size.clone(),
        )
    }

    fn name(&self) -> &str {
        "PreOptimizeRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn pre_optimize_physical_plan(
    p: Arc<dyn ExecutionPlan>,
    memory_handler: Arc<dyn MemoryHandler>,
    data_loaded_size: Option<Arc<DataLoadedSize>>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // TODO upgrade DF
    let p = rewrite_physical_plan(p, &mut |p| push_aggregate_to_workers(p))?;

    // Handles non-root-node cases
    let p = rewrite_physical_plan(p, &mut |p| ensure_partition_merge_with_acceptable_parent(p))?;
    // Handles the root node case
    let p = ensure_partition_merge(p)?;
    Ok(p)
}

fn finalize_physical_plan(
    p: Arc<dyn ExecutionPlan>,
    memory_handler: Arc<dyn MemoryHandler>,
    data_loaded_size: Option<Arc<DataLoadedSize>>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // TODO upgrade DF
    // let p = rewrite_physical_plan(p.as_ref(), &mut |p| try_switch_to_inplace_aggregates(p))?;
    let p = rewrite_physical_plan(p, &mut |p| add_check_memory_exec(p, memory_handler.clone()))?;
    let p = if let Some(data_loaded_size) = data_loaded_size {
        rewrite_physical_plan(p, &mut |p| add_trace_data_loaded_exec(p, &data_loaded_size))?
    } else {
        p
    };
    rewrite_physical_plan(p, &mut |p| add_limit_to_workers(p))
}
