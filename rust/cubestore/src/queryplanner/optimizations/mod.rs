use crate::queryplanner::optimizations::prefer_inplace_aggregates::try_switch_to_inplace_aggregates;
use datafusion::execution::context::{ExecutionContextState, QueryPlanner};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use std::sync::Arc;

pub mod prefer_inplace_aggregates;

pub struct CubeQueryPlanner {}

impl QueryPlanner for CubeQueryPlanner {
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let p = DefaultPhysicalPlanner::default().create_physical_plan(logical_plan, ctx_state)?;
        try_switch_to_inplace_aggregates(p.as_ref())
    }
}
