use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::{ExecutionContextState, QueryPlanner},
    logical_plan::LogicalPlan,
    physical_plan::{planner::DefaultPhysicalPlanner, ExecutionPlan, PhysicalPlanner},
};

use crate::transport::TransportService;

use super::scan::CubeScanExtensionPlanner;

pub struct CubeQueryPlanner {
    pub transport: Arc<dyn TransportService>,
}

impl CubeQueryPlanner {
    pub fn new(transport: Arc<dyn TransportService>) -> Self {
        Self { transport }
    }
}

#[async_trait]
impl QueryPlanner for CubeQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan TopK nodes.
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            CubeScanExtensionPlanner {
                transport: self.transport.clone(),
            },
        )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, ctx_state)
            .await
    }
}
