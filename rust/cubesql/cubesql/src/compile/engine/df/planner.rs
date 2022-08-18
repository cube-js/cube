use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_plan::LogicalPlan,
    physical_plan::{planner::DefaultPhysicalPlanner, ExecutionPlan, PhysicalPlanner},
};

use crate::transport::{LoadRequestMeta, TransportService};

use super::scan::CubeScanExtensionPlanner;

pub struct CubeQueryPlanner {
    pub transport: Arc<dyn TransportService>,
    pub meta: LoadRequestMeta,
}

impl CubeQueryPlanner {
    pub fn new(transport: Arc<dyn TransportService>, meta: LoadRequestMeta) -> Self {
        Self { transport, meta }
    }
}

#[async_trait]
impl QueryPlanner for CubeQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            CubeScanExtensionPlanner {
                transport: self.transport.clone(),
                meta: self.meta.clone(),
            },
        )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
