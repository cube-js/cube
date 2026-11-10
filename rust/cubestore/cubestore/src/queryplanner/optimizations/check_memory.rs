use crate::queryplanner::check_memory::CheckMemoryExec;
use crate::queryplanner::query_executor::ClusterSendExec;
use crate::queryplanner::AsAnyCompat;
use crate::util::memory::MemoryHandler;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Add `CheckMemoryExec` behind some nodes.
#[allow(deprecated)]
pub fn add_check_memory_exec(
    p: Arc<dyn ExecutionPlan>,
    mem_handler: Arc<dyn MemoryHandler>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p_any = p.as_any();
    if p_any.is::<DataSourceExec>() || p_any.is::<ClusterSendExec>() {
        let memory_check = Arc::new(CheckMemoryExec::new(p, mem_handler.clone()));
        Ok(memory_check)
    } else {
        Ok(p)
    }
}
