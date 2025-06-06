use crate::queryplanner::check_memory::CheckMemoryExec;
use crate::queryplanner::query_executor::ClusterSendExec;
use crate::util::memory::MemoryHandler;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::memory::MemoryExec;
use std::sync::Arc;

/// Add `CheckMemoryExec` behind some nodes.
pub fn add_check_memory_exec(
    p: Arc<dyn ExecutionPlan>,
    mem_handler: Arc<dyn MemoryHandler>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p_any = p.as_any();
    // We supposedly don't use ParquetExec, which is deprecated in DF 46, anymore but we keep the check here in case we do.
    if p_any.is::<DataSourceExec>()
        || p_any.is::<ParquetExec>()
        || p_any.is::<MemoryExec>()
        || p_any.is::<ClusterSendExec>()
    {
        let memory_check = Arc::new(CheckMemoryExec::new(p, mem_handler.clone()));
        Ok(memory_check)
    } else {
        Ok(p)
    }
}
