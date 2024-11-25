use crate::queryplanner::trace_data_loaded::{DataLoadedSize, TraceDataLoadedExec};
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Add `TraceDataLoadedExec` behind ParquetExec nodes.
pub fn add_trace_data_loaded_exec(
    p: Arc<dyn ExecutionPlan>,
    data_loaded_size: Arc<DataLoadedSize>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p_any = p.as_any();
    if p_any.is::<ParquetExec>() {
        let trace_data_loaded = Arc::new(TraceDataLoadedExec::new(p, data_loaded_size.clone()));
        Ok(trace_data_loaded)
    } else {
        Ok(p)
    }
}
