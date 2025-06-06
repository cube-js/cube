use crate::queryplanner::trace_data_loaded::{DataLoadedSize, TraceDataLoadedExec};
use datafusion::datasource::physical_plan::{ParquetExec, ParquetSource};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::source::DataSourceExec;
use std::sync::Arc;

/// Add `TraceDataLoadedExec` behind ParquetExec or DataSourceExec (with File hence Parquet source) nodes.
pub fn add_trace_data_loaded_exec(
    p: Arc<dyn ExecutionPlan>,
    data_loaded_size: &Arc<DataLoadedSize>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    fn do_wrap(
        p: Arc<dyn ExecutionPlan>,
        data_loaded_size: &Arc<DataLoadedSize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(TraceDataLoadedExec::new(
            p,
            data_loaded_size.clone(),
        )))
    }

    let p_any = p.as_any();
    if p_any.is::<ParquetExec>() {
        // ParquetExec is deprecated in DF 46 and we don't use it; we shouldn't hit this case, but we keep it just in case.
        return do_wrap(p, data_loaded_size);
    } else if let Some(dse) = p_any.downcast_ref::<DataSourceExec>() {
        if let Some(file_scan) = dse.data_source().as_any().downcast_ref::<FileScanConfig>() {
            if file_scan.file_source().as_any().is::<ParquetSource>() {
                return do_wrap(p, data_loaded_size);
            }
        }
    }
    Ok(p)
}
