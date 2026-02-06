use std::sync::Arc;

use datafusion::datasource::TableProvider;

use crate::{
    compile::{
        engine::{CubeContext, CubeTableProvider, TableName},
        DatabaseProtocol,
    },
    CubeError,
};

impl DatabaseProtocol {
    pub(crate) fn get_arrow_native_provider(
        &self,
        context: &CubeContext,
        tr: datafusion::catalog::TableReference,
    ) -> Option<Arc<dyn TableProvider>> {
        // Extract table name from table reference
        let table = match tr {
            datafusion::catalog::TableReference::Partial { table, .. } => {
                table.to_ascii_lowercase()
            }
            datafusion::catalog::TableReference::Full { table, .. } => table.to_ascii_lowercase(),
            datafusion::catalog::TableReference::Bare { table } => table.to_ascii_lowercase(),
        };

        // Look up cube in metadata
        if let Some(cube) = context
            .meta
            .cubes
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&table))
        {
            return Some(Arc::new(CubeTableProvider::new(cube.clone())));
        }

        None
    }

    pub fn get_arrow_native_table_name(
        &self,
        table_provider: Arc<dyn TableProvider>,
    ) -> Result<String, CubeError> {
        let any = table_provider.as_any();
        Ok(if let Some(t) = any.downcast_ref::<CubeTableProvider>() {
            t.table_name().to_string()
        } else {
            return Err(CubeError::internal(format!(
                "Unable to get table name for ArrowNative protocol provider: {:?}",
                any.type_id()
            )));
        })
    }
}
