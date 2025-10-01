use std::sync::Arc;

use datafusion::datasource::{self, TableProvider};

use crate::{
    compile::{
        engine::{CubeContext, CubeTableProvider, TableName},
        DatabaseProtocol,
    },
    CubeError,
};

impl DatabaseProtocol {
    pub fn get_mysql_table_name(
        &self,
        table_provider: Arc<dyn TableProvider>,
    ) -> Result<String, CubeError> {
        let any = table_provider.as_any();
        Ok(if let Some(t) = any.downcast_ref::<CubeTableProvider>() {
            t.table_name().to_string()
        } else {
            return Err(CubeError::internal(format!(
                "Unknown table provider with schema: {:?}",
                table_provider.schema()
            )));
        })
    }

    pub(crate) fn get_mysql_provider(
        &self,
        context: &CubeContext,
        tr: datafusion::catalog::TableReference,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        let (db, table) = match tr {
            datafusion::catalog::TableReference::Partial { schema, table, .. } => {
                (schema.to_ascii_lowercase(), table.to_ascii_lowercase())
            }
            datafusion::catalog::TableReference::Full {
                catalog: _,
                schema,
                table,
            } => (schema.to_ascii_lowercase(), table.to_ascii_lowercase()),
            datafusion::catalog::TableReference::Bare { table } => {
                ("db".to_string(), table.to_ascii_lowercase())
            }
        };

        match db.as_str() {
            "db" => {
                if let Some(cube) = context
                    .meta
                    .cubes
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&table))
                {
                    // TODO .clone()
                    return Some(Arc::new(CubeTableProvider::new(cube.clone())));
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
}
