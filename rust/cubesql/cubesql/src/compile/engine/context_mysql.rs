use std::sync::Arc;

use datafusion::datasource::{self, TableProvider};

use super::information_schema::mysql::{
    collations::InfoSchemaCollationsProvider as MySqlSchemaCollationsProvider,
    columns::InfoSchemaColumnsProvider as MySqlSchemaColumnsProvider,
    key_column_usage::InfoSchemaKeyColumnUsageProvider as MySqlSchemaKeyColumnUsageProvider,
    processlist::InfoSchemaProcesslistProvider as MySqlSchemaProcesslistProvider,
    referential_constraints::InfoSchemaReferentialConstraintsProvider as MySqlSchemaReferentialConstraintsProvider,
    schemata::InfoSchemaSchemataProvider as MySqlSchemaSchemataProvider,
    statistics::InfoSchemaStatisticsProvider as MySqlSchemaStatisticsProvider,
    tables::InfoSchemaTableProvider as MySqlSchemaTableProvider,
    variables::PerfSchemaVariablesProvider as MySqlPerfSchemaVariablesProvider,
};
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
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaTableProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaColumnsProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaStatisticsProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaKeyColumnUsageProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaSchemataProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaReferentialConstraintsProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaCollationsProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<MySqlPerfSchemaVariablesProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaProcesslistProvider>() {
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
            "information_schema" => match table.as_str() {
                "tables" => {
                    return Some(Arc::new(MySqlSchemaTableProvider::new(
                        context.meta.clone(),
                    )))
                }
                "columns" => {
                    return Some(Arc::new(MySqlSchemaColumnsProvider::new(
                        context.meta.clone(),
                    )))
                }
                "statistics" => return Some(Arc::new(MySqlSchemaStatisticsProvider::new())),
                "key_column_usage" => {
                    return Some(Arc::new(MySqlSchemaKeyColumnUsageProvider::new()))
                }
                "schemata" => return Some(Arc::new(MySqlSchemaSchemataProvider::new())),
                "processlist" => {
                    return Some(Arc::new(MySqlSchemaProcesslistProvider::new(
                        context.sessions.clone(),
                    )))
                }
                "referential_constraints" => {
                    return Some(Arc::new(MySqlSchemaReferentialConstraintsProvider::new()))
                }
                "collations" => return Some(Arc::new(MySqlSchemaCollationsProvider::new())),
                _ => return None,
            },
            "performance_schema" => match table.as_str() {
                "global_variables" => {
                    return Some(Arc::new(MySqlPerfSchemaVariablesProvider::new(
                        "performance_schema.global_variables".to_string(),
                        context
                            .sessions
                            .server
                            .all_variables(context.session_state.protocol.clone()),
                    )))
                }
                "session_variables" => {
                    return Some(Arc::new(MySqlPerfSchemaVariablesProvider::new(
                        "performance_schema.session_variables".to_string(),
                        context.session_state.all_variables(),
                    )))
                }
                _ => return None,
            },
            _ => return None,
        }
    }
}
