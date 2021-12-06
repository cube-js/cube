use std::sync::Arc;

use cubeclient::models::V1CubeMeta;
use datafusion::{
    datasource,
    execution::context::ExecutionContextState,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF},
    sql::planner::ContextProvider,
};

use super::information_schema::{
    columns::InfoSchemaColumnsProvider, statistics::InfoSchemaStatisticsProvider,
    tables::InfoSchemaTableProvider, variables::PerfSchemaVariablesProvider,
};

pub struct CubeContext<'a> {
    /// Internal state for the context (default)
    pub state: &'a ExecutionContextState,
    /// Our context
    pub cubes: &'a Vec<V1CubeMeta>,
}

impl<'a> CubeContext<'a> {
    pub fn new(state: &'a ExecutionContextState, cubes: &'a Vec<V1CubeMeta>) -> Self {
        Self { state, cubes }
    }
}

impl<'a> ContextProvider for CubeContext<'a> {
    fn get_table_provider(
        &self,
        name: datafusion::catalog::TableReference,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        let table_path = match name {
            datafusion::catalog::TableReference::Partial { schema, table, .. } => {
                Some(format!("{}.{}", schema, table))
            }
            datafusion::catalog::TableReference::Full {
                catalog,
                schema,
                table,
            } => Some(format!("{}.{}.{}", catalog, schema, table)),
            _ => None,
        };

        if let Some(tp) = table_path {
            if tp.eq_ignore_ascii_case("information_schema.tables") {
                return Some(Arc::new(InfoSchemaTableProvider::new(self.cubes)));
            }

            if tp.eq_ignore_ascii_case("information_schema.columns") {
                return Some(Arc::new(InfoSchemaColumnsProvider::new(self.cubes)));
            }

            if tp.eq_ignore_ascii_case("information_schema.statistics") {
                return Some(Arc::new(InfoSchemaStatisticsProvider::new()));
            }

            if tp.eq_ignore_ascii_case("performance_schema.global_variables") {
                return Some(Arc::new(PerfSchemaVariablesProvider::new()));
            }

            if tp.eq_ignore_ascii_case("performance_schema.session_variables") {
                return Some(Arc::new(PerfSchemaVariablesProvider::new()));
            }
        };

        None
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions.get(name).cloned()
    }
}
