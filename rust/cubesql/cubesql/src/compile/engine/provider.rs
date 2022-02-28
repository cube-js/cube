use std::sync::Arc;

use datafusion::{
    datasource,
    execution::context::ExecutionContextState,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF},
    sql::planner::ContextProvider,
};

use crate::{
    compile::MetaContext,
    sql::{DatabaseProtocol, SessionManager, SessionState},
};

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

use super::information_schema::postgres::{
    columns::InfoSchemaColumnsProvider as PostgresSchemaColumnsProvider,
    pg_tables::PgCatalogTableProvider,
    tables::InfoSchemaTableProvider as PostgresSchemaTableProvider,
};

pub struct CubeContext<'a> {
    /// Internal state for the context (default)
    pub state: &'a ExecutionContextState,
    /// References
    pub meta: Arc<MetaContext>,
    pub sessions: Arc<SessionManager>,
    pub session_state: Arc<SessionState>,
}

impl<'a> CubeContext<'a> {
    pub fn new(
        state: &'a ExecutionContextState,
        meta: Arc<MetaContext>,
        sessions: Arc<SessionManager>,
        session_state: Arc<SessionState>,
    ) -> Self {
        Self {
            state,
            meta,
            sessions,
            session_state,
        }
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
            return self.session_state.protocol.get_provider(self.clone(), tp);
        }

        None
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions.get(name).cloned()
    }
}

impl DatabaseProtocol {
    fn get_provider(
        &self,
        context: &CubeContext,
        tp: String,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        match self {
            DatabaseProtocol::MySQL => self.get_mysql_provider(context, tp),
            DatabaseProtocol::PostgreSQL => self.get_postgres_provider(context, tp),
        }
    }

    fn get_mysql_provider(
        &self,
        context: &CubeContext,
        tp: String,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        if tp.eq_ignore_ascii_case("information_schema.tables") {
            return Some(Arc::new(MySqlSchemaTableProvider::new(&context.meta.cubes)));
        }

        if tp.eq_ignore_ascii_case("information_schema.columns") {
            return Some(Arc::new(MySqlSchemaColumnsProvider::new(
                &context.meta.cubes,
            )));
        }

        if tp.eq_ignore_ascii_case("information_schema.statistics") {
            return Some(Arc::new(MySqlSchemaStatisticsProvider::new()));
        }

        if tp.eq_ignore_ascii_case("information_schema.key_column_usage") {
            return Some(Arc::new(MySqlSchemaKeyColumnUsageProvider::new()));
        }

        if tp.eq_ignore_ascii_case("information_schema.schemata") {
            return Some(Arc::new(MySqlSchemaSchemataProvider::new()));
        }

        if tp.eq_ignore_ascii_case("information_schema.processlist") {
            return Some(Arc::new(MySqlSchemaProcesslistProvider::new(
                context.sessions.clone(),
            )));
        }

        if tp.eq_ignore_ascii_case("information_schema.referential_constraints") {
            return Some(Arc::new(MySqlSchemaReferentialConstraintsProvider::new()));
        }

        if tp.eq_ignore_ascii_case("information_schema.collations") {
            return Some(Arc::new(MySqlSchemaCollationsProvider::new()));
        }

        if tp.eq_ignore_ascii_case("performance_schema.global_variables") {
            return Some(Arc::new(MySqlPerfSchemaVariablesProvider::new()));
        }

        if tp.eq_ignore_ascii_case("performance_schema.session_variables") {
            return Some(Arc::new(MySqlPerfSchemaVariablesProvider::new()));
        }

        None
    }

    fn get_postgres_provider(
        &self,
        context: &CubeContext,
        tp: String,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        if tp.eq_ignore_ascii_case("information_schema.columns") {
            return Some(Arc::new(PostgresSchemaColumnsProvider::new(
                &context.meta.cubes,
            )));
        }

        if tp.eq_ignore_ascii_case("information_schema.tables") {
            return Some(Arc::new(PostgresSchemaTableProvider::new(
                &context.meta.cubes,
            )));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_tables") {
            return Some(Arc::new(PgCatalogTableProvider::new(&context.meta.cubes)));
        }

        None
    }
}
