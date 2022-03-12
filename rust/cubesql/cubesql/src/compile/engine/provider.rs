use std::sync::Arc;

use datafusion::{
    datasource,
    execution::context::ExecutionContextState,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF},
    sql::planner::ContextProvider,
};

use crate::{
    compile::MetaContext,
    sql::{session::DatabaseProtocol, SessionManager, SessionState},
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
    tables::InfoSchemaTableProvider as PostgresSchemaTableProvider, PgCatalogNamespaceProvider,
    PgCatalogTableProvider, PgCatalogTypeProvider,
};
use crate::compile::engine::information_schema::mysql::ext::CubeColumnMySqlExt;
use crate::transport::V1CubeMetaExt;
use crate::CubeError;
use async_trait::async_trait;
use cubeclient::models::V1CubeMeta;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;

#[derive(Clone)]
pub struct CubeContext {
    /// Internal state for the context (default)
    pub state: Arc<ExecutionContextState>,
    /// References
    pub meta: Arc<MetaContext>,
    pub sessions: Arc<SessionManager>,
    pub session_state: Arc<SessionState>,
}

impl CubeContext {
    pub fn new(
        state: Arc<ExecutionContextState>,
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

    pub fn table_name_by_table_provider(
        &self,
        table_provider: Arc<dyn datasource::TableProvider>,
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
        } else {
            return Err(CubeError::internal(format!(
                "Unknown table provider with schema: {:?}",
                table_provider.schema()
            )));
        })
    }
}

impl ContextProvider for CubeContext {
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
            datafusion::catalog::TableReference::Bare { table } => Some(table.to_string()),
        };

        if let Some(tp) = table_path {
            return self.session_state.protocol.get_provider(&self.clone(), tp);
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
        if let Some(cube) = context
            .meta
            .cubes
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&tp))
        {
            return Some(Arc::new(CubeTableProvider::new(cube.clone()))); // TODO .clone()
        }

        if tp.eq_ignore_ascii_case("information_schema.tables") {
            return Some(Arc::new(MySqlSchemaTableProvider::new(
                context.meta.clone(),
            )));
        }

        if tp.eq_ignore_ascii_case("information_schema.columns") {
            return Some(Arc::new(MySqlSchemaColumnsProvider::new(
                context.meta.clone(),
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
            return Some(Arc::new(MySqlPerfSchemaVariablesProvider::new(
                "performance_schema.global_variables".to_string(),
                context
                    .sessions
                    .server
                    .all_variables(context.session_state.protocol.clone()),
            )));
        }

        if tp.eq_ignore_ascii_case("performance_schema.session_variables") {
            return Some(Arc::new(MySqlPerfSchemaVariablesProvider::new(
                "performance_schema.session_variables".to_string(),
                context.session_state.all_variables(),
            )));
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

        if tp.eq_ignore_ascii_case("pg_catalog.pg_type") {
            return Some(Arc::new(PgCatalogTypeProvider::new()));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_namespace") {
            return Some(Arc::new(PgCatalogNamespaceProvider::new()));
        }

        None
    }
}

pub trait TableName {
    fn table_name(&self) -> &str;
}

pub struct CubeTableProvider {
    cube: V1CubeMeta,
}

impl CubeTableProvider {
    pub fn new(cube: V1CubeMeta) -> Self {
        Self { cube }
    }
}

impl TableName for CubeTableProvider {
    fn table_name(&self) -> &str {
        &self.cube.name
    }
}

#[async_trait]
impl TableProvider for CubeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.cube
                .get_columns()
                .into_iter()
                .map(|c| {
                    Field::new(
                        c.get_name(),
                        match c.get_data_type().as_str() {
                            "datetime" => DataType::Timestamp(TimeUnit::Millisecond, None),
                            "boolean" => DataType::Boolean,
                            "int" => DataType::Int64,
                            _ => DataType::Utf8,
                        },
                        true,
                    )
                })
                .collect(),
        ))
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Plan(format!(
            "Not rewritten table scan node for '{}' cube",
            self.cube.name
        )))
    }
}
