use std::sync::Arc;

use datafusion::{
    datasource,
    execution::context::SessionState as DFSessionState,
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
    character_sets::InfoSchemaCharacterSetsProvider as PostgresSchemaCharacterSetsProvider,
    columns::InfoSchemaColumnsProvider as PostgresSchemaColumnsProvider,
    key_column_usage::InfoSchemaKeyColumnUsageProvider as PostgresSchemaKeyColumnUsageProvider,
    referential_constraints::InfoSchemaReferentialConstraintsProvider as PostgresSchemaReferentialConstraintsProvider,
    table_constraints::InfoSchemaTableConstraintsProvider as PostgresSchemaTableConstraintsProvider,
    tables::InfoSchemaTableProvider as PostgresSchemaTableProvider, PgCatalogAttrdefProvider,
    PgCatalogAttributeProvider, PgCatalogClassProvider, PgCatalogConstraintProvider,
    PgCatalogDescriptionProvider, PgCatalogIndexProvider, PgCatalogNamespaceProvider,
    PgCatalogProcProvider, PgCatalogRangeProvider, PgCatalogSettingsProvider,
    PgCatalogTableProvider, PgCatalogTypeProvider,
};

use crate::sql::ColumnType;
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
    pub state: Arc<DFSessionState>,
    /// References
    pub meta: Arc<MetaContext>,
    pub sessions: Arc<SessionManager>,
    pub session_state: Arc<SessionState>,
}

impl CubeContext {
    pub fn new(
        state: Arc<DFSessionState>,
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
        self.session_state
            .protocol
            .table_name_by_table_provider(table_provider)
    }
}

impl ContextProvider for CubeContext {
    fn get_table_provider(
        &self,
        name: datafusion::catalog::TableReference,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        let table_path = match name {
            datafusion::catalog::TableReference::Partial { schema, table, .. } => {
                if schema == "db" {
                    Some(table.to_string())
                } else {
                    Some(format!("{}.{}", schema, table))
                }
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
        // DF started to use Fn normalize_ident to handle all identifiers, let's cast to lowercase
        self.state
            .scalar_functions
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        // DF started to use Fn normalize_ident to handle all identifiers, let's cast to lowercase
        self.state
            .aggregate_functions
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        Some(DataType::Utf8)
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

    pub fn table_name_by_table_provider(
        &self,
        table_provider: Arc<dyn datasource::TableProvider>,
    ) -> Result<String, CubeError> {
        match self {
            DatabaseProtocol::MySQL => self.get_mysql_table_name(table_provider),
            DatabaseProtocol::PostgreSQL => self.get_postgres_table_name(table_provider),
        }
    }

    pub fn get_mysql_table_name(
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
        } else if let Some(t) = any.downcast_ref::<MySqlSchemaProcesslistProvider>() {
            t.table_name().to_string()
        } else {
            return Err(CubeError::internal(format!(
                "Unknown table provider with schema: {:?}",
                table_provider.schema()
            )));
        })
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

    pub fn get_postgres_table_name(
        &self,
        table_provider: Arc<dyn datasource::TableProvider>,
    ) -> Result<String, CubeError> {
        let any = table_provider.as_any();
        Ok(
            if let Some(_) = any.downcast_ref::<PostgresSchemaColumnsProvider>() {
                "information_schema.columns".to_string()
            } else if let Some(_) = any.downcast_ref::<PostgresSchemaTableProvider>() {
                "information_schema.tables".to_string()
            } else if let Some(_) = any.downcast_ref::<PostgresSchemaCharacterSetsProvider>() {
                "information_schema.character_sets".to_string()
            } else if let Some(_) = any.downcast_ref::<PostgresSchemaKeyColumnUsageProvider>() {
                "information_schema.key_column_usage".to_string()
            } else if let Some(_) =
                any.downcast_ref::<PostgresSchemaReferentialConstraintsProvider>()
            {
                "information_schema.referential_constraints".to_string()
            } else if let Some(_) = any.downcast_ref::<PostgresSchemaTableConstraintsProvider>() {
                "information_schema.table_constraints".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogTableProvider>() {
                "pg_catalog.pg_tables".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogTypeProvider>() {
                "pg_catalog.pg_type".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogNamespaceProvider>() {
                "pg_catalog.pg_namespace".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogRangeProvider>() {
                "pg_catalog.pg_range".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogAttrdefProvider>() {
                "pg_catalog.pg_attrdef".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogAttributeProvider>() {
                "pg_catalog.pg_attribute".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogIndexProvider>() {
                "pg_catalog.pg_index".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogClassProvider>() {
                "pg_catalog.pg_class".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogProcProvider>() {
                "pg_catalog.pg_proc".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogSettingsProvider>() {
                "pg_catalog.pg_settings".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogDescriptionProvider>() {
                "pg_catalog.pg_description".to_string()
            } else if let Some(_) = any.downcast_ref::<PgCatalogConstraintProvider>() {
                "pg_catalog.pg_constraint".to_string()
            } else {
                return Err(CubeError::internal(format!(
                    "Unknown table provider with schema: {:?}",
                    table_provider.schema()
                )));
            },
        )
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

        if tp.eq_ignore_ascii_case("information_schema.character_sets") {
            return Some(Arc::new(PostgresSchemaCharacterSetsProvider::new(
                &context.session_state.database().unwrap_or("db".to_string()),
            )));
        }

        if tp.eq_ignore_ascii_case("information_schema.key_column_usage") {
            return Some(Arc::new(PostgresSchemaKeyColumnUsageProvider::new()));
        }

        if tp.eq_ignore_ascii_case("information_schema.referential_constraints") {
            return Some(Arc::new(PostgresSchemaReferentialConstraintsProvider::new()));
        }

        if tp.eq_ignore_ascii_case("information_schema.table_constraints") {
            return Some(Arc::new(PostgresSchemaTableConstraintsProvider::new()));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_tables") {
            return Some(Arc::new(PgCatalogTableProvider::new(&context.meta.cubes)));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_type") {
            return Some(Arc::new(PgCatalogTypeProvider::new(&context.meta.tables)));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_namespace") {
            return Some(Arc::new(PgCatalogNamespaceProvider::new()));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_range") {
            return Some(Arc::new(PgCatalogRangeProvider::new()));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_attrdef") {
            return Some(Arc::new(PgCatalogAttrdefProvider::new()));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_attribute") {
            return Some(Arc::new(PgCatalogAttributeProvider::new(
                &context.meta.tables,
            )));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_index") {
            return Some(Arc::new(PgCatalogIndexProvider::new()));
        }
        if tp.eq_ignore_ascii_case("pg_catalog.pg_class") {
            return Some(Arc::new(PgCatalogClassProvider::new(&context.meta.tables)));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_proc") {
            return Some(Arc::new(PgCatalogProcProvider::new()));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_settings") {
            return Some(Arc::new(PgCatalogSettingsProvider::new(
                context
                    .sessions
                    .server
                    .all_variables(context.session_state.protocol.clone()),
            )));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_description") {
            return Some(Arc::new(PgCatalogDescriptionProvider::new()));
        }

        if tp.eq_ignore_ascii_case("pg_catalog.pg_constraint") {
            return Some(Arc::new(PgCatalogConstraintProvider::new()));
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
                        match c.get_column_type() {
                            ColumnType::String => DataType::Utf8,
                            ColumnType::VarStr => DataType::Utf8,
                            ColumnType::Boolean => DataType::Boolean,
                            ColumnType::Double => DataType::Float64,
                            ColumnType::Int8 => DataType::Int64,
                            ColumnType::Int32 => DataType::Int64,
                            ColumnType::Int64 => DataType::Int64,
                            ColumnType::Blob => DataType::Utf8,
                            ColumnType::Timestamp => {
                                DataType::Timestamp(TimeUnit::Millisecond, None)
                            }
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Plan(format!(
            "Not rewritten table scan node for '{}' cube",
            self.cube.name
        )))
    }
}
