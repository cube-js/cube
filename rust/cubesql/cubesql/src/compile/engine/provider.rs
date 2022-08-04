use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::V1CubeMeta;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    datasource::{self, TableProvider},
    error::DataFusionError,
    execution::context::SessionState as DFSessionState,
    logical_plan::Expr,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF, udtf::TableUDF, ExecutionPlan},
    sql::planner::ContextProvider,
};

use crate::{
    compile::MetaContext,
    sql::{session::DatabaseProtocol, ColumnType, SessionManager, SessionState},
    transport::V1CubeMetaExt,
    CubeError,
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
    constraint_column_usage::InfoSchemaConstraintColumnUsageProvider as PostgresSchemaConstraintColumnUsageProvider,
    key_column_usage::InfoSchemaKeyColumnUsageProvider as PostgresSchemaKeyColumnUsageProvider,
    referential_constraints::InfoSchemaReferentialConstraintsProvider as PostgresSchemaReferentialConstraintsProvider,
    table_constraints::InfoSchemaTableConstraintsProvider as PostgresSchemaTableConstraintsProvider,
    tables::InfoSchemaTableProvider as PostgresSchemaTableProvider,
    views::InfoSchemaViewsProvider as PostgresSchemaViewsProvider,
    InfoSchemaRoleColumnGrantsProvider as PostgresInfoSchemaRoleColumnGrantsProvider,
    InfoSchemaRoleTableGrantsProvider as PostgresInfoSchemaRoleTableGrantsProvider,
    InfoSchemaTestingBlockingProvider, InfoSchemaTestingDatasetProvider, PgCatalogAmProvider,
    PgCatalogAttrdefProvider, PgCatalogAttributeProvider, PgCatalogClassProvider,
    PgCatalogConstraintProvider, PgCatalogDatabaseProvider, PgCatalogDependProvider,
    PgCatalogDescriptionProvider, PgCatalogEnumProvider, PgCatalogIndexProvider,
    PgCatalogMatviewsProvider, PgCatalogNamespaceProvider, PgCatalogProcProvider,
    PgCatalogRangeProvider, PgCatalogRolesProvider, PgCatalogSequenceProvider,
    PgCatalogSettingsProvider, PgCatalogStatActivityProvider, PgCatalogStatioUserTablesProvider,
    PgCatalogTableProvider, PgCatalogTypeProvider,
};

use super::information_schema::redshift::RedshiftSvvTablesTableProvider;

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

    pub fn get_function<T>(&self, name: &str, udfs: &HashMap<String, Arc<T>>) -> Option<Arc<T>> {
        if name.starts_with("pg_catalog.") {
            return udfs.get(&format!("{}", &name[11..name.len()])).cloned();
        }

        udfs.get(name).cloned()
    }
}

impl ContextProvider for CubeContext {
    fn get_table_provider(
        &self,
        tr: datafusion::catalog::TableReference,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        return self.session_state.protocol.get_provider(&self.clone(), tr);
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.get_function(name, &self.state.scalar_functions)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.get_function(name, &self.state.aggregate_functions)
    }

    fn get_table_function_meta(&self, name: &str) -> Option<Arc<TableUDF>> {
        self.get_function(name, &self.state.table_functions)
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        Some(DataType::Utf8)
    }
}

impl DatabaseProtocol {
    fn get_provider(
        &self,
        context: &CubeContext,
        tr: datafusion::catalog::TableReference,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        match self {
            DatabaseProtocol::MySQL => self.get_mysql_provider(context, tr),
            DatabaseProtocol::PostgreSQL => self.get_postgres_provider(context, tr),
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

    pub fn get_postgres_table_name(
        &self,
        table_provider: Arc<dyn datasource::TableProvider>,
    ) -> Result<String, CubeError> {
        let any = table_provider.as_any();
        Ok(if let Some(t) = any.downcast_ref::<CubeTableProvider>() {
            t.table_name().to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaColumnsProvider>() {
            "information_schema.columns".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaTableProvider>() {
            "information_schema.tables".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaCharacterSetsProvider>() {
            "information_schema.character_sets".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaKeyColumnUsageProvider>() {
            "information_schema.key_column_usage".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaReferentialConstraintsProvider>() {
            "information_schema.referential_constraints".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaTableConstraintsProvider>() {
            "information_schema.table_constraints".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresInfoSchemaRoleTableGrantsProvider>() {
            "information_schema.role_table_grants".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresInfoSchemaRoleColumnGrantsProvider>() {
            "information_schema.role_column_grants".to_string()
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
        } else if let Some(_) = any.downcast_ref::<PgCatalogDependProvider>() {
            "pg_catalog.pg_depend".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogAmProvider>() {
            "pg_catalog.pg_am".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogEnumProvider>() {
            "pg_catalog.pg_enum".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogMatviewsProvider>() {
            "pg_catalog.pg_matviews".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogDatabaseProvider>() {
            "pg_catalog.pg_database".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogRolesProvider>() {
            "pg_catalog.pg_roles".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogStatActivityProvider>() {
            "pg_catalog.pg_stat_activity".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogStatioUserTablesProvider>() {
            "pg_catalog.pg_statio_user_tables".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogSequenceProvider>() {
            "pg_catalog.pg_sequence".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftSvvTablesTableProvider>() {
            "public.svv_tables".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaConstraintColumnUsageProvider>() {
            "information_schema.constraint_column_usage".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaViewsProvider>() {
            "information_schema.views".to_string()
        } else if let Some(_) = any.downcast_ref::<InfoSchemaTestingDatasetProvider>() {
            "information_schema.testing_dataset".to_string()
        } else if let Some(_) = any.downcast_ref::<InfoSchemaTestingBlockingProvider>() {
            "information_schema.testing_blocking".to_string()
        } else {
            return Err(CubeError::internal(format!(
                "Unknown table provider with schema: {:?}",
                table_provider.schema()
            )));
        })
    }

    fn get_postgres_provider(
        &self,
        context: &CubeContext,
        tr: datafusion::catalog::TableReference,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        let (_, schema, table) = match tr {
            datafusion::catalog::TableReference::Partial { schema, table, .. } => (
                "db".to_string(),
                schema.to_ascii_lowercase(),
                table.to_ascii_lowercase(),
            ),
            datafusion::catalog::TableReference::Full {
                catalog,
                schema,
                table,
            } => (
                catalog.to_ascii_lowercase(),
                schema.to_ascii_lowercase(),
                table.to_ascii_lowercase(),
            ),
            datafusion::catalog::TableReference::Bare { table } => {
                if table.starts_with("pg_") {
                    (
                        "db".to_string(),
                        "pg_catalog".to_string(),
                        table.to_ascii_lowercase(),
                    )
                } else {
                    (
                        "db".to_string(),
                        "public".to_string(),
                        table.to_ascii_lowercase(),
                    )
                }
            }
        };

        match schema.as_str() {
            "public" => {
                if let Some(cube) = context
                    .meta
                    .cubes
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&table))
                {
                    return Some(Arc::new(CubeTableProvider::new(cube.clone())));
                    // TODO .clone()
                };

                // TODO: Move to pg_catalog, support SEARCH PATH.
                // Redshift
                match table.as_str() {
                    "svv_tables" => {
                        return Some(Arc::new(RedshiftSvvTablesTableProvider::new(
                            &context.meta.cubes,
                        )))
                    }
                    _ => {}
                };
            }
            "information_schema" => match table.as_str() {
                "columns" => {
                    return Some(Arc::new(PostgresSchemaColumnsProvider::new(
                        &context.meta.cubes,
                    )))
                }
                "tables" => {
                    return Some(Arc::new(PostgresSchemaTableProvider::new(
                        &context.meta.cubes,
                    )))
                }
                "character_sets" => {
                    return Some(Arc::new(PostgresSchemaCharacterSetsProvider::new(
                        &context.session_state.database().unwrap_or("db".to_string()),
                    )))
                }
                "key_column_usage" => {
                    return Some(Arc::new(PostgresSchemaKeyColumnUsageProvider::new()))
                }
                "referential_constraints" => {
                    return Some(Arc::new(PostgresSchemaReferentialConstraintsProvider::new()))
                }
                "role_table_grants" => {
                    return Some(Arc::new(PostgresInfoSchemaRoleTableGrantsProvider::new(
                        context.session_state.user().unwrap_or("test".to_string()),
                        &context.meta.cubes,
                    )))
                }
                "role_column_grants" => {
                    return Some(Arc::new(PostgresInfoSchemaRoleColumnGrantsProvider::new(
                        context.session_state.user().unwrap_or("test".to_string()),
                        &context.meta.cubes,
                    )))
                }
                "table_constraints" => {
                    return Some(Arc::new(PostgresSchemaTableConstraintsProvider::new()))
                }
                "constraint_column_usage" => {
                    return Some(Arc::new(PostgresSchemaConstraintColumnUsageProvider::new()))
                }
                "views" => return Some(Arc::new(PostgresSchemaViewsProvider::new())),
                #[cfg(debug_assertions)]
                "testing_dataset" => {
                    return Some(Arc::new(InfoSchemaTestingDatasetProvider::new(5, 1000)))
                }
                #[cfg(debug_assertions)]
                "testing_blocking" => {
                    return Some(Arc::new(InfoSchemaTestingBlockingProvider::new()))
                }
                _ => return None,
            },
            "pg_catalog" => match table.as_str() {
                "pg_tables" => {
                    return Some(Arc::new(PgCatalogTableProvider::new(&context.meta.cubes)))
                }
                "pg_type" => {
                    return Some(Arc::new(PgCatalogTypeProvider::new(&context.meta.tables)))
                }
                "pg_namespace" => return Some(Arc::new(PgCatalogNamespaceProvider::new())),
                "pg_range" => return Some(Arc::new(PgCatalogRangeProvider::new())),
                "pg_attrdef" => return Some(Arc::new(PgCatalogAttrdefProvider::new())),
                "pg_attribute" => {
                    return Some(Arc::new(PgCatalogAttributeProvider::new(
                        &context.meta.tables,
                    )))
                }
                "pg_index" => return Some(Arc::new(PgCatalogIndexProvider::new())),
                "pg_class" => {
                    return Some(Arc::new(PgCatalogClassProvider::new(&context.meta.tables)))
                }
                "pg_proc" => return Some(Arc::new(PgCatalogProcProvider::new())),
                "pg_settings" => {
                    return Some(Arc::new(PgCatalogSettingsProvider::new(
                        context.session_state.all_variables(),
                    )))
                }
                "pg_description" => return Some(Arc::new(PgCatalogDescriptionProvider::new())),
                "pg_constraint" => return Some(Arc::new(PgCatalogConstraintProvider::new())),
                "pg_depend" => return Some(Arc::new(PgCatalogDependProvider::new())),
                "pg_am" => return Some(Arc::new(PgCatalogAmProvider::new())),
                "pg_enum" => return Some(Arc::new(PgCatalogEnumProvider::new())),
                "pg_matviews" => return Some(Arc::new(PgCatalogMatviewsProvider::new())),
                "pg_database" => {
                    return Some(Arc::new(PgCatalogDatabaseProvider::new(
                        &context.session_state.database().unwrap_or("db".to_string()),
                    )))
                }
                "pg_roles" => {
                    return Some(Arc::new(PgCatalogRolesProvider::new(
                        &context.session_state.user().unwrap_or("test".to_string()),
                    )))
                }
                "pg_stat_activity" => {
                    return Some(Arc::new(PgCatalogStatActivityProvider::new(
                        context.sessions.clone(),
                    )))
                }
                "pg_statio_user_tables" => {
                    return Some(Arc::new(PgCatalogStatioUserTablesProvider::new(
                        &context.meta.tables,
                    )))
                }
                "pg_sequence" => return Some(Arc::new(PgCatalogSequenceProvider::new())),
                _ => return None,
            },
            _ => return None,
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
                            ColumnType::Date(large) => {
                                if large {
                                    DataType::Date64
                                } else {
                                    DataType::Date32
                                }
                            }
                            ColumnType::Interval(unit) => DataType::Interval(unit),
                            ColumnType::String => DataType::Utf8,
                            ColumnType::VarStr => DataType::Utf8,
                            ColumnType::Boolean => DataType::Boolean,
                            ColumnType::Double => DataType::Float64,
                            ColumnType::Int8 => DataType::Int64,
                            ColumnType::Int32 => DataType::Int64,
                            ColumnType::Int64 => DataType::Int64,
                            ColumnType::Blob => DataType::Utf8,
                            ColumnType::Decimal(p, s) => DataType::Decimal(p, s),
                            ColumnType::List(field) => DataType::List(field.clone()),
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
