use std::sync::Arc;

use datafusion::datasource::TableProvider;

use super::information_schema::postgres::{
    character_sets::InfoSchemaCharacterSetsProvider as PostgresSchemaCharacterSetsProvider,
    columns::InfoSchemaColumnsProvider as PostgresSchemaColumnsProvider,
    constraint_column_usage::InfoSchemaConstraintColumnUsageProvider as PostgresSchemaConstraintColumnUsageProvider,
    key_column_usage::InfoSchemaKeyColumnUsageProvider as PostgresSchemaKeyColumnUsageProvider,
    referential_constraints::InfoSchemaReferentialConstraintsProvider as PostgresSchemaReferentialConstraintsProvider,
    schemata::InfoSchemaSchemataProvider as PostgresSchemaSchemataProvider,
    table_constraints::InfoSchemaTableConstraintsProvider as PostgresSchemaTableConstraintsProvider,
    tables::InfoSchemaTableProvider as PostgresSchemaTableProvider,
    views::InfoSchemaViewsProvider as PostgresSchemaViewsProvider,
    InfoSchemaRoleColumnGrantsProvider as PostgresInfoSchemaRoleColumnGrantsProvider,
    InfoSchemaRoleTableGrantsProvider as PostgresInfoSchemaRoleTableGrantsProvider,
    InfoSchemaSqlImplementationInfoProvider as PostgresInfoSchemaSqlImplementationInfoProvider,
    InfoSchemaSqlSizingProvider as PostgresInfoSchemaSqlSizingProvider,
    InfoSchemaTestingBlockingProvider, InfoSchemaTestingDatasetProvider, PgCatalogAmProvider,
    PgCatalogAttrdefProvider, PgCatalogAttributeProvider, PgCatalogClassProvider,
    PgCatalogConstraintProvider, PgCatalogDatabaseProvider, PgCatalogDependProvider,
    PgCatalogDescriptionProvider, PgCatalogEnumProvider, PgCatalogExtensionProvider,
    PgCatalogIndexProvider, PgCatalogInheritsProvider, PgCatalogMatviewsProvider,
    PgCatalogNamespaceProvider, PgCatalogPartitionedTableProvider, PgCatalogProcProvider,
    PgCatalogRangeProvider, PgCatalogRolesProvider, PgCatalogSequenceProvider,
    PgCatalogSettingsProvider, PgCatalogShdescriptionProvider, PgCatalogStatActivityProvider,
    PgCatalogStatUserTablesProvider, PgCatalogStatioUserTablesProvider, PgCatalogStatsProvider,
    PgCatalogTableProvider, PgCatalogTypeProvider, PgCatalogUserProvider, PgCatalogViewsProvider,
    PgPreparedStatementsProvider,
};
use crate::{
    compile::{
        engine::{CubeContext, CubeTableProvider, TableName},
        DatabaseProtocol,
    },
    sql::temp_tables::TempTableProvider,
    CubeError,
};

use super::information_schema::redshift::{
    RedshiftLateBindingViewUnpackedTableProvider, RedshiftPgExternalSchemaProvider,
    RedshiftStlDdltextProvider, RedshiftStlQueryProvider, RedshiftStlQuerytextProvider,
    RedshiftStvSlicesProvider, RedshiftSvvExternalSchemasTableProvider,
    RedshiftSvvTableInfoProvider, RedshiftSvvTablesTableProvider,
};

impl DatabaseProtocol {
    pub fn get_postgres_table_name(
        &self,
        table_provider: Arc<dyn TableProvider>,
    ) -> Result<String, CubeError> {
        let any = table_provider.as_any();
        Ok(if let Some(t) = any.downcast_ref::<TempTableProvider>() {
            t.name().to_string()
        } else if let Some(t) = any.downcast_ref::<CubeTableProvider>() {
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
        } else if let Some(_) = any.downcast_ref::<PostgresSchemaSchemataProvider>() {
            "information_schema.schemata".to_string()
        } else if let Some(_) =
            any.downcast_ref::<PostgresInfoSchemaSqlImplementationInfoProvider>()
        {
            "information_schema.sql_implementation_info".to_string()
        } else if let Some(_) = any.downcast_ref::<PostgresInfoSchemaSqlSizingProvider>() {
            "information_schema.sql_sizing".to_string()
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
        } else if let Some(_) = any.downcast_ref::<PgPreparedStatementsProvider>() {
            "pg_catalog.pg_prepared_statements".to_string()
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
        } else if let Some(_) = any.downcast_ref::<PgCatalogStatsProvider>() {
            "pg_catalog.pg_stats".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogUserProvider>() {
            "pg_catalog.pg_user".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogExtensionProvider>() {
            "pg_catalog.pg_extension".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogPartitionedTableProvider>() {
            "pg_catalog.pg_partitioned_table".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogInheritsProvider>() {
            "pg_catalog.pg_inherits".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogViewsProvider>() {
            "pg_catalog.pg_views".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogStatUserTablesProvider>() {
            "pg_catalog.pg_stat_user_tables".to_string()
        } else if let Some(_) = any.downcast_ref::<PgCatalogShdescriptionProvider>() {
            "pg_catalog.pg_shdescription".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftPgExternalSchemaProvider>() {
            "pg_catalog.pg_external_schema".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftSvvTablesTableProvider>() {
            "public.svv_tables".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftSvvExternalSchemasTableProvider>() {
            "public.svv_external_schemas".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftSvvTableInfoProvider>() {
            "public.svv_table_info".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftStvSlicesProvider>() {
            "public.stv_slices".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftStlDdltextProvider>() {
            "public.stl_ddltext".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftStlQueryProvider>() {
            "public.stl_query".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftStlQuerytextProvider>() {
            "public.stl_querytext".to_string()
        } else if let Some(_) = any.downcast_ref::<RedshiftLateBindingViewUnpackedTableProvider>() {
            "public.get_late_binding_view_cols_unpacked".to_string()
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

    pub(crate) fn get_postgres_provider(
        &self,
        context: &CubeContext,
        tr: datafusion::catalog::TableReference,
    ) -> Option<Arc<dyn TableProvider>> {
        let (_, schema, table) = match tr {
            datafusion::catalog::TableReference::Partial { schema, table, .. } => (
                context.session_state.database().unwrap_or("db".to_string()),
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
                let table_lower = table.to_ascii_lowercase();
                let schema = if context.session_state.temp_tables().has(&table_lower) {
                    "pg_temp_3"
                } else if table.starts_with("pg_") {
                    "pg_catalog"
                } else {
                    "public"
                };
                (
                    context.session_state.database().unwrap_or("db".to_string()),
                    schema.to_string(),
                    table_lower,
                )
            }
        };

        match schema.as_str() {
            "pg_temp_3" => {
                if let Some(temp_table) = context.session_state.temp_tables().get(&table) {
                    return Some(Arc::new(TempTableProvider::new(table, temp_table)));
                }
            }
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
                            &context.session_state.database().unwrap_or("db".to_string()),
                            &context.meta.cubes,
                        )))
                    }
                    "svv_external_schemas" => {
                        return Some(Arc::new(RedshiftSvvExternalSchemasTableProvider::new()))
                    }
                    "svv_table_info" => {
                        return Some(Arc::new(RedshiftSvvTableInfoProvider::new(
                            &context.session_state.database().unwrap_or("db".to_string()),
                            &context.meta.tables,
                        )))
                    }
                    "stv_slices" => return Some(Arc::new(RedshiftStvSlicesProvider::new())),
                    "stl_ddltext" => return Some(Arc::new(RedshiftStlDdltextProvider::new())),
                    "stl_query" => return Some(Arc::new(RedshiftStlQueryProvider::new())),
                    "stl_querytext" => return Some(Arc::new(RedshiftStlQuerytextProvider::new())),
                    "get_late_binding_view_cols_unpacked" => {
                        return Some(Arc::new(RedshiftLateBindingViewUnpackedTableProvider::new()))
                    }
                    _ => {}
                };
            }
            "information_schema" => match table.as_str() {
                "columns" => {
                    return Some(Arc::new(PostgresSchemaColumnsProvider::new(
                        &context.session_state.database().unwrap_or("db".to_string()),
                        &context.meta.cubes,
                    )))
                }
                "tables" => {
                    return Some(Arc::new(PostgresSchemaTableProvider::new(
                        &context.session_state.database().unwrap_or("db".to_string()),
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
                        &context.session_state.database().unwrap_or("db".to_string()),
                        &context.session_state.user().unwrap_or("test".to_string()),
                        &context.meta.cubes,
                    )))
                }
                "role_column_grants" => {
                    return Some(Arc::new(PostgresInfoSchemaRoleColumnGrantsProvider::new(
                        &context.session_state.database().unwrap_or("db".to_string()),
                        &context.session_state.user().unwrap_or("test".to_string()),
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
                "schemata" => {
                    return Some(Arc::new(PostgresSchemaSchemataProvider::new(
                        &context.session_state.database().unwrap_or("db".to_string()),
                    )))
                }
                "sql_implementation_info" => {
                    return Some(Arc::new(
                        PostgresInfoSchemaSqlImplementationInfoProvider::new(),
                    ))
                }
                "sql_sizing" => return Some(Arc::new(PostgresInfoSchemaSqlSizingProvider::new())),
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
                    return Some(Arc::new(PgCatalogTableProvider::new(
                        &context.session_state.user().unwrap_or("test".to_string()),
                        &context.meta.cubes,
                    )))
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
                "pg_description" => {
                    return Some(Arc::new(PgCatalogDescriptionProvider::new(
                        &context.meta.tables,
                    )))
                }
                "pg_constraint" => return Some(Arc::new(PgCatalogConstraintProvider::new())),
                "pg_depend" => return Some(Arc::new(PgCatalogDependProvider::new())),
                "pg_am" => return Some(Arc::new(PgCatalogAmProvider::new())),
                "pg_enum" => return Some(Arc::new(PgCatalogEnumProvider::new())),
                "pg_matviews" => return Some(Arc::new(PgCatalogMatviewsProvider::new())),
                "pg_prepared_statements" => {
                    return Some(Arc::new(PgPreparedStatementsProvider::new(
                        context.session_state.clone(),
                    )))
                }
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
                "pg_stats" => {
                    return Some(Arc::new(PgCatalogStatsProvider::new(&context.meta.tables)))
                }
                "pg_user" => {
                    return Some(Arc::new(PgCatalogUserProvider::new(
                        &context.session_state.user().unwrap_or("test".to_string()),
                    )))
                }
                "pg_extension" => return Some(Arc::new(PgCatalogExtensionProvider::new())),
                "pg_partitioned_table" => {
                    return Some(Arc::new(PgCatalogPartitionedTableProvider::new()))
                }
                "pg_inherits" => return Some(Arc::new(PgCatalogInheritsProvider::new())),
                "pg_views" => return Some(Arc::new(PgCatalogViewsProvider::new())),
                "pg_stat_user_tables" => {
                    return Some(Arc::new(PgCatalogStatUserTablesProvider::new(
                        &context.meta.tables,
                    )))
                }
                "pg_shdescription" => return Some(Arc::new(PgCatalogShdescriptionProvider::new())),
                "pg_external_schema" => {
                    return Some(Arc::new(RedshiftPgExternalSchemaProvider::new()))
                }
                _ => return None,
            },
            _ => return None,
        }

        None
    }
}
