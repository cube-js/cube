use core::fmt;
use cubeclient::models::V1LoadRequestQuery;
use datafusion::{
    arrow::datatypes::DataType,
    execution::context::{
        default_session_builder, SessionConfig as DFSessionConfig,
        SessionContext as DFSessionContext,
    },
    logical_plan::{
        plan::{Analyze, Explain, Extension, Projection, ToStringifiedPlan},
        DFField, DFSchema, DFSchemaRef, Expr, LogicalPlan, PlanType, PlanVisitor, ToDFSchema,
    },
    optimizer::{
        optimizer::{OptimizerConfig, OptimizerRule},
        projection_drop_out::ProjectionDropOut,
    },
    physical_plan::ExecutionPlan,
    prelude::*,
    scalar::ScalarValue,
    sql::{parser::Statement as DFStatement, planner::SqlToRel},
    variable::VarType,
};
use itertools::Itertools;
use log::warn;
use serde::Serialize;
use sqlparser::ast::{self, escape_single_quote_string};
use std::{
    backtrace::Backtrace, collections::HashMap, env, fmt::Formatter, future::Future, pin::Pin,
    sync::Arc,
};

use self::{
    builder::*,
    context::*,
    engine::{
        context::VariablesProvider,
        df::{
            optimizers::{FilterPushDown, LimitPushDown, SortPushDown},
            planner::CubeQueryPlanner,
            scan::{CubeScanNode, MemberField},
        },
        information_schema::mysql::ext::CubeColumnMySqlExt,
        provider::CubeContext,
        udf::{
            create_array_lower_udf, create_array_to_string_udf, create_array_upper_udf,
            create_charindex_udf, create_connection_id_udf, create_convert_tz_udf,
            create_cube_regclass_cast_udf, create_current_schema_udf, create_current_schemas_udf,
            create_current_setting_udf, create_current_timestamp_udf, create_current_user_udf,
            create_date_add_udf, create_date_sub_udf, create_date_to_timestamp_udf,
            create_date_udf, create_dateadd_udf, create_datediff_udf, create_dayofmonth_udf,
            create_dayofweek_udf, create_dayofyear_udf, create_db_udf, create_ends_with_udf,
            create_format_type_udf, create_generate_series_udtf, create_generate_subscripts_udtf,
            create_has_schema_privilege_udf, create_hour_udf, create_if_udf, create_instr_udf,
            create_interval_mul_udf, create_isnull_udf, create_json_build_object_udf,
            create_least_udf, create_locate_udf, create_makedate_udf, create_measure_udaf,
            create_minute_udf, create_pg_backend_pid_udf, create_pg_datetime_precision_udf,
            create_pg_encoding_to_char_udf, create_pg_expandarray_udtf,
            create_pg_get_constraintdef_udf, create_pg_get_expr_udf, create_pg_get_indexdef_udf,
            create_pg_get_serial_sequence_udf, create_pg_get_userbyid_udf,
            create_pg_is_other_temp_schema, create_pg_my_temp_schema,
            create_pg_numeric_precision_udf, create_pg_numeric_scale_udf,
            create_pg_table_is_visible_udf, create_pg_total_relation_size_udf,
            create_pg_truetypid_udf, create_pg_truetypmod_udf, create_pg_type_is_visible_udf,
            create_position_udf, create_quarter_udf, create_quote_ident_udf,
            create_regexp_substr_udf, create_second_udf, create_session_user_udf, create_sha1_udf,
            create_str_to_date_udf, create_time_format_udf, create_timediff_udf,
            create_to_char_udf, create_to_date_udf, create_to_regtype_udf, create_ucase_udf,
            create_unnest_udtf, create_user_udf, create_version_udf, create_year_udf,
            register_fun_stubs,
        },
    },
    parser::parse_sql_to_statement,
    qtrace::Qtrace,
    rewrite::converter::LogicalPlanToLanguageConverter,
};
use crate::{
    compile::engine::df::scan::CubeScanOptions,
    sql::{
        database_variables::{DatabaseVariable, DatabaseVariablesToUpdate},
        dataframe,
        session::DatabaseProtocol,
        statement::{
            ApproximateCountDistinctVisitor, CastReplacer, RedshiftDatePartReplacer,
            SensitiveDataSanitizer, ToTimestampReplacer, UdfWildcardArgReplacer,
        },
        types::{CommandCompletion, StatusFlags},
        ColumnFlags, ColumnType, Session, SessionManager, SessionState,
    },
    transport::{df_data_type_by_column_type, V1CubeMetaExt},
    CubeError, CubeErrorCauseType,
};

pub mod builder;
pub mod context;
pub mod engine;
pub mod error;
mod legacy_compiler;
pub mod parser;
pub mod qtrace;
pub mod rewrite;
pub mod service;

pub mod test;

pub use crate::transport::ctx::*;
pub use error::{CompilationError, CompilationResult};

#[derive(Clone)]
struct QueryPlanner {
    state: Arc<SessionState>,
    meta: Arc<MetaContext>,
    session_manager: Arc<SessionManager>,
}

impl QueryPlanner {
    pub fn new(
        state: Arc<SessionState>,
        meta: Arc<MetaContext>,
        session_manager: Arc<SessionManager>,
    ) -> Self {
        Self {
            state,
            meta,
            session_manager,
        }
    }

    /// Common case for both planners: meta & olap
    /// This method tries to detect what planner to use as earlier as possible
    /// and forward context to correct planner
    async fn select_to_plan(
        &self,
        stmt: &ast::Statement,
        q: &Box<ast::Query>,
        qtrace: &mut Option<Qtrace>,
    ) -> CompilationResult<QueryPlan> {
        // TODO move CUBESQL_REWRITE_ENGINE env to config
        let rewrite_engine = env::var("CUBESQL_REWRITE_ENGINE")
            .ok()
            .map(|v| v.parse::<bool>().unwrap())
            .unwrap_or(self.state.protocol == DatabaseProtocol::PostgreSQL);
        if rewrite_engine {
            return self.create_df_logical_plan(stmt.clone(), qtrace).await;
        }

        let select = match &q.body {
            sqlparser::ast::SetExpr::Select(select) => select,
            _ => {
                return Err(CompilationError::unsupported(
                    "Unsupported Query".to_string(),
                ));
            }
        };

        if select.into.is_some() {
            return Err(CompilationError::unsupported(
                "Unsupported query type: SELECT INTO".to_string(),
            ));
        }

        let from_table = if select.from.len() == 1 {
            &select.from[0]
        } else {
            return self.create_df_logical_plan(stmt.clone(), qtrace).await;
        };

        let (db_name, schema_name, table_name) = match &from_table.relation {
            ast::TableFactor::Table { name, .. } => match name {
                ast::ObjectName(identifiers) => {
                    match identifiers.len() {
                        // db.`KibanaSampleDataEcommerce`
                        2 => match self.state.protocol {
                            DatabaseProtocol::MySQL => (
                                identifiers[0].value.clone(),
                                "public".to_string(),
                                identifiers[1].value.clone(),
                            ),
                            DatabaseProtocol::PostgreSQL => (
                                "db".to_string(),
                                identifiers[0].value.clone(),
                                identifiers[1].value.clone(),
                            ),
                        },
                        // `KibanaSampleDataEcommerce`
                        1 => match self.state.protocol {
                            DatabaseProtocol::MySQL => (
                                "db".to_string(),
                                "public".to_string(),
                                identifiers[0].value.clone(),
                            ),
                            DatabaseProtocol::PostgreSQL => (
                                "db".to_string(),
                                "public".to_string(),
                                identifiers[0].value.clone(),
                            ),
                        },
                        _ => {
                            return Err(CompilationError::unsupported(format!(
                                "Table identifier: {:?}",
                                identifiers
                            )));
                        }
                    }
                }
            },
            factor => {
                return Err(CompilationError::unsupported(format!(
                    "table factor: {:?}",
                    factor
                )));
            }
        };

        match self.state.protocol {
            DatabaseProtocol::MySQL => {
                if db_name.to_lowercase() == "information_schema"
                    || db_name.to_lowercase() == "performance_schema"
                {
                    return self.create_df_logical_plan(stmt.clone(), &mut None).await;
                }
            }
            DatabaseProtocol::PostgreSQL => {
                if schema_name.to_lowercase() == "information_schema"
                    || schema_name.to_lowercase() == "performance_schema"
                    || schema_name.to_lowercase() == "pg_catalog"
                {
                    return self.create_df_logical_plan(stmt.clone(), qtrace).await;
                }
            }
        };

        if db_name.to_lowercase() != "db" {
            return Err(CompilationError::unsupported(format!(
                "Unable to access database {}",
                db_name
            )));
        }

        if !select.from[0].joins.is_empty() {
            return Err(CompilationError::unsupported(
                "Query with JOIN instruction(s)".to_string(),
            ));
        }

        if q.with.is_some() {
            return Err(CompilationError::unsupported(
                "Query with CTE instruction(s)".to_string(),
            ));
        }

        if !select.cluster_by.is_empty() {
            return Err(CompilationError::unsupported(
                "Query with CLUSTER BY instruction(s)".to_string(),
            ));
        }

        if !select.distribute_by.is_empty() {
            return Err(CompilationError::unsupported(
                "Query with DISTRIBUTE BY instruction(s)".to_string(),
            ));
        }

        if select.having.is_some() {
            return Err(CompilationError::unsupported(
                "Query with HAVING instruction(s)".to_string(),
            ));
        }

        // @todo Better solution?
        // Metabase
        if q.to_string()
            == format!(
                "SELECT true AS `_` FROM `{}` WHERE 1 <> 1 LIMIT 0",
                table_name
            )
        {
            return Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Box::new(dataframe::DataFrame::new(
                    vec![dataframe::Column::new(
                        "_".to_string(),
                        ColumnType::Int8,
                        ColumnFlags::empty(),
                    )],
                    vec![],
                )),
            ));
        };

        if let Some(cube) = self.meta.find_cube_with_name(&table_name) {
            let mut ctx = QueryContext::new(&cube);
            let mut builder = legacy_compiler::compile_select(select, &mut ctx)?;

            if let Some(limit_expr) = &q.limit {
                let limit = limit_expr.to_string().parse::<i32>().map_err(|e| {
                    CompilationError::unsupported(format!(
                        "Unable to parse limit: {}",
                        e.to_string()
                    ))
                })?;

                builder.with_limit(limit);
            }

            if let Some(offset_expr) = &q.offset {
                let offset = offset_expr.value.to_string().parse::<i32>().map_err(|e| {
                    CompilationError::unsupported(format!(
                        "Unable to parse offset: {}",
                        e.to_string()
                    ))
                })?;

                builder.with_offset(offset);
            }

            legacy_compiler::compile_group(&select.group_by, &ctx, &mut builder)?;
            legacy_compiler::compile_order(&q.order_by, &ctx, &mut builder)?;

            if let Some(selection) = &select.selection {
                legacy_compiler::compile_where(selection, &ctx, &mut builder)?;
            }

            let query = builder.build();
            let schema = query.meta_as_df_schema();

            let projection_expr = query.meta_as_df_projection_expr();
            let projection_schema = query.meta_as_df_projection_schema();

            let scan_node = LogicalPlan::Extension(Extension {
                node: Arc::new(CubeScanNode::new(
                    schema.clone(),
                    schema
                        .fields()
                        .iter()
                        .map(|f| MemberField::Member(f.name().to_string()))
                        .collect(),
                    query.request,
                    // @todo Remove after split!
                    self.state.auth_context().unwrap(),
                    CubeScanOptions {
                        change_user: None,
                        max_records: None,
                    },
                )),
            });
            let logical_plan = LogicalPlan::Projection(Projection {
                expr: projection_expr,
                input: Arc::new(scan_node),
                schema: projection_schema,
                alias: None,
            });

            let ctx = self.create_execution_ctx();
            Ok(QueryPlan::DataFusionSelect(
                StatusFlags::empty(),
                logical_plan,
                ctx,
            ))
        } else {
            Err(CompilationError::user(format!(
                "Unknown cube '{}'. Please ensure your schema files are valid.",
                table_name,
            )))
        }
    }

    pub async fn plan(
        &self,
        stmt: &ast::Statement,
        qtrace: &mut Option<Qtrace>,
    ) -> CompilationResult<QueryPlan> {
        let plan = match (stmt, &self.state.protocol) {
            (ast::Statement::Query(q), _) => self.select_to_plan(stmt, q, qtrace).await,
            (ast::Statement::SetTransaction { .. }, _) => Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Box::new(dataframe::DataFrame::new(vec![], vec![])),
            )),
            (ast::Statement::SetNames { charset_name, .. }, DatabaseProtocol::MySQL) => {
                if !(charset_name.eq_ignore_ascii_case("utf8")
                    || charset_name.eq_ignore_ascii_case("utf8mb4"))
                {
                    warn!(
                        "SET NAME does not support non utf8 charsets, input: {}",
                        charset_name
                    );
                };

                Ok(QueryPlan::MetaTabular(
                    StatusFlags::empty(),
                    Box::new(dataframe::DataFrame::new(vec![], vec![])),
                ))
            }
            (ast::Statement::Kill { .. }, DatabaseProtocol::MySQL) => Ok(QueryPlan::MetaOk(
                StatusFlags::empty(),
                CommandCompletion::Select(0),
            )),
            (ast::Statement::SetVariable { key_values }, _) => {
                self.set_variable_to_plan(&key_values)
            }
            (ast::Statement::ShowVariable { variable }, _) => {
                self.show_variable_to_plan(variable).await
            }
            (ast::Statement::ShowVariables { filter }, DatabaseProtocol::MySQL) => {
                self.show_variables_to_plan(&filter).await
            }
            (ast::Statement::ShowCreate { obj_name, obj_type }, DatabaseProtocol::MySQL) => {
                self.show_create_to_plan(&obj_name, &obj_type)
            }
            (
                ast::Statement::ShowColumns {
                    extended,
                    full,
                    filter,
                    table_name,
                },
                DatabaseProtocol::MySQL,
            ) => {
                self.show_columns_to_plan(*extended, *full, &filter, &table_name)
                    .await
            }
            (
                ast::Statement::ShowTables {
                    extended,
                    full,
                    filter,
                    db_name,
                },
                DatabaseProtocol::MySQL,
            ) => {
                self.show_tables_to_plan(*extended, *full, &filter, &db_name)
                    .await
            }
            (ast::Statement::ShowCollation { filter }, DatabaseProtocol::MySQL) => {
                self.show_collation_to_plan(&filter).await
            }
            (ast::Statement::ExplainTable { table_name, .. }, DatabaseProtocol::MySQL) => {
                self.explain_table_to_plan(&table_name).await
            }
            (
                ast::Statement::Explain {
                    statement,
                    verbose,
                    analyze,
                    ..
                },
                _,
            ) => self.explain_to_plan(&statement, *verbose, *analyze).await,
            (ast::Statement::Use { db_name }, DatabaseProtocol::MySQL) => {
                self.use_to_plan(&db_name)
            }
            (ast::Statement::StartTransaction { .. }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Real support
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Begin,
                ))
            }
            (ast::Statement::Commit { .. }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Real support
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Commit,
                ))
            }
            (ast::Statement::Rollback { .. }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Real support
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Rollback,
                ))
            }
            (ast::Statement::Discard { object_type }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Cursors + Portals
                self.state.clear_prepared_statements().await;

                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Discard(object_type.to_string()),
                ))
            }
            _ => Err(CompilationError::unsupported(format!(
                "Unsupported query type: {}",
                stmt.to_string()
            ))),
        };

        match plan {
            Err(err) => {
                let meta = Some(HashMap::from([
                    ("query".to_string(), stmt.to_string()),
                    (
                        "sanitizedQuery".to_string(),
                        SensitiveDataSanitizer::new().replace(stmt).to_string(),
                    ),
                ]));
                let msg = err.message();
                Err(err.with_message(msg).with_meta(meta))
            }
            _ => plan,
        }
    }

    async fn show_variable_to_plan(
        &self,
        variable: &Vec<ast::Ident>,
    ) -> CompilationResult<QueryPlan> {
        let name = variable.to_vec()[0].value.clone();
        if self.state.protocol == DatabaseProtocol::PostgreSQL {
            let full_variable = variable.iter().map(|v| v.value.to_lowercase()).join("_");
            let full_variable = match full_variable.as_str() {
                "transaction_isolation_level" => "transaction_isolation",
                x => x,
            };
            let stmt = if name.eq_ignore_ascii_case("all") {
                parse_sql_to_statement(
                    &"SELECT name, setting, short_desc as description FROM pg_catalog.pg_settings"
                        .to_string(),
                    self.state.protocol.clone(),
                    &mut None,
                )?
            } else {
                parse_sql_to_statement(
                    // TODO: column name might be expected to match variable name
                    &format!(
                        "SELECT setting FROM pg_catalog.pg_settings where name = '{}'",
                        escape_single_quote_string(full_variable),
                    ),
                    self.state.protocol.clone(),
                    &mut None,
                )?
            };

            self.create_df_logical_plan(stmt, &mut None).await
        } else if name.eq_ignore_ascii_case("databases") || name.eq_ignore_ascii_case("schemas") {
            Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Box::new(dataframe::DataFrame::new(
                    vec![dataframe::Column::new(
                        "Database".to_string(),
                        ColumnType::String,
                        ColumnFlags::empty(),
                    )],
                    vec![
                        dataframe::Row::new(vec![dataframe::TableValue::String("db".to_string())]),
                        dataframe::Row::new(vec![dataframe::TableValue::String(
                            "information_schema".to_string(),
                        )]),
                        dataframe::Row::new(vec![dataframe::TableValue::String(
                            "mysql".to_string(),
                        )]),
                        dataframe::Row::new(vec![dataframe::TableValue::String(
                            "performance_schema".to_string(),
                        )]),
                        dataframe::Row::new(vec![dataframe::TableValue::String("sys".to_string())]),
                    ],
                )),
            ))
        } else if name.eq_ignore_ascii_case("processlist") {
            let stmt = parse_sql_to_statement(
                &"SELECT * FROM information_schema.processlist".to_string(),
                self.state.protocol.clone(),
                &mut None,
            )?;

            self.create_df_logical_plan(stmt, &mut None).await
        } else if name.eq_ignore_ascii_case("warnings") {
            Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Box::new(dataframe::DataFrame::new(
                    vec![
                        dataframe::Column::new(
                            "Level".to_string(),
                            ColumnType::VarStr,
                            ColumnFlags::NOT_NULL,
                        ),
                        dataframe::Column::new(
                            "Code".to_string(),
                            ColumnType::Int32,
                            ColumnFlags::NOT_NULL | ColumnFlags::UNSIGNED,
                        ),
                        dataframe::Column::new(
                            "Message".to_string(),
                            ColumnType::VarStr,
                            ColumnFlags::NOT_NULL,
                        ),
                    ],
                    vec![],
                )),
            ))
        } else {
            self.create_df_logical_plan(
                ast::Statement::ShowVariable {
                    variable: variable.clone(),
                },
                &mut None,
            )
            .await
        }
    }

    async fn show_variables_to_plan(
        &self,
        filter: &Option<ast::ShowStatementFilter>,
    ) -> Result<QueryPlan, CompilationError> {
        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE VARIABLE_NAME {}", stmt.to_string())
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                return Err(CompilationError::unsupported(format!(
                    "Show variable doesnt support WHERE statement: {}",
                    stmt
                )))
            }
            Some(stmt @ ast::ShowStatementFilter::ILike(_)) => {
                return Err(CompilationError::user(format!(
                    "Show variable doesnt define ILIKE statement: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let stmt = parse_sql_to_statement(
            &format!("SELECT VARIABLE_NAME as Variable_name, VARIABLE_VALUE as Value FROM performance_schema.session_variables {} ORDER BY Variable_name DESC", filter),
            self.state.protocol.clone(),
            &mut None,
        )?;

        self.create_df_logical_plan(stmt, &mut None).await
    }

    fn show_create_to_plan(
        &self,
        obj_name: &ast::ObjectName,
        obj_type: &ast::ShowCreateObject,
    ) -> Result<QueryPlan, CompilationError> {
        match obj_type {
            ast::ShowCreateObject::Table => {}
            _ => {
                return Err(CompilationError::user(format!(
                    "SHOW CREATE doesn't support type: {}",
                    obj_type
                )))
            }
        };

        let table_name_filter = if obj_name.0.len() == 2 {
            &obj_name.0[1].value
        } else {
            &obj_name.0[0].value
        };

        self.meta.cubes.iter().find(|c| c.name.eq(table_name_filter)).map(|cube| {
            let mut fields: Vec<String> = vec![];

            for column in &cube.get_columns() {
                fields.push(format!(
                    "`{}` {}{}",
                    column.get_name(),
                    column.get_mysql_column_type(),
                    if column.sql_can_be_null() { " NOT NULL" } else { "" }
                ));
            }

            QueryPlan::MetaTabular(StatusFlags::empty(), Box::new(dataframe::DataFrame::new(
                vec![
                    dataframe::Column::new(
                        "Table".to_string(),
                        ColumnType::String,
                        ColumnFlags::empty(),
                    ),
                    dataframe::Column::new(
                        "Create Table".to_string(),
                        ColumnType::String,
                        ColumnFlags::empty(),
                    )
                ],
                vec![dataframe::Row::new(vec![
                    dataframe::TableValue::String(cube.name.clone()),
                    dataframe::TableValue::String(
                        format!("CREATE TABLE `{}` (\r\n  {}\r\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", cube.name, fields.join(",\r\n  "))
                    ),
                ])]
            )))
        }).ok_or(
            CompilationError::user(format!(
                "Unknown table: {}",
                table_name_filter
            ))
        )
    }

    async fn show_columns_to_plan(
        &self,
        extended: bool,
        full: bool,
        filter: &Option<ast::ShowStatementFilter>,
        table_name: &ast::ObjectName,
    ) -> Result<QueryPlan, CompilationError> {
        let extended = match extended {
            false => "".to_string(),
            // The planner is unable to correctly process queries with UNION ALL in subqueries as of writing this.
            // Uncomment this to enable EXTENDED support once such queries can be processed.
            /*true => {
                let extended_columns = "'' AS `Type`, NULL AS `Collation`, 'NO' AS `Null`, '' AS `Key`, NULL AS `Default`, '' AS `Extra`, 'select' AS `Privileges`, '' AS `Comment`";
                format!("UNION ALL SELECT 'DB_TRX_ID' AS `Field`, 2 AS `Order`, {} UNION ALL SELECT 'DB_ROLL_PTR' AS `Field`, 3 AS `Order`, {}", extended_columns, extended_columns)
            }*/
            true => {
                return Err(CompilationError::unsupported(
                    "SHOW COLUMNS: EXTENDED is not implemented".to_string(),
                ))
            }
        };

        let columns = match full {
            false => "`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`",
            true => "`Field`, `Type`, `Collation`, `Null`, `Key`, `Default`, `Extra`, `Privileges`, `Comment`",
        };

        let mut object_name = table_name.0.clone();
        let table_name = match object_name.pop() {
            Some(table_name) => escape_single_quote_string(&table_name.value).to_string(),
            None => {
                return Err(CompilationError::internal(format!(
                    "Unexpected lack of table name"
                )))
            }
        };
        let db_name = match object_name.pop() {
            Some(db_name) => escape_single_quote_string(&db_name.value).to_string(),
            None => self.state.database().unwrap_or("db".to_string()).clone(),
        };

        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE `Field` {}", stmt.to_string())
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                format!("{}", stmt.to_string())
            }
            Some(stmt) => {
                return Err(CompilationError::user(format!(
                    "SHOW COLUMNS doesn't support requested filter: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let information_schema_sql = format!("SELECT `COLUMN_NAME` AS `Field`, 1 AS `Order`, `COLUMN_TYPE` AS `Type`, IF(`DATA_TYPE` = 'varchar', 'utf8mb4_0900_ai_ci', NULL) AS `Collation`, `IS_NULLABLE` AS `Null`, `COLUMN_KEY` AS `Key`, NULL AS `Default`, `EXTRA` AS `Extra`, 'select' AS `Privileges`, `COLUMN_COMMENT` AS `Comment` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = '{}' AND `TABLE_SCHEMA` = '{}' {}", table_name, db_name, extended);
        let stmt = parse_sql_to_statement(
            &format!(
                "SELECT {} FROM ({}) AS `COLUMNS` {}",
                columns, information_schema_sql, filter
            ),
            self.state.protocol.clone(),
            &mut None,
        )?;

        self.create_df_logical_plan(stmt, &mut None).await
    }

    async fn show_tables_to_plan(
        &self,
        // EXTENDED is accepted but does not alter the result
        _extended: bool,
        full: bool,
        filter: &Option<ast::ShowStatementFilter>,
        db_name: &Option<ast::Ident>,
    ) -> Result<QueryPlan, CompilationError> {
        let db_name = match db_name {
            Some(db_name) => db_name.clone(),
            None => ast::Ident::new(self.state.database().unwrap_or("db".to_string())),
        };

        let column_name = format!("Tables_in_{}", db_name.value);
        let column_name = match db_name.quote_style {
            Some(quote_style) => ast::Ident::with_quote(quote_style, column_name),
            None => ast::Ident::new(column_name),
        };

        let columns = match full {
            false => format!("{}", column_name),
            true => format!("{}, `Table_type`", column_name),
        };

        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE {} {}", column_name, stmt)
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                format!("{}", stmt)
            }
            Some(stmt) => {
                return Err(CompilationError::user(format!(
                    "SHOW TABLES doesn't support requested filter: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let information_schema_sql = format!(
            "SELECT `TABLE_NAME` AS {}, `TABLE_TYPE` AS `Table_type` FROM `information_schema`.`TABLES`
WHERE `TABLE_SCHEMA` = '{}'",
            column_name,
            escape_single_quote_string(&db_name.value),
        );
        let stmt = parse_sql_to_statement(
            &format!(
                "SELECT {} FROM ({}) AS `TABLES` {}",
                columns, information_schema_sql, filter
            ),
            self.state.protocol.clone(),
            &mut None,
        )?;

        self.create_df_logical_plan(stmt, &mut None).await
    }

    async fn show_collation_to_plan(
        &self,
        filter: &Option<ast::ShowStatementFilter>,
    ) -> Result<QueryPlan, CompilationError> {
        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE `Collation` {}", stmt)
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                format!("{}", stmt)
            }
            Some(stmt) => {
                return Err(CompilationError::user(format!(
                    "SHOW COLLATION doesn't support requested filter: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let information_schema_sql = "SELECT `COLLATION_NAME` AS `Collation`, `CHARACTER_SET_NAME` AS `Charset`, `ID` AS `Id`, `IS_DEFAULT` AS `Default`, `IS_COMPILED` AS `Compiled`, `SORTLEN` AS `Sortlen`, `PAD_ATTRIBUTE` AS `Pad_attribute` FROM `information_schema`.`COLLATIONS` ORDER BY `Collation`";
        let stmt = parse_sql_to_statement(
            &format!(
                "SELECT * FROM ({}) AS `COLLATIONS` {}",
                information_schema_sql, filter
            ),
            self.state.protocol.clone(),
            &mut None,
        )?;

        self.create_df_logical_plan(stmt, &mut None).await
    }

    async fn explain_table_to_plan(
        &self,
        table_name: &ast::ObjectName,
    ) -> Result<QueryPlan, CompilationError> {
        // EXPLAIN <table> matches the SHOW COLUMNS output exactly, reuse the plan
        self.show_columns_to_plan(false, false, &None, table_name)
            .await
    }

    fn explain_to_plan(
        &self,
        statement: &Box<ast::Statement>,
        verbose: bool,
        analyze: bool,
    ) -> Pin<Box<dyn Future<Output = Result<QueryPlan, CompilationError>> + Send + Sync>> {
        let self_cloned = self.clone();

        let statement = statement.clone();
        // This Boxing construct here because of recursive call to self.plan()
        Box::pin(async move {
            let plan = self_cloned.plan(&statement, &mut None).await?;

            match plan {
                QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => Ok(QueryPlan::MetaTabular(
                    StatusFlags::empty(),
                    Box::new(dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "Execution Plan".to_string(),
                            ColumnType::String,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![dataframe::TableValue::String(
                            "This query doesnt have a plan, because it already has values for response"
                                .to_string(),
                        )])],
                    )),
                )),
                QueryPlan::DataFusionSelect(flags, plan, context) => {
                    let plan = Arc::new(plan);
                    let schema = LogicalPlan::explain_schema();
                    let schema = schema.to_dfschema_ref().map_err(|err| {
                        CompilationError::internal(format!(
                            "Unable to get DF schema for explain plan: {}",
                            err
                        ))
                    })?;

                    let explain_plan = if analyze {
                        LogicalPlan::Analyze(Analyze {
                            verbose,
                            input: plan,
                            schema,
                        })
                    } else {
                        let stringified_plans = vec![plan.to_stringified(PlanType::InitialLogicalPlan)];

                        LogicalPlan::Explain(Explain {
                            verbose,
                            plan,
                            stringified_plans,
                            schema,
                        })
                    };

                    Ok(QueryPlan::DataFusionSelect(flags, explain_plan, context))
                }
            }
        })
    }

    fn use_to_plan(&self, db_name: &ast::Ident) -> Result<QueryPlan, CompilationError> {
        self.state.set_database(Some(db_name.value.clone()));

        Ok(QueryPlan::MetaOk(
            StatusFlags::empty(),
            CommandCompletion::Use,
        ))
    }

    fn set_variable_to_plan(
        &self,
        key_values: &Vec<ast::SetVariableKeyValue>,
    ) -> Result<QueryPlan, CompilationError> {
        let mut flags = StatusFlags::SERVER_STATE_CHANGED;

        let mut session_columns_to_update =
            DatabaseVariablesToUpdate::with_capacity(key_values.len());
        let mut global_columns_to_update =
            DatabaseVariablesToUpdate::with_capacity(key_values.len());

        match self.state.protocol {
            DatabaseProtocol::PostgreSQL => {
                for key_value in key_values.iter() {
                    let value: String = match &key_value.value[0] {
                        ast::Expr::Identifier(ident) => ident.value.to_string(),
                        ast::Expr::Value(val) => match val {
                            ast::Value::SingleQuotedString(single_quoted_str) => {
                                single_quoted_str.to_string()
                            }
                            ast::Value::DoubleQuotedString(double_quoted_str) => {
                                double_quoted_str.to_string()
                            }
                            ast::Value::Number(number, _) => number.to_string(),
                            _ => {
                                return Err(CompilationError::user(format!(
                                    "invalid {} variable format",
                                    key_value.key.value
                                )))
                            }
                        },
                        _ => {
                            return Err(CompilationError::user(format!(
                                "invalid {} variable format",
                                key_value.key.value
                            )))
                        }
                    };

                    session_columns_to_update.push(DatabaseVariable::system(
                        key_value.key.value.to_lowercase(),
                        ScalarValue::Utf8(Some(value.clone())),
                        None,
                    ));
                }
            }
            DatabaseProtocol::MySQL => {
                for key_value in key_values.iter() {
                    if key_value.key.value.to_lowercase() == "autocommit".to_string() {
                        flags |= StatusFlags::AUTOCOMMIT;

                        break;
                    }

                    let symbols: Vec<char> = key_value.key.value.chars().collect();
                    if symbols.len() < 2 {
                        continue;
                    }

                    let is_user_defined_var = symbols[0] == '@' && symbols[1] != '@';
                    let is_global_var =
                        (symbols[0] == '@' && symbols[1] == '@') || symbols[0] != '@';

                    let value: String = match &key_value.value[0] {
                        ast::Expr::Identifier(ident) => ident.value.to_string(),
                        ast::Expr::Value(val) => match val {
                            ast::Value::SingleQuotedString(single_quoted_str) => {
                                single_quoted_str.to_string()
                            }
                            ast::Value::DoubleQuotedString(double_quoted_str) => {
                                double_quoted_str.to_string()
                            }
                            ast::Value::Number(number, _) => number.to_string(),
                            _ => {
                                return Err(CompilationError::user(format!(
                                    "invalid {} variable format",
                                    key_value.key.value
                                )))
                            }
                        },
                        _ => {
                            return Err(CompilationError::user(format!(
                                "invalid {} variable format",
                                key_value.key.value
                            )))
                        }
                    };

                    if is_global_var {
                        let key = if symbols[0] == '@' {
                            key_value.key.value[2..].to_lowercase()
                        } else {
                            key_value.key.value.to_lowercase()
                        };
                        global_columns_to_update.push(DatabaseVariable::system(
                            key.to_lowercase(),
                            ScalarValue::Utf8(Some(value.clone())),
                            None,
                        ));
                    } else if is_user_defined_var {
                        let key = key_value.key.value[1..].to_lowercase();
                        session_columns_to_update.push(DatabaseVariable::user_defined(
                            key.to_lowercase(),
                            ScalarValue::Utf8(Some(value.clone())),
                            None,
                        ));
                    }
                }
            }
        }

        if !session_columns_to_update.is_empty() {
            self.state.set_variables(session_columns_to_update);
        }

        if !global_columns_to_update.is_empty() {
            self.session_manager
                .server
                .set_variables(global_columns_to_update, self.state.protocol.clone());
        }

        match self.state.protocol {
            DatabaseProtocol::PostgreSQL => Ok(QueryPlan::MetaOk(flags, CommandCompletion::Set)),
            // TODO: Verify that it's possible to use MetaOk too...
            DatabaseProtocol::MySQL => Ok(QueryPlan::MetaTabular(
                flags,
                Box::new(dataframe::DataFrame::new(vec![], vec![])),
            )),
        }
    }

    fn create_execution_ctx(&self) -> DFSessionContext {
        let query_planner = Arc::new(CubeQueryPlanner::new(
            self.session_manager.server.transport.clone(),
            self.state.get_load_request_meta(),
        ));
        let mut ctx = DFSessionContext::with_state(
            default_session_builder(
                DFSessionConfig::new()
                    .create_default_catalog_and_schema(false)
                    .with_information_schema(false)
                    .with_default_catalog_and_schema("db", "public"),
            )
            .with_query_planner(query_planner),
        );

        if self.state.protocol == DatabaseProtocol::MySQL {
            let system_variable_provider =
                VariablesProvider::new(self.state.clone(), self.session_manager.server.clone());
            let user_defined_variable_provider =
                VariablesProvider::new(self.state.clone(), self.session_manager.server.clone());

            ctx.register_variable(VarType::System, Arc::new(system_variable_provider));
            ctx.register_variable(
                VarType::UserDefined,
                Arc::new(user_defined_variable_provider),
            );
        }

        // udf
        if self.state.protocol == DatabaseProtocol::MySQL {
            ctx.register_udf(create_version_udf("8.0.25".to_string()));
            ctx.register_udf(create_db_udf("database".to_string(), self.state.clone()));
            ctx.register_udf(create_db_udf("schema".to_string(), self.state.clone()));
            ctx.register_udf(create_current_user_udf(
                self.state.clone(),
                "current_user",
                true,
            ));
            ctx.register_udf(create_user_udf(self.state.clone()));
        } else if self.state.protocol == DatabaseProtocol::PostgreSQL {
            ctx.register_udf(create_version_udf(
                "PostgreSQL 14.1 on x86_64-cubesql".to_string(),
            ));
            ctx.register_udf(create_db_udf(
                "current_database".to_string(),
                self.state.clone(),
            ));
            ctx.register_udf(create_db_udf(
                "current_schema".to_string(),
                self.state.clone(),
            ));
            ctx.register_udf(create_current_user_udf(
                self.state.clone(),
                "current_user",
                false,
            ));
            ctx.register_udf(create_current_user_udf(self.state.clone(), "user", false));
            ctx.register_udf(create_session_user_udf(self.state.clone()));
        }

        ctx.register_udf(create_connection_id_udf(self.state.clone()));
        ctx.register_udf(create_pg_backend_pid_udf(self.state.clone()));
        ctx.register_udf(create_instr_udf());
        ctx.register_udf(create_ucase_udf());
        ctx.register_udf(create_isnull_udf());
        ctx.register_udf(create_if_udf());
        ctx.register_udf(create_least_udf());
        ctx.register_udf(create_convert_tz_udf());
        ctx.register_udf(create_timediff_udf());
        ctx.register_udf(create_time_format_udf());
        ctx.register_udf(create_locate_udf());
        ctx.register_udf(create_date_udf());
        ctx.register_udf(create_makedate_udf());
        ctx.register_udf(create_year_udf());
        ctx.register_udf(create_quarter_udf());
        ctx.register_udf(create_hour_udf());
        ctx.register_udf(create_minute_udf());
        ctx.register_udf(create_second_udf());
        ctx.register_udf(create_dayofweek_udf());
        ctx.register_udf(create_dayofmonth_udf());
        ctx.register_udf(create_dayofyear_udf());
        ctx.register_udf(create_date_sub_udf());
        ctx.register_udf(create_date_add_udf());
        ctx.register_udf(create_str_to_date_udf());
        ctx.register_udf(create_current_timestamp_udf("current_timestamp"));
        ctx.register_udf(create_current_timestamp_udf("localtimestamp"));
        ctx.register_udf(create_current_schema_udf());
        ctx.register_udf(create_current_schemas_udf());
        ctx.register_udf(create_format_type_udf());
        ctx.register_udf(create_pg_datetime_precision_udf());
        ctx.register_udf(create_pg_numeric_precision_udf());
        ctx.register_udf(create_pg_numeric_scale_udf());
        ctx.register_udf(create_pg_get_userbyid_udf(self.state.clone()));
        ctx.register_udf(create_pg_get_expr_udf());
        ctx.register_udf(create_pg_table_is_visible_udf());
        ctx.register_udf(create_pg_type_is_visible_udf());
        ctx.register_udf(create_pg_get_constraintdef_udf());
        ctx.register_udf(create_pg_truetypid_udf());
        ctx.register_udf(create_pg_truetypmod_udf());
        ctx.register_udf(create_to_char_udf());
        ctx.register_udf(create_array_lower_udf());
        ctx.register_udf(create_array_upper_udf());
        ctx.register_udf(create_pg_my_temp_schema());
        ctx.register_udf(create_pg_is_other_temp_schema());
        ctx.register_udf(create_has_schema_privilege_udf(self.state.clone()));
        ctx.register_udf(create_pg_total_relation_size_udf());
        ctx.register_udf(create_cube_regclass_cast_udf());
        ctx.register_udf(create_pg_get_serial_sequence_udf());
        ctx.register_udf(create_json_build_object_udf());
        ctx.register_udf(create_regexp_substr_udf());
        ctx.register_udf(create_interval_mul_udf());
        ctx.register_udf(create_ends_with_udf());
        ctx.register_udf(create_position_udf());
        ctx.register_udf(create_date_to_timestamp_udf());
        ctx.register_udf(create_to_date_udf());
        ctx.register_udf(create_sha1_udf());
        ctx.register_udf(create_current_setting_udf());
        ctx.register_udf(create_quote_ident_udf());
        ctx.register_udf(create_pg_encoding_to_char_udf());
        ctx.register_udf(create_array_to_string_udf());
        ctx.register_udf(create_charindex_udf());
        ctx.register_udf(create_to_regtype_udf());
        ctx.register_udf(create_pg_get_indexdef_udf());

        // udaf
        ctx.register_udaf(create_measure_udaf());

        // udtf
        ctx.register_udtf(create_generate_series_udtf());
        ctx.register_udtf(create_unnest_udtf());
        ctx.register_udtf(create_generate_subscripts_udtf());
        ctx.register_udtf(create_pg_expandarray_udtf());

        // redshift
        ctx.register_udf(create_datediff_udf());
        ctx.register_udf(create_dateadd_udf());

        // fn stubs
        ctx = register_fun_stubs(ctx);

        ctx
    }

    async fn create_df_logical_plan(
        &self,
        stmt: ast::Statement,
        qtrace: &mut Option<Qtrace>,
    ) -> CompilationResult<QueryPlan> {
        match &stmt {
            ast::Statement::Query(query) => match &query.body {
                ast::SetExpr::Select(select) if select.into.is_some() => {
                    return Err(CompilationError::unsupported(
                        "Unsupported query type: SELECT INTO".to_string(),
                    ))
                }
                _ => (),
            },
            _ => (),
        }

        let ctx = self.create_execution_ctx();

        let df_state = Arc::new(ctx.state.write().clone());
        let cube_ctx = CubeContext::new(
            df_state,
            self.meta.clone(),
            self.session_manager.clone(),
            self.state.clone(),
        );
        let df_query_planner = SqlToRel::new_with_options(&cube_ctx, true);

        let plan = df_query_planner
            .statement_to_plan(DFStatement::Statement(Box::new(stmt.clone())))
            .map_err(|err| {
                let message = format!("Initial planning error: {}", err,);
                let meta = Some(HashMap::from([
                    ("query".to_string(), stmt.to_string()),
                    (
                        "sanitizedQuery".to_string(),
                        SensitiveDataSanitizer::new().replace(&stmt).to_string(),
                    ),
                ]));

                CompilationError::internal(message).with_meta(meta)
            })?;
        if let Some(qtrace) = qtrace {
            qtrace.set_df_plan(&plan);
        }

        let mut optimized_plan = plan;
        // ctx.optimize(&plan).map_err(|err| {
        //    CompilationError::Internal(format!("Planning optimization error: {}", err))
        // })?;

        let optimizer_config = OptimizerConfig::new();
        let optimizers: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
            Arc::new(ProjectionDropOut::new()),
            Arc::new(FilterPushDown::new()),
            Arc::new(SortPushDown::new()),
            Arc::new(LimitPushDown::new()),
        ];
        for optimizer in optimizers {
            // TODO: report an error when the plan can't be optimized
            optimized_plan = optimizer
                .optimize(&optimized_plan, &optimizer_config)
                .unwrap_or(optimized_plan);
        }

        if let Some(qtrace) = qtrace {
            qtrace.set_optimized_plan(&optimized_plan);
        }

        let mut converter = LogicalPlanToLanguageConverter::new(Arc::new(cube_ctx));
        let root = converter
            .add_logical_plan(&optimized_plan)
            .map_err(|e| CompilationError::internal(e.to_string()))?;
        let result = converter
            .take_rewriter()
            .find_best_plan(root, self.state.auth_context().unwrap(), qtrace)
            .await
            .map_err(|e| match e.cause {
                CubeErrorCauseType::Internal(_) => CompilationError::Internal(
                    format!(
                        "Error during rewrite: {}. Please check logs for additional information.",
                        e.message
                    ),
                    e.to_backtrace().unwrap_or_else(|| Backtrace::capture()),
                    Some(HashMap::from([
                        ("query".to_string(), stmt.to_string()),
                        (
                            "sanitizedQuery".to_string(),
                            SensitiveDataSanitizer::new().replace(&stmt).to_string(),
                        ),
                    ])),
                ),
                CubeErrorCauseType::User(_) => CompilationError::User(
                    format!(
                        "Error during rewrite: {}. Please check logs for additional information.",
                        e.message
                    ),
                    Some(HashMap::from([
                        ("query".to_string(), stmt.to_string()),
                        (
                            "sanitizedQuery".to_string(),
                            SensitiveDataSanitizer::new().replace(&stmt).to_string(),
                        ),
                    ])),
                ),
            });

        if let Err(_) = &result {
            log::error!("It may be this query is not supported yet. Please post an issue on GitHub https://github.com/cube-js/cube.js/issues/new?template=sql_api_query_issue.md or ask about it in Slack https://slack.cube.dev.");
        }

        let rewrite_plan = result?;

        // DF optimizes logical plan (second time) on physical plan creation
        // It's not safety to use all optimizers from DF for OLAP queries, because it will lead to errors
        // From another side, 99% optimizers cannot optimize anything
        if is_olap_query(&rewrite_plan)? {
            let mut guard = ctx.state.write();
            // TODO: We should find what optimizers will be safety to use for OLAP queries
            guard.optimizer.rules = vec![];
        };

        log::debug!("Rewrite: {:#?}", rewrite_plan);
        if let Some(qtrace) = qtrace {
            qtrace.set_best_plan_and_cube_scans(&rewrite_plan);
        }

        Ok(QueryPlan::DataFusionSelect(
            StatusFlags::empty(),
            rewrite_plan,
            ctx,
        ))
    }
}

fn is_olap_query(parent: &LogicalPlan) -> Result<bool, CompilationError> {
    pub struct FindCubeScanNodeVisitor(bool);

    impl PlanVisitor for FindCubeScanNodeVisitor {
        type Error = CompilationError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
            if let LogicalPlan::Extension(ext) = plan {
                if let Some(_) = ext.node.as_any().downcast_ref::<CubeScanNode>() {
                    self.0 = true;

                    return Ok(false);
                }
            }

            Ok(true)
        }
    }

    let mut visitor = FindCubeScanNodeVisitor(false);
    parent.accept(&mut visitor)?;

    Ok(visitor.0)
}

pub fn rewrite_statement(stmt: &ast::Statement) -> ast::Statement {
    let stmt = CastReplacer::new().replace(stmt);
    let stmt = ToTimestampReplacer::new().replace(&stmt);
    let stmt = UdfWildcardArgReplacer::new().replace(&stmt);
    let stmt = RedshiftDatePartReplacer::new().replace(&stmt);
    let stmt = ApproximateCountDistinctVisitor::new().replace(&stmt);

    stmt
}

pub async fn convert_statement_to_cube_query(
    stmt: &ast::Statement,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
    qtrace: &mut Option<Qtrace>,
) -> CompilationResult<QueryPlan> {
    let stmt = rewrite_statement(stmt);
    if let Some(qtrace) = qtrace {
        qtrace.set_visitor_replaced_statement(&stmt);
    }

    let planner = QueryPlanner::new(session.state.clone(), meta, session.session_manager.clone());
    planner.plan(&stmt, qtrace).await
}

#[derive(Debug, PartialEq, Serialize)]
pub struct CompiledQuery {
    pub request: V1LoadRequestQuery,
    pub meta: Vec<CompiledQueryFieldMeta>,
}

impl CompiledQuery {
    pub fn meta_as_df_projection_expr(&self) -> Vec<Expr> {
        let mut projection = Vec::new();

        for meta_field in self.meta.iter() {
            projection.push(Expr::Alias(
                Box::new(Expr::Column(Column {
                    relation: None,
                    name: meta_field.column_from.clone(),
                })),
                meta_field.column_to.clone(),
            ));
        }

        projection
    }

    pub fn meta_as_df_projection_schema(&self) -> Arc<DFSchema> {
        let mut fields: Vec<DFField> = Vec::new();

        for meta_field in self.meta.iter() {
            fields.push(DFField::new(
                None,
                meta_field.column_to.as_str(),
                df_data_type_by_column_type(meta_field.column_type.clone()),
                false,
            ));
        }

        DFSchemaRef::new(DFSchema::new_with_metadata(fields, HashMap::new()).unwrap())
    }

    pub fn meta_as_df_schema(&self) -> Arc<DFSchema> {
        let mut fields: Vec<DFField> = Vec::new();

        for meta_field in self.meta.iter() {
            let exists = fields
                .iter()
                .any(|field| field.name() == &meta_field.column_from);
            if !exists {
                fields.push(DFField::new(
                    None,
                    meta_field.column_from.as_str(),
                    match meta_field.column_type {
                        ColumnType::Int32 | ColumnType::Int64 => DataType::Int64,
                        ColumnType::String => DataType::Utf8,
                        ColumnType::Double => DataType::Float64,
                        ColumnType::Int8 => DataType::Boolean,
                        _ => panic!("Unimplemented support for {:?}", meta_field.column_type),
                    },
                    false,
                ));
            }
        }

        DFSchemaRef::new(DFSchema::new_with_metadata(fields, HashMap::new()).unwrap())
    }
}

pub enum QueryPlan {
    // Meta will not be executed in DF,
    // we already knows how respond to it
    MetaOk(StatusFlags, CommandCompletion),
    MetaTabular(StatusFlags, Box<dataframe::DataFrame>),
    // Query will be executed via Data Fusion
    DataFusionSelect(StatusFlags, LogicalPlan, DFSessionContext),
}

impl fmt::Debug for QueryPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryPlan::MetaOk(flags, completion) => {
                f.write_str(&format!(
                    "MetaOk(StatusFlags: {:?}, CommandCompletion: {:?})", flags, completion
                ))
            },
            QueryPlan::MetaTabular(flags, _) => {
                f.write_str(&format!(
                    "MetaTabular(StatusFlags: {:?}, DataFrame: hidden)",
                    flags
                ))
            },
            QueryPlan::DataFusionSelect(flags, _, _) => {
                f.write_str(&format!(
                    "DataFusionSelect(StatusFlags: {:?}, LogicalPlan: hidden, DFSessionContext: hidden)",
                    flags
                ))
            },
        }
    }
}

impl QueryPlan {
    pub fn as_logical_plan(&self) -> LogicalPlan {
        match self {
            QueryPlan::DataFusionSelect(_, plan, _) => plan.clone(),
            QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => {
                panic!("This query doesnt have a plan, because it already has values for response")
            }
        }
    }

    pub async fn as_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        match self {
            QueryPlan::DataFusionSelect(_, plan, ctx) => DataFrame::new(ctx.state.clone(), plan)
                .create_physical_plan()
                .await
                .map_err(|e| CubeError::user(e.to_string())),
            QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => {
                panic!("This query doesnt have a plan, because it already has values for response")
            }
        }
    }

    pub fn print(&self, pretty: bool) -> Result<String, CubeError> {
        match self {
            QueryPlan::DataFusionSelect(_, plan, _) => {
                if pretty {
                    Ok(plan.display_indent().to_string())
                } else {
                    Ok(plan.display().to_string())
                }
            }
            QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => Ok(
                "This query doesnt have a plan, because it already has values for response"
                    .to_string(),
            ),
        }
    }
}

pub async fn convert_sql_to_cube_query(
    query: &String,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> CompilationResult<QueryPlan> {
    let stmt = parse_sql_to_statement(&query, session.state.protocol.clone(), &mut None)?;
    convert_statement_to_cube_query(&stmt, meta, session, &mut None).await
}

pub fn find_cube_scans_deep_search(
    parent: Arc<LogicalPlan>,
    panic_if_empty: bool,
) -> Vec<CubeScanNode> {
    pub struct FindCubeScanNodeVisitor(Vec<CubeScanNode>);

    impl PlanVisitor for FindCubeScanNodeVisitor {
        type Error = CubeError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
            if let LogicalPlan::Extension(ext) = plan {
                if let Some(scan_node) = ext.node.as_any().downcast_ref::<CubeScanNode>() {
                    self.0.push(scan_node.clone());
                }
            }
            Ok(true)
        }
    }

    let mut visitor = FindCubeScanNodeVisitor(Vec::new());
    parent.accept(&mut visitor).unwrap();

    if panic_if_empty && visitor.0.len() == 0 {
        panic!("No CubeScanNode was found in plan");
    }

    visitor.0
}

#[cfg(test)]
mod tests {
    use chrono::Datelike;
    use cubeclient::models::{
        V1CubeMeta, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
    };
    use datafusion::{dataframe::DataFrame as DFDataFrame, logical_plan::plan::Filter};
    use pretty_assertions::assert_eq;
    use regex::Regex;

    use super::{
        test::{get_test_session, get_test_tenant_ctx},
        *,
    };
    use crate::{
        compile::test::{get_string_cube_meta, get_test_tenant_ctx_with_meta},
        sql::{dataframe::batch_to_dataframe, types::StatusFlags},
    };
    use datafusion::logical_plan::PlanVisitor;
    use log::Level;
    use serde_json::json;
    use simple_logger::SimpleLogger;

    lazy_static! {
        pub static ref TEST_LOGGING_INITIALIZED: std::sync::RwLock<bool> =
            std::sync::RwLock::new(false);
    }

    fn init_logger() {
        let mut initialized = TEST_LOGGING_INITIALIZED.write().unwrap();
        if !*initialized {
            let log_level = Level::Trace;
            let logger = SimpleLogger::new()
                .with_level(Level::Error.to_level_filter())
                .with_module_level("cubeclient", log_level.to_level_filter())
                .with_module_level("cubesql", log_level.to_level_filter())
                .with_module_level("datafusion", Level::Warn.to_level_filter())
                .with_module_level("pg-srv", Level::Warn.to_level_filter());

            log::set_boxed_logger(Box::new(logger)).unwrap();
            log::set_max_level(log_level.to_level_filter());
            *initialized = true;
        }
    }

    async fn convert_select_to_query_plan(query: String, db: DatabaseProtocol) -> QueryPlan {
        env::set_var("TZ", "UTC");

        let query =
            convert_sql_to_cube_query(&query, get_test_tenant_ctx(), get_test_session(db).await)
                .await;

        query.unwrap()
    }

    async fn convert_select_to_query_plan_with_meta(
        query: String,
        meta: Vec<V1CubeMeta>,
    ) -> QueryPlan {
        env::set_var("TZ", "UTC");

        let query = convert_sql_to_cube_query(
            &query,
            get_test_tenant_ctx_with_meta(meta),
            get_test_session(DatabaseProtocol::PostgreSQL).await,
        )
        .await;

        query.unwrap()
    }

    trait LogicalPlanTestUtils {
        fn find_projection_schema(&self) -> DFSchemaRef;

        fn find_cube_scan(&self) -> CubeScanNode;

        fn find_cube_scans(&self) -> Vec<CubeScanNode>;

        fn find_filter(&self) -> Option<Filter>;
    }

    fn find_filter_deep_search(parent: Arc<LogicalPlan>) -> Option<Filter> {
        pub struct FindFilterNodeVisitor(Option<Filter>);

        impl PlanVisitor for FindFilterNodeVisitor {
            type Error = CubeError;

            fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
                if let LogicalPlan::Filter(filter) = plan {
                    self.0 = Some(filter.clone());
                }
                Ok(true)
            }
        }

        let mut visitor = FindFilterNodeVisitor(None);
        parent.accept(&mut visitor).unwrap();
        visitor.0
    }

    impl LogicalPlanTestUtils for LogicalPlan {
        fn find_projection_schema(&self) -> DFSchemaRef {
            match self {
                LogicalPlan::Projection(proj) => proj.schema.clone(),
                _ => panic!("Root plan node is not projection!"),
            }
        }

        fn find_cube_scan(&self) -> CubeScanNode {
            let cube_scans = find_cube_scans_deep_search(Arc::new(self.clone()), true);
            if cube_scans.len() != 1 {
                panic!("The plan includes not 1 cube_scan!");
            }

            cube_scans[0].clone()
        }

        fn find_cube_scans(&self) -> Vec<CubeScanNode> {
            find_cube_scans_deep_search(Arc::new(self.clone()), true)
        }

        fn find_filter(&self) -> Option<Filter> {
            find_filter_deep_search(Arc::new(self.clone()))
        }
    }

    #[tokio::test]
    async fn test_select_measure_via_function() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(maxPrice), MEASURE(minPrice), MEASURE(avgPrice) FROM KibanaSampleDataEcommerce".to_string(),
        DatabaseProtocol::MySQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );
    }

    #[tokio::test]
    async fn test_select_number() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(someNumber) as s1, SUM(someNumber) as s2, MIN(someNumber) as s3, MAX(someNumber) as s4, COUNT(someNumber) as s5 FROM NumberCube".to_string(),
            DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["NumberCube.someNumber".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );
    }

    #[tokio::test]
    async fn test_select_null_if_measure_diff() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(count), NULLIF(MEASURE(count), 0) as t, MEASURE(count) / NULLIF(MEASURE(count), 0) FROM KibanaSampleDataEcommerce;".to_string(),
        DatabaseProtocol::PostgreSQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );
    }

    #[tokio::test]
    async fn test_select_compound_identifiers() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(`KibanaSampleDataEcommerce`.`maxPrice`) AS maxPrice, MEASURE(`KibanaSampleDataEcommerce`.`minPrice`) AS minPrice FROM KibanaSampleDataEcommerce".to_string(), DatabaseProtocol::MySQL
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );
    }

    #[tokio::test]
    async fn test_select_measure_aggregate_functions() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MAX(maxPrice), MIN(minPrice), AVG(avgPrice) FROM KibanaSampleDataEcommerce"
                .to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );

        assert_eq!(
            logical_plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
            vec![DataType::Float64, DataType::Float64, DataType::Float64]
        );
    }

    #[tokio::test]
    async fn test_change_user_via_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher'"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_change_user_via_in_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user IN ('gopher')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_starts_with() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE starts_with(customer_gender, 'fe')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["fe".to_string()]),
                    or: None,
                    and: None
                }])
            }
        )
    }

    #[tokio::test]
    async fn test_ends_with_query() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE ends_with(customer_gender, 'emale')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("endsWith".to_string()),
                    values: Some(vec!["emale".to_string()]),
                    or: None,
                    and: None
                }])
            }
        )
    }

    #[tokio::test]
    async fn test_lower_in_thoughtspot() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE LOWER(customer_gender) IN ('female')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;
        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                // TODO: Migrate to equalsLower operator, when it will be available in Cube?
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("startsWith".to_string()),
                        values: Some(vec!["female".to_string()]),
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("endsWith".to_string()),
                        values: Some(vec!["female".to_string()]),
                        or: None,
                        and: None
                    }
                ])
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE LOWER(customer_gender) IN ('female', 'male')".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;
        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                // TODO: Migrate to equalsLower operator, when it will be available in Cube?
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("startsWith".to_string()),
                                    values: Some(vec!["female".to_string()]),
                                    or: None,
                                    and: None
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("endsWith".to_string()),
                                    values: Some(vec!["female".to_string()]),
                                    or: None,
                                    and: None
                                }),
                            ])
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("startsWith".to_string()),
                                    values: Some(vec!["male".to_string()]),
                                    or: None,
                                    and: None
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("endsWith".to_string()),
                                    values: Some(vec!["male".to_string()]),
                                    or: None,
                                    and: None
                                }),
                            ])
                        }),
                    ]),
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn test_lower_equals_thoughtspot() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE LOWER(customer_gender) = 'female'"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                // TODO: Migrate to equalsLower operator, when it will be available in Cube?
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("startsWith".to_string()),
                        values: Some(vec!["female".to_string()]),
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("endsWith".to_string()),
                        values: Some(vec!["female".to_string()]),
                        or: None,
                        and: None
                    }
                ])
            }
        )
    }

    #[tokio::test]
    async fn test_change_user_via_in_filter_thoughtspot() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce "ta_1" WHERE (LOWER("ta_1"."__user") IN ('gopher')) = TRUE"#.to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let expected_request = V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
            segments: Some(vec![]),
            dimensions: Some(vec![]),
            time_dimensions: None,
            order: None,
            limit: None,
            offset: None,
            filters: None,
        };

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();
        assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));
        assert_eq!(cube_scan.request, expected_request);

        let query_plan = convert_select_to_query_plan(
            r#"SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce "ta_1" WHERE ((LOWER("ta_1"."__user") IN ('gopher') = TRUE) = TRUE)"#.to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();
        assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));
        assert_eq!(cube_scan.request, expected_request);
    }

    #[tokio::test]
    async fn test_change_user_via_filter_and() {
        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher' AND customer_gender = 'male'".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_change_user_via_filter_or() {
        // OR is not allowed for __user
        let query =
            convert_sql_to_cube_query(
                &"SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher' OR customer_gender = 'male'".to_string(),
                get_test_tenant_ctx(),
                get_test_session(DatabaseProtocol::PostgreSQL).await
            ).await;

        // TODO: We need to propagate error to result, to assert message
        query.unwrap_err();
    }

    #[tokio::test]
    async fn test_order_alias_for_measure_default() {
        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce ORDER BY cnt".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_order_by() {
        init_logger();

        let supported_orders = vec![
            // test_order_alias_for_dimension_default
            (
                "SELECT taxful_total_price as total_price FROM KibanaSampleDataEcommerce ORDER BY total_price".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            (
                "SELECT COUNT(*) count, customer_gender, order_date FROM KibanaSampleDataEcommerce GROUP BY customer_gender, order_date ORDER BY order_date".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.customer_gender".to_string(),
                        "KibanaSampleDataEcommerce.order_date".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.order_date".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_indentifier_default
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_compound_identifier_default
            (
                "SELECT taxful_total_price FROM `db`.`KibanaSampleDataEcommerce` ORDER BY `KibanaSampleDataEcommerce`.`taxful_total_price`".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_indentifier_asc
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price ASC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_indentifier_desc
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_identifer_alias_ident_no_escape
            (
                "SELECT taxful_total_price as alias1 FROM KibanaSampleDataEcommerce ORDER BY alias1 DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_identifer_alias_ident_escape
            (
                "SELECT taxful_total_price as `alias1` FROM KibanaSampleDataEcommerce ORDER BY `alias1` DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
        ];

        for (sql, expected_request) in supported_orders.iter() {
            let query_plan =
                convert_select_to_query_plan(sql.to_string(), DatabaseProtocol::MySQL).await;

            assert_eq!(
                &query_plan.as_logical_plan().find_cube_scan().request,
                expected_request
            )
        }
    }

    #[tokio::test]
    async fn test_order_function_date() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE(order_date) FROM KibanaSampleDataEcommerce ORDER BY DATE(order_date) DESC"
                .to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: None
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE(order_date) FROM KibanaSampleDataEcommerce GROUP BY DATE(order_date) ORDER BY DATE(order_date) DESC"
                .to_string(),
            DatabaseProtocol::MySQL,
        ).await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: None
            }
        );
    }

    #[tokio::test]
    async fn test_select_all_fields_by_asterisk_limit_100() {
        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce LIMIT 100".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .dimensions,
            Some(vec![
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "KibanaSampleDataEcommerce.last_mod".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.notes".to_string(),
                "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                "KibanaSampleDataEcommerce.has_subscription".to_string(),
            ])
        )
    }

    #[tokio::test]
    async fn test_select_all_fields_by_asterisk_limit_100_offset_50() {
        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce LIMIT 100 OFFSET 50".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .dimensions,
            Some(vec![
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "KibanaSampleDataEcommerce.last_mod".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.notes".to_string(),
                "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                "KibanaSampleDataEcommerce.has_subscription".to_string(),
            ])
        )
    }

    #[tokio::test]
    async fn test_select_two_fields() {
        let query_plan = convert_select_to_query_plan(
            "SELECT order_date, customer_gender FROM KibanaSampleDataEcommerce".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_select_fields_alias() {
        let query_plan = convert_select_to_query_plan(
            "SELECT order_date as order_date, customer_gender as customer_gender FROM KibanaSampleDataEcommerce"
                .to_string(), DatabaseProtocol::MySQL
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        // assert_eq!(
        //     logical_plan.schema().clone(),
        //     Arc::new(
        //         DFSchema::new_with_metadata(
        //             vec![
        //                 DFField::new(None, "order_date", DataType::Utf8, false),
        //                 DFField::new(None, "customer_gender", DataType::Utf8, false),
        //             ],
        //             HashMap::new()
        //         )
        //         .unwrap()
        //     ),
        // );
    }

    #[tokio::test]
    async fn test_select_where_false() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce WHERE 1 = 0".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    "KibanaSampleDataEcommerce.countDistinct".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.last_mod".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    "KibanaSampleDataEcommerce.has_subscription".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: Some(1),
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_projection_with_casts() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \
             CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS \"customer_gender\",\
             \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",\
             \"KibanaSampleDataEcommerce\".\"maxPrice\" AS \"maxPrice\",\
             \"KibanaSampleDataEcommerce\".\"minPrice\" AS \"minPrice\",\
             \"KibanaSampleDataEcommerce\".\"avgPrice\" AS \"avgPrice\",\
             \"KibanaSampleDataEcommerce\".\"order_date\" AS \"order_date\",\
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price1\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price2\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price3\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price4\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price5\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price6\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price7\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price8\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price9\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price10\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price11\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price12\"
             FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_min_max() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MIN(\"KibanaSampleDataEcommerce\".\"order_date\") AS \"tmn:timestamp:min\", MAX(\"KibanaSampleDataEcommerce\".\"order_date\") AS \"tmn:timestamp:max\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_min_max_number() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MIN(\"KibanaSampleDataEcommerce\".\"taxful_total_price\") AS \"tmn:timestamp:min\", MAX(\"KibanaSampleDataEcommerce\".\"taxful_total_price\") AS \"tmn:timestamp:max\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_filter_and_group_by() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE (CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) = 'female') GROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn tableau_having_count_on_cube_without_count() {
        init_logger();

        // let query_plan = convert_select_to_query_plan(
        //     "SELECT COUNT(DISTINCT \"Logs\".\"agentCount\") AS \"sum:count:ok\" FROM \"public\".\"Logs\" \"Logs\" HAVING (COUNT(1) > 0)".to_string(),
        //     DatabaseProtocol::PostgreSQL,
        // ).await;

        // let logical_plan = query_plan.as_logical_plan();
        // assert_eq!(
        //     logical_plan.find_cube_scan().request,
        //     V1LoadRequestQuery {
        //         measures: Some(vec!["Logs.agentCount".to_string()]),
        //         segments: Some(vec![]),
        //         dimensions: Some(vec![]),
        //         time_dimensions: None,
        //         order: None,
        //         limit: None,
        //         offset: None,
        //         filters: None,
        //     }
        // );
    }

    #[tokio::test]
    async fn tableau_boolean_filter_inplace_where() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE \"KibanaSampleDataEcommerce\".\"is_female\" HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec!["KibanaSampleDataEcommerce.is_female".to_string()]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["0".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE NOT(\"KibanaSampleDataEcommerce\".\"has_subscription\") HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.has_subscription".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["false".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.count".to_string()),
                        operator: Some("gt".to_string()),
                        values: Some(vec!["0".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
            }
        );
    }

    #[tokio::test]
    async fn tableau_not_null_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE (NOT (\"KibanaSampleDataEcommerce\".\"taxful_total_price\" IS NULL)) GROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn tableau_current_timestamp() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS \"COL\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = &query_plan.print(true).unwrap();

        let re = Regex::new(r"TimestampNanosecond\(\d+, None\)").unwrap();
        let logical_plan = re
            .replace_all(logical_plan, "TimestampNanosecond(0, None)")
            .as_ref()
            .to_string();

        assert_eq!(
            logical_plan,
            "Projection: TimestampNanosecond(0, None) AS COL\
            \n  EmptyRelation",
        );
    }

    #[tokio::test]
    async fn tableau_time_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE ((\"KibanaSampleDataEcommerce\".\"order_date\" >= (TIMESTAMP '2020-12-25 22:48:48.000')) AND (\"KibanaSampleDataEcommerce\".\"order_date\" <= (TIMESTAMP '2022-04-01 00:00:00.000')))".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2020-12-25T22:48:48.000Z".to_string(),
                        "2022-03-31T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn superset_pg_time_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE_TRUNC('week', \"order_date\") AS __timestamp,
               count(count) AS \"COUNT(count)\"
FROM public.\"KibanaSampleDataEcommerce\"
WHERE \"order_date\" >= TO_TIMESTAMP('2021-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND \"order_date\" < TO_TIMESTAMP('2022-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY DATE_TRUNC('week', \"order_date\")
ORDER BY \"COUNT(count)\" DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: Some(json!(vec![
                        "2021-05-15T00:00:00.000Z".to_string(),
                        "2022-05-14T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn power_bi_dimension_only() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"_\".\"customer_gender\"\r\nfrom \r\n(\r\n    select \"rows\".\"customer_gender\" as \"customer_gender\"\r\n    from \r\n    (\r\n        select \"customer_gender\"\r\n        from \"public\".\"KibanaSampleDataEcommerce\" \"$Table\"\r\n    ) \"rows\"\r\n    group by \"customer_gender\"\r\n) \"_\"\r\norder by \"_\".\"customer_gender\"\r\nlimit 1001".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ],],),
                limit: Some(1001),
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn power_bi_is_not_empty() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select sum(\"rows\".\"count\") as \"a0\" from (select \"_\".\"count\" from \"public\".\"KibanaSampleDataEcommerce\" \"_\" where (not \"_\".\"customer_gender\" is null and not \"_\".\"customer_gender\" = '' or not (not \"_\".\"customer_gender\" is null))) \"rows\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("set".to_string()),
                                    values: None,
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("notEquals".to_string()),
                                    values: Some(vec!["".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ])
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
            }
        );
    }

    #[tokio::test]
    async fn non_cube_filters_cast_kept() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT id FROM information_schema.testing_dataset WHERE id > CAST('0' AS INTEGER)"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.print(true).unwrap();
        assert!(
            logical_plan.contains("CAST"),
            "{:?} doesn't contain CAST",
            logical_plan
        );
    }

    #[tokio::test]
    async fn tableau_default_having() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nHAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        let cube_scan = logical_plan.find_cube_scan();
        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["0".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );

        assert_eq!(
            cube_scan
                .schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>(),
            vec!["sum:count:ok".to_string(),]
        );
        assert_eq!(
            &cube_scan.member_fields,
            &vec![MemberField::Member(
                "KibanaSampleDataEcommerce.count".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn tableau_group_by_month() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:bytesBilled:ok\",\n  DATE_TRUNC( 'MONTH', CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS TIMESTAMP) ) AS \"tmn:timestamp:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 2".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_group_by_month_and_dimension() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS \"query\",\n  SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:bytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_extract_year() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(TRUNC(EXTRACT(YEAR FROM \"KibanaSampleDataEcommerce\".\"order_date\")) AS INTEGER) AS \"yr:timestamp:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(TRUNC(EXTRACT(YEAR FROM \"KibanaSampleDataEcommerce\".\"order_date\")) AS INTEGER) AS \"yr:timestamp:ok\", SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:teraBytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_week() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST((DATE_TRUNC( 'day', CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS DATE) ) + (-EXTRACT(DOW FROM \"KibanaSampleDataEcommerce\".\"order_date\") * INTERVAL '1 DAY')) AS DATE) AS \"yr:timestamp:ok\", SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:teraBytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:freeCount:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nWHERE (CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) = 'female')".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn tableau_contains_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:freeCount:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nWHERE (STRPOS(CAST(LOWER(CAST(CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS TEXT)) AS TEXT),CAST('fem' AS TEXT)) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn measure_used_on_dimension() {
        init_logger();

        let create_query = convert_sql_to_cube_query(
            &"SELECT MEASURE(customer_gender) FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL).await,
        ).await;

        assert_eq!(
            create_query.err().unwrap().message(),
            "Error during rewrite: Dimension 'customer_gender' was used with the aggregate function 'MEASURE()'. Please use a measure instead. Please check logs for additional information.",
        );
    }

    #[tokio::test]
    async fn powerbi_contains_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"rows\".\"customer_gender\" as \"customer_gender\",
\n    sum(\"rows\".\"count\") as \"a0\"\
\nfrom\
\n(\
\n    select \"_\".\"count\",\
\n        \"_\".\"customer_gender\"\
\n    from \"public\".\"KibanaSampleDataEcommerce\" \"_\"\
\n    where strpos((case\
\n        when \"_\".\"customer_gender\" is not null\
\n        then \"_\".\"customer_gender\"\
\n        else ''\
\n    end), 'fem') > 0\
\n) \"rows\"\
\ngroup by \"customer_gender\"\
\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1000001),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn powerbi_inner_wrapped_dates() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"_\".\"created_at_day\",\
\n    \"_\".\"a0\"\
\nfrom \
\n(\
\n    select \"rows\".\"created_at_day\" as \"created_at_day\",\
\n        sum(\"rows\".\"cnt\") as \"a0\"\
\n    from \
\n    (\
\n        select count(*) cnt,date_trunc('day', order_date) as created_at_day, date_trunc('month', order_date) as created_at_month from public.KibanaSampleDataEcommerce group by 2, 3\
\n    ) \"rows\"\
\n    group by \"created_at_day\"\
\n) \"_\"\
\nwhere not \"_\".\"a0\" is null\
\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: Some(1000001),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn powerbi_inner_wrapped_asterisk() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"rows\".\"customer_gender\" as \"customer_gender\",\
\n    \"rows\".\"created_at_month\" as \"created_at_month\"\
\nfrom \
\n(\
\n    select \"_\".\"count\",\
\n        \"_\".\"minPrice\",\
\n        \"_\".\"maxPrice\",\
\n        \"_\".\"avgPrice\",\
\n        \"_\".\"order_date\",\
\n        \"_\".\"customer_gender\",\
\n        \"_\".\"created_at_day\",\
\n        \"_\".\"created_at_month\"\
\n    from \
\n    (\
\n        select *, date_trunc('day', order_date) created_at_day, date_trunc('month', order_date) created_at_month from public.KibanaSampleDataEcommerce\
\n    ) \"_\"\
\n    where \"_\".\"created_at_month\" < timestamp '2022-06-13 00:00:00' and \"_\".\"created_at_month\" >= timestamp '2021-12-16 00:00:00'\
\n) \"rows\"\
\ngroup by \"customer_gender\",\
\n    \"created_at_month\"\
\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: Some(json!(vec![
                        "2021-12-16T00:00:00.000Z".to_string(),
                        "2022-06-12T23:59:59.999Z".to_string()
                    ])),
                }]),
                order: None,
                limit: Some(1000001),
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn powerbi_inner_decimal_cast() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"_\".\"customer_gender\",\r\n    \"_\".\"a0\"\r\nfrom \r\n(\r\n    select \"rows\".\"customer_gender\" as \"customer_gender\",\r\n        sum(cast(\"rows\".\"count\" as decimal)) as \"a0\"\r\n    from \"public\".\"KibanaSampleDataEcommerce\" \"rows\"\r\n    group by \"customer_gender\"\r\n) \"_\"\r\nwhere not \"_\".\"a0\" is null\r\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1000001),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn powerbi_join() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \
            \n  \"_\".\"semijoin1.c30\" AS \"c30\", \"_\".\"a0\" AS \"a0\" FROM \
            \n  (SELECT \"rows\".\"semijoin1.c30\" AS \"semijoin1.c30\", count(distinct \"rows\".\"basetable0.a0\") AS \"a0\" FROM (\
            \n    SELECT \"$Outer\".\"basetable0.a0\", \"$Inner\".\"semijoin1.c30\" FROM (\
            \n      SELECT \"__cubeJoinField\" AS \"basetable0.c22\", \"agentCount\" AS \"basetable0.a0\" FROM \"public\".\"Logs\" AS \"$Table\"\
            \n    ) AS \"$Outer\" JOIN (\
            \n    SELECT \"rows\".\"customer_gender\" AS \"semijoin1.c30\", \"rows\".\"__cubeJoinField\" AS \"semijoin1.c22\" FROM (\
            \n      SELECT \"customer_gender\", \"__cubeJoinField\" FROM \"public\".\"KibanaSampleDataEcommerce\" AS \"$Table\"\
            \n    ) AS \"rows\" GROUP BY \"customer_gender\", \"__cubeJoinField\"\
            \n  ) AS \"$Inner\" ON (\
            \n    \"$Outer\".\"basetable0.c22\" = \"$Inner\".\"semijoin1.c22\" OR \"$Outer\".\"basetable0.c22\" IS NULL AND \"$Inner\".\"semijoin1.c22\" IS NULL\
            \n  )\
            \n  ) AS \"rows\" GROUP BY \"semijoin1.c30\"\
            \n  ) AS \"_\" WHERE NOT \"_\".\"a0\" IS NULL LIMIT 1000001".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["Logs.agentCount".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1000001),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("Logs.agentCount".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn powerbi_transitive_join() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"SELECT "_"."semijoin3.c98" AS "c98", "_"."a0" AS "a0" FROM (
            SELECT "rows"."semijoin3.c98" AS "semijoin3.c98", sum(CAST("rows"."basetable2.a0" AS NUMERIC)) AS "a0" FROM 
            (
                SELECT "$Outer"."basetable2.a0", "$Inner"."semijoin3.c98" FROM (
                    SELECT "__cubeJoinField" AS "basetable2.c95", "count" AS "basetable2.a0" FROM "public"."KibanaSampleDataEcommerce" AS "$Table"
                ) AS "$Outer" JOIN (
                    SELECT "rows"."semijoin1.c98" AS "semijoin3.c98", "rows"."basetable0.c108" AS "semijoin3.c95" FROM (
                        SELECT "$Outer"."basetable0.c108", "$Inner"."semijoin1.c98" FROM (
                            SELECT "rows"."__cubeJoinField" AS "basetable0.c108" FROM (
                                SELECT "__cubeJoinField" FROM "public"."NumberCube" AS "$Table"
                            ) AS "rows" GROUP BY "__cubeJoinField"
                        ) AS "$Outer" JOIN (
                            SELECT "rows"."content" AS "semijoin1.c98", "rows"."__cubeJoinField" AS "semijoin1.c108" FROM (
                                SELECT "content", "__cubeJoinField" FROM "public"."Logs" AS "$Table"
                            ) AS "rows" GROUP BY "content", "__cubeJoinField"
                        ) AS "$Inner" ON (
                            "$Outer"."basetable0.c108" = "$Inner"."semijoin1.c108" OR "$Outer"."basetable0.c108" IS NULL AND "$Inner"."semijoin1.c108" IS NULL
                        )) AS "rows" GROUP BY "semijoin1.c98", "basetable0.c108"
                    ) AS "$Inner" ON (
                    "$Outer"."basetable2.c95" = "$Inner"."semijoin3.c95" OR "$Outer"."basetable2.c95" IS NULL AND "$Inner"."semijoin3.c95" IS NULL
                )
            ) AS "rows" GROUP BY "semijoin3.c98") AS "_" WHERE NOT "_"."a0" IS NULL LIMIT 1000001
            "#.to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["Logs.content".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1000001),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn test_select_aggregations() {
        let variants = vec![
            (
                "SELECT COUNT(*) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(*) FROM db.KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(1) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(count) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(DISTINCT agentCount) FROM Logs".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["Logs.agentCount".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(DISTINCT agentCountApprox) FROM Logs".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["Logs.agentCountApprox".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT MAX(`maxPrice`) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
        ];

        for (input_query, expected_request) in variants.iter() {
            let logical_plan =
                convert_select_to_query_plan(input_query.clone(), DatabaseProtocol::MySQL)
                    .await
                    .as_logical_plan();

            assert_eq!(&logical_plan.find_cube_scan().request, expected_request);
        }
    }

    #[tokio::test]
    async fn test_string_measure() {
        init_logger();

        let logical_plan = convert_select_to_query_plan_with_meta(
            r#"
            SELECT MIN(StringCube.someString), MAX(StringCube.someString) FROM StringCube
            "#
            .to_string(),
            get_string_cube_meta(),
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["StringCube.someString".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_select_error() {
        let variants = vec![
            (
                "SELECT AVG(maxPrice) FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::user("Error during rewrite: Measure aggregation type doesn't match. The aggregation type for 'maxPrice' is 'MAX()' but 'AVG()' was provided. Please check logs for additional information.".to_string()),
            ),
        ];

        for (input_query, expected_error) in variants.iter() {
            let query = convert_sql_to_cube_query(
                &input_query,
                get_test_tenant_ctx(),
                get_test_session(DatabaseProtocol::PostgreSQL).await,
            )
            .await;

            match query {
                Ok(_) => panic!("Query ({}) should return error", input_query),
                Err(e) => assert_eq!(&e.with_meta(None), expected_error, "for {}", input_query),
            }
        }
    }

    #[tokio::test]
    async fn test_group_by_date_trunc() {
        let supported_granularities = vec![
            // all variants
            [
                "DATE_TRUNC('second', order_date)".to_string(),
                "second".to_string(),
            ],
            [
                "DATE_TRUNC('minute', order_date)".to_string(),
                "minute".to_string(),
            ],
            [
                "DATE_TRUNC('hour', order_date)".to_string(),
                "hour".to_string(),
            ],
            [
                "DATE_TRUNC('week', order_date)".to_string(),
                "week".to_string(),
            ],
            [
                "DATE_TRUNC('month', order_date)".to_string(),
                "month".to_string(),
            ],
            [
                "DATE_TRUNC('quarter', order_date)".to_string(),
                "quarter".to_string(),
            ],
            [
                "DATE_TRUNC('qtr', order_date)".to_string(),
                "quarter".to_string(),
            ],
            [
                "DATE_TRUNC('year', order_date)".to_string(),
                "year".to_string(),
            ],
            // with escaping
            [
                "DATE_TRUNC('second', `order_date`)".to_string(),
                "second".to_string(),
            ],
        ];

        for [subquery, expected_granularity] in supported_granularities.iter() {
            let logical_plan = convert_select_to_query_plan(
                format!("SELECT COUNT(*), {} AS __timestamp FROM KibanaSampleDataEcommerce GROUP BY __timestamp", subquery), DatabaseProtocol::MySQL
            ).await.as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(expected_granularity.to_string()),
                        date_range: None,
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            );

            // assert_eq!(
            //     logical_plan
            //         .find_cube_scan()
            //         .schema
            //         .fields()
            //         .iter()
            //         .map(|f| f.name().to_string())
            //         .collect::<Vec<_>>(),
            //     vec!["COUNT(UInt8(1))", "__timestamp"]
            // );

            // assert_eq!(
            //     logical_plan.find_cube_scan().member_fields,
            //     vec![
            //         "KibanaSampleDataEcommerce.count",
            //         &format!(
            //             "KibanaSampleDataEcommerce.order_date.{}",
            //             expected_granularity
            //         )
            //     ]
            // );
        }
    }

    #[tokio::test]
    async fn test_group_by_date_granularity_superset() {
        let supported_granularities = vec![
            // With MAKEDATE
            ["MAKEDATE(YEAR(order_date), 1) + INTERVAL QUARTER(order_date) QUARTER - INTERVAL 1 QUARTER".to_string(), "quarter".to_string()],
            // With DATE
            ["DATE(DATE_SUB(order_date, INTERVAL DAYOFWEEK(DATE_SUB(order_date, INTERVAL 1 DAY)) - 1 DAY))".to_string(), "week".to_string()],
            // With escaping by `
            ["DATE(DATE_SUB(`order_date`, INTERVAL DAYOFWEEK(DATE_SUB(`order_date`, INTERVAL 1 DAY)) - 1 DAY))".to_string(), "week".to_string()],
            // @todo enable support when cube.js will support it
            // ["DATE(DATE_SUB(order_date, INTERVAL DAYOFWEEK(order_date) - 1 DAY))".to_string(), "week".to_string()],
            ["DATE(DATE_SUB(order_date, INTERVAL DAYOFMONTH(order_date) - 1 DAY))".to_string(), "month".to_string()],
            ["DATE(DATE_SUB(order_date, INTERVAL DAYOFYEAR(order_date) - 1 DAY))".to_string(), "year".to_string()],
            // Simple DATE
            ["DATE(order_date)".to_string(), "day".to_string()],
            ["DATE(`order_date`)".to_string(), "day".to_string()],
            ["DATE(`KibanaSampleDataEcommerce`.`order_date`)".to_string(), "day".to_string()],
            // With DATE_ADD
            ["DATE_ADD(DATE(order_date), INTERVAL HOUR(order_date) HOUR)".to_string(), "hour".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL HOUR(`order_date`) HOUR)".to_string(), "hour".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL (HOUR(order_date) * 60 + MINUTE(order_date)) MINUTE)".to_string(), "minute".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL (HOUR(`order_date`) * 60 + MINUTE(`order_date`)) MINUTE)".to_string(), "minute".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL (HOUR(order_date) * 60 * 60 + MINUTE(order_date) * 60 + SECOND(order_date)) SECOND)".to_string(), "second".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL (HOUR(`order_date`) * 60 * 60 + MINUTE(`order_date`) * 60 + SECOND(`order_date`)) SECOND)".to_string(), "second".to_string()],
        ];

        for [subquery, expected_granularity] in supported_granularities.iter() {
            let logical_plan = convert_select_to_query_plan(
                format!("SELECT COUNT(*), {} AS __timestamp FROM KibanaSampleDataEcommerce GROUP BY __timestamp", subquery), DatabaseProtocol::MySQL
            ).await.as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(expected_granularity.to_string()),
                        date_range: None,
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            )
        }
    }

    #[tokio::test]
    async fn test_date_part_quarter_granularity() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT CAST(TRUNC(EXTRACT(QUARTER FROM KibanaSampleDataEcommerce.order_date)) AS INTEGER)
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_where_filter_daterange() {
        init_logger();

        let to_check = vec![
            // Filter push down to TD (day) - Superset
            (
                "COUNT(*), DATE(order_date) AS __timestamp".to_string(),
                "order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Filter push down to TD (day) - Superset
            (
                "COUNT(*), DATE(order_date) AS __timestamp".to_string(),
                // Now replaced with exact date
                "`KibanaSampleDataEcommerce`.`order_date` >= date(date_add(date('2021-09-30 00:00:00.000000'), INTERVAL -30 day)) AND `KibanaSampleDataEcommerce`.`order_date` < date('2021-09-07 00:00:00.000000')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Column precedence vs projection alias
            (
                "COUNT(*), DATE(order_date) AS order_date".to_string(),
                // Now replaced with exact date
                "`KibanaSampleDataEcommerce`.`order_date` >= date(date_add(date('2021-09-30 00:00:00.000000'), INTERVAL -30 day)) AND `KibanaSampleDataEcommerce`.`order_date` < date('2021-09-07 00:00:00.000000')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Create a new TD (dateRange filter pushdown)
            (
                "COUNT(*)".to_string(),
                "order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Create a new TD (dateRange filter pushdown from right side of CompiledFilterTree::And)
            (
                "COUNT(*)".to_string(),
                "customer_gender = 'FEMALE' AND (order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f'))".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // similar as below but from left side
            (
                "COUNT(*)".to_string(),
                "(order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')) AND customer_gender = 'FEMALE'".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Stacked chart
            (
                "COUNT(*), customer_gender, DATE(order_date) AS __timestamp".to_string(),
                "customer_gender = 'FEMALE' AND (order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f'))".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
        ];

        for (sql_projection, sql_filter, expected_tdm) in to_check.iter() {
            let query = format!(
                "SELECT
                {}
                FROM KibanaSampleDataEcommerce
                WHERE {}
                {}",
                sql_projection,
                sql_filter,
                if sql_projection.contains("__timestamp")
                    && sql_projection.contains("customer_gender")
                {
                    "GROUP BY customer_gender, __timestamp"
                } else if sql_projection.contains("__timestamp") {
                    "GROUP BY __timestamp"
                } else if sql_projection.contains("order_date") {
                    "GROUP BY DATE(order_date)"
                } else {
                    ""
                }
            );
            let logical_plan = convert_select_to_query_plan(query, DatabaseProtocol::MySQL)
                .await
                .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.time_dimensions,
                *expected_tdm
            )
        }
    }

    #[tokio::test]
    async fn test_where_filter_or() {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                COUNT(*), DATE(order_date) AS __timestamp
                FROM KibanaSampleDataEcommerce
                WHERE order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') OR order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')
                GROUP BY __timestamp"
            .to_string(), DatabaseProtocol::MySQL
        ).await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .filters,
            Some(vec![V1LoadRequestQueryFilterItem {
                member: None,
                operator: None,
                values: None,
                or: Some(vec![
                    json!(V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("afterDate".to_string()),
                        values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
                        or: None,
                        and: None,
                    }),
                    json!(V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("beforeDate".to_string()),
                        values: Some(vec!["2021-09-06T23:59:59.999Z".to_string()]),
                        or: None,
                        and: None,
                    })
                ]),
                and: None,
            },])
        )
    }

    #[tokio::test]
    async fn test_where_filter_simple() {
        let to_check = vec![
            // Binary expression with Measures
            (
                "maxPrice = 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "maxPrice > 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Binary expression with Dimensions
            (
                "customer_gender = 'FEMALE'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["FEMALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price > 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price >= 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("gte".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price < 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("lt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price <= 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("lte".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price = -1".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["-1".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price <> -1".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["-1".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // IN
            (
                "customer_gender IN ('FEMALE', 'MALE')".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["FEMALE".to_string(), "MALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT IN ('FEMALE', 'MALE')".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["FEMALE".to_string(), "MALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // NULL
            (
                "customer_gender IS NULL".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notSet".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender IS NOT NULL".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Date
            // (
            //     "order_date = '2021-08-31'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("equals".to_string()),
            //         values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // (
            //     "order_date <> '2021-08-31'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("notEquals".to_string()),
            //         values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // BETWEEN
            // (
            //     "order_date BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
            //     // This filter will be pushed to time_dimension
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ])),
            //     }]),
            // ),
            // (
            //     "order_date NOT BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("notInDateRange".to_string()),
            //         values: Some(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // SIMILAR as BETWEEN but manually
            // (
            //     "order_date >= '2021-08-31' AND order_date < '2021-09-07'".to_string(),
            //     // This filter will be pushed to time_dimension
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             // -1 milleseconds hack for cube.js
            //             "2021-09-06T23:59:59.999Z".to_string(),
            //         ])),
            //     }]),
            // ),
            // //  SIMILAR as BETWEEN but without -1 nanosecond because <=
            // (
            //     "order_date >= '2021-08-31' AND order_date <= '2021-09-07'".to_string(),
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             // without -1 because <=
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ])),
            //     }]),
            // ),
            // LIKE
            (
                "customer_gender LIKE 'female'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT LIKE 'male'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notContains".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Segment
            (
                "is_male = true".to_string(),
                // This filter will be pushed to segments
                None,
                None,
            ),
            (
                "is_male = true AND is_female = true".to_string(),
                // This filters will be pushed to segments
                None,
                None,
            ),
        ];

        for (sql, expected_fitler, expected_time_dimensions) in to_check.iter() {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT
                COUNT(*)
                FROM KibanaSampleDataEcommerce
                WHERE {}",
                    sql
                ),
                DatabaseProtocol::MySQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.filters,
                *expected_fitler,
                "Filters for {}",
                sql
            );
            assert_eq!(
                logical_plan.find_cube_scan().request.time_dimensions,
                *expected_time_dimensions,
                "Time dimensions for {}",
                sql
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_error() {
        let to_check = vec![
            // Binary expr
            (
                "order_date >= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date < 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date = 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <> 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            // Between
            (
                "order_date BETWEEN 'WRONG_DATE' AND '2021-01-01'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date BETWEEN '2021-01-01' AND 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
        ];

        for (sql, expected_error) in to_check.iter() {
            let query = convert_sql_to_cube_query(
                &format!(
                    "SELECT
                    COUNT(*), DATE(order_date) AS __timestamp
                    FROM KibanaSampleDataEcommerce
                    WHERE {}
                    GROUP BY __timestamp",
                    sql
                ),
                get_test_tenant_ctx(),
                get_test_session(DatabaseProtocol::MySQL).await,
            )
            .await;

            match &query {
                Ok(_) => panic!("Query ({}) should return error", sql),
                Err(e) => assert_eq!(e, expected_error, "{}", sql),
            }
        }
    }

    #[tokio::test]
    async fn test_where_filter_complex() {
        let to_check = vec![
            (
                "customer_gender = 'FEMALE' AND customer_gender = 'MALE'".to_string(),
                vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["FEMALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["MALE".to_string()]),
                        or: None,
                        and: None,
                    }
                ],
            ),
            (
                "customer_gender = 'FEMALE' OR customer_gender = 'MALE'".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["MALE".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' AND customer_gender = 'MALE' AND customer_gender = 'UNKNOWN'".to_string(),
                vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["FEMALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["MALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["UNKNOWN".to_string()]),
                        or: None,
                        and: None,
                    }
                ],
            ),
            (
                "customer_gender = 'FEMALE' OR customer_gender = 'MALE' OR customer_gender = 'UNKNOWN'".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["MALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["UNKNOWN".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' OR (customer_gender = 'MALE' AND taxful_total_price > 5)".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                                    operator: Some("equals".to_string()),
                                    values: Some(vec!["MALE".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("gt".to_string()),
                                    values: Some(vec!["5".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ]),
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' OR (customer_gender = 'MALE' AND taxful_total_price > 5 AND taxful_total_price < 100)".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                                    operator: Some("equals".to_string()),
                                    values: Some(vec!["MALE".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("gt".to_string()),
                                    values: Some(vec!["5".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("lt".to_string()),
                                    values: Some(vec!["100".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ]),
                        })
                    ]),
                    and: None,
                }]
            ),
        ];

        for (sql, expected_fitler) in to_check.iter() {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT
                COUNT(*), DATE(order_date) AS __timestamp
                FROM KibanaSampleDataEcommerce
                WHERE {}
                GROUP BY __timestamp",
                    sql
                ),
                DatabaseProtocol::MySQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.filters,
                Some(expected_fitler.clone())
            )
        }
    }

    #[tokio::test]
    async fn test_date_add_sub_postgres() {
        async fn check_fun(name: &str, t: &str, i: &str, expected: &str) {
            assert_eq!(
                execute_query(
                    format!(
                        "SELECT {}(Str_to_date('{}', '%Y-%m-%d %H:%i:%s'), INTERVAL '{}') as result",
                        name, t, i
                    ),
                    DatabaseProtocol::PostgreSQL
                )
                .await
                .unwrap(),
                format!(
                    "+-------------------------+\n\
                | result                  |\n\
                +-------------------------+\n\
                | {} |\n\
                +-------------------------+",
                    expected
                )
            );
        }

        async fn check_adds_to(t: &str, i: &str, expected: &str) {
            check_fun("DATE_ADD", t, i, expected).await
        }

        async fn check_subs_to(t: &str, i: &str, expected: &str) {
            check_fun("DATE_SUB", t, i, expected).await
        }

        check_adds_to("2021-01-01 00:00:00", "1 second", "2021-01-01T00:00:01.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 minute", "2021-01-01T00:01:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 hour", "2021-01-01T01:00:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 day", "2021-01-02T00:00:00.000").await;
        check_adds_to(
            "2021-01-01 00:00:00",
            "-1 second",
            "2020-12-31T23:59:59.000",
        )
        .await;
        check_adds_to(
            "2021-01-01 00:00:00",
            "-1 minute",
            "2020-12-31T23:59:00.000",
        )
        .await;
        check_adds_to("2021-01-01 00:00:00", "-1 hour", "2020-12-31T23:00:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "-1 day", "2020-12-31T00:00:00.000").await;

        check_adds_to(
            "2021-01-01 00:00:00",
            "1 day 1 hour 1 minute 1 second",
            "2021-01-02T01:01:01.000",
        )
        .await;
        check_subs_to(
            "2021-01-02 01:01:01",
            "1 day 1 hour 1 minute 1 second",
            "2021-01-01T00:00:00.000",
        )
        .await;

        check_adds_to("2021-01-01 00:00:00", "1 month", "2021-02-01T00:00:00.000").await;

        check_adds_to("2021-01-01 00:00:00", "1 year", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 year", "2021-01-01T00:00:00.000").await;

        check_adds_to("2021-01-01 00:00:00", "13 month", "2022-02-01T00:00:00.000").await;
        check_subs_to("2022-02-01 00:00:00", "13 month", "2021-01-01T00:00:00.000").await;

        check_adds_to("2021-01-01 23:59:00", "1 minute", "2021-01-02T00:00:00.000").await;
        check_subs_to("2021-01-02 00:00:00", "1 minute", "2021-01-01T23:59:00.000").await;

        check_adds_to("2021-12-01 00:00:00", "1 month", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 month", "2021-12-01T00:00:00.000").await;

        check_adds_to("2021-12-31 00:00:00", "1 day", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 day", "2021-12-31T00:00:00.000").await;

        // Feb 29 on leap and non-leap years.
        check_adds_to("2020-02-29 00:00:00", "1 day", "2020-03-01T00:00:00.000").await;
        check_subs_to("2020-03-01 00:00:00", "1 day", "2020-02-29T00:00:00.000").await;

        check_adds_to("2020-02-28 00:00:00", "1 day", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-02-29 00:00:00", "1 day", "2020-02-28T00:00:00.000").await;

        check_adds_to("2021-02-28 00:00:00", "1 day", "2021-03-01T00:00:00.000").await;
        check_subs_to("2021-03-01 00:00:00", "1 day", "2021-02-28T00:00:00.000").await;

        check_adds_to("2020-02-29 00:00:00", "1 year", "2021-02-28T00:00:00.000").await;
        check_subs_to("2020-02-29 00:00:00", "1 year", "2019-02-28T00:00:00.000").await;

        check_adds_to("2020-01-30 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-03-30 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;

        check_adds_to("2020-01-29 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-03-29 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;

        check_adds_to("2021-01-29 00:00:00", "1 month", "2021-02-28T00:00:00.000").await;
        check_subs_to("2021-03-29 00:00:00", "1 month", "2021-02-28T00:00:00.000").await;
    }

    async fn execute_query(query: String, db: DatabaseProtocol) -> Result<String, CubeError> {
        Ok(execute_query_with_flags(query, db).await?.0)
    }

    async fn execute_query_with_flags(
        query: String,
        db: DatabaseProtocol,
    ) -> Result<(String, StatusFlags), CubeError> {
        execute_queries_with_flags(vec![query], db).await
    }

    async fn execute_queries_with_flags(
        queries: Vec<String>,
        db: DatabaseProtocol,
    ) -> Result<(String, StatusFlags), CubeError> {
        env::set_var("TZ", "UTC");

        let meta = get_test_tenant_ctx();
        let session = get_test_session(db).await;

        let mut output: Vec<String> = Vec::new();
        let mut output_flags = StatusFlags::empty();

        for query in queries {
            let query = convert_sql_to_cube_query(&query, meta.clone(), session.clone()).await;
            match query.unwrap() {
                QueryPlan::DataFusionSelect(flags, plan, ctx) => {
                    let df = DFDataFrame::new(ctx.state, &plan);
                    let batches = df.collect().await?;
                    let frame = batch_to_dataframe(&df.schema().into(), &batches)?;

                    output.push(frame.print());
                    output_flags = flags;
                }
                QueryPlan::MetaTabular(flags, frame) => {
                    output.push(frame.print());
                    output_flags = flags;
                }
                QueryPlan::MetaOk(flags, _) => {
                    output_flags = flags;
                }
            }
        }

        Ok((output.join("\n").to_string(), output_flags))
    }

    #[tokio::test]
    async fn test_show_create_table() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "show_create_table",
            execute_query(
                "show create table KibanaSampleDataEcommerce;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "show_create_table",
            execute_query(
                "show create table `db`.`KibanaSampleDataEcommerce`;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_tables_mysql() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_tables_mysql",
            execute_query(
                "SELECT * FROM information_schema.tables".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_role_table_grants_pg() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_role_table_grants_postgresql",
            execute_query(
                "SELECT * FROM information_schema.role_table_grants".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_observable() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "observable_grants",
            execute_query(
                "SELECT DISTINCT privilege_type
                FROM information_schema.role_table_grants
                WHERE grantee = user
                UNION
                SELECT DISTINCT privilege_type
                FROM information_schema.role_column_grants
                WHERE grantee = user
              "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_role_column_grants_pg() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_role_column_grants_postgresql",
            execute_query(
                "SELECT * FROM information_schema.role_column_grants".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_columns_mysql() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_columns_mysql",
            execute_query(
                "SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = 'db'".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_schemata() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_schemata",
            execute_query(
                "SELECT * FROM information_schema.schemata".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_stats_for_columns() -> Result<(), CubeError> {
        // This query is used by metabase for introspection
        insta::assert_snapshot!(
            "test_information_schema_stats_for_columns",
            execute_query("
            SELECT
                A.TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, A.TABLE_NAME, A.COLUMN_NAME, B.SEQ_IN_INDEX KEY_SEQ, B.INDEX_NAME PK_NAME
            FROM INFORMATION_SCHEMA.COLUMNS A, INFORMATION_SCHEMA.STATISTICS B
            WHERE A.COLUMN_KEY in ('PRI','pri') AND B.INDEX_NAME='PRIMARY'  AND (ISNULL(database()) OR (A.TABLE_SCHEMA = database())) AND (ISNULL(database()) OR (B.TABLE_SCHEMA = database())) AND A.TABLE_NAME = 'OutlierFingerprints'  AND B.TABLE_NAME = 'OutlierFingerprints'  AND A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME
            ORDER BY A.COLUMN_NAME".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_svv_tables() -> Result<(), CubeError> {
        // This query is used by metabase for introspection
        insta::assert_snapshot!(
            "redshift_svv_tables",
            execute_query(
                "SELECT * FROM svv_tables ORDER BY table_name DESC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_svv_table_info() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_svv_table_info",
            execute_query(
                "SELECT * FROM svv_table_info ORDER BY table_id ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_stl_ddltext() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_stl_ddltext",
            execute_query(
                "SELECT * FROM stl_ddltext ORDER BY xid ASC, sequence ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_stl_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_stl_query",
            execute_query(
                "SELECT * FROM stl_query ORDER BY query ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_stl_querytext() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_stl_querytext",
            execute_query(
                "SELECT * FROM stl_querytext ORDER BY query ASC, sequence ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sha1_redshift() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sha1_redshift",
            execute_query(
                "
                SELECT
                    relname,
                    SHA1(relname) hash
                FROM pg_class
                ORDER BY oid ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_monte_carlo_table_introspection() -> Result<(), CubeError> {
        // This query is used by Monte Carlo for introspection
        insta::assert_snapshot!(
            "monte_carlo_table_introspection",
            execute_query(
                r#"
                SELECT
                    "database",
                    "table",
                    "table_id",
                    "schema",
                    "size",
                    "tbl_rows",
                    "estimated_visible_rows"
                FROM svv_table_info
                WHERE (
                    "database" = 'cubedb'
                    AND "schema" = 'public'
                    AND "table" = 'KibanaSampleDataEcommerce'
                ) ORDER BY "table_id"
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_monte_carlo_ddl_introspection() -> Result<(), CubeError> {
        // This query is used by Monte Carlo for introspection
        insta::assert_snapshot!(
            "monte_carlo_ddl_introspection",
            execute_query(
                r#"
                SELECT
                    SHA1(
                        pg_user.usename
                        || '-'
                        || stl_ddltext.xid
                        || '-'
                        || stl_ddltext.pid
                        || '-'
                        || stl_ddltext.starttime
                        || '-'
                        || stl_ddltext.endtime
                    ) as query,
                    stl_ddltext.sequence,
                    stl_ddltext.text,
                    pg_user.usename,
                    stl_ddltext.starttime,
                    stl_ddltext.endtime
                FROM stl_ddltext
                INNER JOIN pg_user ON stl_ddltext.userid = pg_user.usesysid
                WHERE
                    endtime >= '2022-11-15 16:18:47.814515'
                    AND endtime < '2022-11-15 16:31:47.814515'
                ORDER BY 1, 2
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_monte_carlo_query_introspection() -> Result<(), CubeError> {
        // This query is used by Monte Carlo for introspection
        insta::assert_snapshot!(
            "monte_carlo_query_introspection",
            execute_query(
                r#"
                SELECT
                    stl_query.query,
                    stl_querytext.sequence,
                    stl_querytext.text,
                    stl_query.database,
                    pg_user.usename,
                    stl_query.starttime,
                    stl_query.endtime,
                    stl_query.aborted
                FROM stl_query
                INNER JOIN pg_user ON stl_query.userid = pg_user.usesysid
                INNER JOIN stl_querytext USING (query)
                WHERE
                    endtime >= '2022-11-15 16:18:47.814515'
                    AND endtime < '2022-11-15 16:31:47.814515'
                    AND stl_querytext.userid > 1
                ORDER BY 1, 2
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_literal_filter_simplify() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "
                SELECT
                  \"customer_gender\"
                FROM \"KibanaSampleDataEcommerce\"
                WHERE TRUE = TRUE
                LIMIT 1000;"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: None,
            }
        );
        assert_eq!(
            logical_plan.find_filter().is_none(),
            true,
            "Filter must be eliminated"
        );

        let query_plan = convert_select_to_query_plan(
            "
                SELECT
                  \"customer_gender\"
                FROM \"KibanaSampleDataEcommerce\"
                WHERE TRUE = TRUE AND customer_gender = 'male'
                LIMIT 1000;"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_limit_push_down() -> Result<(), CubeError> {
        // 1 level push down
        let query_plan = convert_select_to_query_plan(
            "SELECT l1.*, 1 as projection_should_exist_l1 FROM (\
                    SELECT
                      \"customer_gender\"
                    FROM \"KibanaSampleDataEcommerce\"
                    WHERE TRUE = TRUE
                ) as l1 LIMIT 1000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: None,
            }
        );

        // 2 levels push down
        let query_plan = convert_select_to_query_plan(
            "SELECT l2.*, 1 as projection_should_exist_l2 FROM (\
                SELECT l1.*, 1 as projection_should_exist FROM (\
                    SELECT
                    \"customer_gender\"
                    FROM \"KibanaSampleDataEcommerce\"
                    WHERE TRUE = TRUE
                ) as l1
             ) as l2 LIMIT 1000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_cte() -> Result<(), CubeError> {
        init_logger();

        // CTE called qt_1 is used as ta_2, under the hood DF will use * projection
        let query_plan = convert_select_to_query_plan(
            "WITH \"qt_1\" AS (
                  SELECT
                    \"ta_1\".\"customer_gender\" \"ca_2\",
                    CASE
                      WHEN sum(\"ta_1\".\"count\") IS NOT NULL THEN sum(\"ta_1\".\"count\")
                      ELSE 0
                    END \"ca_3\"
                  FROM \"db\".\"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
                  GROUP BY \"ca_2\"
                )
                SELECT
                  \"qt_1\".\"ca_2\" \"ca_4\",
                  \"qt_1\".\"ca_3\" \"ca_5\"
                FROM \"qt_1\"
                WHERE TRUE = TRUE
                LIMIT 1000;"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_qrt_granularity() -> Result<(), CubeError> {
        init_logger();

        // CTE called qt_1 is used as ta_2, under the hood DF will use * projection
        let query_plan = convert_select_to_query_plan(
            "SELECT
            \"ta_1\".\"count\" \"ca_1\",
            DATE_TRUNC('qtr', \"ta_1\".\"order_date\") \"ca_2\"
            FROM \"db\".\"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
            GROUP BY ca_1, ca_2"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_dow_granularity() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT
              (((DATEDIFF(day, DATE '1970-01-01', \"ta_1\".\"order_date\") + 3) % 7) + 1) \"ca_1\"
            FROM \"db\".\"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
            GROUP BY \"ca_1\""
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_doy_granularity() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"SELECT
              (DATEDIFF(day,
                DATEADD(
                    month,
                    CAST(((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) * -1) AS int),
                    CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + EXTRACT(MONTH FROM "ta_1"."order_date")) * 100) + 1) AS varchar) AS date)),
                    "ta_1"."order_date"
                ) + 1
              ) "ca_1",
              CASE
                WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                ELSE 0
              END "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1"
            LIMIT 5000"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_yearly_granularity() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"SELECT
              CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS varchar) AS date) "ca_1",
              CASE
                WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                ELSE 0
              END "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1";"#
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    // same as test_thought_spot_cte, but with realiasing
    #[tokio::test]
    async fn test_thought_spot_cte_with_realiasing() -> Result<(), CubeError> {
        init_logger();

        // CTE called qt_1 is used as ta_2, under the hood DF will use * projection
        let query_plan = convert_select_to_query_plan(
            "WITH \"qt_1\" AS (
                  SELECT
                    \"ta_1\".\"customer_gender\" \"ca_2\",
                    CASE
                      WHEN sum(\"ta_1\".\"count\") IS NOT NULL THEN sum(\"ta_1\".\"count\")
                      ELSE 0
                    END \"ca_3\"
                  FROM \"db\".\"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
                  GROUP BY \"ca_2\"
                )
                SELECT
                  \"ta_2\".\"ca_2\" \"ca_4\",
                  \"ta_2\".\"ca_3\" \"ca_5\"
                FROM \"qt_1\" \"ta_2\"
                WHERE TRUE = TRUE
                LIMIT 1000;"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "thought_spot_tables",
            execute_query(
                "SELECT * FROM (SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT, table_schema AS TABLE_SCHEM, table_name AS TABLE_NAME, CAST( CASE table_type WHEN 'BASE TABLE' THEN CASE WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM TABLE' WHEN table_schema = 'pg_toast' THEN 'SYSTEM TOAST TABLE' WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY TABLE' ELSE 'TABLE' END WHEN 'VIEW' THEN CASE WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM VIEW' WHEN table_schema = 'pg_toast' THEN NULL WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY VIEW' ELSE 'VIEW' END WHEN 'EXTERNAL TABLE' THEN 'EXTERNAL TABLE' END AS VARCHAR(124)) AS TABLE_TYPE, REMARKS, '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME,  '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM svv_tables) WHERE true  AND current_database() = 'cubedb' AND TABLE_TYPE IN ( 'TABLE', 'VIEW', 'EXTERNAL TABLE')  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "thought_spot_svv_external_schemas",
            execute_query(
                "select 1 from svv_external_schemas where schemaname like 'public'".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "thought_spot_table_columns",
            execute_query(
                "SELECT * FROM ( SELECT current_database() AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname as TABLE_NAME , a.attname as COLUMN_NAME, CAST(case typname when 'text' THEN 12 when 'bit' THEN -7 when 'bool' THEN -7 when 'boolean' THEN -7 when 'varchar' THEN 12 when 'character varying' THEN 12 when 'char' THEN 1 when '\"char\"' THEN 1 when 'character' THEN 1 when 'nchar' THEN 12 when 'bpchar' THEN 1 when 'nvarchar' THEN 12 when 'date' THEN 91 when 'timestamp' THEN 93 when 'timestamp without time zone' THEN 93 when 'smallint' THEN 5 when 'int2' THEN 5 when 'integer' THEN 4 when 'int' THEN 4 when 'int4' THEN 4 when 'bigint' THEN -5 when 'int8' THEN -5 when 'decimal' THEN 3 when 'real' THEN 7 when 'float4' THEN 7 when 'double precision' THEN 8 when 'float8' THEN 8 when 'float' THEN 6 when 'numeric' THEN 2 when '_float4' THEN 2003 when 'timestamptz' THEN 2014 when 'timestamp with time zone' THEN 2014 when '_aclitem' THEN 2003 when '_text' THEN 2003 when 'bytea' THEN -2 when 'oid' THEN -5 when 'name' THEN 12 when '_int4' THEN 2003 when '_int2' THEN 2003 when 'ARRAY' THEN 2003 when 'geometry' THEN -4 when 'super' THEN -16 else 1111 END as SMALLINT) AS DATA_TYPE, t.typname as TYPE_NAME, case typname when 'int4' THEN 10 when 'bit' THEN 1 when 'bool' THEN 1 when 'varchar' THEN atttypmod -4 when 'character varying' THEN atttypmod -4 when 'char' THEN atttypmod -4 when 'character' THEN atttypmod -4 when 'nchar' THEN atttypmod -4 when 'bpchar' THEN atttypmod -4 when 'nvarchar' THEN atttypmod -4 when 'date' THEN 13 when 'timestamp' THEN 29 when 'smallint' THEN 5 when 'int2' THEN 5 when 'integer' THEN 10 when 'int' THEN 10 when 'int4' THEN 10 when 'bigint' THEN 19 when 'int8' THEN 19 when 'decimal' then (atttypmod - 4) >> 16 when 'real' THEN 8 when 'float4' THEN 8 when 'double precision' THEN 17 when 'float8' THEN 17 when 'float' THEN 17 when 'numeric' THEN (atttypmod - 4) >> 16 when '_float4' THEN 8 when 'timestamptz' THEN 35 when 'oid' THEN 10 when '_int4' THEN 10 when '_int2' THEN 5 when 'geometry' THEN NULL when 'super' THEN NULL else 2147483647 end as COLUMN_SIZE , null as BUFFER_LENGTH , case typname when 'float4' then 8 when 'float8' then 17 when 'numeric' then (atttypmod - 4) & 65535 when 'timestamp' then 6 when 'geometry' then NULL when 'super' then NULL else 0 end as DECIMAL_DIGITS, 10 AS NUM_PREC_RADIX , case a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) when 'false' then 1 when NULL then 2 else 0 end AS NULLABLE , dsc.description as REMARKS , pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS COLUMN_DEF, CAST(case typname when 'text' THEN 12 when 'bit' THEN -7 when 'bool' THEN -7 when 'boolean' THEN -7 when 'varchar' THEN 12 when 'character varying' THEN 12 when '\"char\"' THEN 1 when 'char' THEN 1 when 'character' THEN 1 when 'nchar' THEN 1 when 'bpchar' THEN 1 when 'nvarchar' THEN 12 when 'date' THEN 91 when 'timestamp' THEN 93 when 'timestamp without time zone' THEN 93 when 'smallint' THEN 5 when 'int2' THEN 5 when 'integer' THEN 4 when 'int' THEN 4 when 'int4' THEN 4 when 'bigint' THEN -5 when 'int8' THEN -5 when 'decimal' THEN 3 when 'real' THEN 7 when 'float4' THEN 7 when 'double precision' THEN 8 when 'float8' THEN 8 when 'float' THEN 6 when 'numeric' THEN 2 when '_float4' THEN 2003 when 'timestamptz' THEN 2014 when 'timestamp with time zone' THEN 2014 when '_aclitem' THEN 2003 when '_text' THEN 2003 when 'bytea' THEN -2 when 'oid' THEN -5 when 'name' THEN 12 when '_int4' THEN 2003 when '_int2' THEN 2003 when 'ARRAY' THEN 2003 when 'geometry' THEN -4 when 'super' THEN -16 else 1111 END as SMALLINT) AS SQL_DATA_TYPE, CAST(NULL AS SMALLINT) as SQL_DATETIME_SUB , case typname when 'int4' THEN 10 when 'bit' THEN 1 when 'bool' THEN 1 when 'varchar' THEN atttypmod -4 when 'character varying' THEN atttypmod -4 when 'char' THEN atttypmod -4 when 'character' THEN atttypmod -4 when 'nchar' THEN atttypmod -4 when 'bpchar' THEN atttypmod -4 when 'nvarchar' THEN atttypmod -4 when 'date' THEN 13 when 'timestamp' THEN 29 when 'smallint' THEN 5 when 'int2' THEN 5 when 'integer' THEN 10 when 'int' THEN 10 when 'int4' THEN 10 when 'bigint' THEN 19 when 'int8' THEN 19 when 'decimal' then ((atttypmod - 4) >> 16) & 65535 when 'real' THEN 8 when 'float4' THEN 8 when 'double precision' THEN 17 when 'float8' THEN 17 when 'float' THEN 17 when 'numeric' THEN ((atttypmod - 4) >> 16) & 65535 when '_float4' THEN 8 when 'timestamptz' THEN 35 when 'oid' THEN 10 when '_int4' THEN 10 when '_int2' THEN 5 when 'geometry' THEN NULL when 'super' THEN NULL else 2147483647 end as CHAR_OCTET_LENGTH , a.attnum AS ORDINAL_POSITION, case a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) when 'false' then 'YES' when NULL then '' else 'NO' end AS IS_NULLABLE, null as SCOPE_CATALOG , null as SCOPE_SCHEMA , null as SCOPE_TABLE, t.typbasetype AS SOURCE_DATA_TYPE , CASE WHEN left(pg_catalog.pg_get_expr(def.adbin, def.adrelid), 16) = 'default_identity' THEN 'YES' ELSE 'NO' END AS IS_AUTOINCREMENT, IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') WHERE a.attnum > 0 AND NOT a.attisdropped     AND current_database() = 'cubedb' AND n.nspname LIKE 'public' AND c.relname LIKE 'KibanaSampleDataEcommerce' ORDER BY TABLE_SCHEM,c.relname,attnum )  UNION ALL SELECT current_database()::VARCHAR(128) AS TABLE_CAT, schemaname::varchar(128) AS table_schem, tablename::varchar(128) AS table_name, columnname::varchar(128) AS column_name, CAST(CASE columntype_rep WHEN 'text' THEN 12 WHEN 'bit' THEN -7 WHEN 'bool' THEN -7 WHEN 'boolean' THEN -7 WHEN 'varchar' THEN 12 WHEN 'character varying' THEN 12 WHEN 'char' THEN 1 WHEN 'character' THEN 1 WHEN 'nchar' THEN 1 WHEN 'bpchar' THEN 1 WHEN 'nvarchar' THEN 12 WHEN '\"char\"' THEN 1 WHEN 'date' THEN 91 WHEN 'timestamp' THEN 93 WHEN 'timestamp without time zone' THEN 93 WHEN 'timestamp with time zone' THEN 2014 WHEN 'smallint' THEN 5 WHEN 'int2' THEN 5 WHEN 'integer' THEN 4 WHEN 'int' THEN 4 WHEN 'int4' THEN 4 WHEN 'bigint' THEN -5 WHEN 'int8' THEN -5 WHEN 'decimal' THEN 3 WHEN 'real' THEN 7 WHEN 'float4' THEN 7 WHEN 'double precision' THEN 8 WHEN 'float8' THEN 8 WHEN 'float' THEN 6 WHEN 'numeric' THEN 2 WHEN 'timestamptz' THEN 2014 WHEN 'bytea' THEN -2 WHEN 'oid' THEN -5 WHEN 'name' THEN 12 WHEN 'ARRAY' THEN 2003 WHEN 'geometry' THEN -4 WHEN 'super' THEN -16 ELSE 1111 END AS SMALLINT) AS DATA_TYPE, COALESCE(NULL,CASE columntype WHEN 'boolean' THEN 'bool' WHEN 'character varying' THEN 'varchar' WHEN '\"char\"' THEN 'char' WHEN 'smallint' THEN 'int2' WHEN 'integer' THEN 'int4'WHEN 'bigint' THEN 'int8' WHEN 'real' THEN 'float4' WHEN 'double precision' THEN 'float8' WHEN 'timestamp without time zone' THEN 'timestamp' WHEN 'timestamp with time zone' THEN 'timestamptz' ELSE columntype END) AS TYPE_NAME,  CASE columntype_rep WHEN 'int4' THEN 10  WHEN 'bit' THEN 1    WHEN 'bool' THEN 1WHEN 'boolean' THEN 1WHEN 'varchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'character varying' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'char' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER WHEN 'character' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER WHEN 'nchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'bpchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'nvarchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'date' THEN 13 WHEN 'timestamp' THEN 29 WHEN 'timestamp without time zone' THEN 29 WHEN 'smallint' THEN 5 WHEN 'int2' THEN 5 WHEN 'integer' THEN 10 WHEN 'int' THEN 10 WHEN 'int4' THEN 10 WHEN 'bigint' THEN 19 WHEN 'int8' THEN 19 WHEN 'decimal' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN 'real' THEN 8 WHEN 'float4' THEN 8 WHEN 'double precision' THEN 17 WHEN 'float8' THEN 17 WHEN 'float' THEN 17WHEN 'numeric' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN '_float4' THEN 8 WHEN 'timestamptz' THEN 35 WHEN 'timestamp with time zone' THEN 35 WHEN 'oid' THEN 10 WHEN '_int4' THEN 10 WHEN '_int2' THEN 5 WHEN 'geometry' THEN NULL WHEN 'super' THEN NULL ELSE 2147483647 END AS COLUMN_SIZE, NULL AS BUFFER_LENGTH, CASE columntype WHEN 'real' THEN 8 WHEN 'float4' THEN 8 WHEN 'double precision' THEN 17 WHEN 'float8' THEN 17 WHEN 'timestamp' THEN 6 WHEN 'timestamp without time zone' THEN 6 WHEN 'geometry' THEN NULL WHEN 'super' THEN NULL ELSE 0 END AS DECIMAL_DIGITS, 10 AS NUM_PREC_RADIX, NULL AS NULLABLE,  NULL AS REMARKS,   NULL AS COLUMN_DEF, CAST(CASE columntype_rep WHEN 'text' THEN 12 WHEN 'bit' THEN -7 WHEN 'bool' THEN -7 WHEN 'boolean' THEN -7 WHEN 'varchar' THEN 12 WHEN 'character varying' THEN 12 WHEN 'char' THEN 1 WHEN 'character' THEN 1 WHEN 'nchar' THEN 12 WHEN 'bpchar' THEN 1 WHEN 'nvarchar' THEN 12 WHEN '\"char\"' THEN 1 WHEN 'date' THEN 91 WHEN 'timestamp' THEN 93 WHEN 'timestamp without time zone' THEN 93 WHEN 'timestamp with time zone' THEN 2014 WHEN 'smallint' THEN 5 WHEN 'int2' THEN 5 WHEN 'integer' THEN 4 WHEN 'int' THEN 4 WHEN 'int4' THEN 4 WHEN 'bigint' THEN -5 WHEN 'int8' THEN -5 WHEN 'decimal' THEN 3 WHEN 'real' THEN 7 WHEN 'float4' THEN 7 WHEN 'double precision' THEN 8 WHEN 'float8' THEN 8 WHEN 'float' THEN 6 WHEN 'numeric' THEN 2 WHEN 'timestamptz' THEN 2014 WHEN 'bytea' THEN -2 WHEN 'oid' THEN -5 WHEN 'name' THEN 12 WHEN 'ARRAY' THEN 2003 WHEN 'geometry' THEN -4 WHEN 'super' THEN -4 ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE, CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB, CASE WHEN LEFT (columntype,7) = 'varchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER WHEN LEFT (columntype,4) = 'char' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER WHEN columntype = 'string' THEN 16383  ELSE NULL END AS CHAR_OCTET_LENGTH, columnnum AS ORDINAL_POSITION, NULL AS IS_NULLABLE,  NULL AS SCOPE_CATALOG,  NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE, 'NO' AS IS_AUTOINCREMENT, 'NO' as IS_GENERATEDCOLUMN FROM (select lbv_cols.schemaname, lbv_cols.tablename, lbv_cols.columnname,REGEXP_REPLACE(REGEXP_REPLACE(lbv_cols.columntype,'\\\\(.*\\\\)'),'^_.+','ARRAY') as columntype_rep,columntype, lbv_cols.columnnum from pg_get_late_binding_view_cols() lbv_cols( schemaname name, tablename name, columnname name, columntype text, columnnum int)) lbv_columns   WHERE true  AND current_database() = 'cubedb' AND schemaname LIKE 'public' AND tablename LIKE 'KibanaSampleDataEcommerce';".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "thought_spot_attributes",
            execute_query(
                "SELECT
                    current_database() AS PKTABLE_CAT,
                    pkn.nspname AS PKTABLE_SCHEM,
                    pkc.relname AS PKTABLE_NAME,
                    pka.attname AS PKCOLUMN_NAME,
                    current_database() AS FKTABLE_CAT,
                    fkn.nspname AS FKTABLE_SCHEM,
                    fkc.relname AS FKTABLE_NAME,
                    fka.attname AS FKCOLUMN_NAME,
                    pos.n AS KEY_SEQ,
                    CASE
                        con.confupdtype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS UPDATE_RULE,
                    CASE
                        con.confdeltype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS DELETE_RULE,
                    con.conname AS FK_NAME,
                    pkic.relname AS PK_NAME,
                    CASE
                        WHEN con.condeferrable
                        AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                    END AS DEFERRABILITY
                FROM
                    pg_catalog.pg_namespace pkn,
                    pg_catalog.pg_class pkc,
                    pg_catalog.pg_attribute pka,
                    pg_catalog.pg_namespace fkn,
                    pg_catalog.pg_class fkc,
                    pg_catalog.pg_attribute fka,
                    pg_catalog.pg_constraint con,
                    pg_catalog.generate_series(1, 32) pos(n),
                    pg_catalog.pg_class pkic,
                    pg_catalog.pg_depend dep
                WHERE
                    pkn.oid = pkc.relnamespace
                    AND pkc.oid = pka.attrelid
                    AND pka.attnum = con.confkey [pos.n]
                    AND con.confrelid = pkc.oid
                    AND fkn.oid = fkc.relnamespace
                    AND fkc.oid = fka.attrelid
                    AND fka.attnum = con.conkey [pos.n]
                    AND con.conrelid = fkc.oid
                    AND con.contype = 'f'
                    AND pkic.relkind = 'i'
                    AND con.oid = dep.objid
                    AND pkic.oid = dep.refobjid
                    AND dep.classid = 'pg_constraint' :: regclass :: oid
                    AND dep.refclassid = 'pg_class' :: regclass :: oid
                    AND fkn.nspname = 'public'
                    AND fkc.relname = 'KibanaSampleDataEcommerce'
                ORDER BY
                    pkn.nspname,
                    pkc.relname,
                    con.conname,
                    pos.n"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_performance_schema_variables() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "performance_schema_session_variables",
            execute_query("SELECT * FROM performance_schema.session_variables WHERE VARIABLE_NAME = 'max_allowed_packet'".to_string(), DatabaseProtocol::MySQL).await?
        );

        insta::assert_snapshot!(
            "performance_schema_global_variables",
            execute_query("SELECT * FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'max_allowed_packet'".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_processlist() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "show_processlist",
            execute_query("SHOW processlist".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_warnings() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "show_warnings",
            execute_query("SHOW warnings".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_collations() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_collations",
            execute_query(
                "SELECT * FROM information_schema.collations".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_processlist() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_processlist",
            execute_query(
                "SELECT * FROM information_schema.processlist".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_if() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                r#"select
                if(null, true, false) as r1,
                if(true, false, true) as r2,
                if(true, 'true', 'false') as r3,
                if(true, CAST(1 as int), CAST(2 as bigint)) as c1,
                if(false, CAST(1 as int), CAST(2 as bigint)) as c2,
                if(true, CAST(1 as bigint), CAST(2 as int)) as c3
            "#
                .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            "+-------+-------+------+----+----+----+\n\
            | r1    | r2    | r3   | c1 | c2 | c3 |\n\
            +-------+-------+------+----+----+----+\n\
            | false | false | true | 1  | 2  | 1  |\n\
            +-------+-------+------+----+----+----+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_least() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                least(1, 2) as r1, \
                least(2, 1) as r2, \
                least(null, 1) as r3, \
                least(1, null) as r4
            "
                .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            "+----+----+------+------+\n\
            | r1 | r2 | r3   | r4   |\n\
            +----+----+------+------+\n\
            | 1  | 1  | NULL | NULL |\n\
            +----+----+------+------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ucase() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                ucase('super stroka') as r1
            "
                .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            "+--------------+\n\
            | r1           |\n\
            +--------------+\n\
            | SUPER STROKA |\n\
            +--------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_convert_tz() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select convert_tz('2021-12-08T15:50:14.337Z'::timestamp, @@GLOBAL.time_zone, '+00:00') as r1;".to_string(), DatabaseProtocol::MySQL
            )
            .await?,
            "+-------------------------+\n\
            | r1                      |\n\
            +-------------------------+\n\
            | 2021-12-08T15:50:14.337 |\n\
            +-------------------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_timediff() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                    timediff('1994-11-26T13:25:00.000Z'::timestamp, '1994-11-26T13:25:00.000Z'::timestamp) as r1
                ".to_string(), DatabaseProtocol::MySQL
            )
            .await?,
            "+------------------------------------------------+\n\
            | r1                                             |\n\
            +------------------------------------------------+\n\
            | 0 years 0 mons 0 days 0 hours 0 mins 0.00 secs |\n\
            +------------------------------------------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_instr() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                    instr('rust is killing me', 'r') as r1,
                    instr('rust is killing me', 'e') as r2,
                    instr('Rust is killing me', 'unknown') as r3;
                "
                .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            "+----+----+----+\n\
            | r1 | r2 | r3 |\n\
            +----+----+----+\n\
            | 1  | 18 | 0  |\n\
            +----+----+----+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ends_with() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "ends_with",
            execute_query(
                "select \
                    ends_with('rust is killing me', 'me') as r1,
                    ends_with('rust is killing me', 'no') as r2
                "
                .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_locate() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                    locate('r', 'rust is killing me') as r1,
                    locate('e', 'rust is killing me') as r2,
                    locate('unknown', 'Rust is killing me') as r3
                "
                .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            "+----+----+----+\n\
            | r1 | r2 | r3 |\n\
            +----+----+----+\n\
            | 1  | 18 | 0  |\n\
            +----+----+----+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_gdata_studio() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "test_gdata_studio",
            execute_query(
                // This query I saw in Google Data Studio
                "/* mysql-connector-java-5.1.49 ( Revision: ad86f36e100e104cd926c6b81c8cab9565750116 ) */
                SELECT  \
                    @@session.auto_increment_increment AS auto_increment_increment, \
                    @@character_set_client AS character_set_client, \
                    @@character_set_connection AS character_set_connection, \
                    @@character_set_results AS character_set_results, \
                    @@character_set_server AS character_set_server, \
                    @@collation_server AS collation_server, \
                    @@collation_connection AS collation_connection, \
                    @@init_connect AS init_connect, \
                    @@interactive_timeout AS interactive_timeout, \
                    @@license AS license, \
                    @@lower_case_table_names AS lower_case_table_names, \
                    @@max_allowed_packet AS max_allowed_packet, \
                    @@net_buffer_length AS net_buffer_length, \
                    @@net_write_timeout AS net_write_timeout, \
                    @@sql_mode AS sql_mode, \
                    @@system_time_zone AS system_time_zone, \
                    @@time_zone AS time_zone, \
                    @@transaction_isolation AS transaction_isolation, \
                    @@wait_timeout AS wait_timeout
                "
                .to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_variable() -> Result<(), CubeError> {
        // LIKE
        insta::assert_snapshot!(
            "show_variables_like_sql_mode",
            execute_query(
                "show variables like 'sql_mode';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // LIKE pattern
        insta::assert_snapshot!(
            "show_variables_like",
            execute_query(
                "show variables like '%_mode';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Negative test, we dont define this variable
        insta::assert_snapshot!(
            "show_variables_like_aurora",
            execute_query(
                "show variables like 'aurora_version';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // All variables
        insta::assert_snapshot!(
            "show_variables",
            execute_query("show variables;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // Postgres escaped with quotes
        insta::assert_snapshot!(
            "show_variable_quoted",
            execute_query(
                "show \"max_allowed_packet\";".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        // psqlodbc
        insta::assert_snapshot!(
            "show_max_identifier_length",
            execute_query(
                "show max_identifier_length;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_columns() -> Result<(), CubeError> {
        // Simplest syntax
        insta::assert_snapshot!(
            "show_columns",
            execute_query(
                "show columns from KibanaSampleDataEcommerce;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // FULL
        insta::assert_snapshot!(
            "show_columns_full",
            execute_query(
                "show full columns from KibanaSampleDataEcommerce;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // LIKE
        insta::assert_snapshot!(
            "show_columns_like",
            execute_query(
                "show columns from KibanaSampleDataEcommerce like '%ice%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // WHERE
        insta::assert_snapshot!(
            "show_columns_where",
            execute_query(
                "show columns from KibanaSampleDataEcommerce where Type = 'int';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // FROM db FROM tbl
        insta::assert_snapshot!(
            "show_columns_from_db",
            execute_query(
                "show columns from KibanaSampleDataEcommerce from db like 'count';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Everything
        insta::assert_snapshot!(
            "show_columns_everything",
            execute_query(
                "show full columns from KibanaSampleDataEcommerce from db like '%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_tables() -> Result<(), CubeError> {
        // Simplest syntax
        insta::assert_snapshot!(
            "show_tables_simple",
            execute_query("show tables;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // FULL
        insta::assert_snapshot!(
            "show_tables_full",
            execute_query("show full tables;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // LIKE
        insta::assert_snapshot!(
            "show_tables_like",
            execute_query(
                "show tables like '%ban%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // WHERE
        insta::assert_snapshot!(
            "show_tables_where",
            execute_query(
                "show tables where Tables_in_db = 'Logs';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // FROM db
        insta::assert_snapshot!(
            "show_tables_from_db",
            execute_query("show tables from db;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // Everything
        insta::assert_snapshot!(
            "show_tables_everything",
            execute_query(
                "show full tables from db like '%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_tableau() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_table_name_column_name_query",
            execute_query(
                "SELECT `table_name`, `column_name`
                FROM `information_schema`.`columns`
                WHERE `data_type`='enum' AND `table_schema`='db'"
                    .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_null_text_query",
            execute_query(
                "
                SELECT
                    NULL::text AS PKTABLE_CAT,
                    pkn.nspname AS PKTABLE_SCHEM,
                    pkc.relname AS PKTABLE_NAME,
                    pka.attname AS PKCOLUMN_NAME,
                    NULL::text AS FKTABLE_CAT,
                    fkn.nspname AS FKTABLE_SCHEM,
                    fkc.relname AS FKTABLE_NAME,
                    fka.attname AS FKCOLUMN_NAME,
                    pos.n AS KEY_SEQ,
                    CASE con.confupdtype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS UPDATE_RULE,
                    CASE con.confdeltype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS DELETE_RULE,
                    con.conname AS FK_NAME,
                    pkic.relname AS PK_NAME,
                    CASE
                        WHEN con.condeferrable AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                    END AS DEFERRABILITY
                FROM
                    pg_catalog.pg_namespace pkn,
                    pg_catalog.pg_class pkc,
                    pg_catalog.pg_attribute pka,
                    pg_catalog.pg_namespace fkn,
                    pg_catalog.pg_class fkc,
                    pg_catalog.pg_attribute fka,
                    pg_catalog.pg_constraint con,
                    pg_catalog.generate_series(1, 32) pos(n),
                    pg_catalog.pg_class pkic
                WHERE
                    pkn.oid = pkc.relnamespace AND
                    pkc.oid = pka.attrelid AND
                    pka.attnum = con.confkey[pos.n] AND
                    con.confrelid = pkc.oid AND
                    fkn.oid = fkc.relnamespace AND
                    fkc.oid = fka.attrelid AND
                    fka.attnum = con.conkey[pos.n] AND
                    con.conrelid = fkc.oid AND
                    con.contype = 'f' AND
                    (pkic.relkind = 'i' OR pkic.relkind = 'I') AND
                    pkic.oid = con.conindid AND
                    fkn.nspname = 'public' AND
                    fkc.relname = 'payment'
                ORDER BY
                    pkn.nspname,
                    pkc.relname,
                    con.conname,
                    pos.n
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_table_cat_query",
            execute_query(
                "
                SELECT
                    result.TABLE_CAT,
                    result.TABLE_SCHEM,
                    result.TABLE_NAME,
                    result.COLUMN_NAME,
                    result.KEY_SEQ,
                    result.PK_NAME
                FROM
                    (
                        SELECT
                            NULL AS TABLE_CAT,
                            n.nspname AS TABLE_SCHEM,
                            ct.relname AS TABLE_NAME,
                            a.attname AS COLUMN_NAME,
                            (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
                            ci.relname AS PK_NAME,
                            information_schema._pg_expandarray(i.indkey) AS KEYS,
                            a.attnum AS A_ATTNUM
                        FROM pg_catalog.pg_class ct
                        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
                        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                        JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
                        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                        WHERE
                            true AND
                            n.nspname = 'public' AND
                            ct.relname = 'payment' AND
                            i.indisprimary
                    ) result
                    where result.A_ATTNUM = (result.KEYS).x
                ORDER BY
                    result.table_name,
                    result.pk_name,
                    result.key_seq;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_excel() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "excel_select_db_query",
            execute_query(
                "
                SELECT
                    'db' as Database,
                    ns.nspname as Schema,
                    relname as Name,
                    CASE
                        WHEN ns.nspname Like E'pg\\_catalog' then 'Catalog'
                        WHEN ns.nspname Like E'information\\_schema' then 'Information'
                        WHEN relkind = 'f' then 'Foreign'
                        ELSE 'User'
                    END as TableType,
                    pg_get_userbyid(relowner) AS definer,
                    rel.oid as Oid,
                    relacl as ACL,
                    true as HasOids,
                    relhassubclass as HasSubtables,
                    reltuples as RowNumber,
                    description as Comment,
                    relnatts as ColumnNumber,
                    relhastriggers as TriggersNumber,
                    conname as Constraint,
                    conkey as ColumnConstrainsIndexes
                FROM pg_class rel
                INNER JOIN pg_namespace ns ON relnamespace = ns.oid
                LEFT OUTER JOIN pg_description des ON
                    des.objoid = rel.oid AND
                    des.objsubid = 0
                LEFT OUTER JOIN pg_constraint c ON
                    c.conrelid = rel.oid AND
                    c.contype = 'p'
                WHERE
                    (
                        (relkind = 'r') OR
                        (relkind = 's') OR
                        (relkind = 'f')
                    ) AND
                    NOT ns.nspname LIKE E'pg\\_temp\\_%%' AND
                    NOT ns.nspname like E'pg\\_%' AND
                    NOT ns.nspname like E'information\\_schema' AND
                    ns.nspname::varchar like E'public' AND
                    relname::varchar like '%' AND
                    pg_get_userbyid(relowner)::varchar like '%'
                ORDER BY relname
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_typname_big_query",
            execute_query(
                "
                SELECT
                    typname as name,
                    n.nspname as Schema,
                    pg_get_userbyid(typowner) as Definer,
                    typlen as Length,
                    t.oid as oid,
                    typbyval as IsReferenceType,
                    case
                        when typtype = 'b' then 'base'
                        when typtype = 'd' then 'domain'
                        when typtype = 'c' then 'composite'
                        when typtype = 'd' then 'pseudo'
                    end as Type,
                    case
                        when typalign = 'c' then 'char'
                        when typalign = 's' then 'short'
                        when typalign = 'i' then 'int'
                        else 'double'
                    end as alignment,
                    case
                        when typstorage = 'p' then 'plain'
                        when typstorage = 'e' then 'secondary'
                        when typstorage = 'm' then 'compressed inline'
                        else 'secondary or compressed inline'
                    end as ValueStorage,
                    typdefault as DefaultValue,
                    description as comment
                FROM pg_type t
                LEFT OUTER JOIN
                    pg_description des ON des.objoid = t.oid,
                    pg_namespace n
                WHERE
                    t.typnamespace = n.oid and
                    t.oid::varchar like E'1033' and
                    typname like E'%' and
                    n.nspname like E'%' and
                    pg_get_userbyid(typowner)::varchar like E'%' and
                    typtype::varchar like E'c'
                ORDER BY name
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_typname_aclitem_query",
            execute_query(
                "
                SELECT
                    typname as name,
                    t.oid as oid,
                    typtype as Type,
                    typelem as TypeElement
                FROM pg_type t
                WHERE
                    t.oid::varchar like '1034' and
                    typtype::varchar like 'b' and
                    typelem != 0
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_pg_constraint_query",
            execute_query(
                "
                SELECT
                    a.conname as Name,
                    ns.nspname as Schema,
                    mycl.relname as Table,
                    b.conname as ReferencedKey,
                    frns.nspname as ReferencedSchema,
                    frcl.relname as ReferencedTable,
                    a.oid as Oid,
                    a.conkey as ColumnIndexes,
                    a.confkey as ForeignColumnIndexes,
                    a.confupdtype as UpdateActionCode,
                    a.confdeltype as DeleteActionCode,
                    a.confmatchtype as ForeignKeyMatchType,
                    a.condeferrable as IsDeferrable,
                    a.condeferred as Iscondeferred
                FROM pg_constraint a
                inner join pg_constraint b on (
                    a.confrelid = b.conrelid AND
                    a.confkey = b.conkey
                )
                INNER JOIN pg_namespace ns ON a.connamespace = ns.oid
                INNER JOIN pg_class mycl ON a.conrelid = mycl.oid
                LEFT OUTER JOIN pg_class frcl ON a.confrelid = frcl.oid
                INNER JOIN pg_namespace frns ON frcl.relnamespace = frns.oid
                WHERE
                    a.contype = 'f' AND
                    (
                        b.contype = 'p' OR
                        b.contype = 'u'
                    ) AND
                    a.oid::varchar like '%' AND
                    a.conname like '%' AND
                    ns.nspname like E'public' AND
                    mycl.relname like E'KibanaSampleDataEcommerce' AND
                    frns.nspname like '%' AND
                    frcl.relname like '%'
                ORDER BY 1
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_pg_attribute_query",
            execute_query(
                "
                SELECT DISTINCT
                    attname AS Name,
                    attnum
                FROM pg_attribute
                JOIN pg_class ON oid = attrelid
                INNER JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    pg_namespace.nspname like 'public' AND
                    relname like 'KibanaSampleDataEcommerce' AND
                    attnum in (2)
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_fkey_query",
            execute_query(
                "
                SELECT
                    nspname as Schema,
                    cl.relname as Table,
                    clr.relname as RefTableName,
                    conname as Name,
                    conkey as ColumnIndexes,
                    confkey as ColumnRefIndexes
                FROM pg_constraint
                INNER JOIN pg_namespace ON connamespace = pg_namespace.oid
                INNER JOIN pg_class cl ON conrelid = cl.oid
                INNER JOIN pg_class clr ON confrelid = clr.oid
                WHERE
                    contype = 'f' AND
                    conname like E'sample\\_fkey' AND
                    nspname like E'public' AND
                    cl.relname like E'KibanaSampleDataEcommerce'
                order by 1
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_large_select_query",
            execute_query(
                "
                SELECT
                    na.nspname as Schema,
                    cl.relname as Table,
                    att.attname AS Name,
                    att.attnum as Position,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'true'
                        ELSE 'false'
                    END as Nullable,
                    CASE
                        WHEN
                            ty.typname Like 'bit' OR
                            ty.typname Like 'varbit' and
                            att.atttypmod > 0
                        THEN att.atttypmod
                        WHEN ty.typname Like 'interval' THEN -1
                        WHEN att.atttypmod > 0 THEN att.atttypmod - 4
                        ELSE att.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS DatetimeLength,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'false'
                        ELSE 'true'
                    END as IsUnique,
                    att.atthasdef as HasDefaultValue,
                    att.attisdropped as IsDropped,
                    att.attinhcount as ancestorCount,
                    att.attndims as Dimension,
                    CASE
                        WHEN attndims > 0 THEN true
                        ELSE false
                    END AS isarray,
                    CASE
                        WHEN ty.typname = 'bpchar' THEN 'char'
                        WHEN ty.typname = '_bpchar' THEN '_char'
                        ELSE ty.typname
                    END as TypeName,
                    tn.nspname as TypeSchema,
                    et.typname as elementaltypename,
                    description as Comment,
                    cs.relname AS sername,
                    ns.nspname AS serschema,
                    att.attidentity as IdentityMode,
                    CAST(pg_get_expr(def.adbin, def.adrelid) AS varchar) as DefaultValue,
                    (SELECT count(1) FROM pg_type t2 WHERE t2.typname=ty.typname) > 1 AS isdup
                FROM pg_attribute att
                JOIN pg_type ty ON ty.oid=atttypid
                JOIN pg_namespace tn ON tn.oid=ty.typnamespace
                JOIN pg_class cl ON
                    cl.oid=attrelid AND
                    (
                        (cl.relkind = 'r') OR
                        (cl.relkind = 's') OR
                        (cl.relkind = 'v') OR
                        (cl.relkind = 'm') OR
                        (cl.relkind = 'f')
                    )
                JOIN pg_namespace na ON na.oid=cl.relnamespace
                LEFT OUTER JOIN pg_type et ON et.oid=ty.typelem
                LEFT OUTER JOIN pg_attrdef def ON
                    adrelid=attrelid AND
                    adnum=attnum
                LEFT OUTER JOIN pg_description des ON
                    des.objoid=attrelid AND
                    des.objsubid=attnum
                LEFT OUTER JOIN (
                    pg_depend
                    JOIN pg_class cs ON
                        objid=cs.oid AND
                        cs.relkind='S' AND
                        classid='pg_class'::regclass::oid
                ) ON
                    refobjid=attrelid AND
                    refobjsubid=attnum
                LEFT OUTER JOIN pg_namespace ns ON ns.oid=cs.relnamespace
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    cl.relname like E'KibanaSampleDataEcommerce' AND
                    na.nspname like E'public' AND
                    att.attname like '%'
                ORDER BY attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_exists_query",
            execute_query(
                "
                SELECT
                    a.attname as fieldname,
                    a.attnum  as fieldordinal,
                    a.atttypid as datatype,
                    a.atttypmod as fieldmod,
                    a.attnotnull as isnull,
                    c.relname as tablename,
                    n.nspname as schema,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c1
                            where
                                c1.conrelid = c.oid and
                                c1.contype = 'p' and
                                a.attnum = ANY (c1.conkey)
                        ) THEN true
                        ELSE false
                    END as iskey,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c1
                            where
                                c1.conrelid = c.oid and
                                c1.contype = 'u' and
                                a.attnum = ANY (c1.conkey)
                        ) THEN true
                        ELSE false
                    END as isunique,
                    CAST(pg_get_expr(d.adbin, d.adrelid) AS varchar) as defvalue,
                    CASE
                        WHEN t.typtype = 'd' THEN t.typbasetype
                        ELSE a.atttypid
                    END as basetype,
                    CASE
                        WHEN a.attidentity = 'a' THEN true
                        ELSE false
                    END as IsAutoIncrement,
                    CASE
                        WHEN
                            t.typname Like 'bit' OR
                            t.typname Like 'varbit' and
                            a.atttypmod > 0
                        THEN a.atttypmod
                        WHEN
                            t.typname Like 'interval' OR
                            t.typname Like 'timestamp' OR
                            t.typname Like 'timestamptz' OR
                            t.typname Like 'time' OR
                            t.typname Like 'timetz'
                        THEN -1
                        WHEN a.atttypmod > 0 THEN a.atttypmod - 4
                        ELSE a.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS DatetimePrecision
                FROM pg_namespace n
                INNER JOIN pg_class c ON c.relnamespace = n.oid
                INNER JOIN pg_attribute a on c.oid = a.attrelid
                LEFT JOIN pg_attrdef d on
                    d.adrelid = a.attrelid and
                    d.adnum =a.attnum
                LEFT JOIN pg_type t on t.oid = a.atttypid
                WHERE
                    a.attisdropped = false AND
                    (
                        (c.relkind = 'r') OR
                        (c.relkind = 's') OR
                        (c.relkind = 'v') OR
                        (c.relkind = 'm') OR
                        (c.relkind = 'f')
                    ) AND
                    a.attnum > 0 AND
                    ((
                        c.relname LIKE 'KibanaSampleDataEcommerce' AND
                        n.nspname LIKE 'public'
                    ))
                ORDER BY
                    tablename,
                    fieldordinal
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_explain_table() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            execute_query(
                "explain KibanaSampleDataEcommerce;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_use_db() -> Result<(), CubeError> {
        assert_eq!(
            execute_query("use db;".to_string(), DatabaseProtocol::MySQL).await?,
            "".to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_variable() -> Result<(), CubeError> {
        assert_eq!(
            execute_query_with_flags("set autocommit=1;".to_string(), DatabaseProtocol::MySQL)
                .await?,
            (
                "++\n++\n++".to_string(),
                StatusFlags::SERVER_STATE_CHANGED | StatusFlags::AUTOCOMMIT
            )
        );

        assert_eq!(
            execute_query_with_flags(
                "set character_set_results = utf8;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            ("++\n++\n++".to_string(), StatusFlags::SERVER_STATE_CHANGED)
        );

        assert_eq!(
            execute_query_with_flags(
                "set autocommit=1, sql_mode = concat(@@sql_mode,',strict_trans_tables');"
                    .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            (
                "++\n++\n++".to_string(),
                StatusFlags::SERVER_STATE_CHANGED | StatusFlags::AUTOCOMMIT
            )
        );

        insta::assert_snapshot!(
            "pg_set_app_show",
            execute_queries_with_flags(
                vec![
                    "set application_name = 'testing app'".to_string(),
                    "show application_name".to_string()
                ],
                DatabaseProtocol::PostgreSQL
            )
            .await?
            .0
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_backend_pid() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_backend_pid",
            execute_query(
                "select pg_backend_pid();".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_collation() -> Result<(), CubeError> {
        // Simplest syntax
        insta::assert_snapshot!(
            "show_collation",
            execute_query("show collation;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // LIKE
        insta::assert_snapshot!(
            "show_collation_like",
            execute_query(
                "show collation like '%unicode%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // WHERE
        insta::assert_snapshot!(
            "show_collation_where",
            execute_query(
                "show collation where Id between 255 and 260;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Superset query
        insta::assert_snapshot!(
            "show_collation_superset",
            execute_query(
                "show collation where charset = 'utf8mb4' and collation = 'utf8mb4_bin';"
                    .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_explain() -> Result<(), CubeError> {
        // SELECT with no tables (inline eval)
        insta::assert_snapshot!(
            execute_query("EXPLAIN SELECT 1+1;".to_string(), DatabaseProtocol::MySQL).await?
        );

        insta::assert_snapshot!(
            execute_query(
                "EXPLAIN VERBOSE SELECT 1+1;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Execute without asserting with fixture, because metrics can change
        execute_query(
            "EXPLAIN ANALYZE SELECT 1+1;".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await?;

        // SELECT with table and specific columns
        execute_query(
            "EXPLAIN SELECT count, avgPrice FROM KibanaSampleDataEcommerce;".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await?;

        // EXPLAIN for Postgres
        execute_query(
            "EXPLAIN SELECT 1+1;".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            execute_query(
                "SELECT \
                    @@GLOBAL.time_zone AS global_tz, \
                    @@system_time_zone AS system_tz, time_format(   timediff(      now(), convert_tz(now(), @@GLOBAL.time_zone, '+00:00')   ),   '%H:%i' ) AS 'offset'
                ".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        insta::assert_snapshot!(
            execute_query(
                "SELECT \
                TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, \
                CASE data_type WHEN 'bit' THEN -7 WHEN 'tinyblob' THEN -3 WHEN 'mediumblob' THEN -4 WHEN 'longblob' THEN -4 WHEN 'blob' THEN -4 WHEN 'tinytext' THEN 12 WHEN 'mediumtext' THEN -1 WHEN 'longtext' THEN -1 WHEN 'text' THEN -1 WHEN 'date' THEN 91 WHEN 'datetime' THEN 93 WHEN 'decimal' THEN 3 WHEN 'double' THEN 8 WHEN 'enum' THEN 12 WHEN 'float' THEN 7 WHEN 'int' THEN IF( COLUMN_TYPE like '%unsigned%', 4,4) WHEN 'bigint' THEN -5 WHEN 'mediumint' THEN 4 WHEN 'null' THEN 0 WHEN 'set' THEN 12 WHEN 'smallint' THEN IF( COLUMN_TYPE like '%unsigned%', 5,5) WHEN 'varchar' THEN 12 WHEN 'varbinary' THEN -3 WHEN 'char' THEN 1 WHEN 'binary' THEN -2 WHEN 'time' THEN 92 WHEN 'timestamp' THEN 93 WHEN 'tinyint' THEN IF(COLUMN_TYPE like 'tinyint(1)%',-7,-6)  WHEN 'year' THEN 91 ELSE 1111 END  DATA_TYPE, IF(COLUMN_TYPE like 'tinyint(1)%', 'BIT',  UCASE(IF( COLUMN_TYPE LIKE '%(%)%', CONCAT(SUBSTRING( COLUMN_TYPE,1, LOCATE('(',COLUMN_TYPE) - 1 ), SUBSTRING(COLUMN_TYPE ,1+locate(')', COLUMN_TYPE))), COLUMN_TYPE))) TYPE_NAME,  CASE DATA_TYPE  WHEN 'time' THEN IF(DATETIME_PRECISION = 0, 10, CAST(11 + DATETIME_PRECISION as signed integer))  WHEN 'date' THEN 10  WHEN 'datetime' THEN IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))  WHEN 'timestamp' THEN IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))  ELSE   IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,2147483647), NUMERIC_PRECISION)  END COLUMN_SIZE, \
                65535 BUFFER_LENGTH, \
                CONVERT (CASE DATA_TYPE WHEN 'year' THEN NUMERIC_SCALE WHEN 'tinyint' THEN 0 ELSE NUMERIC_SCALE END, UNSIGNED INTEGER) DECIMAL_DIGITS, 10 NUM_PREC_RADIX, \
                IF(IS_NULLABLE = 'yes',1,0) NULLABLE,
                COLUMN_COMMENT REMARKS, \
                COLUMN_DEFAULT COLUMN_DEF, \
                0 SQL_DATA_TYPE, \
                0 SQL_DATETIME_SUB, \
                LEAST(CHARACTER_OCTET_LENGTH,2147483647) CHAR_OCTET_LENGTH, \
                ORDINAL_POSITION, \
                IS_NULLABLE, \
                NULL SCOPE_CATALOG, \
                NULL SCOPE_SCHEMA, \
                NULL SCOPE_TABLE, \
                NULL SOURCE_DATA_TYPE, \
                IF(EXTRA = 'auto_increment','YES','NO') IS_AUTOINCREMENT, \
                IF(EXTRA in ('VIRTUAL', 'PERSISTENT', 'VIRTUAL GENERATED', 'STORED GENERATED') ,'YES','NO') IS_GENERATEDCOLUMN \
                FROM INFORMATION_SCHEMA.COLUMNS  WHERE (ISNULL(database()) OR (TABLE_SCHEMA = database())) AND TABLE_NAME = 'KibanaSampleDataEcommerce' \
                ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        insta::assert_snapshot!(
            execute_query(
                "SELECT
                    KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT,
                    NULL PKTABLE_SCHEM,
                    KCU.REFERENCED_TABLE_NAME PKTABLE_NAME,
                    KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME,
                    KCU.TABLE_SCHEMA FKTABLE_CAT,
                    NULL FKTABLE_SCHEM,
                    KCU.TABLE_NAME FKTABLE_NAME,
                    KCU.COLUMN_NAME FKCOLUMN_NAME,
                    KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,
                    CASE update_rule    WHEN 'RESTRICT' THEN 1   WHEN 'NO ACTION' THEN 3   WHEN 'CASCADE' THEN 0   WHEN 'SET NULL' THEN 2   WHEN 'SET DEFAULT' THEN 4 END UPDATE_RULE,
                    CASE DELETE_RULE WHEN 'RESTRICT' THEN 1  WHEN 'NO ACTION' THEN 3  WHEN 'CASCADE' THEN 0  WHEN 'SET NULL' THEN 2  WHEN 'SET DEFAULT' THEN 4 END DELETE_RULE,
                    RC.CONSTRAINT_NAME FK_NAME,
                    NULL PK_NAME,
                    7 DEFERRABILITY
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
                INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
                WHERE (ISNULL(database()) OR (KCU.TABLE_SCHEMA = database())) AND  KCU.TABLE_NAME = 'SlackMessages' ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ
                ".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_tables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_tables_postgres",
            execute_query(
                "SELECT * FROM information_schema.tables".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_columns_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_columns_postgres",
            execute_query(
                "SELECT * FROM information_schema.columns".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_character_sets_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_character_sets_postgres",
            execute_query(
                "SELECT * FROM information_schema.character_sets".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_key_column_usage_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_key_column_usage_postgres",
            execute_query(
                "SELECT * FROM information_schema.key_column_usage".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_referential_constraints_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_referential_constraints_postgres",
            execute_query(
                "SELECT * FROM information_schema.referential_constraints".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_table_constraints_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_table_constraints_postgres",
            execute_query(
                "SELECT * FROM information_schema.table_constraints".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgtables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgtables_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_tables".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgprepared_statements_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgprepared_statements_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_prepared_statements".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgtype_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgtype_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_type ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgroles_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgroles_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_roles ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgnamespace_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgnamespace_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_namespace".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_am_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgam_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_am".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_regclass() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "dynamic_regclass_postgres_utf8",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT 'pg_class' as a
                    UNION ALL
                    SELECT NULL
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dynamic_regclass_postgres_int32",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT CAST(83 as int) as a
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dynamic_regclass_postgres_int64",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT 83 as a
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_sequence_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgsequence_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_sequence".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgrange_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgrange_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_range".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgattrdef_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgattrdef_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_attrdef".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgattribute_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgattribute_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_attribute".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgindex_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgindex_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_index".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgclass_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgclass_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_class".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgproc_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgproc_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_proc".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdescription_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdescription_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_description".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgconstraint_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgconstraint_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_constraint".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdepend_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdepend_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_depend ORDER BY refclassid ASC, refobjid ASC"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgenum_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgenum_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_enum".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgmatviews_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgmatviews_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_matviews".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdatabase_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdatabase_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_database ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgstatiousertables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgstatiousertables_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_statio_user_tables ORDER BY relid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgstat_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgstats_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_stats".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pg_stat_activity_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pg_stat_activity_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_stat_activity".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pguser_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pguser_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_user ORDER BY usesysid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgextension_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgextension_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_extension".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_constraint_column_usage_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "constraint_column_usage_postgres",
            execute_query(
                "SELECT * FROM information_schema.constraint_column_usage".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_views_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "views_postgres",
            execute_query(
                "SELECT * FROM information_schema.views".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_current_schema_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "current_schema_postgres",
            execute_query(
                "SELECT current_schema()".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_rust_client() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "rust_client_types",
            execute_query(
                r#"SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
                FROM pg_catalog.pg_type t
                LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
                INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                WHERE t.oid = 25"#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_current_schemas_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "current_schemas_postgres",
            execute_query(
                "SELECT current_schemas(false)".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "current_schemas_including_implicit_postgres",
            execute_query(
                "SELECT current_schemas(true)".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_format_type_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "format_type",
            execute_query(
                "
                SELECT
                    t.oid,
                    t.typname,
                    format_type(t.oid, 20) ft20,
                    format_type(t.oid, 5) ft5,
                    format_type(t.oid, 4) ft4,
                    format_type(t.oid, 0) ft0,
                    format_type(t.oid, -1) ftneg,
                    format_type(t.oid, NULL::bigint) ftnull,
                    format_type(cast(t.oid as text), '5') ftstr
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_datetime_precision_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_datetime_precision_simple",
            execute_query(
                "SELECT information_schema._pg_datetime_precision(1184, 3) p".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_datetime_precision_types",
            execute_query(
                "
                SELECT t.oid, information_schema._pg_datetime_precision(t.oid, 3) p
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_numeric_precision_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_numeric_precision_simple",
            execute_query(
                "SELECT information_schema._pg_numeric_precision(1700, 3);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_numeric_precision_types",
            execute_query(
                "
                SELECT t.oid, information_schema._pg_numeric_precision(t.oid, 3) p
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_numeric_scale_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_numeric_scale_simple",
            execute_query(
                "SELECT information_schema._pg_numeric_scale(1700, 50);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_numeric_scale_types",
            execute_query(
                "
                SELECT t.oid, information_schema._pg_numeric_scale(t.oid, 10) s
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_get_userbyid_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_get_userbyid",
            execute_query(
                "
                SELECT pg_get_userbyid(t.id)
                FROM information_schema.testing_dataset t
                WHERE t.id < 15;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unnest_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "unnest_i64_from_table",
            execute_query(
                "SELECT unnest(r.a) FROM (SELECT ARRAY[1,2,3,4] as a UNION ALL SELECT ARRAY[5,6,7,8] as a) as r;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "unnest_str_from_table",
            execute_query(
                "SELECT unnest(r.a) FROM (SELECT ARRAY['1', '2'] as a UNION ALL SELECT ARRAY['3', '4'] as a) as r;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "unnest_i64_scalar",
            execute_query(
                "SELECT unnest(ARRAY[1,2,3,4,5]);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_series_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "generate_series_i64_1",
            execute_query(
                "SELECT generate_series(-5, 5);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_f64_2",
            execute_query(
                "SELECT generate_series(-5, 5, 3);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_f64_1",
            execute_query(
                "SELECT generate_series(-5, 5, 0.5);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_empty_1",
            execute_query(
                "SELECT generate_series(-5, -10, 3);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_empty_2",
            execute_query(
                "SELECT generate_series(1, 5, 0);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_catalog_generate_series_i64",
            execute_query(
                "SELECT pg_catalog.generate_series(1, 5);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_from_table",
            execute_query(
                "select generate_series(1, oid) from pg_catalog.pg_type where oid in (16,17);"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_get_expr_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_get_expr_1",
            execute_query(
                "
                SELECT
                    attrelid,
                    attname,
                    pg_catalog.pg_get_expr(attname, attrelid) default
                FROM pg_catalog.pg_attribute
                ORDER BY
                    attrelid ASC,
                    attname ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        insta::assert_snapshot!(
            "pg_get_expr_2",
            execute_query(
                "
                SELECT
                    attrelid,
                    attname,
                    pg_catalog.pg_get_expr(attname, attrelid, true) default
                FROM pg_catalog.pg_attribute
                ORDER BY
                    attrelid ASC,
                    attname ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_subscripts_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_generate_subscripts_1",
            execute_query(
                "SELECT generate_subscripts(r.a, 1) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_generate_subscripts_2_forward",
            execute_query(
                "SELECT generate_subscripts(r.a, 1, false) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_generate_subscripts_2_reverse",
            execute_query(
                "SELECT generate_subscripts(r.a, 1, true) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_generate_subscripts_3",
            execute_query(
                "SELECT generate_subscripts(r.a, 2) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_expandarray_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_expandarray_value",
            execute_query(
                "SELECT (information_schema._pg_expandarray(t.a)).x FROM pg_catalog.pg_class c, (SELECT ARRAY[5, 10, 15] a) t;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_expandarray_index",
            execute_query(
                "SELECT (information_schema._pg_expandarray(t.a)).n FROM pg_catalog.pg_class c, (SELECT ARRAY[5, 10, 15] a) t;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_type_is_visible_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_type_is_visible",
            execute_query(
                "
                SELECT t.oid, t.typname, n.nspname, pg_catalog.pg_type_is_visible(t.oid) is_visible
                FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n
                WHERE t.typnamespace = n.oid
                ORDER BY t.oid ASC;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_get_constraintdef_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_get_constraintdef_1",
            execute_query(
                "select pg_catalog.pg_get_constraintdef(r.oid, true) from pg_catalog.pg_constraint r;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_get_constraintdef_2",
            execute_query(
                "select pg_catalog.pg_get_constraintdef(r.oid) from pg_catalog.pg_constraint r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_to_regtype_pid() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_to_regtype",
            execute_query(
                "select
                    to_regtype('bool') b,
                    to_regtype('name') n,
                    to_regtype('_int4') ai,
                    to_regtype('unknown') u
                ;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_date_part_quarter() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "date_part_quarter",
            execute_query(
                "
                SELECT
                    t.d,
                    date_part('quarter', t.d) q
                FROM (
                    SELECT TIMESTAMP '2000-01-05 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2005-05-20 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2010-08-02 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2020-10-01 00:00:00+00:00' d
                ) t
                ORDER BY t.d ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_array_lower() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "array_lower_scalar",
            execute_query(
                "
                SELECT
                    array_lower(ARRAY[1,2,3,4,5]) v1,
                    array_lower(ARRAY[5,4,3,2,1]) v2,
                    array_lower(ARRAY[5,4,3,2,1], 1) v3
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "array_lower_column",
            execute_query(
                "
                SELECT
                    array_lower(t.v) q
                FROM (
                    SELECT ARRAY[1,2,3,4,5] as v UNION ALL
                    SELECT ARRAY[5,4,3,2,1] as v
                ) t
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "array_lower_string",
            execute_query(
                "SELECT array_lower(ARRAY['a', 'b']) v1".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_array_upper() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "array_upper_scalar",
            execute_query(
                "
                SELECT
                    array_upper(ARRAY[1,2,3,4,5]) v1,
                    array_upper(ARRAY[5,4,3]) v2,
                    array_upper(ARRAY[5,4], 1) v3
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "array_upper_column",
            execute_query(
                "
                SELECT
                    array_upper(t.v) q
                FROM (
                    SELECT ARRAY[1,2,3,4,5] as v
                    UNION ALL
                    SELECT ARRAY[5,4,3,2] as v
                    UNION ALL
                    SELECT ARRAY[5,4,3] as v
                ) t
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "array_upper_string",
            execute_query(
                "SELECT array_upper(ARRAY['a', 'b']) v1".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_catalog_udf_search_path() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_catalog_udf_search_path",
            execute_query(
                "SELECT version() UNION ALL SELECT pg_catalog.version();".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_has_schema_privilege_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "has_schema_privilege",
            execute_query(
                "SELECT
                    nspname,
                    has_schema_privilege('ovr', nspname, 'CREATE') create,
                    has_schema_privilege('ovr', nspname, 'USAGE') usage
                FROM pg_namespace
                ORDER BY nspname ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "has_schema_privilege_default_user",
            execute_query(
                "SELECT
                    nspname,
                    has_schema_privilege(nspname, 'CREATE') create,
                    has_schema_privilege(nspname, 'USAGE') usage
                FROM pg_namespace
                ORDER BY nspname ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_total_relation_size() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_total_relation_size",
            execute_query(
                "SELECT
                    oid,
                    relname,
                    pg_total_relation_size(oid) relsize
                FROM pg_class
                ORDER BY oid ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_discard_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "discard_postgres_all",
            execute_query("DISCARD ALL;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );
        insta::assert_snapshot!(
            "discard_postgres_plans",
            execute_query("DISCARD PLANS;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );
        insta::assert_snapshot!(
            "discard_postgres_sequences",
            execute_query(
                "DISCARD SEQUENCES;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        insta::assert_snapshot!(
            "discard_postgres_temporary",
            execute_query(
                "DISCARD TEMPORARY;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        insta::assert_snapshot!(
            "discard_postgres_temp",
            execute_query("DISCARD TEMP;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_interval_mul() -> Result<(), CubeError> {
        let base_timestamp = "TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')";
        let units = vec!["year", "month", "week", "day", "hour", "minute", "second"];
        let multiplicands = vec![1, 5, -10];

        let selects = units
            .iter()
            .enumerate()
            .map(|(i, unit)| {
                let columns = multiplicands
                    .iter()
                    .map(|multiplicand| {
                        format!(
                            "{} + {} * interval '1 {}' AS \"i*{}\"",
                            base_timestamp, multiplicand, unit, multiplicand
                        )
                    })
                    .collect::<Vec<_>>();
                format!(
                    "SELECT {} AS id, '{}' AS unit, {}",
                    i,
                    unit,
                    columns.join(", ")
                )
            })
            .collect::<Vec<_>>();
        let query = format!("{} ORDER BY id ASC", selects.join(" UNION ALL "));
        insta::assert_snapshot!(
            "interval_mul",
            execute_query(query, DatabaseProtocol::PostgreSQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_interval_sum() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "interval_sum",
            execute_query(
                r#"
                SELECT
                    TO_TIMESTAMP('2019-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')
                    + INTERVAL '1 MONTH'
                    + INTERVAL '1 WEEK'
                    + INTERVAL '1 DAY'
                    AS t
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_like_escape_symbol() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "like_escape_symbol",
            execute_query(
                "
                SELECT attname, test
                FROM (
                    SELECT
                        attname,
                        't%est' test
                    FROM pg_catalog.pg_attribute
                ) pga
                WHERE
                    attname LIKE 'is\\_%_ale' AND
                    test LIKE 't\\%e%'
                ORDER BY attname
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_psql_list() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "psql_list",
            execute_query(
                r#"
                SELECT
                    d.datname as "Name",
                    pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
                    pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
                    d.datcollate as "Collate",
                    d.datctype as "Ctype",
                    NULL as "ICU Locale",
                    'libc' AS "Locale Provider",
                    pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges"
                FROM pg_catalog.pg_database d
                ORDER BY 1
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_isnull_two_arg() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "isnull_two_arg",
            execute_query(
                r#"
                SELECT id, result
                FROM (
                    SELECT 1 id, isnull('left', 'right') result
                    UNION ALL
                    SELECT 2 id, isnull(NULL, 'right') result
                    UNION ALL
                    SELECT 3 id, isnull(NULL, NULL) result
                ) t
                ORDER BY id
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_regexp_replace_default_replacer() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_regexp_replace_default_replacer",
            execute_query(
                "SELECT regexp_replace('Test test test', 'test')".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_charindex() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "redshift_charindex",
            execute_query(
                r#"
                SELECT
                    charindex('d', 'abcdefg') d,
                    charindex('h', 'abcdefg') none
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn superset_meta_queries() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "superset_attname_query",
            execute_query(
                r#"SELECT a.attname
                FROM pg_attribute a JOIN (
                SELECT unnest(ix.indkey) attnum,
                generate_subscripts(ix.indkey, 1) ord
                FROM pg_index ix
                WHERE ix.indrelid = 13449 AND ix.indisprimary
                ) k ON a.attnum=k.attnum
                WHERE a.attrelid = 13449
                ORDER BY k.ord
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        // TODO should be pg_get_expr instead of format_type
        insta::assert_snapshot!(
            "superset_subquery",
            execute_query(
                "
                SELECT
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    (
                        SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                        FROM pg_catalog.pg_attrdef d
                        WHERE
                            d.adrelid = a.attrelid AND
                            d.adnum = a.attnum AND
                            a.atthasdef
                    ) AS DEFAULT,
                    a.attnotnull,
                    a.attnum,
                    a.attrelid as table_oid,
                    pgd.description as comment,
                    a.attgenerated as generated
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_description pgd ON (
                    pgd.objoid = a.attrelid AND
                    pgd.objsubid = a.attnum
                )
                WHERE
                    a.attrelid = 18000
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                ORDER BY a.attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "superset_visible_query",
            execute_query(
                r#"
                SELECT
                    t.typname as "name",
                    pg_catalog.pg_type_is_visible(t.oid) as "visible",
                    n.nspname as "schema",
                    e.enumlabel as "label"
                FROM pg_catalog.pg_type t
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                LEFT JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
                WHERE t.typtype = 'e'
                ORDER BY
                    "schema",
                    "name",
                    e.oid
                ;
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "superset_attype_query",
            execute_query(
                r#"SELECT
                    t.typname as "name",
                    pg_catalog.format_type(t.typbasetype, t.typtypmod) as "attype",
                    not t.typnotnull as "nullable",
                    t.typdefault as "default",
                    pg_catalog.pg_type_is_visible(t.oid) as "visible",
                    n.nspname as "schema"
                FROM pg_catalog.pg_type t
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typtype = 'd'
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "superset_indkey_varchar_query",
            execute_query(
                r#"SELECT 
                    i.relname as relname, 
                    ix.indisunique, 
                    ix.indexprs, 
                    a.attname, 
                    a.attnum, 
                    c.conrelid, 
                    ix.indkey::varchar, 
                    ix.indoption::varchar, 
                    i.reloptions, 
                    am.amname, 
                    pg_get_expr(ix.indpred, ix.indrelid), 
                    ix.indnkeyatts as indnkeyatts 
                FROM pg_class t 
                    join pg_index ix on t.oid = ix.indrelid 
                    join pg_class i on i.oid = ix.indexrelid 
                    left outer join pg_attribute a on t.oid = a.attrelid and a.attnum = ANY(ix.indkey) 
                    left outer join pg_constraint c on (ix.indrelid = c.conrelid and ix.indexrelid = c.conindid and c.contype in ('p', 'u', 'x')) 
                    left outer join pg_am am on i.relam = am.oid 
                WHERE t.relkind IN ('r', 'v', 'f', 'm', 'p') and t.oid = 18010 and ix.indisprimary = 'f' 
                ORDER BY t.relname, i.relname
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn superset_conname_query() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "superset_conname_query",
            execute_query(
                r#"SELECT r.conname,
                pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
                n.nspname as conschema
                FROM  pg_catalog.pg_constraint r,
                pg_namespace n,
                pg_class c
                WHERE r.conrelid = 13449 AND
                r.contype = 'f' AND
                c.oid = confrelid AND
                n.oid = c.relnamespace
                ORDER BY 1
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // https://github.com/sqlalchemy/sqlalchemy/blob/6104c163eb58e35e46b0bb6a237e824ec1ee1d15/lib/sqlalchemy/dialects/postgresql/base.py
    #[tokio::test]
    async fn sqlalchemy_new_conname_query() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "sqlalchemy_new_conname_query",
            execute_query(
                r#"SELECT
                a.attname,
                pg_catalog.format_type(a.atttypid, a.atttypmod),
                (
                    SELECT
                        pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                    FROM
                        pg_catalog.pg_attrdef AS d
                    WHERE
                        d.adrelid = a.attrelid
                        AND d.adnum = a.attnum
                        AND a.atthasdef
                ) AS DEFAULT,
                a.attnotnull,
                a.attrelid AS table_oid,
                pgd.description AS comment,
                a.attgenerated AS generated,
                (
                    SELECT
                        json_build_object(
                            'always',
                            a.attidentity = 'a',
                            'start',
                            s.seqstart,
                            'increment',
                            s.seqincrement,
                            'minvalue',
                            s.seqmin,
                            'maxvalue',
                            s.seqmax,
                            'cache',
                            s.seqcache,
                            'cycle',
                            s.seqcycle
                        )
                    FROM
                        pg_catalog.pg_sequence AS s
                        JOIN pg_catalog.pg_class AS c ON s.seqrelid = c."oid"
                    WHERE
                        c.relkind = 'S'
                        AND a.attidentity <> ''
                        AND s.seqrelid = CAST(
                            pg_catalog.pg_get_serial_sequence(
                                CAST(CAST(a.attrelid AS REGCLASS) AS TEXT),
                                a.attname
                            ) AS REGCLASS
                        )
                ) AS identity_options
            FROM
                pg_catalog.pg_attribute AS a
                LEFT JOIN pg_catalog.pg_description AS pgd ON (
                    pgd.objoid = a.attrelid
                    AND pgd.objsubid = a.attnum
                )
            WHERE
                a.attrelid = 18000
                AND a.attnum > 0
                AND NOT a.attisdropped
            ORDER BY
                a.attnum"#
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sqlalchemy_regtype() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sqlalchemy_regtype",
            execute_query(
                "SELECT
                    typname AS name,
                    oid,
                    typarray AS array_oid,
                    CAST(CAST(oid AS regtype) AS TEXT) AS regtype,
                    typdelim AS delimiter
                FROM
                    pg_type AS t
                WHERE
                    t.oid = to_regtype('boolean')
                ORDER BY
                    t.oid
                ;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_df_compare_int_with_null() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_compare_int_with_null",
            execute_query(
                "SELECT
                    typname AS name,
                    oid,
                    typarray AS array_oid,
                    CAST(CAST(oid AS regtype) AS TEXT) AS regtype,
                    typdelim AS delimiter
                FROM
                    pg_type AS t
                WHERE
                    t.oid = to_regtype('nonexistent')
                ORDER BY
                    t.oid
                ;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn pgcli_queries() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "pgcli_queries_d",
            execute_query(
                r#"SELECT n.nspname as "Schema",
                    c.relname as "Name",
                    CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
                    pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
                    FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
                    WHERE c.relkind IN ('r','p','v','m','S','f','')
                    AND n.nspname <> 'pg_catalog'
                    AND n.nspname !~ '^pg_toast'
                    AND n.nspname <> 'information_schema'
                    AND pg_catalog.pg_table_is_visible(c.oid)
                "#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_desktop_constraints() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_desktop_constraints",
            execute_query(
                "select	'test'::name as PKTABLE_CAT,
                n2.nspname as PKTABLE_SCHEM,
                c2.relname as PKTABLE_NAME,
                a2.attname as PKCOLUMN_NAME,
                'test'::name as FKTABLE_CAT,
                n1.nspname as FKTABLE_SCHEM,
                c1.relname as FKTABLE_NAME,
                a1.attname as FKCOLUMN_NAME,
                i::int2 as KEY_SEQ,
                case ref.confupdtype
                    when 'c' then 0::int2
                    when 'n' then 2::int2
                    when 'd' then 4::int2
                    when 'r' then 1::int2
                    else 3::int2
                end as UPDATE_RULE,
                case ref.confdeltype
                    when 'c' then 0::int2
                    when 'n' then 2::int2
                    when 'd' then 4::int2
                    when 'r' then 1::int2
                    else 3::int2
                end as DELETE_RULE,
                ref.conname as FK_NAME,
                cn.conname as PK_NAME,
                case
                    when ref.condeferrable then
                        case
                        when ref.condeferred then 5::int2
                        else 6::int2
                        end
                    else 7::int2
                end as DEFERRABLITY
             from
             ((((((( (select cn.oid, conrelid, conkey, confrelid, confkey,
                 generate_series(array_lower(conkey, 1), array_upper(conkey, 1)) as i,
                 confupdtype, confdeltype, conname,
                 condeferrable, condeferred
              from pg_catalog.pg_constraint cn,
                pg_catalog.pg_class c,
                pg_catalog.pg_namespace n
              where contype = 'f'
               and  conrelid = c.oid
               and  relname = 'KibanaSampleDataEcommerce'
               and  n.oid = c.relnamespace
               and  n.nspname = 'public'
             ) ref
             inner join pg_catalog.pg_class c1
              on c1.oid = ref.conrelid)
             inner join pg_catalog.pg_namespace n1
              on  n1.oid = c1.relnamespace)
             inner join pg_catalog.pg_attribute a1
              on  a1.attrelid = c1.oid
              and  a1.attnum = conkey[i])
             inner join pg_catalog.pg_class c2
              on  c2.oid = ref.confrelid)
             inner join pg_catalog.pg_namespace n2
              on  n2.oid = c2.relnamespace)
             inner join pg_catalog.pg_attribute a2
              on  a2.attrelid = c2.oid
              and  a2.attnum = confkey[i])
             left outer join pg_catalog.pg_constraint cn
              on cn.conrelid = ref.confrelid
              and cn.contype = 'p')
              order by ref.oid, ref.i;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_desktop_columns() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_desktop_table_columns",
            execute_query(
                "select
                    n.nspname,
                    c.relname,
                    a.attname,
                    a.atttypid,
                    t.typname,
                    a.attnum,
                    a.attlen,
                    a.atttypmod,
                    a.attnotnull,
                    c.relhasrules,
                    c.relkind,
                    c.oid,
                    pg_get_expr(d.adbin, d.adrelid),
                    case
                        t.typtype
                        when 'd' then t.typbasetype
                        else 0
                    end,
                    t.typtypmod,
                    c.relhasoids
                from
                    (
                        (
                            (
                                pg_catalog.pg_class c
                                inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                                and c.oid = 18000
                            )
                            inner join pg_catalog.pg_attribute a on (not a.attisdropped)
                            and a.attnum > 0
                            and a.attrelid = c.oid
                        )
                        inner join pg_catalog.pg_type t on t.oid = a.atttypid
                    )
                    /* Attention, We have hack for on a.atthasdef */
                    left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum
                order by
                    n.nspname,
                    c.relname,
                    attnum;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_desktop_indexes",
            execute_query(
                "SELECT
                    ta.attname,
                    ia.attnum,
                    ic.relname,
                    n.nspname,
                    tc.relname
                FROM
                    pg_catalog.pg_attribute ta,
                    pg_catalog.pg_attribute ia,
                    pg_catalog.pg_class tc,
                    pg_catalog.pg_index i,
                    pg_catalog.pg_namespace n,
                    pg_catalog.pg_class ic
                WHERE
                    tc.relname = 'KibanaSampleDataEcommerce'
                    AND n.nspname = 'public'
                    AND tc.oid = i.indrelid
                    AND n.oid = tc.relnamespace
                    AND i.indisprimary = 't'
                    AND ia.attrelid = i.indexrelid
                    AND ta.attrelid = i.indrelid
                    AND ta.attnum = i.indkey [ia.attnum-1]
                    AND (NOT ta.attisdropped)
                    AND (NOT ia.attisdropped)
                    AND ic.oid = i.indexrelid
                ORDER BY
                    ia.attnum;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_desktop_pkeys",
            execute_query(
                "SELECT
                    ta.attname,
                    ia.attnum,
                    ic.relname,
                    n.nspname,
                    tc.relname
                FROM
                    pg_catalog.pg_attribute ta,
                    pg_catalog.pg_attribute ia,
                    pg_catalog.pg_class tc,
                    pg_catalog.pg_index i,
                    pg_catalog.pg_namespace n,
                    pg_catalog.pg_class ic
                WHERE
                    tc.relname = 'KibanaSampleDataEcommerce'
                    AND n.nspname = 'public'
                    AND tc.oid = i.indrelid
                    AND n.oid = tc.relnamespace
                    AND i.indisprimary = 't'
                    AND ia.attrelid = i.indexrelid
                    AND ta.attrelid = i.indrelid
                    AND ta.attnum = i.indkey [ia.attnum-1]
                    AND (NOT ta.attisdropped)
                    AND (NOT ia.attisdropped)
                    AND ic.oid = i.indexrelid
                ORDER BY
                    ia.attnum;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_desktop_tables",
            execute_query(
                "select
                    relname,
                    nspname,
                    relkind
                from
                    pg_catalog.pg_class c,
                    pg_catalog.pg_namespace n
                where
                    relkind in ('r', 'v', 'm', 'f')
                    and nspname not in (
                        'pg_catalog',
                        'information_schema',
                        'pg_toast',
                        'pg_temp_1'
                    )
                    and n.oid = relnamespace
                order by
                    nspname,
                    relname"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_get_expr_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_get_expr_query",
            execute_query(
                "SELECT c.oid, a.attnum, a.attname, c.relname, n.nspname, a.attnotnull OR ( t.typtype = 'd' AND t.typnotnull ), a.attidentity != '' OR pg_catalog.Pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval(%'
                FROM   pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n
                    ON ( c.relnamespace = n.oid )
                JOIN pg_catalog.pg_attribute a
                    ON ( c.oid = a.attrelid )
                JOIN pg_catalog.pg_type t
                    ON ( a.atttypid = t.oid )
                LEFT JOIN pg_catalog.pg_attrdef d
                    ON ( d.adrelid = a.attrelid AND d.adnum = a.attnum )
                JOIN (SELECT 2615 AS oid, 2 AS attnum UNION ALL SELECT 1259, 2 UNION ALL SELECT 2609, 4) vals
                ON ( c.oid = vals.oid AND a.attnum = vals.attnum );"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn datagrip_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "datagrip_introspection",
            execute_query(
                "select current_database(), current_schema(), current_user;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn dbeaver_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "dbeaver_introspection_init",
            execute_query(
                "SELECT current_schema(), session_user;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dbeaver_introspection_databases",
            execute_query(
                "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname = 'cubedb'"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dbeaver_introspection_namespaces",
            execute_query(
                "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n
                LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass
                ORDER BY nspname".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dbeaver_introspection_types",
            execute_query(
                "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description
                FROM pg_catalog.pg_type t
                LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem
                LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid
                LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid
                WHERE t.typname IS NOT NULL
                AND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')
                ORDER BY t.oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn postico1_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "postico1_schemas",
            execute_query(
                "SELECT
                    oid,
                    nspname,
                    nspname = ANY (current_schemas(true)) AS is_on_search_path,
                    oid = pg_my_temp_schema() AS is_my_temp_schema,
                    pg_is_other_temp_schema(oid) AS is_other_temp_schema
                FROM pg_namespace"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_regclass_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_regclass_query",
            execute_query(
                "SELECT NULL          AS TABLE_CAT,
                n.nspname     AS TABLE_SCHEM,
                c.relname     AS TABLE_NAME,
                CASE n.nspname ~ '^pg_'
                      OR n.nspname = 'information_schema'
                  WHEN true THEN
                    CASE
                      WHEN n.nspname = 'pg_catalog'
                            OR n.nspname = 'information_schema' THEN
                        CASE c.relkind
                          WHEN 'r' THEN 'SYSTEM TABLE'
                          WHEN 'v' THEN 'SYSTEM VIEW'
                          WHEN 'i' THEN 'SYSTEM INDEX'
                          ELSE NULL
                        end
                      WHEN n.nspname = 'pg_toast' THEN
                        CASE c.relkind
                          WHEN 'r' THEN 'SYSTEM TOAST TABLE'
                          WHEN 'i' THEN 'SYSTEM TOAST INDEX'
                          ELSE NULL
                        end
                      ELSE
                        CASE c.relkind
                          WHEN 'r' THEN 'TEMPORARY TABLE'
                          WHEN 'p' THEN 'TEMPORARY TABLE'
                          WHEN 'i' THEN 'TEMPORARY INDEX'
                          WHEN 'S' THEN 'TEMPORARY SEQUENCE'
                          WHEN 'v' THEN 'TEMPORARY VIEW'
                          ELSE NULL
                        end
                    end
                  WHEN false THEN
                    CASE c.relkind
                      WHEN 'r' THEN 'TABLE'
                      WHEN 'p' THEN 'PARTITIONED TABLE'
                      WHEN 'i' THEN 'INDEX'
                      WHEN 'P' THEN 'PARTITIONED INDEX'
                      WHEN 'S' THEN 'SEQUENCE'
                      WHEN 'v' THEN 'VIEW'
                      WHEN 'c' THEN 'TYPE'
                      WHEN 'f' THEN 'FOREIGN TABLE'
                      WHEN 'm' THEN 'MATERIALIZED VIEW'
                      ELSE NULL
                    end
                  ELSE NULL
                end           AS TABLE_TYPE,
                d.description AS REMARKS,
                ''            AS TYPE_CAT,
                ''            AS TYPE_SCHEM,
                ''            AS TYPE_NAME,
                ''            AS SELF_REFERENCING_COL_NAME,
                ''            AS REF_GENERATION
            FROM   pg_catalog.pg_namespace n,
                pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_description d
                       ON ( c.oid = d.objoid
                            AND d.objsubid = 0
                            AND d.classoid = 'pg_class' :: regclass )
            WHERE  c.relnamespace = n.oid
                AND ( false
                       OR ( c.relkind = 'f' )
                       OR ( c.relkind = 'm' )
                       OR ( c.relkind = 'p'
                            AND n.nspname !~ '^pg_'
                            AND n.nspname <> 'information_schema' )
                       OR ( c.relkind = 'r'
                            AND n.nspname !~ '^pg_'
                            AND n.nspname <> 'information_schema' )
                       OR ( c.relkind = 'v'
                            AND n.nspname <> 'pg_catalog'
                            AND n.nspname <> 'information_schema' ) )
            ORDER BY TABLE_SCHEM ASC, TABLE_NAME ASC
            ;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn powerbi_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "powerbi_supported_types",
            execute_query(
                "/*** Load all supported types ***/
                SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
                CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
                CASE
                  WHEN pg_proc.proname='array_recv' THEN a.typelem
                  WHEN a.typtype='r' THEN rngsubtype
                  ELSE 0
                END AS elemoid,
                CASE
                  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */
                  WHEN a.typtype='r' THEN 2                                        /* Ranges before */
                  WHEN a.typtype='d' THEN 1                                        /* Domains before */
                  ELSE 0                                                           /* Base types first */
                END AS ord
                FROM pg_type AS a
                JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
                JOIN pg_proc ON pg_proc.oid = a.typreceive
                LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
                LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
                LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
                LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)
                WHERE
                  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */
                  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */
                  (pg_proc.proname='array_recv' AND (
                    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */
                    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */
                    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */
                  )) OR
                  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */
                /* changed for stable sort ORDER BY ord */
                ORDER BY a.typname"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_composite_types",
            execute_query(
                "/*** Load field definitions for (free-standing) composite types ***/
                SELECT typ.oid, att.attname, att.atttypid
                FROM pg_type AS typ
                JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
                JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
                JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)
                WHERE
                    (typ.typtype = 'c' AND cls.relkind='c') AND
                attnum > 0 AND     /* Don't load system attributes */
                NOT attisdropped
                ORDER BY typ.oid, att.attnum"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_enums",
            execute_query(
                "/*** Load enum fields ***/
                SELECT pg_type.oid, enumlabel
                FROM pg_enum
                JOIN pg_type ON pg_type.oid=enumtypid
                ORDER BY oid, enumsortorder"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_table_columns",
            execute_query(
                "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE
                from INFORMATION_SCHEMA.columns
                where TABLE_SCHEMA = 'public' and TABLE_NAME = 'KibanaSampleDataEcommerce'
                order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_schemas",
            execute_query(
                "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                from INFORMATION_SCHEMA.tables
                where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')
                order by TABLE_SCHEMA, TABLE_NAME"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_from_subquery",
            execute_query(
                "
                select
                    pkcol.COLUMN_NAME as PK_COLUMN_NAME,
                    fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA,
                    fkcol.TABLE_NAME AS FK_TABLE_NAME,
                    fkcol.COLUMN_NAME as FK_COLUMN_NAME,
                    fkcol.ORDINAL_POSITION as ORDINAL,
                    fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME || '_' || 'users' || '_' || fkcon.CONSTRAINT_NAME as FK_NAME
                from
                    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name
                        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon
                        inner join
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol
                        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA
                        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME
                        inner join
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol
                        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA
                        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME
                where pkcol.TABLE_SCHEMA = 'public' and pkcol.TABLE_NAME = 'users'
                        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION
                order by FK_NAME, fkcol.ORDINAL_POSITION
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_uppercase_alias",
            execute_query(
                "
                select
                    i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME,
                    ii.COLUMN_NAME,
                    ii.ORDINAL_POSITION,
                    case
                        when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y'
                        else 'N'
                    end as PRIMARY_KEY
                from INFORMATION_SCHEMA.table_constraints i
                inner join INFORMATION_SCHEMA.key_column_usage ii on
                    i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and
                    i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME and
                    i.TABLE_SCHEMA = ii.TABLE_SCHEMA and
                    i.TABLE_NAME = ii.TABLE_NAME
                where
                    i.TABLE_SCHEMA = 'public' and
                    i.TABLE_NAME = 'KibanaSampleDataEcommerce' and
                    i.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')
                order by
                    i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME,
                    ii.TABLE_SCHEMA,
                    ii.TABLE_NAME,
                    ii.ORDINAL_POSITION
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_temporary_tables() {
        let create_query = convert_sql_to_cube_query(
            &"
            CREATE LOCAL TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_2_Connect_C\" (
                \"COL\" INTEGER
            ) ON COMMIT PRESERVE ROWS
            ".to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL).await,
        ).await;
        match create_query {
            Err(CompilationError::Unsupported(msg, _)) => assert_eq!(msg, "Unsupported query type: CREATE LOCAL TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_2_Connect_C\" (\"COL\" INT) ON COMMIT PRESERVE ROWS"),
            _ => panic!("CREATE TABLE should throw CompilationError::Unsupported"),
        };

        let select_into_query = convert_sql_to_cube_query(
            &"
            SELECT *
            INTO TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_1_Connect_C\"
            FROM (SELECT 1 AS COL) AS CHECKTEMP
            LIMIT 1
            "
            .to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL).await,
        )
        .await;
        match select_into_query {
            Err(CompilationError::Unsupported(msg, _)) => {
                assert_eq!(msg, "Unsupported query type: SELECT INTO")
            }
            _ => panic!("SELECT INTO should throw CompilationError::unsupported"),
        }
    }

    // This tests asserts that our DF fork contains support for IS TRUE|FALSE
    #[tokio::test]
    async fn df_is_boolean() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_is_boolean",
            execute_query(
                "SELECT r.v, r.v IS TRUE as is_true, r.v IS FALSE as is_false
                 FROM (SELECT true as v UNION ALL SELECT false as v) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn df_cast_date32_additional_formats() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_cast_date32_additional_formats",
            execute_query(
                "SELECT CAST('20220101' as DATE) as no_dim, CAST('2022/02/02' as DATE) as slash_dim,  CAST('2022|03|03' as DATE) as pipe_dim;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for Coalesce
    #[tokio::test]
    async fn df_coalesce() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_coalesce",
            execute_query(
                "SELECT COALESCE(null, 1) as t1, COALESCE(null, 1, null, 2) as t2".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for nullif(scalar,scalar)
    #[tokio::test]
    async fn df_nullif() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_nullif",
            execute_query(
                "SELECT nullif('test1', 'test1') as str_null, nullif('test1', 'test2') as str_first, nullif(3.0, 3.0) as float_null, nullif(3.0, 1.0) as float_first".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork works correct with types
    #[tokio::test]
    async fn df_switch_case_coerc() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_case_fixes",
            execute_query(
                "SELECT
                    CASE 'test' WHEN 'int4' THEN NULL ELSE 100 END as null_in_then,
                    CASE true WHEN 'false' THEN 'yes' ELSE 'no' END as bool_utf8_cast,
                    CASE true WHEN 'false' THEN 'yes' WHEN 'true' THEN true ELSE 'no' END as then_diff_types
                ".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for >> && <<
    #[tokio::test]
    async fn df_is_bitwise_shit() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_bitwise_shit",
            execute_query(
                "SELECT 2 << 10 as t1, 2048 >> 10 as t2;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for escaped single quoted strings
    #[tokio::test]
    async fn df_escaped_strings() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_escaped_strings",
            execute_query(
                "SELECT 'test' LIKE e'%' as v1, 'payment_p2020_01' LIKE E'payment\\_p2020\\_01' as v2;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for string-boolean coercion and cast
    #[tokio::test]
    async fn db_string_boolean_comparison() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_string_boolean_comparison",
            execute_query(
                "SELECT TRUE = 't' t, FALSE <> 'f' f;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_truetyp() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_truetypid_truetypmod",
            execute_query(
                "
                SELECT
                    a.attrelid,
                    a.attname,
                    t.typname,
                    information_schema._pg_truetypid(a.*, t.*) typid,
                    information_schema._pg_truetypmod(a.*, t.*) typmod,
                    information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(a.*, t.*),
                        information_schema._pg_truetypmod(a.*, t.*)
                    ) as_arg
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                ORDER BY a.attrelid ASC, a.attnum ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_to_char_udf() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "to_char_1",
            execute_query(
                "SELECT to_char(x, 'YYYY-MM-DD HH24:MI:SS.MS TZ') FROM (SELECT Str_to_date('2021-08-31 11:05:10.400000', '%Y-%m-%d %H:%i:%s.%f') x) e".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "to_char_2",
            execute_query(
                "
                SELECT to_char(x, 'YYYY-MM-DD HH24:MI:SS.MS TZ')
                FROM  (
                        SELECT Str_to_date('2021-08-31 11:05:10.400000', '%Y-%m-%d %H:%i:%s.%f') x
                    UNION ALL
                        SELECT str_to_date('2021-08-31 11:05', '%Y-%m-%d %H:%i') x
                ) e
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_regexp_substr_udf() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "regexp_substr",
            execute_query(
                "SELECT
                    regexp_substr('test@test.com', '@[^.]*') as match_dot,
                    regexp_substr('12345', '[0-9]+') as match_number,
                    regexp_substr('12345', '[0-9]+', 2) as match_number_pos_2,
                    regexp_substr(null, '@[^.]*') as source_null,
                    regexp_substr('test@test.com', null) as pattern_null,
                    regexp_substr('test@test.com', '@[^.]*', 1) as position_default,
                    regexp_substr('test@test.com', '@[^.]*', 5) as position_no_skip,
                    regexp_substr('test@test.com', '@[^.]*', 6) as position_skip,
                    regexp_substr('test@test.com', '@[^.]*', 0) as position_zero,
                    regexp_substr('test@test.com', '@[^.]*', -1) as position_negative,
                    regexp_substr('test@test.com', '@[^.]*', 100) as position_more_then_input
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "regexp_substr_column",
            execute_query(
                "SELECT r.a as input, regexp_substr(r.a, '@[^.]*') as result FROM (
                    SELECT 'test@test.com' as a
                    UNION ALL
                    SELECT 'test'
                ) as r
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_to_char_query() -> Result<(), CubeError> {
        execute_query(
            "select to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS.MS TZ')".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_table_exists() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "metabase_table_exists",
            execute_query(
                r#"SELECT TRUE AS "_" FROM "public"."KibanaSampleDataEcommerce" WHERE 1 <> 1 LIMIT 0;"#
                    .to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_current_setting() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "current_setting",
            execute_query(
                "SELECT current_setting('max_index_keys'), current_setting('search_path')"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quote_ident() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quote_ident",
            execute_query(
                "SELECT quote_ident('pg_catalog') i1, quote_ident('Foo bar') i2".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_subquery_current_schema() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "microstrategy_subquery_current_schema",
            execute_query(
                "SELECT t.oid FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n ON t.typnamespace = n.oid WHERE t.typname = 'citext' AND (n.nspname = (SELECT current_schema()) OR n.nspname = 'public')".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_insubquery_where_tables() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "grafana_insubquery_where_tables",
            execute_query(
                r#"SELECT quote_ident(table_name) AS "table" FROM information_schema.tables WHERE quote_ident(table_schema) NOT IN ('information_schema', 'pg_catalog', '_timescaledb_cache', '_timescaledb_catalog', '_timescaledb_internal', '_timescaledb_config', 'timescaledb_information', 'timescaledb_experimental') AND table_type = 'BASE TABLE' AND quote_ident(table_schema) IN (SELECT CASE WHEN TRIM(s[i]) = '"$user"' THEN user ELSE TRIM(s[i]) END FROM generate_series(array_lower(string_to_array(current_setting('search_path'), ','), 1), array_upper(string_to_array(current_setting('search_path'), ','), 1)) AS i, string_to_array(current_setting('search_path'), ',') AS s)"#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_insubquery_where_tables_spacing() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "grafana_insubquery_where_tables_spacing",
            execute_query(
                "select quote_ident(table_name) as \"table\" from information_schema.tables\
            \n    where quote_ident(table_schema) not in ('information_schema',\
            \n                             'pg_catalog',\
            \n                             '_timescaledb_cache',\
            \n                             '_timescaledb_catalog',\
            \n                             '_timescaledb_internal',\
            \n                             '_timescaledb_config',\
            \n                             'timescaledb_information',\
            \n                             'timescaledb_experimental')\
            \n      and \
            \n          quote_ident(table_schema) IN (\
            \n          SELECT\
            \n            CASE WHEN trim(s[i]) = '\"$user\"' THEN user ELSE trim(s[i]) END\
            \n          FROM\
            \n            generate_series(\
            \n              array_lower(string_to_array(current_setting('search_path'),','),1),\
            \n              array_upper(string_to_array(current_setting('search_path'),','),1)\
            \n            ) as i,\
            \n            string_to_array(current_setting('search_path'),',') s\
            \n          )"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_grafana_pg_version_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "grafana_pg_version_introspection",
            execute_query(
                "SELECT current_setting('server_version_num')::int/100 as version".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_substring() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT
                    \"source\".\"substring1\" AS \"substring2\",
                    \"source\".\"count\" AS \"count\"
                FROM (
                    SELECT
                        \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",
                        SUBSTRING(\"KibanaSampleDataEcommerce\".\"customer_gender\" FROM 1 FOR 1234) AS \"substring1\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                ) AS \"source\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_skyvia_reaggregate_date_part() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT EXTRACT(MONTH FROM t."order_date") AS expr1
            FROM public."KibanaSampleDataEcommerce" AS t
            ORDER BY expr1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_doy() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                \"source\".\"order_date\" AS \"order_date\",
                \"source\".\"count\" AS \"count\"
            FROM
                (
                    SELECT
                        (
                            CAST(
                                extract(
                                    doy
                                    from
                                        \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                                ) AS integer
                            )
                        ) AS \"order_date\",
                        count(*) AS \"count\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                    GROUP BY CAST(
                        extract(
                            doy
                            from
                                \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                        ) AS integer
                    )
                    ORDER BY CAST(
                        extract(
                            doy
                            from
                                \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                        ) AS integer
                    ) ASC
                ) \"source\"
            WHERE
                \"source\".\"count\" IS NOT NULL
            ORDER BY
                \"source\".\"count\" ASC
            LIMIT
                100"
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_binary_expr_projection_split() -> Result<(), CubeError> {
        let operators = ["+", "-", "*", "/"];

        for operator in operators {
            let query_plan = convert_select_to_query_plan(
                format!("SELECT
                    (
                        CAST(
                             \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS integer
                        ) {} 100
                    ) AS \"taxful_total_price\"
                FROM
                    \"public\".\"KibanaSampleDataEcommerce\"", operator),
                DatabaseProtocol::PostgreSQL,
            )
                .await;

            let logical_plan = query_plan.as_logical_plan();
            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                    ]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                }
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_dow() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                \"source\".\"order_date\" AS \"order_date\",
                \"source\".\"count\" AS \"count\"
            FROM
                (
                    SELECT
                        (
                            CAST(
                                extract(
                                    dow
                                    from
                                        \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                                ) AS integer
                            ) + 1
                        ) AS \"order_date\",
                        count(*) AS \"count\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                    GROUP BY (
                        CAST(
                            extract(
                                dow
                                from
                                    \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                            ) AS integer
                        ) + 1
                    )
                    ORDER BY (
                        CAST(
                            extract(
                                dow
                                from
                                    \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                            ) AS integer
                        ) + 1
                    ) ASC
                ) \"source\"
            WHERE
                \"source\".\"count\" IS NOT NULL
            ORDER BY
                \"source\".\"count\" ASC
            LIMIT
                100"
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_subquery_with_same_name_excel() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "subquery_with_same_name_excel",
            execute_query(
                "SELECT oid, (SELECT oid FROM pg_type WHERE typname like 'geography') as dd FROM pg_type WHERE typname like 'geometry'".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_join_where_and_or() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "join_where_and_or",
            execute_query(
                "
                SELECT
                    att.attname,
                    att.attnum,
                    cl.oid
                FROM pg_attribute att
                JOIN pg_class cl ON
                    cl.oid = attrelid AND (
                        cl.relkind = 's' OR
                        cl.relkind = 'r'
                    )
                ORDER BY
                    cl.oid ASC,
                    att.attnum ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_type_any_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_type_any",
            execute_query(
                "SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n
                ON t.typnamespace = n.oid WHERE t.oid = 25;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_regproc_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_regproc_query",
            execute_query(
                "SELECT typinput='array_in'::regproc as is_array, typtype, typname, pg_type.oid
                FROM pg_catalog.pg_type
                LEFT JOIN (
                    select
                        ns.oid as nspoid,
                        ns.nspname,
                        r.r
                    from pg_namespace as ns
                    join (
                        select
                            s.r,
                            (current_schemas(false))[s.r] as nspname
                        from generate_series(1, array_upper(current_schemas(false), 1)) as s(r)
                    ) as r
                    using ( nspname )
                ) as sp
                ON sp.nspoid = typnamespace
                /* I've changed oid = to oid IN to verify is_array column */
                WHERE pg_type.oid IN (25, 1016)
                ORDER BY sp.r, pg_type.oid DESC;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_namespace_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_namespace",
            execute_query(
                "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG
                FROM pg_catalog.pg_namespace
                WHERE nspname <> 'pg_toast'
                AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1])
                AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))
                ORDER BY TABLE_SCHEM;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_class_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_class_query",
            execute_query(
                "
                SELECT *
                    FROM (
                        SELECT  n.nspname,
                                c.relname,
                                a.attname,
                                a.atttypid,
                                a.attnotnull or (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
                                a.atttypmod,
                                a.attlen,
                                t.typtypmod,
                                row_number() OVER (partition BY a.attrelid ORDER BY a.attnum) AS attnum,
                                NULLIF(a.attidentity, '') AS attidentity,
                                pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
                                dsc.description,
                                t.typbasetype,
                                t.typtype
                            FROM pg_catalog.pg_namespace n
                            JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
                            JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
                            JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
                            LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
                            LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
                            LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
                            LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
                        WHERE c.relkind IN ('r', 'p', 'v', 'f', 'm') AND a.attnum > 0 AND NOT a.attisdropped AND n.nspname LIKE 'public' AND c.relname LIKE 'KibanaSampleDataEcommerce') c
                WHERE true
                ORDER BY nspname, c.relname, attnum;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_table_cat_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_table_cat_query",
            execute_query(
                "
                SELECT  result.table_cat,
                        result.table_schem,
                        result.table_name,
                        result.column_name,
                        result.key_seq,
                        result.pk_name
                    FROM (
                        SELECT  NULL AS table_cat,
                                n.nspname AS table_schem,
                                ct.relname AS table_name,
                                a.attname AS column_name,
                                (information_schema._pg_expandarray(i.indkey)).n as key_seq,
                                ci.relname AS pk_name,
                                information_schema._pg_expandarray(i.indkey) AS keys,
                                a.attnum AS a_attnum
                            FROM   pg_catalog.pg_class ct
                            JOIN   pg_catalog.pg_attribute a ON(ct.oid = a.attrelid)
                            JOIN   pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                            JOIN   pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
                            JOIN   pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                        WHERE true AND ct.relname = 'actor' AND i.indisprimary) result
                WHERE result.a_attnum = (result.keys).x
                ORDER BY result.table_name, result.pk_name, result.key_seq;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pktable_cat_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pktable_cat_query",
            execute_query(
                "
                SELECT  NULL::text  AS pktable_cat,
                        pkn.nspname AS pktable_schem,
                        pkc.relname AS pktable_name,
                        pka.attname AS pkcolumn_name,
                        NULL::text  AS fktable_cat,
                        fkn.nspname AS fktable_schem,
                        fkc.relname AS fktable_name,
                        fka.attname AS fkcolumn_name,
                        pos.n       AS key_seq,
                        CASE con.confupdtype
                            WHEN 'c' THEN 0
                            WHEN 'n' THEN 2
                            WHEN 'd' THEN 4
                            WHEN 'r' THEN 1
                            WHEN 'p' THEN 1
                            WHEN 'a' THEN 3
                            ELSE NULL
                        END AS update_rule,
                        CASE con.confdeltype
                            WHEN 'c' THEN 0
                            WHEN 'n' THEN 2
                            WHEN 'd' THEN 4
                            WHEN 'r' THEN 1
                            WHEN 'p' THEN 1
                            WHEN 'a' THEN 3
                            ELSE NULL
                        END AS delete_rule,
                        con.conname  AS fk_name,
                        pkic.relname AS pk_name,
                        CASE
                            WHEN con.condeferrable AND con.condeferred THEN 5
                            WHEN con.condeferrable THEN 6
                            ELSE 7
                        END AS deferrability
                    FROM    pg_catalog.pg_namespace pkn,
                            pg_catalog.pg_class pkc,
                            pg_catalog.pg_attribute pka,
                            pg_catalog.pg_namespace fkn,
                            pg_catalog.pg_class fkc,
                            pg_catalog.pg_attribute fka,
                            pg_catalog.pg_constraint con,
                            pg_catalog.generate_series(1, 32) pos(n),
                            pg_catalog.pg_class pkic
                WHERE   pkn.oid = pkc.relnamespace
                AND     pkc.oid = pka.attrelid
                AND     pka.attnum = con.confkey[pos.n]
                AND     con.confrelid = pkc.oid
                AND     fkn.oid = fkc.relnamespace
                AND     fkc.oid = fka.attrelid
                AND     fka.attnum = con.conkey[pos.n]
                AND     con.conrelid = fkc.oid
                AND     con.contype = 'f'
                AND     (pkic.relkind = 'i' OR pkic.relkind = 'I')
                AND     pkic.oid = con.conindid
                AND     fkn.nspname = 'public'
                AND     fkc.relname = 'actor'
                ORDER BY pkn.nspname, pkc.relname, con.conname, pos.n;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_type_in_subquery_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_type_in_subquery_query",
            execute_query(
                "
                SELECT nspname, typname 
                FROM pg_type t 
                JOIN pg_namespace n ON n.oid = t.typnamespace 
                WHERE t.oid IN (SELECT DISTINCT enumtypid FROM pg_enum e);
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_computing_ilike_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sigma_computing_ilike_query",
            execute_query(
                "
                select distinct table_schema
                from information_schema.tables
                where
                    table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN', 'FOREIGN TABLE') and
                    table_schema NOT IN ('pg_catalog', 'information_schema') and
                    table_schema ilike '%'
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_computing_pg_matviews_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sigma_computing_pg_matviews_query",
            execute_query(
                "
                SELECT table_name FROM (
                    select table_name
                    from information_schema.tables
                    where
                        table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN', 'FOREIGN TABLE') and
                        table_schema = 'public'
                    UNION
                    select matviewname as table_name
                    from pg_catalog.pg_matviews
                    where schemaname = 'public'
                ) t
                ORDER BY table_name ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_computing_array_subquery_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sigma_computing_array_subquery_query",
            execute_query(
                r#"
                select
                    cl.relname as "source_table",
                    array(
                        select (
                            select attname::text
                            from pg_attribute
                            where
                                attrelid = con.conrelid and
                                attnum = con.conkey[i]
                        )
                        from generate_series(array_lower(con.conkey, 1), array_upper(con.conkey, 1)) i
                    ) as "source_keys",
                    (
                        select nspname
                        from pg_namespace ns2
                        join pg_class cl2 on ns2.oid = cl2.relnamespace
                        where cl2.oid = con.confrelid
                    ) as "target_schema",
                    (
                        select relname
                        from pg_class
                        where oid = con.confrelid
                    ) as "target_table",
                    array(
                        select (
                            select attname::text
                            from pg_attribute
                            where
                                attrelid = con.confrelid and
                                attnum = con.confkey[i]
                        )
                        from generate_series(array_lower(con.confkey, 1), array_upper(con.confkey, 1)) i
                    ) as "target_keys"
                from pg_class cl
                join pg_namespace ns on cl.relnamespace = ns.oid
                join pg_constraint con on con.conrelid = cl.oid
                where
                    ns.nspname = 'public' and
                    cl.relname >= 'A' and
                    cl.relname <= 'z' and
                    con.contype = 'f'
                order by
                    "source_table",
                    con.conname
                ;
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_computing_with_subquery_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sigma_computing_with_subquery_query",
            execute_query(
                "
                with
                    nsp as (
                        select oid
                        from pg_catalog.pg_namespace
                        where nspname = 'public'
                    ),
                    tbl as (
                        select oid
                        from pg_catalog.pg_class
                        where
                            relname = 'KibanaSampleDataEcommerce' and
                            relnamespace = (select oid from nsp)
                    )
                select
                    attname,
                    typname,
                    description
                from pg_attribute a
                join pg_type on atttypid = pg_type.oid
                left join pg_description on
                    attrelid = objoid and
                    attnum = objsubid
                where
                    attnum > 0 and
                    attrelid = (select oid from tbl)
                order by attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_google_sheets_pg_database_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "google_sheets_pg_database_query",
            execute_query(
                "
                SELECT
                    cl.relname as Table,
                    att.attname AS Name,
                    att.attnum as Position,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'true'
                        ELSE 'false'
                    END as Nullable,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c
                            where
                                c.conrelid = cl.oid and
                                c.contype = 'p' and
                                att.attnum = ANY (c.conkey)
                        ) THEN true
                        ELSE false
                    END as IsKey,
                    CASE
                        WHEN cs.relname IS NULL THEN 'false'
                        ELSE 'true'
                    END as IsAutoIncrement,
                    CASE
                        WHEN ty.typname = 'bpchar' THEN 'char'
                        WHEN ty.typname = '_bpchar' THEN '_char'
                        ELSE ty.typname
                    END as TypeName,
                    CASE
                        WHEN
                            ty.typname Like 'bit' OR
                            ty.typname Like 'varbit' and
                            att.atttypmod > 0
                        THEN att.atttypmod
                        WHEN
                            ty.typname Like 'interval' OR
                            ty.typname Like 'timestamp' OR
                            ty.typname Like 'timestamptz' OR
                            ty.typname Like 'time' OR
                            ty.typname Like 'timetz' THEN -1
                        WHEN att.atttypmod > 0 THEN att.atttypmod - 4
                        ELSE att.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS DatetimeLength
                FROM pg_attribute att
                JOIN pg_type ty ON ty.oid = atttypid
                JOIN pg_namespace tn ON tn.oid = ty.typnamespace
                JOIN pg_class cl ON
                    cl.oid = attrelid AND
                    (
                        (cl.relkind = 'r') OR
                        (cl.relkind = 's') OR
                        (cl.relkind = 'v') OR
                        (cl.relkind = 'm') OR
                        (cl.relkind = 'f')
                    )
                JOIN pg_namespace na ON na.oid = cl.relnamespace
                LEFT OUTER JOIN (
                    pg_depend
                    JOIN pg_class cs ON
                        objid = cs.oid AND
                        cs.relkind = 'S' AND
                        classid = 'pg_class'::regclass::oid
                ) ON
                    refobjid = attrelid AND
                    refobjsubid = attnum
                LEFT JOIN pg_database db ON db.datname = current_database()
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    na.nspname = 'public' AND
                    cl.relname = 'KibanaSampleDataEcommerce'
                ORDER BY attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_has_schema_privilege_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_has_schema_privilege_query",
            execute_query(
                "
                SELECT nspname AS schema_name
                FROM pg_namespace
                WHERE
                    (
                        has_schema_privilege('ovr', nspname, 'USAGE') = TRUE OR
                        has_schema_privilege('ovr', nspname, 'CREATE') = TRUE
                    ) AND
                    nspname NOT IN ('pg_catalog', 'information_schema') AND
                    nspname NOT LIKE 'pg_toast%' AND
                    nspname NOT LIKE 'pg_temp_%'
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_pktable_cat_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_pktable_cat_query",
            execute_query(
                "
                SELECT
                    NULL::text AS PKTABLE_CAT,
                    pkn.nspname AS PKTABLE_SCHEM,
                    pkc.relname AS PKTABLE_NAME,
                    pka.attname AS PKCOLUMN_NAME,
                    NULL::text AS FKTABLE_CAT,
                    fkn.nspname AS FKTABLE_SCHEM,
                    fkc.relname AS FKTABLE_NAME,
                    fka.attname AS FKCOLUMN_NAME,
                    pos.n AS KEY_SEQ,
                    CASE con.confupdtype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS UPDATE_RULE,
                    CASE con.confdeltype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS DELETE_RULE,
                    con.conname AS FK_NAME,
                    pkic.relname AS PK_NAME,
                    CASE
                        WHEN con.condeferrable AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                    END AS DEFERRABILITY
                FROM
                    pg_catalog.pg_namespace pkn,
                    pg_catalog.pg_class pkc,
                    pg_catalog.pg_attribute pka,
                    pg_catalog.pg_namespace fkn,
                    pg_catalog.pg_class fkc,
                    pg_catalog.pg_attribute fka,
                    pg_catalog.pg_constraint con,
                    pg_catalog.generate_series(1, 32) pos(n),
                    pg_catalog.pg_depend dep,
                    pg_catalog.pg_class pkic
                WHERE
                    pkn.oid = pkc.relnamespace AND
                    pkc.oid = pka.attrelid AND
                    pka.attnum = con.confkey[pos.n] AND
                    con.confrelid = pkc.oid AND
                    fkn.oid = fkc.relnamespace AND
                    fkc.oid = fka.attrelid AND
                    fka.attnum = con.conkey[pos.n] AND
                    con.conrelid = fkc.oid AND
                    con.contype = 'f' AND
                    con.oid = dep.objid AND
                    pkic.oid = dep.refobjid AND
                    pkic.relkind = 'i' AND
                    dep.classid = 'pg_constraint'::regclass::oid AND
                    dep.refclassid = 'pg_class'::regclass::oid AND
                    fkn.nspname = 'public' AND
                    fkc.relname = 'TABLENAME'
                ORDER BY
                    pkn.nspname,
                    pkc.relname,
                    con.conname,
                    pos.n
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_table_cat_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_table_cat_query",
            execute_query(
                "
                SELECT
                    NULL AS TABLE_CAT,
                    n.nspname AS TABLE_SCHEM,
                    ct.relname AS TABLE_NAME,
                    a.attname AS COLUMN_NAME,
                    (i.keys).n AS KEY_SEQ,
                    ci.relname AS PK_NAME
                FROM pg_catalog.pg_class ct
                JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
                JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                JOIN (
                    SELECT
                        i.indexrelid,
                        i.indrelid,
                        i.indisprimary,
                        information_schema._pg_expandarray(i.indkey) AS keys
                    FROM pg_catalog.pg_index i
                ) i ON (
                    a.attnum = (i.keys).x AND
                    a.attrelid = i.indrelid
                )
                JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                WHERE
                    true AND
                    n.nspname = 'public' AND
                    ct.relname = 'KibanaSampleDataEcommerce' AND
                    i.indisprimary
                ORDER BY
                    table_name,
                    pk_name,
                    key_seq
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thoughtspot_dateadd_literal_date32() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "thoughtspot_dateadd_literal_date32",
            execute_query(
                "
                SELECT 
                    DATE_TRUNC('month', DATEADD(day, CAST(50 AS int), DATE '2014-01-01')) \"ca_1\", 
                    CASE
                        WHEN sum(3) IS NOT NULL THEN sum(3)
                        ELSE 0
                    END \"ca_2\"
                ORDER BY \"ca_2\" ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thoughtspot_table_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "thoughtspot_table_introspection",
            execute_query(
                r#"
                SELECT *
                FROM (
                    SELECT
                        current_database() AS TABLE_CAT,
                        n.nspname AS TABLE_SCHEM,
                        c.relname AS TABLE_NAME,
                        a.attname AS COLUMN_NAME,
                        CAST(
                            CASE typname
                                WHEN 'text' THEN 12
                                WHEN 'bit' THEN - 7
                                WHEN 'bool' THEN - 7
                                WHEN 'boolean' THEN - 7
                                WHEN 'varchar' THEN 12
                                WHEN 'character varying' THEN 12
                                WHEN 'char' THEN 1
                                WHEN '"char"' THEN 1
                                WHEN 'character' THEN 1
                                WHEN 'nchar' THEN 12
                                WHEN 'bpchar' THEN 1
                                WHEN 'nvarchar' THEN 12
                                WHEN 'date' THEN 91
                                WHEN 'time' THEN 92
                                WHEN 'time without time zone' THEN 92
                                WHEN 'timetz' THEN 2013
                                WHEN 'time with time zone' THEN 2013
                                WHEN 'timestamp' THEN 93
                                WHEN 'timestamp without time zone' THEN 93
                                WHEN 'timestamptz' THEN 2014
                                WHEN 'timestamp with time zone' THEN 2014
                                WHEN 'smallint' THEN 5
                                WHEN 'int2' THEN 5
                                WHEN 'integer' THEN 4
                                WHEN 'int' THEN 4
                                WHEN 'int4' THEN 4
                                WHEN 'bigint' THEN - 5
                                WHEN 'int8' THEN - 5
                                WHEN 'decimal' THEN 3
                                WHEN 'real' THEN 7
                                WHEN 'float4' THEN 7
                                WHEN 'double precision' THEN 8
                                WHEN 'float8' THEN 8
                                WHEN 'float' THEN 6
                                WHEN 'numeric' THEN 2
                                WHEN '_float4' THEN 2003
                                WHEN '_aclitem' THEN 2003
                                WHEN '_text' THEN 2003
                                WHEN 'bytea' THEN - 2
                                WHEN 'oid' THEN - 5
                                WHEN 'name' THEN 12
                                WHEN '_int4' THEN 2003
                                WHEN '_int2' THEN 2003
                                WHEN 'ARRAY' THEN 2003
                                WHEN 'geometry' THEN - 4
                                WHEN 'super' THEN - 16
                                WHEN 'varbyte' THEN - 4
                                WHEN 'geography' THEN - 4
                                ELSE 1111
                            END
                            AS SMALLINT
                        ) AS DATA_TYPE,
                        t.typname AS TYPE_NAME,
                        CASE typname
                            WHEN 'int4' THEN 10
                            WHEN 'bit' THEN 1
                            WHEN 'bool' THEN 1
                            WHEN 'varchar' THEN atttypmod - 4
                            WHEN 'character varying' THEN atttypmod - 4
                            WHEN 'char' THEN atttypmod - 4
                            WHEN 'character' THEN atttypmod - 4
                            WHEN 'nchar' THEN atttypmod - 4
                            WHEN 'bpchar' THEN atttypmod - 4
                            WHEN 'nvarchar' THEN atttypmod - 4
                            WHEN 'date' THEN 13
                            WHEN 'time' THEN 15
                            WHEN 'time without time zone' THEN 15
                            WHEN 'timetz' THEN 21
                            WHEN 'time with time zone' THEN 21
                            WHEN 'timestamp' THEN 29
                            WHEN 'timestamp without time zone' THEN 29
                            WHEN 'timestamptz' THEN 35
                            WHEN 'timestamp with time zone' THEN 35
                            WHEN 'smallint' THEN 5
                            WHEN 'int2' THEN 5
                            WHEN 'integer' THEN 10
                            WHEN 'int' THEN 10
                            WHEN 'int4' THEN 10
                            WHEN 'bigint' THEN 19
                            WHEN 'int8' THEN 19
                            WHEN 'decimal' THEN (atttypmod - 4) >> 16
                            WHEN 'real' THEN 8
                            WHEN 'float4' THEN 8
                            WHEN 'double precision' THEN 17
                            WHEN 'float8' THEN 17
                            WHEN 'float' THEN 17
                            WHEN 'numeric' THEN (atttypmod - 4) >> 16
                            WHEN '_float4' THEN 8
                            WHEN 'oid' THEN 10
                            WHEN '_int4' THEN 10
                            WHEN '_int2' THEN 5
                            WHEN 'geometry' THEN NULL
                            WHEN 'super' THEN NULL
                            WHEN 'varbyte' THEN NULL
                            WHEN 'geography' THEN NULL
                            ELSE 2147483647
                        END AS COLUMN_SIZE,
                        NULL AS BUFFER_LENGTH,
                        CASE typname
                            WHEN 'float4' THEN 8
                            WHEN 'float8' THEN 17
                            WHEN 'numeric' THEN (atttypmod - 4) & 65535
                            WHEN 'time without time zone' THEN 6
                            WHEN 'timetz' THEN 6
                            WHEN 'time with time zone' THEN 6
                            WHEN 'timestamp without time zone' THEN 6
                            WHEN 'timestamp' THEN 6
                            WHEN 'geometry' THEN NULL
                            WHEN 'super' THEN NULL
                            WHEN 'varbyte' THEN NULL
                            WHEN 'geography' THEN NULL
                            ELSE 0
                        END AS DECIMAL_DIGITS,
                        CASE typname
                            WHEN 'varbyte' THEN 2
                            WHEN 'geography' THEN 2
                            ELSE 10
                        END AS NUM_PREC_RADIX,
                        CASE a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)
                            WHEN 'false' THEN 1
                            WHEN NULL THEN 2
                            ELSE 0
                        END AS NULLABLE,
                        dsc.description AS REMARKS,
                        pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS COLUMN_DEF,
                        CAST(
                            CASE typname
                                WHEN 'text' THEN 12
                                WHEN 'bit' THEN - 7
                                WHEN 'bool' THEN - 7
                                WHEN 'boolean' THEN - 7
                                WHEN 'varchar' THEN 12
                                WHEN 'character varying' THEN 12
                                WHEN '"char"' THEN 1
                                WHEN 'char' THEN 1
                                WHEN 'character' THEN 1
                                WHEN 'nchar' THEN 1
                                WHEN 'bpchar' THEN 1
                                WHEN 'nvarchar' THEN 12
                                WHEN 'date' THEN 91
                                WHEN 'time' THEN 92
                                WHEN 'time without time zone' THEN 92
                                WHEN 'timetz' THEN 2013
                                WHEN 'time with time zone' THEN 2013
                                WHEN 'timestamp with time zone' THEN 2014
                                WHEN 'timestamp' THEN 93
                                WHEN 'timestamp without time zone' THEN 93
                                WHEN 'smallint' THEN 5
                                WHEN 'int2' THEN 5
                                WHEN 'integer' THEN 4
                                WHEN 'int' THEN 4
                                WHEN 'int4' THEN 4
                                WHEN 'bigint' THEN - 5
                                WHEN 'int8' THEN - 5
                                WHEN 'decimal' THEN 3
                                WHEN 'real' THEN 7
                                WHEN 'float4' THEN 7
                                WHEN 'double precision' THEN 8
                                WHEN 'float8' THEN 8
                                WHEN 'float' THEN 6
                                WHEN 'numeric' THEN 2
                                WHEN '_float4' THEN 2003
                                WHEN 'timestamptz' THEN 2014
                                WHEN 'timestamp with time zone' THEN 2014
                                WHEN '_aclitem' THEN 2003
                                WHEN '_text' THEN 2003
                                WHEN 'bytea' THEN - 2
                                WHEN 'oid' THEN - 5
                                WHEN 'name' THEN 12
                                WHEN '_int4' THEN 2003
                                WHEN '_int2' THEN 2003
                                WHEN 'ARRAY' THEN 2003
                                WHEN 'geometry' THEN - 4
                                WHEN 'super' THEN - 16
                                WHEN 'varbyte' THEN - 4
                                WHEN 'geography' THEN - 4 ELSE 1111
                            END
                            AS SMALLINT
                        ) AS SQL_DATA_TYPE,
                        CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB,
                        CASE typname
                            WHEN 'int4' THEN 10
                            WHEN 'bit' THEN 1
                            WHEN 'bool' THEN 1
                            WHEN 'varchar' THEN atttypmod - 4
                            WHEN 'character varying' THEN atttypmod - 4
                            WHEN 'char' THEN atttypmod - 4
                            WHEN 'character' THEN atttypmod - 4
                            WHEN 'nchar' THEN atttypmod - 4
                            WHEN 'bpchar' THEN atttypmod - 4
                            WHEN 'nvarchar' THEN atttypmod - 4
                            WHEN 'date' THEN 13
                            WHEN 'time' THEN 15
                            WHEN 'time without time zone' THEN 15
                            WHEN 'timetz' THEN 21
                            WHEN 'time with time zone' THEN 21
                            WHEN 'timestamp' THEN 29
                            WHEN 'timestamp without time zone' THEN 29
                            WHEN 'timestamptz' THEN 35
                            WHEN 'timestamp with time zone' THEN 35
                            WHEN 'smallint' THEN 5
                            WHEN 'int2' THEN 5
                            WHEN 'integer' THEN 10
                            WHEN 'int' THEN 10
                            WHEN 'int4' THEN 10
                            WHEN 'bigint' THEN 19
                            WHEN 'int8' THEN 19
                            WHEN 'decimal' THEN ((atttypmod - 4) >> 16) & 65535
                            WHEN 'real' THEN 8
                            WHEN 'float4' THEN 8
                            WHEN 'double precision' THEN 17
                            WHEN 'float8' THEN 17
                            WHEN 'float' THEN 17
                            WHEN 'numeric' THEN ((atttypmod - 4) >> 16) & 65535
                            WHEN '_float4' THEN 8
                            WHEN 'oid' THEN 10
                            WHEN '_int4' THEN 10
                            WHEN '_int2' THEN 5
                            WHEN 'geometry' THEN NULL
                            WHEN 'super' THEN NULL
                            WHEN 'varbyte' THEN NULL
                            WHEN 'geography' THEN NULL
                            ELSE 2147483647
                        END AS CHAR_OCTET_LENGTH,
                        a.attnum AS ORDINAL_POSITION,
                        CASE a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)
                            WHEN 'false' THEN 'YES'
                            WHEN NULL THEN ''
                            ELSE 'NO'
                        END AS IS_NULLABLE,
                        NULL AS SCOPE_CATALOG,
                        NULL AS SCOPE_SCHEMA,
                        NULL AS SCOPE_TABLE,
                        t.typbasetype AS SOURCE_DATA_TYPE,
                        CASE
                            WHEN left(pg_catalog.pg_get_expr(def.adbin, def.adrelid), 16) = 'default_identity' THEN 'YES'
                            ELSE 'NO'
                        END AS IS_AUTOINCREMENT,
                        false AS IS_GENERATEDCOLUMN
                    FROM pg_catalog.pg_namespace AS n
                    JOIN pg_catalog.pg_class AS c ON (c.relnamespace = n.oid)
                    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid = c.oid)
                    JOIN pg_catalog.pg_type AS t ON (a.atttypid = t.oid)
                    LEFT JOIN pg_catalog.pg_attrdef AS def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum)
                    LEFT JOIN pg_catalog.pg_description AS dsc ON (c.oid = dsc.objoid AND a.attnum = dsc.objsubid)
                    LEFT JOIN pg_catalog.pg_class AS dc ON (dc.oid = dsc.classoid AND dc.relname = 'pg_class')
                    LEFT JOIN pg_catalog.pg_namespace AS dn ON (dc.relnamespace = dn.oid AND dn.nspname = 'pg_catalog')
                    WHERE
                        a.attnum > 0 AND
                        NOT a.attisdropped AND
                        current_database() = 'cubedb' AND
                        n.nspname LIKE 'public' AND
                        c.relname LIKE 'KibanaSampleDataEcommerce'
                    ORDER BY
                        TABLE_SCHEM,
                        c.relname,
                        attnum
                ) AS t
                UNION ALL
                SELECT
                    CAST(current_database() AS CHARACTER VARYING(128)) AS TABLE_CAT,
                    CAST(schemaname AS CHARACTER VARYING(128)) AS table_schem,
                    CAST(tablename AS CHARACTER VARYING(128)) AS table_name,
                    CAST(columnname AS CHARACTER VARYING(128)) AS column_name,
                    CAST(
                        CASE columntype_rep
                            WHEN 'text' THEN 12
                            WHEN 'bit' THEN - 7
                            WHEN 'bool' THEN - 7
                            WHEN 'boolean' THEN - 7
                            WHEN 'varchar' THEN 12
                            WHEN 'character varying' THEN 12
                            WHEN 'char' THEN 1
                            WHEN 'character' THEN 1
                            WHEN 'nchar' THEN 1
                            WHEN 'bpchar' THEN 1
                            WHEN 'nvarchar' THEN 12
                            WHEN '"char"' THEN 1
                            WHEN 'date' THEN 91
                            WHEN 'time' THEN 92
                            WHEN 'time without time zone' THEN 92
                            WHEN 'timetz' THEN 2013
                            WHEN 'time with time zone' THEN 2013
                            WHEN 'timestamp' THEN 93
                            WHEN 'timestamp without time zone' THEN 93
                            WHEN 'timestamptz' THEN 2014
                            WHEN 'timestamp with time zone' THEN 2014
                            WHEN 'smallint' THEN 5
                            WHEN 'int2' THEN 5
                            WHEN 'integer' THEN 4
                            WHEN 'int' THEN 4
                            WHEN 'int4' THEN 4
                            WHEN 'bigint' THEN - 5
                            WHEN 'int8' THEN - 5
                            WHEN 'decimal' THEN 3
                            WHEN 'real' THEN 7
                            WHEN 'float4' THEN 7
                            WHEN 'double precision' THEN 8
                            WHEN 'float8' THEN 8
                            WHEN 'float' THEN 6
                            WHEN 'numeric' THEN 2
                            WHEN 'timestamptz' THEN 2014
                            WHEN 'bytea' THEN - 2
                            WHEN 'oid' THEN - 5
                            WHEN 'name' THEN 12
                            WHEN 'ARRAY' THEN 2003
                            WHEN 'geometry' THEN - 4
                            WHEN 'super' THEN - 16
                            WHEN 'varbyte' THEN - 4
                            WHEN 'geography' THEN - 4
                            ELSE 1111
                        END
                        AS SMALLINT
                    ) AS DATA_TYPE,
                    COALESCE(
                        NULL,
                        CASE columntype
                            WHEN 'boolean' THEN 'bool'
                            WHEN 'character varying' THEN 'varchar'
                            WHEN '"char"' THEN 'char'
                            WHEN 'smallint' THEN 'int2'
                            WHEN 'integer' THEN 'int4'
                            WHEN 'bigint' THEN 'int8'
                            WHEN 'real' THEN 'float4'
                            WHEN 'double precision' THEN 'float8'
                            WHEN 'time without time zone' THEN 'time'
                            WHEN 'time with time zone' THEN 'timetz'
                            WHEN 'timestamp without time zone' THEN 'timestamp'
                            WHEN 'timestamp with time zone' THEN 'timestamptz'
                            ELSE columntype
                        END
                    ) AS TYPE_NAME,
                    CASE columntype_rep
                        WHEN 'int4' THEN 10
                        WHEN 'bit' THEN 1
                        WHEN 'bool' THEN 1
                        WHEN 'boolean' THEN 1
                        WHEN 'varchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'character varying' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'char' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 4), ''), '0') AS INT)
                        WHEN 'character' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 4), ''), '0') AS INT)
                        WHEN 'nchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'bpchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'nvarchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'date' THEN 13
                        WHEN 'time' THEN 15
                        WHEN 'time without time zone' THEN 15
                        WHEN 'timetz' THEN 21
                        WHEN 'timestamp' THEN 29
                        WHEN 'timestamp without time zone' THEN 29
                        WHEN 'time with time zone' THEN 21
                        WHEN 'timestamptz' THEN 35
                        WHEN 'timestamp with time zone' THEN 35
                        WHEN 'smallint' THEN 5
                        WHEN 'int2' THEN 5
                        WHEN 'integer' THEN 10
                        WHEN 'int' THEN 10
                        WHEN 'int4' THEN 10
                        WHEN 'bigint' THEN 19
                        WHEN 'int8' THEN 19
                        WHEN 'decimal' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'real' THEN 8
                        WHEN 'float4' THEN 8
                        WHEN 'double precision' THEN 17
                        WHEN 'float8' THEN 17
                        WHEN 'float' THEN 17
                        WHEN 'numeric' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN '_float4' THEN 8
                        WHEN 'oid' THEN 10
                        WHEN '_int4' THEN 10
                        WHEN '_int2' THEN 5
                        WHEN 'geometry' THEN NULL
                        WHEN 'super' THEN NULL
                        WHEN 'varbyte' THEN NULL
                        WHEN 'geography' THEN NULL
                        ELSE 2147483647
                    END AS COLUMN_SIZE,
                    NULL AS BUFFER_LENGTH,
                    CASE REGEXP_REPLACE(columntype, '[()0-9,]')
                        WHEN 'real' THEN 8
                        WHEN 'float4' THEN 8
                        WHEN 'double precision' THEN 17
                        WHEN 'float8' THEN 17
                        WHEN 'timestamp' THEN 6
                        WHEN 'timestamp without time zone' THEN 6
                        WHEN 'geometry' THEN NULL
                        WHEN 'super' THEN NULL
                        WHEN 'numeric' THEN CAST(regexp_substr(columntype, '[0-9]+', charindex(',', columntype)) AS INT)
                        WHEN 'varbyte' THEN NULL
                        WHEN 'geography' THEN NULL
                        ELSE 0
                    END AS DECIMAL_DIGITS,
                    CASE columntype
                        WHEN 'varbyte' THEN 2
                        WHEN 'geography' THEN 2
                        ELSE 10
                    END AS NUM_PREC_RADIX,
                    NULL AS NULLABLE,
                    NULL AS REMARKS,
                    NULL AS COLUMN_DEF,
                    CAST(
                        CASE columntype_rep
                            WHEN 'text' THEN 12
                            WHEN 'bit' THEN - 7
                            WHEN 'bool' THEN - 7
                            WHEN 'boolean' THEN - 7
                            WHEN 'varchar' THEN 12
                            WHEN 'character varying' THEN 12
                            WHEN 'char' THEN 1
                            WHEN 'character' THEN 1
                            WHEN 'nchar' THEN 12
                            WHEN 'bpchar' THEN 1
                            WHEN 'nvarchar' THEN 12
                            WHEN '"char"' THEN 1
                            WHEN 'date' THEN 91
                            WHEN 'time' THEN 92
                            WHEN 'time without time zone' THEN 92
                            WHEN 'timetz' THEN 2013
                            WHEN 'time with time zone' THEN 2013
                            WHEN 'timestamp' THEN 93
                            WHEN 'timestamp without time zone' THEN 93
                            WHEN 'timestamptz' THEN 2014
                            WHEN 'timestamp with time zone' THEN 2014
                            WHEN 'smallint' THEN 5
                            WHEN 'int2' THEN 5
                            WHEN 'integer' THEN 4
                            WHEN 'int' THEN 4
                            WHEN 'int4' THEN 4
                            WHEN 'bigint' THEN - 5
                            WHEN 'int8' THEN - 5
                            WHEN 'decimal' THEN 3
                            WHEN 'real' THEN 7
                            WHEN 'float4' THEN 7
                            WHEN 'double precision' THEN 8
                            WHEN 'float8' THEN 8
                            WHEN 'float' THEN 6
                            WHEN 'numeric' THEN 2
                            WHEN 'bytea' THEN - 2
                            WHEN 'oid' THEN - 5
                            WHEN 'name' THEN 12
                            WHEN 'ARRAY' THEN 2003
                            WHEN 'geometry' THEN - 4
                            WHEN 'super' THEN - 16
                            WHEN 'varbyte' THEN - 4
                            WHEN 'geography' THEN - 4
                            ELSE 1111
                        END
                        AS SMALLINT
                    ) AS SQL_DATA_TYPE,
                    CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB,
                    CASE
                        WHEN LEFT(columntype, 7) = 'varchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN LEFT(columntype, 4) = 'char' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 4), ''), '0') AS INT)
                        WHEN columntype = 'string' THEN 16383
                        ELSE NULL
                    END AS CHAR_OCTET_LENGTH,
                    columnnum AS ORDINAL_POSITION,
                    NULL AS IS_NULLABLE,
                    NULL AS SCOPE_CATALOG,
                    NULL AS SCOPE_SCHEMA,
                    NULL AS SCOPE_TABLE,
                    NULL AS SOURCE_DATA_TYPE,
                    'NO' AS IS_AUTOINCREMENT,
                    'NO' AS IS_GENERATEDCOLUMN
                FROM (
                    SELECT
                        schemaname,
                        tablename,
                        columnname,
                        columntype AS columntype_rep,
                        columntype,
                        columnnum
                    FROM get_late_binding_view_cols_unpacked
                ) AS lbv_columns
                WHERE
                    true AND
                    current_database() = 'cubedb' AND
                    schemaname LIKE 'public' AND
                    tablename LIKE 'KibanaSampleDataEcommerce'
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_to_timestamp_format() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT
                date_trunc('day', "order_date") AS "uuid.order_date_tg",
                COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "order_date" >= date_trunc('second', TO_TIMESTAMP('2019-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')) AND
                "order_date" < date_trunc('second', TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss'))
            GROUP BY date_trunc('day', "order_date")
            ORDER BY date_trunc('day', "order_date") DESC NULLS LAST
            LIMIT 2500
            ;"#.to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2019-01-01T00:00:00.000Z".to_string(),
                        "2019-12-31T23:59:59.999Z".to_string()
                    ])),
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(2500),
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_dense_rank() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date", "isotherrow_1", "faabeaae-5980-4f8f-a5ba-12f56f191f1e.avgPrice_avg", "$otherbucket_group_count", "count"
            FROM (
            SELECT "$f4" AS "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date", "$f5", "$f6" AS "isotherrow_1", SUM("$weighted_avg_unit_4") AS "faabeaae-5980-4f8f-a5ba-12f56f191f1e.avgPrice_avg", COUNT(*) AS "$otherbucket_group_count", SUM("count") AS "count"
            FROM (
            SELECT "count", CASE WHEN "$RANK_1" > 2500 THEN NULL ELSE "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date" END AS "$f4", CASE WHEN "$RANK_1" > 2500 THEN NULL ELSE "$RANK_1" END AS "$f5", CASE WHEN "$RANK_1" > 2500 THEN 1 ELSE 0 END AS "$f6", CAST("$weighted_avg_count_3" AS FLOAT) / NULLIF(CAST(SUM("$weighted_avg_count_3") OVER (PARTITION BY CASE WHEN "$RANK_1" > 2500 THEN NULL ELSE "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date" END, CASE WHEN "$RANK_1" > 2500 THEN NULL ELSE "$RANK_1" END, CASE WHEN "$RANK_1" > 2500 THEN 1 ELSE 0 END) AS FLOAT), 0) * "faabeaae-5980-4f8f-a5ba-12f56f191f1e.avgPrice_avg" AS "$weighted_avg_unit_4"
            FROM (
            SELECT "order_date" AS "faabeaae-5980-4f8f-a5ba-12f56f191f1e.order_date", COUNT(*) AS "count", AVG("avgPrice") AS "faabeaae-5980-4f8f-a5ba-12f56f191f1e.avgPrice_avg", DENSE_RANK() OVER (ORDER BY AVG("avgPrice") DESC NULLS LAST, "order_date" NULLS FIRST) AS "$RANK_1", COUNT("avgPrice") AS "$weighted_avg_count_3"
            FROM "public"."KibanaSampleDataEcommerce"
            GROUP BY "order_date"
            ) AS "t"
            ) AS "t0"
            GROUP BY "$f4", "$f5", "$f6"
            ORDER BY "$f5" NULLS FIRST
            ) AS "t1"
            ;"#.to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string()
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        let physical_plan = query_plan.as_physical_plan().await.unwrap();
        println!("Physical plan: {:?}", physical_plan);

        Ok(())
    }

    #[tokio::test]
    async fn test_localtimestamp() -> Result<(), CubeError> {
        // TODO: the value will be different with the introduction of TZ support
        insta::assert_snapshot!(
            "localtimestamp",
            execute_query(
                "SELECT localtimestamp = current_timestamp".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_current_date() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CURRENT_DATE AS \"COL\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = &query_plan.print(true).unwrap();

        let re = Regex::new(r#"Date32\("\d+"\)"#).unwrap();
        let logical_plan = re
            .replace_all(logical_plan, "Date32(\"0\")")
            .as_ref()
            .to_string();

        assert_eq!(
            logical_plan,
            "Projection: Date32(\"0\") AS COL\
            \n  EmptyRelation",
        );

        insta::assert_snapshot!(
            "current_date",
            execute_query(
                "SELECT current_timestamp::date = current_date".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_union_ctes() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "union_ctes",
            execute_query(
                "
                WITH w AS (SELECT 1 l)
                SELECT w.l
                FROM w
                UNION ALL (SELECT w.l FROM w)
                ;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cast_decimal_default_precision() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "cast_decimal_default_precision",
            execute_query(
                "
                SELECT \"rows\".b as \"plan\", count(1) as \"a0\"
                FROM (SELECT * FROM (select 1 \"teamSize\", 2 b UNION ALL select 1011 \"teamSize\", 3 b) \"_\"
                WHERE ((CAST(\"_\".\"teamSize\" as DECIMAL) = CAST(1011 as DECIMAL)))) \"rows\"
                GROUP BY \"plan\";
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT count FROM KibanaSampleDataEcommerce WHERE (CAST(maxPrice AS Decimal) = CAST(100 AS Decimal));"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["100".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_triple_ident() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "select count
            from \"public\".\"KibanaSampleDataEcommerce\"
            where (\"public\".\"KibanaSampleDataEcommerce\".\"maxPrice\" > 100 and \"public\".\"KibanaSampleDataEcommerce\".\"maxPrice\" < 150);
            ".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("gt".to_string()),
                        values: Some(vec!["100".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("lt".to_string()),
                        values: Some(vec!["150".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn metabase_interval_date_range_filter() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT COUNT(*) 
            FROM KibanaSampleDataEcommerce 
            WHERE KibanaSampleDataEcommerce.order_date >= CAST((CAST(now() AS timestamp) + (INTERVAL '-30 day')) AS date);
            ".to_string(), 
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        let filters = logical_plan
            .find_cube_scan()
            .request
            .filters
            .unwrap_or_default();
        let filter_vals = if filters.len() > 0 {
            filters[0].values.clone()
        } else {
            None
        };

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("afterDate".to_string()),
                    values: filter_vals,
                    or: None,
                    and: None,
                },])
            }
        )
    }

    #[tokio::test]
    async fn superset_timeout_reached() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "
            SELECT \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",\
             \"KibanaSampleDataEcommerce\".\"order_date\" AS \"order_date\", \
             \"KibanaSampleDataEcommerce\".\"is_male\" AS \"is_male\",\
             \"KibanaSampleDataEcommerce\".\"is_female\" AS \"is_female\",\
             \"KibanaSampleDataEcommerce\".\"maxPrice\" AS \"maxPrice\",\
             \"KibanaSampleDataEcommerce\".\"minPrice\" AS \"minPrice\",\
             \"KibanaSampleDataEcommerce\".\"avgPrice\" AS \"avgPrice\"\
             FROM public.\"KibanaSampleDataEcommerce\" WHERE \"order_date\" >= str_to_date('2021-06-30 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US') AND \"order_date\" < str_to_date('2022-06-30 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US') AND \"is_male\" = true ORDER BY \"order_date\" DESC LIMIT 10000
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec!["KibanaSampleDataEcommerce.is_male".to_string()]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-06-30T00:00:00.000Z".to_string(),
                        "2022-06-29T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string(),
                ]]),
                limit: Some(10000),
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn superset_ilike() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT customer_gender AS customer_gender FROM public.\"KibanaSampleDataEcommerce\" WHERE customer_gender ILIKE '%fem%' GROUP BY customer_gender LIMIT 1000".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn metabase_limit_0() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT true AS \"_\" FROM \"public\".\"KibanaSampleDataEcommerce\" WHERE 1 <> 1 LIMIT 0".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1),
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_outer_aggr_simple_count() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT CAST(TRUNC(EXTRACT(YEAR FROM order_date)) AS INTEGER), Count(1) FROM KibanaSampleDataEcommerce GROUP BY 1
            ".to_string(), 
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("year".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn metabase_date_filters() {
        init_logger();

        let now = "str_to_date('2022-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')";
        let cases = vec![
            // last 30 days
            [
                format!("CAST(({} + (INTERVAL '-30 day')) AS date)", now),
                format!("CAST({} AS date)", now),
                "2021-12-02T00:00:00.000Z".to_string(),
                "2021-12-31T23:59:59.999Z".to_string(),
            ],
            // last 30 weeks
            [
                format!("(CAST(date_trunc('week', (({} + (INTERVAL '-30 week')) + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                format!("(CAST(date_trunc('week', ({} + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                "2021-05-30T00:00:00.000Z".to_string(),
                "2021-12-25T23:59:59.999Z".to_string(),
            ],
            // last 30 quarters
            [
                format!("date_trunc('quarter', ({} + (INTERVAL '-90 month')))", now),
                format!("date_trunc('quarter', {})", now),
                "2014-07-01T00:00:00.000Z".to_string(),
                "2021-12-31T23:59:59.999Z".to_string(),
            ],
            // this year
            [
                format!("date_trunc('year', {})", now),
                format!("date_trunc('year', ({} + (INTERVAL '1 year')))", now),
                "2022-01-01T00:00:00.000Z".to_string(),
                "2022-12-31T23:59:59.999Z".to_string(),
            ],
            // next 2 years including current
            [
                format!("date_trunc('year', {})", now),
                format!("date_trunc('year', ({} + (INTERVAL '3 year')))", now),
                "2022-01-01T00:00:00.000Z".to_string(),
                "2024-12-31T23:59:59.999Z".to_string(),
            ],
        ];
        for [lte, gt, from, to] in cases {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT count FROM (SELECT count FROM KibanaSampleDataEcommerce
                    WHERE (order_date >= {} AND order_date < {})) source",
                    lte, gt
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: None,
                        date_range: Some(json!(vec![from, to])),
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            );
        }

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"source\".\"count\" AS \"count\" 
            FROM (
                    SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" FROM \"public\".\"KibanaSampleDataEcommerce\"
                    WHERE \"public\".\"KibanaSampleDataEcommerce\".\"order_date\" 
                    BETWEEN timestamp with time zone '2022-06-13T12:30:00.000Z' 
                    AND timestamp with time zone '2022-06-29T12:30:00.000Z'
            ) 
            \"source\""
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2022-06-13T12:30:00.000Z".to_string(),
                        "2022-06-29T12:30:00.000Z".to_string()
                    ]))
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );

        let cases = vec![
            // prev 5 days starting 4 weeks ago
            [
                "(INTERVAL '4 week')".to_string(),
                format!("CAST(({} + (INTERVAL '-5 day')) AS date)", now),
                format!("CAST({} AS date)", now),
                "2021-11-29T00:00:00.000Z".to_string(),
                "2021-12-04T00:00:00.000Z".to_string(),
            ],
            // prev 5 weeks starting 4 weeks ago
            [
                "(INTERVAL '4 week')".to_string(),
                format!("(CAST(date_trunc('week', (({} + (INTERVAL '-5 week')) + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                format!("(CAST(date_trunc('week', ({} + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                "2021-10-24T00:00:00.000Z".to_string(),
                "2021-11-28T00:00:00.000Z".to_string(),
            ],
            // prev 5 months starting 4 months ago
            [
                "(INTERVAL '4 month')".to_string(),
                format!("date_trunc('month', ({} + (INTERVAL '-5 month')))", now),
                format!("date_trunc('month', {})", now),
                "2021-04-01T00:00:00.000Z".to_string(),
                "2021-09-01T00:00:00.000Z".to_string(),
            ],
        ];

        for [interval, lowest, highest, from, to] in cases {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT \"source\".\"count\" AS \"count\" 
                    FROM (
                        SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" FROM \"public\".\"KibanaSampleDataEcommerce\"
                        WHERE (\"public\".\"KibanaSampleDataEcommerce\".\"order_date\" + {}) BETWEEN {} AND {}
                    ) 
                    \"source\"",
                    interval, lowest, highest
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: None,
                        date_range: Some(json!(vec![from, to])),
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            );
        }

        let logical_plan = convert_select_to_query_plan(
            format!(
                "SELECT \"source\".\"order_date\" AS \"order_date\", \"source\".\"max\" AS \"max\"
                FROM (SELECT date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\") AS \"order_date\", max(\"KibanaSampleDataEcommerce\".\"maxPrice\") AS \"max\" FROM \"KibanaSampleDataEcommerce\"
                GROUP BY date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\")
                ORDER BY date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\") ASC) \"source\"
                WHERE (CAST(date_trunc('month', \"source\".\"order_date\") AS timestamp) + (INTERVAL '60 minute')) BETWEEN date_trunc('minute', ({} + (INTERVAL '-30 minute')))
                AND date_trunc('minute', {})",
                now, now
            ),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: None
            }
        );
    }

    #[tokio::test]
    async fn test_metabase_bins() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1) AS \"taxful_total_price\", count(*) AS \"count\"
            FROM \"public\".\"KibanaSampleDataEcommerce\"
            GROUP BY ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1)
            ORDER BY ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1) ASC;
            ".to_string(), 
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn metabase_contains_str_filters() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\"
                FROM \"public\".\"KibanaSampleDataEcommerce\"
                WHERE (lower(\"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\") like '%female%')
                LIMIT 10"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                },]),
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\"
            FROM \"public\".\"KibanaSampleDataEcommerce\"
            WHERE (NOT (lower(\"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\") like '%female%') OR \"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\" IS NULL)
            LIMIT 10"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notContains".to_string()),
                            values: Some(vec!["female".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
            }
        );
    }

    #[tokio::test]
    async fn metabase_between_numbers_filters() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" 
                FROM \"public\".\"KibanaSampleDataEcommerce\" 
                WHERE \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" BETWEEN 1 AND 2
                LIMIT 10"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10),
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                        operator: Some("gte".to_string()),
                        values: Some(vec!["1".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                        operator: Some("lte".to_string()),
                        values: Some(vec!["2".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" 
            FROM \"public\".\"KibanaSampleDataEcommerce\" 
            WHERE \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" NOT BETWEEN 1 AND 2
            LIMIT 10"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("lt".to_string()),
                            values: Some(vec!["1".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("gt".to_string()),
                            values: Some(vec!["2".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
            }
        );
    }

    #[tokio::test]
    async fn metabase_aggreagte_by_week_of_year() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0)) AS \"order_date\", 
                               min(\"KibanaSampleDataEcommerce\".\"minPrice\") AS \"min\"
                FROM \"KibanaSampleDataEcommerce\"
                GROUP BY ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0))
                ORDER BY ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0)) ASC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.minPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: None,
                },]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn datastudio_date_aggregations() {
        init_logger();

        let supported_granularities = vec![
            // date
            [
                "CAST(DATE_TRUNC('SECOND', \"order_date\") AS DATE)",
                "second",
            ],
            // date, time
            ["DATE_TRUNC('SECOND', \"order_date\")", "second"],
            // date, hour, minute
            [
                "DATE_TRUNC('MINUTE', DATE_TRUNC('SECOND', \"order_date\"))",
                "minute",
            ],
            // month
            [
                "EXTRACT(MONTH FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "month",
            ],
            // minute
            [
                "EXTRACT(MINUTE FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "minute",
            ],
            // hour
            [
                "EXTRACT(HOUR FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "hour",
            ],
            // day of month
            [
                "EXTRACT(DAY FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "day",
            ],
            // iso week / iso year / day of year
            ["DATE_TRUNC('SECOND', \"order_date\")", "second"],
            // month, day
            [
                "CAST(TO_CHAR(DATE_TRUNC('SECOND', \"order_date\"), 'MMDD') AS BIGINT)",
                "second",
            ],
            // date, hour, minute
            [
                "DATE_TRUNC('MINUTE', DATE_TRUNC('SECOND', \"order_date\"))",
                "minute",
            ],
            // date, hour
            [
                "DATE_TRUNC('HOUR', DATE_TRUNC('SECOND', \"order_date\"))",
                "hour",
            ],
            // year, month
            [
                "CAST(DATE_TRUNC('MONTH', DATE_TRUNC('SECOND', \"order_date\")) AS DATE)",
                "month",
            ],
            // year
            [
                "CAST(DATE_TRUNC('YEAR', DATE_TRUNC('SECOND', \"order_date\")) AS DATE)",
                "year",
            ],
        ];

        for [expr, expected_granularity] in supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT {} AS \"qt_u3dj8wr1vc\", COUNT(1) AS \"__record_count\" FROM KibanaSampleDataEcommerce GROUP BY \"qt_u3dj8wr1vc\"",
                    expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(expected_granularity.to_string()),
                        date_range: None,
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            )
        }
    }

    #[tokio::test]
    async fn test_datastudio_min_max_date() {
        init_logger();

        for fun in vec!["Max", "Min"].iter() {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "
                SELECT 
                    CAST(Date_trunc('SECOND', \"order_date\") AS DATE) AS \"qt_m3uskv6gwc\", 
                    {}(Date_trunc('SECOND', \"order_date\")) AS \"qt_d3yqo2towc\"
                FROM  KibanaSampleDataEcommerce
                GROUP BY \"qt_m3uskv6gwc\"
                ",
                    fun
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("second".to_string()),
                        date_range: None,
                    },]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                }
            )
        }
    }

    #[tokio::test]
    async fn test_datastudio_between_dates_filter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "
            SELECT 
                CAST(Date_trunc('SECOND', \"order_date\") AS DATE) AS \"qt_m3uskv6gwc\",
                COUNT(1) AS \"__record_count\"
            FROM KibanaSampleDataEcommerce
            WHERE Date_trunc('SECOND', \"order_date\") 
                BETWEEN 
                    CAST('2022-07-11 18:00:00.000000' AS TIMESTAMP) 
                AND CAST('2022-07-11 19:00:00.000000' AS TIMESTAMP)
            GROUP BY \"qt_m3uskv6gwc\";
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("second".to_string()),
                    date_range: Some(json!(vec![
                        "2022-07-11T18:00:00.000Z".to_string(),
                        "2022-07-11T19:00:00.000Z".to_string()
                    ])),
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_datastudio_string_start_with_filter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "
            SELECT 
                CAST(Date_trunc('SECOND', \"order_date\") AS DATE) AS \"qt_m3uskv6gwc\",
                COUNT(1) AS \"__record_count\",
                \"customer_gender\"
            FROM  KibanaSampleDataEcommerce
            WHERE (\"customer_gender\" ~ 'test')
            GROUP BY \"qt_m3uskv6gwc\", \"customer_gender\";
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("second".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["test".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_extract_date_trunc_week() {
        init_logger();

        let supported_granularities = vec![
            (
                "EXTRACT(WEEK FROM DATE_TRUNC('MONTH', \"order_date\"))::integer",
                "month",
            ),
            (
                "EXTRACT(MONTH FROM DATE_TRUNC('WEEK', \"order_date\"))::integer",
                "week",
            ),
        ];

        for (expr, granularity) in supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT {} AS \"qt_u3dj8wr1vc\" FROM KibanaSampleDataEcommerce GROUP BY \"qt_u3dj8wr1vc\"",
                    expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(granularity.to_string()),
                        date_range: None,
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            )
        }
    }

    #[tokio::test]
    async fn test_metabase_unwrap_date_cast() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT max(CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS date)) AS \"max\" FROM \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_metabase_substring_user() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"source\".\"substring131715\" AS \"substring131715\" 
                FROM (
                    SELECT 
                        \"public\".\"KibanaSampleDataEcommerce\".\"__user\" AS \"__user\", 
                        SUBSTRING(\"public\".\"KibanaSampleDataEcommerce\".\"__user\" FROM 1 FOR 1234) AS \"substring131715\" 
                    FROM \"public\".\"KibanaSampleDataEcommerce\"
                ) AS \"source\" 
                LIMIT 10000".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10000),
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_select_asterisk_cross_join() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT * FROM \"KibanaSampleDataEcommerce\" CROSS JOIN Logs".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    "KibanaSampleDataEcommerce.countDistinct".to_string(),
                    "Logs.agentCount".to_string(),
                    "Logs.agentCountApprox".to_string(),
                ]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.last_mod".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    "KibanaSampleDataEcommerce.has_subscription".to_string(),
                    "Logs.id".to_string(),
                    "Logs.read".to_string(),
                    "Logs.content".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_user_with_join() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT aliased.count as c, aliased.user_1 as u1, aliased.user_2 as u2 FROM (SELECT \"KibanaSampleDataEcommerce\".count as count, \"KibanaSampleDataEcommerce\".__user as user_1, Logs.__user as user_2 FROM \"KibanaSampleDataEcommerce\" CROSS JOIN Logs WHERE __user = 'foo') aliased".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await
            .as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();
        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        assert_eq!(cube_scan.options.change_user, Some("foo".to_string()))
    }

    #[tokio::test]
    async fn test_sort_relations() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "test_sort_relations_0",
            execute_query(
                "select pg_class.oid as oid from pg_class order by pg_class.oid asc".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_1",
            execute_query(
                "select * from (select pg_class.oid AS oid from pg_class order by pg_class.oid) source".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_2",
            execute_query(
                "select * from (select oid from pg_class order by pg_class.oid) t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_3",
            execute_query(
                "select t.oid as oid from (select oid as oid from pg_class) t order by t.oid"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_4",
            execute_query(
                "select oid as oid from (select count(oid) as oid from pg_class order by count(pg_class.oid)) t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_5",
            execute_query(
                "select oid as oid from (select count(oid) as oid from pg_class order by count(oid)) t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_6",
            execute_query(
                "select pg_class.oid as oid from pg_class group by pg_class.oid order by pg_class.oid asc".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_sort_relations_7",
            execute_query(
                "select * from (select oid from pg_class group by pg_class.oid order by pg_class.oid) t".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_offset_limit() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "test_offset_limit_1",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 10 offset 10".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_offset_limit_2",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 10 offset 0".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_offset_limit_3",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 0 offset 10".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_offset_limit_4",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 100 offset 100".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_offset_limit_5",
            execute_query(
                "select n from generate_series(1, 1000) pos(n) limit 100 offset 990".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_superset_pagination() {
        init_logger();

        // At first, Superset gets the total count (no more than 50k)
        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) AS rowcount FROM (SELECT order_date as order_date FROM public.\"KibanaSampleDataEcommerce\" GROUP BY order_date LIMIT 50000) AS rowcount_qry".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await.as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();
        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(50000),
                offset: None,
                filters: None,
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT order_date AS order_date FROM public.\"KibanaSampleDataEcommerce\" GROUP BY order_date LIMIT 200 OFFSET 200".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await.as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();
        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(200),
                offset: Some(200),
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn test_holistics_schema_privilege_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "holistics_schema_privilege_query",
            execute_query(
                "
                SELECT n.nspname AS schema_name
                FROM pg_namespace n
                WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema' AND has_schema_privilege(n.nspname, 'USAGE'::text);
                ".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_holistics_left_join_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "holistics_left_join_query",
            execute_query(
                "
                SELECT 
                    TRIM(c.conname) AS constraint_name, 
                    CASE c.contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'f' THEN 'FOREIGN KEY' END AS constraint_type, 
                    TRIM(cn.nspname) AS constraint_schema, 
                    TRIM(tn.nspname) AS schema_name, 
                    TRIM(tc.relname) AS table_name, 
                    TRIM(ta.attname) AS column_name, 
                    TRIM(fn.nspname) AS referenced_schema_name, 
                    TRIM(fc.relname) AS referenced_table_name, 
                    TRIM(fa.attname) AS referenced_column_name, 
                    o.ord AS ordinal_position
                FROM pg_constraint c
                    LEFT JOIN generate_series(1,1600) as o(ord) ON c.conkey[o.ord] IS NOT  NULL
                    LEFT JOIN pg_attribute ta ON c.conrelid=ta.attrelid AND ta.attnum=c.conkey[o.ord]
                    LEFT JOIN pg_attribute fa ON c.confrelid=fa.attrelid AND fa.attnum=c.confkey[o.ord]
                    LEFT JOIN pg_class tc ON ta.attrelid=tc.oid
                    LEFT JOIN pg_class fc ON fa.attrelid=fc.oid
                    LEFT JOIN pg_namespace cn ON c.connamespace=cn.oid
                    LEFT JOIN pg_namespace tn ON tc.relnamespace=tn.oid
                    LEFT JOIN pg_namespace fn ON fc.relnamespace=fn.oid
                WHERE 
                    CASE c.contype WHEN 'p' 
                    THEN 'PRIMARY KEY' WHEN 'u' 
                    THEN 'UNIQUE' WHEN 'f' 
                    THEN 'FOREIGN KEY' 
                    END 
                IN ('UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY') AND tc.relkind = 'r'
                ".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_holistics_in_subquery_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "holistics_in_subquery_query",
            execute_query(
                "SELECT\n          n.nspname || '.' || c.relname AS \"table_name\",\n          a.attname AS \"column_name\",\n          format_type(a.atttypid, a.atttypmod) AS \"data_type\"\n        FROM pg_namespace n,\n             pg_class c,\n             pg_attribute a\n        WHERE n.oid = c.relnamespace\n          AND c.oid = a.attrelid\n          AND a.attnum > 0\n          AND NOT a.attisdropped\n          AND c.relname IN (SELECT table_name\nFROM information_schema.tables\nWHERE (table_type = 'BASE TABLE' OR table_type = 'VIEW')\n  AND table_schema NOT IN ('pg_catalog', 'information_schema')\n  AND has_schema_privilege(table_schema, 'USAGE'::text)\n)\n
                /* Added to avoid random output order and validate snapshot */
                order by table_name, column_name;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_holistics_group_by_date() {
        init_logger();

        for granularity in vec!["year", "quarter", "month", "week", "day", "hour", "minute"].iter()
        {
            let logical_plan = convert_select_to_query_plan(
                format!("
                    SELECT 
                        TO_CHAR((CAST((DATE_TRUNC('{}', (CAST(\"table\".\"order_date\" AS timestamptz)) AT TIME ZONE 'Etc/UTC')) AT TIME ZONE 'Etc/UTC' AS timestamptz)) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS') AS \"dm_pu_ca_754b1e\",
                        MAX(\"table\".\"maxPrice\") AS \"a_pu_n_51f23b\"
                    FROM \"KibanaSampleDataEcommerce\" \"table\"
                    GROUP BY 1
                    ORDER BY 2 DESC
                    LIMIT 100000", 
                    granularity),
                DatabaseProtocol::PostgreSQL
            ).await.as_logical_plan();

            let cube_scan = logical_plan.find_cube_scan();

            assert_eq!(
                cube_scan.request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(granularity.to_string()),
                        date_range: None
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                }
            );
        }
    }

    #[tokio::test]
    async fn test_holistics_split_with_literals() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT
                \"table\".\"maxPrice\" AS \"pu_mn_287b51__0\",
                MIN(\"table\".\"minPrice\") AS \"m_pu_mn_ad42df__1\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_0\",
                0 AS \"h__model_level\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            GROUP BY
                1,
                3,
                4
            ORDER BY
                4 DESC
            LIMIT 100000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string()
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT
                TO_CHAR((CAST ( (DATE_TRUNC ( 'month', (CAST ( \"table\".\"order_date\" AS timestamptz )) AT TIME ZONE 'Etc/UTC' )) AT TIME ZONE 'Etc/UTC' AS timestamptz )) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS \"dm_pu_ca_754b1e__0\",
                MAX(\"table\".\"maxPrice\") AS \"m_pu_mn_0844e5__1\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_0\",
                0 AS \"h__model_level\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            GROUP BY
                1,
                3,
                4
            ORDER BY
                4 DESC
            LIMIT 100000".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn test_holistics_str_not_contains_filter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(\"table\".\"count\") AS \"c_pu_c_d4696e\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            WHERE NOT(\"table\".\"customer_gender\" ILIKE ('%' || CAST ( 'test' AS text ) || '%'))
            ORDER BY 1 DESC
            LIMIT 100000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(100000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notContains".to_string()),
                    values: Some(vec!["test".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn test_holistics_aggr_fun_with_null() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"table\".\"count\" AS \"pu_c_3dcebf__0\", 
                \"table\".\"maxPrice\" AS \"pu_mn_287b51__1\",
                MIN(\"table\".\"minPrice\") AS \"m_pu_mn_ad42df__2\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_0\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_1\",
                0 AS \"h__model_level\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            GROUP BY
                1,
                2,
                4,
                5,
                6
            UNION ALL
            (
                SELECT
                    CAST ( NULL AS numeric ) AS \"pu_c_3dcebf__0\",
                    \"table\".\"maxPrice\" AS \"pu_mn_287b51__1\",
                    MIN(CAST ( NULL AS numeric )) AS \"m_pu_mn_ad42df__2\",
                    'total' AS \"h__placeholder_marker_0\",
                    CAST ( NULL AS text ) AS \"h__placeholder_marker_1\",
                    2 AS \"h__model_level\"
                FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
                GROUP BY
                1,
                2,
                4,
                5,
                6
            )
            ORDER BY
                6 DESC
            LIMIT 100000"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scans = logical_plan
            .find_cube_scans()
            .iter()
            .map(|cube| cube.request.clone())
            .collect::<Vec<V1LoadRequestQuery>>();

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }),
            true
        );

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string()
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }),
            true
        );
    }

    #[tokio::test]
    async fn test_holistics_split_with_nulls() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT TO_CHAR((CAST ( (DATE_TRUNC ( 'quarter', (CAST ( \"table\".\"order_date\" AS timestamptz )) AT TIME ZONE 'Etc/UTC' )) AT TIME ZONE 'Etc/UTC' AS timestamptz )) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS \"dq_pu_ca_6b9696__0\",
                \"table\".\"maxPrice\" AS \"pu_mn_287b51__1\",
                MIN(\"table\".\"minPrice\") AS \"m_pu_mn_ad42df__2\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_0\",
                CAST ( NULL AS text ) AS \"h__placeholder_marker_1\",
                0 AS \"h__model_level\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            GROUP BY
                1,
                2,
                4,
                5,
                6
            UNION ALL
            (
                SELECT TO_CHAR((CAST ( (DATE_TRUNC ( 'quarter', (CAST ( CAST ( NULL AS timestamptz ) AS timestamptz )) AT TIME ZONE 'Etc/UTC' )) AT TIME ZONE 'Etc/UTC' AS timestamptz )) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS \"dq_pu_ca_6b9696__0\",
                    \"table\".\"maxPrice\" AS \"pu_mn_287b51__1\",
                    MIN(CAST ( NULL AS numeric )) AS \"m_pu_mn_ad42df__2\",
                    'total' AS \"h__placeholder_marker_0\",
                    CAST ( NULL AS text ) AS \"h__placeholder_marker_1\",
                    2 AS \"h__model_level\"
                FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
                GROUP BY
                1,
                2,
                4,
                5,
                6
            )
            ORDER BY 6 DESC
            LIMIT 100000".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scans = logical_plan
            .find_cube_scans()
            .iter()
            .map(|cube| cube.request.clone())
            .collect::<Vec<V1LoadRequestQuery>>();

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }),
            true
        );

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string()
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }),
            true
        );
    }

    #[tokio::test]
    async fn test_holistics_in_dates_list_filter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT COUNT(\"table\".\"count\") AS \"c_pu_c_d4696e\"
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"table\"
            WHERE \"table\".\"order_date\" IN (CAST ( '2022-06-06 13:30:46' AS timestamptz ), CAST ( '2022-06-06 13:30:47' AS timestamptz ))
            ORDER BY 1 DESC
            LIMIT 100000".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scan = logical_plan.find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(100000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec![
                        "2022-06-06T13:30:46.000Z".to_string(),
                        "2022-06-06T13:30:47.000Z".to_string()
                    ]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn test_select_column_with_same_name_as_table() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "test_select_column_with_same_name_as_table",
            execute_query(
                "select table.column as column from (select 1 column, 2 table union all select 3 column, 4 table) table;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_interval_mul_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT date_trunc('day', "order_date") AS "uuid.order_date_tg", COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "order_date" >= date_trunc('year', LOCALTIMESTAMP + -5 * interval '1 YEAR') AND
                "order_date" < date_trunc('year', LOCALTIMESTAMP)
            GROUP BY date_trunc('day', "order_date")
            ORDER BY date_trunc('day', "order_date") DESC NULLS LAST
            LIMIT 2500;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let now = chrono::Utc::now();
        let current_year = now.naive_utc().date().year();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        format!("{}-01-01T00:00:00.000Z", current_year - 5),
                        format!("{}-12-31T23:59:59.999Z", current_year - 1),
                    ])),
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(2500),
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_date_trunc_equals() {
        init_logger();

        let base_date = "2022-08-27 19:43:09";
        let granularities = vec![
            (
                "second",
                "2022-08-27T19:43:09.000Z",
                "2022-08-27T19:43:09.999Z",
            ),
            (
                "minute",
                "2022-08-27T19:43:00.000Z",
                "2022-08-27T19:43:59.999Z",
            ),
            (
                "hour",
                "2022-08-27T19:00:00.000Z",
                "2022-08-27T19:59:59.999Z",
            ),
            (
                "day",
                "2022-08-27T00:00:00.000Z",
                "2022-08-27T23:59:59.999Z",
            ),
            (
                "week",
                "2022-08-22T00:00:00.000Z",
                "2022-08-28T23:59:59.999Z",
            ),
            (
                "month",
                "2022-08-01T00:00:00.000Z",
                "2022-08-31T23:59:59.999Z",
            ),
            (
                "quarter",
                "2022-07-01T00:00:00.000Z",
                "2022-09-30T23:59:59.999Z",
            ),
            (
                "year",
                "2022-01-01T00:00:00.000Z",
                "2022-12-31T23:59:59.999Z",
            ),
        ];

        for (granularity, date_min, date_max) in granularities {
            let sql = format!(
                r#"
                SELECT date_trunc('{}', "order_date") AS "uuid.order_date_tg", COUNT(*) AS "count"
                FROM "public"."KibanaSampleDataEcommerce"
                WHERE date_trunc('{}', "order_date") = date_trunc('{}', TO_TIMESTAMP('{}', 'yyyy-MM-dd HH24:mi:ss'))
                GROUP BY date_trunc('{}', "order_date")
                ORDER BY date_trunc('{}', "order_date") DESC NULLS LAST
                LIMIT 2500;
                "#,
                granularity, granularity, granularity, base_date, granularity, granularity,
            );
            let logical_plan = convert_select_to_query_plan(sql, DatabaseProtocol::PostgreSQL)
                .await
                .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(granularity.to_string()),
                        date_range: Some(json!(vec![date_min.to_string(), date_max.to_string()]))
                    }]),
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.order_date".to_string(),
                        "desc".to_string()
                    ]]),
                    limit: Some(2500),
                    offset: None,
                    filters: None,
                }
            )
        }
    }

    #[tokio::test]
    async fn test_quicksight_str_starts_with_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "customer_gender" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE LEFT("customer_gender", 1) = 'f'
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["f".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_str_ends_with_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "customer_gender" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE RIGHT("customer_gender", 2) = 'le'
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("endsWith".to_string()),
                    values: Some(vec!["le".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_str_contains_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "customer_gender" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE case
                when strpos(substring("customer_gender", 1), 'al') > 0
                    then strpos(substring("customer_gender", 1), 'al') + 1 - 1
                else 0
            end > 0
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["al".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_str_does_not_contain_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "customer_gender" AS "uuid.customer_gender",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "customer_gender" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                case
                    when strpos(substring("customer_gender", 1), 'al') > 0
                        then strpos(substring("customer_gender", 1), 'al') + 1 - 1 else 0
                    end = 0 AND
                "customer_gender" IS NOT NULL
            GROUP BY "customer_gender";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("notContains".to_string()),
                        values: Some(vec!["al".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None,
                    },
                ]),
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_num_starts_with_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "maxPrice" AS "uuid.maxPrice",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "maxPrice" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE LEFT(CAST("maxPrice" AS VARCHAR), 1) = '1'
            GROUP BY "maxPrice";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("startsWith".to_string()),
                    values: Some(vec!["1".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_num_ends_with_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "maxPrice" AS "uuid.maxPrice",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "maxPrice" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE RIGHT(CAST("maxPrice" AS VARCHAR), 2) = '23'
            GROUP BY "maxPrice";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("endsWith".to_string()),
                    values: Some(vec!["23".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_num_contains_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "maxPrice" AS "uuid.maxPrice",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "maxPrice" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE case
                when strpos(substring(CAST("maxPrice" AS VARCHAR), 1), '45') > 0
                    then strpos(substring(CAST("maxPrice" AS VARCHAR), 1), '45') + 1 - 1
                else 0
            end > 0
            GROUP BY "maxPrice";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["45".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_num_does_not_contain_query() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "maxPrice" AS "uuid.maxPrice",
                COUNT(*) AS "count",
                DENSE_RANK() OVER (ORDER BY "maxPrice" DESC NULLS LAST) AS "$RANK_1"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                case
                    when strpos(substring(CAST("maxPrice" AS VARCHAR), 1), '67') > 0
                        then strpos(substring(CAST("maxPrice" AS VARCHAR), 1), '67') + 1 - 1 else 0
                    end = 0 AND
                "maxPrice" IS NOT NULL
            GROUP BY "maxPrice";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("notContains".to_string()),
                        values: Some(vec!["67".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None,
                    },
                ]),
            }
        )
    }

    #[tokio::test]
    async fn test_tableau_filter_by_year() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                COUNT(*) AS "count",
                CAST(TRUNC(EXTRACT(YEAR FROM "KibanaSampleDataEcommerce"."order_date")) AS INTEGER) AS "yr:completedAt:ok"
            FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            WHERE (CAST(TRUNC(EXTRACT(YEAR FROM "KibanaSampleDataEcommerce"."order_date")) AS INTEGER) = 2019)
            GROUP BY 2
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: Some(json!(vec![
                        "2019-01-01".to_string(),
                        "2019-12-31".to_string(),
                    ])),
                },]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_date_trunc_column_less_or_eq() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT date_trunc('day', "order_date") AS "uuid.order_date_tg", COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "order_date" >= date_trunc('day', TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')) AND
                date_trunc('day', "order_date") <= date_trunc('day', LOCALTIMESTAMP + -5 * interval '1 DAY')
            GROUP BY date_trunc('day', "order_date")
            ORDER BY date_trunc('day', "order_date") DESC NULLS LAST
            LIMIT 2500;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let end_date = chrono::Utc::now().date().naive_utc() - chrono::Duration::days(5);
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2020-01-01T00:00:00.000Z".to_string(),
                        format!("{}T23:59:59.999Z", end_date),
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(2500),
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_quicksight_excluding_n_weeks() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT date_trunc('day', "order_date") AS "uuid.order_date_tg", COUNT(*) AS "count"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE
                "order_date" >= date_trunc('day', TO_TIMESTAMP('2020-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')) AND
                DATE_TRUNC(
                    'week',
                    "order_date"  + INTERVAL '1 day'
                ) - INTERVAL '1 day' <= DATE_TRUNC(
                    'week',
                    LOCALTIMESTAMP + 7 * -5 * interval '1 DAY' + INTERVAL '1 day'
                ) - INTERVAL '1 day'
            GROUP BY date_trunc('day', "order_date")
            ORDER BY date_trunc('day', "order_date") DESC NULLS LAST
            LIMIT 2500;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let now = chrono::Utc::now();
        let duration_sub_weeks = chrono::Duration::weeks(4);
        let duration_sub_days =
            chrono::Duration::days(now.weekday().num_days_from_sunday() as i64 + 1);
        let end_date = now.date().naive_utc() - duration_sub_weeks - duration_sub_days;
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2020-01-01T00:00:00.000Z".to_string(),
                        format!("{}T23:59:59.999Z", end_date),
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string()
                ]]),
                limit: Some(2500),
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_char_length() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT char_length("ta_1"."customer_gender") "cl"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1";
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_in_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE customer_gender IN ('female', 'male')"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;
        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string(), "male".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn test_thoughtspot_casts() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT CAST("ta_4"."ca_3" AS FLOAT8), CAST("ta_4"."ca_3" AS INT2), CAST("ta_4"."ca_3" AS BOOL)
            FROM (
                SELECT sum("ta_1"."count") AS "ca_3"
                FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            ) AS "ta_4"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_derived_dot_column() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select
                "_"."t1.agentCountApprox" as "agentCountApprox",
                "_"."a0" as "a0"
            from (
                select
                    sum(cast("rows"."t0.taxful_total_price" as decimal)) as "a0",
                    "rows"."t1.agentCountApprox" as "t1.agentCountApprox"
                from (
                    select
                        "$Outer"."t1.agentCountApprox",
                        "$Inner"."t0.taxful_total_price"
                    from (
                        select
                            "_"."agentCount" as "t1.agentCount",
                            "_"."agentCountApprox" as "t1.agentCountApprox",
                            "_"."__cubeJoinField" as "t1.__cubeJoinField"
                        from "public"."Logs" "_"
                    ) "$Outer"
                    left outer join (
                        select
                            "_"."taxful_total_price" as "t0.taxful_total_price",
                            "_"."count" as "t0.count",
                            "_"."__cubeJoinField" as "t0.__cubeJoinField"
                        from "public"."KibanaSampleDataEcommerce" "_"
                    ) "$Inner" on ("$Outer"."t1.__cubeJoinField" = "$Inner"."t0.__cubeJoinField")
                ) "rows"
                group by "t1.agentCountApprox"
            ) "_"
            where not "_"."a0" is null
            limit 1000001
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["Logs.agentCountApprox".to_string(),]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            },
        );
    }

    #[tokio::test]
    async fn test_thoughtspot_count_distinct_with_year_and_month() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                EXTRACT(MONTH FROM "ta_1"."order_date") "ca_1",
                CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS varchar) AS date) "ca_2",
                count(DISTINCT "ta_1"."countDistinct") "ca_3"
            FROM "database"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.countDistinct".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                        granularity: Some("month".to_owned()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                        granularity: Some("year".to_owned()),
                        date_range: None
                    }
                ]),
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                EXTRACT(MONTH FROM "ta_1"."order_date") "ca_1",
                CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS varchar) AS date) "ca_2",
                ((((EXTRACT(DAY FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) "ca_3",
                count(DISTINCT "ta_1"."countDistinct") "ca_4",
                count("ta_1"."count") "ca_5"
            FROM "database"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY
                "ca_1",
                "ca_2",
                "ca_3"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.countDistinct".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                        granularity: Some("month".to_owned()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                        granularity: Some("year".to_owned()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                        granularity: Some("day".to_owned()),
                        date_range: None
                    }
                ]),
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );

        let query = convert_sql_to_cube_query(
            &r#"
            SELECT
                EXTRACT(MONTH FROM "ta_1"."order_date") "ca_1",
                count(DISTINCT "ta_1"."countDistinct") "ca_2"
            FROM "database"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY
                "ca_1"
            ;"#
            .to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL).await,
        )
        .await;

        query.unwrap_err();
    }

    #[tokio::test]
    async fn test_cast_to_timestamp_timezone_utc() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "test_cast_to_timestamp_timezone_utc_1",
            execute_query(
                "select CAST ('2020-12-25 22:48:48.000' AS timestamptz)".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "test_cast_to_timestamp_timezone_utc_2",
            execute_query(
                "select CAST ('2020-12-25 22:48:48.000' AS timestamp)".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_join_with_distinct() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "test_join_with_distinct",
            execute_query(
                "WITH \"holistics__explore_60963d\" AS (
                    SELECT
                        1 AS \"dm_pu_ca_754b1e\",
                        2 AS \"pu_n_fddcd1\"
                    ), \"holistics__explore_edd38b\" AS (
                    SELECT DISTINCT
                        2 AS \"dm_pu_ca_754b1e\",
                        1 AS \"pu_n_fddcd1\"
                    )
                    SELECT
                        \"holistics__explore_60963d\".\"pu_n_fddcd1\" AS \"pu_n_fddcd1\",
                        \"holistics__explore_edd38b\".\"dm_pu_ca_754b1e\" AS \"dm_pu_ca_754b1e\"
                  FROM
                    \"holistics__explore_60963d\"
                    INNER JOIN \"holistics__explore_edd38b\" ON (\"holistics__explore_60963d\".\"dm_pu_ca_754b1e\" = \"holistics__explore_edd38b\".\"pu_n_fddcd1\");".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_string_field() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "test_extract_string_field",
            execute_query(
                "SELECT EXTRACT('YEAR' FROM CAST ('2020-12-25 22:48:48.000' AS timestamptz))"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_bool_and_or() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "test_bool_and_or",
            execute_query(
                "
                SELECT
                    bool_and(ttt) and_ttt, bool_or(ttt) or_ttt,
                    bool_and(ttf) and_ttf, bool_or(ttf) or_ttf,
                    bool_and(fff) and_fff, bool_or(fff) or_fff,
                    bool_and(ttn) and_ttn, bool_or(ttn) or_ttn,
                    bool_and(tfn) and_tfn, bool_or(tfn) or_tfn,
                    bool_and(ffn) and_ffn, bool_or(ffn) or_ffn,
                    bool_and(nnn) and_nnn, bool_or(nnn) or_nnn
                FROM (
                    SELECT true ttt, true  ttf, false fff, true ttn, true  tfn, false ffn, null::bool nnn
                    UNION ALL
                    SELECT true ttt, true  ttf, false fff, true ttn, false tfn, false ffn, null       nnn
                    UNION ALL
                    SELECT true ttt, false ttf, false fff, null ttn, null  tfn, null  ffn, null       nnn
                ) tbl
                "
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_select_is_null_is_not_null() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                (count IS NOT NULL) c,
                (customer_gender IS NULL) g
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_cast_split_aliasing() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select
                q1.datetrunc_8 datetrunc_8,
                q1.cast_timestamp_to_datetime_10 cast_timestamp_to_datetime_10,
                q1.v_11 v_11
            from (
                select
                    date_trunc('second', "order_date"::timestamptz) datetrunc_8,
                    "order_date"::timestamptz cast_timestamp_to_datetime_10,
                    1 v_11
                from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            ) q1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("second".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_sigma_str_contains() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(lower('el') in lower(customer_gender)) > 0) or
                (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("contains".to_string()),
                            values: Some(vec!["el".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_not_contains() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(lower('ale') in lower(customer_gender)) <= 0) or
                (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notContains".to_string()),
                            values: Some(vec!["ale".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_starts_with() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(lower('fe') in lower(customer_gender)) = 1) or
                (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("startsWith".to_string()),
                            values: Some(vec!["fe".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_not_starts_with() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(lower('fe') in lower(customer_gender)) <> 1)
                or (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notStartsWith".to_string()),
                            values: Some(vec!["fe".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_ends_with() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(reverse(lower('ale')) in reverse(lower(customer_gender))) = 1)
                or (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("endsWith".to_string()),
                            values: Some(vec!["ale".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_str_not_ends_with() -> Result<(), CubeError> {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            r#"
            SELECT customer_gender
            FROM KibanaSampleDataEcommerce
            WHERE
                ((position(reverse(lower('ale')) in reverse(lower(customer_gender))) <> 1)
                or (lower(customer_gender) is null))
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notEndsWith".to_string()),
                            values: Some(vec!["ale".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_union_with_cast_count_to_decimal() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "test_union_with_cast_count_to_decimal",
            execute_query(
                "select count(1) from (select 1 a) x union all select cast(null as decimal) order by 1;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_num_range() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT taxful_total_price
            FROM KibanaSampleDataEcommerce
            WHERE (
                (
                    (500 <= taxful_total_price) AND
                    (10000 >= taxful_total_price)
                ) OR
                (taxful_total_price IS NULL)
            )
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                                    ),
                                    operator: Some("gte".to_string()),
                                    values: Some(vec!["500".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                                    ),
                                    operator: Some("lte".to_string()),
                                    values: Some(vec!["10000".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                            ]),
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_sigma_num_not_in() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT taxful_total_price
            FROM KibanaSampleDataEcommerce
            WHERE (
                NOT (taxful_total_price IN (1, 1.1)) OR
                (taxful_total_price IS NULL)
            )
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("notEquals".to_string()),
                            values: Some(vec!["1".to_string(), "1.1".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_date_granularity_skyvia() {
        let supported_granularities = vec![
            // Day
            ["CAST(DATE_TRUNC('day', t.\"order_date\")::date AS varchar)", "day"],
            // Day of Month
            ["EXTRACT(DAY FROM t.\"order_date\")", "day"],
            // Month
            ["EXTRACT(YEAR FROM t.\"order_date\")::varchar || ',' || LPAD(EXTRACT(MONTH FROM t.\"order_date\")::varchar, 2, '0')", "month"],
            // Month of Year
            ["EXTRACT(MONTH FROM t.\"order_date\")", "month"],
            // Quarter
            ["EXTRACT(YEAR FROM t.\"order_date\")::varchar || ',' || EXTRACT(QUARTER FROM t.\"order_date\")::varchar", "quarter"],
            // Quarter of Year
            ["EXTRACT(QUARTER FROM t.\"order_date\")", "quarter"],
            // Year
            ["CAST(EXTRACT(YEAR FROM t.\"order_date\") AS varchar)", "year"],
        ];

        for [expr, expected_granularity] in &supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT {} AS expr1 FROM public.\"KibanaSampleDataEcommerce\" AS t",
                    expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(expected_granularity.to_string()),
                        date_range: None,
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            )
        }

        for [expr, expected_granularity] in supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "
                    SELECT
                        {} AS expr1,
                        SUM(t.\"count\") AS expr2
                    FROM public.\"KibanaSampleDataEcommerce\" AS t
                    GROUP BY {}
                    ",
                    expr, expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(expected_granularity.to_string()),
                        date_range: None,
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            )
        }
    }

    #[tokio::test]
    async fn test_sigma_literal_relation() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT l1.*
            FROM (
                SELECT
                    "customer_gender",
                    1 as error
                FROM "KibanaSampleDataEcommerce"
            ) as l1
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_join_three_cubes() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT *
            FROM KibanaSampleDataEcommerce 
            LEFT JOIN Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField) 
            LEFT JOIN NumberCube ON (NumberCube.__cubeJoinField = Logs.__cubeJoinField)
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    "KibanaSampleDataEcommerce.countDistinct".to_string(),
                    "Logs.agentCount".to_string(),
                    "Logs.agentCountApprox".to_string(),
                    "NumberCube.someNumber".to_string(),
                ]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.last_mod".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    "KibanaSampleDataEcommerce.has_subscription".to_string(),
                    "Logs.id".to_string(),
                    "Logs.read".to_string(),
                    "Logs.content".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_join_three_cubes_split() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT count(KibanaSampleDataEcommerce.count), Logs.read, NumberCube.someNumber, extract(MONTH FROM KibanaSampleDataEcommerce.order_date)
            FROM KibanaSampleDataEcommerce 
            LEFT JOIN Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField) 
            LEFT JOIN NumberCube ON (NumberCube.__cubeJoinField = Logs.__cubeJoinField)
            WHERE Logs.read
            GROUP BY 2,3,4
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "NumberCube.someNumber".to_string(),
                    "KibanaSampleDataEcommerce.count".to_string(),
                ]),
                dimensions: Some(vec!["Logs.read".to_string(),]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("Logs.read".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["true".to_string()]),
                    or: None,
                    and: None
                }])
            }
        )
    }

    #[tokio::test]
    async fn test_join_two_subqueries_with_filter_order_limit() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT count(KibanaSampleDataEcommerce.count), Logs.read
            FROM (SELECT * FROM KibanaSampleDataEcommerce where customer_gender is not null order by customer_gender) KibanaSampleDataEcommerce
            LEFT JOIN (SELECT read, __cubeJoinField FROM Logs) Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField) 
            WHERE Logs.read
            GROUP BY 2
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec!["Logs.read".to_string(),]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("Logs.read".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["true".to_string()]),
                        or: None,
                        and: None
                    }
                ])
            }
        )
    }

    #[tokio::test]
    async fn test_join_three_subqueries_with_filter_order_limit_and_split() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT count(Ecommerce.count), Logs.r, extract(MONTH FROM Ecommerce.order_date)
            FROM (SELECT * FROM KibanaSampleDataEcommerce where customer_gender is not null order by customer_gender) Ecommerce
            LEFT JOIN (SELECT read r, __cubeJoinField FROM Logs) Logs ON (Ecommerce.__cubeJoinField = Logs.__cubeJoinField)
            LEFT JOIN (SELECT someNumber, __cubeJoinField from NumberCube) NumberC ON (Logs.__cubeJoinField = NumberC.__cubeJoinField)
            WHERE Logs.r
            GROUP BY 2, 3
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec!["Logs.read".to_string(),]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("Logs.read".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["true".to_string()]),
                        or: None,
                        and: None
                    }
                ])
            }
        )
    }

    #[tokio::test]
    async fn test_join_subquery_and_table_with_filter_order_limit() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT count(KibanaSampleDataEcommerce.count), Logs.read
            FROM (SELECT * FROM KibanaSampleDataEcommerce where customer_gender is not null order by customer_gender) KibanaSampleDataEcommerce
            LEFT JOIN Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField) 
            WHERE Logs.read
            GROUP BY 2
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec!["Logs.read".to_string(),]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("Logs.read".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["true".to_string()]),
                        or: None,
                        and: None
                    }
                ])
            }
        )
    }

    #[tokio::test]
    async fn test_join_two_subqueries_and_table_with_filter_order_limit_and_split() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT count(Ecommerce.count), Logs.read, extract(MONTH FROM Ecommerce.order_date)
            FROM (SELECT * FROM KibanaSampleDataEcommerce where customer_gender is not null order by customer_gender) Ecommerce
            LEFT JOIN Logs ON (Ecommerce.__cubeJoinField = Logs.__cubeJoinField)
            LEFT JOIN (SELECT someNumber, __cubeJoinField from NumberCube) NumberC ON (Logs.__cubeJoinField = NumberC.__cubeJoinField)
            WHERE Logs.read
            GROUP BY 2, 3
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec!["Logs.read".to_string(),]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("Logs.read".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["true".to_string()]),
                        or: None,
                        and: None
                    }
                ])
            }
        )
    }

    #[tokio::test]
    async fn test_join_two_subqueries_filter_push_down() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT count(Ecommerce.count), Logs.r, Ecommerce.date
            FROM (SELECT __cubeJoinField, count, order_date date FROM KibanaSampleDataEcommerce where customer_gender = 'female') Ecommerce
            LEFT JOIN (select __cubeJoinField, read r from Logs) Logs ON (Ecommerce.__cubeJoinField = Logs.__cubeJoinField)
            WHERE (Logs.r IS NOT NULL) AND (Ecommerce.date BETWEEN timestamp with time zone '2022-06-13T12:30:00.000Z' AND timestamp with time zone '2022-06-29T12:30:00.000Z')
            GROUP BY 2, 3
            ORDER BY 1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![
                    "Logs.read".to_string(),
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2022-06-13T12:30:00.000Z".to_string(),
                        "2022-06-29T12:30:00.000Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["female".to_string()]),
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("Logs.read".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None
                    }
                ])
            }
        )
    }

    #[tokio::test]
    async fn test_join_cubes_on_wrong_field_error() {
        init_logger();

        let query = convert_sql_to_cube_query(
            &r#"
            SELECT *
            FROM KibanaSampleDataEcommerce 
            LEFT JOIN Logs ON (KibanaSampleDataEcommerce.has_subscription = Logs.read) 
            "#
            .to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL).await,
        )
        .await;

        assert_eq!(
            query.unwrap_err().message(),
            "Error during rewrite: Use __cubeJoinField to join Cubes. Please check logs for additional information.".to_string()
        )
    }

    #[tokio::test]
    async fn test_join_cubes_filter_from_wrong_side_error() {
        init_logger();

        let query = convert_sql_to_cube_query(
            &r#"
            SELECT count(KibanaSampleDataEcommerce.count), Logs.read
            FROM (SELECT * FROM KibanaSampleDataEcommerce) KibanaSampleDataEcommerce
            LEFT JOIN (SELECT read, __cubeJoinField FROM Logs where read order by read limit 10) Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField) 
            GROUP BY 2
            "#
            .to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL).await,
        )
        .await;

        assert_eq!(
            query.unwrap_err().message(),
            "Error during rewrite: Can not join Cubes. This is most likely due to one of the following reasons:\n\
            • one of the cubes contains a group by\n\
            • one of the cubes contains a measure\n\
            • the cube on the right contains a filter, sorting or limits\n\
            . Please check logs for additional information.".to_string()
        )
    }

    #[tokio::test]
    async fn test_join_cubes_with_aggr_error() {
        init_logger();

        let query = convert_sql_to_cube_query(
            &r#"
            SELECT *
            FROM (SELECT count(count), __cubeJoinField FROM KibanaSampleDataEcommerce group by 2) KibanaSampleDataEcommerce
            LEFT JOIN (SELECT read, __cubeJoinField FROM Logs) Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField) 
            "#
            .to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL).await,
        )
        .await;

        assert_eq!(
            query.unwrap_err().message(),
            "Error during rewrite: Can not join Cubes. This is most likely due to one of the following reasons:\n\
            • one of the cubes contains a group by\n\
            • one of the cubes contains a measure\n\
            • the cube on the right contains a filter, sorting or limits\n\
            . Please check logs for additional information.".to_string()
        )
    }

    #[tokio::test]
    async fn test_join_cubes_with_postprocessing() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT *
            FROM (SELECT count(count), __cubeJoinField, extract(MONTH from order_date) FROM KibanaSampleDataEcommerce group by 2, 3) KibanaSampleDataEcommerce
            LEFT JOIN (SELECT read, __cubeJoinField FROM Logs) Logs ON (KibanaSampleDataEcommerce.__cubeJoinField = Logs.__cubeJoinField) 
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scans = logical_plan
            .find_cube_scans()
            .iter()
            .map(|cube| cube.request.clone())
            .collect::<Vec<V1LoadRequestQuery>>();

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }),
            true
        );

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["Logs.read".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }),
            true
        )
    }

    #[tokio::test]
    async fn test_join_cubes_with_postprocessing_and_no_cubejoinfield() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT *
            FROM (SELECT count(count), extract(MONTH from order_date), taxful_total_price FROM KibanaSampleDataEcommerce group by 2, 3) KibanaSampleDataEcommerce
            LEFT JOIN (SELECT id, read FROM Logs) Logs ON (KibanaSampleDataEcommerce.taxful_total_price = Logs.id) 
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scans = logical_plan
            .find_cube_scans()
            .iter()
            .map(|cube| cube.request.clone())
            .collect::<Vec<V1LoadRequestQuery>>();

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }),
            true
        );

        assert_eq!(
            cube_scans.contains(&V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["Logs.id".to_string(), "Logs.read".to_string(),]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }),
            true
        )
    }

    #[tokio::test]
    async fn test_limit_push_down_recursion() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select cast_timestamp_to_datetime_6 "Order Date"
            from (
                select "order_date"::timestamptz cast_timestamp_to_datetime_6
                from (
                    select *
                    from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                    limit 10001
                ) q1
                limit 10001
            ) q3
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10001),
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_push_down_projection_literal() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT cg2 cg3, l2 l3
            FROM (
                SELECT cg1 cg2, l1 l2
                FROM (
                    SELECT cg cg1, l l1
                    FROM (
                        SELECT customer_gender cg, lit l
                        FROM (
                            SELECT customer_gender, 1 lit
                            FROM KibanaSampleDataEcommerce
                        ) k
                    ) k1
                ) k2
            ) k3
            ORDER BY cg3 ASC;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    // TODO: unignore once filter push down to projection is implemented
    #[tokio::test]
    #[ignore]
    async fn test_sigma_date_range() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select count_23 "__Row Count"
            from (
                select count(1) count_23
                from (
                    select *
                    from (
                        select "order_date"::timestamptz cast_timestamp_to_datetime_11
                        from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                    ) q1
                    where (
                        (
                            ('2022-11-01T00:00:00+00:00'::timestamptz <= cast_timestamp_to_datetime_11) and 
                            ('2022-11-15T23:59:59.999+00:00'::timestamptz >= cast_timestamp_to_datetime_11)
                        ) or
                        (cast_timestamp_to_datetime_11 is null)
                    )
                ) q2
                limit 1001
            ) q4
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2022-11-01T00:00:00.000Z".to_string(),
                        "2022-11-15T23:59:59.999Z".to_string(),
                    ]))
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                },])
            }
        )
    }

    // TODO: unignore once filter push down to projection is implemented
    #[tokio::test]
    #[ignore]
    async fn test_sigma_date_top_n() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select cast_timestamp_to_datetime_10 "Order Date"
            from (
                select cast_timestamp_to_datetime_10, isnotnull_11, Rank() over ( order by if_12 desc) "Rank_13" from (
                    select
                        cast_timestamp_to_datetime_10,
                        (cast_timestamp_to_datetime_10 is not null) isnotnull_11,
                        case
                            when (cast_timestamp_to_datetime_10 is not null) then cast_timestamp_to_datetime_10
                        end if_12
                    from (
                        select "order_date"::timestamptz cast_timestamp_to_datetime_10
                        from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                    ) q1
                    where (cast_timestamp_to_datetime_10 is not null)
                ) q2
            ) q3
            where
                case
                    when isnotnull_11 then ("Rank_13" <= 3)
                end
            limit 10001
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None
                },])
            }
        )
    }

    // TODO: unignore once filter push down to projection is implemented
    #[tokio::test]
    #[ignore]
    async fn test_sigma_date_in_list() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            select cast_timestamp_to_datetime_10 "Order Date"
            from (
                select "order_date"::timestamptz cast_timestamp_to_datetime_10
                from "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
            ) q1
            where cast_timestamp_to_datetime_10 in (
                '2019-01-17T15:25:48+00:00'::timestamptz,
                '2019-09-09T00:00:00+00:00'::timestamptz
            )
            limit 10001
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10001),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec![
                        "2019-01-17T15:25:48.000Z".to_string(),
                        "2019-09-09T00:00:00.000Z".to_string(),
                    ]),
                    or: None,
                    and: None
                },])
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_approximate_count_distinct() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT approximate count(distinct "ta_1"."customer_gender") "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_count_distinct_text() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT count(distinct "ta_1"."customer_gender") "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_like_with_escape() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE LOWER("ta_1"."customer_gender") LIKE ('%' || replace(
                replace(
                    replace(
                        'male',
                        '!',
                        '!!'
                    ),
                    '%',
                    '!%'
                ),
                '_',
                '!_'
            ) || '%') ESCAPE '!'
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(1000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None
                }])
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE NOT(LOWER("ta_1"."customer_gender") LIKE (replace(
                replace(
                    replace(
                        'test',
                        '!',
                        '!!'
                    ),
                    '%',
                    '!%'
                ),
                '_',
                '!_'
            ) || '%') ESCAPE '!')
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(1000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notStartsWith".to_string()),
                    values: Some(vec!["test".to_string()]),
                    or: None,
                    and: None
                }])
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE NOT(LOWER("ta_1"."customer_gender") LIKE ('%' || replace(
                replace(
                    replace(
                        'known',
                        '!',
                        '!!'
                    ),
                    '%',
                    '!%'
                ),
                '_',
                '!_'
            )) ESCAPE '!')
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ]]),
                limit: Some(1000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notEndsWith".to_string()),
                    values: Some(vec!["known".to_string()]),
                    or: None,
                    and: None
                }])
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_exclude_single_filter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE (
                LOWER("ta_1"."customer_gender") <> 'male'
                OR "ta_1"."customer_gender" IS NULL
            )
            GROUP BY "ca_1"
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("notStartsWith".to_string()),
                                    values: Some(vec!["male".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("notEndsWith".to_string()),
                                    values: Some(vec!["male".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                            ])
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_exclude_multiple_filter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT "ta_1"."customer_gender" "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            WHERE (
            NOT(LOWER("ta_1"."customer_gender") IN (
                'male', 'female'
            ))
            OR NOT("ta_1"."customer_gender" IS NOT NULL)
            )
            GROUP BY "ca_1"
            LIMIT 1000
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notEquals".to_string()),
                            values: Some(vec!["male".to_string(), "female".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_segment_post_aggr() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT is_male is_male, SUBSTRING(customer_gender FROM 1 FOR 1234) gender
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_select_from_cube_case() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE
                    WHEN notes IS NULL THEN customer_gender
                    ELSE notes
                END customer_info
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_select_from_cube_case_with_group_by() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE
                    WHEN notes IS NULL THEN customer_gender
                    ELSE notes
                END customer_info,
                COUNT(*) count
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.notes".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_select_from_cube_case_with_expr() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE customer_gender
                    WHEN 'f' THEN 'Female'
                    WHEN 'm' THEN 'Male'
                    ELSE CASE
                        WHEN notes IS NULL THEN 'Other'
                        ELSE notes
                    END
                END customer_gender
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_select_from_cube_case_with_expr_and_group_by() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE customer_gender
                    WHEN 'f' THEN 'Female'
                    WHEN 'm' THEN 'Male'
                    ELSE CASE
                        WHEN notes IS NULL THEN 'Other'
                        ELSE notes
                    END
                END customer_gender,
                COUNT(*) count
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_select_case_is_null() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CASE
                    WHEN "ta_1"."customer_gender" IS NULL
                    THEN "ta_1"."notes"
                    ELSE "ta_1"."customer_gender"
                END "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_select_case_when_true() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT CASE
                WHEN TRUE THEN "ta_1"."customer_gender"
                ELSE CASE
                    WHEN "ta_1"."customer_gender" IS NOT NULL THEN "ta_1"."customer_gender"
                    ELSE "ta_1"."notes"
                END
            END "ca_1"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_lower() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT "ta_1"."notes" "ca_1"
                FROM KibanaSampleDataEcommerce "ta_1"
                WHERE (
                    NOT(LOWER("ta_1"."customer_gender") IN (
                        'f', 'm'
                    ))
                    OR NOT("ta_1"."customer_gender" IS NOT NULL)
                )
                GROUP BY "ca_1"
            )
            SELECT count(DISTINCT "ta_2"."ca_1") "ca_2"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.notes".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notEquals".to_string()),
                            values: Some(vec!["f".to_string(), "m".to_string()]),
                            or: None,
                            and: None
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None
                        }),
                    ]),
                    and: None
                }])
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_having_cast_float8() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT "ta_1"."customer_gender" "ca_1"
                FROM "KibanaSampleDataEcommerce" "ta_1"
                GROUP BY "ca_1"
                HAVING CAST(COUNT("ta_1"."count") AS FLOAT8) < 10.0
            )
            SELECT count(DISTINCT "ta_2"."ca_1") "ca_2"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("lt".to_string()),
                    values: Some(vec!["10".to_string()]),
                    or: None,
                    and: None
                }])
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_avg_cast_arg() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT 
                "ta_1"."customer_gender" "ca_1", 
                avg(CAST("ta_1"."avgPrice" AS FLOAT8)) "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY 
                "ca_2" DESC, 
                "ca_1" ASC
            LIMIT 2
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.avgPrice".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_concat() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    ("ta_1"."customer_gender" || 'aa') "ca_1"
                FROM KibanaSampleDataEcommerce "ta_1"
                GROUP BY "ca_1"
            )
            SELECT "ca_1" "ca_2"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_equals() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT (EXTRACT(DAY FROM "ta_1"."order_date") = 15.0) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_month_of_quarter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT 
                (((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) % 3) + 1) "ca_1", 
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_lt_extract() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT (EXTRACT(MONTH FROM "ta_1"."order_date") < (EXTRACT(MONTH FROM "ta_1"."last_mod") + 1.0)) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("month".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.last_mod".to_string(),
                        granularity: Some("month".to_string()),
                        date_range: None
                    },
                ]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_select_eq_or() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT ((
                LOWER("ta_1"."customer_gender") = 'male'
                OR LOWER("ta_1"."customer_gender") = 'female'
            )) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT ((
                LOWER("ta_1"."customer_gender") = 'female'
                OR LOWER("ta_1"."notes") = 'test'
            )) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.notes".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT ((
                "ta_1"."order_date" = DATE '1994-05-01'
                OR "ta_1"."order_date" = DATE '1996-05-03'
            )) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
                JOIN Logs "ta_2"
                    ON "ta_1"."__cubeJoinField" = "ta_2"."__cubeJoinField"
            WHERE LOWER("ta_2"."content") = 'test'
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("Logs.content".to_string()),
                        operator: Some("startsWith".to_string()),
                        values: Some(vec!["test".to_string()]),
                        or: None,
                        and: None
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("Logs.content".to_string()),
                        operator: Some("endsWith".to_string()),
                        values: Some(vec!["test".to_string()]),
                        or: None,
                        and: None
                    }
                ]),
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_column_comparison() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT ("ta_1"."taxful_total_price" > 10.0) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_date_trunc_month_year() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT DATE_TRUNC('month', DATE_TRUNC('month', "ta_1"."order_date")) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string()
                ]]),
                limit: None,
                offset: None,
                filters: None,
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT DATE_TRUNC('month', CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS CHARACTER VARYING) AS timestamp)) "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY "ca_1" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string()
                ]]),
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_day_in_quarter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                (DATEDIFF(day, DATEADD(month, CAST((((((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) % 3) + 1) - 1) * -1) AS int), CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + EXTRACT(MONTH FROM "ta_1"."order_date")) * 100) + 1) AS varchar) AS date)), "ta_1"."order_date") + 1) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_date_trunc_offset() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('qtr', DATEADD(day, CAST(2 AS int), "ta_1"."order_date")) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('qtr', DATEADD(week, CAST(5 AS int), "ta_1"."order_date")) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    // Intentional; Cube DateTrunc by week + post-processing DATEADD
                    // + post-processing DateTrunc will yield incorrect results
                    granularity: Some("day".to_string()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_date_offset_with_filter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    "ta_1"."customer_gender" "ca_1",
                    CAST(DATEADD(day, CAST(2 AS int), "ta_1"."order_date") AS date) "ca_2",
                    DATEADD(second, CAST(2000 AS int), "ta_1"."order_date") "ca_3"
                FROM KibanaSampleDataEcommerce "ta_1"
                WHERE DATEADD(day, CAST(2 AS int), "ta_1"."order_date") < DATE '2014-06-02'
                GROUP BY
                    "ca_1",
                    "ca_2",
                    "ca_3"
            )
            SELECT
                min("ta_2"."ca_2") "ca_3",
                max("ta_2"."ca_2") "ca_4"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("beforeDate".to_string()),
                    values: Some(vec!["2014-05-30T23:59:59.999Z".to_string()]),
                    or: None,
                    and: None
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_min_max_date_offset() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                min(DATEADD(day, CAST(2 AS int), "ta_1"."order_date")) "ca_1",
                max(DATEADD(day, CAST(2 AS int), "ta_1"."order_date")) "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_week_num_in_month() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                FLOOR(((EXTRACT(DAY FROM DATEADD(day, CAST((4 - (((DATEDIFF(day, DATE '1970-01-01', "ta_1"."order_date") + 3) % 7) + 1)) AS int), "ta_1"."order_date")) + 6) / NULLIF(CAST(7 AS FLOAT8),0.0))) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_binary_sum_columns() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    ("ta_2"."taxful_total_price" + "ta_1"."id") "ca_1",
                    CASE
                        WHEN sum("ta_2"."count") IS NOT NULL THEN sum("ta_2"."count")
                        ELSE 0
                    END "ca_2"
                FROM KibanaSampleDataEcommerce "ta_2"
                JOIN Logs "ta_1"
                    ON "ta_2"."__cubeJoinField" = "ta_1"."__cubeJoinField"
                GROUP BY "ca_1"
            )
            SELECT count(DISTINCT "ta_3"."ca_1") "ca_3"
            FROM "qt_0" "ta_3"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    "Logs.id".to_string(),
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_date_trunc_qtr_with_post_processing() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('qtr', DATEADD(minute, CAST(2 AS int), "ta_1"."order_date")) "ca_1",
                DATE_TRUNC('qtr', "ta_1"."order_date") "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("minute".to_string()),
                        date_range: None
                    },
                    V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some("quarter".to_string()),
                        date_range: None
                    },
                ]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_split_date_trunc_qtr() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
             SELECT 
                 TO_CHAR("ta_1"."order_date", 'Mon') "ca_1",
                 DATE_TRUNC('qtr', "ta_1"."order_date") "ca_2"
             FROM KibanaSampleDataEcommerce "ta_1"
             GROUP BY 
                 "ca_1", 
                 "ca_2"
             "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("quarter".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_quarter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT
                    CEIL((EXTRACT(MONTH FROM "ta_1"."order_date") / NULLIF(3.0,0.0))) "ca_1", 
                    CASE
                        WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                        ELSE 0
                    END "ca_2"
                FROM KibanaSampleDataEcommerce "ta_1"
                GROUP BY "ca_1"
            )
            SELECT count(DISTINCT "ta_2"."ca_1") "ca_3"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_where_not_or() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT "ta_1"."customer_gender" "ca_1"
                FROM KibanaSampleDataEcommerce "ta_1"
                WHERE NOT((
                    "ta_1"."customer_gender" IS NULL
                    OR LOWER("ta_1"."customer_gender") IN ('unknown')
                ))
                GROUP BY "ca_1"
            )
            SELECT count(DISTINCT "ta_2"."ca_1") "ca_2"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("notEquals".to_string()),
                        values: Some(vec!["unknown".to_string()]),
                        or: None,
                        and: None,
                    },
                ])
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_where_binary_in_true_false() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                ((
                    LOWER("ta_1"."customer_gender") = 'female'
                    OR LOWER("ta_1"."customer_gender") = 'male'
                )) "ca_1",
                CASE
                    WHEN sum("ta_1"."count") IS NOT NULL THEN sum("ta_1"."count")
                    ELSE 0
                END "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            WHERE ((
                LOWER("ta_1"."customer_gender") = 'female'
                OR LOWER("ta_1"."customer_gender") = 'male'
            )) IN (
                TRUE, FALSE
            )
            GROUP BY "ca_1"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: None,
                        operator: None,
                        values: None,
                        or: Some(vec![
                            json!(V1LoadRequestQueryFilterItem {
                                member: None,
                                operator: None,
                                values: None,
                                or: None,
                                and: Some(vec![
                                    json!(V1LoadRequestQueryFilterItem {
                                        member: Some(
                                            "KibanaSampleDataEcommerce.customer_gender".to_string()
                                        ),
                                        operator: Some("startsWith".to_string()),
                                        values: Some(vec!["female".to_string()]),
                                        or: None,
                                        and: None,
                                    }),
                                    json!(V1LoadRequestQueryFilterItem {
                                        member: Some(
                                            "KibanaSampleDataEcommerce.customer_gender".to_string()
                                        ),
                                        operator: Some("endsWith".to_string()),
                                        values: Some(vec!["female".to_string()]),
                                        or: None,
                                        and: None,
                                    }),
                                ]),
                            }),
                            json!(V1LoadRequestQueryFilterItem {
                                member: None,
                                operator: None,
                                values: None,
                                or: None,
                                and: Some(vec![
                                    json!(V1LoadRequestQueryFilterItem {
                                        member: Some(
                                            "KibanaSampleDataEcommerce.customer_gender".to_string()
                                        ),
                                        operator: Some("startsWith".to_string()),
                                        values: Some(vec!["male".to_string()]),
                                        or: None,
                                        and: None,
                                    }),
                                    json!(V1LoadRequestQueryFilterItem {
                                        member: Some(
                                            "KibanaSampleDataEcommerce.customer_gender".to_string()
                                        ),
                                        operator: Some("endsWith".to_string()),
                                        values: Some(vec!["male".to_string()]),
                                        or: None,
                                        and: None,
                                    }),
                                ]),
                            }),
                        ]),
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("set".to_string()),
                        values: None,
                        or: None,
                        and: None,
                    },
                ])
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_left_right() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" "ca_1",
                LEFT("ta_1"."customer_gender", 2) "ca_2",
                RIGHT("ta_1"."customer_gender", 2) "ca_3"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY
                "ca_1",
                "ca_2",
                "ca_3"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC,
                "ca_3" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_nullif_measure_dimension() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                NULLIF(CAST("ta_1"."taxful_total_price" AS FLOAT8), 0.0) "ca_1",
                NULLIF(CAST("ta_1"."count" AS FLOAT8), 0.0) "ca_2"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            ORDER BY
                "ca_1" ASC,
                "ca_2" ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_datediff_to_date() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            WITH "qt_0" AS (
                SELECT 
                    DATEDIFF(day, min("ta_1"."order_date"), TO_DATE('2020-02-20','YYYY-MM-DD')) "ca_1", 
                    min("ta_1"."order_date") "ca_2"
                FROM KibanaSampleDataEcommerce "ta_1"
                HAVING DATEDIFF(day, min("ta_1"."order_date"), TO_DATE('2020-02-20','YYYY-MM-DD')) > 4
            )
            SELECT DATEDIFF(day, min("ta_2"."ca_2"), TO_DATE('2020-02-20','YYYY-MM-DD')) "ca_3"
            FROM "qt_0" "ta_2"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_filter_date_trunc_column_with_literal() {
        init_logger();

        let test_data = vec![
            // (operator, literal date, filter operator, filter value)
            (">=", "2020-03-25", "afterDate", "2020-04-01T00:00:00.000Z"),
            (">=", "2020-04-01", "afterDate", "2020-04-01T00:00:00.000Z"),
            (">=", "2020-04-10", "afterDate", "2020-05-01T00:00:00.000Z"),
            ("<=", "2020-03-25", "beforeDate", "2020-03-31T23:59:59.999Z"),
            ("<=", "2020-04-01", "beforeDate", "2020-04-30T23:59:59.999Z"),
            ("<=", "2020-04-10", "beforeDate", "2020-04-30T23:59:59.999Z"),
            (">", "2020-03-25", "afterDate", "2020-04-01T00:00:00.000Z"),
            (">", "2020-04-01", "afterDate", "2020-05-01T00:00:00.000Z"),
            (">", "2020-04-10", "afterDate", "2020-05-01T00:00:00.000Z"),
            ("<", "2020-03-25", "beforeDate", "2020-03-31T23:59:59.999Z"),
            ("<", "2020-04-01", "beforeDate", "2020-03-31T23:59:59.999Z"),
            ("<", "2020-04-10", "beforeDate", "2020-04-30T23:59:59.999Z"),
        ];

        for (operator, literal_date, filter_operator, filter_value) in test_data {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "
                    SELECT
                        \"ta_1\".\"order_date\" \"ca_1\"
                    FROM KibanaSampleDataEcommerce \"ta_1\"
                    WHERE DATE_TRUNC('MONTH', CAST(\"ta_1\".\"order_date\" as TIMESTAMP)) {} to_date('{}', 'yyyy-MM-dd')
                    ",
                    operator, literal_date,
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: Some(vec![V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some(filter_operator.to_string()),
                        values: Some(vec![filter_value.to_string()]),
                        or: None,
                        and: None
                    }]),
                }
            );
        }
    }

    #[tokio::test]
    async fn test_thoughtspot_double_date_trunc_with_cast() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('MONTH', CAST(DATE_TRUNC('MONTH', CAST("ta_1"."order_date" as TIMESTAMP)) as TIMESTAMP)) AS "ca_1"
            FROM KibanaSampleDataEcommerce "ta_1"
            GROUP BY "ca_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_metabase_substring_postaggr() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                avgPrice avgPrice,
                countDistinct countDistinct,
                customer_gender customer_gender,
                SUBSTRING(customer_gender FROM 1 FOR 1234) substring_400
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    "KibanaSampleDataEcommerce.countDistinct".to_string(),
                ]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_reaggregate_without_aliases() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                EXTRACT(YEAR FROM order_date),
                CHAR_LENGTH(customer_gender),
                count
            FROM KibanaSampleDataEcommerce
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_extract_year_to_date_trunc() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."order_date") * 100) + 1) * 100) + 1) AS varchar) AS date) "ca_1",
                count(DISTINCT "ta_1"."countDistinct") "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
            GROUP BY "ca_1"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.countDistinct".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_sigma_row_count_cross_join() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                count_25 "__Row Count",
                datetrunc_8 "Second of Order Date",
                cast_timestamp_to_datetime_10 "Order Date",
                v_11 "Target Const"
            FROM (
                SELECT
                    q1.datetrunc_8 datetrunc_8,
                    q1.cast_timestamp_to_datetime_10 cast_timestamp_to_datetime_10,
                    q1.v_11 v_11,
                    q2.count_25 count_25
                FROM (
                    SELECT
                        date_trunc('second', "order_date"::timestamptz) datetrunc_8,
                        "order_date"::timestamptz cast_timestamp_to_datetime_10,
                        1 v_11
                    FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                ) q1
                CROSS JOIN (
                    SELECT count(1) count_25
                    FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
                ) q2
                ORDER BY q1.datetrunc_8 ASC
                LIMIT 10001
            ) q5;
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let cube_scans = logical_plan
            .find_cube_scans()
            .iter()
            .map(|cube| cube.request.clone())
            .collect::<Vec<V1LoadRequestQuery>>();

        assert!(cube_scans.contains(&V1LoadRequestQuery {
            measures: Some(vec![]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
            segments: Some(vec![]),
            time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                granularity: Some("second".to_string()),
                date_range: None,
            }]),
            // Order and Limit and nearly pushed to CubeScan but the Projection
            // before TableScan is a post-processing projection.
            // Splitting such projections into two may be a good idea.
            order: None,
            limit: None,
            offset: None,
            filters: None,
        }))
    }

    #[tokio::test]
    async fn test_metabase_cast_column_to_date() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                CAST("public"."KibanaSampleDataEcommerce"."order_date" AS DATE) AS "order_date",
                avg("public"."KibanaSampleDataEcommerce"."avgPrice") AS "avgPrice"
            FROM "public"."KibanaSampleDataEcommerce"
            WHERE (
                "public"."KibanaSampleDataEcommerce"."order_date" >= CAST((now() + (INTERVAL '-30 day')) AS DATE)
                AND "public"."KibanaSampleDataEcommerce"."order_date" < CAST(now() AS DATE)
                AND (
                    "public"."KibanaSampleDataEcommerce"."notes" = 'note1'
                    OR "public"."KibanaSampleDataEcommerce"."notes" = 'note2'
                    OR "public"."KibanaSampleDataEcommerce"."notes" = 'note3'
                )
            )
            GROUP BY CAST("public"."KibanaSampleDataEcommerce"."order_date" AS DATE)
            ORDER BY CAST("public"."KibanaSampleDataEcommerce"."order_date" AS DATE) ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        let end_date = chrono::Utc::now().date().naive_utc() - chrono::Duration::days(1);
        let start_date = end_date - chrono::Duration::days(29);
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.avgPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        format!("{}T00:00:00.000Z", start_date),
                        format!("{}T23:59:59.999Z", end_date),
                    ]))
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["note1".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["note2".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["note3".to_string()]),
                            or: None,
                            and: None,
                        }),
                    ]),
                    and: None
                }])
            }
        )
    }

    #[tokio::test]
    async fn test_date_trunc_column_equals_literal() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                avg("avgPrice") AS "avgPrice"
            FROM public."KibanaSampleDataEcommerce"
            WHERE
                DATE_TRUNC('week', "order_date") = str_to_date('2022-11-14 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.avgPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2022-11-14T00:00:00.000Z".to_string(),
                        "2022-11-20T23:59:59.999Z".to_string(),
                    ]))
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_psqlodbc_null() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "psqlodbc_null",
            execute_query(
                "select NULL, NULL, NULL".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_simple_wrapper() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MIN(avgPrice) FROM (SELECT avgPrice FROM KibanaSampleDataEcommerce) a"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.avgPrice".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_date_trunc_year() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" AS "ca_1",
                CAST(EXTRACT(YEAR FROM "ta_1"."order_date") || '-' || 1 || '-01' AS DATE) AS "ca_2",
                COALESCE(sum("ta_1"."count"), 0) AS "ca_3"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            WHERE (
                LOWER("ta_1"."customer_gender") = 'none'
                AND LOWER("ta_1"."notes") = ''
            )
            GROUP BY
                "ca_1",
                "ca_2"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("startsWith".to_string()),
                        values: Some(vec!["none".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("endsWith".to_string()),
                        values: Some(vec!["none".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                        operator: Some("startsWith".to_string()),
                        values: Some(vec!["".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                        operator: Some("endsWith".to_string()),
                        values: Some(vec!["".to_string()]),
                        or: None,
                        and: None,
                    },
                ]),
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_date_trunc_quarter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT 
                "ta_1"."customer_gender" AS "ca_1", 
                CAST(
                    EXTRACT(YEAR FROM "ta_1"."order_date")
                    || '-'
                    || ((FLOOR(((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) / NULLIF(3,0))) * 3) + 1)
                    || '-01'
                    AS DATE
                ) AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY 
                "ca_1", 
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: Some(1000),
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_date_trunc_month() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                DATE_TRUNC('MONTH', CAST("ta_1"."order_date" AS date)) AS "ca_1",
                count(DISTINCT "ta_1"."countDistinct") AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY "ca_1"
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.countDistinct".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_extract_month_of_quarter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT
                "ta_1"."customer_gender" AS "ca_1", 
                (MOD(CAST((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) AS numeric), 3) + 1) AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY
                "ca_1",
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_extract_day_of_year() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT 
                (CAST("ta_1"."order_date" AS date) - CAST((CAST(EXTRACT(YEAR FROM "ta_1"."order_date") || '-' || EXTRACT(MONTH FROM "ta_1"."order_date") || '-01' AS DATE) + ((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) * -1) * INTERVAL '1 month') AS date) + 1) AS "ca_1", 
                "ta_1"."customer_gender" AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY 
                "ca_1", 
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_extract_day_of_quarter() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT 
                (CAST("ta_1"."order_date" AS date) - CAST((CAST(EXTRACT(YEAR FROM "ta_1"."order_date") || '-' || EXTRACT(MONTH FROM "ta_1"."order_date") || '-01' AS DATE) + (((MOD(CAST((EXTRACT(MONTH FROM "ta_1"."order_date") - 1) AS numeric), 3) + 1) - 1) * -1) * INTERVAL '1 month') AS date) + 1) AS "ca_1", 
                "ta_1"."customer_gender" AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY 
                "ca_1", 
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_thoughtspot_pg_extract_day_of_week() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            r#"
            SELECT 
                (MOD(CAST((CAST("ta_1"."order_date" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1) AS "ca_1", 
                "ta_1"."customer_gender" AS "ca_2"
            FROM "db"."public"."KibanaSampleDataEcommerce" AS "ta_1"
            GROUP BY 
                "ca_1", 
                "ca_2"
            LIMIT 1000
            ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn test_langchain_pgcatalog_schema() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "langchain_pgcatalog_schema",
            execute_query(
                "
                SELECT pg_catalog.pg_class.relname
                FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
                WHERE
                    pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p'])
                    AND pg_catalog.pg_class.relpersistence != 't'
                    AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid)
                    AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
                ;".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_langchain_array_agg_order_by() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "langchain_array_agg_order_by",
            execute_query(
                "
                SELECT
                    pg_catalog.pg_type.typname AS name,
                    pg_catalog.pg_type_is_visible(pg_catalog.pg_type.oid) AS visible,
                    pg_catalog.pg_namespace.nspname AS schema, lbl_agg.labels AS labels 
                FROM pg_catalog.pg_type
                JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_type.typnamespace
                LEFT OUTER JOIN (
                    SELECT
                        pg_catalog.pg_enum.enumtypid AS enumtypid,
                        array_agg(pg_catalog.pg_enum.enumlabel ORDER BY pg_catalog.pg_enum.enumsortorder) AS labels 
                    FROM pg_catalog.pg_enum
                    GROUP BY pg_catalog.pg_enum.enumtypid
                ) AS lbl_agg ON pg_catalog.pg_type.oid = lbl_agg.enumtypid 
                WHERE pg_catalog.pg_type.typtype = 'e'
                ORDER BY
                    pg_catalog.pg_namespace.nspname,
                    pg_catalog.pg_type.typname
                ;".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_langchain_pg_get_indexdef_and_in_realiasing() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "langchain_pg_get_indexdef_and_in_realiasing",
            execute_query(
                "
                SELECT
                    pg_catalog.pg_index.indrelid,
                    cls_idx.relname AS relname_index,
                    pg_catalog.pg_index.indisunique,
                    pg_catalog.pg_constraint.conrelid IS NOT NULL AS has_constraint,
                    pg_catalog.pg_index.indoption,
                    cls_idx.reloptions,
                    pg_catalog.pg_am.amname,
                    CASE
                        WHEN (pg_catalog.pg_index.indpred IS NOT NULL)
                            THEN pg_catalog.pg_get_expr(pg_catalog.pg_index.indpred, pg_catalog.pg_index.indrelid)
                    END AS filter_definition,
                    pg_catalog.pg_index.indnkeyatts,
                    idx_cols.elements,
                    idx_cols.elements_is_expr 
                FROM pg_catalog.pg_index
                JOIN pg_catalog.pg_class AS cls_idx ON pg_catalog.pg_index.indexrelid = cls_idx.oid
                JOIN pg_catalog.pg_am ON cls_idx.relam = pg_catalog.pg_am.oid
                LEFT OUTER JOIN (
                    SELECT
                        idx_attr.indexrelid AS indexrelid,
                        min(idx_attr.indrelid) AS min_1,
                        array_agg(idx_attr.element ORDER BY idx_attr.ord) AS elements,
                        array_agg(idx_attr.is_expr ORDER BY idx_attr.ord) AS elements_is_expr 
                    FROM (
                        SELECT
                            idx.indexrelid AS indexrelid,
                            idx.indrelid AS indrelid,
                            idx.ord AS ord,
                            CASE
                                WHEN (idx.attnum = 0)
                                    THEN pg_catalog.pg_get_indexdef(idx.indexrelid, idx.ord + 1, true)
                                ELSE CAST(pg_catalog.pg_attribute.attname AS TEXT)
                            END AS element,
                            idx.attnum = 0 AS is_expr 
                        FROM (
                            SELECT
                                pg_catalog.pg_index.indexrelid AS indexrelid,
                                pg_catalog.pg_index.indrelid AS indrelid,
                                unnest(pg_catalog.pg_index.indkey) AS attnum,
                                generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord 
                            FROM pg_catalog.pg_index 
                            WHERE
                                NOT pg_catalog.pg_index.indisprimary
                                AND pg_catalog.pg_index.indrelid IN (18000)
                        ) AS idx
                        LEFT OUTER JOIN pg_catalog.pg_attribute ON
                            pg_catalog.pg_attribute.attnum = idx.attnum
                            AND pg_catalog.pg_attribute.attrelid = idx.indrelid 
                        WHERE idx.indrelid IN (18000)
                    ) AS idx_attr
                    GROUP BY idx_attr.indexrelid
                ) AS idx_cols ON pg_catalog.pg_index.indexrelid = idx_cols.indexrelid
                LEFT OUTER JOIN pg_catalog.pg_constraint ON
                    pg_catalog.pg_index.indrelid = pg_catalog.pg_constraint.conrelid
                    AND pg_catalog.pg_index.indexrelid = pg_catalog.pg_constraint.conindid
                    AND pg_catalog.pg_constraint.contype = ANY (ARRAY['p', 'u', 'x']) 
                WHERE
                    pg_catalog.pg_index.indrelid IN (18000)
                    AND NOT pg_catalog.pg_index.indisprimary
                ORDER BY
                    pg_catalog.pg_index.indrelid,
                    cls_idx.relname
                ;".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }
}
