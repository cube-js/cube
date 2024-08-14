use crate::compile::{qtrace::Qtrace, CommandCompletion, DatabaseProtocol, QueryPlan, StatusFlags};
use sqlparser::ast;
use std::{
    backtrace::Backtrace, collections::HashMap, future::Future, pin::Pin, sync::Arc,
    time::SystemTime,
};

use crate::{
    compile::{
        engine::{
            df::{
                optimizers::{FilterPushDown, LimitPushDown, SortPushDown},
                planner::CubeQueryPlanner,
                scan::CubeScanNode,
                wrapper::CubeScanWrapperNode,
            },
            udf::*,
            CubeContext, VariablesProvider,
        },
        error::{CompilationError, CompilationResult},
        parser::parse_sql_to_statement,
        rewrite::{
            analysis::LogicalPlanAnalysis,
            converter::{LogicalPlanToLanguageContext, LogicalPlanToLanguageConverter},
            rewriter::Rewriter,
        },
    },
    sql::{
        database_variables::{DatabaseVariable, DatabaseVariablesToUpdate},
        dataframe,
        statement::{
            ApproximateCountDistinctVisitor, CastReplacer, RedshiftDatePartReplacer,
            SensitiveDataSanitizer, ToTimestampReplacer, UdfWildcardArgReplacer,
        },
        ColumnFlags, ColumnType, Session, SessionManager, SessionState,
    },
    transport::{LoadRequestMeta, MetaContext, SpanId, TransportService},
    CubeErrorCauseType,
};
use datafusion::{
    execution::context::{
        default_session_builder, SessionConfig as DFSessionConfig,
        SessionContext as DFSessionContext,
    },
    logical_plan::{
        plan::{Analyze, Explain, Extension, ToStringifiedPlan},
        LogicalPlan, PlanType, PlanVisitor, ToDFSchema,
    },
    optimizer::{
        optimizer::{OptimizerConfig, OptimizerRule},
        projection_drop_out::ProjectionDropOut,
        utils::from_plan,
    },
    physical_plan::{planner::DefaultPhysicalPlanner, ExecutionPlan, RecordBatchStream},
    prelude::*,
    scalar::ScalarValue,
    sql::{parser::Statement as DFStatement, planner::SqlToRel},
    variable::VarType,
};
use itertools::Itertools;
use sqlparser::ast::{escape_single_quote_string, ObjectName};

#[derive(Clone)]
pub struct QueryPlanner {
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
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
        flat_list: bool,
    ) -> CompilationResult<QueryPlan> {
        let planning_start = SystemTime::now();
        if let Some(span_id) = span_id.as_ref() {
            if let Some(auth_context) = self.state.auth_context() {
                self.session_manager
                    .server
                    .transport
                    .log_load_state(
                        Some(span_id.clone()),
                        auth_context,
                        self.state.get_load_request_meta(),
                        "SQL API Query Planning".to_string(),
                        serde_json::json!({
                            "query": span_id.query_key.clone(),
                        }),
                    )
                    .await
                    .map_err(|e| CompilationError::internal(e.to_string()))?;
            }
        }
        let result = self
            .create_df_logical_plan(stmt.clone(), qtrace, span_id.clone(), flat_list)
            .await?;

        if let Some(span_id) = span_id.as_ref() {
            if let Some(auth_context) = self.state.auth_context() {
                self.session_manager
                    .server
                    .transport
                    .log_load_state(
                        Some(span_id.clone()),
                        auth_context,
                        self.state.get_load_request_meta(),
                        "SQL API Query Planning Success".to_string(),
                        serde_json::json!({
                            "query": span_id.query_key.clone(),
                            "duration": planning_start.elapsed().unwrap().as_millis() as u64,
                        }),
                    )
                    .await
                    .map_err(|e| CompilationError::internal(e.to_string()))?;
            }
        }

        return Ok(result);
    }

    pub async fn plan(
        &self,
        stmt: &ast::Statement,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
        flat_list: bool,
    ) -> CompilationResult<QueryPlan> {
        let plan = match (stmt, &self.state.protocol) {
            (ast::Statement::Query(q), _) => {
                if let ast::SetExpr::Select(select) = &q.body {
                    if let Some(into) = &select.into {
                        return self
                            .select_into_to_plan(into, q, qtrace, span_id, flat_list)
                            .await;
                    }
                }

                self.select_to_plan(stmt, qtrace, span_id.clone(), flat_list)
                    .await
            }
            (ast::Statement::SetTransaction { .. }, _) => Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Box::new(dataframe::DataFrame::new(vec![], vec![])),
            )),
            (ast::Statement::SetRole { role_name, .. }, _) => self.set_role_to_plan(role_name),
            (ast::Statement::SetVariable { key_values }, _) => {
                self.set_variable_to_plan(&key_values).await
            }
            (ast::Statement::ShowVariable { variable }, _) => {
                self.show_variable_to_plan(variable, span_id.clone(), flat_list)
                    .await
            }
            (
                ast::Statement::Explain {
                    statement,
                    verbose,
                    analyze,
                    ..
                },
                _,
            ) => {
                self.explain_to_plan(&statement, *verbose, *analyze, flat_list)
                    .await
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
            (
                ast::Statement::CreateTable {
                    query: Some(query),
                    name,
                    columns,
                    constraints,
                    table_properties,
                    with_options,
                    temporary,
                    ..
                },
                DatabaseProtocol::PostgreSQL,
            ) if columns.is_empty()
                && constraints.is_empty()
                && table_properties.is_empty()
                && with_options.is_empty()
                && *temporary =>
            {
                let stmt = ast::Statement::Query(query.clone());
                self.create_table_to_plan(name, &stmt, qtrace, span_id.clone(), flat_list)
                    .await
            }
            (
                ast::Statement::Drop {
                    object_type, names, ..
                },
                DatabaseProtocol::PostgreSQL,
            ) if object_type == &ast::ObjectType::Table => self.drop_table_to_plan(names).await,
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
        span_id: Option<Arc<SpanId>>,
        flat_list: bool,
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

            self.create_df_logical_plan(stmt, &mut None, span_id.clone(), flat_list)
                .await
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

            self.create_df_logical_plan(stmt, &mut None, span_id.clone(), flat_list)
                .await
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
                span_id.clone(),
                flat_list,
            )
            .await
        }
    }

    fn explain_to_plan(
        &self,
        statement: &Box<ast::Statement>,
        verbose: bool,
        analyze: bool,
        flat_list: bool,
    ) -> Pin<Box<dyn Future<Output = Result<QueryPlan, CompilationError>> + Send>> {
        let self_cloned = self.clone();

        let statement = statement.clone();
        // This Boxing construct here because of recursive call to self.plan()
        Box::pin(async move {
            // TODO span_id ?
            let plan = self_cloned
                .plan(&statement, &mut None, None, flat_list)
                .await?;

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
                QueryPlan::DataFusionSelect(flags, plan, context)
                | QueryPlan::CreateTempTable(flags, plan, context, _, _) => {
                    // EXPLAIN over CREATE TABLE AS shows the SELECT query plan
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

    fn set_role_to_plan(
        &self,
        role_name: &Option<ast::Ident>,
    ) -> Result<QueryPlan, CompilationError> {
        let flags = StatusFlags::SERVER_STATE_CHANGED;
        let role_name = role_name
            .as_ref()
            .map(|role_name| role_name.value.clone())
            .unwrap_or("none".to_string());
        let variable =
            DatabaseVariable::system("role".to_string(), ScalarValue::Utf8(Some(role_name)), None);
        self.state.set_variables(vec![variable]);

        Ok(QueryPlan::MetaOk(flags, CommandCompletion::Set))
    }

    async fn set_variable_to_plan(
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
            DatabaseProtocol::Extension(_) => {
                log::warn!("set_variable_to_plan is not supported for custom protocol");
            }
        }

        let (user_variables, session_columns_to_update): (Vec<_>, Vec<_>) =
            session_columns_to_update.into_iter().partition(|v| {
                v.name.to_lowercase() == "user" || v.name.to_lowercase() == "current_user"
            });

        for v in user_variables {
            self.reauthenticate_if_needed().await?;

            let auth_context = self.state.auth_context().ok_or(CompilationError::user(
                "No auth context set but tried to set current user".to_string(),
            ))?;
            let to_user = match v.value {
                ScalarValue::Utf8(Some(user)) => user,
                _ => {
                    return Err(CompilationError::user(format!(
                        "Invalid user value: {:?}",
                        v.value
                    )))
                }
            };
            if self
                .session_manager
                .server
                .transport
                .can_switch_user_for_session(auth_context.clone(), to_user.clone())
                .await
                .map_err(|e| {
                    CompilationError::internal(format!(
                        "Error calling can_switch_user_for_session: {}",
                        e
                    ))
                })?
            {
                self.state.set_user(Some(to_user.clone()));
                let authenticate_response = self
                    .session_manager
                    .server
                    .auth
                    // TODO do we want to send actual password here?
                    .authenticate(Some(to_user.clone()), None)
                    .await
                    .map_err(|e| {
                        CompilationError::internal(format!("Error calling authenticate: {}", e))
                    })?;
                self.state
                    .set_auth_context(Some(authenticate_response.context));
            } else {
                return Err(CompilationError::user(format!(
                    "{:?} is not allowed to switch to '{}'",
                    auth_context, to_user
                )));
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

        Ok(QueryPlan::MetaOk(flags, CommandCompletion::Set))
    }

    async fn create_table_to_plan(
        &self,
        name: &ast::ObjectName,
        stmt: &ast::Statement,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
        flat_list: bool,
    ) -> Result<QueryPlan, CompilationError> {
        let plan = self
            .select_to_plan(stmt, qtrace, span_id, flat_list)
            .await?;
        let QueryPlan::DataFusionSelect(flags, plan, ctx) = plan else {
            return Err(CompilationError::internal(
                "unable to build DataFusion plan from Query".to_string(),
            ));
        };

        let ObjectName(ident_parts) = name;
        let Some(table_name) = ident_parts.last() else {
            return Err(CompilationError::internal(
                "table name contains no ident parts".to_string(),
            ));
        };
        Ok(QueryPlan::CreateTempTable(
            flags,
            plan,
            ctx,
            table_name.value.to_string(),
            self.state.temp_tables(),
        ))
    }

    async fn select_into_to_plan(
        &self,
        into: &ast::SelectInto,
        query: &Box<ast::Query>,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
        flat_list: bool,
    ) -> Result<QueryPlan, CompilationError> {
        if !into.temporary || !into.table {
            return Err(CompilationError::unsupported(
                "only TEMPORARY TABLE is supported for SELECT INTO".to_string(),
            ));
        }

        let mut new_query = query.clone();
        if let ast::SetExpr::Select(ref mut select) = new_query.body {
            select.into = None
        } else {
            return Err(CompilationError::internal(
                "query is unexpectedly not SELECT".to_string(),
            ));
        }
        let new_stmt = ast::Statement::Query(new_query);
        self.create_table_to_plan(&into.name, &new_stmt, qtrace, span_id, flat_list)
            .await
    }

    async fn drop_table_to_plan(
        &self,
        names: &[ast::ObjectName],
    ) -> Result<QueryPlan, CompilationError> {
        if names.len() != 1 {
            return Err(CompilationError::unsupported(
                "DROP TABLE supports dropping only one table at a time".to_string(),
            ));
        }
        let ObjectName(ident_parts) = names.first().unwrap();
        let Some(table_name) = ident_parts.last() else {
            return Err(CompilationError::internal(
                "table name contains no ident parts".to_string(),
            ));
        };
        let table_name_lower = table_name.value.to_ascii_lowercase();
        let temp_tables = self.state.temp_tables();
        tokio::task::spawn_blocking(move || temp_tables.remove(&table_name_lower))
            .await
            .map_err(|err| CompilationError::internal(err.to_string()))?
            .map_err(|err| CompilationError::internal(err.to_string()))?;
        let flags = StatusFlags::empty();
        Ok(QueryPlan::MetaOk(flags, CommandCompletion::DropTable))
    }

    pub fn create_execution_ctx(&self) -> DFSessionContext {
        let query_planner = Arc::new(CubeQueryPlanner::new(
            self.session_manager.server.transport.clone(),
            self.state.get_load_request_meta(),
            self.session_manager.server.config_obj.clone(),
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
        ctx.register_udf(create_greatest_udf());
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
        ctx.register_udf(create_has_table_privilege_udf(self.state.clone()));
        ctx.register_udf(create_has_any_column_privilege_udf(self.state.clone()));
        ctx.register_udf(create_pg_total_relation_size_udf());
        ctx.register_udf(create_cube_regclass_cast_udf());
        ctx.register_udf(create_pg_get_serial_sequence_udf());
        ctx.register_udf(create_json_build_object_udf());
        ctx.register_udf(create_regexp_substr_udf());
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
        ctx.register_udf(create_inet_server_addr_udf());

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

    async fn reauthenticate_if_needed(&self) -> CompilationResult<()> {
        if self.state.is_auth_context_expired() {
            let authenticate_response = self
                .session_manager
                .server
                .auth
                .authenticate(self.state.user(), None)
                .await
                .map_err(|e| {
                    CompilationError::fatal(format!(
                        "Error calling authenticate during re-authentication: {}",
                        e
                    ))
                })?;
            self.state
                .set_auth_context(Some(authenticate_response.context));
        }
        Ok(())
    }

    async fn create_df_logical_plan(
        &self,
        stmt: ast::Statement,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
        flat_list: bool,
    ) -> CompilationResult<QueryPlan> {
        self.reauthenticate_if_needed().await?;
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

        log::debug!("Initial Plan: {:#?}", optimized_plan);

        let cube_ctx = Arc::new(cube_ctx);
        let mut converter = LogicalPlanToLanguageConverter::new(cube_ctx.clone(), flat_list);
        let mut query_params = Some(HashMap::new());
        let root = converter
            .add_logical_plan_replace_params(
                &optimized_plan,
                &mut query_params,
                &mut LogicalPlanToLanguageContext::default(),
            )
            .map_err(|e| CompilationError::internal(e.to_string()))?;

        let mut finalized_graph = self
            .session_manager
            .server
            .compiler_cache
            .rewrite(
                self.state.auth_context().unwrap(),
                cube_ctx.clone(),
                converter.take_egraph(),
                &query_params.unwrap(),
                qtrace,
            )
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
            })?;

        // Replace Analysis as at least time has changed but it might be also context may affect rewriting in some other ways
        finalized_graph.analysis = LogicalPlanAnalysis::new(
            cube_ctx.clone(),
            Arc::new(DefaultPhysicalPlanner::default()),
        );

        let mut rewriter = Rewriter::new(finalized_graph, cube_ctx.clone());

        let result = rewriter
            .find_best_plan(
                root,
                self.state.auth_context().unwrap(),
                qtrace,
                span_id.clone(),
            )
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
            {
                let mut guard = ctx.state.write();
                // TODO: We should find what optimizers will be safety to use for OLAP queries
                guard.optimizer.rules = vec![];
            }
            if let Some(span_id) = span_id {
                span_id.set_is_data_query(true).await;
            }
        };

        log::debug!("Rewrite: {:#?}", rewrite_plan);
        let rewrite_plan = Self::evaluate_wrapped_sql(
            self.session_manager.server.transport.clone(),
            Arc::new(self.state.get_load_request_meta()),
            rewrite_plan,
        )
        .await?;
        if let Some(qtrace) = qtrace {
            qtrace.set_best_plan_and_cube_scans(&rewrite_plan);
        }

        Ok(QueryPlan::DataFusionSelect(
            StatusFlags::empty(),
            rewrite_plan,
            ctx,
        ))
    }

    fn evaluate_wrapped_sql(
        transport_service: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        plan: LogicalPlan,
    ) -> Pin<Box<dyn Future<Output = CompilationResult<LogicalPlan>> + Send>> {
        Box::pin(async move {
            if let LogicalPlan::Extension(Extension { node }) = &plan {
                // .cloned() is to avoid borrowing Any to comply with Send + Sync
                let wrapper_option = node.as_any().downcast_ref::<CubeScanWrapperNode>().cloned();
                if let Some(wrapper) = wrapper_option {
                    // TODO evaluate sql
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(
                            wrapper
                                .generate_sql(transport_service.clone(), load_request_meta.clone())
                                .await
                                .map_err(|e| CompilationError::internal(e.to_string()))?,
                        ),
                    }));
                }
            }
            let mut children = Vec::new();
            for input in plan.inputs() {
                children.push(
                    Self::evaluate_wrapped_sql(
                        transport_service.clone(),
                        load_request_meta.clone(),
                        input.clone(),
                    )
                    .await?,
                );
            }
            from_plan(&plan, plan.expressions().as_slice(), children.as_slice())
                .map_err(|e| CompilationError::internal(e.to_string()))
        })
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
    span_id: Option<Arc<SpanId>>,
) -> CompilationResult<QueryPlan> {
    let stmt = rewrite_statement(stmt);
    if let Some(qtrace) = qtrace {
        qtrace.set_visitor_replaced_statement(&stmt);
    }

    let planner = QueryPlanner::new(session.state.clone(), meta, session.session_manager.clone());
    let flat_list = session.server.config_obj.push_down_pull_up_split();
    planner.plan(&stmt, qtrace, span_id, flat_list).await
}

pub async fn convert_sql_to_cube_query(
    query: &String,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> CompilationResult<QueryPlan> {
    let stmt = parse_sql_to_statement(&query, session.state.protocol.clone(), &mut None)?;
    convert_statement_to_cube_query(&stmt, meta, session, &mut None, None).await
}
