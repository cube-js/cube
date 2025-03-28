use crate::compile::{
    qtrace::Qtrace, CommandCompletion, DatabaseProtocol, QueryEngine, QueryPlan, SqlQueryEngine,
    StatusFlags,
};
use sqlparser::ast;
use std::{collections::HashMap, sync::Arc};

use crate::{
    compile::{
        error::{CompilationError, CompilationResult},
        parser::parse_sql_to_statement,
        DatabaseVariable, DatabaseVariablesToUpdate,
    },
    sql::{
        dataframe,
        statement::{
            ApproximateCountDistinctVisitor, CastReplacer, DateTokenNormalizeReplacer,
            RedshiftDatePartReplacer, SensitiveDataSanitizer, ToTimestampReplacer,
            UdfWildcardArgReplacer,
        },
        ColumnFlags, ColumnType, Session, SessionManager, SessionState,
    },
    transport::{MetaContext, SpanId},
};
use datafusion::{
    logical_plan::{
        plan::{Analyze, Explain, ToStringifiedPlan},
        LogicalPlan, PlanType, ToDFSchema,
    },
    scalar::ScalarValue,
};
use itertools::Itertools;
use sqlparser::ast::escape_single_quote_string;

#[derive(Clone)]
pub struct QueryRouter {
    state: Arc<SessionState>,
    meta: Arc<MetaContext>,
    session_manager: Arc<SessionManager>,
}

impl QueryRouter {
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
    ) -> CompilationResult<QueryPlan> {
        self.create_df_logical_plan(stmt.clone(), qtrace, span_id.clone())
            .await
    }

    pub async fn plan(
        &self,
        stmt: ast::Statement,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> CompilationResult<QueryPlan> {
        match stmt {
            ast::Statement::Explain {
                analyze,
                statement,
                verbose,
                ..
            } => self.explain_to_plan(statement, verbose, analyze).await,
            other => self.plan_query(&other, qtrace, span_id).await,
        }
    }

    async fn plan_query(
        &self,
        stmt: &ast::Statement,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> CompilationResult<QueryPlan> {
        let plan = match (stmt, &self.state.protocol) {
            (ast::Statement::Query(q), _) => {
                if let ast::SetExpr::Select(select) = &q.body {
                    if let Some(into) = &select.into {
                        return self.select_into_to_plan(into, q, qtrace, span_id).await;
                    }
                }

                self.select_to_plan(stmt, qtrace, span_id.clone()).await
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
                self.show_variable_to_plan(variable, span_id.clone()).await
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
                self.create_table_to_plan(name, &stmt, qtrace, span_id.clone())
                    .await
            }
            (
                ast::Statement::Drop {
                    object_type, names, ..
                },
                DatabaseProtocol::PostgreSQL,
            ) if object_type == &ast::ObjectType::Table => self.drop_table_to_plan(names).await,
            _ => Err(CompilationError::unsupported(format!(
                "Unsupported query type: {stmt}"
            ))),
        };

        match plan {
            Err(err) => {
                let meta = Some(HashMap::from([
                    ("query".to_string(), stmt.to_string()),
                    (
                        "sanitizedQuery".to_string(),
                        SensitiveDataSanitizer::new()
                            .replace(stmt.clone())
                            .to_string(),
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
    ) -> CompilationResult<QueryPlan> {
        let full_variable = variable.iter().map(|v| v.value.to_lowercase()).join("_");
        let full_variable = match full_variable.as_str() {
            "transaction_isolation_level" => "transaction_isolation",
            x => x,
        };

        let name = variable.to_vec()[0].value.clone();
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

        self.create_df_logical_plan(stmt, &mut None, span_id.clone())
            .await
    }

    async fn explain_to_plan(
        &self,
        statement: Box<ast::Statement>,
        verbose: bool,
        analyze: bool,
    ) -> Result<QueryPlan, CompilationError> {
        // TODO span_id ?
        let plan = self.plan_query(&statement, &mut None, None).await?;

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
            QueryPlan::DataFusionSelect(plan, context)
            | QueryPlan::CreateTempTable(plan, context, _, _) => {
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

                Ok(QueryPlan::DataFusionSelect(explain_plan, context))
            }
        }
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
                    if key_value.key.value.to_lowercase() == "autocommit" {
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
    ) -> Result<QueryPlan, CompilationError> {
        let plan = self.select_to_plan(stmt, qtrace, span_id).await?;
        let QueryPlan::DataFusionSelect(plan, ctx) = plan else {
            return Err(CompilationError::internal(
                "unable to build DataFusion plan from Query".to_string(),
            ));
        };

        let ast::ObjectName(ident_parts) = name;
        let Some(table_name) = ident_parts.last() else {
            return Err(CompilationError::internal(
                "table name contains no ident parts".to_string(),
            ));
        };

        Ok(QueryPlan::CreateTempTable(
            plan,
            ctx,
            table_name.value.to_string(),
            self.state.temp_tables(),
        ))
    }

    async fn select_into_to_plan(
        &self,
        into: &ast::SelectInto,
        query: &ast::Query,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
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
        let new_stmt = ast::Statement::Query(Box::new(new_query));
        self.create_table_to_plan(&into.name, &new_stmt, qtrace, span_id)
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
        let ast::ObjectName(ident_parts) = names.first().unwrap();
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

        let sql_query_engine = SqlQueryEngine::new(self.session_manager.clone());
        let (plan, _) = sql_query_engine
            .plan(stmt, qtrace, span_id, self.meta.clone(), self.state.clone())
            .await?;

        Ok(plan)
    }
}

pub fn rewrite_statement(stmt: ast::Statement) -> ast::Statement {
    let stmt = CastReplacer::new().replace(stmt);
    let stmt = ToTimestampReplacer::new().replace(stmt);
    let stmt = UdfWildcardArgReplacer::new().replace(stmt);
    let stmt = DateTokenNormalizeReplacer::new().replace(stmt);
    let stmt = RedshiftDatePartReplacer::new().replace(stmt);
    let stmt = ApproximateCountDistinctVisitor::new().replace(stmt);

    stmt
}

pub async fn convert_statement_to_cube_query(
    stmt: ast::Statement,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
    qtrace: &mut Option<Qtrace>,
    span_id: Option<Arc<SpanId>>,
) -> CompilationResult<QueryPlan> {
    let stmt = rewrite_statement(stmt);

    if let Some(qtrace) = qtrace {
        qtrace.set_visitor_replaced_statement(&stmt);
    }

    let planner = QueryRouter::new(session.state.clone(), meta, session.session_manager.clone());
    planner.plan(stmt, qtrace, span_id).await
}

pub async fn convert_sql_to_cube_query(
    query: &str,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> CompilationResult<QueryPlan> {
    let stmt = parse_sql_to_statement(&query, session.state.protocol.clone(), &mut None)?;
    convert_statement_to_cube_query(stmt, meta, session, &mut None, None).await
}
