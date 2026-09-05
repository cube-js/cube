use crate::compile::{
    qtrace::Qtrace, CommandCompletion, DatabaseProtocol, QueryEngine, QueryPlan, SqlQueryEngine,
    StatusFlags,
};
use sqlparser::ast;
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};

use crate::{
    compile::{
        copy::CopyOptions,
        engine::df::scan::CacheMode,
        error::{CompilationError, CompilationResult},
        parser::parse_sql_to_statement,
        CopyFromPlan, CreateEmptyTempTablePlan, DatabaseVariable, DatabaseVariablesToUpdate,
    },
    sql::{
        auth_service::SqlAuthServiceAuthenticateRequest,
        dataframe,
        postgres::copy::MAX_LENGTH_METADATA,
        statement::{
            ApproximateCountDistinctVisitor, CastReplacer, RedshiftDatePartReplacer,
            SensitiveDataSanitizer, SqlParser062Normalizer, ToTimestampReplacer,
            UdfWildcardArgReplacer,
        },
        ColumnFlags, ColumnType, Session, SessionManager, SessionState,
    },
    transport::{MetaContext, SpanId},
};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, TimeUnit},
    logical_plan::{
        plan::{Analyze, Explain, ToStringifiedPlan},
        LogicalPlan, PlanType, ToDFSchema,
    },
    scalar::ScalarValue,
};
use itertools::Itertools;
use sqlparser::ast::escape_quoted_string;

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
                if let ast::SetExpr::Select(select) = &*q.body {
                    if let Some(into) = &select.into {
                        return self.select_into_to_plan(into, q, qtrace, span_id).await;
                    }
                }

                self.select_to_plan(stmt, qtrace, span_id.clone()).await
            }
            (ast::Statement::Set(ast::Set::SetTransaction { .. }), _) => {
                Ok(QueryPlan::MetaTabular(
                    StatusFlags::empty(),
                    Box::new(dataframe::DataFrame::new(vec![], vec![])),
                ))
            }
            (ast::Statement::Set(ast::Set::SetRole { role_name, .. }), _) => {
                self.set_role_to_plan(role_name).await
            }
            (
                ast::Statement::Set(
                    set @ (ast::Set::SingleAssignment { .. }
                    | ast::Set::ParenthesizedAssignments { .. }
                    | ast::Set::MultipleAssignments { .. }),
                ),
                _,
            ) => self.set_variable_to_plan(set).await,
            (ast::Statement::Set(ast::Set::SetTimeZone { value, local }), _) => {
                self.set_time_zone_to_plan(value, *local).await
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
            (ast::Statement::Savepoint { .. }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Real support
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Savepoint,
                ))
            }
            (ast::Statement::ReleaseSavepoint { .. }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Real support
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Release,
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
                ast::Statement::CreateTable(ast::CreateTable {
                    query: Some(query),
                    name,
                    columns,
                    constraints,
                    table_options,
                    temporary,
                    if_not_exists,
                    ..
                }),
                DatabaseProtocol::PostgreSQL,
            ) if columns.is_empty()
                && constraints.is_empty()
                && matches!(table_options, ast::CreateTableOptions::None)
                && *temporary =>
            {
                let stmt = ast::Statement::Query(query.clone());
                self.create_table_to_plan(name, &stmt, *if_not_exists, qtrace, span_id.clone())
                    .await
            }
            (
                ast::Statement::CreateTable(ast::CreateTable {
                    query: None,
                    name,
                    columns,
                    constraints,
                    table_options,
                    temporary,
                    on_commit,
                    if_not_exists,
                    ..
                }),
                DatabaseProtocol::PostgreSQL,
            ) if !columns.is_empty()
                && constraints.is_empty()
                && matches!(table_options, ast::CreateTableOptions::None)
                && *temporary =>
            {
                // Rows are always preserved: transactions are not implemented, so
                // there is no point at which data could be dropped
                match on_commit {
                    None | Some(ast::OnCommit::PreserveRows) => (),
                    Some(on_commit) => {
                        return Err(CompilationError::unsupported(format!(
                            "ON COMMIT {} is not supported for a temporary table",
                            match on_commit {
                                ast::OnCommit::DeleteRows => "DELETE ROWS",
                                ast::OnCommit::Drop => "DROP",
                                ast::OnCommit::PreserveRows => "PRESERVE ROWS",
                            }
                        )))
                    }
                }

                self.create_empty_table_to_plan(name, columns, *if_not_exists)
                    .await
            }
            (
                ast::Statement::Drop {
                    object_type, names, ..
                },
                DatabaseProtocol::PostgreSQL,
            ) if object_type == &ast::ObjectType::Table => self.drop_table_to_plan(names).await,
            (
                ast::Statement::Copy {
                    source,
                    to,
                    target,
                    options,
                    legacy_options,
                    values,
                },
                DatabaseProtocol::PostgreSQL,
            ) => {
                self.copy_from_plan(source, *to, target, options, legacy_options, values)
                    .await
            }
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
                    escape_quoted_string(full_variable, '\''),
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
            QueryPlan::MetaOk(_, _)
            | QueryPlan::MetaTabular(_, _)
            | QueryPlan::CopyFrom(_)
            | QueryPlan::CreateEmptyTempTable(_) => Ok(QueryPlan::MetaTabular(
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
            | QueryPlan::CreateTempTable(plan, context, _, _, _) => {
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

    async fn set_role_to_plan(
        &self,
        role_name: &Option<ast::Ident>,
    ) -> Result<QueryPlan, CompilationError> {
        let flags = StatusFlags::SERVER_STATE_CHANGED;
        let username = role_name.as_ref().map(|role_name| role_name.value.clone());
        let Some(to_user) = username.clone().or_else(|| self.state.original_user()) else {
            return Err(CompilationError::internal(
                "Cannot reset role when original role has not been set".to_string(),
            ));
        };
        self.change_user(to_user).await?;
        let variable = DatabaseVariable::system(
            "role".to_string(),
            ScalarValue::Utf8(Some(username.unwrap_or("none".to_string()))),
            None,
        );
        self.state.set_variables(vec![variable]);
        Ok(QueryPlan::MetaOk(flags, CommandCompletion::Set))
    }

    async fn set_variable_to_plan(&self, set: &ast::Set) -> Result<QueryPlan, CompilationError> {
        // Normalize the various sqlparser SET shapes into a flat list of (name, value-exprs).
        let key_values: Vec<(String, &[ast::Expr])> = match set {
            ast::Set::SingleAssignment {
                variable, values, ..
            } => vec![(variable.to_string(), values.as_slice())],
            ast::Set::ParenthesizedAssignments { variables, values } => {
                if variables.len() != values.len() {
                    return Err(CompilationError::user(
                        "SET (...) = (...) requires matching number of variables and values"
                            .to_string(),
                    ));
                }

                variables
                    .iter()
                    .zip(values.iter())
                    .map(|(variable, value)| (variable.to_string(), std::slice::from_ref(value)))
                    .collect()
            }
            ast::Set::MultipleAssignments { assignments } => assignments
                .iter()
                .map(|assignment| {
                    (
                        assignment.name.to_string(),
                        std::slice::from_ref(&assignment.value),
                    )
                })
                .collect(),
            _ => {
                return Err(CompilationError::unsupported(format!(
                    "Unsupported SET statement: {set}"
                )))
            }
        };

        let mut session_columns_to_update =
            DatabaseVariablesToUpdate::with_capacity(key_values.len());

        match self.state.protocol {
            DatabaseProtocol::PostgreSQL => {
                for (key, exprs) in key_values.iter() {
                    let value: String = match exprs.first() {
                        Some(ast::Expr::Identifier(ident)) => ident.value.to_string(),
                        Some(ast::Expr::Value(val)) => match &val.value {
                            ast::Value::SingleQuotedString(single_quoted_str) => {
                                single_quoted_str.to_string()
                            }
                            ast::Value::DoubleQuotedString(double_quoted_str) => {
                                double_quoted_str.to_string()
                            }
                            ast::Value::Number(number, _) => number.to_string(),
                            _ => {
                                return Err(CompilationError::user(format!(
                                    "invalid {key} variable format"
                                )))
                            }
                        },
                        _ => {
                            return Err(CompilationError::user(format!(
                                "invalid {key} variable format"
                            )))
                        }
                    };

                    session_columns_to_update.push(DatabaseVariable::system(
                        key.to_lowercase(),
                        ScalarValue::Utf8(Some(value.clone())),
                        None,
                    ));
                }
            }
            DatabaseProtocol::Extension(_) => {
                log::warn!("set_variable_to_plan is not supported for custom protocol");
            }
        }

        let (special_variables, session_columns_to_update): (Vec<_>, Vec<_>) =
            session_columns_to_update.into_iter().partition(|v| {
                matches!(
                    v.name.to_lowercase().as_str(),
                    "user" | "current_user" | "timezone" | "cube_cache"
                )
            });

        for v in special_variables {
            match v.name.as_str() {
                "user" | "current_user" => {
                    let to_user = match v.value {
                        ScalarValue::Utf8(Some(user)) => user,
                        _ => {
                            return Err(CompilationError::user(format!(
                                "Invalid user value: {:?}",
                                v.value
                            )))
                        }
                    };
                    self.change_user(to_user).await?;
                }
                "timezone" => {
                    let timezone = match v.value {
                        ScalarValue::Utf8(Some(tz)) => tz,
                        _ => {
                            return Err(CompilationError::user(format!(
                                "Invalid timezone value: {:?}",
                                v.value
                            )))
                        }
                    };
                    self.change_timezone(timezone).await?;
                }
                "cube_cache" => {
                    let cache_mode = match v.value {
                        ScalarValue::Utf8(Some(mode)) => mode,
                        _ => {
                            return Err(CompilationError::user(format!(
                                "Invalid cube_cache value: {:?}",
                                v.value
                            )))
                        }
                    };
                    self.change_cache_mode(cache_mode).await?;
                }
                _ => {
                    return Err(CompilationError::user(format!(
                        "Invalid special variable: {:?}",
                        v.name
                    )))
                }
            }
        }

        if !session_columns_to_update.is_empty() {
            self.state.set_variables(session_columns_to_update);
        }

        Ok(QueryPlan::MetaOk(
            StatusFlags::empty(),
            CommandCompletion::Set,
        ))
    }

    async fn set_time_zone_to_plan(
        &self,
        timezone: &ast::Expr,
        local: bool,
    ) -> Result<QueryPlan, CompilationError> {
        if local {
            return Err(CompilationError::unsupported(
                "SET TIME ZONE is not supported with LOCAL, omit or use SESSION".to_string(),
            ));
        }

        let timezone_str = match timezone {
            ast::Expr::Identifier(ident) => ident.value.to_string(),
            ast::Expr::Value(ast::ValueWithSpan {
                value: ast::Value::SingleQuotedString(string),
                ..
            }) => string.clone(),
            _ => {
                return Err(CompilationError::unsupported(format!(
                    "Unsupported TimeZone value: {}",
                    timezone
                )))
            }
        };

        self.change_timezone(timezone_str).await?;
        Ok(QueryPlan::MetaOk(
            StatusFlags::empty(),
            CommandCompletion::Set,
        ))
    }

    async fn change_user(&self, username: String) -> Result<(), CompilationError> {
        self.reauthenticate_if_needed().await?;

        let auth_context = self.state.auth_context().ok_or(CompilationError::internal(
            "No auth context set but tried to set current user".to_string(),
        ))?;

        let can_switch_user = self
            .session_manager
            .server
            .transport
            .can_switch_user_for_session(auth_context.clone(), username.clone())
            .await
            .map_err(|e| {
                CompilationError::internal(format!(
                    "Error calling can_switch_user_for_session: {}",
                    e
                ))
            })?;
        if !can_switch_user {
            return Err(CompilationError::user(format!(
                "user '{}' is not allowed to switch to '{}'",
                auth_context
                    .user()
                    .as_ref()
                    .map(|v| v.as_str())
                    .unwrap_or("not specified"),
                username
            )));
        }

        self.state.set_user(Some(username.clone()));
        let sql_auth_request = SqlAuthServiceAuthenticateRequest {
            protocol: "postgres".to_string(),
            method: "password".to_string(),
            database: self.state.database(),
        };
        let authenticate_response = self
            .session_manager
            .server
            .auth
            .authenticate(sql_auth_request, Some(username), None)
            .await
            .map_err(|e| {
                CompilationError::internal(format!("Error calling authenticate: {}", e))
            })?;
        self.state
            .set_auth_context(Some(authenticate_response.context));
        Ok(())
    }

    async fn change_timezone(&self, timezone: String) -> Result<(), CompilationError> {
        let mut query_timezone = self.state.query_timezone.write().map_err(|err| {
            CompilationError::internal(format!("Unable to acquire query timezone lock: {}", err))
        })?;
        let tz_name =
            if timezone.eq_ignore_ascii_case("default") || timezone.eq_ignore_ascii_case("local") {
                *query_timezone = None;
                "GMT".to_string()
            } else {
                *query_timezone = Some(timezone.clone());
                timezone
            };

        let variable = DatabaseVariable::system(
            "timezone".to_string(),
            ScalarValue::Utf8(Some(tz_name)),
            None,
        );
        self.state.set_variables(vec![variable]);
        Ok(())
    }

    async fn change_cache_mode(&self, cache_mode_str: String) -> Result<(), CompilationError> {
        let mut cache_mode = self.state.cache_mode.write().map_err(|err| {
            CompilationError::internal(format!("Unable to acquire cache mode lock: {}", err))
        })?;
        let cache_mode_value = if cache_mode_str.eq_ignore_ascii_case("default") {
            *cache_mode = None;
            None
        } else {
            let cache_mode_parsed = CacheMode::from_str(&cache_mode_str).map_err(|_| {
                CompilationError::user(format!("Invalid value for cache mode: {}", cache_mode_str))
            })?;
            *cache_mode = Some(cache_mode_parsed);
            Some(cache_mode_parsed.to_string())
        };
        let variable = DatabaseVariable::user_defined(
            "cube_cache".to_string(),
            ScalarValue::Utf8(cache_mode_value),
            None,
        );
        self.state.set_variables(vec![variable]);
        Ok(())
    }

    async fn create_table_to_plan(
        &self,
        name: &ast::ObjectName,
        stmt: &ast::Statement,
        if_not_exists: bool,
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
            table_name
                .as_ident()
                .ok_or_else(|| {
                    CompilationError::internal("table name is not a plain identifier".to_string())
                })?
                .value
                .clone(),
            self.state.temp_tables(),
            if_not_exists,
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
        if let ast::SetExpr::Select(ref mut select) = *new_query.body {
            select.into = None
        } else {
            return Err(CompilationError::internal(
                "query is unexpectedly not SELECT".to_string(),
            ));
        }
        let new_stmt = ast::Statement::Query(Box::new(new_query));
        self.create_table_to_plan(&into.name, &new_stmt, false, qtrace, span_id)
            .await
    }

    /// Plan for `CREATE TEMPORARY TABLE t (a int, ...)`, which creates an empty
    /// table to be filled by `COPY ... FROM STDIN`.
    async fn create_empty_table_to_plan(
        &self,
        name: &ast::ObjectName,
        columns: &[ast::ColumnDef],
        if_not_exists: bool,
    ) -> Result<QueryPlan, CompilationError> {
        let table_name = table_name_from_object_name(name)?;

        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let mut nullable = true;

            for option in column.options.iter() {
                match option.option {
                    ast::ColumnOption::Null => (),
                    ast::ColumnOption::NotNull => nullable = false,
                    _ => {
                        return Err(CompilationError::unsupported(format!(
                            "Unsupported column option for a temporary table: {}",
                            option
                        )))
                    }
                }
            }

            let mut field = Field::new(
                &normalize_ident(&column.name),
                sql_type_to_arrow_type(&column.data_type)?,
                nullable,
            );

            // The width of a character type is not part of the Arrow type, and has to
            // travel with the field for COPY to enforce it
            if let Some(length) = character_length(&column.data_type) {
                field = field.with_metadata(Some(BTreeMap::from([(
                    MAX_LENGTH_METADATA.to_string(),
                    length.to_string(),
                )])));
            }

            fields.push(field);
        }

        // The table is saved when the plan is executed, and not here: planning also
        // happens for Parse, Bind and EXPLAIN, none of which may leave a table behind
        Ok(QueryPlan::CreateEmptyTempTable(Box::new(
            CreateEmptyTempTablePlan {
                table_name,
                schema: Arc::new(Schema::new(fields)),
                if_not_exists,
                temp_tables: self.state.temp_tables(),
            },
        )))
    }

    /// Plan for `COPY ... FROM STDIN`. Cubes are read-only, so a temporary table
    /// of the current session is the only place the data can be loaded into.
    async fn copy_from_plan(
        &self,
        source: &ast::CopySource,
        to: bool,
        target: &ast::CopyTarget,
        options: &[ast::CopyOption],
        legacy_options: &[ast::CopyLegacyOption],
        values: &[Option<String>],
    ) -> Result<QueryPlan, CompilationError> {
        if to {
            return Err(CompilationError::unsupported(
                "COPY TO is not supported, only COPY ... FROM STDIN".to_string(),
            ));
        }

        if !matches!(target, ast::CopyTarget::Stdin) {
            return Err(CompilationError::unsupported(format!(
                "COPY FROM {} is not supported, only COPY ... FROM STDIN",
                target
            )));
        }

        if !values.is_empty() {
            return Err(CompilationError::unsupported(
                "COPY data in the statement itself is not supported, send it as a data stream"
                    .to_string(),
            ));
        }

        // The parser rejects a query as the source of COPY FROM
        let ast::CopySource::Table {
            table_name,
            columns,
        } = source
        else {
            return Err(CompilationError::internal(
                "COPY FROM must have a table as its target".to_string(),
            ));
        };

        let table_name = table_name_from_object_name(table_name)?;
        let temp_tables = self.state.temp_tables();
        let Some(temp_table) = temp_tables.get(&table_name) else {
            return Err(CompilationError::user(format!(
                "COPY FROM STDIN is only supported for temporary tables, and temporary table \"{}\" does not exist in this session",
                table_name
            )));
        };

        let schema = temp_table.schema();
        let column_index = |column: &ast::Ident| {
            let column = normalize_ident(column);

            schema.index_of(&column).map_err(|_| {
                CompilationError::user(format!(
                    r#"column "{}" of relation "{}" does not exist"#,
                    column, table_name
                ))
            })
        };

        let column_indices = match columns.is_empty() {
            true => (0..schema.fields().len()).collect(),
            false => {
                let indices = columns
                    .iter()
                    .map(column_index)
                    .collect::<Result<Vec<_>, _>>()?;

                for (position, index) in indices.iter().enumerate() {
                    if indices[..position].contains(index) {
                        return Err(CompilationError::user(format!(
                            r#"column "{}" is specified more than once"#,
                            schema.field(*index).name()
                        )));
                    }
                }

                indices
            }
        };

        let options = CopyOptions::parse(options, legacy_options)?;
        for (name, columns) in [
            ("FORCE_NOT_NULL", &options.force_not_null),
            ("FORCE_NULL", &options.force_null),
        ] {
            for column in columns {
                let index = schema.index_of(column).map_err(|_| {
                    CompilationError::user(format!(
                        r#"column "{}" of relation "{}" does not exist"#,
                        column, table_name
                    ))
                })?;

                // The option speaks about a column the copy loads, and only those
                if !column_indices.contains(&index) {
                    return Err(CompilationError::user(format!(
                        r#"{} column "{}" not referenced by COPY"#,
                        name, column
                    )));
                }
            }
        }

        Ok(QueryPlan::CopyFrom(Box::new(CopyFromPlan {
            table_name,
            schema,
            column_indices,
            options,
            temp_tables: self.state.temp_tables(),
        })))
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
        let table_name_lower = table_name
            .as_ident()
            .ok_or_else(|| {
                CompilationError::internal("table name is not a plain identifier".to_string())
            })?
            .value
            .to_ascii_lowercase();
        let temp_tables = self.state.temp_tables();
        tokio::task::spawn_blocking(move || temp_tables.remove(&table_name_lower))
            .await
            .map_err(|err| CompilationError::internal(err.to_string()))??;
        let flags = StatusFlags::empty();
        Ok(QueryPlan::MetaOk(flags, CommandCompletion::DropTable))
    }

    async fn reauthenticate_if_needed(&self) -> CompilationResult<()> {
        if self.state.is_auth_context_expired() {
            let sql_auth_request = SqlAuthServiceAuthenticateRequest {
                protocol: "postgres".to_string(),
                method: "password".to_string(),
                database: self.state.database(),
            };
            let authenticate_response = self
                .session_manager
                .server
                .auth
                .authenticate(sql_auth_request, self.state.user(), None)
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
            ast::Statement::Query(query) => match &*query.body {
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

/// Name of a table or a column as it is stored. Unquoted identifiers are
/// case-insensitive in PostgreSQL, and are folded to lower case.
pub fn normalize_ident(ident: &ast::Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_ascii_lowercase(),
    }
}

fn table_name_from_object_name(name: &ast::ObjectName) -> Result<String, CompilationError> {
    let ast::ObjectName(ident_parts) = name;
    let Some(table_name) = ident_parts.last() else {
        return Err(CompilationError::internal(
            "table name contains no ident parts".to_string(),
        ));
    };

    let table_name = table_name.as_ident().ok_or_else(|| {
        CompilationError::internal("table name is not a plain identifier".to_string())
    })?;

    Ok(normalize_ident(table_name))
}

/// Declared width of a character type, which PostgreSQL enforces when data is loaded.
fn character_length(data_type: &ast::DataType) -> Option<u64> {
    let length = match data_type {
        ast::DataType::CharVarying(length)
        | ast::DataType::CharacterVarying(length)
        | ast::DataType::Varchar(length)
        | ast::DataType::Nvarchar(length) => length.as_ref()?,
        _ => return None,
    };

    match length {
        ast::CharacterLength::IntegerLength { length, .. } => Some(*length),
        ast::CharacterLength::Max => None,
    }
}

/// The widest NUMERIC a temporary table can hold: values are stored as 128-bit
/// decimals, which is narrower than what PostgreSQL supports.
const MAX_DECIMAL_PRECISION: usize = 38;

fn decimal_precision(precision: u64) -> Result<usize, CompilationError> {
    if precision < 1 || precision as usize > MAX_DECIMAL_PRECISION {
        return Err(CompilationError::unsupported(format!(
            "NUMERIC precision {} must be between 1 and {}",
            precision, MAX_DECIMAL_PRECISION
        )));
    }

    Ok(precision as usize)
}

/// Type of a column of a temporary table. Only types which `COPY` can load are
/// accepted, so that a table can never be created that cannot be filled.
fn sql_type_to_arrow_type(data_type: &ast::DataType) -> Result<DataType, CompilationError> {
    let arrow_type = match data_type {
        ast::DataType::Bool | ast::DataType::Boolean => DataType::Boolean,
        ast::DataType::SmallInt(_) | ast::DataType::Int2(_) => DataType::Int16,
        ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::Int4(_) => {
            DataType::Int32
        }
        ast::DataType::BigInt(_) | ast::DataType::Int8(_) => DataType::Int64,
        ast::DataType::Real | ast::DataType::Float4 => DataType::Float32,
        ast::DataType::Double(_) | ast::DataType::DoublePrecision | ast::DataType::Float8 => {
            DataType::Float64
        }
        ast::DataType::Float(precision) => match precision {
            ast::ExactNumberInfo::Precision(precision) if *precision <= 24 => DataType::Float32,
            _ => DataType::Float64,
        },
        ast::DataType::Decimal(info) | ast::DataType::Numeric(info) => match info {
            ast::ExactNumberInfo::None => DataType::Decimal(MAX_DECIMAL_PRECISION, 10),
            ast::ExactNumberInfo::Precision(precision) => {
                DataType::Decimal(decimal_precision(*precision)?, 0)
            }
            ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                let precision = decimal_precision(*precision)?;
                let scale = *scale as usize;

                if scale > precision {
                    return Err(CompilationError::unsupported(format!(
                        "NUMERIC scale {} must not exceed the precision {}",
                        scale, precision
                    )));
                }

                DataType::Decimal(precision, scale)
            }
        },
        ast::DataType::CharVarying(_)
        | ast::DataType::CharacterVarying(_)
        | ast::DataType::Varchar(_)
        | ast::DataType::Nvarchar(_)
        | ast::DataType::Text
        | ast::DataType::String(_)
        | ast::DataType::Uuid
        | ast::DataType::JSON
        | ast::DataType::JSONB => DataType::Utf8,
        // A fixed width character type is blank padded to its width, and its trailing
        // blanks do not count in comparisons: a text column carries neither
        ast::DataType::Char(_) | ast::DataType::Character(_) => {
            return Err(CompilationError::unsupported(format!(
                "Unsupported column type for a temporary table: {}, use VARCHAR or TEXT",
                data_type
            )))
        }
        ast::DataType::Date => DataType::Date32,
        // A time zone changes what the values of the column mean, so a column which
        // asks for one is refused rather than quietly stored without it
        ast::DataType::Timestamp(
            _,
            ast::TimezoneInfo::None | ast::TimezoneInfo::WithoutTimeZone,
        ) => DataType::Timestamp(TimeUnit::Nanosecond, None),
        ast::DataType::Datetime(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
        other => {
            return Err(CompilationError::unsupported(format!(
                "Unsupported column type for a temporary table: {}",
                other
            )))
        }
    };

    Ok(arrow_type)
}

pub fn rewrite_statement(stmt: ast::Statement) -> ast::Statement {
    let stmt = SqlParser062Normalizer::new().replace(stmt);
    let stmt = CastReplacer::new().replace(stmt);
    let stmt = ToTimestampReplacer::new().replace(stmt);
    let stmt = UdfWildcardArgReplacer::new().replace(stmt);
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
