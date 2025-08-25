use std::{
    backtrace::Backtrace, collections::HashMap, future::Future, pin::Pin, sync::Arc,
    time::SystemTime,
};

use crate::{
    compile::{
        engine::{
            df::{
                optimizers::{
                    FilterPushDown, FilterSplitMeta, LimitPushDown, PlanNormalize, SortPushDown,
                },
                planner::CubeQueryPlanner,
                scan::CubeScanNode,
                wrapper::{CubeScanWrappedSqlNode, CubeScanWrapperNode},
            },
            udf::*,
            CubeContext, VariablesProvider,
        },
        qtrace::Qtrace,
        rewrite::{
            analysis::LogicalPlanAnalysis,
            converter::{LogicalPlanToLanguageContext, LogicalPlanToLanguageConverter},
        },
        CompilationError, CompilationResult, DatabaseProtocol, QueryPlan, Rewriter,
    },
    config::ConfigObj,
    sql::{
        compiler_cache::{CompilerCache, CompilerCacheEntry},
        statement::SensitiveDataSanitizer,
        SessionManager, SessionState,
    },
    transport::{LoadRequestMeta, MetaContext, SpanId, TransportService},
    CubeErrorCauseType,
};
use datafusion::{
    error::DataFusionError,
    execution::context::{
        default_session_builder, SessionConfig as DFSessionConfig,
        SessionContext as DFSessionContext,
    },
    logical_plan::{plan::Extension, LogicalPlan, PlanVisitor},
    optimizer::{
        optimizer::{OptimizerConfig, OptimizerRule},
        projection_drop_out::ProjectionDropOut,
        utils::from_plan,
    },
    physical_plan::planner::DefaultPhysicalPlanner,
    sql::{parser::Statement as DFStatement, planner::SqlToRel},
    variable::VarType,
};

#[async_trait::async_trait]
pub trait QueryEngine {
    /// Custom type for AST statement type, It allows to use any parsers for SQL
    type AstStatementType: std::fmt::Display + Send;

    /// Additional metadata for results of plan method instead of extending query plan
    type PlanMetadataType: std::fmt::Debug + Send;

    fn compiler_cache_ref(&self) -> &Arc<dyn CompilerCache>;

    fn transport_ref(&self) -> &Arc<dyn TransportService>;

    fn config_ref(&self) -> &Arc<dyn ConfigObj>;

    fn create_cube_ctx(
        &self,
        state: Arc<SessionState>,
        meta: Arc<MetaContext>,
        session_ctx: DFSessionContext,
    ) -> Result<CubeContext, CompilationError>;

    fn create_session_ctx(
        &self,
        state: Arc<SessionState>,
    ) -> Result<DFSessionContext, CompilationError>;

    fn create_logical_plan(
        &self,
        cube_ctx: &CubeContext,
        stmt: &Self::AstStatementType,
    ) -> Result<(LogicalPlan, Self::PlanMetadataType), DataFusionError>;

    fn sanitize_statement(&self, stmt: &Self::AstStatementType) -> Self::AstStatementType;

    async fn get_cache_entry(
        &self,
        state: Arc<SessionState>,
    ) -> Result<Arc<CompilerCacheEntry>, CompilationError>;

    async fn plan(
        &self,
        stmt: Self::AstStatementType,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
        meta: Arc<MetaContext>,
        state: Arc<SessionState>,
    ) -> CompilationResult<(QueryPlan, Self::PlanMetadataType)> {
        let cache_entry = self.get_cache_entry(state.clone()).await?;

        let planning_start = SystemTime::now();
        if let Some(span_id) = span_id.as_ref() {
            if let Some(auth_context) = state.auth_context() {
                self.transport_ref()
                    .log_load_state(
                        Some(span_id.clone()),
                        auth_context,
                        state.get_load_request_meta("sql"),
                        "SQL API Query Planning".to_string(),
                        serde_json::json!({
                            "query": span_id.query_key.clone(),
                        }),
                    )
                    .await
                    .map_err(|e| CompilationError::internal(e.to_string()))?;
            }
        }

        let ctx = self.create_session_ctx(state.clone())?;
        let cube_ctx = self.create_cube_ctx(state.clone(), meta.clone(), ctx.clone())?;

        let (plan, metadata) = self.create_logical_plan(&cube_ctx, &stmt).map_err(|err| {
            let message = format!("Initial planning error: {}", err,);
            let meta = Some(HashMap::from([
                ("query".to_string(), stmt.to_string()),
                (
                    "sanitizedQuery".to_string(),
                    self.sanitize_statement(&stmt).to_string(),
                ),
            ]));

            CompilationError::internal(message).with_meta(meta)
        })?;

        let mut optimized_plan = plan;
        // ctx.optimize(&plan).map_err(|err| {
        //    CompilationError::Internal(format!("Planning optimization error: {}", err))
        // })?;

        let optimizer_config = OptimizerConfig::new();
        let optimizers: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
            Arc::new(PlanNormalize::new(&cube_ctx)),
            Arc::new(ProjectionDropOut::new()),
            Arc::new(FilterPushDown::new()),
            Arc::new(SortPushDown::new()),
            Arc::new(LimitPushDown::new()),
            Arc::new(FilterSplitMeta::new()),
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
        let mut converter = LogicalPlanToLanguageConverter::new(
            cube_ctx.clone(),
            self.config_ref().push_down_pull_up_split(),
        );
        let mut query_params = Some(HashMap::new());
        let root = converter
            .add_logical_plan_replace_params(
                &optimized_plan,
                &mut query_params,
                &mut LogicalPlanToLanguageContext::default(),
            )
            .map_err(|e| CompilationError::internal(e.to_string()))?;

        let rewriting_start = SystemTime::now();
        if let Some(span_id) = span_id.as_ref() {
            if let Some(auth_context) = state.auth_context() {
                self.transport_ref()
                    .log_load_state(
                        Some(span_id.clone()),
                        auth_context,
                        state.get_load_request_meta("sql"),
                        "SQL API Plan Rewrite".to_string(),
                        serde_json::json!({}),
                    )
                    .await
                    .map_err(|e| CompilationError::internal(e.to_string()))?;
            }
        }

        let mut finalized_graph = self
            .compiler_cache_ref()
            .rewrite(
                Arc::clone(&cache_entry),
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
                            self.sanitize_statement(&stmt).to_string(),
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
                            self.sanitize_statement(&stmt).to_string(),
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
                cache_entry,
                state.auth_context().unwrap(),
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
                            self.sanitize_statement(&stmt).to_string(),
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
                            self.sanitize_statement(&stmt).to_string(),
                        ),
                    ])),
                ),
            });

        if let Err(_) = &result {
            log::error!("It may be this query is not supported yet. Please post an issue on GitHub https://github.com/cube-js/cube.js/issues/new?template=sql_api_query_issue.md or ask about it in Slack https://slack.cube.dev.");
        }

        let rewrite_plan = result?;

        if let Some(span_id) = span_id.as_ref() {
            if let Some(auth_context) = state.auth_context() {
                self.transport_ref()
                    .log_load_state(
                        Some(span_id.clone()),
                        auth_context,
                        state.get_load_request_meta("sql"),
                        "SQL API Plan Rewrite Success".to_string(),
                        serde_json::json!({
                            "duration": rewriting_start.elapsed().unwrap().as_millis() as u64,
                        }),
                    )
                    .await
                    .map_err(|e| CompilationError::internal(e.to_string()))?;
            }
        }

        // DF optimizes logical plan (second time) on physical plan creation
        // It's not safety to use all optimizers from DF for OLAP queries, because it will lead to errors
        // From another side, 99% optimizers cannot optimize anything
        if is_olap_query(&rewrite_plan)? {
            {
                let mut guard = ctx.state.write();
                // TODO: We should find what optimizers will be safety to use for OLAP queries
                guard.optimizer.rules = vec![];
            }
            if let Some(span_id) = &span_id {
                span_id.set_is_data_query(true).await;
            }
        };

        log::debug!("Rewrite: {:#?}", rewrite_plan);

        if let Some(span_id) = span_id.as_ref() {
            if let Some(auth_context) = state.auth_context() {
                self.transport_ref()
                    .log_load_state(
                        Some(span_id.clone()),
                        auth_context,
                        state.get_load_request_meta("sql"),
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

        // We want to generate SQL early, as a part of planning, and not later (like during execution)
        // to catch all SQL generation errors during planning
        let rewrite_plan = Self::evaluate_wrapped_sql(
            self.transport_ref().clone(),
            Arc::new(state.get_load_request_meta("sql")),
            rewrite_plan,
        )
        .await?;
        if let Some(qtrace) = qtrace {
            qtrace.set_best_plan_and_cube_scans(&rewrite_plan);
        }

        Ok((QueryPlan::DataFusionSelect(rewrite_plan, ctx), metadata))
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

pub struct SqlQueryEngine {
    session_manager: Arc<SessionManager>,
}

impl SqlQueryEngine {
    pub fn new(session_manager: Arc<SessionManager>) -> Self {
        Self { session_manager }
    }
}

#[async_trait::async_trait]
impl QueryEngine for SqlQueryEngine {
    type AstStatementType = sqlparser::ast::Statement;

    type PlanMetadataType = ();

    fn create_cube_ctx(
        &self,
        state: Arc<SessionState>,
        meta: Arc<MetaContext>,
        session_ctx: DFSessionContext,
    ) -> Result<CubeContext, CompilationError> {
        let df_state = Arc::new(session_ctx.state.write().clone());
        let cube_ctx = CubeContext::new(
            df_state,
            meta.clone(),
            self.session_manager.clone(),
            state.clone(),
        );

        Ok(cube_ctx)
    }

    fn create_session_ctx(
        &self,
        state: Arc<SessionState>,
    ) -> Result<DFSessionContext, CompilationError> {
        let query_planner = Arc::new(CubeQueryPlanner::new(
            self.transport_ref().clone(),
            state.get_load_request_meta("sql"),
            self.config_ref().clone(),
        ));
        let mut df_state = default_session_builder(
            DFSessionConfig::new()
                .create_default_catalog_and_schema(false)
                .with_information_schema(false)
                .with_default_catalog_and_schema("db", "public"),
        )
        .with_query_planner(query_planner);
        df_state
            .optimizer
            .rules
            // projection_push_down is broken even for non-OLAP queries
            // TODO enable it back
            .retain(|r| r.name() != "projection_push_down");
        let mut ctx = DFSessionContext::with_state(df_state);

        if state.protocol == DatabaseProtocol::MySQL {
            let system_variable_provider =
                VariablesProvider::new(state.clone(), self.session_manager.server.clone());
            let user_defined_variable_provider =
                VariablesProvider::new(state.clone(), self.session_manager.server.clone());

            ctx.register_variable(VarType::System, Arc::new(system_variable_provider));
            ctx.register_variable(
                VarType::UserDefined,
                Arc::new(user_defined_variable_provider),
            );
        }

        // udf
        if state.protocol == DatabaseProtocol::MySQL {
            ctx.register_udf(create_version_udf("8.0.25".to_string()));
            ctx.register_udf(create_db_udf("database".to_string(), state.clone()));
            ctx.register_udf(create_db_udf("schema".to_string(), state.clone()));
            ctx.register_udf(create_current_user_udf(state.clone(), "current_user", true));
            ctx.register_udf(create_user_udf(state.clone()));
        } else if state.protocol == DatabaseProtocol::PostgreSQL {
            ctx.register_udf(create_version_udf(
                "PostgreSQL 14.2 on x86_64-cubesql".to_string(),
            ));
            ctx.register_udf(create_db_udf("current_database".to_string(), state.clone()));
            ctx.register_udf(create_db_udf("current_catalog".to_string(), state.clone()));
            ctx.register_udf(create_db_udf("current_schema".to_string(), state.clone()));
            ctx.register_udf(create_current_user_udf(
                state.clone(),
                "current_user",
                false,
            ));
            ctx.register_udf(create_current_user_udf(state.clone(), "user", false));
            ctx.register_udf(create_session_user_udf(state.clone()));
        }

        ctx.register_udf(create_connection_id_udf(state.clone()));
        ctx.register_udf(create_pg_backend_pid_udf(state.clone()));
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
        ctx.register_udf(create_format_udf());
        ctx.register_udf(create_format_type_udf());
        ctx.register_udf(create_col_description_udf());
        ctx.register_udf(create_pg_datetime_precision_udf());
        ctx.register_udf(create_pg_numeric_precision_udf());
        ctx.register_udf(create_pg_numeric_scale_udf());
        ctx.register_udf(create_pg_get_userbyid_udf(state.clone()));
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
        ctx.register_udf(create_has_schema_privilege_udf(state.clone()));
        ctx.register_udf(create_has_table_privilege_udf(state.clone()));
        ctx.register_udf(create_has_any_column_privilege_udf(state.clone()));
        ctx.register_udf(create_pg_total_relation_size_udf());
        ctx.register_udf(create_cube_regclass_cast_udf());
        ctx.register_udf(create_pg_get_serial_sequence_udf());
        ctx.register_udf(create_json_build_object_udf());
        ctx.register_udf(create_regexp_substr_udf());
        ctx.register_udf(create_regexp_instr_udf());
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
        ctx.register_udf(create_age_udf());
        ctx.register_udf(create_pg_get_partkeydef_udf());
        ctx.register_udf(create_pg_relation_size_udf());
        ctx.register_udf(create_pg_postmaster_start_time_udf());
        ctx.register_udf(create_txid_current_udf());
        ctx.register_udf(create_pg_is_in_recovery_udf());
        ctx.register_udf(create_pg_tablespace_location_udf());

        // udaf
        ctx.register_udaf(create_measure_udaf());
        ctx.register_udaf(create_patch_measure_udaf());
        ctx.register_udaf(create_xirr_udaf());

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

        Ok(ctx)
    }

    fn create_logical_plan(
        &self,
        cube_ctx: &CubeContext,
        stmt: &Self::AstStatementType,
    ) -> Result<(LogicalPlan, Self::PlanMetadataType), DataFusionError> {
        let df_query_planner = SqlToRel::new_with_options(cube_ctx, true);
        let plan =
            df_query_planner.statement_to_plan(DFStatement::Statement(Box::new(stmt.clone())))?;

        Ok((plan, ()))
    }

    fn compiler_cache_ref(&self) -> &Arc<dyn CompilerCache> {
        &self.session_manager.server.compiler_cache
    }

    fn transport_ref(&self) -> &Arc<dyn TransportService> {
        &self.session_manager.server.transport
    }

    fn config_ref(&self) -> &Arc<dyn ConfigObj> {
        &self.session_manager.server.config_obj
    }

    fn sanitize_statement(&self, stmt: &Self::AstStatementType) -> Self::AstStatementType {
        SensitiveDataSanitizer::new().replace(stmt.clone())
    }

    async fn get_cache_entry(
        &self,
        state: Arc<SessionState>,
    ) -> Result<Arc<CompilerCacheEntry>, CompilationError> {
        self.compiler_cache_ref()
            .get_cache_entry(
                state.auth_context().ok_or_else(|| {
                    CompilationError::internal("Unable to get auth context".to_string())
                })?,
                state.protocol.clone(),
            )
            .await
            .map_err(|e| CompilationError::internal(e.to_string()))
    }
}

fn is_olap_query(parent: &LogicalPlan) -> Result<bool, CompilationError> {
    pub struct FindCubeScanNodeVisitor(bool);

    impl PlanVisitor for FindCubeScanNodeVisitor {
        type Error = CompilationError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
            if let LogicalPlan::Extension(ext) = plan {
                let node = ext.node.as_any();
                if node.is::<CubeScanNode>()
                    || node.is::<CubeScanWrapperNode>()
                    || node.is::<CubeScanWrappedSqlNode>()
                {
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
