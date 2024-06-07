use std::sync::Arc;

use datafusion::{
    logical_plan::LogicalPlan,
    sql::{parser::Statement, planner::SqlToRel},
};
use egg::Rewrite;

use super::{get_test_session, get_test_tenant_ctx};
use crate::{
    compile::{
        engine::provider::CubeContext,
        parser::parse_sql_to_statement,
        rewrite::{
            analysis::LogicalPlanAnalysis,
            converter::{CubeRunner, LogicalPlanToLanguageConverter},
            rewriter::Rewriter,
            LogicalPlanLanguage,
        },
        rewrite_statement, QueryPlanner,
    },
    config::{ConfigObj, ConfigObjImpl},
    sql::session::DatabaseProtocol,
};

pub async fn cube_context() -> CubeContext {
    let meta = get_test_tenant_ctx();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let planner = QueryPlanner::new(session.state.clone(), meta, session.session_manager.clone());
    let ctx = planner.create_execution_ctx();
    let df_state = Arc::new(ctx.state.write().clone());

    CubeContext::new(
        df_state,
        planner.meta.clone(),
        planner.session_manager.clone(),
        planner.state.clone(),
    )
}

pub fn query_to_logical_plan(query: String, context: &CubeContext) -> LogicalPlan {
    let stmt = parse_sql_to_statement(&query, DatabaseProtocol::PostgreSQL, &mut None).unwrap();
    let stmt = rewrite_statement(&stmt);
    let df_query_planner = SqlToRel::new_with_options(context, true);

    return df_query_planner
        .statement_to_plan(Statement::Statement(Box::new(stmt.clone())))
        .unwrap();
}

pub fn rewrite_runner(plan: LogicalPlan, context: Arc<CubeContext>) -> CubeRunner {
    let config_obj = ConfigObjImpl::default();
    let flat_list = config_obj.push_down_pull_up_split();
    let mut converter = LogicalPlanToLanguageConverter::new(context, flat_list);
    converter.add_logical_plan(&plan).unwrap();

    converter.take_runner()
}

pub fn rewrite_rules(
    cube_context: Arc<CubeContext>,
) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
    Rewriter::rewrite_rules(
        cube_context.meta.clone(),
        cube_context.sessions.server.config_obj.clone(),
        true,
    )
}
