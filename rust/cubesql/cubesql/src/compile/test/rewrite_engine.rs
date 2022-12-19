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
            rewriter::RewriteRules,
            rules::{
                dates::DateRules, filters::FilterRules, members::MemberRules, order::OrderRules,
                split::SplitRules,
            },
            LogicalPlanLanguage,
        },
        rewrite_statement, QueryPlanner,
    },
    sql::session::DatabaseProtocol,
};

pub async fn cube_context() -> CubeContext {
    let session = get_test_session(DatabaseProtocol::PostgreSQL).await;
    let planner = QueryPlanner::new(
        session.state.clone(),
        get_test_tenant_ctx(),
        session.session_manager.clone(),
    );
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
    let mut converter = LogicalPlanToLanguageConverter::new(context);
    converter.add_logical_plan(&plan).unwrap();

    converter.take_runner()
}

pub fn rewrite_rules(
    cube_context: Arc<CubeContext>,
) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
    let rules: Vec<Box<dyn RewriteRules>> = vec![
        Box::new(MemberRules::new(cube_context.clone())),
        Box::new(FilterRules::new(cube_context.clone())),
        Box::new(DateRules::new(cube_context.clone())),
        Box::new(OrderRules::new(cube_context.clone())),
        Box::new(SplitRules::new(cube_context.clone())),
    ];
    let mut rewrites = Vec::new();
    for r in rules {
        rewrites.extend(r.rewrite_rules());
    }
    rewrites
}
