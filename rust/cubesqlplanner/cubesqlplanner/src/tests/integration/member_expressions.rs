use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::options_member::OptionsMember;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockMemberExpressionDefinition, MockMemberSql,
    MockSchema,
};
use crate::test_fixtures::test_utils::TestContext;
use std::rc::Rc;

const SEED: &str = "integration_basic_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

fn make_dim_expression(name: &str, cube: &str, sql: &str) -> OptionsMember {
    let member_sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new(sql).unwrap());
    let expr = MockMemberExpressionDefinition::builder()
        .expression_name(Some(name.to_string()))
        .name(Some(name.to_string()))
        .cube_name(Some(cube.to_string()))
        .expression(MemberExpressionExpressionDef::Sql(member_sql))
        .build();
    OptionsMember::MemberExpression(Rc::new(expr))
}

fn make_measure_expression(name: &str, cube: &str, sql: &str) -> OptionsMember {
    let member_sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new(sql).unwrap());
    let expr = MockMemberExpressionDefinition::builder()
        .expression_name(Some(name.to_string()))
        .name(Some(name.to_string()))
        .cube_name(Some(cube.to_string()))
        .expression(MemberExpressionExpressionDef::Sql(member_sql))
        .build();
    OptionsMember::MemberExpression(Rc::new(expr))
}

// LOWER(status) as dimension expression + count
// completed:5, pending:3, cancelled:1
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_dim_lower() {
    let ctx = create_context();
    let expr = make_dim_expression("lower_status", "orders", "LOWER({orders.status})");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(members_from_strings(vec!["orders.count"])))
            .dimensions(Some(vec![expr]))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// UPPER(city) as dimension expression + count
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_dim_upper_city() {
    let ctx = create_context();
    let expr = make_dim_expression("upper_city", "orders", "UPPER({customers.city})");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(members_from_strings(vec!["orders.count"])))
            .dimensions(Some(vec![expr]))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// LOWER(status) expression + filter status='completed'
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_with_filter() {
    let ctx = create_context();
    let expr = make_dim_expression("lower_status", "orders", "LOWER({orders.status})");

    let filter_item = crate::cube_bridge::base_query_options::FilterItem {
        member: None,
        dimension: Some("orders.status".to_string()),
        operator: Some("equals".to_string()),
        values: Some(vec![Some("completed".to_string())]),
        or: None,
        and: None,
    };

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(members_from_strings(vec!["orders.count"])))
            .dimensions(Some(vec![expr]))
            .filters(Some(vec![filter_item]))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// SUM(amount) as measure expression → 1440
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_measure_sum() {
    let ctx = create_context();
    let expr = make_measure_expression("sum_amount", "orders", "SUM({orders.amount})");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![expr]))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }
}
