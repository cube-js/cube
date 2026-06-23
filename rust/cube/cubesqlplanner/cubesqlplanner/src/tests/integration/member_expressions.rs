use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::options_member::OptionsMember;
use crate::cube_bridge::subquery_join::SubqueryJoin;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockMemberExpressionDefinition, MockMemberSql,
    MockSchema, MockSubqueryJoin,
};
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
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

// Mirrors a SQL-API `subqueryJoins` entry: opaque sub-query `sql`, a join type
// and alias, and an `on` condition expressed as a member expression (the alias
// arrives pre-quoted and is referenced verbatim inside `on`).
fn make_subquery_join(
    sql: &str,
    alias: &str,
    join_type: &str,
    on_cube: &str,
    on_sql: &str,
) -> Result<Rc<dyn SubqueryJoin>, CubeError> {
    let on_member_sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new(on_sql)?);
    let on: Rc<dyn crate::cube_bridge::member_expression::MemberExpressionDefinition> = Rc::new(
        MockMemberExpressionDefinition::builder()
            .cube_name(Some(on_cube.to_string()))
            .expression(MemberExpressionExpressionDef::Sql(on_member_sql))
            .build(),
    );
    Ok(Rc::new(
        MockSubqueryJoin::builder()
            .sql(sql.to_string())
            .join_type(Some(join_type.to_string()))
            .alias(alias.to_string())
            .on(on)
            .build(),
    ))
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
        values: Some(vec![
            crate::cube_bridge::base_query_options::FilterValue::Str("completed".to_string()),
        ]),
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

// COUNT(*) is a dependency-free measure expression: it references no members, so it
// resolves an empty join-hint set, and with no dimensions/filters to seed the join the
// planner must fall back to the measure's own cube (`orders`) instead of building a
// null join. Regression for hint-less member-expression measures: without the fallback,
// the empty hints reach `JoinGraph.build_join([])`, which yields no joinable cube.
// integration_basic seed has 9 orders → 9.
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_measure_count_star_no_hints() {
    let ctx = create_context();
    let expr = make_measure_expression("total_count", "orders", "COUNT(*)");

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

// Multiplied dim-only ME: a measure expression evaluating to a
// dimension expression (MAX over `customers.city`) used together
// with an `orders` dimension. `orders→customers` is many_to_one, so
// customers measures get multiplied — and the dim-only ME fast-path
// in the classifier feeds the AggregateMultipliedBuilder with a
// derived owning cube `customers` (not the symbol's own cube). This
// test exists to lock that path in for the upcoming
// `MeasureGroup { join, measures }` unification.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_dim_only_me_measure() {
    let ctx = create_context();
    let expr = make_measure_expression("city_max", "customers", "MAX({customers.city})");
    let mut measures = members_from_strings(vec!["orders.count"]);
    measures.push(expr);

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(measures))
            .dimensions(Some(members_from_strings(vec!["orders.status"])))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }
}

const TOP_ORDERS_SUBQUERY: &str =
    "SELECT status, SUM(amount) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 2";

// A SQL-API grouped sub-query join: the opaque sub-query (with its inner
// ORDER BY/LIMIT) must be emitted verbatim, INNER-joined under the
// pre-quoted alias used verbatim in the ON condition.
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_join_grouped() -> Result<(), CubeError> {
    let ctx = create_context();
    let subquery_join = make_subquery_join(
        TOP_ORDERS_SUBQUERY,
        "\"top_orders\"",
        "INNER",
        "orders",
        "{orders.status} = \"top_orders\".status",
    )?;

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(members_from_strings(vec!["orders.count"])))
            .dimensions(Some(members_from_strings(vec!["orders.status"])))
            .subquery_joins(Some(vec![subquery_join]))
            .build(),
    );

    let sql = ctx.build_sql_from_options(options.clone())?;

    // The opaque sub-query is emitted verbatim (inner ORDER BY/LIMIT preserved).
    assert!(
        sql.contains(TOP_ORDERS_SUBQUERY),
        "sub-query SQL should be emitted verbatim, got: {sql}"
    );
    assert!(
        sql.contains("INNER JOIN"),
        "expected INNER JOIN, got: {sql}"
    );
    // The pre-quoted alias is emitted as-is, not re-quoted.
    assert!(
        sql.contains("\"top_orders\"") && !sql.contains("\"\"\"top_orders\"\"\""),
        "alias should be emitted verbatim (no re-quoting), got: {sql}"
    );
    assert!(
        sql.contains("\"top_orders\".status"),
        "expected ON condition referencing the sub-query alias, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_join_grouped_left() -> Result<(), CubeError> {
    let ctx = create_context();
    let subquery_join = make_subquery_join(
        TOP_ORDERS_SUBQUERY,
        "\"top_orders\"",
        "LEFT",
        "orders",
        "{orders.status} = \"top_orders\".status",
    )?;

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(members_from_strings(vec!["orders.count"])))
            .dimensions(Some(members_from_strings(vec!["orders.status"])))
            .subquery_joins(Some(vec![subquery_join]))
            .build(),
    );

    let sql = ctx.build_sql_from_options(options.clone())?;

    assert!(
        sql.contains(TOP_ORDERS_SUBQUERY),
        "sub-query SQL should be emitted verbatim, got: {sql}"
    );
    assert!(sql.contains("LEFT JOIN"), "expected LEFT JOIN, got: {sql}");
    // The pre-quoted alias is emitted as-is, not re-quoted.
    assert!(
        sql.contains("\"top_orders\"") && !sql.contains("\"\"\"top_orders\"\"\""),
        "alias should be emitted verbatim (no re-quoting), got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }

    Ok(())
}

#[test]
fn test_subquery_join_unknown_join_type() -> Result<(), CubeError> {
    let ctx = create_context();
    let subquery_join = make_subquery_join(
        TOP_ORDERS_SUBQUERY,
        "\"top_orders\"",
        "RIGHT",
        "orders",
        "{orders.status} = \"top_orders\".status",
    )?;

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(members_from_strings(vec!["orders.count"])))
            .dimensions(Some(members_from_strings(vec!["orders.status"])))
            .subquery_joins(Some(vec![subquery_join]))
            .build(),
    );

    let err = ctx
        .build_sql_from_options(options)
        .expect_err("unsupported join type should be rejected");
    assert!(
        err.message.contains("Unsupported join type") && err.message.contains("RIGHT"),
        "expected a clear unsupported-join-type error, got: {}",
        err.message
    );

    Ok(())
}

// Empty-members case: only the sub-query column is projected, so no `orders`
// member selects the base cube. The join root is derived from the ON
// dependencies so the base cube and the sub-query join are still emitted.
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_join_empty_members() -> Result<(), CubeError> {
    let ctx = create_context();
    let subquery_join = make_subquery_join(
        TOP_ORDERS_SUBQUERY,
        "\"top_orders\"",
        "INNER",
        "orders",
        "{orders.status} = \"top_orders\".status",
    )?;
    let top_status = make_dim_expression("top_status", "orders", "\"top_orders\".status");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .dimensions(Some(vec![top_status]))
            .subquery_joins(Some(vec![subquery_join]))
            .build(),
    );

    let sql = ctx.build_sql_from_options(options.clone())?;

    assert!(
        sql.contains("INNER JOIN"),
        "expected INNER JOIN, got: {sql}"
    );
    assert!(
        sql.contains(TOP_ORDERS_SUBQUERY),
        "sub-query SQL should be emitted verbatim, got: {sql}"
    );
    assert!(
        sql.contains("\"top_orders\".status"),
        "expected reference to the sub-query alias, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }

    Ok(())
}

#[test]
fn test_subquery_join_no_cube_reference_in_on() -> Result<(), CubeError> {
    let ctx = create_context();
    let subquery_join = make_subquery_join(
        TOP_ORDERS_SUBQUERY,
        "\"top_orders\"",
        "INNER",
        "orders",
        "\"top_orders\".status IS NOT NULL",
    )?;
    let top_status = make_dim_expression("top_status", "orders", "\"top_orders\".status");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .dimensions(Some(vec![top_status]))
            .subquery_joins(Some(vec![subquery_join]))
            .build(),
    );

    let err = ctx
        .build_sql_from_options(options)
        .expect_err("sub-query join with no cube reference in ON should be rejected");
    assert!(
        err.message
            .contains("Sub-query join requires its ON condition to reference"),
        "expected a clear no-cube-reference error, got: {}",
        err.message
    );

    Ok(())
}
