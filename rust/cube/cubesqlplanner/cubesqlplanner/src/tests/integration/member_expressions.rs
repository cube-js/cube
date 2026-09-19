use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::options_member::OptionsMember;
use crate::cube_bridge::subquery_join::SubqueryJoin;
use crate::planner::QueryPropertiesCompiler;
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

// Collapses whitespace so an assertion on the shape of a query does not pin the
// renderer's spacing.
fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
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

// Same hint-less `COUNT(*)` case, but the member expression belongs to a view.
// The view is not a joinable cube, so the fallback cannot seed it as a hint; the
// join must be taken from the other measures of the query (here the
// `COUNT(DISTINCT {orders_view.status})` expression, which pulls in `orders`).
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_measure_count_star_no_hints_on_view() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let distinct_status = make_measure_expression(
        "distinct_status",
        "orders_view",
        "COUNT(DISTINCT {orders_view.status})",
    );
    let total_count = make_measure_expression("total_count", "orders_view", "COUNT(*)");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![distinct_status, total_count]))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "integration_multi_fact_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// The only other measure of the query is `amount_share`, which depends on a
// multi-stage measure. Such a measure plans its joins separately and so gets no
// per-measure hints entry, but its hints still count towards its view's - which
// is the only thing that gives the hint-less `COUNT(*)` a cube to query here,
// since `orders_view` has no join map to fall back to.
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_measure_count_star_no_hints_beside_multi_stage_measure() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "orders_view", "COUNT(*)");
    let mut measures = members_from_strings(vec!["orders_view.amount_share"]);
    measures.push(total_count);

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(measures))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "integration_multi_fact_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// Hint-less `COUNT(*)` on a view in a genuinely multi-fact query: `orders_count`
// and `returns_count` sit on two different facts that fan out from `customers`.
// Both are members of this view, so its hints are the union over both facts, and
// the member expression forms a third group over the fan-out tree, counting the
// joined row set of the view -
// which is what `COUNT(*)` over a view means. The legacy planner renders the
// whole query over that same fan-out tree, so it counts the same rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_measure_count_star_no_hints_on_multi_fact_view() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "customer_overview", "COUNT(*)");
    let mut measures = members_from_strings(vec![
        "customer_overview.orders_count",
        "customer_overview.returns_count",
    ]);
    measures.push(total_count);

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(measures))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "integration_multi_fact_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// A hint-less `COUNT(*)` member expression on a view as the *only* query member,
// where the view has a join map: every path in it starts at `customers`, so that
// is the cube to query. BI tools send such member-less profiling queries against
// a view, so they must resolve.
#[tokio::test(flavor = "multi_thread")]
async fn test_expr_measure_count_star_only_member_on_view_with_join_map() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "customer_overview", "COUNT(*)");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![total_count]))
            .build(),
    );

    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "integration_multi_fact_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// The join map of `nested_root_view` holds paths headed by different cubes, but
// `customers` heads one path only because it is reached from `returns` in
// another - so `returns` is the single root and the query resolves against it.
#[test]
fn test_expr_measure_count_star_only_member_on_view_with_nested_join_map() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "nested_root_view", "COUNT(*)");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![total_count]))
            .build(),
    );

    let sql = normalize_sql(&ctx.build_sql_from_options(options).unwrap());
    // Anchored on the join structure rather than on a cube name being absent from
    // the text: the point is that the tree is `returns` by itself. A rule that
    // took the first head instead of the unreached one would root at `customers`
    // and join `orders` onto it.
    assert!(
        sql.contains("FROM returns AS") && !sql.contains("JOIN"),
        "expected the query to resolve against the root cube `returns` alone, got: {sql}"
    );
}

// The same shape as the test below, but with a *dimension* of the other view
// instead of a measure - and it is not rejected. Dimensions land in `base_hints`,
// which is not view-scoped, so the member expression never reaches the same-view
// fallback and counts rows of `customers`, a cube `orders_view` is not built on.
//
// This pins a known hole rather than desired behaviour: the legacy planner does
// the same, and closing it is a wider change than this fix (see the note on
// `MultiFactJoinGroups::fallback_hints_for_measure`). If it is ever closed, this
// test flips to expecting the same rejection as the one below.
#[test]
fn test_expr_measure_count_star_no_hints_beside_other_view_dimension() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "orders_view", "COUNT(*)");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![total_count]))
            .dimensions(Some(members_from_strings(vec!["customer_overview.city"])))
            .build(),
    );

    let sql = normalize_sql(&ctx.build_sql_from_options(options).unwrap());
    assert!(
        sql.contains("FROM customers AS") && !sql.contains("JOIN"),
        "expected the hole to stand: the count resolves against `customers` alone, got: {sql}"
    );
}

// A hint-less `COUNT(*)` on `orders_view` next to a measure of a *different*
// view. `customer_overview.returns_count` pulls in `customers` and `returns`,
// neither of which `orders_view` is built on, so those hints must not be
// borrowed - counting rows of `customers` joined to `returns` would answer a
// question nobody asked. Nothing is left to resolve from, so the query is
// rejected.
#[test]
fn test_expr_measure_count_star_no_hints_beside_other_view_measure() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "orders_view", "COUNT(*)");
    let mut measures = members_from_strings(vec!["customer_overview.returns_count"]);
    measures.push(total_count);

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(measures))
            .build(),
    );

    let err = ctx
        .build_sql_from_options(options)
        .expect_err("a view member expression must not borrow the join of an unrelated view");
    assert!(
        err.message.contains("Can't resolve the cube to query"),
        "expected a clear unresolvable-cube error, got: {}",
        err.message
    );
}

// The join map of `two_roots_view` holds paths under two different roots, so
// there is no one cube the view is rooted at. Which one gets counted would come
// down to the order the view lists its cubes, so the query is rejected instead.
#[test]
fn test_expr_measure_count_star_only_member_on_view_with_ambiguous_join_map() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "two_roots_view", "COUNT(*)");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![total_count]))
            .build(),
    );

    let err = ctx
        .build_sql_from_options(options)
        .expect_err("a view whose join map has several roots should be rejected");
    assert!(
        err.message.contains("don't share a single root")
            && err.message.contains("orders")
            && err.message.contains("returns"),
        "expected an ambiguous-root error naming both roots, got: {}",
        err.message
    );
}

// Every path of `cyclic_paths_view` is headed by a cube another path reaches, so
// the paths lead in a circle. That is a different fault from several roots and
// says so, rather than degrading into the generic nothing-to-join-from error.
#[test]
fn test_expr_measure_count_star_only_member_on_view_with_cyclic_join_map() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "cyclic_paths_view", "COUNT(*)");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![total_count]))
            .build(),
    );

    let err = ctx
        .build_sql_from_options(options)
        .expect_err("a view whose join paths are cyclic should be rejected");
    assert!(
        err.message.contains("join paths of that view are cyclic")
            && err.message.contains("orders.customers"),
        "expected a cyclic-join-map error listing the paths, got: {}",
        err.message
    );
}

// A hint-less `COUNT(*)` member expression on a view as the *only* query member:
// the view is not a joinable cube, nothing else seeds the join, and the view has
// no join map to fall back to, because a view over a single directly joinable cube
// records no path. So there is no cube to query.
//
// This is a known limitation, not the desired end state: a one-cube view is the
// most common shape, and the cube is knowable - the view does record `orders` as
// an included cube, it is just dropped from the join map as "no path needed".
// Lifting it means either exposing the view's included cubes on the cube bridge or
// keeping single-element paths in the join map, and the latter turns every root
// cube hint from `Single` into `Vector` across both planners - too wide to carry
// here. The legacy planner fails on this query too, so nothing regresses; what
// this test locks in is a clear error instead of a bridge deserialization failure
// on the null join tree.
#[test]
fn test_expr_measure_count_star_only_member_on_view() {
    let schema = MockSchema::from_yaml_file("common/integration_views.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let total_count = make_measure_expression("total_count", "orders_view", "COUNT(*)");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![total_count]))
            .build(),
    );

    let err = ctx
        .build_sql_from_options(options)
        .expect_err("a view member expression with nothing to join from should be rejected");
    assert!(
        err.message.contains("Can't resolve the cube to query"),
        "expected a clear unresolvable-cube error, got: {}",
        err.message
    );
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

// The compiled ON condition participates in `QueryProperties` equality: two
// otherwise-identical queries whose sub-query joins differ only in their ON
// must not compare equal, while identical ONs must (structural, not pointer,
// equality).
#[test]
fn test_subquery_join_on_participates_in_equality() -> Result<(), CubeError> {
    let ctx = create_context();

    let build_qp = |on_sql: &str| -> Result<Rc<crate::planner::QueryProperties>, CubeError> {
        let subquery_join = make_subquery_join(
            TOP_ORDERS_SUBQUERY,
            "\"top_orders\"",
            "INNER",
            "orders",
            on_sql,
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
        QueryPropertiesCompiler::new(ctx.query_tools().clone()).build(options)
    };

    let qp_eq = build_qp("{orders.status} = \"top_orders\".status")?;
    let qp_ne = build_qp("{orders.status} <> \"top_orders\".status")?;
    let qp_eq_again = build_qp("{orders.status} = \"top_orders\".status")?;

    assert!(
        qp_eq != qp_ne,
        "sub-query joins differing only in their ON must not compare equal"
    );
    assert!(
        qp_eq == qp_eq_again,
        "sub-query joins with identical ON must compare equal"
    );

    Ok(())
}
