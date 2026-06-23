use crate::cube_bridge::member_expression::{ExpressionStruct, MemberExpressionExpressionDef};
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::options_member::OptionsMember;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockExpressionStruct,
    MockMemberExpressionDefinition, MockMemberSql, MockSchema, MockStructWithSqlMember,
};
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;
use std::rc::Rc;

fn create_test_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/many_to_one_views.yaml");
    TestContext::new(schema).unwrap()
}

fn make_member_expression(expression_name: &str, cube_name: &str, sql: &str) -> OptionsMember {
    let member_sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new(sql).unwrap());
    let expr = MockMemberExpressionDefinition::builder()
        .expression_name(Some(expression_name.to_string()))
        .name(Some(expression_name.to_string()))
        .cube_name(Some(cube_name.to_string()))
        .expression(MemberExpressionExpressionDef::Sql(member_sql))
        .build();
    OptionsMember::MemberExpression(Rc::new(expr))
}

// Builds a `PatchMeasure` member expression that adds one ad-hoc CASE-WHEN
// filter to `source_measure` — the SQL-API mechanism for pushing a filter
// inside a measure's aggregation.
fn make_patched_measure(
    expression_name: &str,
    cube_name: &str,
    source_measure: &str,
    filter_sql: &str,
) -> OptionsMember {
    let filter = MockStructWithSqlMember::builder()
        .sql(filter_sql.to_string())
        .build();
    let expr_struct = MockExpressionStruct::builder()
        .expression_type("PatchMeasure".to_string())
        .source_measure(Some(source_measure.to_string()))
        .add_filters(Some(vec![Rc::new(filter)]))
        .build();
    let expr = MockMemberExpressionDefinition::builder()
        .expression_name(Some(expression_name.to_string()))
        .name(Some(expression_name.to_string()))
        .cube_name(Some(cube_name.to_string()))
        .expression(MemberExpressionExpressionDef::Struct(
            Rc::new(expr_struct) as Rc<dyn ExpressionStruct>
        ))
        .build();
    OptionsMember::MemberExpression(Rc::new(expr))
}

fn build_options_with_member_expression(
    ctx: &TestContext,
    extra_measure: OptionsMember,
) -> Rc<dyn crate::cube_bridge::base_query_options::BaseQueryOptions> {
    let mut measures = members_from_strings(vec![
        "many_to_one_view.root_val_avg",
        "many_to_one_view.child_val_avg",
    ]);
    measures.push(extra_measure);

    Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(measures))
            .dimensions(Some(members_from_strings(vec![
                "many_to_one_view.root_dim",
                "many_to_one_view.child_dim",
            ])))
            .build(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_to_one_view_base_query() {
    let ctx = create_test_context();

    let query_yaml = indoc! {"
        measures:
          - many_to_one_view.root_val_avg
          - many_to_one_view.child_val_avg
        dimensions:
          - many_to_one_view.root_dim
          - many_to_one_view.child_dim
    "};

    ctx.build_sql(query_yaml).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "many_to_one_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_to_one_view_one_sum() {
    let ctx = create_test_context();
    let expr = make_member_expression("one_sum", "many_to_one_view", "SUM(1)");
    let options = build_options_with_member_expression(&ctx, expr);
    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "many_to_one_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// JS member-expressions-on-views.test.ts "many_to_one_view › one_sum":
// shares (root.dim, child.dim) across several root rows so the child
// measure feels the row multiplication. With proper dedup,
// (foo, foo).child_val_avg = (100 + 300) / 2 = 200; without dedup the
// many_to_one join injects child #1 twice and yields 166.66.
#[tokio::test(flavor = "multi_thread")]
async fn test_many_to_one_view_one_sum_multiplied() {
    let ctx = create_test_context();
    let expr = make_member_expression("one_sum", "many_to_one_view", "SUM(1)");
    let options = build_options_with_member_expression(&ctx, expr);
    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "many_to_one_multiplied_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_to_one_view_root_val_sum() {
    let ctx = create_test_context();
    let expr = make_member_expression(
        "root_val_sum_expr",
        "many_to_one_view",
        "{many_to_one_view.root_val_sum}",
    );
    let options = build_options_with_member_expression(&ctx, expr);
    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "many_to_one_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_to_one_view_root_distinct_dim() {
    let ctx = create_test_context();
    let expr = make_member_expression(
        "root_distinct_dim",
        "many_to_one_view",
        "COUNT(DISTINCT {many_to_one_view.root_test_dim})",
    );
    let options = build_options_with_member_expression(&ctx, expr);
    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "many_to_one_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_to_one_view_child_val_sum() {
    let ctx = create_test_context();
    let expr = make_member_expression(
        "child_val_sum_expr",
        "many_to_one_view",
        "{many_to_one_view.child_val_sum}",
    );
    let options = build_options_with_member_expression(&ctx, expr);
    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "many_to_one_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_to_one_view_child_distinct_dim() {
    let ctx = create_test_context();
    let expr = make_member_expression(
        "child_distinct_dim",
        "many_to_one_view",
        "COUNT(DISTINCT {many_to_one_view.child_test_dim})",
    );
    let options = build_options_with_member_expression(&ctx, expr);
    ctx.build_sql_from_options(options.clone()).unwrap();

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "many_to_one_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// A PatchMeasure adding a CASE-WHEN filter to a measure exposed through a view
// (`root_val_sum`, a SUM) must resolve the reference chain to the owning cube
// measure so the filter is pushed inside the aggregation.
// root_test_dim='rt_x' → roots 1,2 → SUM = 10 + 20 = 30.
#[tokio::test(flavor = "multi_thread")]
async fn test_many_to_one_view_patched_measure_filter() -> Result<(), CubeError> {
    let ctx = create_test_context();
    let expr = make_patched_measure(
        "filtered_root_sum",
        "many_to_one_view",
        "many_to_one_view.root_val_sum",
        "{many_to_one_view.root_test_dim} = 'rt_x'",
    );

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![expr]))
            .build(),
    );

    let sql = ctx.build_sql_from_options(options.clone())?;
    // The ad-hoc filter is pushed inside the aggregation (measure_filter.rs
    // renders `CASE WHEN <filter> THEN <result> END`), not as an outer WHERE.
    assert!(
        sql.contains("CASE WHEN"),
        "ad-hoc filter must be pushed inside the aggregation, got: {sql}"
    );

    if let Some(result) = ctx
        .try_execute_pg_from_options(options, "many_to_one_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }

    Ok(())
}
