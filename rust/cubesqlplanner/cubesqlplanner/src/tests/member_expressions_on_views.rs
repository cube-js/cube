use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::options_member::OptionsMember;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockMemberExpressionDefinition, MockMemberSql,
    MockSchema,
};
use crate::test_fixtures::test_utils::TestContext;
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

fn build_query_with_member_expression(
    ctx: &TestContext,
    extra_measure: OptionsMember,
) -> Result<String, cubenativeutils::CubeError> {
    let mut measures = members_from_strings(vec![
        "many_to_one_view.root_val_avg",
        "many_to_one_view.child_val_avg",
    ]);
    measures.push(extra_measure);

    let options = Rc::new(
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
    );

    ctx.build_sql_from_options(options)
}

#[test]
fn test_many_to_one_view_base_query() {
    let ctx = create_test_context();

    let query_yaml = indoc! {"
        measures:
          - many_to_one_view.root_val_avg
          - many_to_one_view.child_val_avg
        dimensions:
          - many_to_one_view.root_dim
          - many_to_one_view.child_dim
    "};

    let result = ctx.build_sql(query_yaml);
    assert!(
        result.is_ok(),
        "Should generate SQL without row multiplication error: {:?}",
        result.err()
    );
}

#[test]
fn test_many_to_one_view_one_sum() {
    let ctx = create_test_context();
    let expr = make_member_expression("one_sum", "many_to_one_view", "SUM(1)");
    let _result = build_query_with_member_expression(&ctx, expr).unwrap();
}

#[test]
fn test_many_to_one_view_root_val_sum() {
    let ctx = create_test_context();
    let expr = make_member_expression(
        "root_val_sum_expr",
        "many_to_one_view",
        "{many_to_one_view.root_val_sum}",
    );
    let result = build_query_with_member_expression(&ctx, expr);
    assert!(
        result.is_ok(),
        "Should generate SQL without row multiplication error: {:?}",
        result.err()
    );
}

#[test]
fn test_many_to_one_view_root_distinct_dim() {
    let ctx = create_test_context();
    let expr = make_member_expression(
        "root_distinct_dim",
        "many_to_one_view",
        "COUNT(DISTINCT {many_to_one_view.root_test_dim})",
    );
    let result = build_query_with_member_expression(&ctx, expr);
    assert!(
        result.is_ok(),
        "Should generate SQL without row multiplication error: {:?}",
        result.err()
    );
}

#[test]
fn test_many_to_one_view_child_val_sum() {
    let ctx = create_test_context();
    let expr = make_member_expression(
        "child_val_sum_expr",
        "many_to_one_view",
        "{many_to_one_view.child_val_sum}",
    );
    let result = build_query_with_member_expression(&ctx, expr);
    assert!(
        result.is_ok(),
        "Should generate SQL without row multiplication error: {:?}",
        result.err()
    );
}

#[test]
fn test_many_to_one_view_child_distinct_dim() {
    let ctx = create_test_context();
    let expr = make_member_expression(
        "child_distinct_dim",
        "many_to_one_view",
        "COUNT(DISTINCT {many_to_one_view.child_test_dim})",
    );
    let result = build_query_with_member_expression(&ctx, expr);
    assert!(
        result.is_ok(),
        "Should generate SQL without row multiplication error: {:?}",
        result.err()
    );
}
