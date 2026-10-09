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

// The segment reads only the joined cube, which is the multiplied side of the
// many_to_one join, so the planner classifies an expression over it as a
// multiplied dimension-only measure.
const SCHEMA: &str = indoc! {r#"
    cubes:
      - name: orders
        sql: "SELECT * FROM orders"
        joins:
          - name: customers
            relationship: many_to_one
            sql: "{orders}.customer_id = {customers.id}"
        dimensions:
          - name: id
            type: number
            sql: id
            primary_key: true
          - name: status
            type: string
            sql: status
        measures:
          - name: count
            type: count
        segments:
          - name: with_customer
            sql: "{customers.id} IS NOT NULL"
      - name: customers
        sql: "SELECT * FROM customers"
        dimensions:
          - name: id
            type: number
            sql: id
            primary_key: true
          - name: name
            type: string
            sql: name
    views:
      - name: orders_view
        cubes:
          - join_path: orders
            includes:
              - status
              - count
              - with_customer
          - join_path: orders.customers
            includes:
              - name
"#};

// The SQL API reads every view column, segments included, when it falls back
// to an ungrouped scan. Each column arrives as a member expression named after
// the column, so this one shares its id with the segment it wraps.
fn segment_column(cube_name: &str, segment: &str) -> OptionsMember {
    let sql = format!("{{{cube_name}.{segment}}}");
    let member_sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new(&sql).unwrap());
    let expr = MockMemberExpressionDefinition::builder()
        .expression_name(Some(segment.to_string()))
        .name(Some(segment.to_string()))
        .cube_name(Some(cube_name.to_string()))
        .expression(MemberExpressionExpressionDef::Sql(member_sql))
        .build();
    OptionsMember::MemberExpression(Rc::new(expr))
}

fn build(ungrouped: bool) -> String {
    let ctx = TestContext::new(MockSchema::from_yaml(SCHEMA).unwrap()).unwrap();
    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![segment_column("orders_view", "with_customer")]))
            .dimensions(Some(members_from_strings(vec!["orders_view.status"])))
            .ungrouped(Some(ungrouped))
            .build(),
    );
    ctx.build_sql_from_options(options).unwrap()
}

// Used to recurse without end and overflow the stack, which kills the Node
// process with SIGSEGV.
#[test]
fn test_segment_column_on_view_ungrouped() {
    let sql = build(true);
    assert!(sql.contains("IS NOT NULL"), "{sql}");
}

#[test]
fn test_segment_column_on_view_grouped() {
    let sql = build(false);
    assert!(sql.contains("IS NOT NULL"), "{sql}");
}
