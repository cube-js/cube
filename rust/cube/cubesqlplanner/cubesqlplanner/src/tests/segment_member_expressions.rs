use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::options_member::OptionsMember;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockMemberExpressionDefinition, MockMemberSql,
    MockSchema,
};
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use itertools::Itertools;
use std::rc::Rc;

// The segment reads only the joined cube, which is the multiplied side of the
// many_to_one join, so an expression over it is a dimension-only measure on
// that side.
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
          - name: done_with_customer
            sql: "{CUBE.status} = 'done' AND {customers.id} IS NOT NULL"
          - name: is_done
            sql: "{CUBE.status} = 'done'"
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
        measures:
          - name: customer_count
            type: count
    views:
      - name: orders_view
        cubes:
          - join_path: orders
            includes:
              - status
              - count
              - with_customer
              - done_with_customer
              - is_done
          - join_path: orders.customers
            includes:
              - name
              - customer_count
"#};

const SEED: &str = "segment_member_expressions_tables.sql";

// The SQL API reads every view column, segments included, when it falls back
// to an ungrouped scan. Each column arrives as a member expression named after
// the column: the same cube and name as the segment it wraps, which it must
// still be told apart from.
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

fn options(
    ctx: &TestContext,
    measures: Vec<OptionsMember>,
    dimensions: Vec<OptionsMember>,
    ungrouped: bool,
) -> Rc<dyn BaseQueryOptions> {
    Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(measures).filter(|m| !m.is_empty()))
            .dimensions(Some(dimensions).filter(|d| !d.is_empty()))
            .ungrouped(Some(ungrouped))
            .build(),
    )
}

fn context() -> TestContext {
    TestContext::new(MockSchema::from_yaml(SCHEMA).unwrap()).unwrap()
}

fn build(ungrouped: bool) -> String {
    let ctx = context();
    let options = options(
        &ctx,
        vec![segment_column("orders_view", "with_customer")],
        members_from_strings(vec!["orders_view.status"]),
        ungrouped,
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

// Guards the planning only: a grouped query with a non-aggregated column in
// `measures` has no valid SQL, and the SQL API sends such a column as a
// dimension instead.
#[test]
fn test_segment_column_on_view_grouped_plans() {
    let sql = build(false);
    assert!(sql.contains("IS NOT NULL"), "{sql}");
}

// The rows of a result table, cells trimmed, in a stable order: ties under
// the query's own ORDER BY come back in any order.
fn sorted_rows(table: &str) -> Vec<Vec<String>> {
    table
        .lines()
        .skip(2)
        .map(|line| {
            line.split('|')
                .map(|cell| cell.trim().to_string())
                .collect()
        })
        .sorted()
        .collect()
}

fn rows(expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .sorted()
        .collect()
}

// One row per order. The segment's column reads its own row of `customers`,
// so orders sharing a customer stay apart, and an order whose customer is
// missing reads `false`.
#[tokio::test(flavor = "multi_thread")]
async fn test_segment_column_on_view_ungrouped_reads_one_row_per_order() {
    let ctx = context();
    let options = options(
        &ctx,
        vec![segment_column("orders_view", "with_customer")],
        members_from_strings(vec!["orders_view.status"]),
        true,
    );
    let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await else {
        return;
    };

    assert_eq!(
        sorted_rows(&result),
        rows(&[
            &["new", "t"],
            &["new", "f"],
            &["done", "t"],
            &["done", "f"],
            &["done", "t"],
        ]),
        "{result}"
    );
}

// A measure of the root cube beside the segment's column: both are read from
// the same rows, not from two scans paired up by status.
#[tokio::test(flavor = "multi_thread")]
async fn test_segment_column_beside_measure_on_view_ungrouped_reads_one_row_per_order() {
    let ctx = context();
    let mut measures = members_from_strings(vec!["orders_view.count"]);
    measures.push(segment_column("orders_view", "with_customer"));
    let options = options(
        &ctx,
        measures,
        members_from_strings(vec!["orders_view.status"]),
        true,
    );
    let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await else {
        return;
    };

    assert_eq!(
        sorted_rows(&result),
        rows(&[
            &["new", "1", "t"],
            &["new", "1", "f"],
            &["done", "1", "t"],
            &["done", "1", "f"],
            &["done", "1", "t"],
        ]),
        "{result}"
    );
}

// A segment over both sides of the join is still one value per row: without
// aggregation there is no multiplication for it to be refused over.
#[tokio::test(flavor = "multi_thread")]
async fn test_two_cube_segment_column_on_view_ungrouped_reads_one_row_per_order() {
    let ctx = context();
    let mut measures = members_from_strings(vec!["orders_view.count"]);
    measures.push(segment_column("orders_view", "done_with_customer"));
    let options = options(
        &ctx,
        measures,
        members_from_strings(vec!["orders_view.status"]),
        true,
    );
    ctx.build_sql_from_options(options.clone()).unwrap();
    let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await else {
        return;
    };

    assert_eq!(
        sorted_rows(&result),
        rows(&[
            &["new", "1", "f"],
            &["new", "1", "f"],
            &["done", "1", "t"],
            &["done", "1", "f"],
            &["done", "1", "t"],
        ]),
        "{result}"
    );
}

// The segment's column reads the root cube alone, so it is the narrower join
// group here and has to move into the tree of the joined cube's measure.
#[tokio::test(flavor = "multi_thread")]
async fn test_root_segment_column_beside_joined_measure_on_view_ungrouped_reads_one_row_per_order()
{
    let ctx = context();
    let mut measures = members_from_strings(vec!["orders_view.customer_count"]);
    measures.push(segment_column("orders_view", "is_done"));
    let options = options(
        &ctx,
        measures,
        members_from_strings(vec!["orders_view.status"]),
        true,
    );
    let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await else {
        return;
    };

    assert_eq!(
        sorted_rows(&result),
        rows(&[
            &["new", "1", "f"],
            &["new", "NULL", "f"],
            &["done", "1", "t"],
            &["done", "NULL", "t"],
            &["done", "1", "t"],
        ]),
        "{result}"
    );
}
