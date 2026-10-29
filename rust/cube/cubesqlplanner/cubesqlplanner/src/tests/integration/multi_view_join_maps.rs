//! Multi-view queries over a nested view join path return the rows of the
//! same query over the cubes.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/multi_view_join_maps.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_fact_tables.sql";

fn rows(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

async fn assert_same_rows(view_query: &str, cube_query: &str, expected: Vec<Vec<String>>) {
    let ctx = create_context();
    ctx.build_sql(view_query).unwrap();
    ctx.build_sql(cube_query).unwrap();

    let Some(view_result) = ctx.try_execute_pg(view_query, SEED).await else {
        return;
    };
    let cube_result = ctx.try_execute_pg(cube_query, SEED).await.unwrap();

    assert_eq!(table_rows(&cube_result), expected);
    assert_eq!(
        table_rows(&view_result),
        expected,
        "view query:\n{view_result}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dimension_view_measure_next_to_fact_view_measure() {
    assert_same_rows(
        indoc! {"
            measures:
              - customers_view.count
              - orders_view.total_amount
            dimensions:
              - customers_view.city
            order:
              - id: customers_view.city
        "},
        indoc! {"
            measures:
              - customers.count
              - orders.total_amount
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        rows(&[
            &["Boston", "1", "550.00"],
            &["Chicago", "1", "NULL"],
            &["New York", "2", "825.00"],
        ]),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fact_view_measure_next_to_dimension_view_measure() {
    assert_same_rows(
        indoc! {"
            measures:
              - returns_view.total_refund
              - customers_view.avg_lifetime_value
            dimensions:
              - customers_view.city
            order:
              - id: customers_view.city
        "},
        indoc! {"
            measures:
              - returns.total_refund
              - customers.avg_lifetime_value
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        rows(&[
            &["Boston", "130.00", "2000.0000000000000000"],
            &["Chicago", "100.00", "500.0000000000000000"],
            &["New York", "100.00", "1250.0000000000000000"],
        ]),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_two_fact_views_by_dimension_of_third_view() {
    assert_same_rows(
        indoc! {"
            measures:
              - orders_view.total_amount
              - returns_view.total_refund
            dimensions:
              - customers_view.city
            order:
              - id: customers_view.city
        "},
        indoc! {"
            measures:
              - orders.total_amount
              - returns.total_refund
            dimensions:
              - customers.city
            order:
              - id: customers.city
        "},
        rows(&[
            &["Boston", "550.00", "130.00"],
            &["Chicago", "NULL", "100.00"],
            &["New York", "825.00", "100.00"],
        ]),
    )
    .await;
}
