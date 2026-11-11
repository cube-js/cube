//! Tests for the rollup build query of a rolling window measure.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn context() -> TestContext {
    TestContext::new(
        MockSchema::from_yaml(indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                joins:
                  - name: products
                    relationship: many_to_one
                    sql: "{CUBE}.product_id = {products}.id"
                dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
              - name: products
                sql: "SELECT * FROM products"
                dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: created_at
                    type: time
                    sql: created_at
                measures:
                  - name: running_price
                    type: sum
                    sql: price
                    rolling_window:
                      trailing: unbounded
        "#})
        .unwrap(),
    )
    .unwrap()
}

// Multiplied, the window is not additive over stored rows, so the build
// computes it, once, rather than also as a plain leaf.
#[test]
fn a_rollup_build_computes_the_window_of_a_multiplied_rolling_measure_once() {
    let sql = context()
        .build_sql(indoc! {r#"
            dimensions:
              - orders.id
              - products.id
            measures:
              - products.running_price
            time_dimensions:
              - dimension: products.created_at
                granularity: day
                dateRange:
                  - "2024-01-01"
                  - "2024-01-31"
            pre_aggregation_query: true
        "#})
        .unwrap();

    assert!(sql.contains("time_series"), "{sql}");
    assert_eq!(sql.matches("SELECT DISTINCT").count(), 1, "{sql}");
}
