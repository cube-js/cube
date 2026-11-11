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

// The build stores the rows a window is read from, so the measure is a plain
// sum, computed once even when the join multiplies it.
#[test]
fn a_rollup_build_computes_a_multiplied_rolling_measure_once() {
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

    assert_eq!(sql.matches("sum(").count(), 1, "{sql}");
}
