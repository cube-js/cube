//! A multi-stage ratio over two facts that share dimension cubes but never join
//! each other directly.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const MODEL: &str = indoc! {r#"
cubes:
  - name: items
    sql: "SELECT 1 AS id, 'Bakery' AS department"
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: department
        sql: "{CUBE}.department"
        type: string
  - name: locations
    sql: "SELECT 1 AS id, 'West' AS region"
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: region
        sql: "{CUBE}.region"
        type: string
  - name: sales_line_item
    sql: "SELECT 1"
    joins:
      - name: items
        sql: "{CUBE}.item_id = {items}.id"
        relationship: many_to_one
      - name: locations
        sql: "{CUBE}.location_id = {locations}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
    measures:
      - name: transactions
        sql: "{CUBE}.transaction_id"
        type: count_distinct
  - name: item_location_sales
    sql: "SELECT 1"
    joins:
      - name: items
        sql: "{CUBE}.item_id = {items}.id"
        relationship: many_to_one
      - name: locations
        sql: "{CUBE}.location_id = {locations}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
    measures:
      - name: sales_amount
        sql: "{CUBE}.sales_amount"
        type: sum
views:
  - name: retail_analysis
    cubes:
      - join_path: item_location_sales
        includes:
          - sales_amount
      - join_path: sales_line_item
        includes:
          - transactions
      - join_path: items
        includes:
          - department
      - join_path: locations
        includes:
          - region
    measures:
      - name: aov_basket
        type: number
        multi_stage: true
        sql: "{CUBE.sales_amount} / NULLIF({CUBE.transactions}, 0)"
"#};

#[test]
fn ratio_filtered_by_a_shared_dimension_outside_the_selection() {
    // Nothing joins `locations` to `items` without a fact, so the root query
    // must not ask for a join of its own.
    let schema = MockSchema::from_yaml(MODEL).unwrap();
    let sql = TestContext::new(schema)
        .unwrap()
        .build_sql(indoc! {r#"
            measures:
              - retail_analysis.aov_basket
            dimensions:
              - retail_analysis.region
            filters:
              - member: retail_analysis.department
                operator: equals
                values:
                  - "Bakery"
        "#})
        .unwrap();
    assert!(
        sql.contains("sum(\"item_location_sales\".sales_amount)"),
        "{sql}"
    );
    assert!(
        sql.contains("\"sales_line_item\".item_id = \"items\".id"),
        "{sql}"
    );
    assert!(
        sql.contains("\"item_location_sales\".item_id = \"items\".id"),
        "{sql}"
    );
}
