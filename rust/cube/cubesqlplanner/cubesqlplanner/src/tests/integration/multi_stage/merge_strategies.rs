//! Stages merged by FULL JOIN. Postgres cannot run a FULL JOIN on null-safe
//! keys, so the plans are checked, not executed.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SCHEMA: &str = "common/integration_multi_stage.yaml";

// Keys are read from the subselect merging the stages, where they are coalesced.
fn assert_full_join_plan(query: &str) {
    let ctx = TestContext::new_with_full_join(MockSchema::from_yaml_file(SCHEMA)).unwrap();
    let sql = ctx.build_sql(query).unwrap();
    assert!(sql.contains("FULL JOIN"), "{sql}");
    assert!(
        sql.contains("COALESCE(\"q_l\".\"orders__category\""),
        "{sql}"
    );
    assert!(
        sql.contains("SELECT \"full_aggregate\".\"orders__category\" \"orders__category\""),
        "{sql}"
    );
}

#[test]
fn test_full_join_of_two_stages() {
    assert_full_join_plan(indoc! {r#"
        measures:
          - orders.amount_reduce_category
          - orders.count_reduce_category
        dimensions:
          - orders.category
          - orders.status
        order:
          - id: orders.category
          - id: orders.status
    "#});
}

#[test]
fn test_full_join_of_three_stages_with_a_measure_filter() {
    assert_full_join_plan(indoc! {r#"
        measures:
          - orders.amount_reduce_category
          - orders.count_reduce_category
          - orders.total_amount
        dimensions:
          - orders.category
          - orders.status
        filters:
          - member: orders.total_amount
            operator: gt
            values:
              - "0"
        order:
          - id: orders.category
          - id: orders.status
    "#});
}

#[test]
fn test_full_join_under_a_calculated_measure() {
    assert_full_join_plan(indoc! {r#"
        measures:
          - orders.amount_plus_customers_reduce_category
          - orders.total_amount
        dimensions:
          - orders.category
          - orders.status
        order:
          - id: orders.category
          - id: orders.status
    "#});
}
