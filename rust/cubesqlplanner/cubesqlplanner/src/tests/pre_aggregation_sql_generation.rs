//! High-level tests for SQL generation with pre-aggregations
//!
//! These tests verify that queries correctly match and use pre-aggregations,
//! checking that the generated SQL contains references to pre-aggregation tables.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

#[test]
fn test_basic_pre_agg_sql() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - visitors.count
        dimensions:
          - visitors.source
    "};

    let (sql, pre_aggrs) = test_context
        .build_sql_with_used_pre_aggregations(query_yaml)
        .expect("Should generate SQL without pre-aggregations");

    assert_eq!(pre_aggrs.len(), 1, "Should use one pre-aggregation");
    assert_eq!(pre_aggrs[0].name(), "daily_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_full_match_main_rollup() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
              - orders.total_amount
            dimensions:
              - orders.status
              - orders.city
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_partial_match_main_rollup() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");

    insta::assert_snapshot!(sql);
}
