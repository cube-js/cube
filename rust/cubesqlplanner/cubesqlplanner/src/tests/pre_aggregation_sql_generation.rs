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

#[test]
fn test_full_match_non_additive_measure() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.avg_amount
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
fn test_no_match_non_additive_measure_partial() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.avg_amount
            dimensions:
              - orders.status
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}

#[test]
fn test_daily_rollup_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["daily_countries_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.country
            time_dimensions:
              - dimension: orders.created_at
                granularity: day
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_daily_rollup_coarser_granularity() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["daily_countries_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.country
            time_dimensions:
              - dimension: orders.created_at
                granularity: month
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_daily_rollup_finer_granularity_no_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["daily_countries_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.country
            time_dimensions:
              - dimension: orders.created_at
                granularity: hour
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}

#[test]
fn test_daily_rollup_non_additive_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["daily_countries_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.avg_amount
            dimensions:
              - orders.country
            time_dimensions:
              - dimension: orders.created_at
                granularity: day
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_daily_rollup_non_additive_coarser_granularity_no_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["daily_countries_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.avg_amount
            dimensions:
              - orders.country
            time_dimensions:
              - dimension: orders.created_at
                granularity: month
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}

// --- multi_level_measure across different pre-aggregations ---

#[test]
fn test_multi_level_all_base_measures_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["all_base_measures_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.multi_level_measure
            dimensions:
              - orders.status
              - orders.city
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "all_base_measures_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_multi_level_all_base_measures_partial_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["all_base_measures_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.multi_level_measure
            dimensions:
              - orders.status
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "all_base_measures_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_multi_level_calculated_measure_no_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["calculated_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.multi_level_measure
            dimensions:
              - orders.status
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}

#[test]
fn test_multi_level_calculated_measure_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["calculated_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.multi_level_measure
            dimensions:
              - orders.status
              - orders.city
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "calculated_measure_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_multi_level_mixed_measure_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["mixed_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.multi_level_measure
            dimensions:
              - orders.status
              - orders.city
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "mixed_measure_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_multi_level_mixed_measure_partial_no_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["mixed_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.multi_level_measure
            dimensions:
              - orders.status
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}
#[test]
fn test_base_and_calculated_measure_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["base_and_calculated_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.amount_per_count
            dimensions:
              - orders.status
              - orders.city
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "base_and_calculated_measure_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_base_and_calculated_measure_parital_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["base_and_calculated_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.amount_per_count
            dimensions:
              - orders.status
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "base_and_calculated_measure_rollup");

    insta::assert_snapshot!(sql);
}

// --- Segment matching tests ---

#[test]
fn test_segment_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["segment_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
            segments:
              - orders.high_priority
            time_dimensions:
              - dimension: orders.created_at
                granularity: day
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_segment_partial_match_unused_segment() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["segment_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
            time_dimensions:
              - dimension: orders.created_at
                granularity: day
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_segment_no_match_missing_in_pre_agg() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
              - orders.city
            segments:
              - orders.high_priority
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}

// --- Custom granularity pre-aggregation tests ---

#[test]
fn test_custom_granularity_full_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["custom_half_year_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
            time_dimensions:
              - dimension: orders.created_at
                granularity: half_year
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_standard_pre_agg_coarser_custom_query() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["daily_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
            time_dimensions:
              - dimension: orders.created_at
                granularity: half_year
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_custom_pre_agg_finer_query_no_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["custom_half_year_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
            time_dimensions:
              - dimension: orders.created_at
                granularity: day
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}

#[test]
fn test_custom_pre_agg_finer_standard_query_no_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["custom_half_year_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
            time_dimensions:
              - dimension: orders.created_at
                granularity: month
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}

#[test]
fn test_custom_granularity_non_additive_full_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["custom_half_year_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.avg_amount
            dimensions:
              - orders.status
            time_dimensions:
              - dimension: orders.created_at
                granularity: half_year
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_rollup");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_custom_granularity_non_additive_coarser_no_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["daily_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.avg_amount
            dimensions:
              - orders.status
            time_dimensions:
              - dimension: orders.created_at
                granularity: half_year
        "})
        .unwrap();

    assert!(pre_aggrs.is_empty());
}

#[test]
fn test_custom_granularity_non_strict_self_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["custom_half_year_non_strict"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            time_dimensions:
              - dimension: orders.created_at
                granularity: half_year
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_non_strict");

    insta::assert_snapshot!(sql);
}

#[test]
fn test_segment_with_coarser_granularity() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["segment_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.count
            dimensions:
              - orders.status
            segments:
              - orders.high_priority
            time_dimensions:
              - dimension: orders.created_at
                granularity: month
        "})
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    insta::assert_snapshot!(sql);
}
