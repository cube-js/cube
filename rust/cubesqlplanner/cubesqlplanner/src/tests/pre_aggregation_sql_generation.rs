//! High-level tests for SQL generation with pre-aggregations
//!
//! These tests verify that queries correctly match and use pre-aggregations,
//! checking that the generated SQL contains references to pre-aggregation tables.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_pre_agg_sql() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - visitors.count
        dimensions:
          - visitors.source
    "};

    let (_sql, pre_aggrs) = test_context
        .build_sql_with_used_pre_aggregations(query_yaml)
        .expect("Should generate SQL without pre-aggregations");

    assert_eq!(pre_aggrs.len(), 1, "Should use one pre-aggregation");
    assert_eq!(pre_aggrs[0].name(), "daily_rollup");

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "pre_aggregation_tables.sql")
        .await
    {
        insta::assert_snapshot!("basic_pre_agg_sql_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_match_main_rollup() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - orders.status
          - orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("full_match_main_rollup_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_match_main_rollup() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("partial_match_main_rollup_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_match_non_additive_measure() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.avg_amount
        dimensions:
          - orders.status
          - orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("full_match_non_additive_measure_pg_result", result);
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_daily_rollup_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["daily_countries_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.country
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("daily_rollup_full_match_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_daily_rollup_coarser_granularity() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["daily_countries_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.country
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("daily_rollup_coarser_granularity_pg_result", result);
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_daily_rollup_non_additive_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["daily_countries_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.avg_amount
        dimensions:
          - orders.country
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("daily_rollup_non_additive_full_match_pg_result", result);
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_level_all_base_measures_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["all_base_measures_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.multi_level_measure
        dimensions:
          - orders.status
          - orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "all_base_measures_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("multi_level_all_base_measures_full_match_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_level_all_base_measures_partial_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["all_base_measures_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.multi_level_measure
        dimensions:
          - orders.status
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "all_base_measures_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "multi_level_all_base_measures_partial_match_pg_result",
            result
        );
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_level_calculated_measure_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["calculated_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.multi_level_measure
        dimensions:
          - orders.status
          - orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "calculated_measure_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "multi_level_calculated_measure_full_match_pg_result",
            result
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_level_mixed_measure_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["mixed_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.multi_level_measure
        dimensions:
          - orders.status
          - orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "mixed_measure_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("multi_level_mixed_measure_full_match_pg_result", result);
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_base_and_calculated_measure_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["base_and_calculated_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.amount_per_count
        dimensions:
          - orders.status
          - orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "base_and_calculated_measure_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("base_and_calculated_measure_full_match_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_base_and_calculated_measure_parital_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["base_and_calculated_measure_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.amount_per_count
        dimensions:
          - orders.status
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "base_and_calculated_measure_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "base_and_calculated_measure_parital_match_pg_result",
            result
        );
    }
}

// --- Segment matching tests ---

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_full_match() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["segment_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        segments:
          - orders.high_priority
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("segment_full_match_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_partial_match_unused_segment() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["segment_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("segment_partial_match_unused_segment_pg_result", result);
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_granularity_full_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["custom_half_year_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("custom_granularity_full_match_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_standard_pre_agg_coarser_custom_query() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["daily_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("standard_pre_agg_coarser_custom_query_pg_result", result);
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_granularity_non_additive_full_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["custom_half_year_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.avg_amount
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "custom_granularity_non_additive_full_match_pg_result",
            result
        );
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_granularity_non_strict_self_match() {
    let schema = MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
        .only_pre_aggregations(&["custom_half_year_non_strict"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_non_strict");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("custom_granularity_non_strict_self_match_pg_result", result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_with_coarser_granularity() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["segment_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        segments:
          - orders.high_priority
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    if let Some(result) = ctx
        .try_execute_pg(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("segment_with_coarser_granularity_pg_result", result);
    }
}

// --- rollupJoin with calculated measures through view ---

#[test]
fn test_rollup_join_calculated_measures_through_view() {
    let schema = MockSchema::from_yaml_file("common/rollup_join_calculated_measures.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - my_view.facts_avg_cost
            time_dimensions:
              - dimension: my_view.facts_day
                granularity: day
        "})
        .unwrap();

    let pre_agg_names: Vec<_> = pre_aggrs
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .collect();
    assert!(
        pre_agg_names
            .iter()
            .any(|n| n == "line_items.combined_rollup_join"),
        "Should use combined_rollup_join, got: {:?}\nSQL:\n{}",
        pre_agg_names,
        sql
    );
    assert!(
        sql.contains("campaigns_rollup"),
        "SQL should reference campaigns_rollup, got:\n{}",
        sql
    );
    assert!(
        sql.contains("facts_rollup"),
        "SQL should reference facts_rollup, got:\n{}",
        sql
    );
    assert!(
        sql.contains("li_rollup"),
        "SQL should reference li_rollup, got:\n{}",
        sql
    );
}
