//! High-level tests for SQL generation with pre-aggregations
//!
//! These tests verify that queries correctly match and use pre-aggregations,
//! checking that the generated SQL contains references to pre-aggregation tables.

use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::options_member::OptionsMember;
use crate::test_fixtures::cube_bridge::{
    MockMemberExpressionDefinition, MockMemberSql, MockSchema,
};
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;
use std::rc::Rc;

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_pre_agg_sql() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - visitors.count
        dimensions:
          - visitors.source
        order:
          - id: visitors.source
    "};

    let (_sql, pre_aggrs) = test_context
        .build_sql_with_used_pre_aggregations(query_yaml)
        .expect("Should generate SQL without pre-aggregations");

    assert_eq!(pre_aggrs.len(), 1, "Should use one pre-aggregation");
    assert_eq!(pre_aggrs[0].name(), "daily_rollup");

    if let Some(result) = test_context
        .try_execute(query_yaml, "pre_aggregation_tables.sql")
        .await
    {
        insta::assert_snapshot!("basic_pre_agg_sql_cubestore_result", result);
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
        order:
          - id: orders.status
          - id: orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("full_match_main_rollup_cubestore_result", result);
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
        order:
          - id: orders.status
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("partial_match_main_rollup_cubestore_result", result);
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
        order:
          - id: orders.status
          - id: orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("full_match_non_additive_measure_cubestore_result", result);
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
        order:
          - id: orders.country
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("daily_rollup_full_match_cubestore_result", result);
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
        order:
          - id: orders.country
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("daily_rollup_coarser_granularity_cubestore_result", result);
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
        order:
          - id: orders.country
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "daily_rollup_non_additive_full_match_cubestore_result",
            result
        );
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
        order:
          - id: orders.status
          - id: orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "all_base_measures_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "multi_level_all_base_measures_full_match_cubestore_result",
            result
        );
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
        order:
          - id: orders.status
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "all_base_measures_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "multi_level_all_base_measures_partial_match_cubestore_result",
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
        order:
          - id: orders.status
          - id: orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "calculated_measure_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "multi_level_calculated_measure_full_match_cubestore_result",
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
        order:
          - id: orders.status
          - id: orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "mixed_measure_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "multi_level_mixed_measure_full_match_cubestore_result",
            result
        );
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
        order:
          - id: orders.status
          - id: orders.city
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "base_and_calculated_measure_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "base_and_calculated_measure_full_match_cubestore_result",
            result
        );
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
        order:
          - id: orders.status
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "base_and_calculated_measure_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "base_and_calculated_measure_parital_match_cubestore_result",
            result
        );
    }
}

// --- Segment matching tests ---

// When a cube's access policy denies the queried members, RBAC
// (CompilerApi.applyRowLevelSecurity) appends a member-expression segment
// `{ expression: () => '1 = 0', cubeName, name: 'rlsAccessDenied' }`. It
// references no members (empty dependencies), so it's a constant filter on top
// of the rollup and must not disqualify pre-aggregation matching.
#[tokio::test(flavor = "multi_thread")]
async fn test_constant_member_expression_segment_keeps_pre_aggregation() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["main_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
    "};

    let access_denied_segment = {
        let sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new("1 = 0").unwrap());
        let expr = MockMemberExpressionDefinition::builder()
            .expression_name(Some("rlsAccessDenied".to_string()))
            .cube_name(Some("orders".to_string()))
            .expression(MemberExpressionExpressionDef::Sql(sql))
            .build();
        OptionsMember::MemberExpression(Rc::new(expr))
    };

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations_with_segments(query_yaml, vec![access_denied_segment])
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_rollup");
    assert!(
        sql.contains("1 = 0"),
        "expected the constant access-denied segment in SQL, got:\n{sql}"
    );
}

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
        order:
          - id: orders.status
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("segment_full_match_cubestore_result", result);
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
        order:
          - id: orders.status
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "segment_partial_match_unused_segment_cubestore_result",
            result
        );
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
        order:
          - id: orders.status
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("custom_granularity_full_match_cubestore_result", result);
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
        order:
          - id: orders.status
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "standard_pre_agg_coarser_custom_query_cubestore_result",
            result
        );
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
        order:
          - id: orders.status
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "custom_granularity_non_additive_full_match_cubestore_result",
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
        order:
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "custom_half_year_non_strict");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "custom_granularity_non_strict_self_match_cubestore_result",
            result
        );
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
        order:
          - id: orders.status
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "segment_rollup");

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!("segment_with_coarser_granularity_cubestore_result", result);
    }
}

// --- Multi-stage count_distinct sum by quarter with pre-aggregation ---

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_count_distinct_sum_by_quarter_with_pre_aggregation() {
    let schema = MockSchema::from_yaml_file("common/multi_stage_sum_by_quarter_test.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - coach.count_distinct__sum_by_quarter
        cubestoreSupportMultistage: true
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main");

    if let Some(result) = ctx
        .try_execute(query_yaml, "multi_stage_sum_by_quarter_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "multi_stage_count_distinct_sum_by_quarter_with_pre_agg_cubestore_result",
            result
        );
    }
}

// --- Multi-stage with separate pre-aggregations ---

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_separate_pre_aggregations() {
    let schema = MockSchema::from_yaml_file("common/multi_stage_separate_pre_aggs_test.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count_reduce_status
          - orders.revenue_reduce_status
        cubestoreSupportMultistage: true
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 2, "Expected 2 pre-aggregation usages");

    let names: Vec<&str> = pre_aggrs
        .iter()
        .map(|u| u.pre_aggregation.name().as_str())
        .collect();
    assert!(
        names.contains(&"count_rollup"),
        "Expected count_rollup, got {:?}",
        names
    );
    assert!(
        names.contains(&"revenue_rollup"),
        "Expected revenue_rollup, got {:?}",
        names
    );

    if let Some(result) = ctx
        .try_execute(query_yaml, "multi_stage_separate_pre_aggs_tables.sql")
        .await
    {
        insta::assert_snapshot!("multi_stage_separate_pre_aggs_cubestore_result", result);
    }
}

// --- Multi-stage with separate pre-aggregations and time shift ---

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_separate_pre_aggs_with_time_shift() {
    let schema = MockSchema::from_yaml_file("common/multi_stage_pre_agg_time_shift_test.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.count_prev_month
          - orders.revenue_reduce_status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - \"2025-01-01\"
              - \"2025-03-31\"
        cubestoreSupportMultistage: true
        order:
          - id: orders.created_at
    "};

    let (_sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 2, "Expected 2 pre-aggregation usages");

    // Find usages by pre-aggregation name
    let count_usage = pre_aggrs
        .iter()
        .find(|u| u.pre_aggregation.name() == "count_rollup")
        .expect("Expected count_rollup usage");
    let revenue_usage = pre_aggrs
        .iter()
        .find(|u| u.pre_aggregation.name() == "revenue_rollup")
        .expect("Expected revenue_rollup usage");

    // count_prev_month has time_shift prior 1 month, so date range should be shifted back
    assert_eq!(
        count_usage.date_range,
        Some((
            "2024-12-01T00:00:00.000".to_string(),
            "2025-02-28T23:59:59.999".to_string()
        )),
        "count_rollup should have date range shifted 1 month prior"
    );

    // revenue_reduce_status has no time shift, so original date range
    assert_eq!(
        revenue_usage.date_range,
        Some((
            "2025-01-01T00:00:00.000".to_string(),
            "2025-03-31T23:59:59.999".to_string()
        )),
        "revenue_rollup should have original date range"
    );

    if let Some(result) = ctx
        .try_execute(query_yaml, "multi_stage_pre_agg_time_shift_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "multi_stage_separate_pre_aggs_time_shift_cubestore_result",
            result
        );
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

// A rolling-window count_distinct_approx whose pre-aggregation stores the
// rolling measure itself: the leaf reads the rollup's HLL state and must
// keep it MERGED (state only), so the outer rolling-window stage can merge
// across the window and finalize to a cardinality. This pins the state
// branch — the read must NOT collapse the state to a cardinality too early.
#[test]
fn test_count_distinct_approx_rolling_pre_agg_keeps_state() {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/integration_rolling_window.yaml",
    ))
    .unwrap();

    let query = indoc! {r#"
        measures:
          - orders.rolling_unique_customers_approx_7d
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
        cubestoreSupportMultistage: true
    "#};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "approx_rolling");

    // Leaf reads the rollup column as a bare merged state (hll_merge).
    assert!(
        sql.contains("merge(\"orders__rolling_unique_customers_approx_7d\")"),
        "Rolling leaf should keep the merged HLL state, got:\n{}",
        sql
    );
    // The rolling-window stage finalizes the merged states to a cardinality.
    assert!(
        sql.contains("cardinality(merge("),
        "Rolling window should finalize merged states to a cardinality, got:\n{}",
        sql
    );
}

// --- HLL count_distinct_approx through a pre-aggregation ---
//
// A count_distinct_approx measure materialized in a rollup keeps an HLL
// state per group. The build (load) SQL must serialize that state
// (hll_init), and a query reading the rollup must merge the states and
// take their cardinality (hll_cardinality_merge) rather than recompute
// an approximate distinct over the state column (count_distinct_approx).
//
// The mock driver renders these as distinct, CubeStore-style forms:
//   hll_init               -> hll_add_agg(hll_hash_any(x))
//   hll_merge              -> merge(x)                       (state only)
//   hll_cardinality_merge  -> cardinality(merge(x))
//   count_distinct_approx  -> round(hll_cardinality(hll_add_agg(hll_hash_any(x))))

#[test]
fn test_count_distinct_approx_pre_agg_read_merges_state() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["approx_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - orders.approx_unique_count
        dimensions:
          - orders.status
          - orders.city
    "};

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "approx_rollup");

    // A top-level read merges the stored states and finalizes them to a
    // cardinality. It must NOT re-hash the state column as a fresh
    // approximate distinct would (hll_add_agg).
    assert!(
        sql.contains("cardinality(merge("),
        "Read query should merge HLL states and take cardinality, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("hll_add_agg"),
        "Read query should not re-init HLL from the state column, got:\n{}",
        sql
    );
}

#[test]
fn test_count_distinct_approx_pre_agg_build_emits_state() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml")
        .only_pre_aggregations(&["approx_rollup"]);
    let ctx = TestContext::new(schema).unwrap();

    // pre_aggregation_query: true renders the rollup build (load) SQL.
    let query_yaml = indoc! {"
        measures:
          - orders.approx_unique_count
        dimensions:
          - orders.status
          - orders.city
        pre_aggregation_query: true
    "};

    let sql = ctx.build_sql(query_yaml).unwrap();

    // The build must serialize the HLL state (hll_init) without merging
    // or taking its cardinality — that happens only on read.
    assert!(
        sql.contains("hll_add_agg(hll_hash_any("),
        "Build SQL should emit HLL state, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("hll_cardinality"),
        "Build SQL should not compute cardinality, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("merge("),
        "Build SQL should not merge states, got:\n{}",
        sql
    );
}

// A multi-stage measure that sums a count_distinct_approx must read the
// rollup as a finalized cardinality, exactly as it would without the
// pre-aggregation — the outer `sum` aggregates counts, not raw HLL states.
// This pins that the pre-agg read does not leak a bare merged state here.
#[test]
fn test_count_distinct_approx_multistage_pre_agg_reads_cardinality() {
    let schema = MockSchema::from_yaml_file("common/multi_stage_sum_by_quarter_test.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query_yaml = indoc! {"
        measures:
          - coach.approx__sum_by_quarter
        cubestoreSupportMultistage: true
    "};

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "main_approx");

    // Leaf finalizes the merged state to a cardinality so the outer sum
    // aggregates numbers (same shape as the non-pre-agg rendering).
    assert!(
        sql.contains("cardinality(merge("),
        "Multi-stage leaf should finalize HLL to cardinality, got:\n{}",
        sql
    );
}

// A cube `foo` whose `originalSql` pre-aggregation (`main`) materializes its base
// SQL, plus a `second` rollup. The mock renders the originalSql pre-agg table as
// `foo__main` and the raw cube SQL references `foo_table`.
fn use_original_sql_pre_aggregations_in_pre_aggregation_schema() -> Result<MockSchema, CubeError> {
    MockSchema::from_yaml(indoc! {"
        cubes:
          - name: foo
            sql: SELECT * FROM foo_table
            dimensions:
              - name: time
                type: time
                sql: timestamp
            measures:
              - name: total
                type: sum
                sql: amount
            pre_aggregations:
              - name: main
                type: originalSql
              - name: second
                type: rollup
                measures:
                  - total
                time_dimension: time
                granularity: day
    "})
}

// Building a rollup with `useOriginalSqlPreAggregations` must source it from the cube's
// `originalSql` pre-aggregation table instead of the raw cube SQL
#[test]
fn test_rollup_build_with_use_original_sql_pre_aggregations_in_pre_aggregation_reads_original_sql_pre_agg(
) -> Result<(), CubeError> {
    let ctx = TestContext::new(use_original_sql_pre_aggregations_in_pre_aggregation_schema()?)?;

    let query_yaml = indoc! {"
        measures:
          - foo.total
        time_dimensions:
          - dimension: foo.time
            granularity: day
        pre_aggregation_query: true
        use_original_sql_pre_aggregations_in_pre_aggregation: true
    "};

    let sql = ctx.build_sql(query_yaml)?;

    assert!(
        sql.contains("foo__main"),
        "Build SQL should source from the originalSql pre-agg table, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("foo_table"),
        "Build SQL should not read the raw cube table when useOriginalSqlPreAggregations is set, got:\n{}",
        sql
    );
    Ok(())
}

// Without the flag, a rollup build reads the raw cube SQL — the originalSql
// pre-agg table must not be substituted in.
#[test]
fn test_rollup_build_without_use_original_sql_pre_aggregations_in_pre_aggregation_reads_raw_table(
) -> Result<(), CubeError> {
    let ctx = TestContext::new(use_original_sql_pre_aggregations_in_pre_aggregation_schema()?)?;

    let query_yaml = indoc! {"
        measures:
          - foo.total
        time_dimensions:
          - dimension: foo.time
            granularity: day
        pre_aggregation_query: true
    "};

    let sql = ctx.build_sql(query_yaml)?;

    assert!(
        sql.contains("foo_table"),
        "Build SQL should read the raw cube table without useOriginalSqlPreAggregations, got:\n{}",
        sql
    );
    assert!(
        !sql.contains("foo__main"),
        "Build SQL should not source from the originalSql pre-agg table without the flag, got:\n{}",
        sql
    );
    Ok(())
}

// A measure referenced only in ORDER BY (not in the selected measures) is dropped
// from the ORDER BY when reading a pre-aggregation. CubeStore cannot ORDER BY an
// aggregate of a rollup column that isn't projected, and the legacy planner
// likewise ignores such keys, so the remaining keys (here the time dimension and
// the selected dimension) drive the order. Mirrors the driver-test "partitioned
// pre-agg" queries that order by `created_at asc, <unselected measure> desc, <dim> asc`.
#[tokio::test(flavor = "multi_thread")]
async fn test_order_by_only_measure_dropped_from_pre_agg() {
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
        order:
          - id: orders.created_at
            desc: false
          - id: orders.total_amount
            desc: true
          - id: orders.country
            desc: false
    "};

    let (sql, pre_aggrs) = ctx
        .build_sql_with_used_pre_aggregations(query_yaml)
        .unwrap();

    assert_eq!(pre_aggrs.len(), 1);
    assert_eq!(pre_aggrs[0].name(), "daily_countries_rollup");
    // total_amount is neither selected nor projected by the rollup read, so it must
    // not appear in ORDER BY — neither as the base column nor the rollup column.
    assert!(
        !sql.contains("\"orders\".amount"),
        "ORDER BY must not reference the base-table column, got:\n{sql}"
    );
    assert!(
        !sql.contains("orders__total_amount"),
        "order-by-only measure must be dropped, not reference the rollup column, got:\n{sql}"
    );

    if let Some(result) = ctx
        .try_execute(query_yaml, "pre_aggregation_matching_tables.sql")
        .await
    {
        insta::assert_snapshot!(
            "order_by_only_measure_dropped_from_pre_agg_cubestore_result",
            result
        );
    }
}
