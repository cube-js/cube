//! Rollups that store one time dimension at several granularities. Each column
//! holds that dimension truncated to its own granularity, so a query must read
//! the column its granularity can be derived from, in the select list and the
//! filters alike.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::{formatdoc, indoc};

const SEED: &str = "integration_multi_granularity_rollup_tables.sql";
const YAML: &str = "common/integration_multi_granularity_rollup.yaml";

fn ctx_with(pre_aggs: &[&str]) -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
}

/// Runs the query served by `pre_agg` (materialized by the harness) and again
/// with no pre-aggregation at all.
async fn rollup_vs_source(query: &str, pre_agg: &str) -> Option<(String, String)> {
    let with_rollup = ctx_with(&[pre_agg]);
    let (sql, usages) = with_rollup
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        usages.iter().any(|u| u.name() == pre_agg),
        "expected the query to be served by `{pre_agg}`; SQL:\n{sql}"
    );
    let rollup = with_rollup.try_execute_pg(query, SEED).await?;
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source))
}

async fn fallback_vs_source(query: &str, pre_agg: &str) -> Option<(String, String)> {
    let with_rollup = ctx_with(&[pre_agg]);
    let (sql, usages) = with_rollup
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        usages.is_empty(),
        "expected the query to fall back to the source; SQL:\n{sql}"
    );
    let rollup = with_rollup.try_execute_pg(query, SEED).await?;
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source))
}

fn query(granularity: &str, from: &str, to: &str) -> String {
    formatdoc! {r#"
        measures:
          - activity.count
          - activity.amount
        dimensions:
          - activity.app_name
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: {granularity}
            dateRange:
              - "{from}"
              - "{to}"
        order:
          - id: activity.cohort_date
          - id: activity.app_name
    "#}
}

#[tokio::test(flavor = "multi_thread")]
async fn day_query_on_a_single_day_reads_the_day_column() {
    let query = query("day", "2025-01-05", "2025-01-05");
    if let Some((rollup, source)) = rollup_vs_source(&query, "all_grains").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn day_query_across_a_year_boundary_reads_the_day_column() {
    let query = query("day", "2024-12-28", "2025-01-03");
    if let Some((rollup, source)) = rollup_vs_source(&query, "all_grains").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn month_query_reads_the_month_column() {
    let query = query("month", "2024-12-01", "2025-01-31");
    if let Some((rollup, source)) = rollup_vs_source(&query, "all_grains").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn day_query_ignores_a_coarser_column_listed_first() {
    let query = query("day", "2025-01-30", "2025-02-02");
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn quarter_query_rolls_up_a_finer_stored_column() {
    let query = query("quarter", "2024-10-01", "2025-03-31");
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn week_query_reads_the_week_column_listed_before_month() {
    let query = indoc! {r#"
        measures:
          - activity.count
          - activity.amount
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: week
            dateRange:
              - "2024-12-23"
              - "2025-01-12"
        order:
          - id: activity.cohort_date
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "week_and_month").await {
        assert_eq!(rollup, source);
    }
}

// Neither stored column yields both a week and a month, so the rollup cannot
// serve the query at all.
#[tokio::test(flavor = "multi_thread")]
async fn week_and_month_in_one_query_fall_back_to_the_source() {
    let query = indoc! {r#"
        measures:
          - activity.amount
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: week
            dateRange:
              - "2024-12-23"
              - "2025-01-12"
          - dimension: activity.cohort_date
            granularity: month
        order:
          - id: activity.cohort_date
    "#};
    if let Some((rollup, source)) = fallback_vs_source(query, "week_and_month").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn day_and_month_in_one_query_read_the_day_column() {
    let query = indoc! {r#"
        measures:
          - activity.amount
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: day
            dateRange:
              - "2025-01-30"
              - "2025-02-02"
          - dimension: activity.cohort_date
            granularity: month
        order:
          - id: activity.cohort_date
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
}
