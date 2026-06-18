use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_without_time_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
    "#};

    // Rolling window without time dimension does not error — it generates SQL
    // that aggregates without time series (effectively a total)
    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_count_distinct() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_unique_customers_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_with_empty_date_range() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-06-01"
              - "2024-06-10"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_with_single_day_range() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-15"
              - "2024-01-15"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Regression test for JS sql-generation case "rolling count without
// date range": the time-series CTE enumerates every month between
// min(created_at) and max(created_at), so months where the trailing
// window covers no rows surface as `count = null`. One row in Jan
// 2024, five in May 2024 — Feb / Mar pick up Jan via the 3-month
// trail, Apr is empty (null), May rolls in all five May rows.
// Schema and seed live in a dedicated yaml/seed pair to keep the gap
// shape isolated from the main rolling-window fixtures.
fn create_context_gaps() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/rolling_window_gaps.yaml");
    TestContext::new_with_generated_time_series(schema).unwrap()
}

const SEED_GAPS: &str = "rolling_window_gaps_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_count_without_date_range_with_gaps() {
    let ctx = create_context_gaps();

    let query = indoc! {r#"
        measures:
          - orders.rolling_count_3m
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED_GAPS).await {
        insta::assert_snapshot!(result);
    }
}

// Same shape as `test_rolling_count_without_date_range_with_gaps` but
// against the `multi_stage: true` flavour of the rolling measure
// (`rolling_count_3m_ms`). `try_build_rolling_window` routes the
// `is_multi_stage` base through `build_multi_stage_cte` (instead of
// `build_rolling_window_base`), and the inode-stage CTE for the base
// ends up registered with the same `(role, [member], state)` triple
// as the rolling-stage CTE that wraps it — `base_state` collapses
// onto `state` when rolling granularity matches the time-dim
// granularity and there's no dateRange. Expected: 5 rows incl. null
// for Apr 2024, same as the non-multi-stage repro.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_rolling_without_date_range_with_gaps() {
    let ctx = create_context_gaps();

    let query = indoc! {r#"
        measures:
          - orders.rolling_count_3m_ms
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED_GAPS).await {
        insta::assert_snapshot!(result);
    }
}
