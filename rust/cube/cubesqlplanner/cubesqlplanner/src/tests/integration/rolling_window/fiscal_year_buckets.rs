use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

// A fiscal year starting on May 1: every month of FY2024 is worth 1 and every month of
// FY2025 is worth 10, so a window summing calendar-year buckets mixes the two.
fn create_context() -> TestContext {
    ctx_with(&[])
}

fn ctx_with(pre_aggs: &[&str]) -> TestContext {
    let schema =
        MockSchema::from_yaml_file("common/integration_rolling_window_fiscal_buckets.yaml")
            .only_pre_aggregations(pre_aggs);
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_fiscal_buckets_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_at_its_own_custom_granularity() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ytd
          - sales.amount_ytd_ms
          - sales.amount_ytd_offset
        time_dimensions:
          - dimension: sales.day
            granularity: fis_year
            dateRange:
              - "2024-05-01"
              - "2026-04-30"
        order:
          - id: sales.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_granularity_at_month() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - sales.amount_ytd
          - sales.amount_ytd_ms
        time_dimensions:
          - dimension: sales.day
            granularity: month
            dateRange:
              - "2024-05-01"
              - "2026-04-30"
        order:
          - id: sales.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_at_a_finer_custom_granularity() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - sales.amount_ytd
          - sales.amount_qtd
        time_dimensions:
          - dimension: sales.day
            granularity: fis_quarter
            dateRange:
              - "2024-05-01"
              - "2026-04-30"
        order:
          - id: sales.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A query grain coarser than the window's period: the window runs from the start of the
// period the row opens in to the row's end.
#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_at_a_coarser_query_granularity() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - sales.amount_qtd
        time_dimensions:
          - dimension: sales.day
            granularity: fis_year
            dateRange:
              - "2024-05-01"
              - "2026-04-30"
        order:
          - id: sales.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_granularity_at_calendar_year() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - sales.amount_ytd
        time_dimensions:
          - dimension: sales.day
            granularity: year
            dateRange:
              - "2024-01-01"
              - "2026-12-31"
        order:
          - id: sales.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trailing_window_at_custom_granularity() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - sales.amount_trailing_year
        time_dimensions:
          - dimension: sales.day
            granularity: fis_year
            dateRange:
              - "2024-05-01"
              - "2026-04-30"
        order:
          - id: sales.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_shifted_to_date_at_custom_granularity() {
    let ctx = create_context();
    let query = indoc! {r#"
        measures:
          - sales.amount_ytd_ms
          - sales.amount_ytd_ly
        time_dimensions:
          - dimension: sales.day
            granularity: fis_year
            dateRange:
              - "2024-05-01"
              - "2026-04-30"
        order:
          - id: sales.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

const FIS_YEAR_YTD_QUERY: &str = indoc! {r#"
    measures:
      - sales.amount_ytd
    time_dimensions:
      - dimension: sales.day
        granularity: fis_year
        dateRange:
          - "2024-05-01"
          - "2026-04-30"
    order:
      - id: sales.day
"#};

fn used_pre_aggregations(ctx: &TestContext, query: &str) -> Vec<String> {
    let (_sql, used) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    used.iter().map(|pa| pa.name().clone()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_month_rollup_serves_custom_granularity_to_date() {
    let ctx = ctx_with(&["ytd_by_month"]);
    assert_eq!(
        used_pre_aggregations(&ctx, FIS_YEAR_YTD_QUERY),
        vec!["ytd_by_month".to_string()]
    );

    if let Some(result) = ctx.try_execute_pg(FIS_YEAR_YTD_QUERY, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Calendar-year buckets straddle the fiscal year boundary, so they cannot serve the window.
#[tokio::test(flavor = "multi_thread")]
async fn test_calendar_year_rollup_does_not_serve_custom_granularity_to_date() {
    let ctx = ctx_with(&["ytd_by_year"]);
    assert!(used_pre_aggregations(&ctx, FIS_YEAR_YTD_QUERY).is_empty());
}
