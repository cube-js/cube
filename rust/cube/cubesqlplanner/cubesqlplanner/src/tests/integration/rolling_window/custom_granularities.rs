use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema =
        MockSchema::from_yaml_file("common/integration_rolling_window_custom_granularity.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_custom_granularity_tables.sql";

// --- To-date with custom granularity ---

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_half_year() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_half_year
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_half_year_with_month_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_half_year
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-12-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_fiscal_year() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_fiscal_year
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-06-30"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_fiscal_year_with_month_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_fiscal_year
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2025-12-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Custom granularity as query granularity ---

// FIXME: Custom granularity as query granularity requires generate_custom_time_series()
// which is not implemented in mock (todo! in mock_base_tools.rs:81). The planner needs
// the mock to generate the time series for non-standard intervals (half_year, bi_weekly).
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_sum_with_half_year_query_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - "2024-01-01"
              - "2025-12-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Same — generate_custom_time_series() not implemented in mock.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_sum_with_bi_weekly_query_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: bi_weekly
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Same — generate_custom_time_series() not implemented in mock.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_with_custom_query_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_month
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - "2024-01-01"
              - "2025-12-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Custom granularity + dimensions/filters ---

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_half_year
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_with_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_half_year
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Multiple rolling with custom granularity ---

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_custom_and_regular_rolling() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_half_year
          - orders.rolling_sum_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_two_to_date_custom_granularities() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_half_year
          - orders.rolling_sum_to_date_fiscal_year
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-12-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
