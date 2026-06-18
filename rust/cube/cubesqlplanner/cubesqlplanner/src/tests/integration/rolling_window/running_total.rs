use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_running_total_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.running_total
        time_dimensions:
          - dimension: orders.created_at
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
async fn test_running_total_with_day_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.running_total
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
async fn test_running_total_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.running_total
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_running_total_with_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.running_total
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
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

#[tokio::test(flavor = "multi_thread")]
async fn test_running_total_and_rolling() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.running_total
          - orders.rolling_sum_trailing_7d
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
async fn test_running_total_and_regular_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.running_total
          - orders.total_amount
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
