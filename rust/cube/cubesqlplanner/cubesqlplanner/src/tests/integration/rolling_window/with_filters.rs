use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_where_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-02-01"
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
async fn test_time_filter_only() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-02-01"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_filters() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-02-01"
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
          - dimension: orders.amount
            operator: gte
            values:
              - "100"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        segments:
          - orders.completed_orders
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-02-01"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_and_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-02-01"
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_or_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-02-01"
        filters:
          - or:
              - dimension: orders.status
                operator: equals
                values:
                  - cancelled
              - dimension: orders.amount
                operator: gte
                values:
                  - "200"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
