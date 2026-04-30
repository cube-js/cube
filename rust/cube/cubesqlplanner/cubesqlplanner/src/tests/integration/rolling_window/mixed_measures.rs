use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_and_regular() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
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

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_count_and_regular() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_count_unbounded
          - orders.count
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
async fn test_rolling_and_multiple_regular() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.count
          - orders.avg_amount
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
async fn test_rolling_regular_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.count
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
