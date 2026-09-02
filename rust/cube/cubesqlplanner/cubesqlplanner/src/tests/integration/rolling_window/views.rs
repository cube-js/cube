use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_in_view() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_rolling_view.rolling_sum_trailing_7d
        dimensions:
          - orders_rolling_view.name
        time_dimensions:
          - dimension: orders_rolling_view.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        order:
          - id: orders_rolling_view.name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_in_view_with_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_rolling_view.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders_rolling_view.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        filters:
          - dimension: orders_rolling_view.category
            operator: equals
            values:
              - electronics
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
