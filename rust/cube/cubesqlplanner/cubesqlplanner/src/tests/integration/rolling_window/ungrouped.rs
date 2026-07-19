use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_ungrouped() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_avg_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        ungrouped: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_ungrouped_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_avg_7d
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        ungrouped: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
