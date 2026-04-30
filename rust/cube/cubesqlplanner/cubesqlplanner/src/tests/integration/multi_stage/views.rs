use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_time_shift() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.amount_prev_month
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
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
async fn test_view_with_calculated() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.mom_growth
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
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
async fn test_view_with_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        filters:
          - dimension: orders_ms_view.status
            operator: equals
            values:
              - completed
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
