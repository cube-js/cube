use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_shifted_and_regular() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
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
async fn test_two_shifts_same_query() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_prev_month
          - orders.amount_next_month
        time_dimensions:
          - dimension: orders.created_at
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
async fn test_shift_with_count_and_sum() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
          - orders.total_amount
          - orders.count_prev_month
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
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
async fn test_shift_with_dimension_and_regular() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
          - orders.avg_amount
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
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

// The shifted leaf joins the dimension CTE on the shifted month, so a row whose
// month has no CTE row must still keep its own keys.
#[tokio::test(flavor = "multi_thread")]
async fn test_shift_keeps_keys_of_rows_without_multi_stage_dimension() {
    let schema =
        MockSchema::from_yaml_file("common/integration_multi_stage_shifted_dimension_keys.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - orders.amount_prev_month
        dimensions:
          - customers.total_spend
          - customers.monthly_spend
          - customers.id
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-02-01"
              - "2024-03-31"
        order:
          - id: orders.created_at
          - id: customers.id
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(
            query,
            "integration_multi_stage_shifted_dimension_keys_tables.sql",
        )
        .await
    {
        insta::assert_snapshot!(result);
    }
}
