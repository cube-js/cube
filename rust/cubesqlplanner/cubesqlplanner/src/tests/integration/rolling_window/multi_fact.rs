use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_multi_fact_tables.sql";

// FIXME: Multi-fact with rolling windows on both facts: the second fact (returns) rolling
// window does not apply time-based filtering. returns.rolling_refund_7d returns the TOTAL
// refund per customer on every time_series row, instead of a 7-day sliding window.
// E.g., Alice has returns on Jan 3, 13, 24 (total=170), but rolling_refund_7d=170 on
// ALL days Jan 12-25. This is the same underlying issue as non-rolling multi-fact with
// time dimensions — the second fact is not broken down by the first fact's time dimension.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_two_rolling_from_different_facts() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - returns.rolling_refund_7d
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
        order:
          - id: customers.name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Multi-fact rolling + regular from different facts: same issue — returns.total_refund
// shows total per customer, not per day. Additionally, it only appears on rows where the
// first fact (orders) has data, with NULL on other days.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_and_regular_from_different_facts() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - returns.total_refund
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
        order:
          - id: customers.name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Multi-fact rolling with shared dimension and filter: same underlying issue —
// second fact's rolling window ignores time dimension. Filter on shared dimension
// (customers.city) works correctly, but time-based windowing does not.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_two_rolling_with_shared_dimension_and_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - returns.rolling_refund_7d
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
        order:
          - id: customers.name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
