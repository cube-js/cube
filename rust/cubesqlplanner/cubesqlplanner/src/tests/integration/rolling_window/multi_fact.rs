use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_multi_fact_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_two_rolling_from_different_facts() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - payments.rolling_sum_7d
          - messages.rolling_count_7d
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: customers.registered_at
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

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_and_regular_from_different_facts() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - payments.rolling_sum_7d
          - messages.count
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: customers.registered_at
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

#[tokio::test(flavor = "multi_thread")]
async fn test_two_rolling_aggregated_by_day() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - payments.rolling_sum_7d
          - messages.rolling_count_7d
        time_dimensions:
          - dimension: customers.registered_at
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
async fn test_two_rolling_with_shared_dimension_and_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - payments.rolling_sum_7d
          - messages.rolling_count_7d
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: customers.registered_at
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
