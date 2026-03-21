use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_custom_granularity.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_custom_granularity_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_half_year_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_half_year_by_1st_april_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year_by_1st_april
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fiscal_year_with_offset() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: fiscal_year
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_half_year_with_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - \"2024-01-01\"
              - \"2024-12-31\"
        order:
          - id: orders.created_at
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_half_year_with_sum_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_granularity_with_daterange_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - \"2024-01-01\"
              - \"2024-06-30\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
