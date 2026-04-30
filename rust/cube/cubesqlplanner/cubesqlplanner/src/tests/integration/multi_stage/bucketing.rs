use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_bucketing.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_bucketing_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_bucketing() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
          - orders.revenue
        dimensions:
          - orders.change_type
        time_dimensions:
          - dimension: orders.created_at
            granularity: year
            dateRange:
              - "2024-01-02"
              - "2026-01-01"
        timezone: UTC
        order:
          - id: orders.change_type
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bucketing_with_multistage_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.revenue
          - orders.revenue_year_ago
        dimensions:
          - orders.change_type
        time_dimensions:
          - dimension: orders.created_at
            granularity: year
            dateRange:
              - "2024-01-02"
              - "2026-01-01"
        timezone: UTC
        order:
          - id: orders.change_type
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bucketing_with_complex_bucket_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.revenue
          - orders.revenue_year_ago
        dimensions:
          - orders.change_type_complex
        time_dimensions:
          - dimension: orders.created_at
            granularity: year
            dateRange:
              - "2024-01-02"
              - "2026-01-01"
        timezone: UTC
        order:
          - id: orders.change_type_complex
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bucketing_with_dimension_over_complex_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.revenue
          - orders.revenue_year_ago
        dimensions:
          - orders.change_type_concat
        time_dimensions:
          - dimension: orders.created_at
            granularity: year
            dateRange:
              - "2024-01-02"
              - "2026-01-01"
        timezone: UTC
        order:
          - id: orders.change_type_concat
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bucketing_with_join_and_bucket_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.revenue
          - orders.revenue_year_ago
        dimensions:
          - orders.change_type_complex_with_join
        time_dimensions:
          - dimension: orders.created_at
            granularity: year
            dateRange:
              - "2024-01-02"
              - "2026-01-01"
        timezone: UTC
        order:
          - id: orders.change_type_complex_with_join
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bucketing_dim_reference_other_cube_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.revenue
        dimensions:
          - first_date.customer_type
        timezone: UTC
        order:
          - id: first_date.customer_type
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bucketing_with_two_dimensions() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.revenue
          - orders.revenue_year_ago
        dimensions:
          - orders.change_type_concat
          - first_date.customer_type2
        time_dimensions:
          - dimension: orders.created_at
            granularity: year
            dateRange:
              - "2024-01-02"
              - "2026-01-01"
        timezone: UTC
        order:
          - id: orders.change_type_concat
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bucketing_with_two_dims_concated() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.revenue
          - orders.revenue_year_ago
        dimensions:
          - orders.two_dims_concat
        time_dimensions:
          - dimension: orders.created_at
            granularity: year
            dateRange:
              - "2024-01-02"
              - "2026-01-01"
        timezone: UTC
        order:
          - id: orders.two_dims_concat
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
