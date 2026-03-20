use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_day_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "integration_basic_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_month_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "integration_basic_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_quarter_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: quarter
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "integration_basic_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_year_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: year
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "integration_basic_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_with_regular_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "integration_basic_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_with_date_range() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - \"2024-01-01\"
              - \"2024-02-29\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "integration_basic_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_no_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - \"2024-01-01\"
              - \"2024-02-29\"
        order:
          - id: orders.count
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "integration_basic_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_with_joined_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - customers.city
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at
          - id: customers.city
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, "integration_basic_tables.sql").await {
        insta::assert_snapshot!(result);
    }
}
