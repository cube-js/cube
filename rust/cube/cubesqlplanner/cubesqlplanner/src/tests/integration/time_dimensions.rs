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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_week_granularity() {
    let ctx = create_context();

    // By week (ISO Monday): 2024-01-15(3), 02-05(1), 02-12(1), 02-26(1), 03-04(1), 03-11(1), 04-01(1)
    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: week
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_hour_granularity() {
    let ctx = create_context();

    // Jan 15 only: 10:00(order 1) → count=1, 15:00(order 9) → count=1
    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: hour
            dateRange:
              - \"2024-01-15\"
              - \"2024-01-15\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_date_range_no_granularity_with_dimension() {
    let ctx = create_context();

    // dateRange [Jan-Feb] + status dimension, no granularity
    // Orders in range: 1,2(completed), 3(pending), 4(completed), 9(pending)
    // → completed=3, pending=2
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - \"2024-01-01\"
              - \"2024-02-29\"
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_dimension_date_range_with_filter() {
    let ctx = create_context();

    // dateRange [Jan-Mar] + month granularity + filter status=completed
    // Completed in range: 1,2(Jan), 4(Feb), 6(Mar) → Jan=2, Feb=1, Mar=1
    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - \"2024-01-01\"
              - \"2024-03-31\"
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_time_dimensions() {
    let ctx = create_context();

    // Two time dimensions: created_at by month + updated_at by month
    // (Jan,Jan)=2, (Jan,Feb)=1, (Feb,Feb)=1, (Feb,Mar)=1, (Mar,Mar)=3, (Apr,Apr)=1
    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
          - dimension: orders.updated_at
            granularity: month
        order:
          - id: orders.created_at
          - id: orders.updated_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_convert_tz_for_raw_time_dimensions() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.created_at
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at
        timezone: \"America/Los_Angeles\"
        convert_tz_for_raw_time_dimension: true
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_basic_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}
