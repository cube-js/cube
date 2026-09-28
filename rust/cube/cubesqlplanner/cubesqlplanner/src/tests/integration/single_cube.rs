use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_measures_no_dimensions() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - orders.avg_amount
          - orders.min_amount
          - orders.max_amount
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
async fn test_single_dimension_with_measures() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - orders.status
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
async fn test_multiple_dimensions_with_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
          - customers.name
        order:
          - id: orders.status
          - id: customers.name
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
async fn test_dimensions_only() {
    let ctx = create_context();

    let query = indoc! {"
        dimensions:
          - orders.status
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
async fn test_count_distinct() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.unique_customers
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
async fn test_count_distinct_approx() {
    let ctx = create_context();

    // countDistinctApprox via HLL — unique customers = 5
    let query = indoc! {"
        measures:
          - orders.approx_unique_customers
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
async fn test_multiple_count_distinct() {
    let ctx = create_context();

    // Two countDistinct measures: unique_customers=5, unique_statuses=3
    let query = indoc! {"
        measures:
          - orders.unique_customers
          - orders.unique_statuses
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
async fn test_number_measure() {
    let ctx = create_context();

    // Calculated measure: total_amount / count = 1440 / 9 = 160
    let query = indoc! {"
        measures:
          - orders.avg_order_value
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
async fn test_dimension_case_when() {
    let ctx = create_context();

    // CASE WHEN amount_tier: high(>=200)=3, medium(>=75)=3, low(<75)=3
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.amount_tier
        order:
          - id: orders.amount_tier
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
async fn test_dimension_concat() {
    let ctx = create_context();

    // CONCAT(name, ' from ', city) — NULL for Charlie Brown (city=NULL)
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - customers.full_location
        order:
          - id: customers.full_location
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
async fn test_number_measure_with_dimension() {
    let ctx = create_context();

    // Calculated measure by status:
    // completed: 1250/5=250, pending: 165/3=55, cancelled: 25/1=25
    let query = indoc! {"
        measures:
          - orders.avg_order_value
        dimensions:
          - orders.status
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
