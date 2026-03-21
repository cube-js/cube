use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_fact_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_totals_no_dimensions() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_counts_by_customer_name() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_sums_by_customer_name() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.total_amount
          - returns.total_refund
        dimensions:
          - customers.name
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_grouped_by_city() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_filter_on_shared_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
        dimensions:
          - customers.name
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_filter_on_fact_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
          - returns.count
          - returns.total_refund
        dimensions:
          - customers.name
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_count_distinct_control() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.unique_customers
          - returns.count
          - returns.unique_customers
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_three_fact_tables() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
          - reviews.count
        dimensions:
          - customers.name
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_hub_measure_by_fact_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - customers.count
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_hub_and_fact_measures() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - customers.count
          - orders.count
          - orders.total_amount
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_segment() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        segments:
          - orders.completed_orders
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_measure_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        filters:
          - member: orders.count
            operator: gt
            values:
              - \"1\"
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_measure_filter_on_second_fact() {
    let ctx = create_context();

    // HAVING returns.count > 1 in multi-fact context
    // returns by customer: Alice=1, Bob=2, Charlie=2, Diana=0
    // After filter: Bob(orders=3, returns=2), Charlie(orders=0/NULL, returns=2)
    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        filters:
          - member: returns.count
            operator: gt
            values:
              - \"1\"
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Multi-fact with time dimension: the second fact (returns) is not broken down
// by time dimension. returns.count shows the TOTAL per customer on every time_series row
// instead of per-day values. E.g., Alice has 1 return on Mar 05, but returns.count=1
// appears on Mar 01, 02, 03, 07 as well.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_time_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Multi-fact with time dimension + dateRange: same issue as above, plus
// dateRange does not filter the second fact. Bob has 2 returns on Mar 08-09 (outside
// dateRange Mar 01-05), yet returns.count=2 appears on Mar 04-05.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_time_and_daterange() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - \"2025-03-01\"
              - \"2025-03-05\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Multi-fact with time dimension + filter: second fact not broken down by day.
// returns.count = total per customer on every row. Dimension filter (city=NY) works,
// but time dimension does not apply to returns.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_time_and_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Multi-fact with month granularity: same underlying issue — second fact not
// broken down by time dimension. Masked here because all data is in a single month
// (March 2025), so total-per-customer == total-per-month by coincidence.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_time_and_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.city
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// FIXME: Multi-fact full combo with month granularity: same underlying issue — second
// fact not broken down by time dimension. Masked by month granularity + single month of data.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_full_combo() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - \"2025-03-01\"
              - \"2025-03-31\"
        segments:
          - orders.completed_orders
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_order_and_limit() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        order:
          - id: orders.count
            desc: true
        row_limit: \"2\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_non_multiplied_multi_join() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - customers.name
          - customers.city
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
