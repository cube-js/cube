use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_equals_string() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
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
async fn test_filter_not_equals_string() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        filters:
          - dimension: orders.status
            operator: notEquals
            values:
              - completed
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
async fn test_filter_gt_boundary() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.amount
            operator: gt
            values:
              - \"100\"
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
async fn test_filter_gte_boundary() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.amount
            operator: gte
            values:
              - \"100\"
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
async fn test_filter_on_joined_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
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
async fn test_filter_not_set_null() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.city
            operator: notSet
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
async fn test_filter_contains_string() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - customers.name
        filters:
          - dimension: customers.name
            operator: contains
            values:
              - Alice
        order:
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
async fn test_segment_completed_orders() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        segments:
          - orders.completed_orders
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
async fn test_segment_plus_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        segments:
          - orders.completed_orders
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
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
async fn test_or_filter_group() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        filters:
          - or:
            - dimension: orders.status
              operator: equals
              values:
                - cancelled
            - dimension: orders.amount
              operator: gte
              values:
                - \"300\"
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
async fn test_multiple_filters_and_logic() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - pending
          - dimension: orders.amount
            operator: gte
            values:
              - \"50\"
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
async fn test_filter_lt() {
    let ctx = create_context();

    // amount < 100 → orders: 3(50), 5(25), 7(75), 9(40) → count=4
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.amount
            operator: lt
            values:
              - \"100\"
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
async fn test_filter_lte() {
    let ctx = create_context();

    // amount <= 100 → orders: 1(100), 3(50), 5(25), 7(75), 9(40) → count=5
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.amount
            operator: lte
            values:
              - \"100\"
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
async fn test_filter_set() {
    let ctx = create_context();

    // city IS NOT NULL → customers: 1,2,4,5 → orders: 1,2,3,4,6,7,8 → count=7
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.city
            operator: set
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
async fn test_filter_starts_with() {
    let ctx = create_context();

    // name startsWith 'Ali' → Alice Johnson(1), Alice Cooper(4) → orders: 1,2,6,7 → count=4
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.name
            operator: startsWith
            values:
              - Ali
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
async fn test_filter_ends_with() {
    let ctx = create_context();

    // name endsWith 'Smith' → Bob Smith(2) → orders: 3,4 → count=2
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.name
            operator: endsWith
            values:
              - Smith
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
async fn test_filter_not_contains() {
    let ctx = create_context();

    // name notContains 'Alice' → Bob Smith(2), Charlie Brown(3), Diana Prince(5)
    // → orders: 3,4 + 5,9 + 8 → count=5
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.name
            operator: notContains
            values:
              - Alice
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
async fn test_filter_in_date_range() {
    let ctx = create_context();

    // inDateRange ['2024-02-01', '2024-02-29']
    // → >= 2024-02-01T00:00:00 AND <= 2024-02-29T23:59:59.999
    // → orders: 3(Feb 10), 4(Feb 15) → count=2
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.created_at
            operator: inDateRange
            values:
              - \"2024-02-01\"
              - \"2024-02-29\"
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
async fn test_filter_not_in_date_range() {
    let ctx = create_context();

    // notInDateRange ['2024-02-01', '2024-02-29']
    // → < 2024-02-01T00:00:00 OR > 2024-02-29T23:59:59.999
    // → orders: 1,2,9(Jan) + 5,6,7(Mar) + 8(Apr) → count=7
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.created_at
            operator: notInDateRange
            values:
              - \"2024-02-01\"
              - \"2024-02-29\"
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
async fn test_filter_before_date() {
    let ctx = create_context();

    // beforeDate '2024-02-01' → < 2024-02-01T00:00:00.000
    // → orders: 1(Jan15), 2(Jan20), 9(Jan15) → count=3
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.created_at
            operator: beforeDate
            values:
              - \"2024-02-01\"
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
async fn test_filter_after_date() {
    let ctx = create_context();

    // afterDate '2024-03-31' → > 2024-03-31T23:59:59.999
    // → orders: 8(Apr 1 10:00) → count=1
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.created_at
            operator: afterDate
            values:
              - \"2024-03-31\"
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
async fn test_filter_equals_multiple_values() {
    let ctx = create_context();

    // status IN ('completed', 'pending')
    // → orders: 1,2,4,6,8(completed) + 3,7,9(pending) → count=8
    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
              - pending
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
async fn test_measure_filter_having() {
    let ctx = create_context();

    // GROUP BY status, HAVING count > 2
    // completed: 5 rows → ✓, pending: 3 rows → ✓, cancelled: 1 row → ✗
    // → 2 groups: completed(5), pending(3)
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        filters:
          - member: orders.count
            operator: gt
            values:
              - \"2\"
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
async fn test_segment_with_dimension_grouping() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - customers.city
        segments:
          - orders.completed_orders
        order:
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
async fn test_multiple_segments_combined() {
    let ctx = create_context();

    // completed_orders AND sf_customers → only order 4 (completed, Bob=SF)
    // count=1, total_amount=300
    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        segments:
          - orders.completed_orders
          - customers.sf_customers
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
async fn test_segment_from_joined_cube() {
    let ctx = create_context();

    // sf_customers segment on joined cube → Bob's orders: 3(50), 4(300)
    // count=2, total_amount=350
    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        segments:
          - customers.sf_customers
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
async fn test_segment_with_time_dimension() {
    let ctx = create_context();

    // completed_orders + month granularity + dateRange [Jan-Mar]
    // Jan: 2(300), Feb: 1(300), Mar: 1(150)
    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        segments:
          - orders.completed_orders
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - \"2024-01-01\"
              - \"2024-03-31\"
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
