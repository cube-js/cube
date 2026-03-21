use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_subquery_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_subquery.yaml");
    TestContext::new(schema).unwrap()
}

fn create_multi_fact_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

fn create_combo_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_combo.yaml");
    TestContext::new(schema).unwrap()
}

fn create_joins_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_joins.yaml");
    TestContext::new(schema).unwrap()
}

fn create_diamond_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_joins_diamond.yaml");
    TestContext::new(schema).unwrap()
}

fn create_basic_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

// 10.1: Multiplied measure + subquery dimension + regular measure
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_subquery_dim() {
    let ctx = create_subquery_context();

    let query = indoc! {"
        measures:
          - Sales.totalAmount
          - Customers.count
        dimensions:
          - Customers.totalSpend
          - Customers.name
        order:
          - id: Customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_subquery_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// 10.2: Multiplied + join filter + segment + time dateRange
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_with_join_filter_segment_time() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - customers.count
          - orders.count
        dimensions:
          - customers.name
        segments:
          - orders.completed_orders
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - \"2025-03-01\"
              - \"2025-03-31\"
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_multi_fact_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// 10.3: Subquery dimension filter + time dateRange
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_filter_with_time_range() {
    let ctx = create_combo_context();

    let query = indoc! {r#"
        measures:
          - Orders.total_amount
        filters:
          - member: Customers.totalSpend
            operator: gt
            values:
              - "100"
        time_dimensions:
          - dimension: Orders.created_at
            granularity: month
            dateRange:
              - "2025-03-01"
              - "2025-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_combo_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// 10.4: Join chain A→B→C with filters at each level
#[tokio::test(flavor = "multi_thread")]
async fn test_join_chain_with_filters_at_each_level() {
    let ctx = create_joins_context();

    let query = indoc! {"
        measures:
          - order_items.count
          - order_items.total_quantity
        dimensions:
          - products.name
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
          - dimension: orders.status
            operator: equals
            values:
              - completed
          - dimension: products.category
            operator: equals
            values:
              - Electronics
        order:
          - id: products.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_joins_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// 10.5: Diamond join with multiplied measures —
//       order_items measures + customers.count (multiplied via order_items→customers)
#[tokio::test(flavor = "multi_thread")]
async fn test_diamond_join_with_multiplied_measures() {
    let ctx = create_diamond_context();

    let query = indoc! {"
        measures:
          - customers.count
          - order_items.count
          - order_items.total_quantity
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_joins_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// 10.6: Everything combined — dim + joined dim + time + dateRange + segment +
//       filter + measure filter + ORDER + LIMIT
#[tokio::test(flavor = "multi_thread")]
async fn test_everything_combined() {
    let ctx = create_combo_context();

    let query = indoc! {r#"
        measures:
          - Orders.count
          - Orders.total_amount
        dimensions:
          - Customers.name
        segments:
          - Orders.completed_orders
        filters:
          - dimension: Customers.city
            operator: equals
            values:
              - New York
          - member: Orders.count
            operator: gt
            values:
              - "1"
        time_dimensions:
          - dimension: Orders.created_at
            granularity: month
            dateRange:
              - "2025-03-01"
              - "2025-04-30"
        order:
          - id: Customers.name
        row_limit: "10"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_combo_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_fact_with_segment_filter_dimension() {
    let ctx = create_multi_fact_context();

    // Multi-fact + segment + filter + dimension
    // orders(completed+NY): Alice(1,2)=2, Diana(6)=1
    // returns(NY): Alice(1)=1, Diana=0
    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_multi_fact_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_with_filter_and_join() {
    let ctx = create_basic_context();

    // Ungrouped + filter on joined dim (city=New York)
    // NY customers: Alice Johnson(1), Alice Cooper(4)
    // Orders: 1,2,6,7
    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.id
          - orders.status
          - customers.name
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
        ungrouped: true
        order:
          - id: orders.id
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
async fn test_order_limit_with_multi_fact() {
    let ctx = create_multi_fact_context();

    // Multi-fact + ORDER + LIMIT
    // Alice(4,1), Bob(3,2), Charlie(0,2), Diana(1,0)
    // Order by orders.count desc, limit 2: Alice(4,1), Bob(3,2)
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

    if let Some(result) = ctx
        .try_execute_pg(query, "integration_multi_fact_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

// 10.7: Filters that eliminate all rows → empty result
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_result_from_filters() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - customers.name
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - Nonexistent City
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
