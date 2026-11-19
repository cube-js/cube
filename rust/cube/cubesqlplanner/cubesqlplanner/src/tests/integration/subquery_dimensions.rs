use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, try_execute_raw_pg, TestContext};
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_subquery.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_subquery_tables.sql";

// 7.1: Subquery dimension in projection
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_in_projection() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - Sales.totalAmount
        dimensions:
          - Customers.totalSpend
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 7.2: Subquery dimension in filter
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_in_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - Sales.totalAmount
        filters:
          - member: Customers.totalSpend
            operator: gt
            values:
              - "100"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 7.3: Subquery dimension in projection and filter
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_in_projection_and_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - Sales.totalAmount
        dimensions:
          - Customers.totalSpend
        filters:
          - member: Customers.totalSpend
            operator: gt
            values:
              - "100"
        order:
          - id: Customers.totalSpend
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 7.4: Subquery dimension with regular dimension
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_with_regular_dim() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - Sales.totalAmount
        dimensions:
          - Customers.totalSpend
          - Customers.name
        order:
          - id: Customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 7.5: Subquery dimension with multi-fact (Sales + Refunds)
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_with_multi_fact() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - Sales.totalAmount
          - Refunds.totalRefund
        dimensions:
          - Customers.totalSpend
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 7.6: Subquery dimension with multiplied measure
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dim_with_multiplied_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - Customers.count
        dimensions:
          - Sales.category
          - Customers.totalSpend
        order:
          - id: Sales.category
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Subquery join aliases derive from `sql_alias`: built from a cube name longer
// than 63 chars, two of them would truncate to the same Postgres identifier.
#[tokio::test(flavor = "multi_thread")]
async fn test_subquery_dims_of_long_named_cube_with_sql_alias() {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/subquery_dimensions_sql_alias.yaml",
    ))
    .unwrap();

    let query = indoc! {"
        dimensions:
          - customers_with_a_very_long_cube_name_that_overflows_postgres_identifiers.id
          - customers_with_a_very_long_cube_name_that_overflows_postgres_identifiers.orderTotal
          - customers_with_a_very_long_cube_name_that_overflows_postgres_identifiers.orderCount
        order:
          - id: customers_with_a_very_long_cube_name_that_overflows_postgres_identifiers.id
    "};

    ctx.build_sql(query).unwrap();

    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    let oracle = try_execute_raw_pg(
        indoc! {"
            SELECT c.id, sum(o.amount), count(o.id)
            FROM customers c
            LEFT JOIN customer_orders o ON o.customer_id = c.id
            GROUP BY c.id
            ORDER BY c.id
        "},
        SEED,
    )
    .await
    .unwrap();
    assert_eq!(table_rows(&result), table_rows(&oracle), "{result}");
}
