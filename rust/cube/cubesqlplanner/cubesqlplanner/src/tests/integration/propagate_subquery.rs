use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_subquery_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_subquery.yaml");
    TestContext::new(schema).unwrap()
}

// Propagated subquery dim + filter on Customers.name='Alice'
// With propagation: subquery also filters by Alice → totalSpendPropagated=130
#[tokio::test(flavor = "multi_thread")]
async fn test_propagated_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - Sales.totalAmount
        dimensions:
          - Customers.totalSpendPropagated
          - Customers.name
        filters:
          - dimension: Customers.name
            operator: equals
            values:
              - Alice
        order:
          - id: Customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Propagated + category filter
#[tokio::test(flavor = "multi_thread")]
async fn test_propagated_with_category_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - Sales.totalAmount
        dimensions:
          - Customers.totalSpendPropagated
          - Customers.name
        filters:
          - dimension: Sales.category
            operator: equals
            values:
              - online
        order:
          - id: Customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Non-propagated control: same query with totalSpend (no propagation)
#[tokio::test(flavor = "multi_thread")]
async fn test_non_propagated_control() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - Sales.totalAmount
        dimensions:
          - Customers.totalSpend
          - Customers.name
        filters:
          - dimension: Customers.name
            operator: equals
            values:
              - Alice
        order:
          - id: Customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Both sub-query dims share their sql, so each must be read from its own
// subquery: excluding Alice's order 1 changes only the propagated one (130 vs 50)
#[tokio::test(flavor = "multi_thread")]
async fn test_propagated_and_plain_side_by_side() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - Customers.count
        dimensions:
          - Customers.totalSpend
          - Customers.totalSpendPropagated
          - Customers.name
        filters:
          - dimension: CustomerOrders.id
            operator: notEquals
            values:
              - "1"
        order:
          - id: Customers.name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plain_and_propagated_side_by_side() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - Customers.count
        dimensions:
          - Customers.totalSpendPropagated
          - Customers.totalSpend
          - Customers.name
        filters:
          - dimension: CustomerOrders.id
            operator: notEquals
            values:
              - "1"
        order:
          - id: Customers.name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
