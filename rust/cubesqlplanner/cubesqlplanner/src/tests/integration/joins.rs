use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_joins.yaml");
    TestContext::new(schema).unwrap()
}

fn create_diamond_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_joins_diamond.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_joins_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_join_many_to_one() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
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
async fn test_join_chain() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - order_items.count
          - order_items.total_quantity
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
async fn test_join_diamond() {
    let ctx = create_diamond_context();

    let query = indoc! {"
        measures:
          - order_items.count
          - order_items.total_quantity
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
async fn test_join_dims_from_multiple_cubes() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - order_items.count
        dimensions:
          - products.category
          - orders.status
        order:
          - id: products.category
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_filter_and_dim_from_different_cubes() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - order_items.count
        dimensions:
          - products.name
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
        order:
          - id: products.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_filter_on_chain() {
    let ctx = create_context();

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
        order:
          - id: products.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
