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

fn create_extended_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_joins_extended.yaml");
    TestContext::new(schema).unwrap()
}

const EXTENDED_SEED: &str = "integration_joins_extended_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_join_one_to_one() {
    let ctx = create_extended_context();

    // one_to_one: employees → employee_profiles
    // Engineering: 150k+120k+110k=380000, Sales: 130k+100k=230000
    let query = indoc! {"
        measures:
          - employee_profiles.total_salary
        dimensions:
          - employees.department
        order:
          - id: employees.department
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, EXTENDED_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_self_reference() {
    let ctx = create_extended_context();

    // Self-join: employees.manager_id → managers.id
    // Alice: 2(Bob,Charlie), Bob: 1(Eve), Charlie: 1(Diana)
    let query = indoc! {"
        measures:
          - employees.count
        dimensions:
          - managers.name
        order:
          - id: managers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, EXTENDED_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_compound_key() {
    let ctx = create_extended_context();

    // Compound key: sales.(region,category) → targets.(region,category)
    // (East,Electronics)=250, (East,Clothing)=50, (West,Electronics)=200, (West,Clothing)=75
    let query = indoc! {"
        measures:
          - sales.total_amount
        dimensions:
          - targets.region
          - targets.category
        order:
          - id: targets.region
          - id: targets.category
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, EXTENDED_SEED).await {
        insta::assert_snapshot!(result);
    }
}
