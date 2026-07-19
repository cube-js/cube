use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_basic_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

// city equals [New York, NULL] → customers 1,3,4 (NY + NULL) → orders: 1,2,5,6,7,9 → count=6
#[tokio::test(flavor = "multi_thread")]
async fn test_equals_value_and_null() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - New York
              - ~
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// city notEquals [New York, NULL] → customers 2,5 (SF, Boston) → orders: 3,4,8 → count=3
#[tokio::test(flavor = "multi_thread")]
async fn test_not_equals_value_and_null() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.city
            operator: notEquals
            values:
              - New York
              - ~
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// city contains [York, NULL] → customers with 'York' in city + NULL → 1,3,4 → count=6
#[tokio::test(flavor = "multi_thread")]
async fn test_contains_value_and_null() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.city
            operator: contains
            values:
              - York
              - ~
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// city equals [NULL] → customer 3 only → orders: 5,9 → count=2
#[tokio::test(flavor = "multi_thread")]
async fn test_equals_only_null() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.city
            operator: equals
            values:
              - ~
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// city notEquals [NULL] → customers 1,2,4,5 (all non-NULL) → orders: 1,2,3,4,6,7,8 → count=7
#[tokio::test(flavor = "multi_thread")]
async fn test_not_equals_only_null() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        filters:
          - dimension: customers.city
            operator: notEquals
            values:
              - ~
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
