use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_basic_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

// HAVING count > 2 without count in SELECT → completed(5), pending(3) pass
#[tokio::test(flavor = "multi_thread")]
async fn test_having_without_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - orders.status
        filters:
          - member: orders.count
            operator: gt
            values:
              - "2"
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// HAVING count > 2 on joined dimension (customers.city)
#[tokio::test(flavor = "multi_thread")]
async fn test_having_with_joined_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - customers.city
        filters:
          - member: orders.count
            operator: gt
            values:
              - "2"
        order:
          - id: customers.city
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// HAVING on time measure: min_created_at afterDate 2024-02-01 → cancelled (min=Mar 1)
#[tokio::test(flavor = "multi_thread")]
async fn test_having_on_time_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - orders.status
        filters:
          - member: orders.min_created_at
            operator: afterDate
            values:
              - "2024-02-01"
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
