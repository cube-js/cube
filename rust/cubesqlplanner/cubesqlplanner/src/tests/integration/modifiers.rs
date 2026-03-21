use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_basic_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
    TestContext::new(schema).unwrap()
}

fn create_multi_fact_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_fact.yaml");
    TestContext::new(schema).unwrap()
}

const BASIC_SEED: &str = "integration_basic_tables.sql";
const MULTI_FACT_SEED: &str = "integration_multi_fact_tables.sql";

// 9.1: ORDER BY dimension ASC, measure DESC
#[tokio::test(flavor = "multi_thread")]
async fn test_order_by_dimension_and_measure() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - customers.city
        order:
          - id: customers.city
          - id: orders.total_amount
            desc: true
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.2: LIMIT with OFFSET
#[tokio::test(flavor = "multi_thread")]
async fn test_limit_with_offset() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        order:
          - id: orders.status
        row_limit: \"2\"
        offset: \"1\"
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.3: Ungrouped — single cube, all rows without GROUP BY
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_single_cube() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
          - orders.amount
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.4: Ungrouped with join — dim from joined cube
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_with_join() {
    let ctx = create_basic_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
          - orders.amount
          - customers.name
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, BASIC_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.5: Ungrouped multi-fact — measures from different fact tables
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_multi_fact() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - orders.count
          - returns.count
        dimensions:
          - customers.name
        ungrouped: true
        order:
          - id: customers.name
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, MULTI_FACT_SEED).await {
        insta::assert_snapshot!(result);
    }
}

// 9.6: Ungrouped with multiplied measure
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_multiplied() {
    let ctx = create_multi_fact_context();

    let query = indoc! {"
        measures:
          - customers.count
        dimensions:
          - orders.status
          - customers.name
        ungrouped: true
        order:
          - id: orders.id
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, MULTI_FACT_SEED).await {
        insta::assert_snapshot!(result);
    }
}
