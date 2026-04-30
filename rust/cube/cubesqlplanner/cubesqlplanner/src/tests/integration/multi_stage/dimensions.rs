use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

// BUG: Multi-stage dimension SQL generation produces invalid SQL.
// The dimension calculation CTE (cte_1) references "customers".name directly
// from the raw table, but "customers" is not in cte_1's FROM clause — it only
// selects from cte_0.
// Expected: cte_1 should reference the column alias from cte_0, which already
// JOINed the customers table and selected customers.id.
// Actual SQL (cte_1):
//   SELECT "customers".name ...   <-- table not available here
//   FROM (SELECT * FROM cte_0)
// Postgres error: "missing FROM-clause entry for table customers"
// SQL generation (build_sql) succeeds, only Postgres execution fails.

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer_name
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.customer_name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_two_multi_stage_dimensions() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer_name
          - orders.customer_city
        order:
          - id: orders.customer_name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_with_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - orders.customer_name
        filters:
          - dimension: orders.customer_name
            operator: equals
            values:
              - Alice
        order:
          - id: orders.customer_name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_with_regular_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer_name
          - orders.status
        order:
          - id: orders.customer_name
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_dim_with_multi_stage_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_id
        dimensions:
          - orders.customer_name
        order:
          - id: orders.customer_name
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
