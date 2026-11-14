use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_add_group_by_single_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_id
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_group_by_with_existing_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_category
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_group_by_multiple_dims() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_status_category
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_group_by_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_id
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_group_by_with_time_proxy() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_category
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[cfg(feature = "integration-postgres")]
async fn query_pg(sql: &str) -> String {
    use crate::test_fixtures::test_utils::{integration_context, pg_service};
    let client = pg_service::connect_and_seed(SEED).await;
    let messages = client.simple_query(sql).await.unwrap();
    integration_context::format_simple_query_results(&messages)
}

// A custom aggregate over a finer grain is grouped back to the query grain.
#[tokio::test(flavor = "multi_thread")]
async fn test_add_group_by_number_agg_over_finer_grain() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.cv_amount_by_id
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    #[cfg(feature = "integration-postgres")]
    {
        use crate::test_fixtures::test_utils::table_rows;
        let result = ctx.try_execute_pg(query, SEED).await.unwrap();
        let expected = query_pg(indoc! {r#"
            SELECT status, STDDEV(total) / NULLIF(AVG(total), 0)
            FROM (SELECT status, id, SUM(amount) AS total FROM ms_orders GROUP BY 1, 2) AS t
            GROUP BY 1
            ORDER BY 1
        "#})
        .await;
        assert_eq!(
            table_rows(&result),
            table_rows(&expected),
            "cube:\n{result}\noracle:\n{expected}"
        );
    }
}
