use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_where_filter() {
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
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_filter_only() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-02-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_filter() {
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
        segments:
          - orders.completed_orders
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_filters() {
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
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
          - dimension: orders.category
            operator: equals
            values:
              - electronics
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_or_filter() {
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
        filters:
          - or:
              - dimension: orders.status
                operator: equals
                values:
                  - cancelled
              - dimension: orders.category
                operator: equals
                values:
                  - books
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_on_multi_stage_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_reduce_category
        dimensions:
          - orders.status
          - orders.category
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
              - pending
        order:
          - id: orders.status
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_on_only_multi_stage_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - orders.status
          - orders.category
        filters:
          - member: orders.amount_reduce_category
            operator: gt
            values:
              - "200"
        order:
          - id: orders.status
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
