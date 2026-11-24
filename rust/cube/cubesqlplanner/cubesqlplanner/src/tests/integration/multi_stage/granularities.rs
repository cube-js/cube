use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_day_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-02-01"
              - "2024-02-29"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_week_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
            granularity: week
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
async fn test_month_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
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
async fn test_quarter_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
            granularity: quarter
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

fn assert_unique_columns(result: &str) {
    let header = result.lines().next().unwrap();
    let columns = header.split('|').map(str::trim).collect::<Vec<_>>();
    let unique = columns.iter().collect::<std::collections::HashSet<_>>();
    assert_eq!(unique.len(), columns.len(), "duplicate columns: {header}");
}

// `created_at.month` selected as a dimension is the same member as `created_at`
// at granularity month, so it must be projected once.
#[tokio::test(flavor = "multi_thread")]
async fn test_granularity_dimension_and_time_dimension_project_once() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
        dimensions:
          - orders.created_at.month
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at.month
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_unique_columns(&result);
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_granularity_dimension_and_time_dimension_with_keys_subquery() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_grain_and_filter_exclude_status
          - orders.total_amount
        dimensions:
          - orders.category
          - orders.status
          - orders.created_at.month
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
        order:
          - id: orders.created_at.month
          - id: orders.category
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_unique_columns(&result);
        insta::assert_snapshot!(result);
    }
}
