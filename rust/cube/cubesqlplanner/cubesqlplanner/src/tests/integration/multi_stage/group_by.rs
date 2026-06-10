use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_override() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_status
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_with_time() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_status_time
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_subset_of_query_dims() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_status
        dimensions:
          - orders.status
          - orders.category
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
async fn test_group_by_equals_query_dims() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_both
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `grain.keep_only: [status]` narrows the partition to `[status]`; the
// query has only `category`, so the measure value is the per-status total
// broadcast across categories.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_keep_only_status_top_level() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_grain_keep_only_status
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_time_dim_multiple_granularities() {
    let ctx = create_context();

    // Regression test mirroring the BigECommerce postgres-driver case (see
    // packages/cubejs-testing-drivers/src/tests/testQueries.ts
    // "multi-stage group by time dimension"). The query projects two
    // granularities of the same time dim (month + week), while the measure
    // `amount_group_by_status_time` has `group_by: [status, created_at.month]`
    // — only the month granularity should stay in PARTITION BY.
    //
    // Expected: every (month, week) row reports the per-month grand total
    // (Jan=500, Feb=750, Mar=1000), NOT the per-week sum. If `group_by`
    // matched via TimeDimensionSymbol → base unwrapping, the week granularity
    // would also stay in PARTITION BY and the result would collapse to
    // per-(month, week) sums.
    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_status_time
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
          - dimension: orders.created_at
            granularity: week
        order:
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
