use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

// BUG: Case/switch multi-stage measures generate incorrect SQL.
// The leaf CTE is filtered by the first WHEN value (e.g. status = 'completed')
// even though no such filter was specified in the query.
// Expected: each WHEN value should produce a separate leaf CTE with its own
// filter, then the final CTE combines results via CASE expression
// (cross-join pattern, similar to calc_groups test).
// Actual: only one leaf CTE with first WHEN value as filter, so only
// 'completed' rows are processed. Result shows completed totals (300, 500, 600)
// instead of weighted sums across all statuses (375, 600, 750).

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.status_weighted_amount
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

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.status_weighted_amount
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_case_switch_with_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.status_weighted_amount
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
