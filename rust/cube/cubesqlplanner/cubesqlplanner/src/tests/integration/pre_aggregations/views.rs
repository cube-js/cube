//! Pre-aggregations declared in a view. Candidates come from the cubes a query
//! reads, and a view is not one of them, so such a rollup never serves a query
//! and the query reads the source tables.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const SEED: &str = "integration_multi_fact_tables.sql";

fn create_context() -> Result<TestContext, CubeError> {
    let schema = MockSchema::from_yaml_file("common/integration_view_members.yaml");
    TestContext::new(schema)
}

fn assert_no_pre_aggregation(ctx: &TestContext, query: &str) -> Result<(), CubeError> {
    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query)?;
    assert!(
        pre_aggrs.is_empty(),
        "expected no pre-aggregation to be used, got SQL:\n{sql}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_rollup_is_not_a_candidate() -> Result<(), CubeError> {
    let ctx = create_context()?;

    let query = indoc! {"
        measures:
          - orders_view.count
          - orders_view.total_amount
        dimensions:
          - orders_view.status
        time_dimensions:
          - dimension: orders_view.created_at
            granularity: month
        order:
          - id: orders_view.created_at
          - id: orders_view.status
    "};

    assert_no_pre_aggregation(&ctx, query)?;

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_rollup_named_by_id_is_not_a_candidate() -> Result<(), CubeError> {
    let ctx = create_context()?;

    let query = indoc! {"
        measures:
          - orders_view.total_amount
        dimensions:
          - orders_view.id
          - orders_view.status
        order:
          - id: orders_view.id
        ungrouped: true
        pre_aggregation_id: orders_view.by_id
    "};

    assert_no_pre_aggregation(&ctx, query)?;

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}
