//! Pre-aggregations declared in a view. Candidates come from the cubes a query
//! reads, and a view is not one of them, so such a rollup never serves a query
//! and the query reads the source tables.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const SEED: &str = "integration_multi_fact_tables.sql";

fn create_context() -> Result<TestContext, CubeError> {
    let schema = MockSchema::from_yaml_file("common/integration_view_members.yaml")
        .only_pre_aggregations(&["by_status", "by_id"]);
    TestContext::new(schema)
}

fn create_cube_rollup_context() -> Result<TestContext, CubeError> {
    let schema = MockSchema::from_yaml_file("common/integration_view_members.yaml")
        .only_pre_aggregations(&["by_view_status"]);
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

fn assert_uses_cube_rollup(ctx: &TestContext, query: &str) -> Result<(), CubeError> {
    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query)?;
    assert_eq!(
        pre_aggrs.len(),
        1,
        "expected `orders.by_view_status` to be used, got SQL:\n{sql}"
    );
    assert_eq!(pre_aggrs[0].cube_name(), "orders");
    assert_eq!(pre_aggrs[0].name(), "by_view_status");
    Ok(())
}

// A cube rollup may name view members. A query through the view reads it.
#[tokio::test(flavor = "multi_thread")]
async fn test_cube_rollup_naming_view_members_serves_view_query() -> Result<(), CubeError> {
    let ctx = create_cube_rollup_context()?;

    let query = indoc! {"
        measures:
          - orders_view.count
          - orders_view.total_amount
        dimensions:
          - orders_view.status
        order:
          - id: orders_view.status
    "};

    assert_uses_cube_rollup(&ctx, query)?;

    if let Some(result) = ctx.try_execute(query, SEED).await {
        insta::assert_snapshot!(
            "cube_rollup_naming_view_members_serves_view_query_cubestore_result",
            result
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cube_rollup_naming_view_members_with_filter() -> Result<(), CubeError> {
    let ctx = create_cube_rollup_context()?;

    let query = indoc! {"
        measures:
          - orders_view.total_amount
        dimensions:
          - orders_view.status
        filters:
          - dimension: orders_view.status
            operator: equals
            values:
              - completed
    "};

    assert_uses_cube_rollup(&ctx, query)?;

    if let Some(result) = ctx.try_execute(query, SEED).await {
        insta::assert_snapshot!(
            "cube_rollup_naming_view_members_with_filter_cubestore_result",
            result
        );
    }
    Ok(())
}

// A distinct count read at the grain the rollup stores it at.
#[tokio::test(flavor = "multi_thread")]
async fn test_cube_rollup_naming_view_members_count_distinct_at_its_grain() -> Result<(), CubeError>
{
    let ctx = create_cube_rollup_context()?;

    let query = indoc! {"
        measures:
          - orders_view.customers_count
        dimensions:
          - orders_view.status
        order:
          - id: orders_view.status
    "};

    assert_uses_cube_rollup(&ctx, query)?;

    if let Some(result) = ctx.try_execute(query, SEED).await {
        insta::assert_snapshot!(
            "cube_rollup_naming_view_members_count_distinct_at_its_grain_cubestore_result",
            result
        );
    }
    Ok(())
}

// The rollup stores the view measures, which are of type `number`, so it only
// serves the grain it was built at.
#[tokio::test(flavor = "multi_thread")]
async fn test_cube_rollup_naming_view_members_only_serves_its_grain() -> Result<(), CubeError> {
    let ctx = create_cube_rollup_context()?;

    let query = indoc! {"
        measures:
          - orders_view.count
    "};

    assert_no_pre_aggregation(&ctx, query)?;

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

// The rollup is keyed by the view members it names, so a query over the cube
// members they reference does not read it.
#[tokio::test(flavor = "multi_thread")]
async fn test_cube_rollup_naming_view_members_does_not_serve_cube_query() -> Result<(), CubeError> {
    let ctx = create_cube_rollup_context()?;

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};

    assert_no_pre_aggregation(&ctx, query)?;

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}
