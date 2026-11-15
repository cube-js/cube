//! A rollup stores a member reached over one join path, and a view pins the
//! same member to another. A filter on the view member must read it over the
//! view's path, as a dimension does, or the two disagree.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const SEED: &str = "integration_filter_join_path_tables.sql";

fn context() -> Result<TestContext, CubeError> {
    TestContext::new(MockSchema::from_yaml_file(
        "common/integration_filter_join_path.yaml",
    ))
}

fn used_names(ctx: &TestContext, query: &str) -> Result<Vec<String>, CubeError> {
    let (_, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query)?;
    Ok(pre_aggrs
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_over_other_path_does_not_match() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - sales_by_gross_brand.amount
        filters:
          - dimension: sales_by_gross_brand.name
            operator: equals
            values:
              - A
    "};

    assert!(used_names(&ctx, query)?.is_empty());

    // Only hcp 2 has a gross sale of brand A; the rollup's path would give 50.
    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dimension_over_other_path_does_not_match() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - sales_by_gross_brand.amount
        dimensions:
          - sales_by_gross_brand.name
        order:
          - id: sales_by_gross_brand.name
    "};

    assert!(used_names(&ctx, query)?.is_empty());

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_over_rollup_path_matches() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - sales_by_own_brand.amount
        filters:
          - dimension: sales_by_own_brand.name
            operator: equals
            values:
              - A
    "};

    assert_eq!(used_names(&ctx, query)?, vec!["sp_sales.by_brand"]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cube_filter_on_default_path_matches() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - sp_sales.amount
        filters:
          - dimension: brands.name
            operator: equals
            values:
              - A
    "};

    assert_eq!(used_names(&ctx, query)?, vec!["sp_sales.by_brand"]);
    Ok(())
}
