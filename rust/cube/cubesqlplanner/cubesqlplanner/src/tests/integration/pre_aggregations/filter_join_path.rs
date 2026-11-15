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

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_in_or_group_over_other_path_does_not_match() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - sales_by_gross_brand.amount
        filters:
          - or:
              - dimension: sales_by_gross_brand.name
                operator: equals
                values:
                  - A
              - dimension: sales_by_gross_brand.name
                operator: equals
                values:
                  - C
    "};

    assert!(used_names(&ctx, query)?.is_empty());
    Ok(())
}

// Hcp 2 has gross sales 2 and 3, so the rollup stores its sale under both;
// summing the two rows would count it twice.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_rollup_is_not_rolled_up_over_filter_member() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - sp_sales.amount
        filters:
          - dimension: gross_sales.id
            operator: equals
            values:
              - '2'
              - '3'
    "};

    assert!(used_names(&ctx, query)?.is_empty());

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

// A single value pins one stored row, so nothing is summed across it.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_rollup_serves_filter_pinned_to_one_value() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - sp_sales.amount
        filters:
          - dimension: gross_sales.id
            operator: equals
            values:
              - '2'
    "};

    assert_eq!(used_names(&ctx, query)?, vec!["sp_sales.by_gross_sale"]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_rollup_serves_its_own_grouping() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - sp_sales.amount
        dimensions:
          - gross_sales.id
        filters:
          - dimension: gross_sales.id
            operator: equals
            values:
              - '2'
              - '3'
        order:
          - id: gross_sales.id
    "};

    assert_eq!(used_names(&ctx, query)?, vec!["sp_sales.by_gross_sale"]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

// Hcp 1 has sales of brands 1 and 2, so the rollup counts it under each.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_rollup_rooted_at_many_side_is_not_rolled_up_over_filter_member(
) -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {"
        measures:
          - hcps.count
        filters:
          - dimension: sp_sales.brand_id
            operator: equals
            values:
              - '1'
              - '2'
    "};

    assert!(used_names(&ctx, query)?.is_empty());

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}
