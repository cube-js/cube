//! Rollups serving view measures under dimension overrides: each measure reads
//! the rollup stored by its own date role, and never one stored by another.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;
use itertools::Itertools;

const SEED: &str = "view_dimension_overrides_tables.sql";

fn context() -> Result<TestContext, CubeError> {
    TestContext::new(MockSchema::from_yaml_file(
        "common/view_dimension_overrides_pre_aggregations.yaml",
    ))
}

fn used_names(ctx: &TestContext, query: &str) -> Result<Vec<String>, CubeError> {
    let (_, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query)?;
    Ok(pre_aggrs
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .sorted()
        .collect())
}

// 2026-02: 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_overridden_measure_reads_its_role_rollup() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {r#"
        measures:
          - biz.cancellations_cancellations
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    assert_eq!(
        used_names(&ctx, query)?,
        vec!["subscriptions.cancellations_by_cancel_month"]
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

// 2026-01: 1 / - / -, 2026-02: 1 / 2 / 2, each leg from its own rollup.
#[tokio::test(flavor = "multi_thread")]
async fn test_each_leg_reads_its_role_rollup() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_cancellations
          - biz.carts_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    assert_eq!(
        used_names(&ctx, query)?,
        vec![
            "carts.counts_by_month",
            "subscriptions.cancellations_by_cancel_month",
            "subscriptions.counts_by_purchase_month",
        ]
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

// `count` is stored by purchase month only; by cancel month it has no rollup.
// Read from the purchase rollup it would give 2026-01: 1, 2026-02: 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_rollup_of_another_role_does_not_match() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {r#"
        measures:
          - biz.cancellations_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    assert!(used_names(&ctx, query)?.is_empty());

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

// Only the purchase leg has a rollup, and legs are read from rollups all or
// none, so both come from the tables; the cancel leg must not read the
// purchase rollup: 2026-01: 1 / -, 2026-02: 1 / 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_one_cube_measure_under_two_roles_with_one_rollup() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {r#"
        measures:
          - biz.purchases_count
          - biz.cancellations_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
        order:
          - id: biz.dt_year_month
    "#};

    assert!(used_names(&ctx, query)?.is_empty());

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}
