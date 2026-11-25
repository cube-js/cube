//! Rollups serving a view's own measures over two unrelated facts, and over a
//! multi-stage measure of one of them.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;
use itertools::Itertools;

const SEED: &str = "view_dimension_overrides_unrelated_facts_tables.sql";

fn context() -> Result<TestContext, CubeError> {
    TestContext::new(MockSchema::from_yaml_file(
        "common/view_dimension_overrides_unrelated_facts_pre_aggregations.yaml",
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

// Each fact's leg reads its own rollup. 2026-01: North 110, South 45.
// 2026-02: East -10, North 20, South -30.
#[tokio::test(flavor = "multi_thread")]
async fn test_balance_reads_a_rollup_per_fact() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {r#"
        measures:
          - finance.balance
        dimensions:
          - finance.branch
        time_dimensions:
          - dimension: finance.date
            granularity: month
            dateRange:
              - "2026-01-01"
              - "2026-02-28"
        order:
          - id: finance.date
          - id: finance.branch
    "#};

    assert_eq!(
        used_names(&ctx, query)?,
        vec!["expenses.by_month_branch", "income.by_month_branch"]
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}

// A view measure over a multi-stage measure of one fact is planned as before,
// its leaves read from the rollup: 270 of 270.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_measure_over_a_multi_stage_leaf_keeps_its_rollup() -> Result<(), CubeError> {
    let ctx = context()?;
    let query = indoc! {r#"
        measures:
          - finance.income_share
    "#};

    assert_eq!(used_names(&ctx, query)?, vec!["income.by_month_branch"]);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
    Ok(())
}
