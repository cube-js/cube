//! A query of multi-stage measures only, grouped by dimensions of two cubes that join
//! only through the fact cube, must still plan when a rollup covers those dimensions
//! (issue #12086).

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const SCHEMA: &str = "common/pre_agg_multi_stage_star_join.yaml";

fn query(measures: &str) -> String {
    format!(
        indoc! {"
            measures:
            {}
            dimensions:
              - customers.name
            time_dimensions:
              - dimension: dates.date
                granularity: year
                dateRange:
                  - \"2025-01-01\"
                  - \"2025-12-31\"
        "},
        measures
    )
}

#[test]
fn test_multi_stage_only_query_plans_with_rollup() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(SCHEMA))?;
    ctx.build_sql_with_used_pre_aggregations(&query("  - orders.amount_prior_year"))?;
    Ok(())
}

#[test]
fn test_multi_stage_only_query_plans_without_rollup() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(SCHEMA).only_pre_aggregations(&[]))?;
    ctx.build_sql_with_used_pre_aggregations(&query("  - orders.amount_prior_year"))?;
    Ok(())
}

#[test]
fn test_multi_stage_with_plain_measure_uses_rollup() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(SCHEMA))?;
    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(&query(
        "  - orders.amount_prior_year\n  - orders.amount",
    ))?;
    assert!(!pre_aggrs.is_empty());
    Ok(())
}
