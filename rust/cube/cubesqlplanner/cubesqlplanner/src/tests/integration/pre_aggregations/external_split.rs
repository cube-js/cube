//! Multi-stage queries whose stages match pre-aggregations in different
//! external types.
//!
//! One SQL query cannot read from CubeStore and from the source database at
//! once, so a set of per-stage matches that spans both external types is
//! unusable as a whole. These tests pin what the matcher does with such a set,
//! and guard the neighbouring cases it must not disturb: a single wider
//! pre-aggregation is able to cover every stage on its own, and a query matched
//! as a whole keeps the pre-aggregation it already matched.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const YAML: &str = "common/pre_agg_external_split.yaml";
const SEED: &str = "pre_agg_external_split_tables.sql";

// Numerator reads revenue for one brand; the denominator lifts the brand
// filter and totals every brand. Only `by_brand_day` carries brand, so only it
// can serve the numerator, while `by_day` is reached first for the denominator.
const BRAND_SHARE_QUERY: &str = indoc! {"
    measures:
      - sales.brand_share
    time_dimensions:
      - dimension: sales.created_at
        granularity: day
        dateRange:
          - '2024-01-01'
          - '2024-01-31'
    filters:
      - dimension: sales.brand
        operator: equals
        values:
          - acme
"};

fn used(pre_aggrs: &[crate::logical_plan::PreAggregationUsage]) -> String {
    pre_aggrs
        .iter()
        .map(|p| format!("{} (external={})", p.name(), p.external()))
        .collect::<Vec<_>>()
        .join(", ")
}

// Per-stage matching lands on `by_brand_day` for the numerator and `by_day`
// for the denominator, a pair no single query can read. Retrying within one
// external type has to find `by_brand_day` covering both stages.
#[tokio::test(flavor = "multi_thread")]
async fn test_stages_split_across_external_types_serve_from_one_engine() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(YAML))?;

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(BRAND_SHARE_QUERY)?;

    assert_eq!(
        pre_aggrs.len(),
        2,
        "both stages must be served by a pre-aggregation, not the fact table; \
         got [{}]. Generated SQL:\n{sql}",
        used(&pre_aggrs)
    );
    assert!(
        pre_aggrs.iter().all(|p| p.name() == "by_brand_day"),
        "`by_brand_day` is the only candidate that can cover both stages in one \
         external type, so both usages must be it; got [{}]",
        used(&pre_aggrs)
    );
    assert!(
        pre_aggrs.iter().all(|p| p.external()),
        "usages must agree on their external type; got [{}]",
        used(&pre_aggrs)
    );
    assert!(
        !sql.contains("pa_sales"),
        "no stage may read the fact table once a pre-aggregation covers the \
         query. Generated SQL:\n{sql}"
    );

    Ok(())
}

// The wider pre-aggregation can cover both stages on its own: the denominator
// aggregates over the brands it stores. This is what makes a retry within one
// external type possible at all.
#[tokio::test(flavor = "multi_thread")]
async fn test_wider_pre_aggregation_alone_covers_every_stage() -> Result<(), CubeError> {
    let ctx = TestContext::new(
        MockSchema::from_yaml_file(YAML).only_pre_aggregations(&["by_brand_day"]),
    )?;

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(BRAND_SHARE_QUERY)?;

    assert_eq!(
        pre_aggrs.len(),
        2,
        "`by_brand_day` must serve both stages; got [{}]. Generated SQL:\n{sql}",
        used(&pre_aggrs)
    );
    assert!(
        pre_aggrs.iter().all(|p| p.name() == "by_brand_day"),
        "got [{}]",
        used(&pre_aggrs)
    );

    Ok(())
}

// The narrow pre-aggregation carries no brand, so it can serve neither the
// brand-filtered numerator nor, on its own, the whole query.
#[tokio::test(flavor = "multi_thread")]
async fn test_narrow_pre_aggregation_alone_cannot_serve_the_query() -> Result<(), CubeError> {
    let ctx =
        TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&["by_day"]))?;

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(BRAND_SHARE_QUERY)?;

    assert!(
        pre_aggrs.is_empty(),
        "`by_day` cannot apply a brand filter, so it must not serve this query; \
         got [{}]. Generated SQL:\n{sql}",
        used(&pre_aggrs)
    );

    Ok(())
}

// A query that matches as a whole never reaches the multi-stage path, so the
// retry cannot drag it onto the wider pre-aggregation. Candidates are tried in
// declaration order with no ranking between them, so what this pins is that
// `by_day` — declared first, and the narrower of the two — keeps winning.
#[tokio::test(flavor = "multi_thread")]
async fn test_whole_query_match_is_left_alone_by_the_retry() -> Result<(), CubeError> {
    let ctx = TestContext::new(MockSchema::from_yaml_file(YAML))?;

    let query = indoc! {"
        measures:
          - sales.revenue
        time_dimensions:
          - dimension: sales.created_at
            granularity: day
            dateRange:
              - '2024-01-01'
              - '2024-01-31'
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query)?;

    assert_eq!(
        pre_aggrs.len(),
        1,
        "expected a single usage; got [{}]. Generated SQL:\n{sql}",
        used(&pre_aggrs)
    );
    assert_eq!(
        pre_aggrs[0].name(),
        "by_day",
        "a day-grain query must keep matching `by_day`, not the wider \
         `by_brand_day`; got [{}]",
        used(&pre_aggrs)
    );

    Ok(())
}

// Finding the fallback set is only half of it: the denominator now reads
// `by_brand_day` and re-aggregates across the brands it stores, instead of
// reading a pre-summed `by_day`. The rows must still be the ones the fact
// table produces — a brand predicate leaking into that read would leave every
// share at 1.0 while all the assertions above still passed.
//
// `try_execute_pg` rather than `try_execute`: the served plan is external, so
// `try_execute` would route it to CubeStore and skip. The arithmetic under
// test does not depend on which engine stores the rollup.
#[tokio::test(flavor = "multi_thread")]
async fn test_fallback_rows_agree_with_the_fact_table() -> Result<(), CubeError> {
    let served = TestContext::new(MockSchema::from_yaml_file(YAML))?;
    let fact_table = TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[]))?;

    let (_sql, pre_aggrs) = served.build_sql_with_used_pre_aggregations(BRAND_SHARE_QUERY)?;
    assert_eq!(
        pre_aggrs.len(),
        2,
        "the comparison is only meaningful once the fallback fires; got [{}]",
        used(&pre_aggrs)
    );

    let (_sql, no_pre_aggrs) =
        fact_table.build_sql_with_used_pre_aggregations(BRAND_SHARE_QUERY)?;
    assert!(
        no_pre_aggrs.is_empty(),
        "the baseline must read the fact table; got [{}]",
        used(&no_pre_aggrs)
    );

    let served_rows = served.try_execute_pg(BRAND_SHARE_QUERY, SEED).await;
    let fact_table_rows = fact_table.try_execute_pg(BRAND_SHARE_QUERY, SEED).await;

    assert_eq!(
        served_rows, fact_table_rows,
        "rows served from the pre-aggregation must match the fact table"
    );

    // Without Postgres execution there are no rows to compare, so make it
    // explicit that the comparison above does run when it is enabled.
    #[cfg(feature = "integration-postgres")]
    assert!(
        served_rows.is_some(),
        "Postgres execution is enabled but returned no result"
    );

    if let Some(rows) = served_rows {
        insta::assert_snapshot!(rows);
    }

    Ok(())
}
