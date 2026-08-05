//! Eligibility of pre-aggregations for ungrouped queries.
//!
//! An ungrouped query returns raw rows, so a pre-aggregation may serve it only
//! when each stored row maps to exactly one row of the query result. These tests
//! pin both directions of that rule: rollups whose grouping collapses rows must
//! be refused, and rollups that are provably 1:1 with raw rows must be kept.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

fn ctx_with(pre_aggregations: &[&str]) -> Result<TestContext, CubeError> {
    let schema = MockSchema::from_yaml_file("common/ungrouped_pre_agg_gate.yaml")
        .only_pre_aggregations(pre_aggregations);
    TestContext::new(schema)
}

fn used_names(pre_aggrs: &[crate::logical_plan::PreAggregationUsage]) -> String {
    pre_aggrs
        .iter()
        .map(|p| p.name().clone())
        .collect::<Vec<_>>()
        .join(", ")
}

// A cube may legally declare no primary key. Nothing then identifies a raw row,
// so no rollup can be read as raw rows and the query must fall back to the
// source table.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_rollup_when_cube_has_no_primary_key() -> Result<(), CubeError> {
    let ctx = ctx_with(&["collapsing"])?;

    let query_yaml = indoc! {"
        measures:
          - no_pk_orders.total
        dimensions:
          - no_pk_orders.status
          - no_pk_orders.city
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "a cube without a primary key has no raw-row identity, so `{}` must not \
         serve an ungrouped query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// `chain_checkins` is not named by the query, but it sits between the two cubes
// that are, and its one_to_many edge multiplies rows. The rollup is grouped by
// the visitor and city keys only, so it stores one row per (visitor, city) pair
// while the raw join yields one row per checkin.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_rollup_missing_transit_cube_key() -> Result<(), CubeError> {
    let ctx = ctx_with(&["chain_rollup"])?;

    let query_yaml = indoc! {"
        dimensions:
          - chain_visitors.source
          - chain_cities.name
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "the fan-out transit cube `chain_checkins` is unaccounted for, so `{}` \
         must not serve an ungrouped query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// A many_to_one join cannot multiply rows, so the fact primary key alone
// identifies every stored row and the dimension cube's key is not needed.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_accepts_star_rollup_without_dimension_cube_key() -> Result<(), CubeError> {
    let ctx = ctx_with(&["star_rollup"])?;

    let query_yaml = indoc! {"
        measures:
          - star_checkins.cnt
        dimensions:
          - star_checkins.id
          - star_visitors.source
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert_eq!(
        pre_aggrs.len(),
        1,
        "the fact key identifies each row across a many_to_one join, so \
         `star_rollup` should be used. Generated SQL:\n{sql}"
    );
    assert_eq!(pre_aggrs[0].name(), "star_rollup");

    Ok(())
}

// `own_ref` is a proxy for the fact cube's own primary key, which row identity
// does require, so the key is stored even though it is spelled under another
// name and only a resolved comparison can see it.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_accepts_rollup_storing_primary_key_by_reference() -> Result<(), CubeError> {
    let ctx = ctx_with(&["proxy_rollup"])?;

    let query_yaml = indoc! {"
        measures:
          - star_checkins.cnt
        dimensions:
          - star_checkins.own_ref
          - star_visitors.source
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert_eq!(
        pre_aggrs.len(),
        1,
        "`own_ref` resolves to `star_checkins.id`, so `proxy_rollup` does store \
         the identifying key and should be used. Generated SQL:\n{sql}"
    );
    assert_eq!(pre_aggrs[0].name(), "proxy_rollup");

    Ok(())
}

// A cross-cube segment only filters rows; it neither adds columns nor changes
// the stored grain, so the segment's cube must not be required to contribute a
// primary key.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_accepts_rollup_whose_extra_cube_comes_from_a_segment(
) -> Result<(), CubeError> {
    let ctx = ctx_with(&["seg_rollup"])?;

    let query_yaml = indoc! {"
        measures:
          - star_checkins.cnt
        dimensions:
          - star_checkins.id
          - star_checkins.visitor_id
        segments:
          - star_visitors.source_google
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert_eq!(
        pre_aggrs.len(),
        1,
        "a segment only filters, so `seg_rollup` stays 1:1 with raw rows and \
         should be used. Generated SQL:\n{sql}"
    );
    assert_eq!(pre_aggrs[0].name(), "seg_rollup");

    Ok(())
}

// The rollup is 1:1 with raw rows, but `uniq` is stored as an HLL sketch that
// only means anything after a merge. An ungrouped read projects the column
// as-is, so it would hand the client a binary sketch instead of a count.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_rollup_storing_non_additive_state() -> Result<(), CubeError> {
    let ctx = ctx_with(&["hll_pk_rollup"])?;

    let query_yaml = indoc! {"
        measures:
          - hll_orders.uniq
        dimensions:
          - hll_orders.id
          - hll_orders.status
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "an ungrouped read cannot finalize the stored HLL state, so `{}` must \
         not serve this query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// A fan-out measure under `ungrouped` is rendered inline rather than through a
// multiplied subquery, so this stays a simple query. The rollup groups by
// non-key columns and must be refused.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_collapsing_rollup_with_fan_out_measure() -> Result<(), CubeError> {
    let ctx = ctx_with(&["mult_collapsing"])?;

    let query_yaml = indoc! {"
        measures:
          - mult_customers.count
        dimensions:
          - mult_customers.name
          - mult_orders.status
        ungrouped: true
        cubestoreSupportMultistage: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "`{}` groups by non-key columns, so it must not serve an ungrouped \
         query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// A non-additive rolling measure makes the planner render its leaf stage
// ungrouped, but the user query is grouped and asks for aggregates, so the
// raw-row rule must not be applied to that leaf.
#[tokio::test(flavor = "multi_thread")]
async fn test_grouped_non_additive_rolling_query_still_uses_rollup() -> Result<(), CubeError> {
    let ctx = ctx_with(&["rolling_avg_rollup"])?;

    let query_yaml = indoc! {r#"
        measures:
          - roll_orders.rolling_avg_7d
        dimensions:
          - roll_orders.category
        time_dimensions:
          - dimension: roll_orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
        cubestoreSupportMultistage: true
    "#};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert_eq!(
        pre_aggrs.len(),
        1,
        "the user query is grouped, so the internal ungrouped leaf must not cost \
         it `rolling_avg_rollup`. Generated SQL:\n{sql}"
    );
    assert_eq!(pre_aggrs[0].name(), "rolling_avg_rollup");

    Ok(())
}

// `lam_union` exposes only its first member rollup's symbols. The second branch
// groups by a non-unique column that merely shares the short name `id`, so the
// union as a whole is not 1:1 with raw rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_lambda_whose_other_branch_collapses() -> Result<(), CubeError> {
    let ctx = ctx_with(&["lam_base", "lam_union"])?;

    let query_yaml = indoc! {"
        measures:
          - lam_a.count
        dimensions:
          - lam_a.id
          - lam_a.visitor_id
        time_dimensions:
          - dimension: lam_a.created_at
            granularity: day
        ungrouped: true
        pre_aggregation_id: lam_a.lam_union
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "the union's second branch is grouped on a non-key column, so `{}` must \
         not serve an ungrouped query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// `rj_keys_join` declares both primary keys, but the rollups it actually reads
// store neither `rj_checkins.id` nor a per-checkin grain, so its rows are one
// per visitor.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_rollup_join_advertising_unstored_keys() -> Result<(), CubeError> {
    let ctx = ctx_with(&["rj_base", "rj_keys_join"])?;

    let query_yaml = indoc! {"
        measures:
          - rj_checkins.count
        dimensions:
          - rj_checkins.visitor_id
          - rj_visitors.source
        ungrouped: true
        pre_aggregation_id: rj_checkins.rj_keys_join
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "the member rollups do not store `rj_checkins.id`, so `{}` must not serve \
         an ungrouped query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// The single-cube shape: the cube has a key, the rollup omits it, so its rows
// are collapsed and an ungrouped query must read the source table instead.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_single_cube_rejects_rollup_missing_primary_key() -> Result<(), CubeError> {
    let ctx = ctx_with(&["sc_coarse"])?;

    let query_yaml = indoc! {"
        measures:
          - sc_orders.total
        dimensions:
          - sc_orders.status
          - sc_orders.city
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "`{}` groups by non-key columns, so it must not serve an ungrouped \
         single-cube query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// A filter is enough to pull a fan-out cube into the join even though the query
// selects none of its members, so row identity has to account for it.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_rollup_when_a_filter_widens_the_join() -> Result<(), CubeError> {
    let ctx = ctx_with(&["chain_rollup"])?;

    let query_yaml = indoc! {"
        dimensions:
          - chain_visitors.id
          - chain_visitors.source
        filters:
          - dimension: chain_cities.name
            operator: equals
            values:
              - X
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "the filter joins in the fan-out cube `chain_checkins`, so `{}` must not \
         serve an ungrouped query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// The mirror direction of the rule: the rollup is grouped across a fan-out cube
// the query never joins, so it holds more rows than the query asks for and would
// return each visitor once per city.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_rollup_stored_at_a_finer_grain() -> Result<(), CubeError> {
    let ctx = ctx_with(&["chain_rollup"])?;

    let query_yaml = indoc! {"
        dimensions:
          - chain_visitors.id
          - chain_visitors.source
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "`{}` is stored per (visitor, city) while the query reads one row per \
         visitor, so it must not serve it. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// A segment only filters, but reaching it joins in the fan-out cube, so a rollup
// keyed on the root alone no longer holds one row per output row.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_rollup_whose_segment_crosses_a_fan_out() -> Result<(), CubeError> {
    let ctx = ctx_with(&["root_key_seg_rollup"])?;

    let query_yaml = indoc! {"
        dimensions:
          - chain_visitors.id
          - chain_visitors.source
        segments:
          - chain_cities.named_x
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "the segment joins in the fan-out cube `chain_checkins`, so `{}` must not \
         serve an ungrouped query. Generated SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}

// `bridge_stores` only bridges two many_to_one edges, so it cannot split a row
// and its key is no part of row identity — the fact key alone identifies the
// output row even though the bridge sits in the middle of the join.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_accepts_rollup_across_a_many_to_one_bridge() -> Result<(), CubeError> {
    let ctx = ctx_with(&["bridge_rollup"])?;

    let query_yaml = indoc! {"
        measures:
          - bridge_orders.cnt
        dimensions:
          - bridge_orders.id
          - bridge_regions.name
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert_eq!(
        pre_aggrs.len(),
        1,
        "a many_to_one bridge cannot split a row, so `bridge_rollup` should be \
         used. Generated SQL:\n{sql}"
    );
    assert_eq!(pre_aggrs[0].name(), "bridge_rollup");

    Ok(())
}

// A measure joins whatever its own sql references, but the rollup aggregates
// that join away, so the referenced cube is no part of the stored grain and must
// not make the rollup ineligible.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_accepts_rollup_with_a_measure_reaching_another_cube(
) -> Result<(), CubeError> {
    let ctx = ctx_with(&["mult_keyed"])?;

    let query_yaml = indoc! {"
        dimensions:
          - mult_customers.id
          - mult_customers.name
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert_eq!(
        pre_aggrs.len(),
        1,
        "`mult_keyed` is grouped by the primary key, so it should be used. \
         Generated SQL:\n{sql}"
    );
    assert_eq!(pre_aggrs[0].name(), "mult_keyed");

    Ok(())
}

// A rollup's segments are appended to its dimension list when the table is
// materialized, so a segment groups the stored rows just like a dimension. One
// reaching across a row-splitting join therefore stores a row per (entity,
// segment value) even though the query never asks for the segment.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rejects_rollup_whose_unrequested_segment_splits_rows(
) -> Result<(), CubeError> {
    let ctx = ctx_with(&["root_key_seg_rollup"])?;

    let query_yaml = indoc! {"
        dimensions:
          - chain_visitors.id
          - chain_visitors.source
        ungrouped: true
    "};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query_yaml)?;

    assert!(
        pre_aggrs.is_empty(),
        "`{}` groups by a segment of `chain_cities`, reached over the fan-out to \
         `chain_checkins`, so it holds more than one row per visitor. Generated \
         SQL:\n{sql}",
        used_names(&pre_aggrs)
    );

    Ok(())
}
