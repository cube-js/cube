//! Reproduction for https://github.com/cube-js/cube/issues/11770.
//!
//! Several `rolling_window` measures queried together with a high-cardinality
//! dimension produce a plan whose intermediate row count is
//! (entities × window length × anchors):
//!
//! * every rolling measure gets its own scan of the fact table, even when the
//!   window and the filters are byte-identical (5 scans for the query below,
//!   which only needs two distinct windows), and
//! * every rolling CTE joins `time_series` to its base CTE on a date range
//!   only. With no equality predicate the engine cannot hash-join: Postgres
//!   picks a nested loop with a join filter and materialises ~954K rows for a
//!   7-day window over 4.7K entities × 33 anchors (~3.6M for the 30-day one)
//!   before the `GROUP BY` separates the entities again.
//!
//! The per-measure scan is fixed, and so is the third observation below — the
//! base scan's date bound. The join shape is not: restricting it by the
//! dimension would first mean giving the series side a dimension column to
//! restrict against, since it carries none. The test asserting the restricted
//! shape therefore stays ignored — run it with
//! `cargo test rolling_window::fanout_repro -- --ignored`.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use itertools::Itertools;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window_fanout.yaml");
    TestContext::new(schema).unwrap()
}

/// Three calculated measures over five rolling sums (two distinct windows),
/// grouped by a high-cardinality dimension over a 33-day day-granularity range.
const QUERY: &str = indoc! {r#"
    measures:
      - daily_activity.events_per_hour_7d
      - daily_activity.events_per_hour_30d
      - daily_activity.error_rate_7d
    dimensions:
      - daily_activity.entity_id
    time_dimensions:
      - dimension: daily_activity.activity_date
        granularity: day
        dateRange:
          - "2026-08-01"
          - "2026-09-02"
"#};

/// Collects the `ON` condition of every rolling-window join in the plan.
fn rolling_join_conditions(sql: &str) -> Vec<String> {
    sql.split("AS \"rolling_source\" ON ")
        .skip(1)
        .map(|tail| {
            let end = tail.find("\n  GROUP BY").unwrap_or(tail.len());
            tail[..end].to_string()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_base_table_is_not_rescanned_per_rolling_measure() {
    let ctx = create_context();
    let sql = ctx.build_sql(QUERY).unwrap();

    // Five rolling sums, but only two distinct (window, filter) pairs:
    // trailing 7 day and trailing 30 day, both `offset: start`.
    assert!(
        sql.matches("rw_daily_activity").count() <= 2,
        "fact table is scanned {} times:\n{sql}",
        sql.matches("rw_daily_activity").count()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "reproduces #11770: rolling join has no equality predicate"]
async fn test_rolling_join_restricts_by_dimension() {
    let ctx = create_context();
    let sql = ctx.build_sql(QUERY).unwrap();

    let conditions = rolling_join_conditions(&sql);
    assert!(!conditions.is_empty(), "no rolling join found in:\n{sql}");
    for condition in conditions.iter() {
        assert!(
            condition.contains("\"daily_activity__entity_id\" ="),
            "rolling join has no equality predicate on the group-by dimension: {condition}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_base_scan_date_bound_is_literal() {
    let ctx = create_context();
    let (sql, params) = ctx.build_sql_and_params(QUERY).unwrap();

    // A bound read back off the series with a scalar sub-select is opaque to
    // partition elimination, and every base scan carries one.
    assert!(
        !sql.contains("min(\"date_from\")"),
        "base scan date bound is a scalar sub-select over time_series:\n{sql}"
    );

    // The literals span the series exactly here — the range is whole days at
    // day granularity, so both ends land on a bucket boundary. The trailing
    // interval is subtracted from the lower bound in SQL, and the rolling join
    // applies the exact frame on top.
    let bounds = params
        .iter()
        .filter_map(|value| value.to_param_string())
        .unique()
        .collect_vec();
    assert_eq!(
        bounds,
        vec!["2026-08-01T00:00:00.000", "2026-09-02T23:59:59.999"],
        "unexpected base scan bounds in:\n{sql}"
    );
}

// Sharing a base scan must not cost pre-aggregation coverage: a rollup carrying
// one of the shared measures cannot answer for a scan carrying both, so a model
// storing one rollup per rolling measure would silently fall to the fact table.
#[tokio::test(flavor = "multi_thread")]
async fn test_sharing_yields_to_a_rollup_per_rolling_measure() {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window_fanout_preagg.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - daily_activity.events_7d
          - daily_activity.minutes_7d
        dimensions:
          - daily_activity.entity_id
        time_dimensions:
          - dimension: daily_activity.activity_date
            granularity: day
            dateRange:
              - "2026-08-01"
              - "2026-09-02"
    "#};

    let (sql, used) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let mut names = used
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .collect_vec();
    names.sort();

    assert_eq!(
        names,
        vec![
            "daily_activity.rolling_events",
            "daily_activity.rolling_minutes"
        ],
        "{sql}"
    );
}
