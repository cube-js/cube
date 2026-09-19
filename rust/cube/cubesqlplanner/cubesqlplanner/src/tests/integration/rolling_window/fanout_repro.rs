//! Reproduction for https://github.com/cube-js/cube/issues/11770.
//!
//! Several `rolling_window` measures queried together with a high-cardinality
//! dimension produce a plan whose intermediate row count is
//! (entities × window length × anchors). Two observations feed that.
//!
//! The base scan's date bound was a scalar sub-select over `time_series`, which
//! no engine can eliminate partitions by. That is fixed.
//!
//! The rolling CTE still joins `time_series` to its base CTE on a date range
//! only, so the engine cannot hash-join and its row estimate stays badly off.
//! Restricting that join would first mean giving the series side a dimension
//! column to restrict against, so its test stays ignored — run it with
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

#[test]
#[ignore = "reproduces #11770: rolling join has no equality predicate"]
fn test_rolling_join_restricts_by_dimension() {
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

#[test]
fn test_base_scan_date_bound_is_literal() {
    let ctx = create_context();
    let (sql, params) = ctx.build_sql_and_params(QUERY).unwrap();

    // A bound read back off the series with a scalar sub-select is opaque to
    // partition elimination, and every base scan carries one.
    assert!(
        !sql.contains("min(\"date_from\")"),
        "base scan date bound is a scalar sub-select over time_series:\n{sql}"
    );

    // The frame is folded into the bound rather than applied around it. A bound
    // a dialect still has to evaluate while planning is one it may fail on.
    let base_scans = sql.split("FROM  rw_daily_activity").skip(1);
    for scan in base_scans {
        let predicate = &scan[..scan.find("GROUP BY").unwrap_or(scan.len())];
        assert!(
            !predicate.contains("interval"),
            "the frame is still applied in SQL:\n{predicate}"
        );
    }

    // The range is whole days at day granularity, so both ends land on a bucket
    // boundary; each window's own frame is then folded into its lower bound,
    // seven days back for one and thirty for the other. The rolling join
    // applies the exact frame on top.
    let bounds = params
        .iter()
        .filter_map(|value| value.to_param_string())
        .collect_vec();
    for edge in [
        "2026-07-25T00:00:00.000",
        "2026-07-02T00:00:00.000",
        "2026-09-02T23:59:59.999",
    ] {
        assert!(
            bounds.iter().any(|bound| bound == edge),
            "{edge} missing from the base scan bounds {bounds:?} in:\n{sql}"
        );
    }
}
