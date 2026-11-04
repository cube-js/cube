//! Week rolling windows over week rollups, used only for week-aligned buckets.
//! `weekly` executes in Postgres, `weekly_external` in CubeStore.

use super::switch_rolling::normalize;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::{formatdoc, indoc};

const YAML: &str = "common/integration_week_rolling_window.yaml";
const SEED: &str = "integration_week_rolling_window_tables.sql";

const ALL_MEASURES: &[&str] = &[
    "trailing_3w_count",
    "trailing_3w_amount",
    "cumulative_count",
    "trailing_3w_pct",
    "around_1w_count",
    "leading_1w_start_count",
];

fn ctx(pre_aggs: &[&str]) -> TestContext {
    let schema = MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs);
    if pre_aggs.contains(&"weekly_external") {
        TestContext::new_with_external_cubestore(schema).unwrap()
    } else {
        TestContext::new(schema).unwrap()
    }
}

fn query(measures: &[&str], granularity: &str, from: &str, to: &str) -> String {
    let measures = measures
        .iter()
        .map(|m| format!("  - events.{m}"))
        .collect::<Vec<_>>()
        .join("\n");
    formatdoc! {r#"
        measures:
        {measures}
        time_dimensions:
          - dimension: events.created_at
            granularity: {granularity}
            dateRange:
              - "{from}"
              - "{to}"
        order:
          - id: events.created_at
    "#}
}

/// A single total over the date range, with no time buckets.
fn total_query(from: &str, to: &str) -> String {
    formatdoc! {r#"
        measures:
          - events.trailing_3w_count
          - events.trailing_3w_amount
          - events.cumulative_count
        time_dimensions:
          - dimension: events.created_at
            dateRange:
              - "{from}"
              - "{to}"
    "#}
}

fn used_pre_aggregations(ctx: &TestContext, query: &str) -> Vec<String> {
    let (_sql, used) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let mut names = used.iter().map(|u| u.name().clone()).collect::<Vec<_>>();
    names.dedup();
    names
}

/// Executes `query` over `rollup` and over the source tables, asserts both
/// agree, and returns the source result.
async fn assert_rollup_matches_source(rollup: &str, query: &str) -> Option<String> {
    let rollup_ctx = ctx(&[rollup]);
    let used = used_pre_aggregations(&rollup_ctx, query);
    assert!(
        !used.is_empty() && used.iter().all(|name| name == rollup),
        "expected the query to be served by {rollup}, got {used:?}"
    );
    let from_rollup = if rollup == "weekly_external" {
        rollup_ctx.try_execute_cubestore(query, SEED).await
    } else {
        rollup_ctx.try_execute_pg(query, SEED).await
    };
    let from_source = ctx(&[]).try_execute_pg(query, SEED).await;
    if let (Some(from_rollup), Some(from_source)) = (&from_rollup, &from_source) {
        assert_eq!(
            normalize(from_rollup),
            normalize(from_source),
            "rollup and source disagree\n--- rollup ---\n{from_rollup}\n--- source ---\n{from_source}"
        );
    }
    from_source
}

#[test]
fn test_week_window_at_week_granularity_matches_week_rollup() {
    for rollup in ["weekly", "weekly_external"] {
        let ctx = ctx(&[rollup]);
        for measure in ALL_MEASURES {
            let q = query(&[measure], "week", "2023-06-05", "2023-07-30T23:59:59.999");
            assert_eq!(used_pre_aggregations(&ctx, &q), vec![rollup.to_string()]);
        }
    }
}

/// A finer or week-incompatible query grain needs day buckets, which a week
/// rollup cannot provide.
#[test]
fn test_week_window_at_other_granularities_skips_week_rollup() {
    for rollup in ["weekly", "weekly_external"] {
        let ctx = ctx(&[rollup]);
        for (granularity, from, to) in [
            ("day", "2023-06-05", "2023-07-30T23:59:59.999"),
            ("month", "2023-06-01", "2023-07-31T23:59:59.999"),
            ("quarter", "2023-04-01", "2023-09-30T23:59:59.999"),
            ("week", "2023-06-07", "2023-07-26T23:59:59.999"),
        ] {
            let q = query(&["trailing_3w_count"], granularity, from, to);
            assert!(
                used_pre_aggregations(&ctx, &q).is_empty(),
                "{granularity} query must not read the week rollup {rollup}"
            );
        }
        let unaligned_total = total_query("2023-06-07", "2023-07-26T23:59:59.999");
        assert!(used_pre_aggregations(&ctx, &unaligned_total).is_empty());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_week_window_week_rollup_values() {
    let q = query(
        ALL_MEASURES,
        "week",
        "2023-06-05",
        "2023-07-30T23:59:59.999",
    );
    let total = total_query("2023-06-05", "2023-07-30T23:59:59.999");
    // CubeStore's rolling rewrite reads every window as `(lower, upper]`, which
    // shifts `offset: start` windows by a bucket at any grain.
    let cubestore_measures = ALL_MEASURES
        .iter()
        .copied()
        .filter(|m| *m != "leading_1w_start_count")
        .collect::<Vec<_>>();
    let cubestore_q = query(
        &cubestore_measures,
        "week",
        "2023-06-05",
        "2023-07-30T23:59:59.999",
    );
    assert_rollup_matches_source("weekly", &q).await;
    assert_rollup_matches_source("weekly_external", &cubestore_q).await;
    for rollup in ["weekly", "weekly_external"] {
        assert_rollup_matches_source(rollup, &total).await;
    }

    let oracle_query = query(
        &[
            "trailing_3w_count",
            "trailing_3w_amount",
            "cumulative_count",
        ],
        "week",
        "2023-06-05",
        "2023-07-30T23:59:59.999",
    );
    let Some(from_source) = assert_rollup_matches_source("weekly", &oracle_query).await else {
        return;
    };
    assert_eq!(data_rows(&from_source).len(), 8);
    assert_eq!(
        data_rows(&from_source),
        data_rows(&oracle().await),
        "source result disagrees with the SQL oracle"
    );
}

/// Week windows grouped by week must equal the same windows written in days,
/// which keep a day-grain base, for aligned and unaligned ranges alike.
#[tokio::test(flavor = "multi_thread")]
async fn test_week_windows_equal_their_day_twins() {
    let twins = [
        ("trailing_3w_count", "trailing_21d_count"),
        ("around_1w_count", "around_7d_count"),
        ("leading_1w_start_count", "leading_7d_start_count"),
    ];
    let measures = twins
        .iter()
        .flat_map(|(week, day)| [*week, *day])
        .collect::<Vec<_>>();
    for (from, to) in [
        ("2023-06-05", "2023-07-30T23:59:59.999"),
        ("2023-06-07", "2023-07-26T23:59:59.999"),
    ] {
        let q = query(&measures, "week", from, to);
        let Some(result) = ctx(&[]).try_execute_pg(&q, SEED).await else {
            return;
        };
        let rows = table_rows(&result);
        assert_eq!(rows.len(), 8, "{result}");
        for (i, (week, day)) in twins.iter().enumerate() {
            let column = |offset: usize| rows.iter().map(move |r| r[1 + 2 * i + offset].clone());
            let week_values = column(0).collect::<Vec<_>>();
            assert_eq!(
                week_values,
                column(1).collect::<Vec<_>>(),
                "{week} vs {day}\n{result}"
            );
            assert!(
                week_values.iter().any(|v| v != &week_values[0]),
                "{week} is constant over weeks, so it cannot catch a shifted window\n{result}"
            );
        }
    }
}

fn data_rows(table: &str) -> Vec<String> {
    normalize(table)
        .lines()
        .skip(1)
        .map(str::to_string)
        .collect()
}

#[cfg(feature = "integration-postgres")]
async fn oracle() -> String {
    use crate::test_fixtures::test_utils::{integration_context, pg_service};
    let client = pg_service::connect_and_seed(SEED).await;
    let sql = indoc! {"
        SELECT w.week AS created_at,
               COUNT(e.id) FILTER (WHERE e.created_at >= w.week - interval '2 week') AS trailing_3w_count,
               SUM(e.value) FILTER (WHERE e.created_at >= w.week - interval '2 week') AS trailing_3w_amount,
               COUNT(e.id) AS cumulative_count
        FROM generate_series(timestamp '2023-06-05', timestamp '2023-07-24', interval '1 week') AS w(week)
        LEFT JOIN week_rolling_events e ON e.created_at < w.week + interval '1 week'
        GROUP BY w.week
        ORDER BY w.week
    "};
    let messages = client.simple_query(sql).await.unwrap();
    integration_context::format_simple_query_results(&messages)
}

#[cfg(not(feature = "integration-postgres"))]
async fn oracle() -> String {
    unreachable!("the source result is only produced under integration-postgres")
}
