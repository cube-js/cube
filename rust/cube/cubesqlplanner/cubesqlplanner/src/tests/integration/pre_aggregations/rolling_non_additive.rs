//! A rolling window over a non-additive measure aggregates raw values across
//! the window, which no rollup stores: the distinct users of a week are not
//! derivable from daily distinct counts.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use crate::tests::integration::cubestore::switch_rolling::normalize;
use indoc::formatdoc;

const YAML: &str = "common/rolling_count_distinct_pre_agg.yaml";
const SEED: &str = "rolling_count_distinct_pre_agg_tables.sql";

fn ctx(pre_aggs: &[&str]) -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
}

fn query(measures: &[&str], ungrouped: bool) -> String {
    let measures = measures
        .iter()
        .map(|m| format!("  - rcd_sessions.{m}"))
        .collect::<Vec<_>>()
        .join("\n");
    formatdoc! {"
        measures:
        {measures}
        time_dimensions:
          - dimension: rcd_sessions.created_at
            granularity: day
            dateRange:
              - '2024-01-01'
              - '2024-01-10'
        ungrouped: {ungrouped}
    "}
}

// Ungrouped results carry no ORDER BY within a day.
fn sorted(result: &str) -> Vec<String> {
    let mut lines = normalize(result)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    lines.sort();
    lines
}

fn used_pre_aggregations(rollup: &str, query: &str) -> Vec<String> {
    let (_, usages) = ctx(&[rollup])
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    usages.iter().map(|u| u.name().clone()).collect()
}

/// Asserts whether `rollup` serves the query and, when it does, that Postgres
/// and, if `on_cubestore`, CubeStore return the source result from it.
/// Returns the source result, `None` without a database.
async fn check(rollup: &str, query: &str, served: bool, on_cubestore: bool) -> Option<String> {
    let used = used_pre_aggregations(rollup, query);
    assert_eq!(!used.is_empty(), served, "{rollup} used: {used:?}");
    let source = ctx(&[]).try_execute_pg(query, SEED).await?;
    if served {
        let from_pg = ctx(&[rollup]).try_execute_pg(query, SEED).await.unwrap();
        assert_eq!(sorted(&from_pg), sorted(&source), "{rollup} on Postgres");
        let from_cubestore = match on_cubestore {
            true => ctx(&[rollup]).try_execute(query, SEED).await,
            false => None,
        };
        if let Some(from_cubestore) = from_cubestore {
            assert_eq!(
                sorted(&from_cubestore),
                sorted(&source),
                "{rollup} on CubeStore"
            );
        }
    }
    Some(source)
}

/// A rollup serves the window only when its stored values add up to it.
async fn check_grouped(name: &str, measures: &[&str], additive: bool) {
    let query = query(measures, false);
    check("by_id", &query, additive, true).await;
    if let Some(source) = check("daily", &query, additive, true).await {
        insta::assert_snapshot!(name, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_count_distinct() {
    check_grouped("rolling_count_distinct", &["weekly_users"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_avg() {
    check_grouped("rolling_avg", &["weekly_avg_amount"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_sum() {
    check_grouped("rolling_sum", &["weekly_amount"], true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_sum_with_count_distinct() {
    // One query reads either the rollup or the source, never both.
    check_grouped(
        "rolling_sum_with_count_distinct",
        &["weekly_amount", "weekly_users"],
        false,
    )
    .await;
}

// An ungrouped query reads a window's rows one by one, so a rollup collapsing
// them can't serve even an additive measure. CubeStore can't plan this
// ungrouped window join, so it runs on Postgres only.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rolling_sum_needs_source_rows() {
    let query = query(&["weekly_amount"], true);
    check("by_id", &query, true, false).await;
    check("daily", &query, false, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rolling_count_distinct_skips_rollups() {
    let query = query(&["weekly_users"], true);
    check("by_id", &query, false, false).await;
    check("daily", &query, false, false).await;
}

// The window merges the stored sketches, so they never reach the client.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rolling_approx_reads_stored_sketches() {
    let query = query(&["weekly_users_approx"], true);
    check("by_id_approx", &query, true, false).await;
}
