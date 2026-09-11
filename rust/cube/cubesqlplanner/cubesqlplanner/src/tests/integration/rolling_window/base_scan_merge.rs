//! A rolling window's base CTE aggregates one measure over the rows the window
//! can reach, and which rows those are is decided by the frame and the query's
//! filters, not by the measure. Windows reading the same rows therefore ride on
//! one scan; anything that changes the rows keeps its own.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use itertools::Itertools;

const SEED: &str = "integration_rolling_window_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

fn fact_scans(sql: &str) -> usize {
    sql.matches("rw_orders").count()
}

/// The CTE names a `LEFT JOIN` reads, in order. Only meaningful for a plan
/// whose joins are all the outer query's — a rolling-window stage joins its
/// own base scan the same way.
fn left_joined_cte_names(sql: &str) -> Vec<String> {
    sql.split("LEFT JOIN")
        .skip(1)
        .filter_map(|tail| tail.split_whitespace().next().map(str::to_string))
        .collect()
}

// Four windows over the same frame and the same filters: one scan, and each
// window reads its own column off it.
#[tokio::test(flavor = "multi_thread")]
async fn test_measures_over_one_frame_share_a_base_scan() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.rolling_count_7d
          - orders.rolling_min_7d
          - orders.rolling_max_7d
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert_eq!(
        fact_scans(&sql),
        1,
        "fact table is scanned more than once:\n{sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// An average is evaluated at row grain so its window can average over the
// whole frame, and raw rows are not the rows an aggregating scan reads. Two
// measures needing that grain still share with each other.
#[tokio::test(flavor = "multi_thread")]
async fn test_row_grain_measures_share_only_with_each_other() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.rolling_avg_7d
          - orders.rolling_unique_customers_7d
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert_eq!(fact_scans(&sql), 2, "{sql}");

    // The merged scan of the two row-grain measures emits both raw-value
    // columns from one un-aggregated read, a shape no other test covers.
    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Windows over a joined query share a scan too. The join condition is compared
// by identity, which holds because the join tree is built once and handed to
// every leaf of the query; this pins that, so the sharing cannot quietly stop
// happening for every joined model.
#[tokio::test(flavor = "multi_thread")]
async fn test_measures_over_a_joined_query_share_a_base_scan() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.rolling_count_7d
        dimensions:
          - customers.city
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert_eq!(fact_scans(&sql), 1, "{sql}");
    assert_eq!(sql.matches("rw_customers").count(), 1, "{sql}");
}

// A time shift moves the period a window covers without changing anything in
// the query the leaf holds, so two scans differing only by one read different
// rows. Merging them would answer the shifted value with the unshifted one.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_shifted_window_keeps_its_own_scan() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.rolling_sum_trailing_7d_prior
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert_eq!(fact_scans(&sql), 2, "{sql}");
}

// A different trailing interval reads different rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_different_frames_keep_their_own_scan() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.rolling_sum_14d
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert_eq!(fact_scans(&sql), 2, "{sql}");
}

// Windows over the same frame but different facts are different scans, however
// identical the frames.
#[tokio::test(flavor = "multi_thread")]
async fn test_different_facts_keep_their_own_scan() {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window_multi_fact.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - payments.rolling_sum_7d
          - messages.rolling_count_7d
        dimensions:
          - customers.name
        time_dimensions:
          - dimension: customers.registered_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-25"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert_eq!(sql.matches("mf_payments").count(), 1, "{sql}");
    assert_eq!(sql.matches("mf_messages").count(), 1, "{sql}");
}

// Without a granularity there is no series to walk and no stage on top: the
// base CTE is the requested measure's own result, registered under that
// measure. A CTE shared between measures cannot answer for it, so these keep
// one each and are joined once each.
#[tokio::test(flavor = "multi_thread")]
async fn test_windows_without_granularity_keep_their_own_cte() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.rolling_count_7d
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert_eq!(fact_scans(&sql), 2, "{sql}");

    // Without a stage on top there are no rolling joins, so every join here is
    // the outer query reading one CTE. A CTE shared between the two measures
    // would appear in that list twice, joined once under each measure's name.
    let joined = left_joined_cte_names(&sql);
    assert_eq!(
        joined.len(),
        joined.iter().unique().count(),
        "a CTE is joined more than once: {joined:?}\n{sql}"
    );
}

// A pre-aggregation only answers for a query whose every measure it carries, so
// a scan shared between two measures could not be served by a rollup holding
// one of them. Rollups are matched before the scans are merged, so a model
// storing one rollup per rolling measure keeps both.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_rollup_per_rolling_measure_survives() {
    let schema =
        MockSchema::from_yaml_file("common/integration_rolling_window_shared_scan_preagg.yaml");
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
    let names = used
        .iter()
        .map(|pa| format!("{}.{}", pa.cube_name(), pa.name()))
        .sorted()
        .collect_vec();

    assert_eq!(
        names,
        vec![
            "daily_activity.rolling_events",
            "daily_activity.rolling_minutes"
        ],
        "{sql}"
    );
}
