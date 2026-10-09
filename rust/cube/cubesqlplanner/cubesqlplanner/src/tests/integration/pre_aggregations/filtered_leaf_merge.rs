//! Multi-stage measures that read one base measure over different date
//! windows of the same rollup, folded into one scan of it. Every query runs
//! folded, unfolded (one scan per window) and over the source tables with
//! pre-aggregations switched off. Folding is an optimization, so all three
//! must agree.
//!
//! With the rollup in CubeStore the unfolded query is left out of the
//! comparison: it joins the windows on their keys, and CubeStore drops the
//! values of a NULL key in that join.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use chrono_tz::Tz;
use indoc::indoc;
use itertools::Itertools;

const SEED: &str = "integration_multi_fact_tables.sql";
const YAML: &str = "common/integration_multi_stage_windows_pre_agg.yaml";
const ROLLUP: &str = "orders_by_status_size_day";

fn schema_with_rollup(external: bool) -> MockSchema {
    let path = format!(
        "{}/src/test_fixtures/schemas/yaml_files/{}",
        env!("CARGO_MANIFEST_DIR"),
        YAML
    );
    let yaml = std::fs::read_to_string(path).unwrap().replace(
        "            type: rollup\n",
        &format!("            type: rollup\n            external: {external}\n"),
    );
    MockSchema::from_yaml(&yaml).unwrap()
}

/// The rollup kept in the source Postgres.
fn pg_ctx() -> TestContext {
    TestContext::new(schema_with_rollup(false)).unwrap()
}

fn cubestore_ctx() -> TestContext {
    TestContext::new_with_external_cubestore(schema_with_rollup(true)).unwrap()
}

fn raw_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[])).unwrap()
}

fn with_merge(query: &str, merge: bool) -> String {
    format!("{}multi_stage_leaf_merge: {}\n", query, merge)
}

/// CubeStore prints a decimal sum as `200`, Postgres as `200.00`.
fn normalize(result: Option<String>) -> Option<String> {
    result.map(|table| {
        table
            .lines()
            .map(|line| {
                line.split('|')
                    .map(|cell| match cell.trim().parse::<f64>() {
                        Ok(value) => format!("{value:.6}"),
                        Err(_) => cell.trim().to_string(),
                    })
                    .join("|")
            })
            .join("\n")
    })
}

/// Plans `query` folded, runs it, and checks it against the source tables
/// and, when `with_unfolded`, against the unfolded query. Returns the folded
/// SQL.
async fn assert_folding_agrees(ctx: &TestContext, query: &str, with_unfolded: bool) -> String {
    let (merged_sql, usages) = ctx
        .build_sql_with_used_pre_aggregations(&with_merge(query, true))
        .unwrap();
    assert!(
        !usages.is_empty() && usages.iter().all(|u| u.name() == ROLLUP),
        "Every window must read the rollup; got {:?}",
        usages.iter().map(|u| u.name()).collect_vec()
    );
    let merged = normalize(ctx.try_execute(&with_merge(query, true), SEED).await);
    if with_unfolded {
        let separate = normalize(ctx.try_execute(&with_merge(query, false), SEED).await);
        assert_eq!(
            merged, separate,
            "Folded and unfolded results differ\n{merged_sql}"
        );
    }
    let source = normalize(raw_ctx().try_execute(query, SEED).await);
    if merged.is_some() {
        assert_eq!(
            merged, source,
            "Folded result differs from source tables\n{merged_sql}"
        );
    }
    merged_sql
}

const WINDOWS_BY_STATUS_SIZE: &str = indoc! {"
    measures:
      - orders.amount_all_week
      - orders.amount_early_week
      - orders.amount_mid_week
      - orders.max_amount_late_week
      - orders.count_early_week
      - orders.count_late_week
    dimensions:
      - orders.status
      - orders.size
    order:
      - id: orders.status
      - id: orders.size
"};

#[tokio::test(flavor = "multi_thread")]
async fn test_windows_fold_into_one_rollup_scan() {
    // size is NULL for small orders, and the early and late windows leave
    // some keys without rows: those read NULL, not 0, including a count,
    // which a rollup stores as a column that is summed.
    let ctx = pg_ctx();
    let sql = assert_folding_agrees(&ctx, WINDOWS_BY_STATUS_SIZE, true).await;
    assert!(sql.contains("CASE WHEN"), "{sql}");
    let separate = ctx
        .build_sql(&with_merge(WINDOWS_BY_STATUS_SIZE, false))
        .unwrap();
    assert!(!separate.contains("CASE WHEN"), "{separate}");

    if let Some(result) = ctx
        .try_execute(&with_merge(WINDOWS_BY_STATUS_SIZE, true), SEED)
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_windows_fold_over_cubestore_rollup() {
    let sql = assert_folding_agrees(&cubestore_ctx(), WINDOWS_BY_STATUS_SIZE, false).await;
    assert!(sql.contains("CASE WHEN"), "{sql}");
}

/// Each query with whether the folded scan must be inlined into it, so that
/// ORDER BY / LIMIT sit directly on the rollup read.
const ORDER_LIMIT_AND_FILTER_QUERIES: [(&str, bool); 4] = [
    (
        indoc! {"
            measures:
              - orders.amount_all_week
              - orders.amount_mid_week
            dimensions:
              - orders.status
              - orders.size
            order:
              - id: orders.amount_mid_week
                desc: true
              - id: orders.status
            limit: 2
        "},
        true,
    ),
    (
        indoc! {"
            measures:
              - orders.amount_all_week
              - orders.amount_mid_week
            dimensions:
              - orders.status
              - orders.size
            order:
              - id: orders.status
              - id: orders.size
            limit: 1
            offset: 1
        "},
        true,
    ),
    (
        indoc! {"
            measures:
              - orders.amount_all_week
              - orders.amount_early_week
            dimensions:
              - orders.status
              - orders.size
            filters:
              - member: orders.amount_early_week
                operator: gt
                values: ['60']
            order:
              - id: orders.status
              - id: orders.size
        "},
        false,
    ),
    (
        indoc! {"
            measures:
              - orders.amount_all_week
              - orders.amount_early_week
            dimensions:
              - orders.status
              - orders.size
            filters:
              - member: orders.status
                operator: equals
                values: ['completed']
            order:
              - id: orders.size
        "},
        true,
    ),
];

#[tokio::test(flavor = "multi_thread")]
async fn test_folded_scan_keeps_order_limit_and_measure_filter() {
    for (query, inlined) in ORDER_LIMIT_AND_FILTER_QUERIES {
        for sql in [
            assert_folding_agrees(&pg_ctx(), query, true).await,
            assert_folding_agrees(&cubestore_ctx(), query, false).await,
        ] {
            assert!(sql.contains("CASE WHEN"), "{sql}");
            // Inlined, the rollup read is the only SELECT and carries the
            // query's ORDER BY / LIMIT; a measure filter needs the folded
            // scan in a CTE so it can filter its aggregates.
            assert_eq!(!sql.contains("WITH"), inlined, "{sql}");
            assert_eq!(sql.matches("SELECT").count() == 1, inlined, "{sql}");
        }
    }
    let (query, _) = ORDER_LIMIT_AND_FILTER_QUERIES[0];
    insta::assert_snapshot!(
        "folded_scan_inlined_with_order_limit",
        pg_ctx().build_sql(&with_merge(query, true)).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_windows_by_day_in_non_utc_timezone() {
    // The query reads the stored days in another zone: each window's
    // condition must render against the stored column the way the scan's
    // filter does. The fixture builds the rollup in UTC, so only the folded
    // and unfolded queries are compared.
    for tz in [Tz::America__Los_Angeles, Tz::Asia__Kamchatka] {
        let ctx = TestContext::new_with_timezone(schema_with_rollup(false), tz).unwrap();
        let query = indoc! {"
            measures:
              - orders.amount_all_week
              - orders.amount_early_week
              - orders.amount_mid_week
            time_dimensions:
              - dimension: orders.created_at
                granularity: day
            order:
              - id: orders.created_at
        "};
        let merged_sql = ctx.build_sql(&with_merge(query, true)).unwrap();
        assert!(merged_sql.contains("CASE WHEN"), "{merged_sql}");
        assert_eq!(
            ctx.try_execute(&with_merge(query, true), SEED).await,
            ctx.try_execute(&with_merge(query, false), SEED).await,
            "{tz}: {merged_sql}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_windows_with_different_dimension_filters_do_not_fold_together() {
    let query = indoc! {"
        measures:
          - orders.amount_early_week
          - orders.completed_amount_early_week
        dimensions:
          - orders.size
        order:
          - id: orders.size
    "};
    let sql = assert_folding_agrees(&pg_ctx(), query, true).await;
    assert!(!sql.contains("CASE WHEN"), "{sql}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_approximate_distinct_windows_do_not_fold() {
    // An HLL column holds a sketch per row; it cannot be gated row by row.
    let query = indoc! {"
        measures:
          - orders.distinct_customers_early_week
          - orders.distinct_customers_all_week
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};
    let sql = pg_ctx().build_sql(&with_merge(query, true)).unwrap();
    assert!(!sql.contains("CASE WHEN"), "{sql}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_windows_read_by_another_stage() {
    // Two of the windows are read by a ratio over them as well as by the
    // query itself, so they keep scans of their own; the two windows only the
    // query reads still fold.
    let query = indoc! {"
        measures:
          - orders.early_week_share
          - orders.amount_all_week
          - orders.amount_mid_week
          - orders.count_all_week
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};
    let sql = assert_folding_agrees(&pg_ctx(), query, true).await;
    assert_eq!(sql.matches("CASE WHEN").count(), 2, "{sql}");
    // One scan for each shared window, one for the folded pair.
    assert_eq!(
        sql.matches("FROM  orders__orders_by_status_size_day")
            .count(),
        3,
        "{sql}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_windows_without_a_covering_window_do_not_fold() {
    // Neither window's usage covers the other's partitions, so no scan can
    // serve both.
    let query = indoc! {"
        measures:
          - orders.amount_early_week
          - orders.max_amount_late_week
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};
    let sql = assert_folding_agrees(&pg_ctx(), query, true).await;
    assert!(!sql.contains("CASE WHEN"), "{sql}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_windows() {
    let query = indoc! {"
        measures:
          - orders.amount_all_week
          - orders.amount_early_week
        dimensions:
          - orders.status
          - orders.size
        order:
          - id: orders.status
          - id: orders.size
        ungrouped: true
    "};
    let ctx = pg_ctx();
    let merged = ctx.build_sql(&with_merge(query, true)).unwrap();
    let separate = ctx.build_sql(&with_merge(query, false)).unwrap();
    // An ungrouped leaf has no aggregate to gate, so nothing folds.
    assert!(!merged.contains("CASE WHEN"), "{merged}");
    assert_eq!(merged, separate);
    assert_eq!(
        ctx.try_execute(&with_merge(query, true), SEED).await,
        ctx.try_execute(&with_merge(query, false), SEED).await
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_masked_windows_do_not_fold() {
    // A constant mask replaces the whole aggregate, so a folded measure would
    // read the mask, not NULL, for a key with no rows in its window. Through
    // a view the mask may sit on the view member or on the cube member it
    // reads.
    let cases = [
        ("orders", "orders.total_amount"),
        ("orders", "orders.amount_mid_week"),
        ("orders_view", "orders.total_amount"),
        ("orders_view", "orders.amount_mid_week"),
        ("orders_view", "orders_view.amount_mid_week"),
    ];
    for (source, masked) in cases {
        let query = format!(
            indoc! {"
                measures:
                  - {source}.amount_all_week
                  - {source}.amount_mid_week
                dimensions:
                  - {source}.status
                  - {source}.size
                order:
                  - id: {source}.status
                  - id: {source}.size
                maskedMembers:
                  - member: {masked}
            "},
            source = source,
            masked = masked
        );
        let ctx = pg_ctx();
        let sql = ctx.build_sql(&with_merge(&query, true)).unwrap();
        assert!(!sql.contains("CASE WHEN"), "{masked} via {source}: {sql}");
        assert_eq!(
            ctx.try_execute(&with_merge(&query, true), SEED).await,
            ctx.try_execute(&with_merge(&query, false), SEED).await,
            "{masked} via {source}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_windows_fold_under_query_filters_and_segment() {
    let queries = [
        // The query's own date range narrows every window.
        indoc! {"
            measures:
              - orders.amount_all_week
              - orders.amount_early_week
              - orders.amount_mid_week
            dimensions:
              - orders.status
            time_dimensions:
              - dimension: orders.created_at
                dateRange:
                  - '2025-03-02'
                  - '2025-03-05'
            order:
              - id: orders.status
        "},
        indoc! {"
            measures:
              - orders.amount_all_week
              - orders.amount_mid_week
            dimensions:
              - orders.status
            segments:
              - orders.big_orders
            order:
              - id: orders.status
        "},
    ];
    for query in queries {
        let sql = assert_folding_agrees(&pg_ctx(), query, true).await;
        assert!(sql.contains("CASE WHEN"), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_windows() {
    let query = indoc! {"
        measures:
          - orders_view.amount_all_week
          - orders_view.amount_early_week
          - orders_view.max_amount_late_week
        dimensions:
          - orders_view.status
          - orders_view.size
        order:
          - id: orders_view.status
          - id: orders_view.size
    "};
    let sql = assert_folding_agrees(&pg_ctx(), query, true).await;
    assert!(sql.contains("CASE WHEN"), "{sql}");
}

#[test]
fn test_folded_scan_reads_only_the_widest_usage() {
    // Each window matched the rollup with its own date range. The folded scan
    // reads the usage whose range covers all of them, and only that usage is
    // reported: the others would have their partitions loaded for nothing.
    let ctx = pg_ctx();
    let (sql, usages) = ctx
        .build_sql_with_used_pre_aggregations(&with_merge(WINDOWS_BY_STATUS_SIZE, true))
        .unwrap();
    let (_, unfolded) = ctx
        .build_sql_with_used_pre_aggregations(&with_merge(WINDOWS_BY_STATUS_SIZE, false))
        .unwrap();
    assert_eq!(usages.len(), 1, "{sql}");
    assert!(
        sql.contains(&format!("__usage_{} ", usages[0].index)),
        "{sql}"
    );
    let stored = usages[0]
        .pre_aggregation
        .measures()
        .iter()
        .map(|m| m.full_name())
        .collect_vec();
    for base in ["orders.total_amount", "orders.max_amount", "orders.count"] {
        assert!(stored.iter().any(|m| m == base), "{stored:?}");
    }
    let (from, to) = usages[0].date_range.clone().unwrap();
    for usage in unfolded.iter() {
        let (other_from, other_to) = usage.date_range.clone().unwrap();
        assert!(from <= other_from && to >= other_to, "{sql}");
    }
}
