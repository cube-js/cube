//! Rolling-window measures reached through a `case` (switch) entrypoint, each
//! shape run twice: served from an external rollup in CubeStore, and straight
//! from the source Postgres with pre-aggregations switched off. The two results
//! must agree — the rollup path is an optimization, not a different answer.
//!
//! Requires `--features integration-cubestore` and a `cubestored` binary;
//! without them both paths return `None` and only matching is asserted.
//!
//! Two shapes are `#[ignore]`d for CubeStore limitations unrelated to the
//! calc-group grain — see the note on each; run them with `--ignored` to
//! reproduce. The rolling-rewrite defect these tests were originally written
//! against was fixed in #11410.
//! Note that a debug-built `cubestored` overflows its stack on rolling-window
//! queries, so `CUBESTORED_BIN_PATH` must point at a release build.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const YAML: &str = "common/integration_cubestore_switch_rolling.yaml";
const SEED: &str = "integration_cubestore_switch_rolling_tables.sql";

fn rollup_ctx(pre_aggs: &[&str]) -> TestContext {
    TestContext::new_with_external_cubestore(
        MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs),
    )
    .unwrap()
}

/// The same schema with every pre-aggregation filtered out, so the query runs
/// against the source tables.
fn raw_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[])).unwrap()
}

/// Query for `measures` with the switch pinned to `window`, at month grain.
fn query(measures: &[&str], window: &str, dimension: &str) -> String {
    let measures = measures
        .iter()
        .map(|m| format!("  - sales.{m}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        indoc! {r#"
            measures:
            {}
            dimensions:
              - {}
            filters:
              - dimension: sales.window_kind
                operator: equals
                values:
                  - {}
            time_dimensions:
              - dimension: sales.created_at
                granularity: month
                dateRange:
                  - "2024-04-01"
                  - "2024-06-30"
            order:
              - id: {}
              - id: sales.created_at
        "#},
        measures, dimension, window, dimension
    )
}

/// Same, with the switch left unpinned so it is grouped over instead.
fn query_grouped_switch(measure: &str) -> String {
    format!(
        indoc! {r#"
            measures:
              - sales.{}
            dimensions:
              - sales.category
              - sales.window_kind
            time_dimensions:
              - dimension: sales.created_at
                granularity: month
                dateRange:
                  - "2024-05-01"
                  - "2024-06-30"
            order:
              - id: sales.window_kind
              - id: sales.category
              - id: sales.created_at
        "#},
        measure
    )
}

const DIRECT_MEASURE: &str = indoc! {r#"
    measures:
      - sales.r3_amount
    dimensions:
      - sales.category
    time_dimensions:
      - dimension: sales.created_at
        granularity: month
        dateRange:
          - "2024-04-01"
          - "2024-06-30"
    order:
      - id: sales.category
      - id: sales.created_at
"#};

fn assert_served_by(ctx: &TestContext, query: &str, expected_rollup: &str) {
    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    assert!(
        !pre_aggrs.is_empty() && pre_aggrs.iter().all(|u| u.name() == expected_rollup),
        "expected every usage to be {expected_rollup}, got {:?}",
        pre_aggrs
            .iter()
            .map(|u| u.name().clone())
            .collect::<Vec<_>>()
    );
}

/// Engine-independent form of a result table. CubeStore renders timestamps as
/// `...T00:00:00.000Z` where Postgres uses `... 00:00:00`, and ratios are
/// computed in f64 against Postgres' NUMERIC, so the two differ in the last
/// digit (`2.4285714285714284` vs `...86`); numbers are therefore compared
/// rounded. Everything else must match cell for cell.
fn normalize(table: &str) -> String {
    fn normalize_cell(cell: &str) -> String {
        let cell = cell
            .trim()
            .replace('T', " ")
            .replace(".000Z", "")
            .replace('Z', "");
        match cell.parse::<f64>() {
            Ok(value) => format!("{value:.10}"),
            Err(_) => cell,
        }
    }

    table
        .lines()
        .filter(|line| {
            // the `---+---` separator, not a data row whose first cell is negative
            !line
                .trim()
                .chars()
                .all(|c| matches!(c, '-' | '+' | ' ' | '|'))
        })
        .map(|line| {
            line.split('|')
                .map(normalize_cell)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Runs `query` through the rollup and through the raw source, asserts the two
/// agree, and snapshots the rollup-backed result.
async fn run_both(pre_aggs: &[&str], query: &str, expected_rollup: &str, snapshot: &str) {
    let rollup = rollup_ctx(pre_aggs);
    assert_served_by(&rollup, query, expected_rollup);

    let raw = raw_ctx();
    let (_sql, none_used) = raw.build_sql_with_used_pre_aggregations(query).unwrap();
    assert!(
        none_used.is_empty(),
        "the raw context must not use pre-aggregations, got {:?}",
        none_used
            .iter()
            .map(|u| u.name().clone())
            .collect::<Vec<_>>()
    );

    let from_rollup = rollup.try_execute_cubestore(query, SEED).await;
    let from_source = raw.try_execute_pg(query, SEED).await;

    if let (Some(from_rollup), Some(from_source)) = (&from_rollup, &from_source) {
        assert_eq!(
            normalize(from_rollup),
            normalize(from_source),
            "rollup and raw-source results disagree\n--- rollup ---\n{from_rollup}\n--- source ---\n{from_source}"
        );
    }
    // Snapshot the engine-independent form of whichever engine ran, so values are
    // pinned in a plain `integration-postgres` run too, not only when CubeStore
    // is available.
    if let Some(result) = from_rollup.as_deref().or(from_source.as_deref()) {
        insta::assert_snapshot!(snapshot, normalize(result));
    }
}

/// Matching and planning for every shape below, without executing — the part
/// that must hold regardless of whether CubeStore can run the plan.
#[test]
fn test_switch_shapes_are_served_by_rollup() {
    let by_category = rollup_ctx(&["rolling_by_category"]);
    assert_served_by(&by_category, DIRECT_MEASURE, "rolling_by_category");
    for (measure, window) in [
        ("rolling_amount", "R3"),
        ("rolling_amount", "R12"),
        ("rolling_amount", "YTD"),
        ("prev_rolling_amount", "R3"),
        ("rolling_amount_change", "R3"),
        ("rolling_amount_growth", "R3"),
        ("rolling_amount_change", "YTD"),
        ("rolling_amount_growth", "YTD"),
    ] {
        assert_served_by(
            &by_category,
            &query(&[measure], window, "sales.category"),
            "rolling_by_category",
        );
    }
    assert_served_by(
        &by_category,
        &query(
            &[
                "rolling_amount",
                "prev_rolling_amount",
                "rolling_amount_change",
                "rolling_amount_growth",
            ],
            "R3",
            "sales.category",
        ),
        "rolling_by_category",
    );
    assert_served_by(
        &by_category,
        &query_grouped_switch("rolling_amount"),
        "rolling_by_category",
    );

    assert_served_by(&by_category, DATE_RANGE_ONLY, "rolling_by_category");

    assert_served_by(
        &rollup_ctx(&["rolling_by_category_stored_switch"]),
        &query(&["rolling_amount"], "R3", "sales.category"),
        "rolling_by_category_stored_switch",
    );
    assert_served_by(
        &rollup_ctx(&["rolling_by_category_agg_idx"]),
        &query(&["rolling_amount"], "R3", "sales.category"),
        "rolling_by_category_agg_idx",
    );
    assert_served_by(
        &rollup_ctx(&["rolling_by_account"]),
        &query(&["rolling_amount"], "R3", "accounts.name"),
        "rolling_by_account",
    );
}

/// Control: the rolling measure queried directly, no switch involved.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_measure() {
    run_both(
        &["rolling_by_category"],
        DIRECT_MEASURE,
        "rolling_by_category",
        "switch_rolling_direct_measure",
    )
    .await;
}

/// The plain case entrypoint over a `trailing` window: nothing above the rolling
/// windows consumes the calc-group dimension, so the outer projection prunes it.
///
/// This shape used to fail in CubeStore. `RollingOptimizerRule` replaced that
/// projection with a `RollingWindowAggregate` node emitting
/// `dimension + partition_by + rolling aggregates`, so the pruned GROUP BY column
/// reappeared, the replacement was wider than what it replaced, and ancestors
/// resolving columns by position hit `Optimizer rule 'optimize_projections'
/// failed / Schema error: No field named fk_aggregate.sales__r3_amount`. Fixed in
/// #11410; this test guards that rewrite together with the CTE-grain fix in this
/// PR.
#[tokio::test(flavor = "multi_thread")]
async fn test_case_entrypoint() {
    run_both(
        &["rolling_by_category"],
        &query(&["rolling_amount"], "R3", "sales.category"),
        "rolling_by_category",
        "switch_rolling_case_entrypoint",
    )
    .await;
}

/// Same, dispatching to the other window, so the switch value actually selects
/// a different rolling measure.
#[tokio::test(flavor = "multi_thread")]
async fn test_case_entrypoint_other_window() {
    run_both(
        &["rolling_by_category"],
        &query(&["rolling_amount"], "R12", "sales.category"),
        "rolling_by_category",
        "switch_rolling_case_entrypoint_r12",
    )
    .await;
}

/// The grain dimension is owned by a joined cube.
#[tokio::test(flavor = "multi_thread")]
async fn test_case_entrypoint_joined_dimension() {
    run_both(
        &["rolling_by_account"],
        &query(&["rolling_amount"], "R3", "accounts.name"),
        "rolling_by_account",
        "switch_rolling_joined_dimension",
    )
    .await;
}

/// A rollup that stores the calc group as an ordinary dimension: the value is
/// then read from the rollup column instead of being rendered as a literal.
#[tokio::test(flavor = "multi_thread")]
async fn test_case_entrypoint_rollup_stores_switch() {
    run_both(
        &["rolling_by_category_stored_switch"],
        &query(&["rolling_amount"], "R3", "sales.category"),
        "rolling_by_category_stored_switch",
        "switch_rolling_stored_switch",
    )
    .await;
}

/// Same grain, but the rollup carries an aggregating index. Creating the table
/// fails before any query runs: `Create table failed: Internal: task panicked
/// ... InvalidArgumentError("column types must match schema types")`.
#[ignore = "CubeStore fails to create the aggregating-index table for this rollup"]
#[tokio::test(flavor = "multi_thread")]
async fn test_case_entrypoint_aggregating_index() {
    run_both(
        &["rolling_by_category_agg_idx"],
        &query(&["rolling_amount"], "R3", "sales.category"),
        "rolling_by_category_agg_idx",
        "switch_rolling_aggregating_index",
    )
    .await;
}

/// The switch is not pinned by a filter but grouped over, so the calc group is
/// a real cross-joined values column rather than a literal.
#[tokio::test(flavor = "multi_thread")]
async fn test_case_entrypoint_grouped_switch() {
    run_both(
        &["rolling_by_category"],
        &query_grouped_switch("rolling_amount"),
        "rolling_by_category",
        "switch_rolling_grouped_switch",
    )
    .await;
}

/// An entrypoint dispatching straight onto a `time_shift` measure.
#[tokio::test(flavor = "multi_thread")]
async fn test_prev_case_entrypoint() {
    run_both(
        &["rolling_by_category"],
        &query(&["prev_rolling_amount"], "R3", "sales.category"),
        "rolling_by_category",
        "switch_rolling_prev_entrypoint",
    )
    .await;
}

/// The derived entrypoint: the arithmetic layer above the rolling windows
/// partitions by the calc-group dimension, so the outer projection keeps it and
/// nothing is pruned.
#[tokio::test(flavor = "multi_thread")]
async fn test_derived_case_entrypoint() {
    run_both(
        &["rolling_by_category"],
        &query(&["rolling_amount_change"], "R3", "sales.category"),
        "rolling_by_category",
        "switch_rolling_derived_entrypoint",
    )
    .await;
}

/// A ratio over two multi-stage layers, the growth-percentage shape.
#[tokio::test(flavor = "multi_thread")]
async fn test_growth_case_entrypoint() {
    run_both(
        &["rolling_by_category"],
        &query(&["rolling_amount_growth"], "R3", "sales.category"),
        "rolling_by_category",
        "switch_rolling_growth_entrypoint",
    )
    .await;
}

/// Four switch entrypoints at once — the dashboard shape, and the deepest
/// FullKeyAggregate plan this model produces. CubeStore cannot decode a
/// serialized plan this deep:
///
/// ```text
/// Error during planning: Error decoding expr as protobuf: failed to decode
/// Protobuf message: ... recursion limit reached
/// ```
///
/// Unrelated to the calc-group grain — the plan is valid and the raw half of
/// this test returns the expected rows.
#[ignore = "CubeStore cannot decode a serialized plan this deep (protobuf recursion limit)"]
#[tokio::test(flavor = "multi_thread")]
async fn test_four_entrypoints_in_one_query() {
    run_both(
        &["rolling_by_category"],
        &query(
            &[
                "rolling_amount",
                "prev_rolling_amount",
                "rolling_amount_change",
                "rolling_amount_growth",
            ],
            "R3",
            "sales.category",
        ),
        "rolling_by_category",
        "switch_rolling_four_measures",
    )
    .await;
}

/// A derived entrypoint dispatching to its `else` branch: an arithmetic layer
/// over a to-date window, which is planned without the time-series LEFT JOIN the
/// trailing windows use, while still partitioning by the calc group.
#[tokio::test(flavor = "multi_thread")]
async fn test_derived_case_entrypoint_else_branch() {
    run_both(
        &["rolling_by_category"],
        &query(&["rolling_amount_change"], "YTD", "sales.category"),
        "rolling_by_category",
        "switch_rolling_derived_else_branch",
    )
    .await;
}

/// Same for the ratio layer.
#[tokio::test(flavor = "multi_thread")]
async fn test_growth_case_entrypoint_else_branch() {
    run_both(
        &["rolling_by_category"],
        &query(&["rolling_amount_growth"], "YTD", "sales.category"),
        "rolling_by_category",
        "switch_rolling_growth_else_branch",
    )
    .await;
}

/// The `else` branch resolves to a to-date window, which is planned without the
/// time-series LEFT JOIN the trailing windows use.
#[tokio::test(flavor = "multi_thread")]
async fn test_case_entrypoint_else_branch() {
    run_both(
        &["rolling_by_category"],
        &query(&["rolling_amount"], "YTD", "sales.category"),
        "rolling_by_category",
        "switch_rolling_else_branch",
    )
    .await;
}

const DATE_RANGE_ONLY: &str = indoc! {r#"
    measures:
      - sales.rolling_amount
    dimensions:
      - sales.category
    filters:
      - dimension: sales.window_kind
        operator: equals
        values:
          - R3
    time_dimensions:
      - dimension: sales.created_at
        dateRange:
          - "2024-04-01"
          - "2024-06-30"
    order:
      - id: sales.category
"#};

/// A date range with no granularity still matches the rollup thanks to
/// `allow_non_strict_date_range_match`.
#[tokio::test(flavor = "multi_thread")]
async fn test_date_range_without_granularity() {
    run_both(
        &["rolling_by_category"],
        DATE_RANGE_ONLY,
        "rolling_by_category",
        "switch_rolling_date_range_only",
    )
    .await;
}
