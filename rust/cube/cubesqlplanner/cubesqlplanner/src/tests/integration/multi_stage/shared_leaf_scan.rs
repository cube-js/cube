//! Multi-stage leaves reading the same rows ride on one scan; a grain, filter
//! or time shift of their own keeps them apart. Every query runs with and
//! without the sharing, and both must agree.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use itertools::Itertools;

const SEED: &str = "integration_multi_fact_tables.sql";
const YAML: &str = "common/integration_multi_stage_shared_leaf_scan.yaml";
const ROLLUP: &str = "orders_by_status_city_day";

fn source_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[])).unwrap()
}

fn schema_with_rollups(names: &[&str], external: bool) -> MockSchema {
    let path = format!(
        "{}/src/test_fixtures/schemas/yaml_files/{}",
        env!("CARGO_MANIFEST_DIR"),
        YAML
    );
    let yaml = std::fs::read_to_string(path).unwrap().replace(
        "            type: rollup\n",
        &format!("            type: rollup\n            external: {external}\n"),
    );
    MockSchema::from_yaml(&yaml)
        .unwrap()
        .only_pre_aggregations(names)
}

fn rollup_ctx(names: &[&str]) -> TestContext {
    TestContext::new(schema_with_rollups(names, false)).unwrap()
}

fn with_sharing(query: &str, sharing: bool) -> String {
    format!("{}multi_stage_leaf_merge: {}\n", query, sharing)
}

fn orders_scans(sql: &str) -> usize {
    sql.matches("AS \"orders\"").count()
}

/// CubeStore prints a decimal sum as `200`, Postgres as `200.00`, and the
/// header rule follows the column widths.
fn normalize(result: Option<String>) -> Option<String> {
    result.map(|table| {
        table
            .lines()
            .filter(|line| !line.starts_with('-'))
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

/// Scans of the orders table with and without the sharing, after checking
/// that both plans return the same rows.
async fn scans_shared_and_separate(query: &str) -> (usize, usize) {
    let ctx = source_ctx();
    let shared_sql = ctx.build_sql(&with_sharing(query, true)).unwrap();
    let separate_sql = ctx.build_sql(&with_sharing(query, false)).unwrap();
    let shared = ctx.try_execute_pg(&with_sharing(query, true), SEED).await;
    let separate = ctx.try_execute_pg(&with_sharing(query, false), SEED).await;
    assert_eq!(
        shared, separate,
        "Shared and separate scans return different rows\n{shared_sql}"
    );
    (orders_scans(&shared_sql), orders_scans(&separate_sql))
}

const RATIOS_BY_STATUS: &str = indoc! {"
    measures:
      - orders.avg_amount
      - orders.big_share
      - orders.max_to_avg
    dimensions:
      - orders.status
    order:
      - id: orders.status
"};

#[tokio::test(flavor = "multi_thread")]
async fn test_leaves_reading_the_same_rows_share_one_scan() {
    // total_amount, count, big_amount and max_amount, each read once however
    // many ratios use it; big_amount keeps its own row condition.
    assert_eq!(scans_shared_and_separate(RATIOS_BY_STATUS).await, (1, 4));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leaves_share_a_scan_across_a_join() {
    let query = indoc! {"
        measures:
          - orders.avg_amount
          - orders.big_share
        dimensions:
          - customers.city
        filters:
          - member: orders.status
            operator: equals
            values: ['completed']
        order:
          - id: customers.city
    "};
    assert_eq!(scans_shared_and_separate(query).await, (1, 3));
    let sql = source_ctx().build_sql(&with_sharing(query, true)).unwrap();
    assert_eq!(sql.matches("AS \"customers\"").count(), 1, "{sql}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_regular_measure_keeps_its_own_scan() {
    // The query's own leaf is not a multi-stage one and is left alone.
    let query = indoc! {"
        measures:
          - orders.total_amount
          - orders.avg_amount
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};
    assert_eq!(scans_shared_and_separate(query).await, (2, 3));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_differently_filtered_leaves_keep_their_scans() {
    // The same two measures over all orders and over the completed ones.
    let query = indoc! {"
        measures:
          - orders.avg_amount
          - orders.completed_avg_amount
        dimensions:
          - customers.city
        order:
          - id: customers.city
    "};
    assert_eq!(scans_shared_and_separate(query).await, (2, 4));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_shifted_leaves_keep_their_scans() {
    // The shift is not part of the leaf's filters, only of how it renders.
    let query = indoc! {r#"
        measures:
          - orders.avg_amount
          - orders.avg_amount_prev_day
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2025-03-02"
              - "2025-03-08"
        order:
          - id: orders.created_at
    "#};
    assert_eq!(scans_shared_and_separate(query).await, (2, 4));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leaves_at_another_grain_keep_their_scans() {
    let query = indoc! {"
        measures:
          - orders.avg_amount
          - orders.city_max_per_order
        dimensions:
          - orders.status
          - customers.city
        order:
          - id: orders.status
          - id: customers.city
    "};
    // One scan per grain: the ratio's own leaves at the city grain, and at
    // the query grain the leaves of avg_amount with the ones its grid is
    // built from.
    assert_eq!(scans_shared_and_separate(query).await, (2, 5));
}

/// Usages of the rollups with the sharing, after checking the result against
/// the unshared plan and the source tables.
async fn usages_of_shared_plan(ctx: &TestContext, query: &str) -> Vec<String> {
    let (sql, usages) = ctx
        .build_sql_with_used_pre_aggregations(&with_sharing(query, true))
        .unwrap();
    let shared = normalize(ctx.try_execute(&with_sharing(query, true), SEED).await);
    let separate = normalize(ctx.try_execute(&with_sharing(query, false), SEED).await);
    let source = normalize(source_ctx().try_execute(query, SEED).await);
    if shared.is_some() {
        assert_eq!(shared, separate, "Shared and separate differ\n{sql}");
        assert_eq!(shared, source, "Shared differs from source tables\n{sql}");
    }
    usages.iter().map(|usage| usage.name().clone()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leaves_over_one_rollup_share_its_scan() {
    let ctx = rollup_ctx(&[ROLLUP]);
    let (_, separate) = ctx
        .build_sql_with_used_pre_aggregations(&with_sharing(RATIOS_BY_STATUS, false))
        .unwrap();
    assert_eq!(separate.len(), 4);
    assert_eq!(
        usages_of_shared_plan(&ctx, RATIOS_BY_STATUS).await,
        vec![ROLLUP]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leaves_share_a_rollup_scan_in_cubestore() {
    let ctx =
        TestContext::new_with_external_cubestore(schema_with_rollups(&[ROLLUP], true)).unwrap();
    assert_eq!(
        usages_of_shared_plan(&ctx, RATIOS_BY_STATUS).await,
        vec![ROLLUP]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_time_shifted_leaves_keep_their_rollup_usages() {
    let query = indoc! {r#"
        measures:
          - orders.avg_amount
          - orders.avg_amount_prev_day
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2025-03-02"
              - "2025-03-08"
        order:
          - id: orders.created_at
    "#};
    let ctx = rollup_ctx(&[ROLLUP]);
    assert_eq!(
        usages_of_shared_plan(&ctx, query).await,
        vec![ROLLUP, ROLLUP]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leaves_served_by_different_rollups_keep_them() {
    // No rollup stores both measures, so a leaf carrying both would fall back
    // to the source table.
    let query = indoc! {"
        measures:
          - orders.avg_amount
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "};
    let ctx = rollup_ctx(&["amount_by_status", "count_by_status"]);
    let usages = usages_of_shared_plan(&ctx, query).await;
    assert_eq!(
        usages.iter().sorted().collect_vec(),
        vec!["amount_by_status", "count_by_status"]
    );
}
