//! Measures whose join trees are nested one in the other: `carts` is reached
//! from `sites`, `checkouts` from `carts`, so a measure on `checkouts` needs
//! every join a measure on `carts` needs and one more.
//!
//! Grouping measures by the exact join tree puts such measures in separate
//! groups, and each group re-scans the shared part of the join from scratch.
//! They are folded into one group - one scan - whenever moving a measure into
//! the wider tree cannot change what it computes: the extra joins are `LEFT`,
//! so they only ever replicate a row, and a distinct aggregation is immune to
//! that. A plain count or a sum is not, and keeps its own group.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_nested_join_trees.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_nested_join_trees_tables.sql";

/// How many times the query reads the cube at the root of the join. One read
/// per group is exactly the duplication that folding nested groups removes.
fn base_scan_count(sql: &str) -> usize {
    sql.matches(r#"AS "sites""#).count()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_distinct_measures_share_one_base_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The same measure on its own, as the value the merged query must reproduce.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_distinct_measure_alone() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A plain count would read one row per checkout instead of one per cart, so
/// its group stays on its own tree.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_count_keeps_its_own_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.count
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Same for a sum, which the replicated rows would inflate.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_sum_keeps_its_own_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.total_value
          - checkouts.total_amount
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// An ungrouped query returns the joined rows themselves, so the replication
/// the wider tree introduces would show up in the result.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_ungrouped_keeps_separate_scans() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
        ungrouped: true
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");
}
