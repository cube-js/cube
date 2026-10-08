//! Queries over a rollup partitioned by month, so CubeStore reads it as the
//! UNION ALL of one table per month, as the query orchestrator renders it. Each
//! shape runs through the rollup in CubeStore and straight from Postgres, and
//! the two results must agree.
//!
//! Grouping by the month (or anything finer) never mixes two partitions, so
//! CubeStore may aggregate each partition table on its own
//! (`CUBESTORE_DISJOINT_UNION_AGGREGATE`); these shapes pin the answer either
//! way.
//!
//! Requires `--features integration-cubestore` and a release `cubestored`.

use super::normalize;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const YAML: &str = "common/integration_cubestore_partitioned.yaml";
const SEED: &str = "integration_cubestore_partitioned_tables.sql";

async fn run_both(query: &str, snapshot: &str) {
    let rollup =
        TestContext::new_with_external_cubestore(MockSchema::from_yaml_file(YAML)).unwrap();
    let (_sql, pre_aggrs) = rollup.build_sql_with_used_pre_aggregations(query).unwrap();
    assert!(
        !pre_aggrs.is_empty() && pre_aggrs.iter().all(|u| u.name() == "monthly"),
        "expected the monthly rollup, got {:?}",
        pre_aggrs
            .iter()
            .map(|u| u.name().clone())
            .collect::<Vec<_>>()
    );
    let raw =
        TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[])).unwrap();

    let from_rollup = rollup.try_execute_cubestore(query, SEED).await;
    let from_source = raw.try_execute_pg(query, SEED).await;

    if let (Some(from_rollup), Some(from_source)) = (&from_rollup, &from_source) {
        assert_eq!(
            normalize(from_rollup),
            normalize(from_source),
            "rollup and raw-source results disagree\n--- rollup ---\n{from_rollup}\n--- source ---\n{from_source}"
        );
    }
    if let Some(result) = from_rollup.as_deref().or(from_source.as_deref()) {
        insta::assert_snapshot!(snapshot, normalize(result));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partitioned_rollup_top_cells() {
    run_both(
        indoc! {r#"
            measures:
              - part_sales.amount
            dimensions:
              - part_sales.hcp
              - part_sales.brand
            time_dimensions:
              - dimension: part_sales.sold_at
                granularity: month
            order:
              - id: part_sales.amount
                desc: true
            limit: "5"
        "#},
        "partitioned_rollup_top_cells",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partitioned_rollup_month_totals() {
    run_both(
        indoc! {r#"
            measures:
              - part_sales.amount
            time_dimensions:
              - dimension: part_sales.sold_at
                granularity: month
            order:
              - id: part_sales.sold_at
        "#},
        "partitioned_rollup_month_totals",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partitioned_rollup_filtered_range_and_measure() {
    run_both(
        indoc! {r#"
            measures:
              - part_sales.amount
            dimensions:
              - part_sales.hcp
            filters:
              - member: part_sales.brand
                operator: equals
                values:
                  - b2
              - member: part_sales.amount
                operator: gt
                values:
                  - "9000"
            time_dimensions:
              - dimension: part_sales.sold_at
                granularity: month
                dateRange:
                  - "2024-02-01"
                  - "2024-03-31"
            order:
              - id: part_sales.hcp
              - id: part_sales.sold_at
        "#},
        "partitioned_rollup_filtered_range_and_measure",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partitioned_rollup_coarser_than_partition() {
    run_both(
        indoc! {r#"
            measures:
              - part_sales.amount
            dimensions:
              - part_sales.brand
            time_dimensions:
              - dimension: part_sales.sold_at
                granularity: year
            order:
              - id: part_sales.brand
        "#},
        "partitioned_rollup_coarser_than_partition",
    )
    .await;
}
