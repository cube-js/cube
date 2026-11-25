//! Multi-stage measures of a fact under two date roles, both read from one
//! Cube Store rollup. Scan sharing must keep the legs of different roles apart:
//! every query agrees with and without the sharing, and with the source tables.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use itertools::Itertools;

const SEED: &str = "view_dimension_map_tables.sql";
const YAML: &str = "common/view_dimension_map_shared_scan.yaml";
const ROLLUP: &str = "by_both_roles";

fn source_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[])).unwrap()
}

fn cubestore_ctx() -> TestContext {
    TestContext::new_with_external_cubestore(MockSchema::from_yaml_file(YAML)).unwrap()
}

fn with_sharing(query: &str, sharing: bool) -> String {
    format!("{}multi_stage_scan_sharing: {}\n", query, sharing)
}

// Cube Store and Postgres print numbers differently.
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
            .sorted()
            .join("\n")
    })
}

// The rows with the sharing, after checking both legs read the rollup and the
// rows agree without the sharing and with the source tables.
async fn shared_rows(query: &str) -> Option<String> {
    let ctx = cubestore_ctx();
    let (sql, usages) = ctx
        .build_sql_with_used_pre_aggregations(&with_sharing(query, true))
        .unwrap();
    assert!(
        usages.len() == 2 && usages.iter().all(|usage| usage.name() == ROLLUP),
        "{sql}"
    );
    let shared = normalize(ctx.try_execute(&with_sharing(query, true), SEED).await);
    let separate = normalize(ctx.try_execute(&with_sharing(query, false), SEED).await);
    let source = normalize(source_ctx().try_execute_pg(query, SEED).await);
    if shared.is_some() {
        assert_eq!(shared, separate, "Shared and separate differ\n{sql}");
        assert_eq!(shared, source, "Shared differs from source tables\n{sql}");
    }
    shared
}

// The same cube measure under both roles. 2026-01: 2 / -, 2026-02: 2 / 4.
#[tokio::test(flavor = "multi_thread")]
async fn test_one_multi_stage_measure_under_two_roles() {
    let query = indoc! {r#"
        measures:
          - biz.purchases_count_doubled
          - biz.cancellations_count_doubled
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
    "#};

    if let Some(rows) = shared_rows(query).await {
        insta::assert_snapshot!(rows);
    }
}

// A ratio of two leaves under both roles. Purchased in 2026-01: 1 of 1
// cancelled, in 2026-02: 0 of 1; cancelled in 2026-02: 2 of 2.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_ratio_under_two_roles() {
    let query = indoc! {r#"
        measures:
          - biz.purchases_cancellations_to_count
          - biz.cancellations_cancellations_to_count
        dimensions:
          - biz.dt_year_month
        filters:
          - member: biz.dt_year
            operator: equals
            values: ["2026"]
    "#};

    if let Some(rows) = shared_rows(query).await {
        insta::assert_snapshot!(rows);
    }
}
