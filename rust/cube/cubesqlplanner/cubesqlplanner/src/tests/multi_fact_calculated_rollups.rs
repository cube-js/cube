//! Rollups serving a calculated measure over several facts.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

// Each leaf is read from the rollup of its own fact.
#[test]
fn each_leaf_of_a_calculated_measure_reads_the_rollup_of_its_fact() {
    let ctx = TestContext::new(MockSchema::from_yaml_file(
        "common/multi_fact_calculated_rollups.yaml",
    ))
    .unwrap();
    let (sql, usages) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {"
            measures:
              - orders.avg_amount_per_return
            dimensions:
              - customers.city
        "})
        .unwrap();

    let mut used = usages
        .iter()
        .map(|u| u.pre_aggregation.name().clone())
        .collect::<Vec<_>>();
    used.sort();
    assert_eq!(
        used,
        vec!["by_city".to_string(), "by_city".to_string()],
        "{sql}"
    );
    assert!(
        sql.contains("orders__by_city") && sql.contains("returns__by_city"),
        "{sql}"
    );
}
