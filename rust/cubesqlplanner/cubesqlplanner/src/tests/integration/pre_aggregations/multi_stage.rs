use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_leaf_time_shift_pre_agg() {
    // Multi-stage time_shift measure whose leaf subquery is a full multiplied
    // AggregateMultipliedSubquery (sum(customers.lifetime_value) grouped by
    // returns.created_at through one_to_many join). Pre-aggregation
    // customers_lifetime_by_returns_month must replace the leaf — both for
    // the shifted and non-shifted CTE — so we expect 2 usages of the same
    // pre-aggregation.
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage_multiplied_pre_agg.yaml");
    let ctx = TestContext::new(schema).unwrap();

    // returns.count is added to force the optimizer off the simple-match path
    // (no single pre-agg covers all measures) and onto the multi-stage leaf
    // rewrite path, where the shifted and non-shifted CTEs are processed
    // independently.
    let query = indoc! {r#"
        measures:
          - customers.total_lifetime_value
          - customers.total_lifetime_value_prev_month_by_returns
          - returns.count
        time_dimensions:
          - dimension: returns.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();

    assert_eq!(
        pre_aggrs.len(),
        3,
        "Expected 3 usages (shifted customers + unshifted customers + returns); got {:?}",
        names
    );
    let customers_count = names
        .iter()
        .filter(|n| **n == "customers_lifetime_by_returns_month")
        .count();
    assert_eq!(
        customers_count, 2,
        "customers_lifetime_by_returns_month must be used twice (shifted + unshifted); got {:?}",
        names
    );
    assert!(
        names.contains(&"returns_count_by_month"),
        "Expected returns_count_by_month usage; got {:?}",
        names
    );

    // Partition usages by date_range: the shifted leaf must have a range
    // rewound by one month, the non-shifted usages must keep the original.
    let shifted_range = Some((
        "2023-12-01T00:00:00.000".to_string(),
        "2024-02-29T23:59:59.999".to_string(),
    ));
    let original_range = Some((
        "2024-01-01T00:00:00.000".to_string(),
        "2024-03-31T23:59:59.999".to_string(),
    ));

    let customers_usages: Vec<_> = pre_aggrs
        .iter()
        .filter(|u| u.name() == "customers_lifetime_by_returns_month")
        .collect();
    let shifted = customers_usages
        .iter()
        .find(|u| u.date_range == shifted_range)
        .expect("Expected a customers usage with shifted date_range");
    let unshifted = customers_usages
        .iter()
        .find(|u| u.date_range == original_range)
        .expect("Expected a customers usage with original date_range");
    assert_ne!(
        shifted.index, unshifted.index,
        "Shifted and unshifted usages must have different usage indexes"
    );

    let returns_usage = pre_aggrs
        .iter()
        .find(|u| u.name() == "returns_count_by_month")
        .expect("Expected returns_count_by_month usage");
    assert_eq!(
        returns_usage.date_range, original_range,
        "returns_count_by_month should have the original date range"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
