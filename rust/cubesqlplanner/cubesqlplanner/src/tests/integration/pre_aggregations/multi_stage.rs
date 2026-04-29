use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_multi_stage_tables.sql";
const YAML: &str = "common/integration_multi_stage_multiplied_pre_agg.yaml";

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_time_shift_pre_agg_with_leaf_measure() {
    // Pre-aggregation stores only the base (leaf) measure total_lifetime_value.
    // The matcher refuses simple-match for the multi-stage measure (so its
    // time_shift semantics is preserved) and falls back to multi-stage leaf
    // rewrite: two CTEs are produced (shifted + non-shifted), each backed by
    // the same pre-aggregation with its own date_range.
    let schema = MockSchema::from_yaml_file(YAML)
        .only_pre_aggregations(&["customers_lifetime_by_returns_month"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - customers.total_lifetime_value
          - customers.total_lifetime_value_prev_month_by_returns
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
        2,
        "Expected 2 usages (shifted + unshifted leaf); got {:?}",
        names
    );
    assert!(
        names
            .iter()
            .all(|n| *n == "customers_lifetime_by_returns_month"),
        "Both usages must be customers_lifetime_by_returns_month; got {:?}",
        names
    );

    let shifted_range = Some((
        "2023-12-01T00:00:00.000".to_string(),
        "2024-02-29T23:59:59.999".to_string(),
    ));
    let original_range = Some((
        "2024-01-01T00:00:00.000".to_string(),
        "2024-03-31T23:59:59.999".to_string(),
    ));
    let shifted = pre_aggrs
        .iter()
        .find(|u| u.date_range == shifted_range)
        .expect("Expected a usage with shifted date_range");
    let unshifted = pre_aggrs
        .iter()
        .find(|u| u.date_range == original_range)
        .expect("Expected a usage with original date_range");
    assert_ne!(
        shifted.index, unshifted.index,
        "Shifted and unshifted usages must have different usage indexes"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_time_shift_pre_agg_with_multi_stage_measure() {
    let schema = MockSchema::from_yaml_file(YAML)
        .only_pre_aggregations(&["customers_lifetime_prev_month_pre_agg"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - customers.total_lifetime_value_prev_month_by_returns
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
        1,
        "Expected single pre-agg usage when pre-agg directly stores the multi-stage measure; got {:?}",
        names
    );
    assert_eq!(pre_aggrs[0].name(), "customers_lifetime_prev_month_pre_agg");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
