use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use itertools::Itertools;

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
        order:
          - id: returns.created_at
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

    if let Some(result) = ctx.try_execute(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_time_shift_pre_agg_with_leaf_measure_via_view() {
    // Same shape as test_multi_stage_time_shift_pre_agg_with_leaf_measure, but
    // through a view: the time dimension filter is view-qualified while the
    // accumulated time shifts are keyed by the underlying cube member. The
    // shifted leaf must still get its own widened date_range.
    //
    // The widened range only decides which rollup partitions are loaded, so
    // the assertions on the usages are what guard it; the executed rows are a
    // parity check against the cube-level query.
    let schema = MockSchema::from_yaml_file(YAML)
        .only_pre_aggregations(&["customers_lifetime_by_returns_month"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - customers_view.total_lifetime_value
          - customers_view.total_lifetime_value_prev_month_by_returns
        time_dimensions:
          - dimension: customers_view.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: customers_view.created_at
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

    if let Some(result) = ctx.try_execute(query, SEED).await {
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
        order:
          - id: returns.created_at
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

    if let Some(result) = ctx.try_execute(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_pre_agg_with_multiplied_leaf() {
    // Pre-aggregation stores the base measure at returns.created_at month
    // grain; the query asks for a to_date rolling window whose leaf is an
    // AggregateMultipliedSubquery over the same grain.
    let schema = MockSchema::from_yaml_file(YAML)
        .only_pre_aggregations(&["customers_lifetime_by_returns_month"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - customers.total_lifetime_value_ytd
        time_dimensions:
          - dimension: returns.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    // The rolling-window leaf measure is the unrolled `*_ytd` symbol whose
    // sql is a raw column, so the matcher can match it neither by name nor
    // by member decomposition — the query goes to raw tables.
    assert!(
        pre_aggrs.is_empty(),
        "Rolling-window leaf is not matchable by name; got {:?}",
        pre_aggrs
            .iter()
            .map(|u| u.name().clone())
            .collect::<Vec<_>>()
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_pre_agg_with_filter_multiplied_leaf() {
    // The filter on returns pulls a one_to_many join into the leaf, so
    // sum(amount) becomes multiplied. The pre-aggregation has no returns
    // members and cannot cover the filter.
    let schema =
        MockSchema::from_yaml_file(YAML).only_pre_aggregations(&["orders_amount_by_month"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
        filters:
          - member: returns.customer_id
            operator: set
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    assert!(
        pre_aggrs.is_empty(),
        "Pre-agg without the filter dimension must not match; got {:?}",
        pre_aggrs
            .iter()
            .map(|u| u.name().clone())
            .collect::<Vec<_>>()
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_pre_agg_covering_multiplying_filter() {
    // Same query, but the pre-aggregation also stores returns.customer_id —
    // the dimension the multiplying filter applies to.
    let schema = MockSchema::from_yaml_file(YAML)
        .only_pre_aggregations(&["orders_amount_by_month_and_return_customer"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
        filters:
          - member: returns.customer_id
            operator: set
        time_dimensions:
          - dimension: orders.created_at
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
            .all(|n| *n == "orders_amount_by_month_and_return_customer"),
        "Both usages must be orders_amount_by_month_and_return_customer; got {:?}",
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
    assert!(
        pre_aggrs.iter().any(|u| u.date_range == shifted_range),
        "Expected a usage with shifted date_range"
    );
    assert!(
        pre_aggrs.iter().any(|u| u.date_range == original_range),
        "Expected a usage with original date_range"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

const DERIVED_YAML: &str = "common/integration_derived_time_dim_shift_pre_agg.yaml";
const DERIVED_SEED: &str = "integration_derived_time_dim_shift_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_time_shift_pre_agg_on_derived_time_dimension() {
    // The shift is declared on a time dimension derived from another cube's
    // time dimension, and the pre-aggregation materializes that derived
    // dimension. The shift must reach both the widened date_range and the
    // rollup column the shifted leaf reads.
    let schema = MockSchema::from_yaml_file(DERIVED_YAML)
        .only_pre_aggregations(&["value_by_return_day_month"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - pa_customers.total_value
          - pa_customers.total_value_prev_month
        time_dimensions:
          - dimension: pa_customers.return_day
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: pa_customers.return_day
    "#};

    let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();

    assert_eq!(
        pre_aggrs.len(),
        2,
        "Expected 2 usages (shifted + unshifted leaf); got {:?}",
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
    assert!(
        pre_aggrs.iter().any(|u| u.date_range == shifted_range),
        "Expected a usage with shifted date_range; got {:?}",
        pre_aggrs.iter().map(|u| u.date_range.clone()).collect_vec()
    );
    assert!(
        pre_aggrs.iter().any(|u| u.date_range == original_range),
        "Expected a usage with original date_range; got {:?}",
        pre_aggrs.iter().map(|u| u.date_range.clone()).collect_vec()
    );

    // The shifted leaf reads the rollup column, so the interval has to be
    // applied to that column — the derived member's own SQL is never
    // expanded here and cannot carry the shift. Every occurrence must sit
    // directly on the column: one landing on an already shifted expression
    // would mean the interval was added twice.
    let shifts = sql.matches("interval '1 month'").count();
    let shifts_on_column = sql
        .matches(r#""pa_customers__return_day_month" + interval '1 month'"#)
        .count();
    assert!(
        shifts > 0,
        "Shifted leaf must offset the pre-aggregation column:\n{}",
        sql
    );
    assert_eq!(
        shifts_on_column, shifts,
        "Every shift must be applied to the rollup column itself:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute(query, DERIVED_SEED).await {
        insta::assert_snapshot!(result);
    }
}

const MULTI_DEP_YAML: &str = "common/integration_multi_dep_time_dim_shift_pre_agg.yaml";
const MULTI_DEP_SEED: &str = "integration_multi_dep_time_dim_shift_tables.sql";

// A rollup stores one column for a time dimension built from two owned time
// dimensions, while the shift covers only one of them. Offsetting the stored
// column would move every row, including those the shift must leave in place,
// so no offset of that column can reproduce the shifted values and the rollup
// cannot serve the shifted leaf at all.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_time_shift_pre_agg_on_partially_shifted_time_dimension() {
    let schema = MockSchema::from_yaml_file(MULTI_DEP_YAML)
        .only_pre_aggregations(&["total_by_effective_month"]);
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - mdd_events.total
          - mdd_events.total_prev_month
        time_dimensions:
          - dimension: mdd_events.effective_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: mdd_events.effective_at
    "#};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    assert!(
        pre_aggrs.is_empty(),
        "Rollup cannot represent the shifted leaf, so the query must fall back to base SQL; got {:?}",
        pre_aggrs.iter().map(|u| u.name().clone()).collect_vec()
    );

    // Snapshot holds the values the same query produces from base SQL.
    if let Some(result) = ctx.try_execute(query, MULTI_DEP_SEED).await {
        insta::assert_snapshot!(result);
    }
}
