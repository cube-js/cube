use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

#[test]
fn test_rolling_window_trailing_unbounded_no_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val
        time_dimensions:
          - dimension: test_cube.created_at
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for trailing unbounded");

    // trailing: unbounded means no lower time bound — only upper bound should exist
    assert!(
        !sql.contains(">= $_0_$"),
        "Trailing unbounded should not have a lower time bound (>=), got: {sql}"
    );
    // Without granularity there's no time_series CTE, so the upper bound
    // must use a parameter directly, not a subquery from time_series
    assert!(
        !sql.contains("time_series"),
        "Without granularity should not reference time_series CTE, got: {sql}"
    );
}

#[test]
fn test_rolling_window_leading_unbounded_no_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_leading
        time_dimensions:
          - dimension: test_cube.created_at
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for leading unbounded");

    // leading: unbounded means no upper time bound — only lower bound should exist
    assert!(
        !sql.contains("<= $_1_$"),
        "Leading unbounded should not have an upper time bound (<=), got: {sql}"
    );
}

#[test]
fn test_rolling_window_both_unbounded_no_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_both
        time_dimensions:
          - dimension: test_cube.created_at
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for both unbounded");

    // both unbounded means no time bounds at all
    assert!(
        !sql.contains(">= $_0_$"),
        "Both unbounded should not have a lower time bound (>=), got: {sql}"
    );
    assert!(
        !sql.contains("<= $_1_$"),
        "Both unbounded should not have an upper time bound (<=), got: {sql}"
    );
}

#[test]
fn test_rolling_window_trailing_unbounded_with_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val
        time_dimensions:
          - dimension: test_cube.created_at
            granularity: day
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for trailing unbounded with granularity");

    // With granularity, trailing: unbounded should not have a lower time bound
    // in either the WHERE clause or the JOIN ON condition
    assert!(
        !sql.contains(">= \"time_series\".\"date_from\""),
        "JOIN should not have lower bound with trailing unbounded, got: {sql}"
    );
}