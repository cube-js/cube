use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

// A multi-stage `filter.include` date range narrows the rolling window it
// wraps to the included period. The results are checked against Postgres in
// the schema compiler's `multi-stage-filter-include-date-range` spec.

fn schema() -> MockSchema {
    MockSchema::from_yaml_file("common/integration_rolling_window.yaml")
}

#[test]
fn test_include_window_has_a_series_of_its_own() {
    let ctx = TestContext::new(schema()).unwrap();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_3d_jan15
          - orders.rolling_sum_3d_multistage
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-13"
              - "2024-01-16"
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(sql.contains("time_series_1"), "got: {sql}");
}

#[test]
fn test_include_outside_the_query_range_has_no_rows() {
    for ctx in [
        TestContext::new(schema()).unwrap(),
        TestContext::new_with_generated_time_series(schema()).unwrap(),
    ] {
        let query = indoc! {r#"
            measures:
              - orders.rolling_sum_3d_jan15
            time_dimensions:
              - dimension: orders.created_at
                granularity: day
                dateRange:
                  - "2024-01-01"
                  - "2024-01-05"
        "#};

        let sql = ctx.build_sql(query).unwrap();
        assert!(sql.contains("WHERE 1 = 0"), "got: {sql}");
    }
}
