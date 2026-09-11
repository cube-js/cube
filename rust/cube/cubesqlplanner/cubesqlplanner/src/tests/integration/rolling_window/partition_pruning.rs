//! What a rolling window's base scan bounds itself by, seen from the engine's
//! side: a bound it can evaluate before reading the table eliminates
//! partitions, and a scalar sub-select over the time series does not. A table
//! declared with a mandatory partition filter rejects the query outright.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SERIES_SUBSELECT: &str = "min(\"date_from\")";

/// A `to_date` window's lower bound is the start of the period the series
/// opens in, and its upper bound the series' own end. Both follow from the
/// time dimension's granularity and date range, so both are known while
/// planning.
#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_base_scan_bounds_are_literal() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-15"
              - "2024-02-15"
    "#};

    let (sql, params) = ctx.build_sql_and_params(query).unwrap();

    assert!(
        !sql.contains(SERIES_SUBSELECT),
        "base scan date bound is a scalar sub-select over time_series:\n{sql}"
    );

    // The window reaches back to the start of January, not to the start of the
    // reported range.
    let bounds = params
        .iter()
        .filter_map(|value| value.to_param_string())
        .collect::<Vec<_>>();
    assert!(
        bounds.contains(&"2024-01-01T00:00:00.000".to_string()),
        "base scan does not reach the start of the window's period: {bounds:?}\n{sql}"
    );
}

/// The same for a window whose period is a quarter: the bound is the quarter
/// the range opens in, not the month or the range.
#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_quarter_base_scan_bounds_are_literal() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date_quarter
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-02-01"
              - "2024-03-31"
    "#};

    let (sql, params) = ctx.build_sql_and_params(query).unwrap();

    assert!(
        !sql.contains(SERIES_SUBSELECT),
        "base scan date bound is a scalar sub-select over time_series:\n{sql}"
    );

    let bounds = params
        .iter()
        .filter_map(|value| value.to_param_string())
        .collect::<Vec<_>>();
    assert!(
        bounds.contains(&"2024-01-01T00:00:00.000".to_string()),
        "base scan does not reach the start of the window's quarter: {bounds:?}\n{sql}"
    );
}

/// Characterisation: a period whose boundaries are rows of a calendar cube is
/// not reachable by interval math, so its lower bound stays a sub-select over
/// the series that read it. Recorded to mark the case literals cannot close;
/// the upper bound is the series' own end and is derivable either way.
#[tokio::test(flavor = "multi_thread")]
async fn test_calendar_period_start_stays_a_sub_select() {
    let schema = MockSchema::from_yaml_file("common/integration_calendar.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count_month_to_date
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: day
            dateRange:
              - "2024-02-29"
              - "2024-03-09"
    "#};

    let sql = ctx.build_sql(query).unwrap();

    assert!(
        sql.contains("min(\"date_period_start_month\")"),
        "expected the calendar period start to be read off the series:\n{sql}"
    );
}
