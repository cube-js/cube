use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_timezone_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_with_timezone() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        timezone: "America/New_York"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_with_timezone() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        timezone: "Europe/Berlin"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// The base scan compares the member converted into the query's timezone, so
// the bounds it is given have to sit in that timezone too — which is also
// where the series places its points, and where the frame folded into the
// lower bound is counted. Carried into the database's timezone instead they
// would sit an offset away, and the scan would drop the first hours of every
// window. Invisible at UTC, where the conversion does nothing.
#[tokio::test(flavor = "multi_thread")]
async fn test_base_scan_bounds_share_the_timezone_of_the_series() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-12"
        timezone: "America/Los_Angeles"
    "#};

    let (sql, params) = ctx.build_sql_and_params(query).unwrap();
    let bounds = params
        .iter()
        .filter_map(|value| value.to_param_string())
        .collect::<Vec<_>>();

    assert_eq!(
        bounds,
        vec!["2024-01-03T00:00:00.000", "2024-01-12T23:59:59.999"],
        "the bounds left the timezone the series and the member are in:\n{sql}"
    );
}
