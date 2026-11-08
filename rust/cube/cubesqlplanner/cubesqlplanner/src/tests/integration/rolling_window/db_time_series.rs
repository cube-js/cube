use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new_with_generated_time_series(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_db_ts_rolling_trailing_unbounded_with_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_ts_rolling_bounded_with_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_bounded
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_ts_rolling_to_date_with_granularity() {
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
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_ts_rolling_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
        order:
          - id: orders.category
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_ts_rolling_with_filter() {
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
        filters:
          - dimension: orders.status
            operator: equals
            values:
              - completed
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_ts_multiple_rolling() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
          - orders.rolling_count_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The series of a predefined granularity starts on the first bucket boundary at or before the
/// date range, as the series that Cube generates for the sources with no generate_series.
#[test]
fn test_db_ts_starts_on_a_bucket_boundary() {
    let ctx = create_context();

    for (granularity, from, to, expected) in [
        (
            "month",
            "2024-01-15",
            "2024-02-20",
            "2024-01-01T00:00:00.000",
        ),
        (
            "month",
            "2024-01-01",
            "2024-02-20",
            "2024-01-01T00:00:00.000",
        ),
        (
            "month",
            "2024-01-31",
            "2024-01-31",
            "2024-01-01T00:00:00.000",
        ),
        (
            "quarter",
            "2024-02-15",
            "2024-05-20",
            "2024-01-01T00:00:00.000",
        ),
        (
            "year",
            "2024-02-15",
            "2024-05-20",
            "2024-01-01T00:00:00.000",
        ),
        // A start on the end of a month or on a leap day must not move the next buckets
        (
            "month",
            "2024-02-29",
            "2024-03-31",
            "2024-02-01T00:00:00.000",
        ),
        (
            "month",
            "2024-03-31",
            "2024-05-31",
            "2024-03-01T00:00:00.000",
        ),
        // 2024-01-10 is a Wednesday
        (
            "week",
            "2024-01-10",
            "2024-01-20",
            "2024-01-08T00:00:00.000",
        ),
        (
            "week",
            "2024-01-08",
            "2024-01-20",
            "2024-01-08T00:00:00.000",
        ),
        ("day", "2024-01-10", "2024-01-20", "2024-01-10T00:00:00.000"),
        (
            "day",
            "2024-01-10T12:30:00.000",
            "2024-01-20",
            "2024-01-10T00:00:00.000",
        ),
        (
            "hour",
            "2024-01-10T12:30:00.000",
            "2024-01-10T15:00:00.000",
            "2024-01-10T12:00:00.000",
        ),
    ] {
        let query = format!(
            "measures:\n  - orders.rolling_sum_bounded\ntime_dimensions:\n  - dimension: orders.created_at\n    granularity: {granularity}\n    dateRange:\n      - \"{from}\"\n      - \"{to}\"\n"
        );

        let sql = ctx.build_sql(&query).unwrap();

        assert!(
            sql.contains(&format!("'{expected}'")),
            "{granularity} from {from} must start at {expected}: {sql}"
        );
    }
}
