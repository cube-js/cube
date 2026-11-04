//! A time-shifted measure grouped by the time dimension of a joined cube
//! returns the shifted values, the same as grouping by the fact's own column.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::{formatdoc, indoc};

const SEED: &str = "integration_calendar_joined_time_shift_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_calendar_joined_time_shift.yaml");
    TestContext::new(schema).unwrap()
}

fn day_query(measure: &str, dimension: &str) -> String {
    day_query_with(measure, dimension, "")
}

fn day_query_with(measure: &str, dimension: &str, extra: &str) -> String {
    formatdoc! {r#"
        measures:
          - jts_sales.total
          - jts_sales.{measure}
        {extra}
        time_dimensions:
          - dimension: {dimension}
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-07"
        order:
          - id: {dimension}
    "#}
}

/// Rows of `current` and `shifted` (both `(day, value)`) side by side over
/// the query's date range, the way the planner lays them out.
fn oracle_sql(shifted: &str) -> String {
    formatdoc! {r#"
        WITH cur AS (SELECT sale_date AS d, sum(amount) AS v FROM jts_sales GROUP BY 1),
             shifted AS ({shifted})
        SELECT coalesce(cur.d, shifted.d), cur.v, shifted.v
        FROM cur FULL JOIN shifted ON cur.d = shifted.d
        WHERE coalesce(cur.d, shifted.d) BETWEEN '2024-01-01' AND '2024-01-07 23:59:59.999'
        ORDER BY 1
    "#}
}

#[cfg(feature = "integration-postgres")]
async fn oracle_rows(sql: &str) -> Vec<Vec<String>> {
    use crate::test_fixtures::test_utils::{integration_context, pg_service};
    let client = pg_service::connect_and_seed(SEED).await;
    let messages = client.simple_query(sql).await.unwrap();
    table_rows(&integration_context::format_simple_query_results(&messages))
}

#[cfg(not(feature = "integration-postgres"))]
async fn oracle_rows(_sql: &str) -> Vec<Vec<String>> {
    unreachable!()
}

async fn assert_matches_oracle(query: &str, oracle: &str, expected: &[&[&str]]) {
    let ctx = create_context();
    ctx.build_sql(query).unwrap();

    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    let oracle = oracle_rows(oracle).await;
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect();
    assert_eq!(oracle, expected, "oracle disagrees with the expected table");
    assert_eq!(table_rows(&result), expected, "{result}");
}

const PREV_DAY: &str = indoc! {"
    SELECT sale_date + interval '1 day' AS d, sum(amount) AS v FROM jts_sales GROUP BY 1
"};

const PREV_DAY_EXPECTED: &[&[&str]] = &[
    &["2024-01-01 00:00:00", "5", "NULL"],
    &["2024-01-02 00:00:00", "10", "5"],
    &["2024-01-03 00:00:00", "20", "10"],
    &["2024-01-04 00:00:00", "NULL", "20"],
];

#[tokio::test(flavor = "multi_thread")]
async fn test_named_calendar_shift_by_calendar_pk() {
    let oracle = oracle_sql(indoc! {"
        SELECT c.date AS d, sum(s.amount) AS v
        FROM jts_calendar c JOIN jts_sales s ON s.sale_date = c.prior_peak
        GROUP BY 1
    "});
    assert_matches_oracle(
        &day_query("total_previous_peak", "jts_cal.date"),
        &oracle,
        NAMED_PEAK_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_named_calendar_shift_by_raw_column_joined_calendar() {
    let oracle = oracle_sql(indoc! {"
        SELECT c.next_peak AS d, sum(s.amount) AS v
        FROM jts_calendar c JOIN jts_sales s ON s.sale_date = c.date
        GROUP BY 1
    "});
    assert_matches_oracle(
        &day_query("total_previous_peak", "jts_cal_raw.date"),
        &oracle,
        NAMED_PEAK_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_raw_column_joined_calendar() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_cal_raw.date"),
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_calendar_pk() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_cal.date"),
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_joined_non_calendar_dimension() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_days.date"),
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_fact_dimension() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_sales.sale_date"),
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

const NAMED_PEAK_EXPECTED: &[&[&str]] = &[
    &["2024-01-01 00:00:00", "5", "NULL"],
    &["2024-01-02 00:00:00", "10", "NULL"],
    &["2024-01-03 00:00:00", "20", "5"],
    &["2024-01-04 00:00:00", "NULL", "10"],
    &["2024-01-05 00:00:00", "NULL", "20"],
];

async fn assert_rows(query: &str, expected: &[&[&str]]) {
    let ctx = create_context();
    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect();
    assert_eq!(table_rows(&result), expected, "{result}");
}

// A join off the calendar describes the reporting date and does not move the
// shift out of the bucket.
#[tokio::test(flavor = "multi_thread")]
async fn test_named_calendar_shift_next_to_a_join_off_the_calendar() {
    assert_rows(
        &day_query_with(
            "total_previous_peak",
            "jts_cal.date",
            "dimensions:\n  - jts_days_off.day_of_month",
        ),
        &[
            &["1", "2024-01-01 00:00:00", "5", "NULL"],
            &["2", "2024-01-02 00:00:00", "10", "NULL"],
            &["3", "2024-01-03 00:00:00", "20", "5"],
            &["4", "2024-01-04 00:00:00", "NULL", "10"],
            &["5", "2024-01-05 00:00:00", "NULL", "20"],
        ],
    )
    .await;
}

fn with_leading(value: &str, expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|row| {
            std::iter::once(value.to_string())
                .chain(row.iter().map(|cell| cell.to_string()))
                .collect()
        })
        .collect()
}

async fn assert_shift_kept_next_to_a_join_off_a_raw_column_joined_calendar(
    measure: &str,
    expected: &[&[&str]],
) {
    let query = day_query_with(
        measure,
        "jts_cal_raw.date",
        "dimensions:\n  - jts_days_off_raw.is_weekend",
    );
    let ctx = create_context();
    let Some(result) = ctx.try_execute_pg(&query, SEED).await else {
        return;
    };
    assert_eq!(table_rows(&result), with_leading("f", expected), "{result}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_named_calendar_shift_next_to_a_join_off_a_raw_column_joined_calendar() {
    assert_shift_kept_next_to_a_join_off_a_raw_column_joined_calendar(
        "total_previous_peak",
        NAMED_PEAK_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_next_to_a_join_off_a_raw_column_joined_calendar() {
    assert_shift_kept_next_to_a_join_off_a_raw_column_joined_calendar(
        "total_prev_day",
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_declared_interval_shift_by_calendar_pk() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_cal_declared.date"),
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_declared_interval_shift_by_raw_column_joined_calendar() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_cal_declared_raw.date"),
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}
