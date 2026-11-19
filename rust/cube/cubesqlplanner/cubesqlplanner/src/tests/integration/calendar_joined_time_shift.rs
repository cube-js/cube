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

/// Fragments of the generated SQL that pin where the shift is rendered, so the
/// tests guard the fix without a database too.
struct Shape<'a> {
    present: &'a [&'a str],
    absent: &'a [&'a str],
}

fn assert_shape(sql: &str, shape: &Shape) {
    let sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    for fragment in shape.present {
        assert!(sql.contains(fragment), "missing `{fragment}` in:\n{sql}");
    }
    for fragment in shape.absent {
        assert!(
            !sql.contains(fragment),
            "unexpected `{fragment}` in:\n{sql}"
        );
    }
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

fn to_rows(expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

async fn assert_matches_oracle(query: &str, shape: Shape<'_>, oracle: &str, expected: &[&[&str]]) {
    let ctx = create_context();
    assert_shape(&ctx.build_sql(query).unwrap(), &shape);

    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    let expected = to_rows(expected);
    assert_eq!(
        oracle_rows(oracle).await,
        expected,
        "oracle disagrees with the expected table"
    );
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

const NAMED_PEAK_EXPECTED: &[&[&str]] = &[
    &["2024-01-01 00:00:00", "5", "NULL"],
    &["2024-01-02 00:00:00", "10", "NULL"],
    &["2024-01-03 00:00:00", "20", "5"],
    &["2024-01-04 00:00:00", "NULL", "10"],
    &["2024-01-05 00:00:00", "NULL", "20"],
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
        Shape {
            present: &[r#"ON "jts_sales".sale_date = "jts_cal".prior_peak"#],
            absent: &[
                r#"("jts_cal".prior_peak::timestamptz"#,
                r#""jts_cal".prior_peak >="#,
            ],
        },
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
        Shape {
            present: &[
                r#"date_trunc('day', ("jts_cal_raw".next_peak::timestamptz"#,
                r#"ON "jts_sales".sale_date = "jts_cal_raw".date"#,
            ],
            absent: &[r#"ON "jts_sales".sale_date = "jts_cal_raw".next_peak"#],
        },
        &oracle,
        NAMED_PEAK_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_calendar_pk() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_cal.date"),
        Shape {
            present: &[r#"ON "jts_sales".sale_date = ("jts_cal".date + interval '-1 day')"#],
            absent: &[r#"("jts_cal".date + interval '-1 day')::timestamptz"#],
        },
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_raw_column_joined_calendar() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_cal_raw.date"),
        Shape {
            present: &[
                r#"date_trunc('day', (("jts_cal_raw".date + interval '1 day')::timestamptz"#,
            ],
            absent: &["interval '-1 day'"],
        },
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_declared_interval_shift_by_calendar_pk() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_cal_declared.date"),
        Shape {
            present: &[
                r#"ON "jts_sales".sale_date = ("jts_cal_declared".date + interval '-1 day')"#,
            ],
            absent: &[r#"("jts_cal_declared".date + interval '-1 day')::timestamptz"#],
        },
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_declared_interval_shift_by_raw_column_joined_calendar() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_cal_declared_raw.date"),
        Shape {
            present: &[
                r#"date_trunc('day', (("jts_cal_declared_raw".date + interval '1 day')::timestamptz"#,
            ],
            absent: &["interval '-1 day'"],
        },
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_joined_non_calendar_dimension() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_days.date"),
        Shape {
            present: &[r#"date_trunc('day', (("jts_days".date + interval '1 day')::timestamptz"#],
            absent: &[r#"ON "jts_sales".sale_date = ("jts_days".date"#],
        },
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_fact_dimension() {
    assert_matches_oracle(
        &day_query("total_prev_day", "jts_sales.sale_date"),
        Shape {
            present: &[
                r#"date_trunc('day', (("jts_sales".sale_date + interval '1 day')::timestamptz"#,
            ],
            absent: &[],
        },
        &oracle_sql(PREV_DAY),
        PREV_DAY_EXPECTED,
    )
    .await;
}

// A cube joined off the shifted member's cube describes the reporting date,
// and its join does not move the shift out of the bucket.
async fn assert_day_of_month_next_to(
    measure: &str,
    dimension: &str,
    attributes: &str,
    shape: Shape<'_>,
    expected: &[&[&str]],
) {
    let query = day_query_with(
        measure,
        dimension,
        &format!("dimensions:\n  - {attributes}.day_of_month"),
    );
    let ctx = create_context();
    assert_shape(&ctx.build_sql(&query).unwrap(), &shape);

    let Some(result) = ctx.try_execute_pg(&query, SEED).await else {
        return;
    };
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| {
            let day = row[0][8..10].trim_start_matches('0').to_string();
            std::iter::once(day)
                .chain(row.iter().map(|cell| cell.to_string()))
                .collect()
        })
        .collect();
    assert_eq!(table_rows(&result), expected, "{result}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_named_calendar_shift_next_to_a_join_off_the_calendar() {
    assert_day_of_month_next_to(
        "total_previous_peak",
        "jts_cal.date",
        "jts_days_off",
        Shape {
            present: &[
                r#"ON "jts_sales".sale_date = "jts_cal".prior_peak"#,
                r#"ON "jts_cal".date = "jts_days_off".date"#,
            ],
            absent: &[r#""jts_cal".prior_peak = "jts_days_off".date"#],
        },
        NAMED_PEAK_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_named_calendar_shift_next_to_a_join_off_a_raw_column_joined_calendar() {
    assert_day_of_month_next_to(
        "total_previous_peak",
        "jts_cal_raw.date",
        "jts_days_off_raw",
        Shape {
            present: &[r#"ON "jts_cal_raw".next_peak = "jts_days_off_raw".date"#],
            absent: &[r#"ON "jts_sales".sale_date = "jts_cal_raw".next_peak"#],
        },
        NAMED_PEAK_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_next_to_a_join_off_a_raw_column_joined_calendar() {
    assert_day_of_month_next_to(
        "total_prev_day",
        "jts_cal_raw.date",
        "jts_days_off_raw",
        Shape {
            present: &[r#"ON ("jts_cal_raw".date + interval '1 day') = "jts_days_off_raw".date"#],
            absent: &["interval '-1 day'"],
        },
        PREV_DAY_EXPECTED,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_next_to_a_join_off_a_non_calendar_cube() {
    assert_day_of_month_next_to(
        "total_prev_day",
        "jts_days.date",
        "jts_days_attrs",
        Shape {
            present: &[r#"ON ("jts_days".date + interval '1 day') = "jts_days_attrs".date"#],
            absent: &[r#"ON "jts_sales".sale_date = ("jts_days".date"#],
        },
        PREV_DAY_EXPECTED,
    )
    .await;
}

fn calendar_measure_query(dimensions: &str) -> String {
    let order = if dimensions.is_empty() {
        String::new()
    } else {
        format!("\n  - id: {dimensions}")
    };
    formatdoc! {r#"
        measures:
          - jts_cal.day_sum
          - jts_cal.day_sum_prev_day
        dimensions: [{dimensions}]
        time_dimensions:
          - dimension: jts_cal.date
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-07"
        order:
          - id: jts_cal.date{order}
    "#}
}

async fn assert_rows(query: &str, shape: Shape<'_>, expected: &[&[&str]]) {
    let ctx = create_context();
    assert_shape(&ctx.build_sql(query).unwrap(), &shape);

    let Some(result) = ctx.try_execute_pg(query, SEED).await else {
        return;
    };
    assert_eq!(table_rows(&result), to_rows(expected), "{result}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_calendar_measure_interval_shift_by_calendar_pk() {
    assert_rows(
        &calendar_measure_query(""),
        Shape {
            present: &[r#"date_trunc('day', (("jts_cal".date + interval '1 day')::timestamptz"#],
            absent: &["interval '-1 day'"],
        },
        &[
            &["2024-01-01 00:00:00", "1", "NULL"],
            &["2024-01-02 00:00:00", "2", "1"],
            &["2024-01-03 00:00:00", "3", "2"],
            &["2024-01-04 00:00:00", "4", "3"],
            &["2024-01-05 00:00:00", "5", "4"],
            &["2024-01-06 00:00:00", "6", "5"],
            &["2024-01-07 00:00:00", "7", "6"],
        ],
    )
    .await;
}

// A fact dimension multiplies the calendar measure, so it is aggregated over
// the keys of the calendar rows the facts reach.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiplied_calendar_measure_interval_shift_by_calendar_pk() {
    let oracle = indoc! {"
        WITH k AS (SELECT DISTINCT s.kind, c.date FROM jts_sales s JOIN jts_calendar c ON s.sale_date = c.date),
             cur AS (SELECT kind, date AS d, sum(extract(day from date)::int) AS v FROM k GROUP BY 1, 2),
             shifted AS (SELECT kind, d + interval '1 day' AS d, v FROM cur)
        SELECT coalesce(cur.kind, shifted.kind), coalesce(cur.d, shifted.d), cur.v, shifted.v
        FROM cur FULL JOIN shifted ON cur.kind = shifted.kind AND cur.d = shifted.d
        ORDER BY 2, 1
    "};
    let expected: &[&[&str]] = &[
        &["a", "2024-01-01 00:00:00", "1", "NULL"],
        &["a", "2024-01-02 00:00:00", "NULL", "1"],
        &["b", "2024-01-02 00:00:00", "2", "NULL"],
        &["a", "2024-01-03 00:00:00", "3", "NULL"],
        &["b", "2024-01-03 00:00:00", "NULL", "2"],
        &["a", "2024-01-04 00:00:00", "NULL", "3"],
    ];
    let query = calendar_measure_query("jts_sales.kind");
    let ctx = create_context();
    if ctx.try_execute_pg(&query, SEED).await.is_some() {
        assert_eq!(
            oracle_rows(oracle).await,
            to_rows(expected),
            "oracle disagrees with the expected table"
        );
    }
    assert_rows(
        &query,
        Shape {
            present: &[
                r#"ON "jts_cal_key_jts_sales".sale_date = ("jts_cal_key_jts_cal".date + interval '-1 day')"#,
                r#"ON (("keys"."jts_cal__date" = ("jts_cal_key_jts_cal".date + interval '1 day')))"#,
            ],
            absent: &[r#""keys"."jts_cal__date" = ("jts_cal_key_jts_cal".date + interval '-1 day')"#],
        },
        expected,
    )
    .await;
}

// A multi-stage dimension is joined to the measure by the time dimension, so
// both have to read it the same way.
async fn assert_same_rows_as_by_fact_dimension(dimension: &str, shape: Shape<'_>) {
    let query = |time_dimension: &str| {
        formatdoc! {r#"
            measures:
              - jts_sales.total
              - jts_sales.total_prev_day
            dimensions:
              - jts_sales.tier
            time_dimensions:
              - dimension: {time_dimension}
                granularity: day
                dateRange:
                  - "2024-01-01"
                  - "2024-01-07"
            order:
              - id: {time_dimension}
        "#}
    };
    let ctx = create_context();
    let joined = query(dimension);
    assert_shape(&ctx.build_sql(&joined).unwrap(), &shape);

    let Some(result) = ctx.try_execute_pg(&joined, SEED).await else {
        return;
    };
    let control = ctx
        .try_execute_pg(&query("jts_sales.sale_date"), SEED)
        .await
        .unwrap();
    assert_eq!(table_rows(&result), table_rows(&control), "{result}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_calendar_pk_next_to_a_multi_stage_dimension() {
    assert_same_rows_as_by_fact_dimension(
        "jts_cal.date",
        Shape {
            present: &[r#"ON "jts_sales".sale_date = ("jts_cal".date + interval '-1 day')"#],
            absent: &[r#"date_trunc('day', (("jts_cal".date + interval '-1 day')"#],
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_interval_shift_by_joined_non_calendar_dimension_next_to_a_multi_stage_dimension() {
    assert_same_rows_as_by_fact_dimension(
        "jts_days.date",
        Shape {
            present: &[r#"date_trunc('day', (("jts_days".date + interval '1 day')::timestamptz"#],
            absent: &[r#"ON "jts_sales".sale_date = ("jts_days".date"#],
        },
    )
    .await;
}
