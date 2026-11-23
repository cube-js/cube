//! Rollups storing one time dimension at several granularities: a query must
//! read the column its granularity derives from, in select and filters alike.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::{formatdoc, indoc};

const SEED: &str = "integration_multi_granularity_rollup_tables.sql";
const YAML: &str = "common/integration_multi_granularity_rollup.yaml";

fn ctx_with(pre_aggs: &[&str]) -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
}

/// Runs the query served by `pre_agg` (materialized by the harness) and again
/// with no pre-aggregation at all.
async fn rollup_vs_source(query: &str, pre_agg: &str) -> Option<(String, String)> {
    let with_rollup = ctx_with(&[pre_agg]);
    let (sql, usages) = with_rollup
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        usages.iter().any(|u| u.name() == pre_agg),
        "expected the query to be served by `{pre_agg}`; SQL:\n{sql}"
    );
    let rollup = with_rollup.try_execute_pg(query, SEED).await?;
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source))
}

// CubeStore formats values its own way, so its result is a snapshot.
async fn on_cubestore(query: &str, pre_agg: &str) -> Option<String> {
    let schema = MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[pre_agg]);
    let ctx = TestContext::new_with_external_cubestore(schema).unwrap();
    ctx.try_execute_cubestore(query, SEED).await
}

async fn fallback_vs_source(query: &str, pre_agg: &str) -> Option<(String, String)> {
    let with_rollup = ctx_with(&[pre_agg]);
    let (sql, usages) = with_rollup
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        usages.is_empty(),
        "expected the query to fall back to the source; SQL:\n{sql}"
    );
    let rollup = with_rollup.try_execute_pg(query, SEED).await?;
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source))
}

fn query(granularity: &str, from: &str, to: &str) -> String {
    formatdoc! {r#"
        measures:
          - activity.count
          - activity.amount
        dimensions:
          - activity.app_name
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: {granularity}
            dateRange:
              - "{from}"
              - "{to}"
        order:
          - id: activity.cohort_date
          - id: activity.app_name
    "#}
}

#[tokio::test(flavor = "multi_thread")]
async fn day_query_on_a_single_day_reads_the_day_column() {
    let query = query("day", "2025-01-05", "2025-01-05");
    if let Some((rollup, source)) = rollup_vs_source(&query, "all_grains").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "all_grains").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn day_query_across_a_year_boundary_reads_the_day_column() {
    let query = query("day", "2024-12-28", "2025-01-03");
    if let Some((rollup, source)) = rollup_vs_source(&query, "all_grains").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "all_grains").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn month_query_reads_the_month_column() {
    let query = query("month", "2024-12-01", "2025-01-31");
    if let Some((rollup, source)) = rollup_vs_source(&query, "all_grains").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "all_grains").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn day_query_ignores_a_coarser_column_listed_first() {
    let query = query("day", "2025-01-30", "2025-02-02");
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "month_then_day").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn quarter_query_rolls_up_a_finer_stored_column() {
    let query = query("quarter", "2024-10-01", "2025-03-31");
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "month_then_day").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn week_query_reads_the_week_column_listed_before_month() {
    let query = indoc! {r#"
        measures:
          - activity.count
          - activity.amount
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: week
            dateRange:
              - "2024-12-23"
              - "2025-01-12"
        order:
          - id: activity.cohort_date
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "week_and_month").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(query, "week_and_month").await {
        insta::assert_snapshot!(result);
    }
}

// Neither stored column yields both a week and a month, so the rollup cannot
// serve the query at all.
#[tokio::test(flavor = "multi_thread")]
async fn week_and_month_in_one_query_fall_back_to_the_source() {
    let query = indoc! {r#"
        measures:
          - activity.amount
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: week
            dateRange:
              - "2024-12-23"
              - "2025-01-12"
          - dimension: activity.cohort_date
            granularity: month
        order:
          - id: activity.cohort_date
    "#};
    if let Some((rollup, source)) = fallback_vs_source(query, "week_and_month").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn day_and_month_in_one_query_read_the_day_column() {
    let query = indoc! {r#"
        measures:
          - activity.amount
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: day
            dateRange:
              - "2025-01-30"
              - "2025-02-02"
          - dimension: activity.cohort_date
            granularity: month
        order:
          - id: activity.cohort_date
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(query, "month_then_day").await {
        insta::assert_snapshot!(result);
    }
}

// The raw read takes the raw column, the date filter the day column.
#[tokio::test(flavor = "multi_thread")]
async fn raw_dimension_beside_its_grains_reads_the_raw_column() {
    let query = indoc! {r#"
        measures:
          - activity.amount
        dimensions:
          - activity.cohort_date
        filters:
          - member: activity.cohort_date
            operator: inDateRange
            values:
              - "2025-01-30"
              - "2025-02-02"
        order:
          - id: activity.cohort_date
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "raw_and_grains").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(query, "raw_and_grains").await {
        insta::assert_snapshot!(result);
    }
}

fn raw_dimension_query(extra: &str) -> String {
    let query = indoc! {"
        measures:
          - activity.count
          - activity.amount
        dimensions:
          - activity.app_name
          - activity.cohort_date
        order:
          - id: activity.cohort_date
          - id: activity.app_name
    "};
    format!("{query}{extra}")
}

const DAY_WITH_DATE_RANGE: &str = indoc! {r#"
    time_dimensions:
      - dimension: activity.cohort_date
        granularity: day
        dateRange:
          - "2024-12-28"
          - "2025-01-03"
"#};

#[tokio::test(flavor = "multi_thread")]
async fn raw_dimension_listed_beside_its_time_dimension_reads_the_raw_column() {
    let query = raw_dimension_query("");
    if let Some((rollup, source)) = rollup_vs_source(&query, "day_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "day_with_raw_dimension").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_dimension_and_its_day_time_dimension_read_their_own_columns() {
    let query = raw_dimension_query(DAY_WITH_DATE_RANGE);
    if let Some((rollup, source)) = rollup_vs_source(&query, "day_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "day_with_raw_dimension").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_dimension_and_its_time_dimension_in_a_non_utc_timezone() {
    for convert_tz in [false, true] {
        let query = formatdoc! {"
            {}timezone: Asia/Kamchatka
            convert_tz_for_raw_time_dimension: {convert_tz}
        ", raw_dimension_query(DAY_WITH_DATE_RANGE)};
        if let Some((rollup, source)) = rollup_vs_source(&query, "day_with_raw_dimension").await {
            assert_eq!(
                rollup, source,
                "convert_tz_for_raw_time_dimension: {convert_tz}"
            );
        }
    }
}

// A coarser grain column must not stand in for the raw value.
#[tokio::test(flavor = "multi_thread")]
async fn raw_dimension_beside_a_month_grain_reads_the_raw_column() {
    let query = raw_dimension_query("");
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn equality_filter_on_the_raw_dimension_reads_the_raw_column() {
    let query = raw_dimension_query(indoc! {r#"
        filters:
          - member: activity.cohort_date
            operator: equals
            values:
              - "2025-01-05T13:00:00.000"
    "#});
    for pre_agg in ["day_with_raw_dimension", "month_with_raw_dimension"] {
        if let Some((rollup, source)) = rollup_vs_source(&query, pre_agg).await {
            assert_eq!(rollup, source, "{pre_agg}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ungrouped_raw_dimension_reads_the_raw_column() {
    let query = indoc! {r#"
        dimensions:
          - activity.id
          - activity.cohort_date
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: day
            dateRange:
              - "2025-01-04"
              - "2025-01-06"
        order:
          - id: activity.id
        ungrouped: true
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "rows_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn day_query_with_the_raw_dimension_stored_matches_the_source() {
    let query = query("day", "2024-12-28", "2025-01-03");
    if let Some((rollup, source)) = rollup_vs_source(&query, "raw_and_grains").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "raw_and_grains").await {
        insta::assert_snapshot!(result);
    }
}

// Grain columns are stored in the query timezone, so a raw dimension stored
// beside them must not take over their reads.
#[tokio::test(flavor = "multi_thread")]
async fn day_query_in_a_non_utc_timezone_reads_the_day_column() {
    let query = formatdoc! {"
        {}timezone: Asia/Kamchatka
    ", query("day", "2024-12-28", "2025-01-03")};
    if let Some((rollup, source)) = rollup_vs_source(&query, "raw_and_grains").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "raw_and_grains").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn include_window_without_granularity_reads_the_day_column() {
    let query = indoc! {r#"
        measures:
          - activity.amount_early_january
        dimensions:
          - activity.app_name
        time_dimensions:
          - dimension: activity.cohort_date
            dateRange:
              - "2025-01-01"
              - "2025-01-31"
        order:
          - id: activity.app_name
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(query, "month_then_day").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn include_windows_of_different_alignment_in_one_month_query() {
    let query = indoc! {r#"
        measures:
          - activity.amount_all_january
          - activity.amount_early_january
        time_dimensions:
          - dimension: activity.cohort_date
            granularity: month
        order:
          - id: activity.cohort_date
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(query, "month_then_day").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn include_window_ending_on_a_whole_second_reads_the_day_column() {
    let query = indoc! {r#"
        measures:
          - activity.amount_january_to_last_second
        dimensions:
          - activity.app_name
        order:
          - id: activity.app_name
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "all_grains").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(query, "all_grains").await {
        insta::assert_snapshot!(result);
    }
}

fn raw_filter(operator: &str, values: &[&str]) -> String {
    let values = values
        .iter()
        .map(|v| format!("\n              - \"{v}\""))
        .collect::<String>();
    formatdoc! {r#"
        measures:
          - activity.count
          - activity.amount
        dimensions:
          - activity.app_name
        filters:
          - member: activity.cohort_date
            operator: {operator}
            values:{values}
        order:
          - id: activity.app_name
    "#}
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_date_range_aligned_with_the_grain_reads_its_column() {
    let query = raw_filter("inDateRange", &["2024-12-01", "2025-01-31"]);
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "month_with_raw_dimension").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_date_range_excluded_on_the_grain_reads_its_column() {
    let query = raw_filter("notInDateRange", &["2024-12-01", "2025-01-31"]);
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "month_with_raw_dimension").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_single_bounds_on_the_grain_read_its_column() {
    for (operator, at) in [
        ("beforeDate", "2025-01-01"),
        ("afterOrOnDate", "2025-01-01"),
        ("beforeOrOnDate", "2024-12-31"),
        ("afterDate", "2024-12-31"),
    ] {
        let query = raw_filter(operator, &[at]);
        if let Some((rollup, source)) = rollup_vs_source(&query, "month_with_raw_dimension").await {
            assert_eq!(rollup, source, "{operator} {at}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_set_filter_is_served_by_the_rollup() {
    let query = raw_filter("set", &[]);
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
}

// Bounds off the grain are checked against the raw column, in the database
// timezone whatever the query's.
#[tokio::test(flavor = "multi_thread")]
async fn raw_date_bounds_off_the_grain_read_the_raw_column() {
    for (operator, values) in [
        ("inDateRange", vec!["2025-01-04", "2025-01-06"]),
        ("notInDateRange", vec!["2025-01-04", "2025-01-06"]),
        ("beforeDate", vec!["2025-01-15"]),
        ("afterOrOnDate", vec!["2025-01-15"]),
        ("beforeOrOnDate", vec!["2025-01-15"]),
        ("afterDate", vec!["2025-01-15"]),
        ("afterDate", vec!["2025-01-15T11:30:00"]),
    ] {
        for timezone in ["UTC", "Asia/Kamchatka", "America/Los_Angeles"] {
            let query = format!("{}timezone: {timezone}\n", raw_filter(operator, &values));
            if let Some((rollup, source)) =
                rollup_vs_source(&query, "month_with_raw_dimension").await
            {
                assert_eq!(rollup, source, "{operator} {values:?} {timezone}");
            }
        }
    }
    // Bounds a non-UTC query can't take at all.
    for (operator, values) in [
        ("beforeDate", vec!["2025-01-15T00:00:00Z"]),
        ("beforeDate", vec!["2025-01-15", "2025-01-20"]),
        ("afterDate", vec!["2025-01-15T00:00:00.000+05:00"]),
    ] {
        let query = raw_filter(operator, &values);
        if let Some((rollup, source)) = rollup_vs_source(&query, "month_with_raw_dimension").await {
            assert_eq!(rollup, source, "{operator} {values:?}");
        }
    }
    let query = raw_filter("afterDate", &["2025-01-15T11:30:00"]);
    if let Some(result) = on_cubestore(&query, "month_with_raw_dimension").await {
        insta::assert_snapshot!(result);
    }
}

// The member is stored only raw: its date filter takes database-timezone bounds.
#[tokio::test(flavor = "multi_thread")]
async fn date_filter_on_a_raw_only_member_in_a_non_utc_timezone() {
    for timezone in ["Asia/Kamchatka", "America/Los_Angeles"] {
        let query = formatdoc! {"
            {}timezone: {timezone}
        ", raw_filter("inDateRange", &["2025-01-04", "2025-01-06"])};
        if let Some((rollup, source)) = rollup_vs_source(&query, "raw_only").await {
            assert_eq!(rollup, source, "{timezone}");
        }
        if let Some(result) = on_cubestore(&query, "raw_only").await {
            insta::assert_snapshot!(format!("raw_only_{timezone}").replace('/', "_"), result);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_date_range_in_a_non_utc_timezone_reads_the_grain_column() {
    let query = formatdoc! {"
        {}timezone: America/Los_Angeles
    ", raw_filter("inDateRange", &["2024-12-01", "2025-01-31"])};
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "month_with_raw_dimension").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_date_range_reads_the_finest_grain_aligned_with_it() {
    let query = raw_filter("inDateRange", &["2025-01-04", "2025-01-06"]);
    if let Some((rollup, source)) = rollup_vs_source(&query, "raw_and_grains").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(&query, "raw_and_grains").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn include_window_on_a_quarter_reads_the_quarter_column() {
    let query = indoc! {r#"
        measures:
          - activity.amount_q4_2024
        dimensions:
          - activity.app_name
        order:
          - id: activity.app_name
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "quarter_only").await {
        assert_eq!(rollup, source);
    }
    if let Some(result) = on_cubestore(query, "quarter_only").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_date_range_on_a_quarter_reads_the_quarter_column() {
    let query = raw_filter("inDateRange", &["2024-10-01", "2024-12-31"]);
    if let Some((rollup, source)) = rollup_vs_source(&query, "quarter_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn date_range_without_granularity_on_a_quarter_reads_the_quarter_column() {
    let query = indoc! {r#"
        measures:
          - activity.count
          - activity.amount
        dimensions:
          - activity.app_name
        time_dimensions:
          - dimension: activity.cohort_date
            dateRange:
              - "2024-10-01"
              - "2025-03-31"
        order:
          - id: activity.app_name
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, "quarter_only").await {
        assert_eq!(rollup, source);
    }
}

// The range's end falls on a Monday that is also the first of a month.
#[tokio::test(flavor = "multi_thread")]
async fn raw_week_range_ending_before_a_month_start_reads_the_week_column() {
    let query = raw_filter("inDateRange", &["2024-06-03", "2024-06-30"]);
    if let Some((rollup, source)) = rollup_vs_source(&query, "week_with_raw_dimension").await {
        assert_eq!(rollup, source);
    }
}

// Origin stepping in absolute time puts the half-day grid at 23:00 and 11:00
// in December, an hour off the wall-clock buckets the rollup stores.
#[tokio::test(flavor = "multi_thread")]
async fn date_range_on_an_hourly_grain_across_dst_falls_back() {
    let query = indoc! {r#"
        measures:
          - activity.count
          - activity.amount
        dimensions:
          - activity.app_name
        time_dimensions:
          - dimension: activity.cohort_date
            dateRange:
              - "2024-12-04T23:00:00.000"
              - "2024-12-05T10:59:59.999"
        order:
          - id: activity.app_name
        timezone: America/New_York
    "#};
    if let Some((rollup, source)) = fallback_vs_source(query, "half_day_only").await {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_date_bounds_off_the_grain_read_the_raw_column_on_a_non_strict_rollup() {
    for (operator, values) in [
        ("inDateRange", vec!["2025-01-04", "2025-01-06"]),
        ("beforeDate", vec!["2025-01-15"]),
    ] {
        let query = raw_filter(operator, &values);
        if let Some((rollup, source)) =
            rollup_vs_source(&query, "non_strict_month_with_raw_dimension").await
        {
            assert_eq!(rollup, source, "{operator} {values:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_date_range_aligned_with_the_grain_reads_it_on_a_non_strict_rollup() {
    let query = raw_filter("inDateRange", &["2024-12-01", "2025-01-31"]);
    if let Some((rollup, source)) =
        rollup_vs_source(&query, "non_strict_month_with_raw_dimension").await
    {
        assert_eq!(rollup, source);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn date_filter_on_a_member_built_from_the_raw_dimension_reads_the_raw_column() {
    let query = indoc! {r#"
        measures:
          - activity.amount
        dimensions:
          - activity.app_name
        filters:
          - member: activity.cohort_date_shifted
            operator: inDateRange
            values:
              - "2025-01-06"
              - "2025-01-06"
        order:
          - id: activity.app_name
    "#};
    for timezone in ["UTC", "Asia/Kamchatka"] {
        let query = format!("{query}timezone: {timezone}\n");
        if let Some((rollup, source)) = rollup_vs_source(&query, "day_with_raw_dimension").await {
            assert_eq!(rollup, source, "{timezone}");
        }
    }
}

// Without its raw column the member's date filter has nothing to read.
#[tokio::test(flavor = "multi_thread")]
async fn date_filter_on_a_member_built_from_a_grain_only_dimension_falls_back() {
    let query = indoc! {r#"
        measures:
          - activity.amount
        dimensions:
          - activity.app_name
        filters:
          - member: activity.cohort_date_shifted
            operator: inDateRange
            values:
              - "2025-01-06"
              - "2025-01-06"
        order:
          - id: activity.app_name
    "#};
    if let Some((rollup, source)) = fallback_vs_source(query, "all_grains").await {
        assert_eq!(rollup, source);
    }
}

// Bounds on a stored grain are checked against that grain column, in the
// query timezone, as a time dimension's range would be.
#[tokio::test(flavor = "multi_thread")]
async fn date_filter_on_a_grain_only_member_reads_the_grain_column() {
    for (operator, values) in [
        ("inDateRange", vec!["2024-12-01", "2025-01-31"]),
        ("notInDateRange", vec!["2024-12-01", "2025-01-31"]),
        ("inDateRange", vec!["2025-01-04", "2025-01-06"]),
        ("notInDateRange", vec!["2025-01-04", "2025-01-06"]),
        ("beforeDate", vec!["2025-01-01"]),
        ("afterOrOnDate", vec!["2025-01-01"]),
        ("beforeOrOnDate", vec!["2024-12-31"]),
        ("afterDate", vec!["2024-12-31"]),
    ] {
        for timezone in ["UTC", "Asia/Kamchatka", "America/Los_Angeles"] {
            let query = format!("{}timezone: {timezone}\n", raw_filter(operator, &values));
            if let Some((rollup, source)) = rollup_vs_source(&query, "all_grains").await {
                assert_eq!(rollup, source, "{operator} {values:?} {timezone}");
            }
        }
    }
    let query = format!(
        "{}timezone: Asia/Kamchatka\n",
        raw_filter("inDateRange", &["2025-01-04", "2025-01-06"])
    );
    if let Some(result) = on_cubestore(&query, "all_grains").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn date_filter_off_the_grain_of_a_grain_only_member_falls_back() {
    for (pre_agg, operator, values) in [
        (
            "quarter_only",
            "inDateRange",
            vec!["2024-12-01", "2025-01-31"],
        ),
        ("all_grains", "afterDate", vec!["2025-01-15T11:30:00"]),
        (
            "all_grains",
            "inDateRange",
            vec!["2025-01-04T12:00:00", "2025-01-06"],
        ),
        ("week_and_month", "beforeDate", vec!["2025-01-15"]),
    ] {
        let query = raw_filter(operator, &values);
        if let Some((rollup, source)) = fallback_vs_source(&query, pre_agg).await {
            assert_eq!(rollup, source, "{pre_agg} {operator} {values:?}");
        }
    }
}

// The filter and the time dimension read one column, so it is the grain both
// can be served from.
#[tokio::test(flavor = "multi_thread")]
async fn date_filter_and_time_dimension_on_a_grain_only_member_read_one_column() {
    let with_filter = |granularity: &str, from: &str, to: &str| {
        formatdoc! {r#"
            measures:
              - activity.count
              - activity.amount
            time_dimensions:
              - dimension: activity.cohort_date
                granularity: {granularity}
            filters:
              - member: activity.cohort_date
                operator: inDateRange
                values:
                  - "{from}"
                  - "{to}"
            order:
              - id: activity.cohort_date
        "#}
    };
    let query = with_filter("month", "2025-01-04", "2025-01-06");
    if let Some((rollup, source)) = rollup_vs_source(&query, "month_then_day").await {
        assert_eq!(rollup, source);
    }
    // Neither the week nor the month column serves both reads.
    let query = with_filter("week", "2024-12-01", "2025-01-31");
    if let Some((rollup, source)) = fallback_vs_source(&query, "week_and_month").await {
        assert_eq!(rollup, source);
    }
}

fn filter_clause(pre_agg: &str, query: &str) -> String {
    let (sql, usages) = ctx_with(&[pre_agg])
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        usages.iter().any(|u| u.name() == pre_agg),
        "expected the query to be served by `{pre_agg}`; SQL:\n{sql}"
    );
    sql.lines()
        .find(|line| line.starts_with("WHERE"))
        .unwrap_or_else(|| panic!("no WHERE in:\n{sql}"))
        .to_string()
}

fn assert_reads(clause: &str, column: &str) {
    assert!(
        clause.contains(&format!("\"{column}\"")),
        "expected the filter to read {column}: {clause}"
    );
}

// The raw column stays out of the way of a filter its grains hold, so storing
// it adds reads without changing the ones a grain served.
#[tokio::test(flavor = "multi_thread")]
async fn date_filter_on_a_grain_reads_it_beside_the_raw_column() {
    for (operator, values) in [
        ("inDateRange", vec!["2024-12-01", "2025-01-31"]),
        ("notInDateRange", vec!["2024-12-01", "2025-01-31"]),
        ("beforeDate", vec!["2025-01-01"]),
        ("afterOrOnDate", vec!["2025-01-01"]),
        ("beforeOrOnDate", vec!["2024-12-31"]),
        ("afterDate", vec!["2024-12-31"]),
    ] {
        for timezone in ["UTC", "Asia/Kamchatka", "America/Los_Angeles"] {
            let query = format!("{}timezone: {timezone}\n", raw_filter(operator, &values));
            assert_reads(
                &filter_clause("raw_week_and_month", &query),
                "activity__cohort_date_month",
            );
            if let Some((rollup, source)) = rollup_vs_source(&query, "raw_week_and_month").await {
                assert_eq!(rollup, source, "{operator} {values:?} {timezone}");
            }
        }
    }
    let query = formatdoc! {r#"
        measures:
          - activity.count
        filters:
          - member: activity.cohort_date
            operator: inDateRange
            values:
              - "2024-12-01"
              - "2025-01-31"
    "#};
    assert_eq!(
        filter_clause("raw_week_and_month", &query),
        filter_clause("week_and_month", &query)
    );
    let query = format!(
        "{}timezone: Asia/Kamchatka\n",
        raw_filter("inDateRange", &["2024-12-01", "2025-01-31"])
    );
    if let Some(result) = on_cubestore(&query, "raw_week_and_month").await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn date_filter_off_every_grain_reads_the_raw_column() {
    for (operator, values) in [
        ("inDateRange", vec!["2025-01-04", "2025-01-06"]),
        ("afterDate", vec!["2025-01-15T11:30:00"]),
    ] {
        for timezone in ["UTC", "Asia/Kamchatka", "America/Los_Angeles"] {
            let query = format!("{}timezone: {timezone}\n", raw_filter(operator, &values));
            assert_reads(
                &filter_clause("raw_week_and_month", &query),
                "activity__cohort_date",
            );
            if let Some((rollup, source)) = rollup_vs_source(&query, "raw_week_and_month").await {
                assert_eq!(rollup, source, "{operator} {values:?} {timezone}");
            }
        }
    }
}

// The month read keeps only the month column, which the week-aligned filter
// does not fall on.
#[tokio::test(flavor = "multi_thread")]
async fn date_filter_on_a_grain_the_time_dimension_drops_reads_the_raw_column() {
    for timezone in ["UTC", "Asia/Kamchatka", "America/Los_Angeles"] {
        let query = formatdoc! {r#"
            measures:
              - activity.count
              - activity.amount
            time_dimensions:
              - dimension: activity.cohort_date
                granularity: month
            filters:
              - member: activity.cohort_date
                operator: inDateRange
                values:
                  - "2025-01-06"
                  - "2025-01-19"
            order:
              - id: activity.cohort_date
            timezone: {timezone}
        "#};
        assert_reads(
            &filter_clause("raw_week_and_month", &query),
            "activity__cohort_date",
        );
        if let Some((rollup, source)) = rollup_vs_source(&query, "raw_week_and_month").await {
            assert_eq!(rollup, source, "{timezone}");
        }
    }
}

// A member computed from the stored one reads its columns, so its range takes
// the timezone of the column it lands on.
#[tokio::test(flavor = "multi_thread")]
async fn date_range_on_a_member_computed_from_the_stored_one() {
    for pre_agg in ["week_and_month", "raw_week_and_month"] {
        for timezone in ["UTC", "America/Los_Angeles", "Asia/Kamchatka"] {
            let query = formatdoc! {r#"
                measures:
                  - activity.count
                  - activity.amount
                time_dimensions:
                  - dimension: activity.cohort_date_alias
                    granularity: month
                    dateRange:
                      - "2025-01-01"
                      - "2025-01-31"
                timezone: "{timezone}"
            "#};
            if let Some((rollup, source)) = rollup_vs_source(&query, pre_agg).await {
                assert_eq!(rollup, source, "{pre_agg} {timezone}");
            }
        }
    }
}
