//! A calendar-shifted measure stored in a rollup. Its build runs the shifted
//! join itself, so a query reads the result back with no join and no calendar
//! mapping left to apply — unlike a rollup storing only the unshifted measure,
//! over which the shift cannot be applied at all.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_calendar_stored_shift_tables.sql";
const YAML: &str = "common/integration_calendar_stored_shift.yaml";
const SOURCE_TABLES: [&str; 2] = ["cal_ss_sales", "cal_ss_dates"];

fn ctx_with(pre_aggs: &[&str]) -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
}

/// The query must be answered by `pre_agg` alone, reading no source table, and
/// agree with the same query run without any pre-aggregation.
async fn served_vs_source(query: &str, pre_agg: &str) -> Option<(String, String)> {
    let with_rollup = ctx_with(&[pre_agg]);
    let (sql, usages) = with_rollup
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        !usages.is_empty() && usages.iter().all(|usage| usage.name() == pre_agg),
        "expected the query to be served by {}; SQL:\n{}",
        pre_agg,
        sql
    );
    for table in SOURCE_TABLES {
        assert!(
            !sql.contains(table),
            "expected no source table in the SQL, found {}:\n{}",
            table,
            sql
        );
    }
    let rollup = with_rollup.try_execute_pg(query, SEED).await?;
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source))
}

/// The mirror of `served_vs_source`: the query has to go to the source as a
/// whole and, having done so, agree with it.
async fn fallback_vs_source(query: &str, pre_agg: &str) -> Option<(String, String)> {
    let with_rollup = ctx_with(&[pre_agg]);
    let (sql, usages) = with_rollup
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        usages.is_empty(),
        "expected the query to fall back to the source; SQL:\n{}",
        sql
    );
    let rollup = with_rollup.try_execute_pg(query, SEED).await?;
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source))
}

/// The value of `column` in the row whose cells match every `(column, value)`
/// pair of `key`.
fn cell(result: &str, key: &[(&str, &str)], column: &str) -> String {
    let mut lines = result.lines();
    let header: Vec<&str> = lines.next().unwrap().split('|').map(str::trim).collect();
    let index = |name: &str| {
        header
            .iter()
            .position(|h| *h == name)
            .unwrap_or_else(|| panic!("no column {} in:\n{}", name, result))
    };
    lines
        .skip(1)
        .map(|line| line.split('|').map(str::trim).collect::<Vec<_>>())
        .find(|row| key.iter().all(|(name, value)| row[index(name)] == *value))
        .map(|row| row[index(column)].to_string())
        .unwrap_or_else(|| panic!("no row {:?} in:\n{}", key, result))
}

/// Week 53 of the long retail year compares to the prior year's week 52, the
/// same prior rows week 52 reads: the build's join puts them in both weeks, as
/// the source does.
#[tokio::test(flavor = "multi_thread")]
async fn prior_year_stored_by_retail_week_is_served_from_the_rollup() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly
        dimensions:
          - sales.store
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        order:
          - id: retail_calendar.retail_date
          - id: sales.store
    "#};
    if let Some((rollup, source)) = served_vs_source(query, "sales_ly_by_week").await {
        assert_eq!(rollup, source);
        let ly_of_week = |week: &str| {
            cell(
                &rollup,
                &[
                    ("retail_calendar__retail_date_week", week),
                    ("sales__store", "north"),
                ],
                "sales__amount_ly",
            )
        };
        assert_eq!(
            ly_of_week("2007-01-21 00:00:00"),
            ly_of_week("2007-01-28 00:00:00")
        );
    }
}

/// Rolled up by retail year, the prior year of the long year counts the prior
/// week 52 twice, so it exceeds that year's own total: the stored value keeps
/// the calendar's mapping rather than reading the previous year's row.
#[tokio::test(flavor = "multi_thread")]
async fn prior_year_stored_by_retail_year_is_served_from_the_rollup() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly
        dimensions:
          - sales.store
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: year
        order:
          - id: retail_calendar.retail_date
          - id: sales.store
    "#};
    if let Some((rollup, source)) = served_vs_source(query, "sales_ly_by_year").await {
        assert_eq!(rollup, source);
        let north = |year: &str, column: &str| {
            cell(
                &rollup,
                &[
                    ("retail_calendar__retail_date_year", year),
                    ("sales__store", "north"),
                ],
                column,
            )
            .parse::<i64>()
            .unwrap()
        };
        assert!(
            north("2006-01-29 00:00:00", "sales__amount_ly")
                > north("2005-01-30 00:00:00", "sales__amount")
        );
    }
}

/// A filter on a calendar attribute selects reporting rows, and the rollup
/// stores that attribute of the reporting row. Retail 2007 follows the long
/// year, so its prior year lies 371 days back rather than 364.
#[tokio::test(flavor = "multi_thread")]
async fn prior_year_filtered_by_a_stored_calendar_attribute_matches_source() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly
        dimensions:
          - sales.store
          - retail_calendar.retail_year
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        filters:
          - member: retail_calendar.retail_year
            operator: equals
            values:
              - "2007"
        order:
          - id: retail_calendar.retail_date
          - id: sales.store
    "#};
    if let Some((rollup, source)) = served_vs_source(query, "sales_ly_by_week_and_year").await {
        assert_eq!(rollup, source);
    }
}

/// The range applies to the reporting weeks the rollup is keyed by, across the
/// boundary where the offset changes from 364 to 371 days.
#[tokio::test(flavor = "multi_thread")]
async fn prior_year_over_a_date_range_matches_source() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly
        dimensions:
          - sales.store
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
            dateRange:
              - "2007-01-07"
              - "2007-02-17"
        order:
          - id: retail_calendar.retail_date
          - id: sales.store
    "#};
    if let Some((rollup, source)) = served_vs_source(query, "sales_ly_by_week_non_strict").await {
        assert_eq!(rollup, source);
    }
}

/// A multi-stage measure is not additive, so it is served only at the grain
/// it was stored at: summing it over a stored dimension is refused.
#[tokio::test(flavor = "multi_thread")]
async fn stored_prior_year_is_not_rolled_up_over_a_dimension() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        order:
          - id: retail_calendar.retail_date
    "#};
    if let Some((rollup, source)) = fallback_vs_source(query, "sales_ly_by_week").await {
        assert_eq!(rollup, source);
    }
}

/// Nor is it rolled up to a coarser granularity.
#[tokio::test(flavor = "multi_thread")]
async fn stored_prior_year_is_not_rolled_up_to_a_coarser_grain() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly
        dimensions:
          - sales.store
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: year
        order:
          - id: retail_calendar.retail_date
          - id: sales.store
    "#};
    if let Some((rollup, source)) = fallback_vs_source(query, "sales_ly_by_week").await {
        assert_eq!(rollup, source);
    }
}

/// Control: storing only the unshifted measure leaves the shift to be applied
/// on top of the rollup, which a calendar mapping cannot be.
#[tokio::test(flavor = "multi_thread")]
async fn prior_year_over_an_unshifted_rollup_falls_back() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly
        dimensions:
          - sales.store
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        order:
          - id: retail_calendar.retail_date
          - id: sales.store
    "#};
    if let Some((rollup, source)) = fallback_vs_source(query, "sales_by_week").await {
        assert_eq!(rollup, source);
    }
}
