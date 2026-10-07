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

const BY_WEEK: &str = indoc! {r#"
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

const BY_YEAR: &str = indoc! {r#"
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

const FILTERED_BY_YEAR: &str = indoc! {r#"
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

const OVER_A_RANGE: &str = indoc! {r#"
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

/// Week 53 of the long retail year compares to the prior year's week 52, the
/// same prior rows week 52 reads: the build's join puts them in both weeks, as
/// the source does.
#[tokio::test(flavor = "multi_thread")]
async fn prior_year_stored_by_retail_week_is_served_from_the_rollup() {
    if let Some((rollup, source)) = served_vs_source(BY_WEEK, "sales_ly_by_week").await {
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
    if let Some((rollup, source)) = served_vs_source(BY_YEAR, "sales_ly_by_year").await {
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
    if let Some((rollup, source)) =
        served_vs_source(FILTERED_BY_YEAR, "sales_ly_by_week_and_year").await
    {
        assert_eq!(rollup, source);
    }
}

/// The range applies to the reporting weeks the rollup is keyed by, across the
/// boundary where the offset changes from 364 to 371 days.
#[tokio::test(flavor = "multi_thread")]
async fn prior_year_over_a_date_range_matches_source() {
    if let Some((rollup, source)) =
        served_vs_source(OVER_A_RANGE, "sales_ly_by_week_non_strict").await
    {
        assert_eq!(rollup, source);
    }
}

/// A time-shift proxy of an additive measure is additive: each stored row holds
/// the prior-year sum of its reporting days, so summing rows over a stored
/// dimension sums those days.
#[tokio::test(flavor = "multi_thread")]
async fn stored_prior_year_is_rolled_up_over_a_dimension() {
    if let Some((rollup, source)) = served_vs_source(BY_WEEK_ALL_STORES, "sales_ly_by_week").await {
        assert_eq!(rollup, source);
    }
}

/// A retail week does not nest in the `sql`-defined retail year as far as the
/// granularity hierarchy knows, so the rollup is not rolled up to it.
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

const BY_WEEK_ALL_STORES: &str = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        order:
          - id: retail_calendar.retail_date
"#};

const NAMED_BY_WEEK_ALL_STORES: &str = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly_named
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        order:
          - id: retail_calendar.retail_date
"#};

/// A named shift resolves to the same calendar declaration as the interval one
/// and rolls up the same way.
#[tokio::test(flavor = "multi_thread")]
async fn stored_named_prior_year_is_rolled_up_over_a_dimension() {
    if let Some((rollup, source)) =
        served_vs_source(NAMED_BY_WEEK_ALL_STORES, "sales_ly_named_by_week").await
    {
        assert_eq!(rollup, source);
    }
}

/// The build also landed the named shift on `retail_date_alt`, whose
/// declaration maps the calendar differently from `retail_date`'s, so the
/// stored value is not the one a query reading only `retail_date` computes.
#[tokio::test(flavor = "multi_thread")]
async fn stored_named_shift_resolved_differently_by_a_stored_member_falls_back() {
    if let Some((rollup, source)) =
        fallback_vs_source(NAMED_BY_WEEK_ALL_STORES, "sales_ly_named_by_week_with_alt").await
    {
        assert_eq!(rollup, source);
    }
}

/// Reading none of the shifted time members, the query applies no shift at
/// all, while the build shifted every stored row, so the stored shifted
/// column must not be read. The unshifted leaves still are.
#[tokio::test(flavor = "multi_thread")]
async fn stored_shift_without_its_time_member_is_not_read() {
    for (measure, pre_agg) in [
        ("amount_ly", "sales_ly_by_week"),
        ("amount_ly_named", "sales_ly_named_by_week"),
        ("amount_prev_year", "sales_prev_year_by_day"),
        ("max_amount_prev_month", "sales_prev_year_by_day"),
    ] {
        let query = format!(
            indoc! {r#"
                measures:
                  - sales.amount
                  - sales.{}
                dimensions:
                  - sales.store
                order:
                  - id: sales.store
            "#},
            measure
        );
        let with_rollup = ctx_with(&[pre_agg]);
        let (sql, _) = with_rollup
            .build_sql_with_used_pre_aggregations(&query)
            .unwrap();
        let reads_stored_column =
            regex::Regex::new(&format!(r#"\w+\("sales__{}"\)"#, measure)).unwrap();
        assert!(
            !reads_stored_column.is_match(&sql),
            "expected the stored shifted column to stay unread; SQL:\n{}",
            sql
        );
        let Some(rollup) = with_rollup.try_execute_pg(&query, SEED).await else {
            return;
        };
        let source = ctx_with(&[]).try_execute_pg(&query, SEED).await.unwrap();
        assert_eq!(rollup, source);
    }
}

/// A rollup built for a range holds the prior year of the labels in it,
/// which is not the range's own amount. A query reading no `sale_date`
/// applies no shift, so over that rollup the prior year must equal the
/// amount it reads alongside, both limited to the built range.
#[tokio::test(flavor = "multi_thread")]
async fn stored_interval_shift_without_its_time_member_over_a_build_range() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_prev_year
        dimensions:
          - sales.store
        order:
          - id: sales.store
    "#};
    let with_rollup = ctx_with(&["sales_prev_year_by_day_ranged"]);
    let Some(rollup) = with_rollup.try_execute_pg(query, SEED).await else {
        return;
    };
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await.unwrap();
    for store in ["north", "south"] {
        let amount = cell(&rollup, &[("sales__store", store)], "sales__amount");
        assert_eq!(
            cell(
                &rollup,
                &[("sales__store", store)],
                "sales__amount_prev_year"
            ),
            amount
        );
        // The range did cut the rollup, or the check above proves nothing.
        assert_ne!(
            amount,
            cell(&source, &[("sales__store", store)], "sales__amount")
        );
    }
}

/// A proxy of a rolling window stores one window per row, and windows of
/// neighbouring days overlap, so it is served only at the stored grain.
#[tokio::test(flavor = "multi_thread")]
async fn stored_proxy_of_a_rolling_window_is_not_rolled_up() {
    let at_stored_grain = indoc! {r#"
        measures:
          - sales.amount_7d_prev_year
        dimensions:
          - sales.store
        time_dimensions:
          - dimension: sales.sale_date
            granularity: day
            dateRange:
              - "2007-03-01"
              - "2007-03-14"
        order:
          - id: sales.sale_date
          - id: sales.store
    "#};
    if let Some((rollup, source)) =
        served_vs_source(at_stored_grain, "sales_rolling_prev_year_by_day").await
    {
        assert_eq!(rollup, source);
    }
    let by_month = indoc! {r#"
        measures:
          - sales.amount_7d_prev_year
        time_dimensions:
          - dimension: sales.sale_date
            granularity: month
            dateRange:
              - "2007-01-01"
              - "2007-06-30"
        order:
          - id: sales.sale_date
    "#};
    let with_rollup = ctx_with(&["sales_rolling_prev_year_by_day"]);
    let (sql, _) = with_rollup
        .build_sql_with_used_pre_aggregations(by_month)
        .unwrap();
    assert!(
        !sql.contains("\"sales__amount_7d_prev_year\")"),
        "expected the stored windows to stay unread; SQL:\n{}",
        sql
    );
    if let Some(rollup) = with_rollup.try_execute_pg(by_month, SEED).await {
        let source = ctx_with(&[]).try_execute_pg(by_month, SEED).await.unwrap();
        assert_eq!(rollup, source);
    }
}

/// The two proxies land a two-year shift together, which `retail_date_alt2`
/// declares differently from `retail_date`, although both agree on one year.
#[tokio::test(flavor = "multi_thread")]
async fn composed_shift_resolved_differently_by_a_stored_member_falls_back() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_2ly
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        order:
          - id: retail_calendar.retail_date
    "#};
    if let Some((rollup, source)) = fallback_vs_source(query, "sales_2ly_by_week_with_alt2").await {
        assert_eq!(rollup, source);
    }
}

/// An expression over the shifted measure is not a proxy of it, so it is
/// served only at the grain it was stored at.
#[tokio::test(flavor = "multi_thread")]
async fn stored_shifted_expression_is_not_rolled_up() {
    let rolled_up = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly_doubled
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        order:
          - id: retail_calendar.retail_date
    "#};
    if let Some((rollup, source)) = fallback_vs_source(rolled_up, "sales_ly_doubled_by_week").await
    {
        assert_eq!(rollup, source);
    }
    let exact = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_ly_doubled
        dimensions:
          - sales.store
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: week
        order:
          - id: retail_calendar.retail_date
          - id: sales.store
    "#};
    if let Some((rollup, source)) = served_vs_source(exact, "sales_ly_doubled_by_week").await {
        assert_eq!(rollup, source);
    }
}

/// Interval shifts over the fact's own time dimension, stored by day and read
/// by month across all stores: the sum rolls up by `sum`, the maximum by `max`.
#[tokio::test(flavor = "multi_thread")]
async fn stored_interval_shifts_are_rolled_up_to_a_coarser_grain() {
    let query = indoc! {r#"
        measures:
          - sales.amount
          - sales.amount_prev_year
          - sales.max_amount
          - sales.max_amount_prev_month
        time_dimensions:
          - dimension: sales.sale_date
            granularity: month
            dateRange:
              - "2006-01-01"
              - "2007-12-31"
        order:
          - id: sales.sale_date
    "#};
    if let Some((rollup, source)) = served_vs_source(query, "sales_prev_year_by_day").await {
        assert_eq!(rollup, source);
        let march_2007 = |column: &str| {
            cell(
                &rollup,
                &[("sales__sale_date_month", "2007-03-01 00:00:00")],
                column,
            )
        };
        assert_ne!(
            march_2007("sales__amount_prev_year"),
            march_2007("sales__amount")
        );
        assert_ne!(
            march_2007("sales__max_amount_prev_month"),
            march_2007("sales__max_amount")
        );
    }
}

/// A `type: number` proxy of a maximum rolls up by `max`, the kind of the
/// measure it reads, not by the `sum` its own type would pick.
#[tokio::test(flavor = "multi_thread")]
async fn stored_number_proxy_of_a_maximum_rolls_up_by_max() {
    let query = indoc! {r#"
        measures:
          - sales.max_amount
          - sales.max_amount_prev_year
        time_dimensions:
          - dimension: sales.sale_date
            granularity: month
            dateRange:
              - "2006-01-01"
              - "2007-12-31"
        order:
          - id: sales.sale_date
    "#};
    if let Some((rollup, source)) = served_vs_source(query, "sales_prev_year_by_day").await {
        assert_eq!(rollup, source);
    }
}

/// The build stores the HLL state of the measure the proxy reads, so the
/// stored column is merged, both at the stored grain and rolled up.
#[tokio::test(flavor = "multi_thread")]
async fn stored_proxy_of_an_approximate_distinct_count_merges_its_state() {
    let at_stored_grain = indoc! {r#"
        measures:
          - sales.approx_stores
          - sales.approx_stores_prev_year
        dimensions:
          - sales.store
        time_dimensions:
          - dimension: sales.sale_date
            granularity: day
            dateRange:
              - "2007-03-01"
              - "2007-03-07"
        order:
          - id: sales.sale_date
          - id: sales.store
    "#};
    let rolled_up = indoc! {r#"
        measures:
          - sales.approx_stores
          - sales.approx_stores_prev_year
        time_dimensions:
          - dimension: sales.sale_date
            granularity: month
            dateRange:
              - "2006-01-01"
              - "2007-12-31"
        order:
          - id: sales.sale_date
    "#};
    // The HLL merge is CubeStore's, so the rollup is read there.
    for query in [at_stored_grain, rolled_up] {
        let schema =
            MockSchema::from_yaml_file(YAML).only_pre_aggregations(&["sales_prev_year_by_day"]);
        let ctx = TestContext::new_with_external_cubestore(schema).unwrap();
        let (sql, usages) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
        assert!(
            usages.len() == 1 && !SOURCE_TABLES.iter().any(|table| sql.contains(table)),
            "expected the query to be served by the rollup alone; SQL:\n{}",
            sql
        );
        let Some(cubestore) = ctx.try_execute_cubestore(query, SEED).await else {
            return;
        };
        let source = ctx_with(&[]).try_execute_pg(query, SEED).await.unwrap();
        assert!(!rows(&source).is_empty());
        assert_eq!(rows(&cubestore), rows(&source));
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

/// The same stored rollups executed on a live CubeStore. The harness builds
/// each rollup in Postgres and uploads it, so this exercises only the query
/// side; the build is the one the Postgres tests cover.
#[tokio::test(flavor = "multi_thread")]
async fn stored_prior_year_runs_on_cubestore() {
    for (query, pre_agg) in [
        (BY_WEEK, "sales_ly_by_week"),
        (BY_YEAR, "sales_ly_by_year"),
        (FILTERED_BY_YEAR, "sales_ly_by_week_and_year"),
        (OVER_A_RANGE, "sales_ly_by_week_non_strict"),
    ] {
        let schema = MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[pre_agg]);
        let ctx = TestContext::new_with_external_cubestore(schema).unwrap();
        let Some(cubestore) = ctx.try_execute_cubestore(query, SEED).await else {
            return;
        };
        let source = ctx_with(&[]).try_execute_pg(query, SEED).await.unwrap();
        assert!(!rows(&source).is_empty(), "{}", pre_agg);
        assert_eq!(rows(&cubestore), rows(&source), "{}", pre_agg);
    }
}

/// Cells of a result table, with CubeStore's ISO timestamps in the form
/// Postgres prints them.
fn rows(result: &str) -> Vec<Vec<String>> {
    let iso = regex::Regex::new(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})\.000Z$").unwrap();
    result
        .lines()
        .filter(|line| !line.trim().is_empty())
        .skip(2)
        .map(|line| {
            line.split('|')
                .map(|value| iso.replace(value.trim(), "$1 $2").into_owned())
                .collect()
        })
        .collect()
}
