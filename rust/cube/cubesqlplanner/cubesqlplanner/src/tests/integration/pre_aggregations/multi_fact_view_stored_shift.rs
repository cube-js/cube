//! A named calendar shift stored in a rollup keyed by a three-level product
//! hierarchy, queried through a multi-fact view by the top level alone and
//! pinned to one day the way the SQL API renders `date_column = '<day>'`: a
//! time dimension with no granularity and a date range over a single instant.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_multi_fact_view_stored_shift_tables.sql";
const YAML: &str = "common/integration_multi_fact_view_stored_shift.yaml";
const PRE_AGG: &str = "demand_by_hierarchy";
const SOURCE_TABLES: [&str; 3] = ["mf_demand_lines", "mf_dates", "mf_products"];

fn ctx_with(pre_aggs: &[&str]) -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
}

fn assert_served(query: &str) {
    let (sql, usages) = ctx_with(&[PRE_AGG])
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        !usages.is_empty() && usages.iter().all(|usage| usage.name() == PRE_AGG),
        "expected the query to be served by {}; SQL:\n{}",
        PRE_AGG,
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
}

async fn rollup_and_source(query: &str) -> Option<(String, String)> {
    let rollup = ctx_with(&[PRE_AGG]).try_execute_pg(query, SEED).await?;
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source))
}

fn cell(result: &str, key: (&str, &str), column: &str) -> String {
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
        .find(|row| row[index(key.0)] == key.1)
        .map(|row| row[index(column)].to_string())
        .unwrap_or_else(|| panic!("no row {:?} in:\n{}", key, result))
}

const ONE_DAY_BY_CATEGORY: &str = indoc! {r#"
        measures:
          - mf_view.amount
          - mf_view.amount_ly
        dimensions:
          - mf_view.category_name
        time_dimensions:
          - dimension: mf_view.report_date
            dateRange:
              - "2026-06-23T00:00:00.000"
              - "2026-06-23T00:00:00.000"
        order:
          - id: mf_view.category_name
"#};

/// 2026-06-23 falls in the band mapped 371 days back, to 2025-06-17: day 167
/// of the seed. The `home` products are the first three, two lines each, so
/// the prior-year amount is 6 * 167 + 2 * (1000 + 2000 + 3000) + 3 * (1 + 2).
#[tokio::test(flavor = "multi_thread")]
async fn one_day_by_top_level_through_the_view_is_served_from_the_rollup() {
    assert_served(ONE_DAY_BY_CATEGORY);
    if let Some((rollup, source)) = rollup_and_source(ONE_DAY_BY_CATEGORY).await {
        assert_eq!(rollup, source);
        assert_eq!(
            cell(
                &rollup,
                ("mf_view__category_name", "home"),
                "mf_view__amount_ly"
            ),
            "13011"
        );
    }
}

/// The same question asked of the fact cube directly rather than the view.
#[tokio::test(flavor = "multi_thread")]
async fn one_day_by_top_level_on_the_cube_is_served_from_the_rollup() {
    let query = indoc! {r#"
        measures:
          - mf_demand.amount
          - mf_demand.amount_ly
        dimensions:
          - mf_product.category_name
        time_dimensions:
          - dimension: mf_calendar.report_date
            dateRange:
              - "2026-06-23T00:00:00.000"
              - "2026-06-23T00:00:00.000"
        order:
          - id: mf_product.category_name
    "#};
    assert_served(query);
    if let Some((rollup, source)) = rollup_and_source(query).await {
        assert_eq!(rollup, source);
    }
}

/// A week grouped by day and by the middle level of the hierarchy, across the
/// boundary where the mapping changes from 364 to 371 days.
#[tokio::test(flavor = "multi_thread")]
async fn days_across_the_mapping_change_by_middle_level_are_served_from_the_rollup() {
    let query = indoc! {r#"
        measures:
          - mf_view.amount
          - mf_view.amount_ly
        dimensions:
          - mf_view.subcategory_name
        time_dimensions:
          - dimension: mf_view.report_date
            granularity: day
            dateRange:
              - "2026-06-18"
              - "2026-06-24"
        order:
          - id: mf_view.report_date
          - id: mf_view.subcategory_name
    "#};
    assert_served(query);
    if let Some((rollup, source)) = rollup_and_source(query).await {
        assert_eq!(rollup, source);
    }
}

/// A distinct count stored next to the shifted measure does not roll up, so a
/// query asking for it at the top level alone goes to the source as a whole.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_count_alongside_keeps_the_query_on_the_source() {
    let query = indoc! {r#"
        measures:
          - mf_view.amount
          - mf_view.amount_ly
          - mf_view.order_count
        dimensions:
          - mf_view.category_name
        time_dimensions:
          - dimension: mf_view.report_date
            dateRange:
              - "2026-06-23T00:00:00.000"
              - "2026-06-23T00:00:00.000"
        order:
          - id: mf_view.category_name
    "#};
    let (sql, usages) = ctx_with(&[PRE_AGG])
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        usages.is_empty(),
        "expected the query to fall back to the source; SQL:\n{}",
        sql
    );
    if let Some((rollup, source)) = rollup_and_source(query).await {
        assert_eq!(rollup, source);
    }
}
