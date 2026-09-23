use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_calendar_pre_agg_tables.sql";
const YAML: &str = "common/integration_calendar_pre_agg.yaml";

fn ctx_with(pre_aggs: &[&str]) -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
}

/// Runs the query twice against the same source rows: once served by the named
/// pre-aggregations (materialized in Postgres by the harness) and once with no
/// pre-aggregation at all. A rollup that cannot answer the query is a failure
/// of the test's premise, not a pass.
async fn rollup_vs_source(query: &str, pre_aggs: &[&str]) -> Option<(String, String)> {
    let with_rollup = ctx_with(pre_aggs);
    let (_, usages) = with_rollup
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        !usages.is_empty(),
        "expected the query to be served by a pre-aggregation"
    );
    let rollup = with_rollup.try_execute_pg(query, SEED).await?;
    let source = ctx_with(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source))
}

/// The mirror of `rollup_vs_source` for shapes a rollup must NOT answer: the
/// query has to fall back to the source and, having done so, agree with it.
async fn fallback_vs_source(query: &str, pre_aggs: &[&str]) -> Option<(String, String)> {
    let with_rollup = ctx_with(pre_aggs);
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

fn query(measure: &str, time_dimension: &str, granularity: &str) -> String {
    format!(
        indoc! {r#"
            measures:
              - demand.net_demand_a
              - {}
            time_dimensions:
              - dimension: {}
                granularity: {}
                dateRange:
                  - "2025-02-09"
                  - "2025-02-11"
            order:
              - id: {}
        "#},
        measure, time_dimension, granularity, time_dimension
    )
}

/// A prior-year shift declared as an interval that the calendar maps to its
/// own `prev_year_date` column. The mapping lives in the calendar's table, so
/// a rollup that does not materialize it cannot answer the stage at all and
/// the query goes to the source.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_interval_shift_falls_back() {
    let query = query(
        "demand.net_demand_a_ly",
        "retail_calendar.retail_date",
        "day",
    );
    if let Some((rollup, source)) =
        fallback_vs_source(&query, &["demand_by_item_hierarchies1"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// The same shift addressed by the name the calendar declares it under, which
/// reaches the mapping through a different branch of `extract_time_shifts`.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_named_shift_falls_back() {
    let query = query(
        "demand.net_demand_a_ly_named",
        "retail_calendar.retail_date",
        "day",
    );
    if let Some((rollup, source)) =
        fallback_vs_source(&query, &["demand_by_item_hierarchies1"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// The calendar's primary key as the query's own time dimension: the shift
/// then rewrites the projection too, so it is a no-op semantically — but the
/// rollup still cannot produce it.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_pk_shift_falls_back() {
    let query = query("demand.net_demand_a_ly", "retail_calendar.date_val", "day");
    if let Some((rollup, source)) =
        fallback_vs_source(&query, &["demand_by_item_hierarchies1"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// A granularity the calendar overrides with `sql` cannot be assembled out of
/// the day buckets a finer rollup stores.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_sql_granularity_falls_back_to_source() {
    let query = query(
        "demand.net_demand_a_ly",
        "retail_calendar.retail_date",
        "week",
    );
    if let Some((rollup, source)) =
        fallback_vs_source(&query, &["demand_by_item_hierarchies1"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// Control: the same shift on the fact table's own date, where the stored
/// column really can carry it.
#[tokio::test(flavor = "multi_thread")]
async fn fact_date_shift_from_rollup_matches_source() {
    let query = query("demand.net_demand_a_ly", "demand.demand_date", "day");
    if let Some((rollup, source)) =
        rollup_vs_source(&query, &["demand_by_item_hierarchies_fact_date"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// Control: no shift in play, so the same rollup answers the calendar query
/// correctly.
#[tokio::test(flavor = "multi_thread")]
async fn unshifted_measure_from_calendar_rollup_matches_source() {
    let query = indoc! {r#"
        measures:
          - demand.net_demand_a
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: day
            dateRange:
              - "2025-02-09"
              - "2025-02-11"
        order:
          - id: retail_calendar.retail_date
    "#};
    if let Some((rollup, source)) = rollup_vs_source(query, &["demand_by_item_hierarchies1"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// A joined cube that is NOT a calendar, carrying the same declarations. The
/// shift here is an interval the stored column really can be offset by, so the
/// rollup answers it the same way the source does.
#[tokio::test(flavor = "multi_thread")]
async fn plain_joined_cube_shift_from_rollup_matches_source() {
    let query = query("demand.net_demand_a_ly", "plain_dates.plain_date", "day");
    if let Some((rollup, source)) = rollup_vs_source(&query, &["demand_by_plain_date"]).await {
        assert_eq!(rollup, source);
    }
}

/// The same `sql`-overridden granularity on a cube that is not a calendar.
#[tokio::test(flavor = "multi_thread")]
async fn plain_joined_cube_sql_granularity_falls_back_to_source() {
    let query = query("demand.net_demand_a", "plain_dates.plain_date", "week");
    if let Some((rollup, source)) = fallback_vs_source(&query, &["demand_by_plain_date"]).await {
        assert_eq!(rollup, source);
    }
}

/// A `sql`-overridden granularity on the fact cube's own time dimension, with
/// no joined cube and no time shift anywhere in the query.
#[tokio::test(flavor = "multi_thread")]
async fn own_cube_sql_granularity_falls_back_to_source() {
    let query = query("demand.net_demand_a", "demand.demand_date", "week");
    if let Some((rollup, source)) =
        fallback_vs_source(&query, &["demand_by_item_hierarchies_fact_date"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// A rollup that declares the `sql`-overridden granularity itself stores the
/// overriding column's value, so it is a correct source for it. Cube cannot
/// verify that a range falls on the overriding column's boundaries — it knows
/// only the interval and a default origin — so serving a range at all takes
/// `allow_non_strict_date_range_match`. The range used here does align: the
/// overridden week starts Sunday 2025-02-02.
#[tokio::test(flavor = "multi_thread")]
async fn sql_granularity_rollup_declaring_it_matches_source() {
    let query = indoc! {r#"
        measures:
          - demand.net_demand_a
        time_dimensions:
          - dimension: plain_dates.plain_date
            granularity: week
            dateRange:
              - "2025-02-02"
              - "2025-02-15"
        order:
          - id: plain_dates.plain_date
    "#};
    if let Some((rollup, source)) =
        rollup_vs_source(query, &["demand_by_plain_week_non_strict"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// Without that opt-in the same rollup must not answer a range whose alignment
/// with the stored weeks cannot be established.
#[tokio::test(flavor = "multi_thread")]
async fn sql_granularity_rollup_refuses_unaligned_range() {
    let query = query("demand.net_demand_a", "plain_dates.plain_date", "week");
    if let Some((rollup, source)) = fallback_vs_source(&query, &["demand_by_plain_week"]).await {
        assert_eq!(rollup, source);
    }
}

/// An interval the calendar declares no mapping for is still applied to its
/// primary key, so the rows arrive through the shifted join just the same and
/// no stored column stands in for them.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_undeclared_interval_shift_falls_back() {
    let query = query(
        "demand.net_demand_a_prev_week",
        "retail_calendar.retail_date",
        "day",
    );
    if let Some((rollup, source)) =
        fallback_vs_source(&query, &["demand_by_item_hierarchies1"]).await
    {
        assert_eq!(rollup, source);
    }
}
/// A rollup keeping one dimension at several granularities has a column per
/// granularity but is addressed by the member alone, so one of them stands for
/// all. A `sql` granularity is read straight from the column, so the wrong one
/// would silently return the wrong period — the rollup is refused instead.
/// The fixture declares `week` before `day` on purpose: that is the order in
/// which the surviving column is the wrong one.
#[tokio::test(flavor = "multi_thread")]
async fn sql_granularity_rollup_with_several_grains_falls_back() {
    let query = indoc! {r#"
        measures:
          - demand.net_demand_a
        time_dimensions:
          - dimension: plain_dates.plain_date
            granularity: week
            dateRange:
              - "2025-02-02"
              - "2025-02-15"
        order:
          - id: plain_dates.plain_date
    "#};
    if let Some((rollup, source)) =
        fallback_vs_source(query, &["demand_by_plain_day_and_week"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// The same rollup with `day` declared first leaves the `sql` week's column
/// standing for both, so a day query would read retail weeks: refused too.
#[tokio::test(flavor = "multi_thread")]
async fn plain_grain_beside_a_sql_granularity_falls_back() {
    let query = indoc! {r#"
        measures:
          - demand.net_demand_a
        time_dimensions:
          - dimension: plain_dates.plain_date
            granularity: day
            dateRange:
              - "2025-02-02"
              - "2025-02-15"
        order:
          - id: plain_dates.plain_date
    "#};
    if let Some((rollup, source)) =
        fallback_vs_source(query, &["demand_by_plain_week_after_day"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// A stored `sql` week is not a bucket of the default calendar, so no coarser
/// default granularity can be derived from it: a retail week straddling a
/// month boundary would land entirely in the month it starts in.
#[tokio::test(flavor = "multi_thread")]
async fn sql_granularity_is_not_rolled_up_to_a_coarser_default() {
    let query = indoc! {r#"
        measures:
          - demand.net_demand_a
        time_dimensions:
          - dimension: plain_dates.plain_date
            granularity: month
            dateRange:
              - "2025-02-01"
              - "2025-03-31"
        order:
          - id: plain_dates.plain_date
    "#};
    if let Some((rollup, source)) =
        fallback_vs_source(query, &["demand_by_plain_week_non_strict"]).await
    {
        assert_eq!(rollup, source);
    }
}
