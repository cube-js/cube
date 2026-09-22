use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const SEED: &str = "integration_calendar_pre_agg_tables.sql";
const YAML: &str = "common/integration_calendar_pre_agg.yaml";

fn ctx_with(pre_aggs: &[&str]) -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(pre_aggs)).unwrap()
}

/// The same fact and calendar, modelled so a calendar shift can be served
/// without reading anything live: the calendar has its own rollup and a
/// `rollupJoin` ties the two together.
const YAML_ROLLUP_JOIN: &str = "common/integration_calendar_rollup_join.yaml";

/// A `rollupJoin` is ephemeral — it is served by the rollups it names, so
/// those have to stay in the schema alongside it.
fn ctx_rollup_join(pre_aggs: &[&str]) -> TestContext {
    let referenced = [
        "demand_rollup",
        "calendar_rollup",
        "calendar_rollup_no_shift_column",
    ];
    let names = pre_aggs
        .iter()
        .copied()
        .chain(referenced)
        .collect::<Vec<_>>();
    TestContext::new(MockSchema::from_yaml_file(YAML_ROLLUP_JOIN).only_pre_aggregations(&names))
        .unwrap()
}

/// Runs the query against the rollup-join model with and without its
/// pre-aggregations, the way `rollup_vs_source` does for the plain model.
async fn rollup_join_vs_source(query: &str, pre_aggs: &[&str]) -> Option<(String, String, String)> {
    let with_rollup = ctx_rollup_join(pre_aggs);
    let (sql, usages) = with_rollup
        .build_sql_with_used_pre_aggregations(query)
        .unwrap();
    assert!(
        !usages.is_empty(),
        "expected the query to be served by a pre-aggregation; SQL:\n{}",
        sql
    );
    let rollup = with_rollup.try_execute_pg(query, SEED).await?;
    let source = ctx_rollup_join(&[]).try_execute_pg(query, SEED).await?;
    Some((rollup, source, sql))
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

/// The third retail year maps back 364 days where the second maps back 371, so
/// one rollup answering both is only possible by reading the calendar.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_shift_served_from_rollups_in_a_second_retail_year() {
    let query = indoc! {r#"
        measures:
          - demand.net_demand_a
          - demand.net_demand_a_ly
        time_dimensions:
          - dimension: retail_calendar.retail_date
            granularity: day
            dateRange:
              - "2026-02-08"
              - "2026-02-10"
        order:
          - id: retail_calendar.retail_date
    "#};
    if let Some((rollup, source, _)) = rollup_join_vs_source(query, &["demand_with_calendar"]).await
    {
        assert_eq!(rollup, source);
    }
}

/// Rule 2: everything the shift reads is materialized, so the whole query is
/// answered from rollup tables. Grouped by a non-primary-key calendar
/// dimension on purpose — with the primary key the shift rewrites the
/// projection too and becomes a no-op that would pass without proving
/// anything.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_shift_served_entirely_from_rollups() {
    let query = query(
        "demand.net_demand_a_ly",
        "retail_calendar.retail_date",
        "day",
    );
    if let Some((rollup, source, sql)) =
        rollup_join_vs_source(&query, &["demand_with_calendar"]).await
    {
        assert_eq!(rollup, source);
        for source_table in ["cal_pa_dates", "cal_pa_demand"] {
            assert!(
                !sql.contains(source_table),
                "query must read rollups only, but names {}; SQL:\n{}",
                source_table,
                sql
            );
        }
    }
}

/// The shift reads a column the calendar's rollup does not store, so there is
/// no way to apply it without the calendar itself: the query falls back.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_shift_rollup_join_without_shift_column_falls_back() {
    let query = query(
        "demand.net_demand_a_ly",
        "retail_calendar.retail_date",
        "day",
    );
    let ctx = ctx_rollup_join(&["demand_with_calendar_no_shift_column"]);
    let (sql, usages) = ctx.build_sql_with_used_pre_aggregations(&query).unwrap();
    assert!(
        usages.is_empty(),
        "expected the query to fall back to the source; SQL:\n{}",
        sql
    );
    let rollup = ctx.try_execute_pg(&query, SEED).await;
    let source = ctx_rollup_join(&[]).try_execute_pg(&query, SEED).await;
    assert_eq!(rollup, source);
}

/// The range a usage carries prunes partitions, so it has to describe the rows
/// the rendered filter asks for. A calendar maps the period through its own
/// table, which no band derived from the reporting one reproduces, so the
/// shifted stage must carry no range at all rather than the range the user typed.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_shift_stage_prunes_no_partitions() {
    let query = query(
        "demand.net_demand_a_ly",
        "retail_calendar.retail_date",
        "day",
    );
    let ctx = ctx_rollup_join(&["demand_with_calendar"]);
    let (sql, usages) = ctx.build_sql_with_used_pre_aggregations(&query).unwrap();
    assert!(!usages.is_empty(), "expected the rollup to be read");

    let reporting_range = Some((
        "2025-02-09T00:00:00.000".to_string(),
        "2025-02-11T23:59:59.999".to_string(),
    ));
    assert!(
        usages.iter().any(|usage| usage.date_range.is_none()),
        "the shifted stage must prune no partitions; SQL:\n{}",
        sql
    );
    assert!(
        usages
            .iter()
            .all(|usage| usage.date_range.is_none() || usage.date_range == reporting_range),
        "an unshifted stage may only be pruned to the reporting range; SQL:\n{}",
        sql
    );
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

/// The same shift, executed against a live CubeStore. Nothing but rollup
/// tables exists there, so this is what proves the query needs no source
/// table rather than merely omitting one from the SQL. Both retail years are
/// snapshotted: the second maps back 371 days and the third 364, so the
/// numbers show the calendar being read rather than an interval applied.
#[tokio::test(flavor = "multi_thread")]
async fn calendar_shift_runs_on_cubestore() {
    let schema = MockSchema::from_yaml_file(YAML_ROLLUP_JOIN).only_pre_aggregations(&[
        "demand_with_calendar",
        "demand_rollup",
        "calendar_rollup",
    ]);
    let ctx = TestContext::new_with_external_cubestore(schema).unwrap();

    for (name, range) in [
        (
            "calendar_shift_runs_on_cubestore",
            ("2025-02-09", "2025-02-11"),
        ),
        (
            "calendar_shift_runs_on_cubestore_second_year",
            ("2026-02-08", "2026-02-10"),
        ),
    ] {
        let query = format!(
            indoc! {r#"
                measures:
                  - demand.net_demand_a
                  - demand.net_demand_a_ly
                time_dimensions:
                  - dimension: retail_calendar.retail_date
                    granularity: day
                    dateRange:
                      - "{}"
                      - "{}"
                order:
                  - id: retail_calendar.retail_date
            "#},
            range.0, range.1
        );
        if let Some(result) = ctx.try_execute_cubestore(&query, SEED).await {
            insta::assert_snapshot!(name, result);
        }
    }
}
