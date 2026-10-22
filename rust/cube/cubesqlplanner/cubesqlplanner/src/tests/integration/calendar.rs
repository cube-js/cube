use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{table_rows, TestContext};
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_calendar.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_calendar_tables.sql";

// --- Direct queries to calendar cube ---

#[tokio::test(flavor = "multi_thread")]
async fn test_non_pk_time_dimension_with_date_range() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - custom_calendar.retail_date
        time_dimensions:
          - dimension: custom_calendar.retail_date
            dateRange:
              - "2025-02-02"
              - "2025-02-06"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pk_time_dimension_with_date_range() {
    let ctx = create_context();

    let query = indoc! {r#"
        dimensions:
          - custom_calendar.date_val
        time_dimensions:
          - dimension: custom_calendar.date_val
            dateRange:
              - "2025-02-02"
              - "2025-02-06"
        order:
          - id: custom_calendar.date_val
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_non_pk_year_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: year
            dateRange:
              - "2025-02-02"
              - "2025-02-06"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pk_year_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: year
            dateRange:
              - "2025-02-02"
              - "2025-02-06"
        order:
          - id: custom_calendar.date_val
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Custom granularities ---

#[tokio::test(flavor = "multi_thread")]
async fn test_count_by_retail_year() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: year
            dateRange:
              - "2025-02-02"
              - "2026-02-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_count_by_retail_week() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: week
            dateRange:
              - "2025-02-02"
              - "2025-04-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_count_by_fortnight() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: fortnight
            dateRange:
              - "2025-02-02"
              - "2025-04-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Time shifts (non-PK dimension) ---

#[tokio::test(flavor = "multi_thread")]
async fn test_shifted_by_retail_year() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
          - calendar_orders.count_shifted_calendar_y
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: year
            dateRange:
              - "2025-02-02"
              - "2026-02-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shifted_by_retail_month() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
          - calendar_orders.count_shifted_calendar_m
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: month
            dateRange:
              - "2025-02-02"
              - "2026-02-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shifted_by_retail_week() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
          - calendar_orders.count_shifted_calendar_w
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: week
            dateRange:
              - "2025-02-02"
              - "2025-04-12"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shifted_by_named_year() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
          - calendar_orders.count_shifted_y_named
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: year
            dateRange:
              - "2025-02-02"
              - "2026-02-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shifted_by_named_common_interval() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
          - calendar_orders.count_shifted_y_named_common_interval
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: year
            dateRange:
              - "2025-02-02"
              - "2026-02-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_two_named_shifts() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
          - calendar_orders.count_shifted_y_named
          - calendar_orders.count_shifted_y_named_common_interval
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: year
            dateRange:
              - "2025-02-02"
              - "2026-02-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- nested interval shifts on a calendar dimension ---

// Characterisation, not a statement of intent.
//
// Nested interval shifts are added together before the calendar is consulted,
// and the calendar is then asked for one shift of the summed interval. It
// declares 1 month and 1 year but nothing for their sum, so the composition
// falls through to plain arithmetic on the calendar's primary key: the retail
// year is 364 days and its months are 4 or 5 weeks long, so a nominal
// `1 year 1 month` back is not where either mapping leads. Recorded here so
// the fall-through is visible; what it should do instead is not settled by
// this test.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_interval_shifts_fall_through_to_plain_arithmetic() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count
          - calendar_orders.count_shifted_calendar_m
          - calendar_orders.count_shifted_calendar_y
          - calendar_orders.count_shifted_m_then_y
        time_dimensions:
          - dimension: custom_calendar.retail_date
            granularity: year
            dateRange:
              - "2025-02-02"
              - "2026-02-01"
        order:
          - id: custom_calendar.retail_date
    "#};

    let sql = ctx.build_sql(query).unwrap();

    // The stage carrying both shifts offsets the primary key by their sum
    // instead of following either declaration.
    assert!(
        sql.contains("date_val + interval '-1 year -1 month'")
            || sql.contains("date_val - interval '1 year 1 month'"),
        "the composed shift is plain arithmetic on the primary key\nsql: {}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- to_date windows bounded by the calendar ---
//
// The retail calendar starts 2024-02-04 with 7-day weeks and 4-5-4 months, so
// month 1 runs 2024-02-04..2024-03-02, month 2 2024-03-03..2024-04-06 (35 days)
// and month 3 2024-04-07..2024-05-04. None of those lengths is reachable from a
// nominal interval anchored anywhere.

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_retail_week_by_day() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count_week_to_date
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: day
            dateRange:
              - "2024-02-08"
              - "2024-02-18"
        order:
          - id: custom_calendar.date_val
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_retail_month_by_day() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count_month_to_date
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: day
            dateRange:
              - "2024-02-29"
              - "2024-03-09"
        order:
          - id: custom_calendar.date_val
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A 5-week retail month grouped by itself: a nominal `1 month` upper bound
/// reaches past its end and folds the next month in.
#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_retail_month_by_retail_month() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count_month_to_date
          - calendar_orders.count
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: month
            dateRange:
              - "2024-02-04"
              - "2024-05-04"
        order:
          - id: custom_calendar.date_val
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The range ends exactly where the 5-week month does, so the point that bounds
/// it lies outside the range.
#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_range_ending_on_a_period_end() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count_month_to_date
          - calendar_orders.count
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: month
            dateRange:
              - "2024-03-03"
              - "2024-04-06"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A range opening mid-period still has to see the period it opens inside.
#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_range_opening_mid_period() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count_month_to_date
          - calendar_orders.count
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: month
            dateRange:
              - "2024-04-01"
              - "2024-04-20"
        order:
          - id: custom_calendar.date_val
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Both windows are driven by the same series, each by its own period: the
/// weekly one restarts on 2024-03-10 while the monthly one keeps accumulating.
#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_week_and_month_together() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - calendar_orders.count_week_to_date
          - calendar_orders.count_month_to_date
        time_dimensions:
          - dimension: custom_calendar.date_val
            granularity: day
            dateRange:
              - "2024-03-08"
              - "2024-03-16"
        order:
          - id: custom_calendar.date_val
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// --- Calendar granularities through a view ---

/// Cubes the granularity `sql` of `member` (a `cube.dimension.granularity`
/// path) refers to, through `{CUBE}` and through `{CUBE.member}` alike.
fn calendar_sql_cubes(ctx: &TestContext, member: &str) -> Vec<String> {
    let symbol = ctx.create_symbol(member).unwrap();
    let time_dimension = symbol.as_time_dimension().unwrap();
    let calendar_sql = time_dimension
        .granularity_obj()
        .as_ref()
        .and_then(|granularity| granularity.calendar_sql().clone())
        .expect("granularity defines no sql");
    let mut cubes = calendar_sql
        .get_cube_refs()
        .iter()
        .map(|cube_ref| cube_ref.cube_id().target().to_string())
        .chain(
            calendar_sql
                .get_dependencies()
                .iter()
                .map(|dep| dep.cube_id().target().to_string()),
        )
        .collect::<Vec<_>>();
    cubes.sort();
    cubes.dedup();
    cubes
}

/// Checks the view against the cube query and returns the view's result,
/// or `None` without a database.
async fn assert_view_matches_cubes(
    view_member: &str,
    view_query: &str,
    cube_query: &str,
) -> Option<String> {
    let ctx = create_context();

    assert_eq!(
        calendar_sql_cubes(&ctx, view_member),
        vec!["custom_calendar"]
    );

    ctx.build_sql(view_query).unwrap();
    ctx.build_sql(cube_query).unwrap();

    let view_result = ctx.try_execute_pg(view_query, SEED).await?;
    let cube_result = ctx.try_execute_pg(cube_query, SEED).await.unwrap();
    let cube_rows = table_rows(&cube_result);
    assert!(!cube_rows.is_empty(), "{cube_result}");
    assert_eq!(table_rows(&view_result), cube_rows, "{view_result}");
    Some(view_result)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_count_by_calendar_year() {
    if let Some(result) = assert_view_matches_cubes(
        "calendar_orders_view.custom_calendar_date_val.year",
        indoc! {r#"
            measures:
              - calendar_orders_view.count
            time_dimensions:
              - dimension: calendar_orders_view.custom_calendar_date_val
                granularity: year
            order:
              - id: calendar_orders_view.custom_calendar_date_val
        "#},
        indoc! {r#"
            measures:
              - calendar_orders.count
            time_dimensions:
              - dimension: custom_calendar.date_val
                granularity: year
            order:
              - id: custom_calendar.date_val
        "#},
    )
    .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_count_by_calendar_month() {
    if let Some(result) = assert_view_matches_cubes(
        "calendar_orders_view.custom_calendar_date_val.month",
        indoc! {r#"
            measures:
              - calendar_orders_view.count
            time_dimensions:
              - dimension: calendar_orders_view.custom_calendar_date_val
                granularity: month
            order:
              - id: calendar_orders_view.custom_calendar_date_val
        "#},
        indoc! {r#"
            measures:
              - calendar_orders.count
            time_dimensions:
              - dimension: custom_calendar.date_val
                granularity: month
            order:
              - id: custom_calendar.date_val
        "#},
    )
    .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_count_by_non_pk_calendar_year() {
    if let Some(result) = assert_view_matches_cubes(
        "calendar_orders_view.custom_calendar_retail_date.year",
        indoc! {r#"
            measures:
              - calendar_orders_view.count
            time_dimensions:
              - dimension: calendar_orders_view.custom_calendar_retail_date
                granularity: year
            order:
              - id: calendar_orders_view.custom_calendar_retail_date
        "#},
        indoc! {r#"
            measures:
              - calendar_orders.count
            time_dimensions:
              - dimension: custom_calendar.retail_date
                granularity: year
            order:
              - id: custom_calendar.retail_date
        "#},
    )
    .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_calendar_year_as_dimension() {
    if let Some(result) = assert_view_matches_cubes(
        "calendar_orders_view.custom_calendar_date_val.year",
        indoc! {r#"
            measures:
              - calendar_orders_view.count
            dimensions:
              - calendar_orders_view.custom_calendar_date_val.year
            order:
              - id: calendar_orders_view.custom_calendar_date_val.year
        "#},
        indoc! {r#"
            measures:
              - calendar_orders.count
            dimensions:
              - custom_calendar.date_val.year
            order:
              - id: custom_calendar.date_val.year
        "#},
    )
    .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_without_prefix_count_by_calendar_year() {
    if let Some(result) = assert_view_matches_cubes(
        "calendar_orders_view_no_prefix.date_val.year",
        indoc! {r#"
            measures:
              - calendar_orders_view_no_prefix.count
            time_dimensions:
              - dimension: calendar_orders_view_no_prefix.date_val
                granularity: year
            order:
              - id: calendar_orders_view_no_prefix.date_val
        "#},
        indoc! {r#"
            measures:
              - calendar_orders.count
            time_dimensions:
              - dimension: custom_calendar.date_val
                granularity: year
            order:
              - id: custom_calendar.date_val
        "#},
    )
    .await
    {
        insta::assert_snapshot!(result);
    }
}
