use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
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
