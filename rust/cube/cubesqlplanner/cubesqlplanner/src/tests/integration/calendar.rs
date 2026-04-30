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
