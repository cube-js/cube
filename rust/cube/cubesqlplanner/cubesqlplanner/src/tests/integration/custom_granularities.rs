use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_custom_granularity.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_custom_granularity_tables.sql";

fn create_origin_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_custom_granularity_origin.yaml");
    TestContext::new(schema).unwrap()
}

const ORIGIN_SEED: &str = "integration_custom_granularity_origin_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_half_year_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_half_year_by_1st_april_granularity() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year_by_1st_april
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fiscal_year_with_offset() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: fiscal_year
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_half_year_with_dimension() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - \"2024-01-01\"
              - \"2024-12-31\"
        order:
          - id: orders.created_at
          - id: orders.status
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_half_year_with_sum_measure() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_time_alias_wraps_compound_exprs_before_tz_cast() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
        dimensions:
          - orders.fiscal_year_alias
          - orders.created_at_minus_one_day
        order:
          - id: orders.fiscal_year_alias
        convert_tz_for_raw_time_dimension: true
    "};

    let sql = ctx.build_sql(query).unwrap();

    // Two independent precedence-trap fingerprints — `::` latching onto a
    // trailing interval literal instead of the wrapped composed expression.
    for bad in [
        "interval '1 month'::timestamptz",
        "interval '1 day'::timestamptz",
    ] {
        assert!(
            !sql.contains(bad),
            "cast-precedence trap detected: {bad}\nFull SQL:\n{sql}"
        );
    }

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fiscal_year_by_origin_does_not_group_by_calendar_year() {
    let ctx = create_origin_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: fiscal_year_by_1st_april
            dateRange:
              - \"2024-04-01\"
              - \"2026-03-31\"
        order:
          - id: orders.created_at
    "};

    if let Some(result) = ctx.try_execute_pg(query, ORIGIN_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_calendar_year_by_origin_still_groups_by_calendar_year() {
    let ctx = create_origin_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: calendar_year_by_origin
            dateRange:
              - \"2024-01-01\"
              - \"2025-12-31\"
        order:
          - id: orders.created_at
    "};

    if let Some(result) = ctx.try_execute_pg(query, ORIGIN_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_monthly_by_mid_month_origin() {
    let ctx = create_origin_context();

    let query = indoc! {"
        measures:
          - orders.count
        time_dimensions:
          - dimension: orders.created_at
            granularity: monthly_from_15th
            dateRange:
              - \"2024-12-15\"
              - \"2025-04-14\"
        order:
          - id: orders.created_at
    "};

    if let Some(result) = ctx.try_execute_pg(query, ORIGIN_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fiscal_year_by_origin_with_time_shift_measure() {
    let ctx = create_origin_context();

    // The shape from the original report: a time-shift measure alongside the plain one, both
    // grouped by a fiscal-year grain. The shifted leaf has to bin on the same origin as the
    // unshifted one, or the two sides join on incompatible buckets.
    let query = indoc! {"
        measures:
          - orders.total_amount
          - orders.total_amount_prior_year
        time_dimensions:
          - dimension: orders.created_at
            granularity: fiscal_year_by_1st_april
            dateRange:
              - \"2024-04-01\"
              - \"2026-03-31\"
        order:
          - id: orders.created_at
    "};

    if let Some(result) = ctx.try_execute_pg(query, ORIGIN_SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_granularity_with_daterange_filter() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - orders.count
          - orders.total_amount
        time_dimensions:
          - dimension: orders.created_at
            granularity: half_year
            dateRange:
              - \"2024-01-01\"
              - \"2024-06-30\"
        order:
          - id: orders.created_at
    "};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
