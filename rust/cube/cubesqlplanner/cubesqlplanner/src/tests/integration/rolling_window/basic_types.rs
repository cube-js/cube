use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_trailing_unbounded_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for trailing unbounded");

    assert!(
        sql.contains(r#""orders".created_at <= $_0_$::timestamptz"#),
        "Trailing unbounded should have upper time bound (<=), got: {sql}"
    );
    assert!(
        !sql.contains(r#"created_at >= $_"#),
        "Trailing unbounded should not have a lower time bound (>=), got: {sql}"
    );
    assert!(
        !sql.contains("time_series"),
        "Without granularity should not reference time_series CTE, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leading_unbounded_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_leading
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for leading unbounded");

    // Default offset is 'end', so the window anchors at the range end: everything
    // strictly after `to`.
    assert!(
        sql.contains(r#""orders".created_at > $_0_$::timestamptz"#),
        "Leading unbounded should have strict lower time bound (> to), got: {sql}"
    );
    assert!(
        !sql.contains("created_at >= "),
        "Leading unbounded (offset end) should not use inclusive lower bound, got: {sql}"
    );
    assert!(
        !sql.contains("created_at <"),
        "Leading unbounded should not have an upper time bound, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_both_unbounded_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_both
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for both unbounded");

    assert!(
        sql.contains(r#""orders".amount"#),
        "Should reference the measure column, got: {sql}"
    );
    assert!(
        !sql.contains("WHERE"),
        "Both unbounded should not have WHERE clause, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trailing_unbounded_with_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for trailing unbounded with granularity");

    assert!(
        sql.contains("time_series"),
        "With granularity should reference time_series CTE, got: {sql}"
    );
    assert!(
        !sql.contains(r#">= "time_series"."date_from""#),
        "JOIN should not have lower bound with trailing unbounded, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bounded_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_bounded
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for bounded rolling window");

    // Default offset 'end' anchors at the range end: the trailing interval shifts
    // the (strict) lower bound back, the leading interval shifts the (inclusive)
    // upper bound forward.
    assert!(
        !sql.contains("time_series"),
        "Without granularity should not reference time_series CTE, got: {sql}"
    );
    assert!(
        sql.contains("- interval '3 day'"),
        "Should subtract trailing interval '3 day', got: {sql}"
    );
    assert!(
        sql.contains("+ interval '1 day'"),
        "Should add leading interval '1 day', got: {sql}"
    );
    assert!(
        sql.contains("created_at > ") && !sql.contains("created_at >= "),
        "Should have strict lower bound (> to - trailing), got: {sql}"
    );
    assert!(
        sql.contains("created_at <= "),
        "Should have inclusive upper bound (<= to + leading), got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bounded_with_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_bounded
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for bounded rolling window with granularity");

    assert!(
        sql.contains("- interval '3 day'"),
        "Should subtract trailing interval '3 day', got: {sql}"
    );
    assert!(
        sql.contains("+ interval '1 day'"),
        "Should add leading interval '1 day', got: {sql}"
    );
    assert!(
        sql.contains("AT TIME ZONE 'UTC'"),
        "Should apply convert_tz, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trailing_bounded_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for trailing bounded rolling window");

    // Default offset 'end' anchors at the range end: trailing shifts the (strict)
    // lower bound back; with no leading the upper bound is the inclusive range end.
    assert!(
        sql.contains("- interval '7 day'"),
        "Should subtract trailing interval '7 day', got: {sql}"
    );
    assert!(
        !sql.contains("+ interval"),
        "Should not have a leading interval, got: {sql}"
    );
    assert!(
        sql.contains("created_at > ") && !sql.contains("created_at >= "),
        "Should have strict lower bound (> to - trailing), got: {sql}"
    );
    assert!(
        sql.contains("created_at <= "),
        "Should have inclusive upper bound (<= to), got: {sql}"
    );
    assert!(
        !sql.contains("time_series"),
        "Without granularity should not reference time_series CTE, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trailing_bounded_with_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_7d
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for trailing bounded with granularity");

    assert!(
        sql.contains("- interval '7 day'"),
        "Should subtract trailing interval '7 day', got: {sql}"
    );
    assert!(
        !sql.contains("+ interval"),
        "Should not have leading interval (only trailing), got: {sql}"
    );
    assert!(
        sql.contains("AT TIME ZONE 'UTC'"),
        "Should apply convert_tz, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_to_date_with_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_to_date
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-10"
              - "2024-01-20"
    "#};

    let sql = ctx
        .build_sql(query)
        .expect("Should generate SQL for to_date rolling window");

    assert!(
        sql.contains("date_trunc('month'"),
        "To_date should apply month granularity truncation, got: {sql}"
    );
    assert!(
        sql.contains("AT TIME ZONE 'UTC'"),
        "Should apply convert_tz, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
