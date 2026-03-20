use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_trailing_unbounded_no_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val
        time_dimensions:
          - dimension: test_cube.created_at
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for trailing unbounded");

    assert!(
        sql.contains(r#""test_cube".created_at <= $_0_$::timestamptz"#),
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

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_leading_unbounded_no_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_leading
        time_dimensions:
          - dimension: test_cube.created_at
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for leading unbounded");

    assert!(
        sql.contains(r#""test_cube".created_at >= $_0_$::timestamptz"#),
        "Leading unbounded should have lower time bound (>=), got: {sql}"
    );
    assert!(
        !sql.contains(r#"created_at <= $_"#),
        "Leading unbounded should not have an upper time bound (<=), got: {sql}"
    );

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_both_unbounded_no_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_both
        time_dimensions:
          - dimension: test_cube.created_at
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for both unbounded");

    assert!(
        sql.contains(r#""test_cube".val"#),
        "Should reference the measure column, got: {sql}"
    );
    assert!(
        !sql.contains("WHERE"),
        "Both unbounded should not have WHERE clause, got: {sql}"
    );

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_trailing_unbounded_with_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val
        time_dimensions:
          - dimension: test_cube.created_at
            granularity: day
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for trailing unbounded with granularity");

    assert!(
        sql.contains("time_series"),
        "With granularity should reference time_series CTE, got: {sql}"
    );
    assert!(
        !sql.contains(r#">= "time_series"."date_from""#),
        "JOIN should not have lower bound with trailing unbounded, got: {sql}"
    );

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_bounded_no_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_bounded
        time_dimensions:
          - dimension: test_cube.created_at
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for bounded rolling window");

    assert!(
        !sql.contains("time_series"),
        "Without granularity should not reference time_series CTE, got: {sql}"
    );
    assert!(
        sql.contains(r#"created_at >= $_0_$::timestamptz"#)
            && sql.contains(r#"created_at <= $_1_$::timestamptz"#),
        "Should use parameterized date range on created_at, got: {sql}"
    );

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_bounded_with_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_bounded
        time_dimensions:
          - dimension: test_cube.created_at
            granularity: day
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
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

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_trailing_bounded_no_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_trailing_bounded
        time_dimensions:
          - dimension: test_cube.created_at
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for trailing bounded rolling window");

    // Without granularity, trailing bounded falls back to plain inDateRange
    assert!(
        sql.contains(r#"created_at >= $_0_$::timestamptz"#)
            && sql.contains(r#"created_at <= $_1_$::timestamptz"#),
        "Should use parameterized date range on created_at, got: {sql}"
    );
    assert!(
        !sql.contains("time_series"),
        "Without granularity should not reference time_series CTE, got: {sql}"
    );
    assert!(
        !sql.contains("interval"),
        "Without granularity should not have interval arithmetic, got: {sql}"
    );

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_trailing_bounded_with_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_trailing_bounded
        time_dimensions:
          - dimension: test_cube.created_at
            granularity: day
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
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

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_to_date_with_granularity() {
    let test_context = create_context();

    let query_yaml = indoc! {r#"
        measures:
          - test_cube.val_to_date
        time_dimensions:
          - dimension: test_cube.created_at
            granularity: day
            dateRange:
              - "2025-10-07"
              - "2025-10-08"
    "#};

    let sql = test_context
        .build_sql(query_yaml)
        .expect("Should generate SQL for to_date rolling window");

    assert!(
        sql.contains("date_trunc('month'"),
        "To_date should apply month granularity truncation, got: {sql}"
    );
    assert!(
        sql.contains("AT TIME ZONE 'UTC'"),
        "Should apply convert_tz, got: {sql}"
    );

    if let Some(result) = test_context
        .try_execute_pg(query_yaml, "rolling_window_tables.sql")
        .await
    {
        insta::assert_snapshot!(result);
    }
}
