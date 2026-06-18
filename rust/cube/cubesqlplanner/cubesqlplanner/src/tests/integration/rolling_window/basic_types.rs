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

    assert!(
        sql.contains(r#""orders".created_at >= $_0_$::timestamptz"#),
        "Leading unbounded should have lower time bound (>=), got: {sql}"
    );
    assert!(
        !sql.contains(r#"created_at <= $_"#),
        "Leading unbounded should not have an upper time bound (<=), got: {sql}"
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

// FIXME: Bounded rolling window (trailing: 3 day, leading: 1 day) without granularity
// currently ignores the trailing/leading intervals entirely and uses the raw dateRange
// as a plain WHERE filter: created_at >= Jan 10 AND created_at <= Jan 20, producing 830.
// Expected behavior: the intervals should expand the date range, so the effective window
// becomes [Jan 10 - 3d .. Jan 20 + 1d] = [Jan 7 .. Jan 21], which would include
// order ID4 (200, Jan 8) and produce a larger result.
// The correct expected value depends on the chosen anchor semantics (start-of-range vs
// end-of-range), but the current behavior of silently discarding the intervals is wrong.
#[ignore]
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

    assert!(
        !sql.contains("time_series"),
        "Without granularity should not reference time_series CTE, got: {sql}"
    );
    assert!(
        sql.contains(r#"created_at >= $_0_$::timestamptz"#)
            && sql.contains(r#"created_at <= $_1_$::timestamptz"#),
        "Should use parameterized date range on created_at, got: {sql}"
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

// FIXME: Trailing bounded rolling window (trailing: 7 day) without granularity currently
// ignores the trailing interval and uses the raw dateRange as a plain WHERE filter:
// created_at >= Jan 10 AND created_at <= Jan 20, producing 830 (sum of orders in
// [Jan 10..Jan 20]).
// Expected behavior: the trailing interval should expand the lower bound of the date
// range, so the effective window becomes [Jan 10 - 7d .. Jan 20] = [Jan 3 .. Jan 20],
// which would include orders ID2 (45, Jan 3), ID3 (75, Jan 4), ID4 (200, Jan 8) and
// produce 1150 (830 + 45 + 75 + 200).
#[ignore]
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
