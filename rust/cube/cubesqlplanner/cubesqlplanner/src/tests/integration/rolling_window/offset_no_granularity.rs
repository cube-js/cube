use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_rolling_window.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_rolling_window_tables.sql";

// Rolling window measures queried without a granularity (dateRange only). The
// whole range is a single window anchored by `offset`: 'start' anchors at the
// range start (`from`), 'end' at the range end (`to`). The seed has orders before
// (January) and after (March) the queried February range, which is what makes the
// start/end anchors produce different aggregates.

#[tokio::test(flavor = "multi_thread")]
async fn test_trailing_unbounded_offset_start_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_offset_start
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-02-01"
              - "2024-02-29"
    "#};

    let sql = ctx.build_sql(query).expect("Should generate SQL");

    // offset start + trailing unbounded => everything strictly before the range start.
    assert!(
        sql.contains(r#""orders".created_at < $_0_$::timestamptz"#),
        "Should have strict upper bound at range start (< from), got: {sql}"
    );
    assert!(
        !sql.contains("created_at <= "),
        "Should not use inclusive upper bound, got: {sql}"
    );
    assert!(
        !sql.contains("created_at >"),
        "Should not have a lower bound, got: {sql}"
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
async fn test_trailing_unbounded_offset_end_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_trailing_offset_end
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-02-01"
              - "2024-02-29"
    "#};

    let sql = ctx.build_sql(query).expect("Should generate SQL");

    // offset end + trailing unbounded => everything up to and including the range end.
    assert!(
        sql.contains(r#""orders".created_at <= $_0_$::timestamptz"#),
        "Should have inclusive upper bound at range end (<= to), got: {sql}"
    );
    assert!(
        !sql.contains("created_at >"),
        "Should not have a lower bound, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leading_unbounded_offset_start_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_leading_offset_start
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-02-01"
              - "2024-02-29"
    "#};

    let sql = ctx.build_sql(query).expect("Should generate SQL");

    // offset start + leading unbounded => everything from the range start onward.
    assert!(
        sql.contains(r#""orders".created_at >= $_0_$::timestamptz"#),
        "Should have inclusive lower bound at range start (>= from), got: {sql}"
    );
    assert!(
        !sql.contains("created_at <"),
        "Should not have an upper bound, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leading_unbounded_offset_end_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_leading_offset_end
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-02-01"
              - "2024-02-29"
    "#};

    let sql = ctx.build_sql(query).expect("Should generate SQL");

    // offset end + leading unbounded => everything strictly after the range end.
    assert!(
        sql.contains(r#""orders".created_at > $_0_$::timestamptz"#),
        "Should have strict lower bound at range end (> to), got: {sql}"
    );
    assert!(
        !sql.contains("created_at >= "),
        "Should not use inclusive lower bound, got: {sql}"
    );
    assert!(
        !sql.contains("created_at <"),
        "Should not have an upper bound, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trailing_finite_offset_start_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_7d_offset_start
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-10"
              - "2024-01-16"
    "#};

    let sql = ctx.build_sql(query).expect("Should generate SQL");

    // offset start + trailing 7 day => [from - 7 day, from): the trailing interval
    // shifts the lower bound; the upper bound is the strict range start.
    assert!(
        sql.contains("7 day"),
        "Should apply the trailing interval, got: {sql}"
    );
    assert!(
        sql.contains(r#""orders".created_at < $"#),
        "Should have strict upper bound at range start (< from), got: {sql}"
    );
    assert!(
        !sql.contains("created_at <= "),
        "Should not use inclusive upper bound, got: {sql}"
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
async fn test_trailing_finite_offset_end_no_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_7d_offset_end
        time_dimensions:
          - dimension: orders.created_at
            dateRange:
              - "2024-01-10"
              - "2024-01-16"
    "#};

    let sql = ctx.build_sql(query).expect("Should generate SQL");

    // offset end + trailing 7 day => (to - 7 day, to]: the trailing interval shifts
    // the lower bound (strict); the upper bound is the inclusive range end.
    assert!(
        sql.contains("7 day"),
        "Should apply the trailing interval, got: {sql}"
    );
    assert!(
        sql.contains(r#""orders".created_at <= $"#),
        "Should have inclusive upper bound at range end (<= to), got: {sql}"
    );
    assert!(
        sql.contains("created_at > ") && !sql.contains("created_at >= "),
        "Should have strict lower bound (> to - interval), got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
