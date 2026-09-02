use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage_grain_rolling.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_grain_rolling_tables.sql";

/// Splits the pipe-separated result table into header + rows of trimmed cells.
fn parse_table(result: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let mut lines = result.lines();
    let header = lines
        .next()
        .expect("result has no header")
        .split('|')
        .map(|c| c.trim().to_string())
        .collect::<Vec<_>>();
    lines.next();
    let rows = lines
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split('|').map(|c| c.trim().to_string()).collect())
        .collect();
    (header, rows)
}

fn assert_measures(result: &str, expected: &[(&str, Vec<f64>)]) {
    let (header, rows) = parse_table(result);
    for (column, values) in expected {
        let idx = header
            .iter()
            .position(|h| h == column)
            .unwrap_or_else(|| panic!("column `{column}` not found in:\n{result}"));
        assert_eq!(
            rows.len(),
            values.len(),
            "unexpected row count for `{column}` in:\n{result}"
        );
        for (row_index, expected_value) in values.iter().enumerate() {
            let actual: f64 = rows[row_index][idx].parse().unwrap_or_else(|_| {
                panic!(
                    "`{column}` row {row_index} is not a number: `{}`\n{result}",
                    rows[row_index][idx]
                )
            });
            assert!(
                (actual - expected_value).abs() < 1e-9,
                "`{column}` row {row_index}: expected {expected_value}, got {actual}\n{result}"
            );
        }
    }
}

// The rolling measures declare `grain.include: [returns.day]`, so their inner
// stage is summed per day whatever grain the query asks for. Each window is
// wide enough to cover all three days, so at any grain coarser than a day both
// windows have to agree with the plain `twr`.

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_grain_include_day_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.twr
          - returns.twr_ytd
          - returns.twr_1y
          - returns.twr_ytd_day_granularity
        time_dimensions:
          - dimension: returns.day
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-03"
        order:
          - id: returns.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_measures(
            &result,
            &[
                ("returns__twr", vec![0.1, 0.2, -0.5]),
                ("returns__twr_ytd", vec![0.1, 0.32, -0.34]),
                ("returns__twr_1y", vec![0.1, 0.32, -0.34]),
                ("returns__twr_ytd_day_granularity", vec![0.1, 0.32, -0.34]),
            ],
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_grain_include_month_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.twr
          - returns.twr_ytd
          - returns.twr_1y
          - returns.twr_ytd_day_granularity
        time_dimensions:
          - dimension: returns.day
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
        order:
          - id: returns.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_measures(
            &result,
            &[
                ("returns__twr", vec![-0.34]),
                ("returns__twr_ytd", vec![-0.34]),
                ("returns__twr_1y", vec![-0.34]),
                ("returns__twr_ytd_day_granularity", vec![-0.34]),
            ],
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_grain_include_no_time_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.twr
          - returns.twr_ytd
          - returns.twr_1y
          - returns.twr_ytd_day_granularity
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_measures(
            &result,
            &[
                ("returns__twr", vec![-0.34]),
                ("returns__twr_ytd", vec![-0.34]),
                ("returns__twr_1y", vec![-0.34]),
                ("returns__twr_ytd_day_granularity", vec![-0.34]),
            ],
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_grain_include_non_time_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.twr
          - returns.twr_ytd
          - returns.twr_1y
          - returns.twr_ytd_day_granularity
        dimensions:
          - returns.security
        order:
          - id: returns.security
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_measures(
            &result,
            &[
                ("returns__twr", vec![-0.34, -0.34]),
                ("returns__twr_ytd", vec![-0.34, -0.34]),
                ("returns__twr_1y", vec![-0.34, -0.34]),
                ("returns__twr_ytd_day_granularity", vec![-0.34, -0.34]),
            ],
        );
    }
}

// A `grain.include` naming a granularity the query already groups by is a
// no-op, not a second copy of the same column.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_include_duplicates_query_granularity() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.twr_day_granularity
        time_dimensions:
          - dimension: returns.day
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-03"
        order:
          - id: returns.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_measures(
            &result,
            &[("returns__twr_day_granularity", vec![0.1, 0.2, -0.5])],
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_grain_keep_only_is_rejected() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.log_return_sum_ytd_keep_only
        dimensions:
          - returns.security
        time_dimensions:
          - dimension: returns.day
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let err = ctx
        .build_sql(query)
        .expect_err("keep_only must be rejected");
    assert!(
        err.message.contains("grain.exclude") && err.message.contains("grain.keep_only"),
        "unexpected error: {}",
        err.message
    );
}

// The older `reduce_by` / `group_by` spellings compile into the same grain
// lists, so a model that never writes the word `grain` must still be told
// which of its own keys the message is about.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_grain_reduce_by_is_rejected() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.log_return_sum_ytd_reduce_by
        dimensions:
          - returns.security
        time_dimensions:
          - dimension: returns.day
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let err = ctx
        .build_sql(query)
        .expect_err("reduce_by must be rejected");
    assert!(
        err.message.contains("reduce_by") && err.message.contains("group_by"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_grain_empty_keep_only_is_rejected() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.log_return_sum_ytd_empty_keep_only
        dimensions:
          - returns.security
        time_dimensions:
          - dimension: returns.day
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    let err = ctx
        .build_sql(query)
        .expect_err("an empty keep_only must be rejected");
    assert!(
        err.message.contains("grain.keep_only"),
        "unexpected error: {}",
        err.message
    );
}

// A narrowing grain key can be inert for a given query: `exclude` of a member
// the grain does not carry subtracts nothing, and `keep_only` listing exactly
// what the query groups by intersects to the same list. Such a measure answers
// like one that declares no grain at all, and that answer is correct.

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_inert_reduce_by_answers_as_plain() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.weight_ytd
          - returns.weight_ytd_reduce_security
        time_dimensions:
          - dimension: returns.day
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
        order:
          - id: returns.day
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_measures(
            &result,
            &[
                ("returns__weight_ytd", vec![600.0]),
                ("returns__weight_ytd_reduce_security", vec![600.0]),
            ],
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_inert_group_by_answers_as_plain() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.weight_ytd
          - returns.weight_ytd_group_by_query_grain
        dimensions:
          - returns.security
        time_dimensions:
          - dimension: returns.day
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
        order:
          - id: returns.security
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_measures(
            &result,
            &[
                ("returns__weight_ytd", vec![300.0, 300.0]),
                (
                    "returns__weight_ytd_group_by_query_grain",
                    vec![300.0, 300.0],
                ),
            ],
        );
    }
}

// The same narrowing key, on a query with no time dimension: no frame is built,
// the measure goes through the ordinary multi-stage path, and the narrowing is
// honoured — the value is pooled over both securities while the rows stay per
// security.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_narrowing_without_time_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - returns.weight_ytd
          - returns.weight_ytd_reduce_security
        dimensions:
          - returns.security
        order:
          - id: returns.security
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        assert_measures(
            &result,
            &[
                ("returns__weight_ytd", vec![300.0, 300.0]),
                ("returns__weight_ytd_reduce_security", vec![600.0, 600.0]),
            ],
        );
    }
}
