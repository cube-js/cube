//! A rolling window planned under another multi-stage member computes over the
//! state that member hands it, whatever time dimension the query itself has.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::{assert_same_rows, rows_without_column, TestContext};
use indoc::indoc;
use std::collections::BTreeMap;

const SEED: &str = "integration_multi_stage_tables.sql";

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml(indoc! {"
        cubes:
            - name: rw_orders
              sql: \"SELECT * FROM ms_orders\"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: category
                    type: string
                    sql: category
                  - name: created_at
                    type: time
                    sql: created_at
              measures:
                  - name: rolling_amount
                    type: sum
                    sql: amount
                    rolling_window:
                        trailing: 7 day
                  - name: rolling_amount_as_is
                    type: sum
                    sql: \"{CUBE.rolling_amount}\"
                    multi_stage: true
                  - name: rolling_amount_reduce_time
                    type: sum
                    sql: \"{CUBE.rolling_amount}\"
                    multi_stage: true
                    reduce_by:
                        - rw_orders.created_at
                  - name: rolling_amount_by_category
                    type: sum
                    sql: \"{CUBE.rolling_amount}\"
                    multi_stage: true
                    group_by:
                        - rw_orders.category
                  - name: rolling_amount_add_day
                    type: sum
                    sql: \"{CUBE.rolling_amount}\"
                    multi_stage: true
                    add_group_by:
                        - rw_orders.created_at.day
                  - name: rolling_amount_by_month
                    type: sum
                    sql: \"{CUBE.rolling_amount}\"
                    multi_stage: true
                    group_by:
                        - rw_orders.category
                        - rw_orders.created_at.month
    "})
    .unwrap();
    TestContext::new(schema).unwrap()
}

// The rows come from the query's own grid; each must carry the value the
// equivalent query computes for its key, or none when that query has no row.
async fn assert_values_on_grid(
    ctx: &TestContext,
    query: &str,
    column: &str,
    equivalent: &str,
    equivalent_column: &str,
) {
    let (Some(result), Some(expected)) = (
        ctx.try_execute_pg(query, SEED).await,
        ctx.try_execute_pg(equivalent, SEED).await,
    ) else {
        return;
    };
    let expected_values: BTreeMap<Vec<String>, String> =
        rows_without_column(&expected, equivalent_column)
            .into_iter()
            .map(|mut row| {
                let value = row.pop().unwrap();
                (row, value)
            })
            .collect();
    let rows = rows_without_column(&result, column);
    assert!(!rows.is_empty(), "\nresult:\n{}", result);
    for row in rows {
        let (value, key) = row.split_last().unwrap();
        let expected_value = expected_values
            .get(key)
            .map(String::as_str)
            .unwrap_or("NULL");
        assert_eq!(
            value, expected_value,
            "row {:?}\nresult:\n{}\nexpected:\n{}",
            key, result, expected
        );
    }
}

// Sums the last column of each row per the remaining columns, skipping rows
// without a value.
fn summed_per_key(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut sums: BTreeMap<Vec<String>, f64> = BTreeMap::new();
    for row in rows {
        let (value, key) = row.split_last().unwrap();
        if value != "NULL" {
            *sums.entry(key.to_vec()).or_default() += value.parse::<f64>().unwrap();
        }
    }
    sums.into_iter()
        .map(|(mut key, sum)| {
            key.push(format!("{:.2}", sum));
            key
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_under_stage_keeping_the_query_grain() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - rw_orders.rolling_amount_as_is
        dimensions:
          - rw_orders.category
        time_dimensions:
          - dimension: rw_orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};
    let equivalent = indoc! {r#"
        measures:
          - rw_orders.rolling_amount
        dimensions:
          - rw_orders.category
        time_dimensions:
          - dimension: rw_orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();
    assert_same_rows(&ctx, SEED, query, "", equivalent, "").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_under_stage_reducing_time() {
    let ctx = create_context();

    // The stage has no time dimension, so the window is anchored at the end of
    // the date range, as for the measure queried without a granularity: it
    // reaches back past the start of the query's last month bucket.
    let query = indoc! {r#"
        measures:
          - rw_orders.rolling_amount_reduce_time
        time_dimensions:
          - dimension: rw_orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-26"
    "#};
    let equivalent = indoc! {r#"
        measures:
          - rw_orders.rolling_amount
        time_dimensions:
          - dimension: rw_orders.created_at
            dateRange:
              - "2024-01-01"
              - "2024-01-26"
    "#};

    ctx.build_sql(query).unwrap();
    assert_values_on_grid(&ctx, query, "rw_orders__created_at_month", equivalent, "").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_under_stage_keeping_only_category() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - rw_orders.rolling_amount_by_category
        time_dimensions:
          - dimension: rw_orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-26"
    "#};
    let equivalent = indoc! {r#"
        measures:
          - rw_orders.rolling_amount
        time_dimensions:
          - dimension: rw_orders.created_at
            dateRange:
              - "2024-01-01"
              - "2024-01-26"
    "#};

    ctx.build_sql(query).unwrap();
    assert_values_on_grid(&ctx, query, "rw_orders__created_at_month", equivalent, "").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a time dimension added to a stage's grain carries no date range"]
async fn test_rolling_window_under_stage_adding_a_day_the_query_lacks() {
    let ctx = create_context();

    // The stage groups by day although the query has no time dimension, so the
    // window walks the days and the stage sums what it yields per day.
    let query = indoc! {r#"
        measures:
          - rw_orders.rolling_amount_add_day
        dimensions:
          - rw_orders.category
        time_dimensions:
          - dimension: rw_orders.created_at
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};
    let per_day = indoc! {r#"
        measures:
          - rw_orders.rolling_amount
        dimensions:
          - rw_orders.category
        time_dimensions:
          - dimension: rw_orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    ctx.build_sql(query).unwrap();
    let (Some(result), Some(per_day_result)) = (
        ctx.try_execute_pg(query, SEED).await,
        ctx.try_execute_pg(per_day, SEED).await,
    ) else {
        return;
    };
    let rows = rows_without_column(&result, "")
        .into_iter()
        .filter(|row| row.last().is_some_and(|value| value != "NULL"))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        summed_per_key(rows_without_column(
            &per_day_result,
            "rw_orders__created_at_day"
        )),
        "\nresult:\n{}\nper day:\n{}",
        result,
        per_day_result
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_window_under_stage_at_a_coarser_granularity() {
    let ctx = create_context();

    // The query walks days, the stage months: the window is applied per month
    // and each day reports its month's value.
    let query = indoc! {r#"
        measures:
          - rw_orders.rolling_amount_by_month
        dimensions:
          - rw_orders.category
        time_dimensions:
          - dimension: rw_orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};
    let equivalent = indoc! {r#"
        measures:
          - rw_orders.rolling_amount
        dimensions:
          - rw_orders.category
        time_dimensions:
          - dimension: rw_orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    ctx.build_sql(query).unwrap();
    assert_values_on_grid(
        &ctx,
        query,
        "rw_orders__created_at_day",
        equivalent,
        "rw_orders__created_at_month",
    )
    .await;
}
