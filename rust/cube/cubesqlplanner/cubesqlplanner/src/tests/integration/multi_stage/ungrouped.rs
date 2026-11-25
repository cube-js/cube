use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use itertools::Itertools;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_add_group_by() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_id
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        ungrouped: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_id
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        ungrouped: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// A sum over count_distinct cannot take the window path: its stage sums the
// inputs' per-group distinct counts, so an ungrouped query must report what
// the grouped one does over the same distinct key rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_group_by_locked_aggregate_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.unique_customers_group_by_status
        dimensions:
          - orders.category
        ungrouped: true
        order:
          - id: orders.category
    "#};
    let grouped_query = query.replace("ungrouped: true\n", "");

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        let grouped = ctx.try_execute_pg(&grouped_query, SEED).await.unwrap();
        assert_eq!(result, grouped);
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_reduce_by_aggregate_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.unique_customers_reduce_category
        dimensions:
          - orders.status
          - orders.category
        ungrouped: true
        order:
          - id: orders.status
          - id: orders.category
    "#};
    let grouped_query = query.replace("ungrouped: true\n", "");

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        let grouped = ctx.try_execute_pg(&grouped_query, SEED).await.unwrap();
        assert_eq!(result, grouped);
        insta::assert_snapshot!(result);
    }
}

// Without a grain the stage groups by every query dimension, so the ungrouped
// query must report what the grouped one does.
async fn assert_ungrouped_matches_grouped_without_grain(measure: &str) {
    let ctx = create_context();

    let query = format!(
        indoc! {r#"
            measures:
              - {}
            dimensions:
              - orders.status
              - orders.category
            ungrouped: true
            order:
              - id: orders.status
              - id: orders.category
        "#},
        measure
    );
    let grouped_query = query.replace("ungrouped: true\n", "");

    ctx.build_sql(&query).unwrap();

    if let Some(result) = ctx.try_execute_pg(&query, SEED).await {
        let grouped = ctx.try_execute_pg(&grouped_query, SEED).await.unwrap();
        assert_eq!(result, grouped);
    }
}

// Sum over count_distinct takes the JOIN model.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_join_model_aggregate_without_grain() {
    assert_ungrouped_matches_grouped_without_grain("orders.unique_customers_no_grain").await;
}

// Sum over sum is window-path eligible, but a partition covering every
// dimension renders as a plain aggregate.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_window_eligible_aggregate_without_grain() {
    assert_ungrouped_matches_grouped_without_grain("orders.total_amount_no_grain").await;
}

// A `mode: fixed` input restarts from the query's state, yet it still feeds an
// aggregate stage that needs its values grouped.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_aggregate_over_fixed_filter_input() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.unique_customers_fixed_group_by_status
        dimensions:
          - orders.category
        ungrouped: true
        order:
          - id: orders.category
    "#};
    let grouped_query = query.replace("ungrouped: true\n", "");

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        let grouped = ctx.try_execute_pg(&grouped_query, SEED).await.unwrap();
        assert_eq!(result, grouped);
        insta::assert_snapshot!(result);
    }
}

// The same leaf is read grouped by an aggregate stage and row by row by a
// calculated one; each must get its own stage.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_grouped_and_raw_inputs_of_one_leaf() {
    let ctx = create_context();

    let query = |measures: &str| {
        format!(
            indoc! {r#"
                measures: [{}]
                dimensions:
                  - orders.status
                ungrouped: true
            "#},
            measures
        )
    };
    let both = query("orders.max_total_amount_reduce_category, orders.category_share");
    let max_alone = query("orders.max_total_amount_reduce_category");
    let share_alone = query("orders.category_share");

    ctx.build_sql(&both).unwrap();

    if let Some(result) = ctx.try_execute_pg(&both, SEED).await {
        let max_alone = ctx.try_execute_pg(&max_alone, SEED).await.unwrap();
        let share_alone = ctx.try_execute_pg(&share_alone, SEED).await.unwrap();
        let rows = |table: &str| {
            table
                .lines()
                .skip(2)
                .map(|line| line.split('|').map(|c| c.trim().to_string()).collect_vec())
                .collect_vec()
        };
        // Next to a row-level measure the answer has a row per order, so the
        // aggregate is compared per status rather than row by row.
        let max_by_status = rows(&max_alone)
            .into_iter()
            .map(|r| (r[0].clone(), r[1].clone()))
            .collect::<std::collections::HashMap<_, _>>();
        let both_rows = rows(&result);
        for row in both_rows.iter() {
            assert_eq!(Some(&row[1]), max_by_status.get(&row[0]), "{result}");
        }
        let shares = |rows: Vec<Vec<String>>, idx: usize| {
            rows.into_iter()
                .map(|r| r[idx].clone())
                .sorted()
                .collect_vec()
        };
        assert_eq!(shares(both_rows, 2), shares(rows(&share_alone), 1));
    }
}

// Cross-product issue: ungrouped mode prevents aggregation in both
// current and shifted CTEs. The JOIN between them produces a cartesian
// product — Feb has 5 current rows × 5 shifted Jan rows = 25 rows,
// resulting in 55 total rows instead of expected 15.
// In grouped mode, the shifted CTE aggregates to 1 row per month,
// so the JOIN is 1:1. In ungrouped mode, both sides have per-row
// granularity and there's no join key to match individual rows.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_time_shift() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        ungrouped: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Each row of a time-shifted count or distinct count carries its row value,
// 1, as the plain measures do; the column they are built on must not leak.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_time_shift_over_count() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count
          - orders.count_prev_month
          - orders.unique_customers_prev_month
        dimensions:
          - orders.id
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-02-01"
              - "2024-03-31"
        order:
          - id: orders.id
          - id: orders.created_at
        ungrouped: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        for column in [
            "orders__count_prev_month",
            "orders__unique_customers_prev_month",
        ] {
            let values = column_values(&result, column);
            assert_eq!(values.iter().filter(|v| *v == "1").count(), 10, "{result}");
            assert!(values.iter().all(|v| v == "1" || v == "NULL"), "{result}");
        }
        insta::assert_snapshot!(result);
    }
}

// A rolling sum over a count reads the count through a multi-stage leaf too;
// its rows must sum the count's row values, not primary keys.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_rolling_sum_over_count() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count_rolling_sum
        dimensions:
          - orders.id
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.id
          - id: orders.created_at
        ungrouped: true
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        let values = column_values(&result, "orders__count_rolling_sum");
        assert!(!values.is_empty(), "{result}");
        assert!(values.iter().all(|v| v == "1"), "{result}");
        insta::assert_snapshot!(result);
    }
}

fn column_values(result: &str, column: &str) -> Vec<String> {
    let mut lines = result.lines();
    let index = lines
        .next()
        .unwrap()
        .split('|')
        .position(|name| name.trim() == column)
        .unwrap();
    lines
        .skip(1)
        .map(|row| row.split('|').nth(index).unwrap().trim().to_string())
        .collect()
}
