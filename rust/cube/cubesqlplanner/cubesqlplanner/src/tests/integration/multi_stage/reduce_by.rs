use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

fn assert_uses_window(sql: &str) {
    assert!(
        sql.contains("OVER (PARTITION BY"),
        "expected SQL to use a window function (`... OVER (PARTITION BY ...)`),\n\
         got:\n{}",
        sql,
    );
}

fn assert_no_window(sql: &str) {
    assert!(
        !sql.contains("OVER (PARTITION BY"),
        "expected SQL to assemble via JOIN-model (no window function),\n\
         got:\n{}",
        sql,
    );
}

// add_group_by + reduce_by together: leaf grain extends with customer_id
// while partition grain shrinks by removing category. Three distinct grains:
//   leaf       = (status, category, customer_id)  ← per-customer sum(amount)
//   query      = (status, category)
//   partition  = (status,)                        ← reduce_by removes category
// Expected per JOIN-semantic (outer sum collapses add_group_by + reduce_by
// down to partition, broadcast to query grid): total sum(amount) per status.
//   cancelled = 200, completed = 1400, pending = 650.
#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_add_group_by_combo() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_by_customer_reduce_category
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_no_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Inner base = max (idempotent), outer multi-stage = sum. JOIN-model
// computes overall max(amount) per status, broadcast across categories
// (100 / 400 / 250 on this seed).
#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_sum_of_max() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.max_sum_reduce_category
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_no_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Inner additive (sum), outer idempotent (max). JOIN-model computes overall
// sum(amount) per status broadcast across categories
// (200 / 1400 / 650 on this seed).
#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_max_of_sum() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.max_total_amount_reduce_category
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_no_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Calculated multi-stage measure summing two reduce_by Aggregate children.
// Each child renders through the JOIN-model path independently; the parent
// Calculate inode then stitches both via FullKeyAggregate on the query grain.
// Expected per status (broadcast across categories):
//   amount_reduce_category:           200 / 1400 / 650
//   unique_customers_reduce_category:   3 /    3 /   3
//   sum:                              203 / 1403 / 653
#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_calculated_over_two() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_plus_customers_reduce_category
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    // Mixed: the sum-of-sum child renders through window-path, the
    // count_distinct child through JOIN-model, and the parent Calculate
    // stitches both via FullKeyAggregate. SQL contains at least one window.
    let sql = ctx.build_sql(query).unwrap();
    assert_uses_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_single_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_reduce_category
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_uses_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_other_dim() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_reduce_status
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_uses_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_multiple_dims() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_reduce_all
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    // reduce_by drops every query dim → partition equals nothing; window-path
    // is skipped as redundant and we fall back to plain group-by.
    let sql = ctx.build_sql(query).unwrap();
    assert_no_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_dim_not_in_query() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_reduce_category
        dimensions:
          - orders.status
        order:
          - id: orders.status
    "#};

    // reduce_by targets a dim absent from the query → partition equals
    // the query grain, window is redundant, falls back to plain group-by.
    let sql = ctx.build_sql(query).unwrap();
    assert_no_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// avg with reduce_by — JOIN-model computes overall avg(amount) per status
// (= 66.67 / 233.33 / 108.33 on this seed) instead of the window-path
// avg-of-bucket-avgs which would have diverged on uneven bucket sizes.
#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_avg() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.avg_amount_reduce_category
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_no_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Multi-stage measure has outer `type: sum` over base `count` — this is the
// correct user-level shape for "total count per partition" (count rolls up as
// sum). JOIN-model picks the partition-grain leaf and broadcasts to query.
#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_count() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.count_reduce_category
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_uses_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Multi-stage measure has outer `type: sum` over base `count_distinct` —
// the correct shape for "rolled-up distinct count per partition". On this
// seed customers don't overlap across statuses, so sum of per-status
// count_distinct equals the true distinct count (3/3/3); when partitions
// overlap callers should use an HLL-based path instead.
#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_count_distinct() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.unique_customers_reduce_category
        dimensions:
          - orders.status
          - orders.category
        order:
          - id: orders.status
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_no_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_with_time() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_reduce_category
        dimensions:
          - orders.category
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.category
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_uses_window(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_time_dim() {
    let ctx = create_context();

    // Regression test for cube-js/cube#10854: `reduce_by: [<time_dim>]` was
    // silently ignored because `TimeDimensionSymbol::full_name` carries a
    // granularity suffix (defaulting to "_day" even with no granularity)
    // while the reduce_by entry resolves to a base DimensionSymbol without
    // the suffix — string equality at the comparison site failed and the
    // time dim stayed in PARTITION BY, collapsing each window to a one-row
    // partition.
    //
    // Expected: `amount_reduce_created_at` (sum reducing by created_at)
    // returns the per-status grand total across all months, not split per
    // (status, month). Status totals from seed: completed=1400, pending=650,
    // cancelled=200. With reduce_by working, every row for a given status
    // shows the same total regardless of month.
    let query = indoc! {r#"
        measures:
          - orders.amount_reduce_created_at
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Same bug as test_reduce_by_time_dim, but on the JOIN-model path: a non-sum
// outer aggregation is not window-eligible, so the grain is applied via
// partition_filter on the leaf state instead of PARTITION BY. Expected: each
// status row carries the per-status max across all months.
#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_by_time_dim_join_model() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.max_amount_reduce_created_at
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Bare time-dim exclude spelled via the `grain:` directive, with a non-empty
// include (which forces the JOIN-model path). Expected: per-status totals
// independent of month.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_exclude_time_dim_join_model() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_grain_exclude_created_at_include_category
        dimensions:
          - orders.status
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders.status
          - id: orders.created_at
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
