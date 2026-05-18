use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// TODO: planner currently produces avg-of-avg via window for multi-stage avg
// with reduce_by. avg is non-additive, so the result is "mean of per-bucket
// means" instead of the actual mean over the partition — silently wrong on
// data with uneven bucket sizes. Re-enable after switching reduce_by for
// non-additive measures to the JOIN-based model.
//
// Honest avg(amount) per status (with category reduced):
//   cancelled — orders 4, 9, 14 → values 50, 50, 100 → (200 / 3)  ≈ 66.67
//   completed — 100, 200, 300, 200, 400, 200          → (1400 / 6) ≈ 233.33
//   pending   — 120, 30, 150, 50, 250, 50             → (650 / 6)  ≈ 108.33
//
// avg-of-avg via window yields different numbers for completed/pending because
// per-(status, category) bucket sizes differ.
#[ignore]
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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// TODO: planner emits count(count(x)) OVER for multi-stage count with reduce_by.
// On per-bucket single-row groups, inner count(x) collapses to 1, and outer
// count(1) OVER (...) becomes the number of (full_grain) buckets in the partition
// — not the count of source rows in the partition. Re-enable after moving
// reduce_by for non-additive outer aggregations to the JOIN-based model.
//
// Honest count(*) per status (with category reduced):
//   cancelled — orders 4, 9, 14                       → 3
//   completed — orders 1, 2, 6, 7, 11, 12             → 6
//   pending   — orders 3, 5, 8, 10, 13, 15            → 6
//
// Window count-of-count returns the number of distinct categories per status,
// which on this seed is 3 for every status.
#[ignore]
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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// TODO: planner currently emits invalid SQL for multi-stage count_distinct
// with reduce_by (non-additive inner aggregated again via window). Re-enable
// when reduce_by/group_by for non-additive measures is reworked off the window
// path onto the JOIN-based model.
#[ignore]
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

    ctx.build_sql(query).unwrap();

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

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
