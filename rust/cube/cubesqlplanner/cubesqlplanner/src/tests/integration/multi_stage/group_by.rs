use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_override() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_status
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_with_time() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_status_time
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

#[tokio::test(flavor = "multi_thread")]
async fn test_group_by_subset_of_query_dims() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_status
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
async fn test_group_by_equals_query_dims() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_group_by_both
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

// Smoke test: `grain.keep_only: [status]` yields the same partition as
// `group_by: [status]`. Snapshot must match `test_group_by_override`.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_keep_only_matches_group_by_override() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_grain_keep_only_status
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `grain.mode: fixed` (no keep_only/include) → grand total. The measure
// value is the SUM across all rows, replicated per query category.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_fixed_grand_total() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_grain_fixed_grand_total
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `grain.mode: fixed, keep_only: [status]` with status absent from the
// query → effective grain is empty (grand total), since the FIXED base is
// the original query context, not the parent state.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_fixed_keep_only_dim_not_in_query() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_grain_fixed_keep_only_status
        dimensions:
          - orders.category
        order:
          - id: orders.category
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// `grain.mode: fixed, keep_only: [status]` with status in the query →
// effective grain is [status], matching the RELATIVE behavior at top
// level (parent context == query). Smoke test that both modes coexist.
#[tokio::test(flavor = "multi_thread")]
async fn test_grain_fixed_keep_only_dim_in_query() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_grain_fixed_keep_only_status
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
