use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

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
