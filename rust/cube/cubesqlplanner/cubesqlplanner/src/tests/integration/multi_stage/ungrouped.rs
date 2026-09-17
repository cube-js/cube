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

// An ungrouped query selecting a group_by-locked Aggregate measure whose
// input is not sum-rollable (count_distinct) goes through the
// FullKeyAggregate JOIN model: the measure stage projects the query
// dimensions next to a plain sum(). That stage must be grouped even though
// the query itself is ungrouped — otherwise databases enforcing SQL
// grouping rules (e.g. Postgres) reject the statement. This is exactly the
// shape BI tools emit when browsing a table through the SQL API.
#[tokio::test(flavor = "multi_thread")]
async fn test_ungrouped_group_by_locked_aggregate_with_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.unique_customers_group_by_status
        dimensions:
          - orders.category
        ungrouped: true
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert!(
        sql.contains("GROUP BY"),
        "the Aggregate stage projects sum() next to bare dimension columns \
         and must be grouped even in an ungrouped query, got: {sql}"
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
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
