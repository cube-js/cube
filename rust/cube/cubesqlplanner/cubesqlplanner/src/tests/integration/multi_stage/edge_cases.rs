use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_stage_without_time_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_id
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

#[tokio::test(flavor = "multi_thread")]
async fn test_time_shift_without_date_range() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
    "#};

    let result = ctx.build_sql(query);
    match result {
        Ok(sql) => {
            println!("SQL generated without dateRange:\n{}", sql);
            if let Some(result) = ctx.try_execute_pg(query, SEED).await {
                insta::assert_snapshot!(result);
            }
        }
        Err(e) => {
            insta::assert_snapshot!("time_shift_without_date_range_error", e.to_string());
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_result_set() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_id
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-06-01"
              - "2024-06-30"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_row_result() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.amount_by_id
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-05"
              - "2024-01-05"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
