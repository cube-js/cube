use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_and_add_group_by() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_7d
          - orders.amount_by_id
        time_dimensions:
          - dimension: orders.created_at
            granularity: day
            dateRange:
              - "2024-01-01"
              - "2024-01-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_and_time_shift() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_7d
          - orders.amount_prev_month
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_and_reduce_by() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_7d
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

/// Asserts that every CTE in the generated SQL is defined only once.
/// Duplicate names mean a nested `WITH cte_0 AS (WITH cte_0 AS ...)` —
/// shadowing that is invalid on some dialects and confusing on the rest.
fn assert_no_duplicate_cte_names(sql: &str) {
    let mut seen = std::collections::HashSet::new();
    for (pos, _) in sql.match_indices(" AS (") {
        let name = sql[..pos].split_whitespace().last().unwrap_or("");
        if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            assert!(
                seen.insert(name.to_string()),
                "duplicate CTE name `{}` in SQL:\n{}",
                name,
                sql
            );
        }
    }
}

// Rolling window measure whose leaf becomes a multiplied subquery: the
// filter on `returns` joins it through customers (one_to_many), so
// sum(amount) is multiplied and the leaf query plans its own CTEs.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_with_multiplied_leaf() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.total_amount_ytd
        filters:
          - member: returns.customer_id
            operator: set
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_no_duplicate_cte_names(&sql);

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_and_calculated() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_sum_7d
          - orders.mom_growth
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
