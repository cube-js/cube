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

/// PARTITION BY clause bodies (text between `PARTITION BY` and the following
/// `ORDER BY`) of every window function in `sql`.
fn partition_by_clauses(sql: &str) -> Vec<String> {
    sql.match_indices("PARTITION BY")
        .map(|(i, _)| {
            let rest = &sql[i + "PARTITION BY".len()..];
            let end = rest.find("ORDER BY").unwrap_or(rest.len());
            rest[..end].to_string()
        })
        .collect()
}

// CORE-550 through a view: the query groups by view dimensions
// (orders_ms_view.status / .created_at) while the measure's reduce_by targets
// the underlying base cube member (orders.created_at). The granular view time
// dimension (created_at_month) must still be dropped from the window's
// PARTITION BY, leaving [status]. Guards that the fix resolves the reduce_by
// reference through the view→base reference chain, not just for plain cubes.
#[tokio::test(flavor = "multi_thread")]
async fn test_view_reduce_by_time_dimension() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.amount_reduce_time
        dimensions:
          - orders_ms_view.status
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        order:
          - id: orders_ms_view.status
          - id: orders_ms_view.created_at
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_uses_window(&sql);
    for clause in partition_by_clauses(&sql) {
        assert!(
            !clause.contains("created_at"),
            "reduce_by(created_at) must drop the view time dimension from PARTITION BY,\n\
             partition clause was: {}\nfull SQL:\n{}",
            clause,
            sql,
        );
        assert!(
            clause.contains("status"),
            "status must remain in PARTITION BY, partition clause was: {}",
            clause,
        );
    }

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_time_shift() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.amount_prev_month
        time_dimensions:
          - dimension: orders_ms_view.created_at
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
async fn test_view_with_calculated() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
          - orders_ms_view.mom_growth
        time_dimensions:
          - dimension: orders_ms_view.created_at
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
async fn test_view_with_filter() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders_ms_view.total_amount
        time_dimensions:
          - dimension: orders_ms_view.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
        filters:
          - dimension: orders_ms_view.status
            operator: equals
            values:
              - completed
    "#};

    ctx.build_sql(query).unwrap();

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
