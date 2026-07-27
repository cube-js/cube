use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

/// Body of the CTE that assembles the rolling window — the one reaching the base
/// rollup through the `rolling_source` alias. Each `AS (` opener is paren-matched
/// so the returned slice never spills into an adjacent CTE.
fn rolling_rollup_cte(sql: &str) -> &str {
    let bytes = sql.as_bytes();
    for (open, _) in sql.match_indices(" AS (") {
        let body_start = open + " AS (".len();
        let mut depth = 1usize;
        let mut i = body_start;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        let body = &sql[body_start..i - 1];
        if body.contains("rolling_source") {
            return body;
        }
    }
    panic!("no rolling-window rollup CTE (`rolling_source` alias) found in SQL:\n{sql}");
}

/// Asserts `dim_alias` is projected by the rolling-window rollup CTE. The buggy
/// planner built that CTE's grain from the top-level query dimensions, so an
/// added dimension was dropped there while downstream CTEs still referenced it.
fn assert_dim_projected_in_rollup(sql: &str, dim_alias: &str) {
    let cte = rolling_rollup_cte(sql);
    let select_start = cte.find("SELECT").expect("rollup CTE without SELECT") + "SELECT".len();
    let from_pos = select_start
        + cte[select_start..]
            .find(" FROM")
            .expect("rollup CTE without FROM");
    assert!(
        cte[select_start..from_pos].contains(&format!("\"{dim_alias}\"")),
        "`{dim_alias}` not projected in rolling-window rollup CTE:\n{cte}",
    );
}

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

// A rolling-window leaf under an `add_group_by` calc, queried without the added
// dimension. The rollup CTE used to take its grain from the top-level query
// dimensions, dropping `orders.category` while a downstream CTE still joined on
// it, so the planner emitted SQL naming a column the rollup never selected.
// Grain is declared on both the leaf and the parent calc here; only the parent's
// push reaches the rollup (see fixture note), so this also guards that the
// redundant leaf declaration stays harmless.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_multi_stage_add_group_by_leaf_and_parent() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_leaf_own_reduce
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_dim_projected_in_rollup(&sql, "orders__category");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Same regression with the grain declared only on the parent calc.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_multi_stage_add_group_by_pushed_from_parent() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - orders.rolling_parent_pushed_reduce
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "2024-01-01"
              - "2024-03-31"
    "#};

    let sql = ctx.build_sql(query).unwrap();
    assert_dim_projected_in_rollup(&sql, "orders__category");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
