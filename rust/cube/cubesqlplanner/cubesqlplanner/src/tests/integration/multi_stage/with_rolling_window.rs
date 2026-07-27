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

/// Splits `s` on commas that are not nested inside parentheses.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Returns the body of the CTE that assembles the rolling window: the one whose
/// `FROM` reaches the base rollup through the `rolling_source` alias. CTE bodies
/// are `<name> AS ( ... )`; each `AS (` opener is paren-matched so the returned
/// slice is exactly that CTE and never spills into an adjacent one.
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

/// Asserts that `dim_alias` is carried through the rolling-window rollup CTE:
/// it must be BOTH projected in that CTE's SELECT list AND present in its
/// GROUP BY (matched either by its 1-based ordinal position or by name). This
/// is the exact shape that regressed — the buggy planner dropped the added
/// dimension from the rollup, so it was neither selected nor grouped.
fn assert_dim_carried_through_rollup(sql: &str, dim_alias: &str) {
    let cte = rolling_rollup_cte(sql);
    let quoted = format!("\"{dim_alias}\"");

    let select_start = cte.find("SELECT").expect("rollup CTE without SELECT") + "SELECT".len();
    let from_pos = select_start
        + cte[select_start..]
            .find(" FROM")
            .expect("rollup CTE without FROM");
    let projection = &cte[select_start..from_pos];

    let projections = split_top_level_commas(projection);
    let dim_index = projections
        .iter()
        .position(|p| p.trim_end().ends_with(&quoted))
        .unwrap_or_else(|| {
            panic!("`{dim_alias}` not projected in rolling-window rollup CTE:\n{cte}")
        });

    let gb_start = cte
        .find("GROUP BY")
        .unwrap_or_else(|| panic!("rolling-window rollup CTE without GROUP BY:\n{cte}"))
        + "GROUP BY".len();
    let gb_end = cte[gb_start..]
        .find("ORDER BY")
        .map(|p| gb_start + p)
        .unwrap_or(cte.len());
    let group_by = &cte[gb_start..gb_end];

    let ordinal = (dim_index + 1).to_string();
    let grouped =
        group_by.contains(dim_alias) || group_by.split(',').map(str::trim).any(|t| t == ordinal);
    assert!(
        grouped,
        "`{dim_alias}` (projection #{}) is projected but missing from GROUP BY `{}` \
         of the rolling-window rollup CTE:\n{cte}",
        dim_index + 1,
        group_by.trim(),
    );
}

// Real-model regression (mirrors valid_license_seats.licensed_seats_*): a
// rolling-window leaf declares `add_group_by` and the parent calc re-declares
// it. The rolling-window rollup CTE used to source its grain from the
// top-level query dimensions, dropping the added dimension and emitting SQL
// that referenced a column absent from the rollup CTE. `orders__category` must
// now be projected and grouped in the rollup CTE feeding `rolling_source`.
// (The leaf's own `add_group_by` is inert on the rolling path — see fixture
// note — so it is the parent calc's pushed-down grain that reaches the rollup;
// this shape guards that the redundant leaf declaration stays harmless.)
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
    assert_dim_carried_through_rollup(&sql, "orders__category");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

// Same regression via the parent-push path in isolation: the rolling-window
// leaf `rolling_leaf_plain` declares NO grain of its own, and the parent
// multi-stage calc `rolling_parent_pushes_category` pushes
// `add_group_by: [orders.category]` down into it. The pushed-down dimension
// must reach the leaf's rollup CTE (projected and grouped), which the bug
// prevented because the rollup read the top-level (category-less) query grain.
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
    assert_dim_carried_through_rollup(&sql, "orders__category");

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
