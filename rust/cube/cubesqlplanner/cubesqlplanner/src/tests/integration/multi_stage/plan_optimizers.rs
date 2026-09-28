//! Tests for physical-plan optimizer passes applied to multi-stage queries.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use regex::Regex;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_multi_stage_tables.sql";

/// Multi-stage planning wraps CTE references into trivial
/// `(SELECT * FROM cte_n AS cte_n) AS alias` derived tables; the
/// trivial-subquery optimizer must collapse them into direct
/// `cte_n AS alias` references so engines that inline CTE bodies at
/// every reference (Cube Store / DataFusion) don't pay two extra plan
/// nodes per usage.
#[tokio::test(flavor = "multi_thread")]
async fn test_trivial_cte_wrappers_are_collapsed() {
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

    let sql = ctx.build_sql(query).unwrap();

    assert!(
        sql.contains("cte_0"),
        "Expected a multi-stage CTE in the generated SQL:\n{}",
        sql
    );

    // `SELECT * FROM <identifier>` (a bare table or CTE reference, not a
    // parenthesized subquery) is exactly the trivial pass-through shape the
    // optimizer must have collapsed.
    let trivial_wrapper = Regex::new(r"(?i)\(\s*SELECT\s+\*\s+FROM\s+[A-Za-z_]").unwrap();
    assert!(
        !trivial_wrapper.is_match(&sql),
        "Generated SQL still contains a trivial pass-through subquery wrapper:\n{}",
        sql
    );

    // The collapsed references keep the outer alias.
    assert!(
        sql.contains("cte_0  AS"),
        "Expected a direct aliased CTE reference in the generated SQL:\n{}",
        sql
    );

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
