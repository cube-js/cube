//! A rollup that cannot serve a query must not change how that query is planned.
//!
//! The SQL API sends explicit query-level join hints whenever the queried members alone
//! do not determine a join root. Pre-aggregation matching re-derives the query's join
//! groups to compare them against a candidate's, and that re-derivation has to keep the
//! hints the query came with.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const SCHEMA: &str = "common/pre_agg_query_join_hints.yaml";
const ROLLUP: &str = "orders_and_line_items_of_users";

// Members from `users` and `line_items`, which have no join between them: only the
// query-level hints name `base_orders` as the root.
const COVERED_QUERY: &str = indoc! {"
    measures:
      - line_items.count
    dimensions:
      - users.gender
    joinHints:
      - [base_orders, users]
      - [base_orders, line_items]
"};

// Same shape, but `base_orders.count` is multiplied over the `line_items` fan-out,
// so the rollup cannot serve it.
const UNCOVERED_QUERY: &str = indoc! {"
    measures:
      - base_orders.count
      - line_items.count
    dimensions:
      - users.state
    joinHints:
      - [base_orders, users]
      - [base_orders, line_items]
"};

fn context_with_rollup() -> Result<TestContext, CubeError> {
    TestContext::new(MockSchema::from_yaml_file(SCHEMA).only_pre_aggregations(&[ROLLUP]))
}

fn context_without_pre_aggregations() -> Result<TestContext, CubeError> {
    TestContext::new(MockSchema::from_yaml_file(SCHEMA).only_pre_aggregations(&[]))
}

#[test]
fn test_query_join_hints_are_kept_while_matching() -> Result<(), CubeError> {
    let (_sql, pre_aggrs) =
        context_with_rollup()?.build_sql_with_used_pre_aggregations(COVERED_QUERY)?;

    assert_eq!(
        pre_aggrs
            .iter()
            .map(|p| p.name().clone())
            .collect::<Vec<_>>(),
        vec![ROLLUP.to_string()]
    );

    Ok(())
}

#[test]
fn test_unusable_rollup_does_not_change_the_plan() -> Result<(), CubeError> {
    let (sql, pre_aggrs) =
        context_with_rollup()?.build_sql_with_used_pre_aggregations(UNCOVERED_QUERY)?;

    assert!(
        pre_aggrs.is_empty(),
        "Rollup cannot serve a measure multiplied by the line_items fan-out, got: {:?}",
        pre_aggrs.iter().map(|p| p.name()).collect::<Vec<_>>()
    );

    let (baseline, _) = context_without_pre_aggregations()?
        .build_sql_with_used_pre_aggregations(UNCOVERED_QUERY)?;
    assert_eq!(sql, baseline);

    Ok(())
}

#[test]
fn test_query_join_hints_plan_without_pre_aggregations() -> Result<(), CubeError> {
    let ctx = context_without_pre_aggregations()?;

    for query in [COVERED_QUERY, UNCOVERED_QUERY] {
        let (sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query)?;
        assert!(pre_aggrs.is_empty());
        assert!(
            sql.contains("line_items") && sql.contains("users"),
            "Query must be planned along its own join hints, got: {sql}"
        );
    }

    Ok(())
}

// A view resolves the same ambiguity through its own `join_path`, so its queries carry
// no query-level hints and matching must keep working exactly as before.
#[test]
fn test_view_query_still_matches_without_query_join_hints() -> Result<(), CubeError> {
    let query = indoc! {"
        measures:
          - orders_view.line_items_count
        dimensions:
          - orders_view.users_gender
    "};

    let (sql, pre_aggrs) = context_with_rollup()?.build_sql_with_used_pre_aggregations(query)?;

    assert_eq!(
        pre_aggrs
            .iter()
            .map(|p| p.name().clone())
            .collect::<Vec<_>>(),
        vec![ROLLUP.to_string()]
    );
    assert!(
        sql.contains(ROLLUP),
        "Query must read the rollup, got: {sql}"
    );

    Ok(())
}
