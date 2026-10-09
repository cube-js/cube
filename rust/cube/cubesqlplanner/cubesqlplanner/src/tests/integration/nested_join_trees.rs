//! Measures whose join trees are nested one in the other: `carts` is reached
//! from `sites`, `checkouts` from `carts`, so a measure on `checkouts` needs
//! every join a measure on `carts` needs and one more.
//!
//! Grouping measures by the exact join tree puts such measures in separate
//! groups, and each group re-scans the shared part of the join from scratch.
//! They are folded into one group - one scan - whenever moving a measure into
//! the wider tree cannot change what it computes: the extra joins are `LEFT`,
//! so they only ever replicate a row, and a distinct aggregation is immune to
//! that. A plain count or a sum is not, and keeps its own group.

use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::options_member::OptionsMember;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockMemberExpressionDefinition, MockMemberSql,
    MockSchema,
};
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::rc::Rc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_nested_join_trees.yaml");
    TestContext::new(schema).unwrap()
}

const SEED: &str = "integration_nested_join_trees_tables.sql";

/// How many times the query reads the cube at the root of the join. One read
/// per group is exactly the duplication that folding nested groups removes.
fn base_scan_count(sql: &str) -> usize {
    sql.matches(r#"AS "sites""#).count()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_distinct_measures_share_one_base_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The same measure on its own, as the value the merged query must reproduce.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_distinct_measure_alone() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A plain count would read one row per checkout instead of one per cart, so
/// its group stays on its own tree.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_count_keeps_its_own_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.count
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Same for a sum, which the replicated rows would inflate.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_sum_keeps_its_own_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.total_value
          - checkouts.total_amount
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// An ungrouped query returns the joined rows themselves, so the replication
/// the wider tree introduces would show up in the result.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_ungrouped_keeps_separate_scans() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
        ungrouped: true
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");
}

fn make_measure_expression(name: &str, cube: &str, sql: &str) -> OptionsMember {
    let member_sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new(sql).unwrap());
    let expr = MockMemberExpressionDefinition::builder()
        .expression_name(Some(name.to_string()))
        .name(Some(name.to_string()))
        .cube_name(Some(cube.to_string()))
        .expression(MemberExpressionExpressionDef::Sql(member_sql))
        .build();
    OptionsMember::MemberExpression(Rc::new(expr))
}

/// A `COUNT(*)` member expression references no member of any cube, so what it
/// counts is decided entirely by the join tree it lands on. Moving it into a
/// wider tree would silently turn it into a count of the fanned-out rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_count_star_expression_keeps_its_own_scan() {
    let ctx = create_context();

    let total_count = make_measure_expression("total_count", "sites", "COUNT(*)");
    let mut measures = vec![total_count];
    measures.extend(members_from_strings(vec!["checkouts.unique_msid"]));

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(measures))
            .dimensions(Some(members_from_strings(vec!["sites.country"])))
            .build(),
    );

    let sql = ctx.build_sql_from_options(options.clone()).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The `COUNT(*)` of an expression is a member of nothing, so the reference
/// beside it is the only leaf the fan-out check can read - and a distinct count
/// answering "cannot move" says nothing about the raw count next to it.
///
/// Only the grouping is asserted: a raw aggregate written inside an expression
/// that also references a member is rendered above the per-group CTEs, where it
/// has no rows of the join left to read, so the split query is not executable.
#[test]
fn test_nested_trees_expression_around_distinct_measure_keeps_its_own_scan() {
    let ctx = create_context();

    let net_carts = make_measure_expression("net_carts", "carts", "COUNT(*) - {carts.unique_msid}");
    let mut measures = vec![net_carts];
    measures.extend(members_from_strings(vec!["checkouts.unique_msid"]));

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(measures))
            .dimensions(Some(members_from_strings(vec!["sites.country"])))
            .build(),
    );

    let sql = ctx.build_sql_from_options(options).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");
}

/// The same expression on its own, over the `carts` tree it belongs to: one
/// repeated cart in the US, none in DE. The `checkouts` tree carries four US
/// rows for the same three carts, so evaluating the expression there would
/// answer 2 instead.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_expression_around_distinct_measure_alone() {
    let ctx = create_context();

    let net_carts = make_measure_expression("net_carts", "carts", "COUNT(*) - {carts.unique_msid}");

    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(vec![net_carts]))
            .dimensions(Some(members_from_strings(vec!["sites.country"])))
            .build(),
    );

    let sql = ctx.build_sql_from_options(options.clone()).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg_from_options(options, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Every measure of a view is a bare reference to the cube measure, so the same
/// two nested trees fold into one scan when the query is written on the view -
/// and answer what the cube paths answer.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_view_measures_share_one_base_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - funnel.unique_msid
          - funnel.checkouts_unique_msid
        dimensions:
          - funnel.country
        order:
          - id: funnel.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A calculated measure that writes a raw aggregate of its own around a
/// reference keeps its tree: the referenced distinct count is the only leaf, and
/// the count beside it would read the fanned-out rows.
#[test]
fn test_nested_trees_calculated_measure_keeps_its_own_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.repeated_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");
}

/// Each cube has its own rollup keyed by the shared dimension. Folding the
/// groups leaves one query that no single rollup covers, so the merge has to
/// stand down where per-leg rollups are available.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_separate_pre_aggs_still_match() {
    let schema = MockSchema::from_yaml_file("common/integration_nested_join_trees_pre_aggs.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - carts.unique_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let (_sql, pre_aggrs) = ctx.build_sql_with_used_pre_aggregations(query).unwrap();
    let names: Vec<&str> = pre_aggrs.iter().map(|u| u.name().as_str()).collect();

    assert_eq!(pre_aggrs.len(), 2, "got {names:?}");
}

/// A filter on the shallower cube leaves the trees nested, so the merge still
/// happens and has to keep answering what the unmerged legs would.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_merge_with_filter_on_shallower_cube() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
        filters:
          - member: carts.msid
            operator: equals
            values:
              - m1
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// A filter on the deeper cube pulls it into the base hints, so both measures
/// resolve to the same tree and there is nothing left to merge.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_filter_on_deeper_cube_leaves_one_tree() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
        filters:
          - member: checkouts.msid
            operator: equals
            values:
              - x1
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// Building a rollup describes the rows to store, and matching later compares
/// the query's groups against the pre-aggregation's, so the build must be
/// grouped the way an unmerged query is.
#[test]
fn test_nested_trees_pre_aggregation_query_keeps_separate_scans() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.unique_msid
          - checkouts.unique_msid
        dimensions:
          - sites.country
        pre_aggregation_query: true
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");
}

/// A minimum or a maximum does not move when the wider tree replicates the row
/// it already saw, so it merges on the same grounds a distinct count does.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_min_max_share_one_base_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.max_value
          - carts.min_value
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// The same measures on their own, as the values the merged query must
/// reproduce.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_min_max_alone() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.max_value
          - carts.min_value
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 1, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}

/// An average counts every row it is given, so the replication would move it.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_trees_avg_keeps_its_own_scan() {
    let ctx = create_context();

    let query = indoc! {"
        measures:
          - carts.avg_value
          - checkouts.unique_msid
        dimensions:
          - sites.country
        order:
          - id: sites.country
    "};

    let sql = ctx.build_sql(query).unwrap();
    assert_eq!(base_scan_count(&sql), 2, "sql: {sql}");

    if let Some(result) = ctx.try_execute_pg(query, SEED).await {
        insta::assert_snapshot!(result);
    }
}
