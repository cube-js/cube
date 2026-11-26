//! A rollup can't apply a SQL API sub-query join, so a query carrying one is
//! never served from a rollup, even when the rollup covers all its members.
//! Holds for rollups in Cube Store and in the source database alike.

use crate::cube_bridge::member_expression::{
    MemberExpressionDefinition, MemberExpressionExpressionDef,
};
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::subquery_join::SubqueryJoin;
use crate::logical_plan::PreAggregationUsage;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockMemberExpressionDefinition, MockMemberSql,
    MockSchema, MockSubqueryJoin,
};
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

const SCHEMA: &str = include_str!(
    "../../../test_fixtures/schemas/yaml_files/common/integration_cubestore_basic.yaml"
);

const TOP_SOURCES_SUBQUERY: &str = "SELECT source FROM visitors GROUP BY 1 LIMIT 1";

fn top_sources_join(join_type: &str) -> Result<Rc<dyn SubqueryJoin>, CubeError> {
    let on_sql: Rc<dyn MemberSql> = Rc::new(MockMemberSql::new(
        "{visitors.source} = \"top_sources\".source",
    )?);
    let on: Rc<dyn MemberExpressionDefinition> = Rc::new(
        MockMemberExpressionDefinition::builder()
            .cube_name(Some("visitors".to_string()))
            .expression(MemberExpressionExpressionDef::Sql(on_sql))
            .build(),
    );
    Ok(Rc::new(
        MockSubqueryJoin::builder()
            .sql(TOP_SOURCES_SUBQUERY.to_string())
            .join_type(Some(join_type.to_string()))
            .alias("\"top_sources\"".to_string())
            .on(on)
            .build(),
    ))
}

fn plan(
    external: bool,
    subquery_joins: Option<Vec<Rc<dyn SubqueryJoin>>>,
) -> Result<(String, Vec<PreAggregationUsage>), CubeError> {
    let ctx = if external {
        TestContext::new_with_external_cubestore(MockSchema::from_yaml(SCHEMA)?)?
    } else {
        let yaml = SCHEMA.replace("external: true", "external: false");
        assert_ne!(yaml, SCHEMA, "the fixture's rollup should be external");
        TestContext::new(MockSchema::from_yaml(&yaml)?)?
    };
    let options = Rc::new(
        MockBaseQueryOptions::builder()
            .cube_evaluator(ctx.query_tools().cube_evaluator().clone())
            .base_tools(ctx.query_tools().base_tools().clone())
            .join_graph(ctx.query_tools().join_graph().clone())
            .security_context(ctx.security_context().clone())
            .measures(Some(members_from_strings(vec!["visitors.count"])))
            .dimensions(Some(members_from_strings(vec!["visitors.source"])))
            .subquery_joins(subquery_joins)
            .build(),
    );
    ctx.build_sql_with_used_pre_aggregations_from_options(options)
}

// Control: without the join, the same query is served from the rollup.
#[tokio::test(flavor = "multi_thread")]
async fn test_rollup_matches_without_subquery_join() -> Result<(), CubeError> {
    for external in [true, false] {
        let (sql, usages) = plan(external, None)?;

        assert_eq!(usages.len(), 1, "expected the rollup to match, got: {sql}");
        assert_eq!(usages[0].name(), "daily_rollup");
        assert_eq!(usages[0].pre_aggregation.external(), external);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_inner_subquery_join_skips_rollup() -> Result<(), CubeError> {
    for external in [true, false] {
        let (sql, usages) = plan(external, Some(vec![top_sources_join("INNER")?]))?;

        assert!(usages.is_empty(), "expected no rollup, got: {sql}");
        assert!(
            !sql.contains("daily_rollup"),
            "expected no rollup table, got: {sql}"
        );
        assert!(
            sql.contains("INNER JOIN") && sql.contains(TOP_SOURCES_SUBQUERY),
            "expected the sub-query join, got: {sql}"
        );
        assert!(
            sql.contains("\"top_sources\".source"),
            "expected the join condition, got: {sql}"
        );
    }
    Ok(())
}

// A LEFT join doesn't filter, but it can still fan out rows and its columns
// can be read by the outer query, so it skips the rollup too.
#[tokio::test(flavor = "multi_thread")]
async fn test_left_subquery_join_skips_rollup() -> Result<(), CubeError> {
    for external in [true, false] {
        let (sql, usages) = plan(external, Some(vec![top_sources_join("LEFT")?]))?;

        assert!(usages.is_empty(), "expected no rollup, got: {sql}");
        assert!(
            sql.contains("LEFT JOIN") && sql.contains(TOP_SOURCES_SUBQUERY),
            "expected the sub-query join, got: {sql}"
        );
    }
    Ok(())
}
