//! High-level tests for SQL generation with pre-aggregations
//!
//! These tests verify that queries correctly match and use pre-aggregations,
//! checking that the generated SQL contains references to pre-aggregation tables.

use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::planners::QueryPlanner;
use crate::planner::query_tools::QueryTools;
use crate::planner::QueryProperties;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::rc::Rc;

/// Helper function to build SQL from query options
fn build_sql_without_pre_agg(
    query_tools: Rc<QueryTools>,
    options: Rc<dyn BaseQueryOptions>,
) -> Result<String, cubenativeutils::CubeError> {
    let request = QueryProperties::try_new(query_tools.clone(), options.clone())?;
    let query_planner = QueryPlanner::new(request.clone(), query_tools.clone());
    let logical_plan = query_planner.plan()?;

    let templates = query_tools.plan_sql_templates(false)?;
    let physical_plan_builder = PhysicalPlanBuilder::new(query_tools.clone(), templates.clone());

    let physical_plan = physical_plan_builder.build(
        logical_plan,
        std::collections::HashMap::new(),
        request.is_total_query(),
    )?;

    let sql = physical_plan.to_sql(&templates)?;
    let (result_sql, _params) = query_tools.build_sql_and_params(&sql, true, &templates)?;

    Ok(result_sql)
}

#[test]
fn test_basic_sql_generation_without_pre_agg() {
    let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
    let test_context = TestContext::new(schema).unwrap();

    // Simple query: count by source without trying to use pre-aggregations
    let query_yaml = indoc! {"
        measures:
          - visitors.count
        dimensions:
          - visitors.source
    "};

    let options = test_context.create_query_options_from_yaml(query_yaml);
    let sql = build_sql_without_pre_agg(test_context.query_tools().clone(), options)
        .expect("Should generate SQL without pre-aggregations");

    println!("Generated SQL (no pre-agg optimization):\n{}", sql);

    // Basic checks
    assert!(!sql.is_empty(), "SQL should not be empty");
    assert!(
        sql.to_lowercase().contains("visitors"),
        "SQL should reference visitors table"
    );
    assert!(
        sql.to_lowercase().contains("count"),
        "SQL should contain COUNT aggregation"
    );
}
