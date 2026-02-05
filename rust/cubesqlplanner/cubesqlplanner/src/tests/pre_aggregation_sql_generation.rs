//! High-level tests for SQL generation with pre-aggregations
//!
//! These tests verify that queries correctly match and use pre-aggregations,
//! checking that the generated SQL contains references to pre-aggregation tables.

use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::logical_plan::PreAggregationOptimizer;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::planners::QueryPlanner;
use crate::planner::query_tools::QueryTools;
use crate::planner::QueryProperties;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::rc::Rc;

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

    let sql = test_context
        .build_sql(query_yaml)
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
