//! Tests for SQL generation for individual symbols

use crate::cube_bridge::base_tools::BaseTools;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::{SqlNode, SqlNodesFactory};
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::test_fixtures::cube_bridge::{
    MockBaseTools, MockJoinGraph, MockSchema, MockSecurityContext,
};
use cubenativeutils::CubeError;
use indoc::indoc;
use std::rc::Rc;

fn create_count_schema_no_pk() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: users
            sql: "SELECT 1"
            dimensions:
              - name: id
                type: number
                sql: id
              - name: userName
                type: string
                sql: user_name
            measures:
              - name: count
                type: count
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

fn create_count_schema_one_pk() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: users
            sql: "SELECT 1"
            dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: userName
                type: string
                sql: user_name
            measures:
              - name: count
                type: count
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

fn create_count_schema_two_pk() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: users
            sql: "SELECT 1"
            dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: userName
                type: string
                sql: user_name
                primary_key: true
            measures:
              - name: count
                type: count
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

fn create_test_schema() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: test_cube
            sql: "SELECT 1"
            dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: source
                type: string
                sql: "{CUBE}.source"
              - name: source_extended
                type: string
                sql: "CONCAT({CUBE.source}, '_source')"
              - name: created_at
                type: time
                sql: created_at
              - name: location
                type: geo
                latitude: latitude
                longitude: longitude
            measures:
              - name: sum_revenue
                type: sum
                sql: revenue
              - name: min_revenue
                type: min
                sql: revenue
              - name: max_revenue
                type: max
                sql: revenue
              - name: avg_revenue
                type: avg
                sql: revenue
              - name: complex_measure
                type: number
                sql: "{sum_revenue} + {CUBE.avg_revenue}/{test_cube.min_revenue} - {test_cube.min_revenue}"
              - name: count_distinct_id
                type: countDistinct
                sql: id
              - name: count_distinct_approx_id
                type: countDistinctApprox
                sql: id
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

/// Helper structure for SQL evaluation in tests
///
/// Encapsulates all the boilerplate needed to evaluate symbols to SQL:
/// - QueryTools with all mock dependencies
/// - SqlEvaluatorVisitor
/// - PlanSqlTemplates
/// - Default node processor
pub struct SqlEvaluationContext {
    query_tools: Rc<QueryTools>,
    visitor: SqlEvaluatorVisitor,
    templates: PlanSqlTemplates,
    node_processor: Rc<dyn SqlNode>,
}

impl SqlEvaluationContext {
    /// Create a new SQL evaluation context with a custom schema
    pub fn new_with_schema(schema: MockSchema) -> Self {
        let evaluator = schema.create_evaluator();

        // Create QueryTools with mocks
        let security_context = Rc::new(MockSecurityContext);
        let base_tools = Rc::new(MockBaseTools::builder().build());
        let join_graph = Rc::new(MockJoinGraph::new());

        let query_tools = QueryTools::try_new(
            evaluator.clone(),
            security_context,
            base_tools.clone(),
            join_graph,
            None,  // timezone
            false, // export_annotated_sql
        )
        .unwrap();

        // Create SqlEvaluatorVisitor
        let visitor = SqlEvaluatorVisitor::new(query_tools.clone(), None);

        // Create PlanSqlTemplates
        let driver_tools = base_tools.driver_tools(false).unwrap();
        let templates = PlanSqlTemplates::try_new(driver_tools, false).unwrap();

        // Get default node processor
        let node_processor = SqlNodesFactory::default().default_node_processor();

        Self {
            query_tools,
            visitor,
            templates,
            node_processor,
        }
    }

    /// Create a new SQL evaluation context with test schema
    pub fn new() -> Self {
        let schema = create_test_schema();
        Self::new_with_schema(schema)
    }

    /// Evaluate a dimension to SQL
    pub fn evaluate_dimension(&self, path: &str) -> Result<String, CubeError> {
        let mut compiler = self.query_tools.evaluator_compiler().borrow_mut();
        let symbol = compiler.add_dimension_evaluator(path.to_string())?;
        drop(compiler); // Release borrow before calling visitor

        self.visitor
            .apply(&symbol, self.node_processor.clone(), &self.templates)
    }

    /// Evaluate a measure to SQL
    #[allow(dead_code)]
    pub fn evaluate_measure(&self, path: &str) -> Result<String, CubeError> {
        let mut compiler = self.query_tools.evaluator_compiler().borrow_mut();
        let symbol = compiler.add_measure_evaluator(path.to_string())?;
        drop(compiler); // Release borrow before calling visitor

        self.visitor
            .apply(&symbol, self.node_processor.clone(), &self.templates)
    }
}

#[test]
fn simple_dimension_sql_evaluation() {
    let context = SqlEvaluationContext::new();

    // Test simple dimension without dependencies
    let id_sql = context.evaluate_dimension("test_cube.id").unwrap();
    assert_eq!(id_sql, r#""test_cube".id"#);

    // Test dimension with {CUBE} reference
    let source_sql = context.evaluate_dimension("test_cube.source").unwrap();
    assert_eq!(source_sql, r#""test_cube".source"#);

    // Test time dimension
    let created_at_sql = context.evaluate_dimension("test_cube.created_at").unwrap();
    assert_eq!(created_at_sql, r#""test_cube".created_at"#);

    // Test geo dimension (latitude || ',' || longitude)
    let location_sql = context.evaluate_dimension("test_cube.location").unwrap();
    assert_eq!(location_sql, "latitude || ',' || longitude");

    // Test time dimension with granularity (day)
    let created_at_day_sql = context
        .evaluate_dimension("test_cube.created_at.day")
        .unwrap();
    assert_eq!(
        created_at_day_sql,
        "date_trunc('day', (\"test_cube\".created_at::timestamptz AT TIME ZONE 'UTC'))"
    );
}

#[test]
fn simple_aggregate_measures() {
    let context = SqlEvaluationContext::new();

    // Test SUM measure
    let sum_sql = context.evaluate_measure("test_cube.sum_revenue").unwrap();
    assert_eq!(sum_sql, r#"sum("test_cube".revenue)"#);

    // Test MIN measure
    let min_sql = context.evaluate_measure("test_cube.min_revenue").unwrap();
    assert_eq!(min_sql, r#"min("test_cube".revenue)"#);

    // Test MAX measure
    let max_sql = context.evaluate_measure("test_cube.max_revenue").unwrap();
    assert_eq!(max_sql, r#"max("test_cube".revenue)"#);

    // Test AVG measure
    let avg_sql = context.evaluate_measure("test_cube.avg_revenue").unwrap();
    assert_eq!(avg_sql, r#"avg("test_cube".revenue)"#);

    // Test COUNT DISTINCT measure
    let count_distinct_sql = context
        .evaluate_measure("test_cube.count_distinct_id")
        .unwrap();
    assert_eq!(count_distinct_sql, r#"COUNT(DISTINCT "test_cube".id)"#);

    // Test COUNT DISTINCT APPROX measure
    let count_distinct_approx_sql = context
        .evaluate_measure("test_cube.count_distinct_approx_id")
        .unwrap();
    assert_eq!(
        count_distinct_approx_sql,
        r#"round(hll_cardinality(hll_add_agg(hll_hash_any("test_cube".id))))"#
    );
}

#[test]
fn count_measure_variants() {
    // Test COUNT with no primary keys - should use COUNT(*)
    let schema_no_pk = create_count_schema_no_pk();
    let context_no_pk = SqlEvaluationContext::new_with_schema(schema_no_pk);
    let count_no_pk_sql = context_no_pk.evaluate_measure("users.count").unwrap();
    assert_eq!(count_no_pk_sql, "count(*)");

    // Test COUNT with one primary key - should use count(pk)
    let schema_one_pk = create_count_schema_one_pk();
    let context_one_pk = SqlEvaluationContext::new_with_schema(schema_one_pk);
    let count_one_pk_sql = context_one_pk.evaluate_measure("users.count").unwrap();
    assert_eq!(count_one_pk_sql, r#"count("users".id)"#);

    // Test COUNT with two primary keys - should use count(CAST(pk1) || CAST(pk2))
    let schema_two_pk = create_count_schema_two_pk();
    let context_two_pk = SqlEvaluationContext::new_with_schema(schema_two_pk);
    let count_two_pk_sql = context_two_pk.evaluate_measure("users.count").unwrap();
    assert_eq!(
        count_two_pk_sql,
        "count(CAST(id AS STRING) || CAST(user_name AS STRING))"
    );
}

#[test]
fn composite_symbols() {
    let context = SqlEvaluationContext::new();

    // Test dimension with member dependency ({CUBE.source})
    let source_extended_sql = context
        .evaluate_dimension("test_cube.source_extended")
        .unwrap();
    assert_eq!(
        source_extended_sql,
        r#"CONCAT("test_cube".source, '_source')"#
    );

    // Test measure with multiple member dependencies
    // {sum_revenue} + {CUBE.avg_revenue}/{test_cube.min_revenue} - {test_cube.min_revenue}
    let complex_measure_sql = context
        .evaluate_measure("test_cube.complex_measure")
        .unwrap();
    assert_eq!(
        complex_measure_sql,
        r#"sum("test_cube".revenue) + avg("test_cube".revenue)/min("test_cube".revenue) - min("test_cube".revenue)"#
    );
}
