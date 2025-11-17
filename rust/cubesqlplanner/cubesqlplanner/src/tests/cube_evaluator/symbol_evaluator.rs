//! Tests for SQL generation for individual symbols

use crate::cube_bridge::base_tools::BaseTools;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::{SqlNode, SqlNodesFactory};
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::test_fixtures::cube_bridge::{
    MockBaseTools, MockCubeDefinition, MockDimensionDefinition, MockJoinGraph,
    MockMeasureDefinition, MockSchema, MockSchemaBuilder, MockSecurityContext,
};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Creates a schema for count measure testing with no primary keys
fn create_count_schema_no_pk() -> MockSchema {
    MockSchemaBuilder::new()
        .add_cube("users")
        .cube_def(
            MockCubeDefinition::builder()
                .name("users".to_string())
                .sql("SELECT 1".to_string())
                .build(),
        )
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .build(),
        )
        .add_dimension(
            "userName",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("user_name".to_string())
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .build(),
        )
        .finish_cube()
        .build()
}

/// Creates a schema for count measure testing with one primary key
fn create_count_schema_one_pk() -> MockSchema {
    MockSchemaBuilder::new()
        .add_cube("users")
        .cube_def(
            MockCubeDefinition::builder()
                .name("users".to_string())
                .sql("SELECT 1".to_string())
                .build(),
        )
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_dimension(
            "userName",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("user_name".to_string())
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .build(),
        )
        .finish_cube()
        .build()
}

/// Creates a schema for count measure testing with two primary keys
fn create_count_schema_two_pk() -> MockSchema {
    MockSchemaBuilder::new()
        .add_cube("users")
        .cube_def(
            MockCubeDefinition::builder()
                .name("users".to_string())
                .sql("SELECT 1".to_string())
                .build(),
        )
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_dimension(
            "userName",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("user_name".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .build(),
        )
        .finish_cube()
        .build()
}

/// Creates a test schema for symbol SQL generation tests
fn create_test_schema() -> MockSchema {
    MockSchemaBuilder::new()
        .add_cube("test_cube")
        .cube_def(
            MockCubeDefinition::builder()
                .name("test_cube".to_string())
                .sql("SELECT 1".to_string())
                .build(),
        )
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_dimension(
            "source",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("{CUBE}.source".to_string())
                .build(),
        )
        .add_dimension(
            "source_extended",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("CONCAT({CUBE.source}, '_source')".to_string())
                .build(),
        )
        .add_dimension(
            "created_at",
            MockDimensionDefinition::builder()
                .dimension_type("time".to_string())
                .sql("created_at".to_string())
                .build(),
        )
        .add_dimension(
            "location",
            MockDimensionDefinition::builder()
                .dimension_type("geo".to_string())
                .latitude("latitude".to_string())
                .longitude("longitude".to_string())
                .build(),
        )
        .add_measure(
            "sum_revenue",
            MockMeasureDefinition::builder()
                .measure_type("sum".to_string())
                .sql("revenue".to_string())
                .build(),
        )
        .add_measure(
            "min_revenue",
            MockMeasureDefinition::builder()
                .measure_type("min".to_string())
                .sql("revenue".to_string())
                .build(),
        )
        .add_measure(
            "max_revenue",
            MockMeasureDefinition::builder()
                .measure_type("max".to_string())
                .sql("revenue".to_string())
                .build(),
        )
        .add_measure(
            "avg_revenue",
            MockMeasureDefinition::builder()
                .measure_type("avg".to_string())
                .sql("revenue".to_string())
                .build(),
        )
        .add_measure(
            "complex_measure",
            MockMeasureDefinition::builder()
                .measure_type("number".to_string())
                .sql("{sum_revenue} + {CUBE.avg_revenue}/{test_cube.min_revenue} - {test_cube.min_revenue}".to_string())
                .build(),
        )
        .add_measure(
            "count_distinct_id",
            MockMeasureDefinition::builder()
                .measure_type("countDistinct".to_string())
                .sql("id".to_string())
                .build(),
        )
        .add_measure(
            "count_distinct_approx_id",
            MockMeasureDefinition::builder()
                .measure_type("countDistinctApprox".to_string())
                .sql("id".to_string())
                .build(),
        )
        .finish_cube()
        .build()
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
        let join_graph = Rc::new(MockJoinGraph);

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
