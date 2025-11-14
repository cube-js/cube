//! Tests for SQL generation for individual symbols

use crate::cube_bridge::base_tools::BaseTools;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::{SqlNode, SqlNodesFactory};
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::test_fixtures::cube_bridge::{
    MockBaseTools, MockCubeDefinition, MockDimensionDefinition, MockJoinGraph, MockSchema,
    MockSchemaBuilder, MockSecurityContext,
};
use cubenativeutils::CubeError;
use std::rc::Rc;

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
    /// Create a new SQL evaluation context with test schema
    pub fn new() -> Self {
        // Create schema and evaluator
        let schema = create_test_schema();
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
fn test_dimension_sql_evaluation() {
    let context = SqlEvaluationContext::new();
    let sql = context.evaluate_dimension("test_cube.id").unwrap();

    println!("Generated SQL: {}", sql);
    assert_eq!(sql, r#""test_cube".id"#);
}

