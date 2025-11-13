//! Tests for Compiler member evaluation

use crate::planner::sql_evaluator::Compiler;
use crate::test_fixtures::schemas::{create_visitors_schema, TestCompiler};

#[test]
fn test_add_dimension_evaluator_number_dimension() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.id".to_string())
        .unwrap();

    assert!(symbol.is_dimension());
    assert!(!symbol.is_measure());
    assert_eq!(symbol.full_name(), "visitors.id");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.name(), "id");
    assert_eq!(symbol.get_dependencies().len(), 0);
    assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "number");
}

#[test]
fn test_add_dimension_evaluator_string_dimension() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.source".to_string())
        .unwrap();

    assert!(symbol.is_dimension());
    assert!(!symbol.is_measure());
    assert_eq!(symbol.full_name(), "visitors.source");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.name(), "source");
    assert_eq!(symbol.get_dependencies().len(), 0);
    assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "string");
}

#[test]
fn test_add_dimension_evaluator_caching() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol1 = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.id".to_string())
        .unwrap();
    let symbol2 = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.id".to_string())
        .unwrap();

    assert_eq!(symbol1.full_name(), symbol2.full_name());
}

#[test]
fn test_add_dimension_evaluator_invalid_path() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let result = test_compiler
        .compiler
        .add_dimension_evaluator("nonexistent.dimension".to_string());

    assert!(result.is_err());
}

#[test]
fn test_add_dimension_evaluator_multiple_dimensions() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let id_symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.id".to_string())
        .unwrap();
    let source_symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.source".to_string())
        .unwrap();
    let created_at_symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.created_at".to_string())
        .unwrap();

    assert_eq!(id_symbol.full_name(), "visitors.id");
    assert_eq!(id_symbol.as_dimension().unwrap().dimension_type(), "number");
    assert_eq!(source_symbol.full_name(), "visitors.source");
    assert_eq!(
        source_symbol.as_dimension().unwrap().dimension_type(),
        "string"
    );
    assert_eq!(created_at_symbol.full_name(), "visitors.created_at");
    assert_eq!(
        created_at_symbol.as_dimension().unwrap().dimension_type(),
        "time"
    );
    assert_eq!(id_symbol.get_dependencies().len(), 0);
    assert_eq!(source_symbol.get_dependencies().len(), 0);
    assert_eq!(created_at_symbol.get_dependencies().len(), 0);
}

#[test]
fn test_add_measure_evaluator_count_measure() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol = test_compiler
        .compiler
        .add_measure_evaluator("visitor_checkins.count".to_string())
        .unwrap();

    assert!(symbol.is_measure());
    assert!(!symbol.is_dimension());
    assert_eq!(symbol.full_name(), "visitor_checkins.count");
    assert_eq!(symbol.cube_name(), "visitor_checkins");
    assert_eq!(symbol.name(), "count");
    assert_eq!(symbol.get_dependencies().len(), 0);
    assert_eq!(symbol.as_measure().unwrap().measure_type(), "count");
}

#[test]
fn test_add_measure_evaluator_sum_measure() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol = test_compiler
        .compiler
        .add_measure_evaluator("visitors.total_revenue".to_string())
        .unwrap();

    assert!(symbol.is_measure());
    assert!(!symbol.is_dimension());
    assert_eq!(symbol.full_name(), "visitors.total_revenue");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.name(), "total_revenue");
    assert_eq!(symbol.get_dependencies().len(), 0);
    assert_eq!(symbol.as_measure().unwrap().measure_type(), "sum");
}

#[test]
fn test_add_measure_evaluator_caching() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol1 = test_compiler
        .compiler
        .add_measure_evaluator("visitors.total_revenue".to_string())
        .unwrap();
    let symbol2 = test_compiler
        .compiler
        .add_measure_evaluator("visitors.total_revenue".to_string())
        .unwrap();

    assert_eq!(symbol1.full_name(), symbol2.full_name());
}

#[test]
fn test_add_measure_evaluator_invalid_path() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let result = test_compiler
        .compiler
        .add_measure_evaluator("nonexistent.measure".to_string());

    assert!(result.is_err());
}

#[test]
fn test_add_measure_evaluator_multiple_measures() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let count_symbol = test_compiler
        .compiler
        .add_measure_evaluator("visitor_checkins.count".to_string())
        .unwrap();
    let revenue_symbol = test_compiler
        .compiler
        .add_measure_evaluator("visitors.total_revenue".to_string())
        .unwrap();

    assert_eq!(count_symbol.full_name(), "visitor_checkins.count");
    assert_eq!(count_symbol.as_measure().unwrap().measure_type(), "count");
    assert_eq!(revenue_symbol.full_name(), "visitors.total_revenue");
    assert_eq!(revenue_symbol.as_measure().unwrap().measure_type(), "sum");
    assert_eq!(count_symbol.get_dependencies().len(), 0);
    assert_eq!(revenue_symbol.get_dependencies().len(), 0);
}

#[test]
fn test_add_auto_resolved_member_evaluator_dimension() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol = test_compiler
        .compiler
        .add_auto_resolved_member_evaluator("visitors.source".to_string())
        .unwrap();

    assert!(symbol.is_dimension());
    assert!(!symbol.is_measure());
    assert_eq!(symbol.full_name(), "visitors.source");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.name(), "source");
    assert_eq!(symbol.get_dependencies().len(), 0);
    assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "string");
}

#[test]
fn test_add_auto_resolved_member_evaluator_measure() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol = test_compiler
        .compiler
        .add_auto_resolved_member_evaluator("visitors.total_revenue".to_string())
        .unwrap();

    assert!(symbol.is_measure());
    assert!(!symbol.is_dimension());
    assert_eq!(symbol.full_name(), "visitors.total_revenue");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.name(), "total_revenue");
    assert_eq!(symbol.get_dependencies().len(), 0);
    assert_eq!(symbol.as_measure().unwrap().measure_type(), "sum");
}

#[test]
fn test_add_cube_table_evaluator() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol = test_compiler
        .compiler
        .add_cube_table_evaluator("visitors".to_string())
        .unwrap();

    assert!(symbol.is_cube());
    assert!(!symbol.is_dimension());
    assert!(!symbol.is_measure());
    assert_eq!(symbol.full_name(), "visitors");
    assert_eq!(symbol.cube_name(), "visitors");
}

#[test]
fn test_add_cube_name_evaluator() {
    let evaluator = create_visitors_schema().create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let symbol = test_compiler
        .compiler
        .add_cube_name_evaluator("visitors".to_string())
        .unwrap();

    assert!(symbol.is_cube());
    assert!(!symbol.is_dimension());
    assert!(!symbol.is_measure());
    assert_eq!(symbol.full_name(), "visitors");
    assert_eq!(symbol.cube_name(), "visitors");
}

