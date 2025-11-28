//! Tests for Compiler member evaluation

use crate::test_fixtures::{cube_bridge::MockSchema, schemas::TestCompiler};

#[test]
fn test_add_dimension_evaluator_number_dimension() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let result = test_compiler
        .compiler
        .add_dimension_evaluator("nonexistent.dimension".to_string());

    assert!(result.is_err());
}

#[test]
fn test_add_dimension_evaluator_multiple_dimensions() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    let result = test_compiler
        .compiler
        .add_measure_evaluator("nonexistent.measure".to_string());

    assert!(result.is_err());
}

#[test]
fn test_add_measure_evaluator_multiple_measures() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
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

// Tests for dimensions and measures with dependencies

#[test]
fn test_dimension_with_cube_table_dependency() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // visitor_id has dependency on {CUBE.}visitor_id
    let symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.visitor_id".to_string())
        .unwrap();

    assert!(symbol.is_dimension());
    assert_eq!(symbol.full_name(), "visitors.visitor_id");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "number");

    // Should have 1 dependency: CubeTable
    let dependencies = symbol.get_dependencies();
    assert_eq!(dependencies.len(), 1, "Should have 1 dependency on CUBE");

    let dep = &dependencies[0];
    assert!(dep.is_cube(), "Dependency should be a cube symbol");
    assert_eq!(dep.full_name(), "visitors");
    assert_eq!(dep.cube_name(), "visitors");
}

#[test]
fn test_dimension_with_member_dependency_no_prefix() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // visitor_id_twice has dependency on {visitor_id} without cube prefix
    let symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.visitor_id_twice".to_string())
        .unwrap();

    assert!(symbol.is_dimension());
    assert_eq!(symbol.full_name(), "visitors.visitor_id_twice");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "number");

    // Should have 1 dependency: visitor_id dimension
    let dependencies = symbol.get_dependencies();
    assert_eq!(
        dependencies.len(),
        1,
        "Should have 1 dependency on visitor_id"
    );

    let dep = &dependencies[0];
    assert!(dep.is_dimension(), "Dependency should be a dimension");
    assert_eq!(dep.full_name(), "visitors.visitor_id");
    assert_eq!(dep.cube_name(), "visitors");
}

#[test]
fn test_dimension_with_mixed_dependencies() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // source_concat_id has dependencies on {CUBE.source} and {visitors.visitor_id}
    let symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.source_concat_id".to_string())
        .unwrap();

    assert!(symbol.is_dimension());
    assert_eq!(symbol.full_name(), "visitors.source_concat_id");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "string");

    // Should have 2 dependencies: visitors.source and visitors.visitor_id
    let dependencies = symbol.get_dependencies();
    assert_eq!(
        dependencies.len(),
        2,
        "Should have 2 dimension dependencies"
    );

    // Both should be dimensions
    for dep in &dependencies {
        assert!(dep.is_dimension(), "All dependencies should be dimensions");
        assert_eq!(dep.cube_name(), "visitors");
    }

    // Check we have both expected dependencies
    let dep_names: Vec<String> = dependencies.iter().map(|d| d.full_name()).collect();
    assert!(
        dep_names.contains(&"visitors.source".to_string()),
        "Should have dependency on visitors.source"
    );
    assert!(
        dep_names.contains(&"visitors.visitor_id".to_string()),
        "Should have dependency on visitors.visitor_id"
    );
}

#[test]
fn test_measure_with_cube_table_dependency() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // revenue has dependency on {CUBE}.revenue
    let symbol = test_compiler
        .compiler
        .add_measure_evaluator("visitors.revenue".to_string())
        .unwrap();

    assert!(symbol.is_measure());
    assert_eq!(symbol.full_name(), "visitors.revenue");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.as_measure().unwrap().measure_type(), "sum");

    // Should have 1 dependency: CubeTable
    let dependencies = symbol.get_dependencies();
    assert_eq!(dependencies.len(), 1, "Should have 1 dependency on CUBE");

    let dep = &dependencies[0];
    assert!(dep.is_cube(), "Dependency should be a cube symbol");
    assert_eq!(dep.full_name(), "visitors");
    assert_eq!(dep.cube_name(), "visitors");
}

#[test]
fn test_measure_with_explicit_cube_and_member_dependencies() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // total_revenue_per_count has dependencies on {visitors.count} and {total_revenue}
    let symbol = test_compiler
        .compiler
        .add_measure_evaluator("visitors.total_revenue_per_count".to_string())
        .unwrap();

    assert!(symbol.is_measure());
    assert_eq!(symbol.full_name(), "visitors.total_revenue_per_count");
    assert_eq!(symbol.cube_name(), "visitors");
    assert_eq!(symbol.as_measure().unwrap().measure_type(), "number");

    // Should have 2 dependencies: visitors.count and total_revenue
    let dependencies = symbol.get_dependencies();
    assert_eq!(dependencies.len(), 2, "Should have 2 measure dependencies");

    // Both should be measures
    for dep in &dependencies {
        assert!(dep.is_measure(), "All dependencies should be measures");
        assert_eq!(dep.cube_name(), "visitors");
    }

    // Check we have both expected dependencies
    let dep_names: Vec<String> = dependencies.iter().map(|d| d.full_name()).collect();
    assert!(
        dep_names.contains(&"visitors.count".to_string()),
        "Should have dependency on visitors.count"
    );
    assert!(
        dep_names.contains(&"visitors.total_revenue".to_string()),
        "Should have dependency on visitors.total_revenue"
    );
}

#[test]
fn test_view_dimension_compilation() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // Compile dimension from view with simple join path
    let id_symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors_visitors_checkins.id".to_string())
        .unwrap();

    // Check basic properties
    assert!(id_symbol.is_dimension());
    assert_eq!(id_symbol.full_name(), "visitors_visitors_checkins.id");
    assert_eq!(id_symbol.cube_name(), "visitors_visitors_checkins");
    assert_eq!(id_symbol.name(), "id");

    // Check that it's a view member
    let dimension = id_symbol.as_dimension().unwrap();
    assert!(dimension.is_view(), "Should be a view member");

    // Check that it's a reference (view members reference original cube members)
    assert!(
        dimension.is_reference(),
        "Should be a reference to original member"
    );

    // Resolve reference chain to get the original member
    let resolved = id_symbol.clone().resolve_reference_chain();
    assert_eq!(
        resolved.full_name(),
        "visitors.id",
        "Should resolve to visitors.id"
    );
    assert!(
        !resolved.as_dimension().unwrap().is_view(),
        "Resolved member should not be a view"
    );

    // Compile dimension from view with long join path
    let visitor_id_symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors_visitors_checkins.visitor_id".to_string())
        .unwrap();

    assert!(visitor_id_symbol.is_dimension());
    assert_eq!(
        visitor_id_symbol.full_name(),
        "visitors_visitors_checkins.visitor_id"
    );

    let visitor_id_dim = visitor_id_symbol.as_dimension().unwrap();
    assert!(visitor_id_dim.is_view(), "Should be a view member");
    assert!(visitor_id_dim.is_reference(), "Should be a reference");

    // Resolve to original member from visitor_checkins cube
    let resolved = visitor_id_symbol.clone().resolve_reference_chain();
    assert_eq!(
        resolved.full_name(),
        "visitor_checkins.visitor_id",
        "Should resolve to visitor_checkins.visitor_id"
    );
    assert!(
        !resolved.as_dimension().unwrap().is_view(),
        "Resolved member should not be a view"
    );
}

#[test]
fn test_view_measure_compilation() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // Compile measure from view with long join path
    let count_symbol = test_compiler
        .compiler
        .add_measure_evaluator("visitors_visitors_checkins.count".to_string())
        .unwrap();

    // Check basic properties
    assert!(count_symbol.is_measure());
    assert_eq!(count_symbol.full_name(), "visitors_visitors_checkins.count");
    assert_eq!(count_symbol.cube_name(), "visitors_visitors_checkins");
    assert_eq!(count_symbol.name(), "count");

    // Check that it's a view member
    let measure = count_symbol.as_measure().unwrap();
    assert!(measure.is_view(), "Should be a view member");

    // Check that it's a reference
    assert!(
        measure.is_reference(),
        "Should be a reference to original member"
    );

    // Resolve reference chain to get the original member
    let resolved = count_symbol.clone().resolve_reference_chain();
    assert_eq!(
        resolved.full_name(),
        "visitor_checkins.count",
        "Should resolve to visitor_checkins.count"
    );
    assert!(
        !resolved.as_measure().unwrap().is_view(),
        "Resolved member should not be a view"
    );
}

#[test]
fn test_proxy_dimension_compilation() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // Compile proxy dimension that references another dimension
    let proxy_symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.visitor_id_proxy".to_string())
        .unwrap();

    // Check basic properties
    assert!(proxy_symbol.is_dimension());
    assert_eq!(proxy_symbol.full_name(), "visitors.visitor_id_proxy");
    assert_eq!(proxy_symbol.cube_name(), "visitors");
    assert_eq!(proxy_symbol.name(), "visitor_id_proxy");

    let dimension = proxy_symbol.as_dimension().unwrap();

    // Check that it's NOT a view member
    assert!(!dimension.is_view(), "Proxy should not be a view member");

    // Check that it IS a reference (proxy references another member)
    assert!(
        dimension.is_reference(),
        "Proxy should be a reference to another member"
    );

    // Resolve reference chain to get the target member
    let resolved = proxy_symbol.clone().resolve_reference_chain();
    assert_eq!(
        resolved.full_name(),
        "visitors.visitor_id",
        "Proxy should resolve to visitors.visitor_id"
    );

    // Verify the resolved member is not a view
    assert!(
        !resolved.as_dimension().unwrap().is_view(),
        "Target member should not be a view"
    );

    // Verify the resolved member is also not a reference (it's the actual dimension)
    assert!(
        !resolved.as_dimension().unwrap().is_reference(),
        "Target member should not be a reference"
    );
}

#[test]
fn test_proxy_measure_compilation() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // Compile proxy measure that references another measure
    let proxy_symbol = test_compiler
        .compiler
        .add_measure_evaluator("visitors.total_revenue_proxy".to_string())
        .unwrap();

    // Check basic properties
    assert!(proxy_symbol.is_measure());
    assert_eq!(proxy_symbol.full_name(), "visitors.total_revenue_proxy");
    assert_eq!(proxy_symbol.cube_name(), "visitors");
    assert_eq!(proxy_symbol.name(), "total_revenue_proxy");

    let measure = proxy_symbol.as_measure().unwrap();
    let dependencies = proxy_symbol.get_dependencies();
    assert_eq!(dependencies.len(), 1, "Should have 1 measure dependencies");

    assert!(!measure.is_view(), "Proxy should not be a view member");

    // Check that it IS a reference (proxy references another member)
    assert!(
        measure.is_reference(),
        "Proxy should be a reference to another member"
    );

    // Resolve reference chain to get the target member
    let resolved = proxy_symbol.clone().resolve_reference_chain();
    assert_eq!(
        resolved.full_name(),
        "visitors.total_revenue",
        "Proxy should resolve to visitors.total_revenue"
    );

    // Verify the resolved member is not a view
    assert!(
        !resolved.as_measure().unwrap().is_view(),
        "Target member should not be a view"
    );

    // Verify the resolved member is not a reference (it's the actual measure)
    assert!(
        !resolved.as_measure().unwrap().is_reference(),
        "Target member should not be a reference"
    );
}

#[test]
fn test_time_dimension_with_granularity_compilation() {
    let schema = MockSchema::from_yaml_file("common/visitors.yaml");
    let evaluator = schema.create_evaluator();
    let mut test_compiler = TestCompiler::new(evaluator);

    // Compile time dimension with month granularity
    let time_symbol = test_compiler
        .compiler
        .add_dimension_evaluator("visitors.created_at.month".to_string())
        .unwrap();

    // Check that it's a time dimension, not a regular dimension
    assert!(
        time_symbol.as_time_dimension().is_ok(),
        "Should be a time dimension"
    );

    // Check full name includes granularity
    assert_eq!(
        time_symbol.full_name(),
        "visitors.created_at_month",
        "Full name should be visitors.created_at_month"
    );
    assert_eq!(time_symbol.cube_name(), "visitors");
    assert_eq!(time_symbol.name(), "created_at");

    // Get as time dimension to check specific properties
    let time_dim = time_symbol.as_time_dimension().unwrap();

    // Check granularity
    assert_eq!(
        time_dim.granularity(),
        &Some("month".to_string()),
        "Granularity should be month"
    );

    // Check that it's NOT a reference
    assert!(
        !time_dim.is_reference(),
        "Time dimension with granularity should not be a reference"
    );

    // Check base symbol - should be the original dimension without granularity
    let base_symbol = time_dim.base_symbol();
    assert!(
        base_symbol.is_dimension(),
        "Base symbol should be a dimension"
    );
    assert_eq!(
        base_symbol.full_name(),
        "visitors.created_at",
        "Base symbol should be visitors.created_at"
    );
    assert_eq!(
        base_symbol.as_dimension().unwrap().dimension_type(),
        "time",
        "Base dimension should be time type"
    );
}
