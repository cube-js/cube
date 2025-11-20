use crate::test_fixtures::cube_bridge::MockSchema;

/// Creates a simple schema with orders and customers cubes
///
/// Loads from: `src/test_fixtures/schemas/yaml_files/common/simple.yaml`
///
/// This schema demonstrates:
/// - Two cubes with basic dimensions and measures
/// - Single many-to-one join from orders to customers
/// - Standard measure types (count, max, min)
pub fn create_simple_schema() -> MockSchema {
    MockSchema::from_yaml_file("common/simple.yaml").expect("Failed to load simple schema")
}
