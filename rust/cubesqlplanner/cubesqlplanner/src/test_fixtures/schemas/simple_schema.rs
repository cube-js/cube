use crate::test_fixtures::cube_bridge::MockSchema;

pub fn create_simple_schema() -> MockSchema {
    MockSchema::from_yaml_file("common/simple.yaml").expect("Failed to load simple schema")
}
