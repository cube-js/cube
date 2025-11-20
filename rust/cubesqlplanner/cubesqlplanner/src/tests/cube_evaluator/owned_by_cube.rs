use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;

#[test]
fn dimensions_ownships() {
    let schema = MockSchema::from_yaml_file("owned_by_cube/ownership_test.yaml");
    let context = TestContext::new(schema).unwrap();
    let symbol = context.create_dimension("users.id").unwrap();
    assert!(symbol.owned_by_cube());
}
