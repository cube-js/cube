use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_ownership_test_schema() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
            - name: users
              sql: "SELECT 1"
              joins:
                  - name: orders
                    relationship: one_to_many
                    sql: "{users}.id = {orders.user_id}"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: userName
                    type: string
                    sql: "{CUBE}.user_name"
                  - name: userNameProxy
                    type: string
                    sql: "{CUBE.user_name}"
                  - name: secondId
                    type: number
                    sql: "{CUBE}.secondId"
                  - name: complexId
                    type: number
                    sql: "{id} % {users.secondId}"
              measures:
                  - name: count
                    type: count
            - name: orders
              sql: "SELECT 1"
              dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: orderUserName
                    type: string
                    sql: "{users.userName}"
              measures:
                  - name: count
                    type: count
        views:
            - name: users_to_orders
              cubes:
                  - join_path: users
                    includes: "*"
                  - join_path: users.orders
                    includes:
                        - orderUserName
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

#[test]
fn dimensions_ownships() {
    let schema = create_ownership_test_schema();
    let context = TestContext::new(schema).unwrap();
    let symbol = context.create_dimension("users.id").unwrap();
    assert!(symbol.owned_by_cube());
}
