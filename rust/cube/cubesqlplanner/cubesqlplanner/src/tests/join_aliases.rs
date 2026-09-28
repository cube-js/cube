use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn bridged_joins(schema: &MockSchema, cube: &str) -> Vec<(String, Option<String>)> {
    let definition = &schema.get_cube(cube).unwrap().definition;
    CubeDefinition::joins(definition)
        .unwrap()
        .unwrap_or_default()
        .iter()
        .map(|join| {
            let static_data = join.static_data();
            (static_data.name.clone(), static_data.alias.clone())
        })
        .collect()
}

fn join_graph_error(yaml: &str) -> String {
    MockSchema::from_yaml(yaml)
        .unwrap()
        .create_join_graph()
        .err()
        .expect("join graph should reject the joins")
        .message
}

#[test]
fn test_cube_exposes_aliased_joins() {
    let schema = MockSchema::from_yaml_file("common/join_aliases.yaml");

    assert_eq!(
        bridged_joins(&schema, "orders"),
        vec![
            ("users".to_string(), Some("customer".to_string())),
            ("users".to_string(), Some("manager".to_string())),
            ("products".to_string(), None),
        ]
    );
    assert_eq!(
        bridged_joins(&schema, "invoices"),
        vec![("users".to_string(), Some("payer".to_string()))]
    );
}

#[test]
fn test_unaliased_join_next_to_aliased_ones() {
    let schema = MockSchema::from_yaml_file("common/join_aliases.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let sql = test_context
        .build_sql(indoc! {r#"
            measures:
              - orders.count
            dimensions:
              - products.name
        "#})
        .unwrap();

    assert!(sql.contains("products"), "{sql}");
    assert!(!sql.contains("users"), "{sql}");
}

#[test]
fn test_aliased_target_is_not_reachable_by_cube_name() {
    let schema = MockSchema::from_yaml_file("common/join_aliases.yaml");
    let test_context = TestContext::new(schema).unwrap();

    let err = test_context
        .build_sql(indoc! {r#"
            measures:
              - orders.count
            dimensions:
              - users.city
        "#})
        .unwrap_err();

    assert!(
        err.message.contains("Can't find join path"),
        "{}",
        err.message
    );
}

#[test]
fn test_rejects_unaliased_joins_to_same_cube() {
    let message = join_graph_error(indoc! {r#"
        cubes:
          - name: orders
            sql_table: orders
            joins:
              - name: users
                sql: "{CUBE}.customer_id = {users}.id"
                relationship: many_to_one
              - name: users
                sql: "{CUBE}.manager_id = {users}.id"
                relationship: many_to_one
          - name: users
            sql_table: users
    "#});

    assert!(message.contains("declares 2 joins to 'users'"), "{message}");
}

#[test]
fn test_rejects_mixed_aliased_and_unaliased_joins_to_same_cube() {
    let message = join_graph_error(indoc! {r#"
        cubes:
          - name: orders
            sql_table: orders
            joins:
              - name: users
                alias: customer
                sql: "{CUBE}.customer_id = {users}.id"
                relationship: many_to_one
              - name: users
                sql: "{CUBE}.manager_id = {users}.id"
                relationship: many_to_one
          - name: users
            sql_table: users
    "#});

    assert!(message.contains("declares 2 joins to 'users'"), "{message}");
}

#[test]
fn test_rejects_duplicate_alias() {
    let message = join_graph_error(indoc! {r#"
        cubes:
          - name: orders
            sql_table: orders
            joins:
              - name: users
                alias: person
                sql: "{CUBE}.customer_id = {users}.id"
                relationship: many_to_one
              - name: managers
                alias: person
                sql: "{CUBE}.manager_id = {managers}.id"
                relationship: many_to_one
          - name: users
            sql_table: users
          - name: managers
            sql_table: managers
    "#});

    assert!(
        message.contains("several joins named 'person'"),
        "{message}"
    );
}

#[test]
fn test_rejects_alias_equal_to_joined_cube() {
    let message = join_graph_error(indoc! {r#"
        cubes:
          - name: orders
            sql_table: orders
            joins:
              - name: users
                alias: users
                sql: "{CUBE}.customer_id = {users}.id"
                relationship: many_to_one
          - name: users
            sql_table: users
    "#});

    assert!(
        message.contains("with the alias 'users', which is the name of the joined cube"),
        "{message}"
    );
}
