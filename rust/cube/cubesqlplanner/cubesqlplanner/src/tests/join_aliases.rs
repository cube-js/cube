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

fn context() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file("common/join_aliases.yaml")).unwrap()
}

fn sql(query: &str) -> String {
    context().build_sql(query).unwrap()
}

fn sql_error(query: &str) -> String {
    context().build_sql(query).unwrap_err().message
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
            (
                "retail_calendar".to_string(),
                Some("calendar_created".to_string())
            ),
            (
                "retail_calendar".to_string(),
                Some("calendar_completed".to_string())
            ),
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

#[test]
fn test_two_aliases_of_one_cube_in_one_query() {
    let sql = sql(indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer.city
          - orders.manager.city
    "#});

    assert!(
        sql.contains(r#"LEFT JOIN users AS "orders__customer" ON "orders".customer_id = "orders__customer".id"#)
            || sql.contains(r#"users  AS "orders__customer" ON "orders".customer_id = "orders__customer".id"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#"AS "orders__manager" ON "orders".manager_id = "orders__manager".id"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#""orders__customer".city "orders__customer__city""#),
        "{sql}"
    );
    assert!(
        sql.contains(r#""orders__manager".city "orders__manager__city""#),
        "{sql}"
    );
    assert!(!sql.contains(r#"AS "users""#), "{sql}");
}

#[test]
fn test_single_alias_without_measure() {
    let sql = sql(indoc! {r#"
        dimensions:
          - orders.customer.city
    "#});

    assert!(
        sql.contains(r#"FROM orders AS "orders""#) || sql.contains(r#"orders  AS "orders""#),
        "{sql}"
    );
    assert!(sql.contains(r#"AS "orders__customer""#), "{sql}");
}

#[test]
fn test_join_below_an_alias_is_its_own_instance() {
    let sql = sql(indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.manager.departments.name
          - orders.customer.departments.name
    "#});

    assert!(
        sql.contains(r#"AS "orders__manager__departments" ON "orders__manager".department_id = "orders__manager__departments".id"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#"AS "orders__customer__departments" ON "orders__customer".department_id = "orders__customer__departments".id"#),
        "{sql}"
    );
}

#[test]
fn test_instance_member_reading_a_joined_cube() {
    let sql = sql(indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer.department_name
          - orders.manager.department_name
    "#});

    assert!(
        sql.contains(r#""orders__customer__departments".name"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#""orders__manager__departments".name"#),
        "{sql}"
    );
}

#[test]
fn test_self_join() {
    let sql = sql(indoc! {r#"
        dimensions:
          - employees.name
          - employees.supervisor.name
          - employees.supervisor.supervisor.name
    "#});

    assert!(
        sql.contains(r#"AS "employees__supervisor" ON "employees".supervisor_id = "employees__supervisor".id"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#"AS "employees__supervisor__supervisor" ON "employees__supervisor".supervisor_id = "employees__supervisor__supervisor".id"#),
        "{sql}"
    );
}

#[test]
fn test_alias_in_declaring_cube_member() {
    let sql = sql(indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.customer_city
    "#});

    assert!(
        sql.contains(r#""orders__customer".city "orders__customer_city""#),
        "{sql}"
    );
    assert!(sql.contains(r#"AS "orders__customer""#), "{sql}");
}

#[test]
fn test_measure_of_an_instance() {
    let sql = sql(indoc! {r#"
        measures:
          - orders.customer.total_score
        dimensions:
          - orders.status
    "#});

    // Orders repeat a customer, so the customer's score is summed over the
    // distinct customers each status has.
    assert!(sql.contains("SELECT DISTINCT"), "{sql}");
    assert!(
        sql.contains(r#"ON "orders__customer_key_orders".customer_id = "orders__customer_key_orders__customer".id"#),
        "{sql}"
    );
    assert!(sql.contains(r#""orders__customer__total_score""#), "{sql}");
    assert!(!sql.contains("orders.customer"), "{sql}");
}

#[test]
fn test_filter_and_segment_on_an_instance() {
    let sql = sql(indoc! {r#"
        measures:
          - orders.count
        filters:
          - member: orders.manager.city
            operator: equals
            values:
              - Paris
        segments:
          - orders.customer.berliners
    "#});

    assert!(
        sql.contains(r#""orders__manager".city = $"#)
            || sql.contains(r#"("orders__manager".city = $"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#""orders__customer".city = 'Berlin'"#),
        "{sql}"
    );
}

#[test]
fn test_view_through_an_alias() {
    let ctx = context();
    let city = ctx.create_dimension("orders_view.customer_city").unwrap();
    let target = city.clone().resolve_reference_chain();
    assert_eq!(target.full_name(), "orders.customer.city");

    let sql = ctx
        .build_sql(indoc! {r#"
            measures:
              - orders_view.count
            dimensions:
              - orders_view.customer_city
        "#})
        .unwrap();
    assert!(
        sql.contains(r#"AS "orders__customer" ON "orders".customer_id = "orders__customer".id"#),
        "{sql}"
    );
}

#[test]
fn test_calendar_instances_keep_their_own_join() {
    let sql = sql(indoc! {r#"
        measures:
          - orders.count
        dimensions:
          - orders.calendar_created.date_val
          - orders.calendar_completed.date_val
    "#});

    assert!(
        sql.contains(r#"AS "orders__calendar_created" ON "orders".created_at = "orders__calendar_created".date_val"#),
        "{sql}"
    );
    assert!(
        sql.contains(r#"AS "orders__calendar_completed" ON "orders".completed_at = "orders__calendar_completed".date_val"#),
        "{sql}"
    );
}

#[test]
fn test_rollup_of_the_target_does_not_serve_an_instance() {
    let ctx = context();
    let (_, pre_aggregations) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {r#"
            measures:
              - orders.customer.count
            dimensions:
              - orders.customer.city
        "#})
        .unwrap();
    assert!(pre_aggregations.is_empty());

    let (_, pre_aggregations) = ctx
        .build_sql_with_used_pre_aggregations(indoc! {r#"
            measures:
              - users.count
            dimensions:
              - users.city
        "#})
        .unwrap();
    assert_eq!(pre_aggregations.len(), 1);
}

#[test]
fn test_bare_alias_is_not_a_cube() {
    let message = sql_error(indoc! {r#"
        dimensions:
          - customer.city
    "#});
    assert!(message.contains("customer"), "{message}");
}

#[test]
fn test_instance_reaches_only_joined_cubes() {
    let message = sql_error(indoc! {r#"
        dimensions:
          - orders.customer.products.name
    "#});
    assert!(message.contains("products"), "{message}");
}

#[test]
fn test_multi_stage_filter_of_an_instance_member() {
    let sql = sql(indoc! {r#"
        measures:
          - orders.customer.berlin_score
        dimensions:
          - orders.status
    "#});

    assert!(sql.contains(r#".city = $"#), "{sql}");
    assert!(!sql.contains(r#""users".city"#), "{sql}");
    assert!(!sql.contains(r#"AS "users""#), "{sql}");
}
