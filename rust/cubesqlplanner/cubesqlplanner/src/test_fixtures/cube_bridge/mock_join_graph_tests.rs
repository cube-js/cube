use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::test_fixtures::cube_bridge::{
    JoinEdge, MockCubeDefinition, MockCubeEvaluator, MockDimensionDefinition,
    MockJoinItemDefinition, MockJoinGraph, MockMeasureDefinition, MockSchemaBuilder,
};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Creates comprehensive test schema covering all join graph test scenarios
///
/// This schema includes multiple independent subgraphs designed to test different
/// join patterns and relationships:
///
/// 1. **Simple Join (orders -> users)**
///    - For: simple join tests, basic compilation
///    - Cubes: orders, users
///    - Join: orders many_to_one users
///
/// 2. **Chain (products -> categories -> departments)**
///    - For: chain tests, transitive multiplication
///    - Cubes: products, categories, departments
///    - Joins: products -> categories -> departments
///
/// 3. **Star Pattern (accounts -> [contacts, deals, tasks])**
///    - For: star pattern tests
///    - Cubes: accounts, contacts, deals, tasks
///    - Joins: accounts -> contacts, accounts -> deals, accounts -> tasks
///
/// 4. **Relationship Variations (companies <-> employees)**
///    - For: hasMany, belongsTo, bidirectional tests
///    - Cubes: companies, employees, projects
///    - Joins: companies hasMany employees, employees belongsTo companies, employees many_to_one projects
///
/// 5. **Cycle (regions -> countries -> cities -> regions)**
///    - For: cycle detection tests
///    - Cubes: regions, countries, cities
///    - Joins: regions -> countries -> cities -> regions (back to regions)
///
/// 6. **Disconnected (warehouses, suppliers - no joins between them)**
///    - For: disconnected component tests
///    - Cubes: warehouses, suppliers (isolated)
///
/// 7. **Validation Scenarios**
///    - orders_with_measures: has measures, has primary key
///    - orders_without_pk: has measures, NO primary key (for error tests)
///
/// # Returns
///
/// Tuple of (evaluator, cubes_map) where cubes_map is a HashMap
/// allowing easy access to cubes by name for test setup.
fn create_comprehensive_test_schema() -> (
    Rc<MockCubeEvaluator>,
    HashMap<String, Rc<MockCubeDefinition>>,
) {
    let schema = MockSchemaBuilder::new()
        // === 1. SIMPLE JOIN: orders -> users ===
        .add_cube("orders")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .build(),
        )
        .add_join(
            "users",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.user_id = {users.id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("users")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .finish_cube()
        // === 2. CHAIN: products -> categories -> departments ===
        .add_cube("products")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "categories",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.category_id = {categories.id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("categories")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "departments",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.department_id = {departments.id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("departments")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .finish_cube()
        // === 3. STAR: accounts -> [contacts, deals, tasks] ===
        .add_cube("accounts")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "contacts",
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{CUBE}.id = {contacts.account_id}".to_string())
                .build(),
        )
        .add_join(
            "deals",
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{CUBE}.id = {deals.account_id}".to_string())
                .build(),
        )
        .add_join(
            "tasks",
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{CUBE}.id = {tasks.account_id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("contacts")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("deals")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("tasks")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .finish_cube()
        // === 4. BIDIRECTIONAL: companies <-> employees -> projects ===
        .add_cube("companies")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "employees",
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{CUBE}.id = {employees.company_id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("employees")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "companies",
            MockJoinItemDefinition::builder()
                .relationship("belongsTo".to_string())
                .sql("{CUBE}.company_id = {companies.id}".to_string())
                .build(),
        )
        .add_join(
            "projects",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.project_id = {projects.id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("projects")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .finish_cube()
        // === 5. CYCLE: regions -> countries -> cities -> regions ===
        .add_cube("regions")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "countries",
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{CUBE}.id = {countries.region_id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("countries")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "cities",
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{CUBE}.id = {cities.country_id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("cities")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "regions",
            MockJoinItemDefinition::builder()
                .relationship("belongsTo".to_string())
                .sql("{CUBE}.region_id = {regions.id}".to_string())
                .build(),
        )
        .finish_cube()
        // === 6. DISCONNECTED: warehouses, suppliers (isolated) ===
        .add_cube("warehouses")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .finish_cube()
        .add_cube("suppliers")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .finish_cube()
        // === 7. VALIDATION: orders_with_measures (has PK), orders_without_pk (no PK) ===
        .add_cube("orders_with_measures")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("orders_without_pk")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                // NO primary_key set
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .add_join(
            "users",
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{orders_without_pk}.user_id = {users.id}".to_string())
                .build(),
        )
        .finish_cube()
        .build();

    let evaluator = schema.create_evaluator();

    // Build cubes map for easy access by name
    let cube_names = vec![
        "orders",
        "users",
        "products",
        "categories",
        "departments",
        "accounts",
        "contacts",
        "deals",
        "tasks",
        "companies",
        "employees",
        "projects",
        "regions",
        "countries",
        "cities",
        "warehouses",
        "suppliers",
        "orders_with_measures",
        "orders_without_pk",
    ];

    let mut cubes_map = HashMap::new();
    for name in cube_names {
        let cube = evaluator
            .cube_from_path(name.to_string())
            .unwrap()
            .as_any()
            .downcast_ref::<MockCubeDefinition>()
            .unwrap()
            .clone();
        cubes_map.insert(name.to_string(), Rc::new(cube));
    }

    (evaluator, cubes_map)
}

/// Extracts a subset of cubes from the cubes map by name
///
/// # Arguments
/// * `cubes_map` - HashMap of all available cubes
/// * `cube_names` - Names of cubes to extract
///
/// # Returns
/// Vec of cubes in the order specified by cube_names
fn get_cubes_vec(
    cubes_map: &HashMap<String, Rc<MockCubeDefinition>>,
    cube_names: &[&str],
) -> Vec<Rc<MockCubeDefinition>> {
    cube_names
        .iter()
        .map(|name| cubes_map.get(*name).unwrap().clone())
        .collect()
}

/// Creates and compiles a join graph from specified cubes
///
/// # Arguments
/// * `cubes_map` - HashMap of all available cubes
/// * `cube_names` - Names of cubes to include in graph
/// * `evaluator` - Cube evaluator
///
/// # Returns
/// Compiled MockJoinGraph
fn compile_test_graph(
    cubes_map: &HashMap<String, Rc<MockCubeDefinition>>,
    cube_names: &[&str],
    evaluator: &Rc<MockCubeEvaluator>,
) -> Result<MockJoinGraph, CubeError> {
    let cubes = get_cubes_vec(cubes_map, cube_names);
    let mut graph = MockJoinGraph::new();
    graph.compile(&cubes, evaluator)?;
    Ok(graph)
}

#[test]
fn test_mock_join_graph_new() {
    let graph = MockJoinGraph::new();

    // Verify all fields are empty
    assert!(graph.nodes.is_empty());
    assert!(graph.undirected_nodes.is_empty());
    assert!(graph.edges.is_empty());
    assert!(graph.built_joins.borrow().is_empty());
    assert!(graph.cached_connected_components.is_none());
}

#[test]
fn test_edge_key_format() {
    let key = MockJoinGraph::edge_key("orders", "users");
    assert_eq!(key, "orders-users");

    let key2 = MockJoinGraph::edge_key("users", "countries");
    assert_eq!(key2, "users-countries");

    // Verify different order creates different key
    let key3 = MockJoinGraph::edge_key("users", "orders");
    assert_ne!(key, key3);
}

#[test]
fn test_join_edge_creation() {
    let join_def = Rc::new(
        MockJoinItemDefinition::builder()
            .relationship("many_to_one".to_string())
            .sql("{orders.user_id} = {users.id}".to_string())
            .build(),
    );

    let edge = JoinEdge {
        join: join_def.clone(),
        from: "orders".to_string(),
        to: "users".to_string(),
        original_from: "Orders".to_string(),
        original_to: "Users".to_string(),
    };

    assert_eq!(edge.from, "orders");
    assert_eq!(edge.to, "users");
    assert_eq!(edge.original_from, "Orders");
    assert_eq!(edge.original_to, "Users");
    assert_eq!(edge.join.static_data().relationship, "many_to_one");
}

#[test]
fn test_default_trait() {
    let graph = MockJoinGraph::default();
    assert!(graph.nodes.is_empty());
    assert!(graph.undirected_nodes.is_empty());
}

#[test]
fn test_clone_trait() {
    let graph = MockJoinGraph::new();
    let cloned = graph.clone();

    assert!(cloned.nodes.is_empty());
    assert!(cloned.undirected_nodes.is_empty());
}

#[test]
fn test_compile_simple_graph() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    let graph = compile_test_graph(&cubes_map, &["orders", "users"], &evaluator).unwrap();

    // Verify edges contains "orders-users"
    assert!(graph.edges.contains_key("orders-users"));
    assert_eq!(graph.edges.len(), 1);

    // Verify nodes: both cubes present, "orders" has edge to "users"
    assert_eq!(graph.nodes.len(), 2);
    assert!(graph.nodes.contains_key("orders"));
    assert!(graph.nodes.contains_key("users"));
    let orders_destinations = graph.nodes.get("orders").unwrap();
    assert_eq!(orders_destinations.get("users"), Some(&1));

    // Verify undirected_nodes: {"users": {"orders": 1}}
    assert_eq!(graph.undirected_nodes.len(), 1);
    assert!(graph.undirected_nodes.contains_key("users"));
    let users_connections = graph.undirected_nodes.get("users").unwrap();
    assert_eq!(users_connections.get("orders"), Some(&1));
}

#[test]
fn test_compile_multiple_joins() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    // Use accounts star pattern: accounts -> contacts, accounts -> deals, accounts -> tasks
    let graph = compile_test_graph(
        &cubes_map,
        &["accounts", "contacts", "deals", "tasks"],
        &evaluator,
    )
    .unwrap();

    // Verify all edges present
    assert_eq!(graph.edges.len(), 3);
    assert!(graph.edges.contains_key("accounts-contacts"));
    assert!(graph.edges.contains_key("accounts-deals"));
    assert!(graph.edges.contains_key("accounts-tasks"));

    // Verify nodes correctly structured - all 4 cubes should be present
    assert_eq!(graph.nodes.len(), 4);
    assert!(graph.nodes.contains_key("accounts"));
    assert!(graph.nodes.contains_key("contacts"));
    assert!(graph.nodes.contains_key("deals"));
    assert!(graph.nodes.contains_key("tasks"));

    let accounts_dests = graph.nodes.get("accounts").unwrap();
    assert_eq!(accounts_dests.len(), 3);
    assert_eq!(accounts_dests.get("contacts"), Some(&1));
    assert_eq!(accounts_dests.get("deals"), Some(&1));
    assert_eq!(accounts_dests.get("tasks"), Some(&1));
}

#[test]
fn test_compile_bidirectional() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    // Use companies <-> employees bidirectional relationship
    // Note: employees also joins to projects, so include it in compilation
    let graph = compile_test_graph(
        &cubes_map,
        &["companies", "employees", "projects"],
        &evaluator,
    )
    .unwrap();

    // Verify both bidirectional edges exist
    assert!(graph.edges.contains_key("companies-employees"));
    assert!(graph.edges.contains_key("employees-companies"));

    // Also verify the employees -> projects edge
    assert!(graph.edges.contains_key("employees-projects"));
    assert_eq!(graph.edges.len(), 3);

    // Verify undirected_nodes includes all three cubes
    assert_eq!(graph.undirected_nodes.len(), 3);
    assert!(graph.undirected_nodes.contains_key("companies"));
    assert!(graph.undirected_nodes.contains_key("employees"));
    assert!(graph.undirected_nodes.contains_key("projects"));
}

#[test]
fn test_compile_nonexistent_cube() {
    // Create cube A with join to nonexistent B
    let schema = MockSchemaBuilder::new()
        .add_cube("A")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .build(),
        )
        .add_join(
            "B",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.b_id = {B.id}".to_string())
                .build(),
        )
        .finish_cube()
        .build();

    let evaluator = schema.create_evaluator();
    let cubes: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![Rc::new(
        evaluator
            .cube_from_path("A".to_string())
            .unwrap()
            .as_any()
            .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
            .unwrap()
            .clone(),
    )];

    let mut graph = MockJoinGraph::new();
    let result = graph.compile(&cubes, &evaluator);

    // Compile should return error
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.message.contains("Cube B doesn't exist"));
}

#[test]
fn test_compile_missing_primary_key() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    // orders_without_pk has measures and hasMany join but no primary key
    let cubes = get_cubes_vec(&cubes_map, &["orders_without_pk", "users"]);

    let mut graph = MockJoinGraph::new();
    let result = graph.compile(&cubes, &evaluator);

    // Compile should return error about missing primary key
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err
        .message
        .contains("primary key for 'orders_without_pk' is required"));
}

#[test]
fn test_compile_with_primary_key() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    // orders_with_measures has measure with primary key - should compile successfully
    let graph = compile_test_graph(&cubes_map, &["orders_with_measures"], &evaluator).unwrap();

    // Compile should succeed with cube that has measures and primary key
    // Single cube with no joins means no edges
    assert!(graph.edges.is_empty());
}

#[test]
fn test_recompile_clears_state() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    // First compile with orders -> users
    let cubes1 = get_cubes_vec(&cubes_map, &["orders", "users"]);
    let mut graph = MockJoinGraph::new();
    graph.compile(&cubes1, &evaluator).unwrap();
    assert_eq!(graph.edges.len(), 1);
    assert!(graph.edges.contains_key("orders-users"));

    // Recompile with products -> categories -> departments
    let cubes2 = get_cubes_vec(&cubes_map, &["products", "categories", "departments"]);
    graph.compile(&cubes2, &evaluator).unwrap();

    // Verify old edges gone
    assert!(!graph.edges.contains_key("orders-users"));

    // Verify only new edges present
    assert_eq!(graph.edges.len(), 2);
    assert!(graph.edges.contains_key("products-categories"));
    assert!(graph.edges.contains_key("categories-departments"));
}

#[test]
fn test_compile_empty() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    let graph = compile_test_graph(&cubes_map, &[], &evaluator).unwrap();

    // Verify all HashMaps empty
    assert!(graph.edges.is_empty());
    assert!(graph.nodes.is_empty());
    assert!(graph.undirected_nodes.is_empty());
    assert!(graph.built_joins.borrow().is_empty());
}

// Tests for build_join functionality

#[test]
fn test_build_join_simple() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    let graph = compile_test_graph(&cubes_map, &["orders", "users"], &evaluator).unwrap();

    // Build join: orders -> users
    let cubes_to_join = vec![
        JoinHintItem::Single("orders".to_string()),
        JoinHintItem::Single("users".to_string()),
    ];
    let result = graph.build_join(cubes_to_join).unwrap();

    // Expected: root=orders, joins=[orders->users], no multiplication
    assert_eq!(result.static_data().root, "orders");
    let joins = result.joins().unwrap();
    assert_eq!(joins.len(), 1);

    let join_static = joins[0].static_data();
    assert_eq!(join_static.from, "orders");
    assert_eq!(join_static.to, "users");

    // Check multiplication factors
    let mult_factors = result.static_data().multiplication_factor.clone();
    assert_eq!(mult_factors.get("orders"), Some(&false));
    assert_eq!(mult_factors.get("users"), Some(&false));
}

#[test]
fn test_build_join_chain() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();

    let graph = compile_test_graph(
        &cubes_map,
        &["products", "categories", "departments"],
        &evaluator,
    )
    .unwrap();

    // Build join: products -> categories -> departments
    let cubes_to_join = vec![
        JoinHintItem::Single("products".to_string()),
        JoinHintItem::Single("categories".to_string()),
        JoinHintItem::Single("departments".to_string()),
    ];
    let result = graph.build_join(cubes_to_join).unwrap();

    // Expected: root=products, joins=[products->categories, categories->departments]
    assert_eq!(result.static_data().root, "products");
    let joins = result.joins().unwrap();
    assert_eq!(joins.len(), 2);

    assert_eq!(joins[0].static_data().from, "products");
    assert_eq!(joins[0].static_data().to, "categories");
    assert_eq!(joins[1].static_data().from, "categories");
    assert_eq!(joins[1].static_data().to, "departments");
}

#[test]
fn test_build_join_shortest_path() {
    // Schema: A -> B -> C (2 hops)
    //         A -> C (1 hop - shortest)
    let schema = MockSchemaBuilder::new()
        .add_cube("A")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "B",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.b_id = {B.id}".to_string())
                .build(),
        )
        .add_join(
            "C",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.c_id = {C.id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("B")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "C",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.c_id = {C.id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("C")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .finish_cube()
        .build();

    let evaluator = schema.create_evaluator();
    let cubes: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![
        Rc::new(
            evaluator
                .cube_from_path("A".to_string())
                .unwrap()
                .as_any()
                .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                .unwrap()
                .clone(),
        ),
        Rc::new(
            evaluator
                .cube_from_path("B".to_string())
                .unwrap()
                .as_any()
                .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                .unwrap()
                .clone(),
        ),
        Rc::new(
            evaluator
                .cube_from_path("C".to_string())
                .unwrap()
                .as_any()
                .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                .unwrap()
                .clone(),
        ),
    ];

    let mut graph = MockJoinGraph::new();
    graph.compile(&cubes, &evaluator).unwrap();

    // Build join: A, C
    let cubes_to_join = vec![
        JoinHintItem::Single("A".to_string()),
        JoinHintItem::Single("C".to_string()),
    ];
    let result = graph.build_join(cubes_to_join).unwrap();

    // Expected: use direct path A->C (not A->B->C)
    assert_eq!(result.static_data().root, "A");
    let joins = result.joins().unwrap();
    assert_eq!(joins.len(), 1);

    assert_eq!(joins[0].static_data().from, "A");
    assert_eq!(joins[0].static_data().to, "C");
}

#[test]
fn test_build_join_star_pattern() {
    // Schema: accounts -> contacts, accounts -> deals, accounts -> tasks
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let graph = compile_test_graph(
        &cubes_map,
        &["accounts", "contacts", "deals", "tasks"],
        &evaluator,
    )
    .unwrap();

    // Build join: accounts, contacts, deals, tasks
    let cubes_to_join = vec![
        JoinHintItem::Single("accounts".to_string()),
        JoinHintItem::Single("contacts".to_string()),
        JoinHintItem::Single("deals".to_string()),
        JoinHintItem::Single("tasks".to_string()),
    ];
    let result = graph.build_join(cubes_to_join).unwrap();

    // Expected: root=accounts, joins to all others
    assert_eq!(result.static_data().root, "accounts");
    let joins = result.joins().unwrap();
    assert_eq!(joins.len(), 3);

    // All joins should be from accounts
    assert_eq!(joins[0].static_data().from, "accounts");
    assert_eq!(joins[1].static_data().from, "accounts");
    assert_eq!(joins[2].static_data().from, "accounts");
}

#[test]
fn test_build_join_disconnected() {
    // Schema: warehouses and suppliers are disconnected (no join)
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let graph =
        compile_test_graph(&cubes_map, &["warehouses", "suppliers"], &evaluator).unwrap();

    // Build join: warehouses, suppliers (disconnected)
    let cubes_to_join = vec![
        JoinHintItem::Single("warehouses".to_string()),
        JoinHintItem::Single("suppliers".to_string()),
    ];
    let result = graph.build_join(cubes_to_join);

    // Expected: error "Can't find join path"
    assert!(result.is_err());
    let err_msg = result.unwrap_err().message;
    assert!(err_msg.contains("Can't find join path"));
    assert!(err_msg.contains("'warehouses'"));
    assert!(err_msg.contains("'suppliers'"));
}

#[test]
fn test_build_join_empty() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let graph = compile_test_graph(&cubes_map, &[], &evaluator).unwrap();

    // Build join with empty list
    let cubes_to_join = vec![];
    let result = graph.build_join(cubes_to_join);

    // Expected: error
    assert!(result.is_err());
    let err_msg = result.unwrap_err().message;
    assert!(err_msg.contains("empty"));
}

#[test]
fn test_build_join_single_cube() {
    // Schema: orders (single cube)
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let graph = compile_test_graph(&cubes_map, &["orders"], &evaluator).unwrap();

    // Build join with single cube
    let cubes_to_join = vec![JoinHintItem::Single("orders".to_string())];
    let result = graph.build_join(cubes_to_join).unwrap();

    // Expected: root=orders, no joins
    assert_eq!(result.static_data().root, "orders");
    let joins = result.joins().unwrap();
    assert_eq!(joins.len(), 0);
}

#[test]
fn test_build_join_caching() {
    // Schema: orders -> users
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let graph = compile_test_graph(&cubes_map, &["orders", "users"], &evaluator).unwrap();

    // Build join twice
    let cubes_to_join = vec![
        JoinHintItem::Single("orders".to_string()),
        JoinHintItem::Single("users".to_string()),
    ];
    let result1 = graph.build_join(cubes_to_join.clone()).unwrap();
    let result2 = graph.build_join(cubes_to_join).unwrap();

    // Verify same Rc returned (pointer equality)
    assert!(Rc::ptr_eq(&result1, &result2));
}

#[test]
fn test_multiplication_factor_has_many() {
    // users hasMany orders
    // users should multiply, orders should not
    let graph = MockJoinGraph::new();

    let joins = vec![JoinEdge {
        join: Rc::new(
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{CUBE}.id = {orders.user_id}".to_string())
                .build(),
        ),
        from: "users".to_string(),
        to: "orders".to_string(),
        original_from: "users".to_string(),
        original_to: "orders".to_string(),
    }];

    assert!(graph.find_multiplication_factor_for("users", &joins));
    assert!(!graph.find_multiplication_factor_for("orders", &joins));
}

#[test]
fn test_multiplication_factor_belongs_to() {
    // orders belongsTo users
    // users should multiply, orders should not
    let graph = MockJoinGraph::new();

    let joins = vec![JoinEdge {
        join: Rc::new(
            MockJoinItemDefinition::builder()
                .relationship("belongsTo".to_string())
                .sql("{CUBE}.user_id = {users.id}".to_string())
                .build(),
        ),
        from: "orders".to_string(),
        to: "users".to_string(),
        original_from: "orders".to_string(),
        original_to: "users".to_string(),
    }];

    assert!(graph.find_multiplication_factor_for("users", &joins));
    assert!(!graph.find_multiplication_factor_for("orders", &joins));
}

#[test]
fn test_multiplication_factor_transitive() {
    // users hasMany orders, orders hasMany items
    // users multiplies (direct hasMany)
    // orders multiplies (has hasMany to items)
    // items does not multiply
    let graph = MockJoinGraph::new();

    let joins = vec![
        JoinEdge {
            join: Rc::new(
                MockJoinItemDefinition::builder()
                    .relationship("hasMany".to_string())
                    .sql("{CUBE}.id = {orders.user_id}".to_string())
                    .build(),
            ),
            from: "users".to_string(),
            to: "orders".to_string(),
            original_from: "users".to_string(),
            original_to: "orders".to_string(),
        },
        JoinEdge {
            join: Rc::new(
                MockJoinItemDefinition::builder()
                    .relationship("hasMany".to_string())
                    .sql("{CUBE}.id = {items.order_id}".to_string())
                    .build(),
            ),
            from: "orders".to_string(),
            to: "items".to_string(),
            original_from: "orders".to_string(),
            original_to: "items".to_string(),
        },
    ];

    assert!(graph.find_multiplication_factor_for("users", &joins));
    assert!(graph.find_multiplication_factor_for("orders", &joins));
    assert!(!graph.find_multiplication_factor_for("items", &joins));
}

#[test]
fn test_multiplication_factor_many_to_one() {
    // orders many_to_one users (neither multiplies)
    let graph = MockJoinGraph::new();

    let joins = vec![JoinEdge {
        join: Rc::new(
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.user_id = {users.id}".to_string())
                .build(),
        ),
        from: "orders".to_string(),
        to: "users".to_string(),
        original_from: "orders".to_string(),
        original_to: "users".to_string(),
    }];

    assert!(!graph.find_multiplication_factor_for("users", &joins));
    assert!(!graph.find_multiplication_factor_for("orders", &joins));
}

#[test]
fn test_multiplication_factor_star_pattern() {
    // users hasMany orders, users hasMany sessions
    // In this graph topology:
    // - users multiplies (has hasMany to unvisited nodes)
    // - orders multiplies (connected to users which has hasMany to sessions)
    // - sessions multiplies (connected to users which has hasMany to orders)
    // This is because the algorithm checks for multiplication in the connected component
    let graph = MockJoinGraph::new();

    let joins = vec![
        JoinEdge {
            join: Rc::new(
                MockJoinItemDefinition::builder()
                    .relationship("hasMany".to_string())
                    .sql("{CUBE}.id = {orders.user_id}".to_string())
                    .build(),
            ),
            from: "users".to_string(),
            to: "orders".to_string(),
            original_from: "users".to_string(),
            original_to: "orders".to_string(),
        },
        JoinEdge {
            join: Rc::new(
                MockJoinItemDefinition::builder()
                    .relationship("hasMany".to_string())
                    .sql("{CUBE}.id = {sessions.user_id}".to_string())
                    .build(),
            ),
            from: "users".to_string(),
            to: "sessions".to_string(),
            original_from: "users".to_string(),
            original_to: "sessions".to_string(),
        },
    ];

    assert!(graph.find_multiplication_factor_for("users", &joins));
    // orders and sessions both return true because users (connected node) has hasMany
    assert!(graph.find_multiplication_factor_for("orders", &joins));
    assert!(graph.find_multiplication_factor_for("sessions", &joins));
}

#[test]
fn test_multiplication_factor_cycle() {
    // A hasMany B, B hasMany A (cycle)
    // Both should multiply
    let graph = MockJoinGraph::new();

    let joins = vec![
        JoinEdge {
            join: Rc::new(
                MockJoinItemDefinition::builder()
                    .relationship("hasMany".to_string())
                    .sql("{CUBE}.id = {B.a_id}".to_string())
                    .build(),
            ),
            from: "A".to_string(),
            to: "B".to_string(),
            original_from: "A".to_string(),
            original_to: "B".to_string(),
        },
        JoinEdge {
            join: Rc::new(
                MockJoinItemDefinition::builder()
                    .relationship("hasMany".to_string())
                    .sql("{CUBE}.id = {A.b_id}".to_string())
                    .build(),
            ),
            from: "B".to_string(),
            to: "A".to_string(),
            original_from: "B".to_string(),
            original_to: "A".to_string(),
        },
    ];

    assert!(graph.find_multiplication_factor_for("A", &joins));
    assert!(graph.find_multiplication_factor_for("B", &joins));
}

#[test]
fn test_build_join_with_multiplication_factors() {
    // Schema: users hasMany orders, orders many_to_one products
    let schema = MockSchemaBuilder::new()
        .add_cube("users")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "orders",
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{CUBE}.id = {orders.user_id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("orders")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_join(
            "products",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{CUBE}.product_id = {products.id}".to_string())
                .build(),
        )
        .finish_cube()
        .add_cube("products")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .finish_cube()
        .build();

    let evaluator = schema.create_evaluator();
    let cubes: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![
        Rc::new(
            evaluator
                .cube_from_path("users".to_string())
                .unwrap()
                .as_any()
                .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                .unwrap()
                .clone(),
        ),
        Rc::new(
            evaluator
                .cube_from_path("orders".to_string())
                .unwrap()
                .as_any()
                .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                .unwrap()
                .clone(),
        ),
        Rc::new(
            evaluator
                .cube_from_path("products".to_string())
                .unwrap()
                .as_any()
                .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                .unwrap()
                .clone(),
        ),
    ];

    let mut graph = MockJoinGraph::new();
    graph.compile(&cubes, &evaluator).unwrap();

    // Build join: users -> orders -> products
    let cubes_to_join = vec![
        JoinHintItem::Single("users".to_string()),
        JoinHintItem::Single("orders".to_string()),
        JoinHintItem::Single("products".to_string()),
    ];
    let result = graph.build_join(cubes_to_join).unwrap();

    // Check multiplication factors
    let mult_factors = result.static_data().multiplication_factor.clone();

    // users hasMany orders -> users multiplies
    assert_eq!(mult_factors.get("users"), Some(&true));

    // orders is in the middle, does not have its own hasMany, does not multiply
    assert_eq!(mult_factors.get("orders"), Some(&false));

    // products is leaf with many_to_one, does not multiply
    assert_eq!(mult_factors.get("products"), Some(&false));
}

#[test]
fn test_check_if_cube_multiplied() {
    let graph = MockJoinGraph::new();

    // hasMany: from side multiplies
    let join_has_many = JoinEdge {
        join: Rc::new(
            MockJoinItemDefinition::builder()
                .relationship("hasMany".to_string())
                .sql("{orders.user_id} = {users.id}".to_string())
                .build(),
        ),
        from: "users".to_string(),
        to: "orders".to_string(),
        original_from: "users".to_string(),
        original_to: "orders".to_string(),
    };

    assert!(graph.check_if_cube_multiplied("users", &join_has_many));
    assert!(!graph.check_if_cube_multiplied("orders", &join_has_many));

    // belongsTo: to side multiplies
    let join_belongs_to = JoinEdge {
        join: Rc::new(
            MockJoinItemDefinition::builder()
                .relationship("belongsTo".to_string())
                .sql("{orders.user_id} = {users.id}".to_string())
                .build(),
        ),
        from: "orders".to_string(),
        to: "users".to_string(),
        original_from: "orders".to_string(),
        original_to: "users".to_string(),
    };

    assert!(graph.check_if_cube_multiplied("users", &join_belongs_to));
    assert!(!graph.check_if_cube_multiplied("orders", &join_belongs_to));

    // many_to_one: no multiplication
    let join_many_to_one = JoinEdge {
        join: Rc::new(
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{orders.user_id} = {users.id}".to_string())
                .build(),
        ),
        from: "orders".to_string(),
        to: "users".to_string(),
        original_from: "orders".to_string(),
        original_to: "users".to_string(),
    };

    assert!(!graph.check_if_cube_multiplied("users", &join_many_to_one));
    assert!(!graph.check_if_cube_multiplied("orders", &join_many_to_one));
}

#[test]
fn test_build_join_with_vector_hint() {
    // Schema: products -> categories -> departments
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let graph = compile_test_graph(
        &cubes_map,
        &["products", "categories", "departments"],
        &evaluator,
    )
    .unwrap();

    // Build join with Vector hint: [products, categories] becomes root=products, join to categories, then join to departments
    let cubes_to_join = vec![
        JoinHintItem::Vector(vec!["products".to_string(), "categories".to_string()]),
        JoinHintItem::Single("departments".to_string()),
    ];
    let result = graph.build_join(cubes_to_join).unwrap();

    // Expected: root=products, joins=[products->categories, categories->departments]
    assert_eq!(result.static_data().root, "products");
    let joins = result.joins().unwrap();
    assert_eq!(joins.len(), 2);

    assert_eq!(joins[0].static_data().from, "products");
    assert_eq!(joins[0].static_data().to, "categories");
    assert_eq!(joins[1].static_data().from, "categories");
    assert_eq!(joins[1].static_data().to, "departments");
}

#[test]
fn test_connected_components_simple() {
    // Graph: orders -> users (both in same component)
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let mut graph = compile_test_graph(&cubes_map, &["orders", "users"], &evaluator).unwrap();

    let components = graph.connected_components();

    // Both cubes should be in same component
    assert_eq!(components.len(), 2);
    let orders_comp = components.get("orders").unwrap();
    let users_comp = components.get("users").unwrap();
    assert_eq!(orders_comp, users_comp);
}

#[test]
fn test_connected_components_disconnected() {
    // Graph: orders -> users (connected), warehouses, suppliers (both isolated)
    // Three components: {orders, users}, {warehouses}, {suppliers}
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let mut graph = compile_test_graph(
        &cubes_map,
        &["orders", "users", "warehouses", "suppliers"],
        &evaluator,
    )
    .unwrap();

    let components = graph.connected_components();

    // All four cubes should have component IDs
    assert_eq!(components.len(), 4);

    // orders and users in same component
    let orders_comp = components.get("orders").unwrap();
    let users_comp = components.get("users").unwrap();
    assert_eq!(orders_comp, users_comp);

    // warehouses and suppliers in different components
    let warehouses_comp = components.get("warehouses").unwrap();
    let suppliers_comp = components.get("suppliers").unwrap();
    assert_ne!(orders_comp, warehouses_comp);
    assert_ne!(orders_comp, suppliers_comp);
    assert_ne!(warehouses_comp, suppliers_comp);
}

#[test]
fn test_connected_components_all_isolated() {
    // Graph: warehouses, suppliers, orders_with_measures (no joins)
    // Three components: {warehouses}, {suppliers}, {orders_with_measures}
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let mut graph = compile_test_graph(
        &cubes_map,
        &["warehouses", "suppliers", "orders_with_measures"],
        &evaluator,
    )
    .unwrap();

    let components = graph.connected_components();

    // All three cubes in different components
    assert_eq!(components.len(), 3);
    let warehouses_comp = components.get("warehouses").unwrap();
    let suppliers_comp = components.get("suppliers").unwrap();
    let orders_with_measures_comp = components.get("orders_with_measures").unwrap();
    assert_ne!(warehouses_comp, suppliers_comp);
    assert_ne!(suppliers_comp, orders_with_measures_comp);
    assert_ne!(warehouses_comp, orders_with_measures_comp);
}

#[test]
fn test_connected_components_large_connected() {
    // Chain: products -> categories -> departments (all in same component)
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let mut graph = compile_test_graph(
        &cubes_map,
        &["products", "categories", "departments"],
        &evaluator,
    )
    .unwrap();

    let components = graph.connected_components();

    // All cubes in same component
    assert_eq!(components.len(), 3);
    let products_comp = components.get("products").unwrap();
    let categories_comp = components.get("categories").unwrap();
    let departments_comp = components.get("departments").unwrap();

    assert_eq!(products_comp, categories_comp);
    assert_eq!(categories_comp, departments_comp);
}

#[test]
fn test_connected_components_cycle() {
    // Cycle: regions -> countries -> cities -> regions
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let mut graph =
        compile_test_graph(&cubes_map, &["regions", "countries", "cities"], &evaluator)
            .unwrap();

    let components = graph.connected_components();

    // All three in same component (cycle)
    assert_eq!(components.len(), 3);
    let regions_comp = components.get("regions").unwrap();
    let countries_comp = components.get("countries").unwrap();
    let cities_comp = components.get("cities").unwrap();

    assert_eq!(regions_comp, countries_comp);
    assert_eq!(countries_comp, cities_comp);
}

#[test]
fn test_connected_components_empty() {
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let mut graph = compile_test_graph(&cubes_map, &[], &evaluator).unwrap();

    let components = graph.connected_components();

    // Empty graph
    assert_eq!(components.len(), 0);
}

#[test]
fn test_connected_components_caching() {
    // Verify components are cached
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let mut graph = compile_test_graph(&cubes_map, &["orders", "users"], &evaluator).unwrap();

    // First call calculates
    let components1 = graph.connected_components();

    // Second call should return cached result
    let components2 = graph.connected_components();

    assert_eq!(components1, components2);
}

#[test]
fn test_connected_components_multiple_groups() {
    // Three disconnected groups:
    // - orders -> users
    // - warehouses
    // - suppliers
    let (evaluator, cubes_map) = create_comprehensive_test_schema();
    let mut graph = compile_test_graph(
        &cubes_map,
        &["orders", "users", "warehouses", "suppliers"],
        &evaluator,
    )
    .unwrap();

    let components = graph.connected_components();

    assert_eq!(components.len(), 4);

    let orders_comp = components.get("orders").unwrap();
    let users_comp = components.get("users").unwrap();
    let warehouses_comp = components.get("warehouses").unwrap();
    let suppliers_comp = components.get("suppliers").unwrap();

    // orders and users connected
    assert_eq!(orders_comp, users_comp);

    // Others disconnected
    assert_ne!(orders_comp, warehouses_comp);
    assert_ne!(orders_comp, suppliers_comp);
    assert_ne!(warehouses_comp, suppliers_comp);
}
