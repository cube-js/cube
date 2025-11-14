use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::test_fixtures::cube_bridge::{MockJoinDefinition, MockJoinItemDefinition};
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

/// Represents an edge in the join graph
///
/// Each edge represents a join relationship between two cubes, including both
/// the current routing (from/to) and the original cube names (original_from/original_to).
/// This distinction is important when dealing with cube aliases.
///
/// # Example
///
/// ```
/// use cubesqlplanner::test_fixtures::cube_bridge::{JoinEdge, MockJoinItemDefinition};
/// use std::rc::Rc;
///
/// let join_def = Rc::new(
///     MockJoinItemDefinition::builder()
///         .relationship("many_to_one".to_string())
///         .sql("{orders.user_id} = {users.id}".to_string())
///         .build()
/// );
///
/// let edge = JoinEdge {
///     join: join_def,
///     from: "orders".to_string(),
///     to: "users".to_string(),
///     original_from: "Orders".to_string(),
///     original_to: "Users".to_string(),
/// };
///
/// assert_eq!(edge.from, "orders");
/// assert_eq!(edge.original_from, "Orders");
/// ```
#[derive(Debug, Clone)]
pub struct JoinEdge {
    /// The join definition containing the relationship and SQL
    pub join: Rc<MockJoinItemDefinition>,
    /// The current source cube name (may be an alias)
    pub from: String,
    /// The current destination cube name (may be an alias)
    pub to: String,
    /// The original source cube name (without aliases)
    pub original_from: String,
    /// The original destination cube name (without aliases)
    pub original_to: String,
}

/// Mock implementation of JoinGraph for testing
///
/// This implementation provides a graph-based representation of join relationships
/// between cubes, matching the TypeScript JoinGraph structure from
/// `/packages/cubejs-schema-compiler/src/compiler/JoinGraph.ts`.
///
/// The graph maintains both directed and undirected representations to support
/// pathfinding and connectivity queries. It also caches built join trees to avoid
/// redundant computation.
///
/// # Example
///
/// ```
/// use cubesqlplanner::test_fixtures::cube_bridge::MockJoinGraph;
///
/// let graph = MockJoinGraph::new();
/// // Add edges and build joins...
/// ```
#[derive(Clone)]
pub struct MockJoinGraph {
    /// Directed graph: source -> destination -> weight
    /// Represents the directed join relationships between cubes
    nodes: HashMap<String, HashMap<String, u32>>,

    /// Undirected graph: destination -> source -> weight
    /// Used for connectivity checks and pathfinding
    undirected_nodes: HashMap<String, HashMap<String, u32>>,

    /// Edge lookup: "from-to" -> JoinEdge
    /// Maps edge keys to their corresponding join definitions
    edges: HashMap<String, JoinEdge>,

    /// Cache of built join trees: serialized cubes -> JoinDefinition
    /// Stores previously computed join paths for reuse
    built_joins: HashMap<String, Rc<MockJoinDefinition>>,

    /// Cache for connected components
    /// Stores the connected component ID for each cube
    /// None until first calculation
    cached_connected_components: Option<HashMap<String, u32>>,
}

impl MockJoinGraph {
    /// Creates a new empty join graph
    ///
    /// # Example
    ///
    /// ```
    /// use cubesqlplanner::test_fixtures::cube_bridge::MockJoinGraph;
    ///
    /// let graph = MockJoinGraph::new();
    /// ```
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            undirected_nodes: HashMap::new(),
            edges: HashMap::new(),
            built_joins: HashMap::new(),
            cached_connected_components: None,
        }
    }

    /// Creates an edge key from source and destination cube names
    ///
    /// The key format is "from-to", matching the TypeScript implementation.
    ///
    /// # Arguments
    ///
    /// * `from` - Source cube name
    /// * `to` - Destination cube name
    ///
    /// # Example
    ///
    /// ```
    /// # use cubesqlplanner::test_fixtures::cube_bridge::MockJoinGraph;
    /// let key = MockJoinGraph::edge_key("orders", "users");
    /// assert_eq!(key, "orders-users");
    /// ```
    fn edge_key(from: &str, to: &str) -> String {
        format!("{}-{}", from, to)
    }

    /// Builds join edges for a single cube
    ///
    /// This method extracts all joins from the cube, validates them, and creates JoinEdge instances.
    ///
    /// # Validation
    /// - Target cube must exist
    /// - Source and target cubes with multiplied measures must have primary keys
    ///
    /// # Returns
    /// Vector of (edge_key, JoinEdge) tuples
    fn build_join_edges(
        &self,
        cube: &crate::test_fixtures::cube_bridge::MockCubeDefinition,
        evaluator: &crate::test_fixtures::cube_bridge::MockCubeEvaluator,
    ) -> Result<Vec<(String, JoinEdge)>, CubeError> {
        let joins = cube.joins();
        if joins.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        let cube_name = &cube.static_data().name;

        for (join_name, join_def) in joins {
            // Validate target cube exists
            if !evaluator.cube_exists(join_name.clone())? {
                return Err(CubeError::user(format!("Cube {} doesn't exist", join_name)));
            }

            // Check multiplied measures for source cube
            let from_multiplied = self.get_multiplied_measures(cube_name, evaluator)?;
            if !from_multiplied.is_empty() {
                let static_data = evaluator.static_data();
                let primary_keys = static_data.primary_keys.get(cube_name);
                if primary_keys.map_or(true, |pk| pk.is_empty()) {
                    return Err(CubeError::user(format!(
                        "primary key for '{}' is required when join is defined in order to make aggregates work properly",
                        cube_name
                    )));
                }
            }

            // Check multiplied measures for target cube
            let to_multiplied = self.get_multiplied_measures(join_name, evaluator)?;
            if !to_multiplied.is_empty() {
                let static_data = evaluator.static_data();
                let primary_keys = static_data.primary_keys.get(join_name);
                if primary_keys.map_or(true, |pk| pk.is_empty()) {
                    return Err(CubeError::user(format!(
                        "primary key for '{}' is required when join is defined in order to make aggregates work properly",
                        join_name
                    )));
                }
            }

            // Create JoinEdge
            let edge = JoinEdge {
                join: Rc::new(join_def.clone()),
                from: cube_name.clone(),
                to: join_name.clone(),
                original_from: cube_name.clone(),
                original_to: join_name.clone(),
            };

            let edge_key = Self::edge_key(cube_name, join_name);
            result.push((edge_key, edge));
        }

        Ok(result)
    }

    /// Gets measures that are "multiplied" by joins (require primary keys)
    ///
    /// Multiplied measure types: sum, avg, count, number
    fn get_multiplied_measures(
        &self,
        cube_name: &str,
        evaluator: &crate::test_fixtures::cube_bridge::MockCubeEvaluator,
    ) -> Result<Vec<String>, CubeError> {
        let measures = evaluator.measures_for_cube(cube_name);
        let multiplied_types = ["sum", "avg", "count", "number"];

        let mut result = Vec::new();
        for (measure_name, measure) in measures {
            let measure_type = &measure.static_data().measure_type;
            if multiplied_types.contains(&measure_type.as_str()) {
                result.push(measure_name);
            }
        }

        Ok(result)
    }

    /// Compiles the join graph from cube definitions
    ///
    /// This method processes all cubes and their join definitions to build the internal
    /// graph structure needed for join path finding. It validates that:
    /// - All referenced cubes exist
    /// - Cubes with multiplied measures have primary keys defined
    ///
    /// # Arguments
    /// * `cubes` - Slice of cube definitions to compile
    /// * `evaluator` - Evaluator for validation and lookups
    ///
    /// # Returns
    /// * `Ok(())` if compilation succeeds
    /// * `Err(CubeError)` if validation fails
    pub fn compile(
        &mut self,
        cubes: &[Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>],
        evaluator: &crate::test_fixtures::cube_bridge::MockCubeEvaluator,
    ) -> Result<(), CubeError> {
        // Clear existing state
        self.edges.clear();
        self.nodes.clear();
        self.undirected_nodes.clear();
        self.cached_connected_components = None;

        // Build edges from all cubes
        for cube in cubes {
            let cube_edges = self.build_join_edges(cube, evaluator)?;
            for (key, edge) in cube_edges {
                self.edges.insert(key, edge);
            }
        }

        // Build nodes HashMap (directed graph)
        // Group edges by 'from' field and create HashMap of destinations
        for (_, edge) in &self.edges {
            self.nodes
                .entry(edge.from.clone())
                .or_insert_with(HashMap::new)
                .insert(edge.to.clone(), 1);
        }

        // Build undirected_nodes HashMap
        // For each edge (from -> to), also add (to -> from) for bidirectional connectivity
        for (_, edge) in &self.edges {
            self.undirected_nodes
                .entry(edge.to.clone())
                .or_insert_with(HashMap::new)
                .insert(edge.from.clone(), 1);
        }

        Ok(())
    }
}

impl Default for MockJoinGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinGraph for MockJoinGraph {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }

    fn build_join(
        &self,
        _cubes_to_join: Vec<JoinHintItem>,
    ) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        todo!("build_join not implemented in MockJoinGraph")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::evaluator::CubeEvaluator;
    use crate::test_fixtures::cube_bridge::{
        MockDimensionDefinition, MockMeasureDefinition, MockSchemaBuilder,
    };

    #[test]
    fn test_mock_join_graph_new() {
        let graph = MockJoinGraph::new();

        // Verify all fields are empty
        assert!(graph.nodes.is_empty());
        assert!(graph.undirected_nodes.is_empty());
        assert!(graph.edges.is_empty());
        assert!(graph.built_joins.is_empty());
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
        // Create schema: orders -> users
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
            .finish_cube()
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
        ];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

        // Verify edges contains "orders-users"
        assert!(graph.edges.contains_key("orders-users"));
        assert_eq!(graph.edges.len(), 1);

        // Verify nodes: {"orders": {"users": 1}}
        assert_eq!(graph.nodes.len(), 1);
        assert!(graph.nodes.contains_key("orders"));
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
        // Create schema: orders -> users, orders -> products, products -> categories
        let schema = MockSchemaBuilder::new()
            .add_cube("categories")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
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
            .add_join(
                "categories",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.category_id = {categories.id}".to_string())
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
            .add_join(
                "products",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.product_id = {products.id}".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        let evaluator = schema.create_evaluator();
        let cubes: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![
            Rc::new(
                evaluator
                    .cube_from_path("categories".to_string())
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
        ];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

        // Verify all edges present
        assert_eq!(graph.edges.len(), 3);
        assert!(graph.edges.contains_key("orders-users"));
        assert!(graph.edges.contains_key("orders-products"));
        assert!(graph.edges.contains_key("products-categories"));

        // Verify nodes correctly structured
        assert_eq!(graph.nodes.len(), 2);
        assert!(graph.nodes.contains_key("orders"));
        assert!(graph.nodes.contains_key("products"));

        let orders_dests = graph.nodes.get("orders").unwrap();
        assert_eq!(orders_dests.len(), 2);
        assert_eq!(orders_dests.get("users"), Some(&1));
        assert_eq!(orders_dests.get("products"), Some(&1));

        let products_dests = graph.nodes.get("products").unwrap();
        assert_eq!(products_dests.len(), 1);
        assert_eq!(products_dests.get("categories"), Some(&1));
    }

    #[test]
    fn test_compile_bidirectional() {
        // Create schema: A -> B, B -> A
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
                    .relationship("one_to_many".to_string())
                    .sql("{CUBE}.id = {B.a_id}".to_string())
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
                "A",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.a_id = {A.id}".to_string())
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
        ];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

        // Verify both directions in edges
        assert_eq!(graph.edges.len(), 2);
        assert!(graph.edges.contains_key("A-B"));
        assert!(graph.edges.contains_key("B-A"));

        // Verify undirected_nodes handles properly
        assert_eq!(graph.undirected_nodes.len(), 2);
        assert!(graph.undirected_nodes.contains_key("A"));
        assert!(graph.undirected_nodes.contains_key("B"));
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
        // Create cube A with multiplied measure (count) and no primary key
        let schema = MockSchemaBuilder::new()
            .add_cube("B")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
                    .build(),
            )
            .finish_cube()
            .add_cube("A")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
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
                "B",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.b_id = {B.id}".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        let evaluator = schema.create_evaluator();
        let cubes: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![
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
                    .cube_from_path("A".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
        ];

        let mut graph = MockJoinGraph::new();
        let result = graph.compile(&cubes, &evaluator);

        // Compile should return error
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("primary key for 'A' is required"));
    }

    #[test]
    fn test_compile_with_primary_key() {
        // Create cube A with multiplied measure and primary key
        let schema = MockSchemaBuilder::new()
            .add_cube("B")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
                    .build(),
            )
            .finish_cube()
            .add_cube("A")
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
        let cubes: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![
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
                    .cube_from_path("A".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
        ];

        let mut graph = MockJoinGraph::new();
        let result = graph.compile(&cubes, &evaluator);

        // Compile should succeed
        assert!(result.is_ok());
        assert_eq!(graph.edges.len(), 1);
        assert!(graph.edges.contains_key("A-B"));
    }

    #[test]
    fn test_recompile_clears_state() {
        // Compile with schema A -> B
        let schema1 = MockSchemaBuilder::new()
            .add_cube("B")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
                    .build(),
            )
            .finish_cube()
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

        let evaluator1 = schema1.create_evaluator();
        let cubes1: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![
            Rc::new(
                evaluator1
                    .cube_from_path("B".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
            Rc::new(
                evaluator1
                    .cube_from_path("A".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
        ];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes1, &evaluator1).unwrap();
        assert_eq!(graph.edges.len(), 1);
        assert!(graph.edges.contains_key("A-B"));

        // Recompile with schema C -> D
        let schema2 = MockSchemaBuilder::new()
            .add_cube("D")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
                    .build(),
            )
            .finish_cube()
            .add_cube("C")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_join(
                "D",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.d_id = {D.id}".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        let evaluator2 = schema2.create_evaluator();
        let cubes2: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![
            Rc::new(
                evaluator2
                    .cube_from_path("D".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
            Rc::new(
                evaluator2
                    .cube_from_path("C".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
        ];

        graph.compile(&cubes2, &evaluator2).unwrap();

        // Verify old edges gone
        assert!(!graph.edges.contains_key("A-B"));

        // Verify only new edges present
        assert_eq!(graph.edges.len(), 1);
        assert!(graph.edges.contains_key("C-D"));
    }

    #[test]
    fn test_compile_empty() {
        // Compile with empty cube list
        let schema = MockSchemaBuilder::new().build();
        let evaluator = schema.create_evaluator();
        let cubes: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

        // Verify all HashMaps empty
        assert!(graph.edges.is_empty());
        assert!(graph.nodes.is_empty());
        assert!(graph.undirected_nodes.is_empty());
    }
}
