use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::test_fixtures::cube_bridge::{MockJoinDefinition, MockJoinItemDefinition};
use cubenativeutils::CubeError;
use std::any::Any;
use std::cell::RefCell;
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
    /// Uses RefCell for interior mutability (allows caching through &self)
    built_joins: RefCell<HashMap<String, Rc<MockJoinDefinition>>>,

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
            built_joins: RefCell::new(HashMap::new()),
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

    /// Extracts the cube name from a JoinHintItem
    ///
    /// For Single variants, returns the cube name directly.
    /// For Vector variants, returns the last element (the destination).
    ///
    /// # Arguments
    /// * `cube_path` - The JoinHintItem to extract from
    ///
    /// # Returns
    /// The cube name as a String
    ///
    /// # Example
    /// ```
    /// use cubesqlplanner::cube_bridge::join_hints::JoinHintItem;
    /// # use cubesqlplanner::test_fixtures::cube_bridge::MockJoinGraph;
    /// # let graph = MockJoinGraph::new();
    ///
    /// let single = JoinHintItem::Single("users".to_string());
    /// assert_eq!(graph.cube_from_path(&single), "users");
    ///
    /// let vector = JoinHintItem::Vector(vec!["orders".to_string(), "users".to_string()]);
    /// assert_eq!(graph.cube_from_path(&vector), "users");
    /// ```
    fn cube_from_path(&self, cube_path: &JoinHintItem) -> String {
        match cube_path {
            JoinHintItem::Single(name) => name.clone(),
            JoinHintItem::Vector(path) => path
                .last()
                .expect("Vector path should not be empty")
                .clone(),
        }
    }

    /// Converts a path of cube names to a list of JoinEdges
    ///
    /// For a path [A, B, C], this looks up edges "A-B" and "B-C" in the edges HashMap.
    ///
    /// # Arguments
    /// * `path` - Slice of cube names representing the path
    ///
    /// # Returns
    /// Vector of JoinEdge instances corresponding to consecutive pairs in the path
    ///
    /// # Example
    /// ```ignore
    /// // For path ["orders", "users", "countries"]
    /// // Returns edges for "orders-users" and "users-countries"
    /// let path = vec!["orders".to_string(), "users".to_string(), "countries".to_string()];
    /// let joins = graph.joins_by_path(&path);
    /// ```
    fn joins_by_path(&self, path: &[String]) -> Vec<JoinEdge> {
        let mut result = Vec::new();
        for i in 0..path.len().saturating_sub(1) {
            let key = Self::edge_key(&path[i], &path[i + 1]);
            if let Some(edge) = self.edges.get(&key) {
                result.push(edge.clone());
            }
        }
        result
    }

    /// Builds a join tree with a specific root cube
    ///
    /// This method tries to build a join tree starting from the specified root,
    /// connecting to all cubes in cubes_to_join. It uses Dijkstra's algorithm
    /// to find the shortest paths.
    ///
    /// # Arguments
    /// * `root` - The root cube (can be Single or Vector)
    /// * `cubes_to_join` - Other cubes to connect to the root
    ///
    /// # Returns
    /// * `Some((root_name, joins))` - If a valid join tree can be built
    /// * `None` - If no path exists to connect all cubes
    ///
    /// # Algorithm
    /// 1. Extract root name (if Vector, first element becomes root, rest go to cubes_to_join)
    /// 2. Track joined nodes to avoid duplicates
    /// 3. For each cube to join:
    ///    - Find shortest path from previous node
    ///    - Convert path to JoinEdge list
    ///    - Mark nodes as joined
    /// 4. Collect and deduplicate all joins
    fn build_join_tree_for_root(
        &self,
        root: &JoinHintItem,
        cubes_to_join: &[JoinHintItem],
    ) -> Option<(String, Vec<JoinEdge>)> {
        use crate::test_fixtures::graph_utils::find_shortest_path;
        use std::collections::HashSet;

        // Extract root and additional cubes to join
        let (root_name, additional_cubes) = match root {
            JoinHintItem::Single(name) => (name.clone(), Vec::new()),
            JoinHintItem::Vector(path) => {
                if path.is_empty() {
                    return None;
                }
                let root_name = path[0].clone();
                let additional = if path.len() > 1 {
                    vec![JoinHintItem::Vector(path[1..].to_vec())]
                } else {
                    Vec::new()
                };
                (root_name, additional)
            }
        };

        // Combine additional cubes with cubes_to_join
        let mut all_cubes_to_join = additional_cubes;
        all_cubes_to_join.extend_from_slice(cubes_to_join);

        // Track which nodes have been joined
        let mut nodes_joined: HashSet<String> = HashSet::new();

        // Collect all joins with their indices
        let mut all_joins: Vec<(usize, JoinEdge)> = Vec::new();
        let mut next_index = 0;

        // Process each cube to join
        for join_hint in &all_cubes_to_join {
            // Convert to Vector if Single
            let path_elements = match join_hint {
                JoinHintItem::Single(name) => vec![name.clone()],
                JoinHintItem::Vector(path) => path.clone(),
            };

            // Find path from previous node to each target
            let mut prev_node = root_name.clone();

            for to_join in &path_elements {
                // Skip if already joined or same as previous
                if to_join == &prev_node {
                    continue;
                }

                if nodes_joined.contains(to_join) {
                    prev_node = to_join.clone();
                    continue;
                }

                // Find shortest path
                let path = find_shortest_path(&self.nodes, &prev_node, to_join);
                if path.is_none() {
                    return None; // Can't find path
                }

                let path = path.unwrap();

                // Convert path to joins
                let found_joins = self.joins_by_path(&path);

                // Add joins with indices
                for join in found_joins {
                    all_joins.push((next_index, join));
                    next_index += 1;
                }

                // Mark as joined
                nodes_joined.insert(to_join.clone());
                prev_node = to_join.clone();
            }
        }

        // Sort by index and remove duplicates
        all_joins.sort_by_key(|(idx, _)| *idx);

        // Remove duplicates by edge key
        let mut seen_keys: HashSet<String> = HashSet::new();
        let mut unique_joins: Vec<JoinEdge> = Vec::new();

        for (_, join) in all_joins {
            let key = Self::edge_key(&join.from, &join.to);
            if !seen_keys.contains(&key) {
                seen_keys.insert(key);
                unique_joins.push(join);
            }
        }

        Some((root_name, unique_joins))
    }

    /// Builds a join definition from a list of cubes to join
    ///
    /// This is the main entry point for finding optimal join paths between cubes.
    /// It tries each cube as a potential root and selects the shortest join tree.
    ///
    /// # Arguments
    /// * `cubes_to_join` - Vector of JoinHintItem specifying which cubes to join
    ///
    /// # Returns
    /// * `Ok(Rc<MockJoinDefinition>)` - The optimal join definition with multiplication factors
    /// * `Err(CubeError)` - If no join path exists or input is empty
    ///
    /// # Caching
    /// Results are cached based on the serialized cubes_to_join.
    /// Subsequent calls with the same cubes return the cached result.
    ///
    /// # Algorithm
    /// 1. Check cache for existing result
    /// 2. Try each cube as root, find shortest tree
    /// 3. Calculate multiplication factors for each cube
    /// 4. Create MockJoinDefinition with results
    /// 5. Cache and return
    ///
    /// # Example
    /// ```ignore
    /// let cubes = vec![
    ///     JoinHintItem::Single("orders".to_string()),
    ///     JoinHintItem::Single("users".to_string()),
    /// ];
    /// let join_def = graph.build_join(cubes)?;
    /// ```
    pub fn build_join(
        &self,
        cubes_to_join: Vec<JoinHintItem>,
    ) -> Result<Rc<MockJoinDefinition>, CubeError> {
        // Handle empty input
        if cubes_to_join.is_empty() {
            return Err(CubeError::user(
                "Cannot build join with empty cube list".to_string(),
            ));
        }

        // Check cache
        let cache_key = serde_json::to_string(&cubes_to_join).map_err(|e| {
            CubeError::internal(format!("Failed to serialize cubes_to_join: {}", e))
        })?;

        {
            let cache = self.built_joins.borrow();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        // Try each cube as root
        let mut join_trees: Vec<(String, Vec<JoinEdge>)> = Vec::new();

        for i in 0..cubes_to_join.len() {
            let root = &cubes_to_join[i];
            let mut other_cubes = Vec::new();
            other_cubes.extend_from_slice(&cubes_to_join[0..i]);
            other_cubes.extend_from_slice(&cubes_to_join[i + 1..]);

            if let Some(tree) = self.build_join_tree_for_root(root, &other_cubes) {
                join_trees.push(tree);
            }
        }

        // Sort by number of joins (shortest first)
        join_trees.sort_by_key(|(_, joins)| joins.len());

        // Take the shortest tree
        let (root_name, joins) = join_trees.first().ok_or_else(|| {
            let cube_names: Vec<String> = cubes_to_join
                .iter()
                .map(|hint| match hint {
                    JoinHintItem::Single(name) => format!("'{}'", name),
                    JoinHintItem::Vector(path) => format!("'{}'", path.join(".")),
                })
                .collect();
            CubeError::user(format!(
                "Can't find join path to join {}",
                cube_names.join(", ")
            ))
        })?;

        // Calculate multiplication factors
        let mut multiplication_factor: HashMap<String, bool> = HashMap::new();
        for cube_hint in &cubes_to_join {
            let cube_name = self.cube_from_path(cube_hint);
            let factor = self.find_multiplication_factor_for(&cube_name, joins);
            multiplication_factor.insert(cube_name, factor);
        }

        // Convert JoinEdges to MockJoinItems
        let join_items: Vec<Rc<crate::test_fixtures::cube_bridge::MockJoinItem>> = joins
            .iter()
            .map(|edge| self.join_edge_to_mock_join_item(edge))
            .collect();

        // Create MockJoinDefinition
        let join_def = Rc::new(
            MockJoinDefinition::builder()
                .root(root_name.clone())
                .joins(join_items)
                .multiplication_factor(multiplication_factor)
                .build(),
        );

        // Cache and return
        self.built_joins
            .borrow_mut()
            .insert(cache_key, join_def.clone());

        Ok(join_def)
    }

    /// Converts a JoinEdge to a MockJoinItem
    ///
    /// Helper method to convert internal JoinEdge representation to the MockJoinItem
    /// type used in MockJoinDefinition.
    ///
    /// # Arguments
    /// * `edge` - The JoinEdge to convert
    ///
    /// # Returns
    /// Rc<MockJoinItem> with the same from/to/original_from/original_to and join definition
    fn join_edge_to_mock_join_item(
        &self,
        edge: &JoinEdge,
    ) -> Rc<crate::test_fixtures::cube_bridge::MockJoinItem> {
        use crate::test_fixtures::cube_bridge::MockJoinItem;

        Rc::new(
            MockJoinItem::builder()
                .from(edge.from.clone())
                .to(edge.to.clone())
                .original_from(edge.original_from.clone())
                .original_to(edge.original_to.clone())
                .join(edge.join.clone())
                .build(),
        )
    }

    /// Checks if a specific join causes row multiplication for a cube
    ///
    /// # Multiplication Rules
    /// - If join.from == cube && relationship == "hasMany": multiplies
    /// - If join.to == cube && relationship == "belongsTo": multiplies
    /// - Otherwise: no multiplication
    ///
    /// # Arguments
    /// * `cube` - The cube name to check
    /// * `join` - The join edge to examine
    ///
    /// # Returns
    /// * `true` if this join multiplies rows for the cube
    /// * `false` otherwise
    fn check_if_cube_multiplied(&self, cube: &str, join: &JoinEdge) -> bool {
        let relationship = &join.join.static_data().relationship;

        (join.from == cube && relationship == "hasMany")
            || (join.to == cube && relationship == "belongsTo")
    }

    /// Determines if a cube has a multiplication factor in the join tree
    ///
    /// This method walks the join tree recursively to determine if joining
    /// this cube causes row multiplication due to hasMany or belongsTo relationships.
    ///
    /// # Algorithm
    /// 1. Start from the target cube
    /// 2. Find all adjacent joins in the tree
    /// 3. Check if any immediate join causes multiplication
    /// 4. If not, recursively check adjacent cubes
    /// 5. Use visited set to prevent infinite loops
    ///
    /// # Arguments
    /// * `cube` - The cube name to check
    /// * `joins` - The join edges in the tree
    ///
    /// # Returns
    /// * `true` if this cube causes row multiplication
    /// * `false` otherwise
    ///
    /// # Example
    /// ```ignore
    /// // users hasMany orders
    /// let joins = vec![join_users_to_orders];
    /// assert!(graph.find_multiplication_factor_for("users", &joins));
    /// assert!(!graph.find_multiplication_factor_for("orders", &joins));
    /// ```
    fn find_multiplication_factor_for(&self, cube: &str, joins: &[JoinEdge]) -> bool {
        use std::collections::HashSet;

        let mut visited: HashSet<String> = HashSet::new();

        fn find_if_multiplied_recursive(
            graph: &MockJoinGraph,
            current_cube: &str,
            joins: &[JoinEdge],
            visited: &mut HashSet<String>,
        ) -> bool {
            // Check if already visited (prevent cycles)
            if visited.contains(current_cube) {
                return false;
            }
            visited.insert(current_cube.to_string());

            // Helper to get next node in edge
            let next_node = |join: &JoinEdge| -> String {
                if join.from == current_cube {
                    join.to.clone()
                } else {
                    join.from.clone()
                }
            };

            // Find all joins adjacent to current cube
            let next_joins: Vec<&JoinEdge> = joins
                .iter()
                .filter(|j| j.from == current_cube || j.to == current_cube)
                .collect();

            // Check if any immediate join multiplies AND leads to unvisited node
            if next_joins.iter().any(|next_join| {
                let next = next_node(next_join);
                graph.check_if_cube_multiplied(current_cube, next_join) && !visited.contains(&next)
            }) {
                return true;
            }

            // Recursively check adjacent cubes
            next_joins.iter().any(|next_join| {
                let next = next_node(next_join);
                find_if_multiplied_recursive(graph, &next, joins, visited)
            })
        }

        find_if_multiplied_recursive(self, cube, joins, &mut visited)
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

        // First, ensure all cubes exist in nodes HashMap (even if they have no joins)
        for cube in cubes {
            let cube_name = cube.static_data().name.clone();
            self.nodes.entry(cube_name).or_insert_with(HashMap::new);
        }

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

    /// Recursively marks all cubes in a connected component
    ///
    /// This method performs a depth-first search starting from the given node,
    /// marking all reachable nodes with the same component ID. It uses the
    /// undirected_nodes graph to traverse in both directions.
    ///
    /// # Algorithm
    /// 1. Check if node already has a component ID (base case)
    /// 2. Assign component ID to current node
    /// 3. Find all connected nodes in undirected_nodes graph
    /// 4. Recursively process each connected node
    ///
    /// # Arguments
    /// * `component_id` - The ID to assign to this component
    /// * `node` - The current cube name being processed
    /// * `components` - Mutable map of cube -> component_id
    ///
    /// # Example
    /// ```ignore
    /// let mut components = HashMap::new();
    /// graph.find_connected_component(1, "users", &mut components);
    /// // All cubes reachable from "users" now have component_id = 1
    /// ```
    fn find_connected_component(
        &self,
        component_id: u32,
        node: &str,
        components: &mut HashMap<String, u32>,
    ) {
        // Base case: already visited
        if components.contains_key(node) {
            return;
        }

        // Mark this node with component ID
        components.insert(node.to_string(), component_id);

        // Get connected nodes from undirected graph (backward edges: to -> from)
        if let Some(connected_nodes) = self.undirected_nodes.get(node) {
            for connected_node in connected_nodes.keys() {
                self.find_connected_component(component_id, connected_node, components);
            }
        }

        // Also traverse forward edges (from -> to)
        if let Some(connected_nodes) = self.nodes.get(node) {
            for connected_node in connected_nodes.keys() {
                self.find_connected_component(component_id, connected_node, components);
            }
        }
    }

    /// Returns connected components of the join graph
    ///
    /// This method identifies which cubes are connected through join relationships.
    /// Cubes in the same component can be joined together. Cubes in different
    /// components cannot be joined and would result in a query error.
    ///
    /// Component IDs start at 1 and increment for each disconnected subgraph.
    /// Isolated cubes (with no joins) each get their own unique component ID.
    ///
    /// # Returns
    /// HashMap mapping cube name to component ID (1-based)
    ///
    /// # Example
    /// ```ignore
    /// // Graph: users <-> orders, products (isolated)
    /// let components = graph.connected_components();
    /// assert_eq!(components.get("users"), components.get("orders")); // Same component
    /// assert_ne!(components.get("users"), components.get("products")); // Different
    /// ```
    ///
    /// # Caching
    /// Results are cached and reused on subsequent calls until `compile()` is called.
    pub fn connected_components(&mut self) -> HashMap<String, u32> {
        // Return cached result if available
        if let Some(cached) = &self.cached_connected_components {
            return cached.clone();
        }

        let mut component_id: u32 = 1;
        let mut components: HashMap<String, u32> = HashMap::new();

        // Process all nodes (includes isolated cubes)
        let node_names: Vec<String> = self.nodes.keys().cloned().collect();

        for node in node_names {
            // Only process if not already assigned to a component
            if !components.contains_key(&node) {
                self.find_connected_component(component_id, &node, &mut components);
                component_id += 1;
            }
        }

        // Cache results
        self.cached_connected_components = Some(components.clone());

        components
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
        cubes_to_join: Vec<JoinHintItem>,
    ) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        // Call our implementation and cast to trait object
        let result = self.build_join(cubes_to_join)?;
        Ok(result as Rc<dyn JoinDefinition>)
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

        // Verify nodes correctly structured - all 4 cubes should be present
        assert_eq!(graph.nodes.len(), 4);
        assert!(graph.nodes.contains_key("orders"));
        assert!(graph.nodes.contains_key("products"));
        assert!(graph.nodes.contains_key("users"));
        assert!(graph.nodes.contains_key("categories"));

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
        assert!(graph.built_joins.borrow().is_empty());
    }

    // Tests for build_join functionality

    #[test]
    fn test_build_join_simple() {
        // Schema: A -> B
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

        // Build join: A -> B
        let cubes_to_join = vec![
            JoinHintItem::Single("A".to_string()),
            JoinHintItem::Single("B".to_string()),
        ];
        let result = graph.build_join(cubes_to_join).unwrap();

        // Expected: root=A, joins=[A->B], no multiplication
        assert_eq!(result.static_data().root, "A");
        let joins = result.joins().unwrap();
        assert_eq!(joins.len(), 1);

        let join_static = joins[0].static_data();
        assert_eq!(join_static.from, "A");
        assert_eq!(join_static.to, "B");

        // Check multiplication factors
        let mult_factors = result.static_data().multiplication_factor.clone();
        assert_eq!(mult_factors.get("A"), Some(&false));
        assert_eq!(mult_factors.get("B"), Some(&false));
    }

    #[test]
    fn test_build_join_chain() {
        // Schema: A -> B -> C
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

        // Build join: A -> B -> C
        let cubes_to_join = vec![
            JoinHintItem::Single("A".to_string()),
            JoinHintItem::Single("B".to_string()),
            JoinHintItem::Single("C".to_string()),
        ];
        let result = graph.build_join(cubes_to_join).unwrap();

        // Expected: root=A, joins=[A->B, B->C]
        assert_eq!(result.static_data().root, "A");
        let joins = result.joins().unwrap();
        assert_eq!(joins.len(), 2);

        assert_eq!(joins[0].static_data().from, "A");
        assert_eq!(joins[0].static_data().to, "B");
        assert_eq!(joins[1].static_data().from, "B");
        assert_eq!(joins[1].static_data().to, "C");
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
        // Schema: A -> B, A -> C, A -> D
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
            .add_join(
                "D",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.d_id = {D.id}".to_string())
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
            Rc::new(
                evaluator
                    .cube_from_path("D".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
        ];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

        // Build join: A, B, C, D
        let cubes_to_join = vec![
            JoinHintItem::Single("A".to_string()),
            JoinHintItem::Single("B".to_string()),
            JoinHintItem::Single("C".to_string()),
            JoinHintItem::Single("D".to_string()),
        ];
        let result = graph.build_join(cubes_to_join).unwrap();

        // Expected: root=A, joins to all others
        assert_eq!(result.static_data().root, "A");
        let joins = result.joins().unwrap();
        assert_eq!(joins.len(), 3);

        // All joins should be from A
        assert_eq!(joins[0].static_data().from, "A");
        assert_eq!(joins[1].static_data().from, "A");
        assert_eq!(joins[2].static_data().from, "A");
    }

    #[test]
    fn test_build_join_disconnected() {
        // Schema: A -> B, C -> D (no connection)
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
            .add_join(
                "D",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.d_id = {D.id}".to_string())
                    .build(),
            )
            .finish_cube()
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
            Rc::new(
                evaluator
                    .cube_from_path("D".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
        ];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

        // Build join: A, D (disconnected)
        let cubes_to_join = vec![
            JoinHintItem::Single("A".to_string()),
            JoinHintItem::Single("D".to_string()),
        ];
        let result = graph.build_join(cubes_to_join);

        // Expected: error "Can't find join path"
        assert!(result.is_err());
        let err_msg = result.unwrap_err().message;
        assert!(err_msg.contains("Can't find join path"));
        assert!(err_msg.contains("'A'"));
        assert!(err_msg.contains("'D'"));
    }

    #[test]
    fn test_build_join_empty() {
        let schema = MockSchemaBuilder::new().build();
        let evaluator = schema.create_evaluator();
        let cubes: Vec<Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>> = vec![];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

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
        // Schema: A
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
        graph.compile(&cubes, &evaluator).unwrap();

        // Build join with single cube
        let cubes_to_join = vec![JoinHintItem::Single("A".to_string())];
        let result = graph.build_join(cubes_to_join).unwrap();

        // Expected: root=A, no joins
        assert_eq!(result.static_data().root, "A");
        let joins = result.joins().unwrap();
        assert_eq!(joins.len(), 0);
    }

    #[test]
    fn test_build_join_caching() {
        // Schema: A -> B
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

        // Build join twice
        let cubes_to_join = vec![
            JoinHintItem::Single("A".to_string()),
            JoinHintItem::Single("B".to_string()),
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
    fn test_multiplication_factor_empty_joins() {
        // No joins, no multiplication
        let graph = MockJoinGraph::new();
        let joins = vec![];

        assert!(!graph.find_multiplication_factor_for("users", &joins));
    }

    #[test]
    fn test_multiplication_factor_disconnected() {
        // orders hasMany items (users disconnected)
        // users not in join tree, should not multiply
        let graph = MockJoinGraph::new();

        let joins = vec![JoinEdge {
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
        }];

        assert!(!graph.find_multiplication_factor_for("users", &joins));
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
        // Schema: A -> B -> C
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

        // Build join with Vector hint: [A, B] becomes root=A, join to B, then join to C
        let cubes_to_join = vec![
            JoinHintItem::Vector(vec!["A".to_string(), "B".to_string()]),
            JoinHintItem::Single("C".to_string()),
        ];
        let result = graph.build_join(cubes_to_join).unwrap();

        // Expected: root=A, joins=[A->B, B->C]
        assert_eq!(result.static_data().root, "A");
        let joins = result.joins().unwrap();
        assert_eq!(joins.len(), 2);

        assert_eq!(joins[0].static_data().from, "A");
        assert_eq!(joins[0].static_data().to, "B");
        assert_eq!(joins[1].static_data().from, "B");
        assert_eq!(joins[1].static_data().to, "C");
    }

    #[test]
    fn test_connected_components_simple() {
        // Graph: users -> orders (both in same component)
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

        let components = graph.connected_components();

        // Both cubes should be in same component
        assert_eq!(components.len(), 2);
        let users_comp = components.get("users").unwrap();
        let orders_comp = components.get("orders").unwrap();
        assert_eq!(users_comp, orders_comp);
    }

    #[test]
    fn test_connected_components_disconnected() {
        // Graph: users -> orders, products (isolated)
        // Two components: {users, orders}, {products}
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

        let components = graph.connected_components();

        // All three cubes should have component IDs
        assert_eq!(components.len(), 3);

        // users and orders in same component
        let users_comp = components.get("users").unwrap();
        let orders_comp = components.get("orders").unwrap();
        assert_eq!(users_comp, orders_comp);

        // products in different component
        let products_comp = components.get("products").unwrap();
        assert_ne!(users_comp, products_comp);
    }

    #[test]
    fn test_connected_components_all_isolated() {
        // Graph: A, B, C (no joins)
        // Three components: {A}, {B}, {C}
        let schema = MockSchemaBuilder::new()
            .add_cube("A")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .finish_cube()
            .add_cube("B")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
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

        let components = graph.connected_components();

        // All three cubes in different components
        assert_eq!(components.len(), 3);
        let a_comp = components.get("A").unwrap();
        let b_comp = components.get("B").unwrap();
        let c_comp = components.get("C").unwrap();
        assert_ne!(a_comp, b_comp);
        assert_ne!(b_comp, c_comp);
        assert_ne!(a_comp, c_comp);
    }

    #[test]
    fn test_connected_components_large_connected() {
        // Chain: A -> B -> C -> D (all in same component)
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
            .add_join(
                "D",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.d_id = {D.id}".to_string())
                    .build(),
            )
            .finish_cube()
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
            Rc::new(
                evaluator
                    .cube_from_path("D".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
        ];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

        let components = graph.connected_components();

        // All four cubes in same component
        assert_eq!(components.len(), 4);
        let a_comp = components.get("A").unwrap();
        let b_comp = components.get("B").unwrap();
        let c_comp = components.get("C").unwrap();
        let d_comp = components.get("D").unwrap();
        assert_eq!(a_comp, b_comp);
        assert_eq!(b_comp, c_comp);
        assert_eq!(c_comp, d_comp);
    }

    #[test]
    fn test_connected_components_cycle() {
        // Cycle: A -> B -> C -> A (all in same component)
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

        let components = graph.connected_components();

        // All three cubes in same component (cycle doesn't break connectivity)
        assert_eq!(components.len(), 3);
        let a_comp = components.get("A").unwrap();
        let b_comp = components.get("B").unwrap();
        let c_comp = components.get("C").unwrap();
        assert_eq!(a_comp, b_comp);
        assert_eq!(b_comp, c_comp);
    }

    #[test]
    fn test_connected_components_empty() {
        // Empty graph (no cubes)
        let mut graph = MockJoinGraph::new();
        let components = graph.connected_components();

        // Empty result
        assert_eq!(components.len(), 0);
    }

    #[test]
    fn test_connected_components_caching() {
        // Verify that results are cached
        let schema = MockSchemaBuilder::new()
            .add_cube("A")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
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
        graph.compile(&cubes, &evaluator).unwrap();

        // First call - computes components
        let components1 = graph.connected_components();
        assert_eq!(components1.len(), 1);

        // Second call - uses cache
        let components2 = graph.connected_components();
        assert_eq!(components2.len(), 1);
        assert_eq!(components1.get("A"), components2.get("A"));

        // Recompile - cache should be invalidated
        graph.compile(&cubes, &evaluator).unwrap();

        // Third call - recomputes after cache invalidation
        let components3 = graph.connected_components();
        assert_eq!(components3.len(), 1);
    }

    #[test]
    fn test_connected_components_multiple_groups() {
        // Three separate components: {A, B}, {C, D}, {E}
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
            .add_join(
                "D",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.d_id = {D.id}".to_string())
                    .build(),
            )
            .finish_cube()
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
            .add_cube("E")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
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
            Rc::new(
                evaluator
                    .cube_from_path("D".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
            Rc::new(
                evaluator
                    .cube_from_path("E".to_string())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<crate::test_fixtures::cube_bridge::MockCubeDefinition>()
                    .unwrap()
                    .clone(),
            ),
        ];

        let mut graph = MockJoinGraph::new();
        graph.compile(&cubes, &evaluator).unwrap();

        let components = graph.connected_components();

        // All five cubes should have component IDs
        assert_eq!(components.len(), 5);

        // A and B in same component
        let a_comp = components.get("A").unwrap();
        let b_comp = components.get("B").unwrap();
        assert_eq!(a_comp, b_comp);

        // C and D in same component
        let c_comp = components.get("C").unwrap();
        let d_comp = components.get("D").unwrap();
        assert_eq!(c_comp, d_comp);

        // E in its own component
        let e_comp = components.get("E").unwrap();

        // All three components are different
        assert_ne!(a_comp, c_comp);
        assert_ne!(a_comp, e_comp);
        assert_ne!(c_comp, e_comp);
    }
}
