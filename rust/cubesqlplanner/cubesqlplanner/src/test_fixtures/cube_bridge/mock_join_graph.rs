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
}
