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
/// ```
#[derive(Debug, Clone)]
pub struct JoinEdge {
    pub join: Rc<MockJoinItemDefinition>,
    pub from: String,
    pub to: String,
    pub original_from: String,
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
/// ```
#[derive(Clone)]
pub struct MockJoinGraph {
    /// Directed graph: source -> destination -> weight
    /// Represents the directed join relationships between cubes
    pub(crate) nodes: HashMap<String, HashMap<String, u32>>,

    /// Undirected graph: destination -> source -> weight
    /// Used for connectivity checks and pathfinding
    pub(crate) undirected_nodes: HashMap<String, HashMap<String, u32>>,

    /// Edge lookup: "from-to" -> JoinEdge
    /// Maps edge keys to their corresponding join definitions
    pub(crate) edges: HashMap<String, JoinEdge>,

    /// Cache of built join trees: serialized cubes -> JoinDefinition
    /// Stores previously computed join paths for reuse
    /// Uses RefCell for interior mutability (allows caching through &self)
    pub(crate) built_joins: RefCell<HashMap<String, Rc<MockJoinDefinition>>>,

    /// Cache for connected components
    /// Stores the connected component ID for each cube
    /// None until first calculation
    pub(crate) cached_connected_components: Option<HashMap<String, u32>>,
}

impl MockJoinGraph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            undirected_nodes: HashMap::new(),
            edges: HashMap::new(),
            built_joins: RefCell::new(HashMap::new()),
            cached_connected_components: None,
        }
    }

    pub(crate) fn edge_key(from: &str, to: &str) -> String {
        format!("{}-{}", from, to)
    }

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
            if !evaluator.cube_exists(join_name.clone())? {
                return Err(CubeError::user(format!("Cube {} doesn't exist", join_name)));
            }

            let from_multiplied = self.get_multiplied_measures(cube_name, evaluator)?;
            if !from_multiplied.is_empty() {
                let static_data = evaluator.static_data();
                let primary_keys = static_data.primary_keys.get(cube_name);
                if primary_keys.is_none_or(|pk| pk.is_empty()) {
                    return Err(CubeError::user(format!(
                        "primary key for '{}' is required when join is defined in order to make aggregates work properly",
                        cube_name
                    )));
                }
            }

            let to_multiplied = self.get_multiplied_measures(join_name, evaluator)?;
            if !to_multiplied.is_empty() {
                let static_data = evaluator.static_data();
                let primary_keys = static_data.primary_keys.get(join_name);
                if primary_keys.is_none_or(|pk| pk.is_empty()) {
                    return Err(CubeError::user(format!(
                        "primary key for '{}' is required when join is defined in order to make aggregates work properly",
                        join_name
                    )));
                }
            }

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

    fn cube_from_path(&self, cube_path: &JoinHintItem) -> String {
        match cube_path {
            JoinHintItem::Single(name) => name.clone(),
            JoinHintItem::Vector(path) => path
                .last()
                .expect("Vector path should not be empty")
                .clone(),
        }
    }

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

    fn build_join_tree_for_root(
        &self,
        root: &JoinHintItem,
        cubes_to_join: &[JoinHintItem],
    ) -> Option<(String, Vec<JoinEdge>)> {
        use crate::test_fixtures::graph_utils::find_shortest_path;
        use std::collections::HashSet;

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

        let mut all_cubes_to_join = additional_cubes;
        all_cubes_to_join.extend_from_slice(cubes_to_join);

        let mut nodes_joined: HashSet<String> = HashSet::new();

        let mut all_joins: Vec<(usize, JoinEdge)> = Vec::new();
        let mut next_index = 0;

        for join_hint in &all_cubes_to_join {
            let path_elements = match join_hint {
                JoinHintItem::Single(name) => vec![name.clone()],
                JoinHintItem::Vector(path) => path.clone(),
            };

            let mut prev_node = root_name.clone();

            for to_join in &path_elements {
                if to_join == &prev_node {
                    continue;
                }

                if nodes_joined.contains(to_join) {
                    prev_node = to_join.clone();
                    continue;
                }

                let path = find_shortest_path(&self.nodes, &prev_node, to_join);
                path.as_ref()?;

                let path = path.unwrap();

                let found_joins = self.joins_by_path(&path);

                for join in found_joins {
                    all_joins.push((next_index, join));
                    next_index += 1;
                }

                nodes_joined.insert(to_join.clone());
                prev_node = to_join.clone();
            }
        }

        all_joins.sort_by_key(|(idx, _)| *idx);

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

    pub fn build_join(
        &self,
        cubes_to_join: Vec<JoinHintItem>,
    ) -> Result<Rc<MockJoinDefinition>, CubeError> {
        if cubes_to_join.is_empty() {
            return Err(CubeError::user(
                "Cannot build join with empty cube list".to_string(),
            ));
        }

        let cache_key = serde_json::to_string(&cubes_to_join).map_err(|e| {
            CubeError::internal(format!("Failed to serialize cubes_to_join: {}", e))
        })?;

        {
            let cache = self.built_joins.borrow();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

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

        join_trees.sort_by_key(|(_, joins)| joins.len());

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

        let mut multiplication_factor: HashMap<String, bool> = HashMap::new();
        for cube_hint in &cubes_to_join {
            let cube_name = self.cube_from_path(cube_hint);
            let factor = self.find_multiplication_factor_for(&cube_name, joins);
            multiplication_factor.insert(cube_name, factor);
        }

        let join_items: Vec<Rc<crate::test_fixtures::cube_bridge::MockJoinItem>> = joins
            .iter()
            .map(|edge| self.join_edge_to_mock_join_item(edge))
            .collect();

        let join_def = Rc::new(
            MockJoinDefinition::builder()
                .root(root_name.clone())
                .joins(join_items)
                .multiplication_factor(multiplication_factor)
                .build(),
        );

        self.built_joins
            .borrow_mut()
            .insert(cache_key, join_def.clone());

        Ok(join_def)
    }

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

    pub(crate) fn check_if_cube_multiplied(&self, cube: &str, join: &JoinEdge) -> bool {
        let relationship = &join.join.static_data().relationship;

        (join.from == cube && relationship == "hasMany")
            || (join.to == cube && relationship == "belongsTo")
    }

    pub(crate) fn find_multiplication_factor_for(&self, cube: &str, joins: &[JoinEdge]) -> bool {
        use std::collections::HashSet;

        let mut visited: HashSet<String> = HashSet::new();

        fn find_if_multiplied_recursive(
            graph: &MockJoinGraph,
            current_cube: &str,
            joins: &[JoinEdge],
            visited: &mut HashSet<String>,
        ) -> bool {
            if visited.contains(current_cube) {
                return false;
            }
            visited.insert(current_cube.to_string());

            let next_node = |join: &JoinEdge| -> String {
                if join.from == current_cube {
                    join.to.clone()
                } else {
                    join.from.clone()
                }
            };

            let next_joins: Vec<&JoinEdge> = joins
                .iter()
                .filter(|j| j.from == current_cube || j.to == current_cube)
                .collect();

            if next_joins.iter().any(|next_join| {
                let next = next_node(next_join);
                graph.check_if_cube_multiplied(current_cube, next_join) && !visited.contains(&next)
            }) {
                return true;
            }

            next_joins.iter().any(|next_join| {
                let next = next_node(next_join);
                find_if_multiplied_recursive(graph, &next, joins, visited)
            })
        }

        find_if_multiplied_recursive(self, cube, joins, &mut visited)
    }

    pub fn compile(
        &mut self,
        cubes: &[Rc<crate::test_fixtures::cube_bridge::MockCubeDefinition>],
        evaluator: &crate::test_fixtures::cube_bridge::MockCubeEvaluator,
    ) -> Result<(), CubeError> {
        self.edges.clear();
        self.nodes.clear();
        self.undirected_nodes.clear();
        self.cached_connected_components = None;

        for cube in cubes {
            let cube_name = cube.static_data().name.clone();
            self.nodes.entry(cube_name).or_default();
        }

        for cube in cubes {
            let cube_edges = self.build_join_edges(cube, evaluator)?;
            for (key, edge) in cube_edges {
                self.edges.insert(key, edge);
            }
        }

        for edge in self.edges.values() {
            self.nodes
                .entry(edge.from.clone())
                .or_default()
                .insert(edge.to.clone(), 1);
        }

        for edge in self.edges.values() {
            self.undirected_nodes
                .entry(edge.to.clone())
                .or_default()
                .insert(edge.from.clone(), 1);
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn find_connected_component(
        &self,
        component_id: u32,
        node: &str,
        components: &mut HashMap<String, u32>,
    ) {
        if components.contains_key(node) {
            return;
        }

        components.insert(node.to_string(), component_id);

        if let Some(connected_nodes) = self.undirected_nodes.get(node) {
            for connected_node in connected_nodes.keys() {
                self.find_connected_component(component_id, connected_node, components);
            }
        }

        if let Some(connected_nodes) = self.nodes.get(node) {
            for connected_node in connected_nodes.keys() {
                self.find_connected_component(component_id, connected_node, components);
            }
        }
    }

    #[allow(dead_code)]
    pub fn connected_components(&mut self) -> HashMap<String, u32> {
        if let Some(cached) = &self.cached_connected_components {
            return cached.clone();
        }

        let mut component_id: u32 = 1;
        let mut components: HashMap<String, u32> = HashMap::new();

        let node_names: Vec<String> = self.nodes.keys().cloned().collect();

        for node in node_names {
            if !components.contains_key(&node) {
                self.find_connected_component(component_id, &node, &mut components);
                component_id += 1;
            }
        }

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
        let result = self.build_join(cubes_to_join)?;
        Ok(result as Rc<dyn JoinDefinition>)
    }
}
