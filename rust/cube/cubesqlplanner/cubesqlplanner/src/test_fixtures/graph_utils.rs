use petgraph::graph::NodeIndex;
use petgraph::Graph;
use std::collections::HashMap;

/// Converts a HashMap-based graph representation to a petgraph directed graph.
///
/// # Input Format
///
/// The input is a nested HashMap where:
/// - Outer map keys are cube names (all nodes in the graph)
/// - Inner map represents edges: destination cube name -> edge weight
///
///
/// # Returns
///
/// A tuple containing:
/// - Directed graph with cube names as node data and weights as edge data
/// - Mapping from cube name to NodeIndex for quick lookups
///
/// # Note
///
/// All cube names that appear in edges must also exist as keys in the outer HashMap.
pub fn build_petgraph_from_hashmap(
    nodes: &HashMap<String, HashMap<String, u32>>,
) -> (Graph<String, u32>, HashMap<String, NodeIndex>) {
    let mut graph = Graph::<String, u32>::new();
    let mut node_indices = HashMap::new();

    // First pass: Add all nodes to the graph
    for cube_name in nodes.keys() {
        let node_index = graph.add_node(cube_name.clone());
        node_indices.insert(cube_name.clone(), node_index);
    }

    // Second pass: Add all edges
    for (from_cube, edges) in nodes.iter() {
        let from_index = node_indices[from_cube];
        for (to_cube, weight) in edges.iter() {
            let to_index = node_indices[to_cube];
            graph.add_edge(from_index, to_index, *weight);
        }
    }

    (graph, node_indices)
}

/// Finds the shortest path between two cubes using Dijkstra's algorithm.
///
/// This function wraps petgraph's A* algorithm with a zero heuristic, which is
/// equivalent to Dijkstra's algorithm. It provides an API similar to node-dijkstra
/// for JavaScript compatibility.
///
/// # Arguments
///
/// * `nodes` - Graph representation as nested HashMap (see `build_petgraph_from_hashmap`)
/// * `start` - Name of the starting cube
/// * `end` - Name of the destination cube
///
/// # Returns
///
/// - `Some(Vec<String>)` - Path from start to end (inclusive) if a path exists
/// - `None` - If no path exists or if start/end nodes don't exist in the graph
///
/// # Edge Cases
///
/// - If `start == end`, returns `Some(vec![start])`
/// - If `start` or `end` don't exist in the graph, returns `None`
/// - If nodes are disconnected, returns `None`
///
/// ```
pub fn find_shortest_path(
    nodes: &HashMap<String, HashMap<String, u32>>,
    start: &str,
    end: &str,
) -> Option<Vec<String>> {
    if start == end {
        return Some(vec![start.to_string()]);
    }

    if !nodes.contains_key(start) || !nodes.contains_key(end) {
        return None;
    }

    let (graph, node_indices) = build_petgraph_from_hashmap(nodes);

    let start_index = node_indices[start];
    let end_index = node_indices[end];

    let result = petgraph::algo::astar(
        &graph,
        start_index,
        |n| n == end_index,
        |e| *e.weight(),
        |_| 0, // Zero heuristic makes this equivalent to Dijkstra
    );

    match result {
        Some((_cost, path)) => {
            let cube_names: Vec<String> = path
                .iter()
                .map(|&node_index| graph[node_index].clone())
                .collect();
            Some(cube_names)
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_path() {
        // Graph: A -> B (weight 1)
        let mut nodes = HashMap::new();
        let mut a_edges = HashMap::new();
        a_edges.insert("B".to_string(), 1);
        nodes.insert("A".to_string(), a_edges);
        nodes.insert("B".to_string(), HashMap::new());

        let path = find_shortest_path(&nodes, "A", "B");
        assert_eq!(path, Some(vec!["A".to_string(), "B".to_string()]));
    }

    #[test]
    fn test_multi_hop_path() {
        // Graph: A -> B -> C
        let mut nodes = HashMap::new();
        let mut a_edges = HashMap::new();
        a_edges.insert("B".to_string(), 1);
        nodes.insert("A".to_string(), a_edges);

        let mut b_edges = HashMap::new();
        b_edges.insert("C".to_string(), 1);
        nodes.insert("B".to_string(), b_edges);

        nodes.insert("C".to_string(), HashMap::new());

        let path = find_shortest_path(&nodes, "A", "C");
        assert_eq!(
            path,
            Some(vec!["A".to_string(), "B".to_string(), "C".to_string()])
        );
    }

    #[test]
    fn test_shortest_path_selection() {
        // Graph: A -> B -> C (total weight 2)
        //        A -> D -> C (total weight 5)
        let mut nodes = HashMap::new();

        let mut a_edges = HashMap::new();
        a_edges.insert("B".to_string(), 1);
        a_edges.insert("D".to_string(), 3);
        nodes.insert("A".to_string(), a_edges);

        let mut b_edges = HashMap::new();
        b_edges.insert("C".to_string(), 1);
        nodes.insert("B".to_string(), b_edges);

        let mut d_edges = HashMap::new();
        d_edges.insert("C".to_string(), 2);
        nodes.insert("D".to_string(), d_edges);

        nodes.insert("C".to_string(), HashMap::new());

        let path = find_shortest_path(&nodes, "A", "C");
        // Should take the shorter path: A -> B -> C
        assert_eq!(
            path,
            Some(vec!["A".to_string(), "B".to_string(), "C".to_string()])
        );
    }

    #[test]
    fn test_disconnected_nodes() {
        // Graph: A -> B, C -> D (no connection between them)
        let mut nodes = HashMap::new();

        let mut a_edges = HashMap::new();
        a_edges.insert("B".to_string(), 1);
        nodes.insert("A".to_string(), a_edges);

        nodes.insert("B".to_string(), HashMap::new());

        let mut c_edges = HashMap::new();
        c_edges.insert("D".to_string(), 1);
        nodes.insert("C".to_string(), c_edges);

        nodes.insert("D".to_string(), HashMap::new());

        // No path from A to D
        let path = find_shortest_path(&nodes, "A", "D");
        assert_eq!(path, None);
    }

    #[test]
    fn test_same_start_and_end() {
        // Graph: A -> B
        let mut nodes = HashMap::new();
        let mut a_edges = HashMap::new();
        a_edges.insert("B".to_string(), 1);
        nodes.insert("A".to_string(), a_edges);
        nodes.insert("B".to_string(), HashMap::new());

        // Path from A to A should be just [A]
        let path = find_shortest_path(&nodes, "A", "A");
        assert_eq!(path, Some(vec!["A".to_string()]));
    }

    #[test]
    fn test_nonexistent_node() {
        // Graph: A -> B
        let mut nodes = HashMap::new();
        let mut a_edges = HashMap::new();
        a_edges.insert("B".to_string(), 1);
        nodes.insert("A".to_string(), a_edges);
        nodes.insert("B".to_string(), HashMap::new());

        // C doesn't exist
        let path = find_shortest_path(&nodes, "A", "C");
        assert_eq!(path, None);

        // Z doesn't exist either
        let path = find_shortest_path(&nodes, "Z", "A");
        assert_eq!(path, None);
    }

    #[test]
    fn test_graph_with_cycles() {
        // Graph: A -> B -> C -> A (cycle)
        //        A -> D -> C (alternate path)
        let mut nodes = HashMap::new();

        let mut a_edges = HashMap::new();
        a_edges.insert("B".to_string(), 1);
        a_edges.insert("D".to_string(), 5);
        nodes.insert("A".to_string(), a_edges);

        let mut b_edges = HashMap::new();
        b_edges.insert("C".to_string(), 1);
        nodes.insert("B".to_string(), b_edges);

        let mut c_edges = HashMap::new();
        c_edges.insert("A".to_string(), 1); // Cycle back to A
        nodes.insert("C".to_string(), c_edges);

        let mut d_edges = HashMap::new();
        d_edges.insert("C".to_string(), 1);
        nodes.insert("D".to_string(), d_edges);

        // Should find shortest path A -> B -> C
        let path = find_shortest_path(&nodes, "A", "C");
        assert_eq!(
            path,
            Some(vec!["A".to_string(), "B".to_string(), "C".to_string()])
        );
    }

    #[test]
    fn test_build_petgraph_from_hashmap() {
        // Verify graph is constructed correctly
        let mut nodes = HashMap::new();

        let mut a_edges = HashMap::new();
        a_edges.insert("B".to_string(), 1);
        a_edges.insert("C".to_string(), 2);
        nodes.insert("A".to_string(), a_edges);

        let mut b_edges = HashMap::new();
        b_edges.insert("C".to_string(), 1);
        nodes.insert("B".to_string(), b_edges);

        nodes.insert("C".to_string(), HashMap::new());

        let (graph, node_indices) = build_petgraph_from_hashmap(&nodes);

        // Check node count
        assert_eq!(graph.node_count(), 3);

        // Check edge count: A->B, A->C, B->C = 3 edges
        assert_eq!(graph.edge_count(), 3);

        // Check that all node names are in the mapping
        assert!(node_indices.contains_key("A"));
        assert!(node_indices.contains_key("B"));
        assert!(node_indices.contains_key("C"));

        // Check that node indices are valid
        let a_index = node_indices["A"];
        let b_index = node_indices["B"];
        let c_index = node_indices["C"];

        assert_eq!(graph[a_index], "A");
        assert_eq!(graph[b_index], "B");
        assert_eq!(graph[c_index], "C");
    }
}
