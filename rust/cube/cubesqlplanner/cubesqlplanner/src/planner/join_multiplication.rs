use crate::planner::CubeId;
use std::collections::HashSet;

/// One edge of a join tree as far as row multiplication is concerned.
pub struct MultiplicationEdge<'a> {
    pub from: &'a CubeId,
    pub to: &'a CubeId,
    pub relationship: &'a str,
}

fn is_has_many(relationship: &str) -> bool {
    matches!(
        relationship,
        "hasMany" | "has_many" | "one_to_many" | "oneToMany"
    )
}

fn is_belongs_to(relationship: &str) -> bool {
    matches!(
        relationship,
        "belongsTo" | "belongs_to" | "many_to_one" | "manyToOne"
    )
}

fn multiplies(cube: &CubeId, edge: &MultiplicationEdge) -> bool {
    (edge.from == cube && is_has_many(edge.relationship))
        || (edge.to == cube && is_belongs_to(edge.relationship))
}

/// Whether the rows of `cube` repeat once the tree's joins are applied: some
/// cube reachable from it, walking edges in either direction, joins in more
/// than one row per row on its side.
pub fn is_multiplied(cube: &CubeId, edges: &[MultiplicationEdge]) -> bool {
    fn walk(current: &CubeId, edges: &[MultiplicationEdge], visited: &mut HashSet<CubeId>) -> bool {
        if !visited.insert(current.clone()) {
            return false;
        }
        let next_node = |edge: &MultiplicationEdge| -> CubeId {
            if edge.from == current {
                edge.to.clone()
            } else {
                edge.from.clone()
            }
        };
        let next_edges = edges
            .iter()
            .filter(|edge| edge.from == current || edge.to == current)
            .collect::<Vec<_>>();
        if next_edges
            .iter()
            .any(|edge| multiplies(current, edge) && !visited.contains(&next_node(edge)))
        {
            return true;
        }
        next_edges
            .iter()
            .any(|edge| walk(&next_node(edge), edges, visited))
    }
    walk(cube, edges, &mut HashSet::new())
}
