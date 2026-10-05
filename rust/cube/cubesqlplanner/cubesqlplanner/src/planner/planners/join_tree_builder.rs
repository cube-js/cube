use super::CommonUtils;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::planner::collectors::collect_cube_names_from_symbols;
use crate::planner::join_hints::{JoinHint, JoinHints};
use crate::planner::join_multiplication::{is_multiplied, MultiplicationEdge};
use crate::planner::query_tools::JoinKey;
use crate::planner::state::State;
use crate::planner::{CubeId, JoinTree, JoinTreeItem};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Resolves join hints into a `JoinTree`: the join graph builds the tree over
/// data-model cubes, joined cube instances are attached here, and every ON SQL
/// is compiled once.
pub struct JoinTreeBuilder {
    query_tools: Rc<State>,
    utils: CommonUtils,
}

impl JoinTreeBuilder {
    pub fn new(query_tools: Rc<State>) -> Self {
        Self {
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
        }
    }

    /// The join graph's tree for `hints`.
    pub fn build_for_hints(&self, hints: &JoinHints) -> Result<(JoinKey, Rc<JoinTree>), CubeError> {
        let (graph_hints, joined) = hints.split_joined();
        let join = self.query_tools.join_graph().build_join(graph_hints)?;
        let tree = self.build(join, hints, &joined)?;
        Ok((JoinKey::from_tree(&tree), tree))
    }

    /// The query's join resolution of `hints`, or `None` when the join graph
    /// has no path covering them.
    pub fn try_build_for_hints(
        &self,
        hints: &JoinHints,
    ) -> Result<Option<(JoinKey, Rc<JoinTree>)>, CubeError> {
        let (graph_hints, joined) = hints.split_joined();
        let Some(join) = self
            .query_tools
            .base_tools()
            .try_join_tree_for_hints(graph_hints)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let tree = self.build(join, hints, &joined)?;
        Ok(Some((JoinKey::from_tree(&tree), tree)))
    }

    fn build(
        &self,
        join: Rc<dyn JoinDefinition>,
        hints: &JoinHints,
        joined: &[CubeId],
    ) -> Result<Rc<JoinTree>, CubeError> {
        let root = self
            .utils
            .cube_from_path(&CubeId::cube(join.static_data().root.clone()))?;
        let mut joins = vec![];
        for join_definition in join.joins()?.iter() {
            let static_data = join_definition.static_data();
            let cube = self
                .utils
                .cube_from_path(&CubeId::cube(static_data.original_to.clone()))?;
            let on_sql = self.utils.compile_join_condition(join_definition.clone())?;
            let relationship = join_definition.join()?.static_data().relationship.clone();
            joins.push(JoinTreeItem::new(
                cube,
                CubeId::cube(static_data.original_from.clone()),
                on_sql,
                relationship.clone(),
                relationship_splits_rows(&relationship),
            ));
        }
        for instance in joined {
            joins.push(self.joined_item(instance, root.cube_id(), &joins)?);
        }

        let multiplication_factor = if joined.is_empty() {
            join.static_data()
                .multiplication_factor
                .iter()
                .map(|(cube, multiplied)| (CubeId::cube(cube.clone()), *multiplied))
                .collect()
        } else {
            Self::multiplication_factor(hints, &joins)
        };
        Ok(JoinTree::new(root, joins, multiplication_factor))
    }

    // Joins a cube instance to its parent, which is already in the tree: the
    // root, a data-model cube the join graph joined, or an instance attached
    // before it.
    fn joined_item(
        &self,
        instance: &CubeId,
        root: &CubeId,
        joins: &[JoinTreeItem],
    ) -> Result<JoinTreeItem, CubeError> {
        let parent = instance.parent().ok_or_else(|| {
            CubeError::internal(format!("`{}` is not a joined cube instance", instance))
        })?;
        if parent != root && !joins.iter().any(|item| item.cube().cube_id() == parent) {
            return Err(CubeError::user(format!(
                "Can't join `{}`: `{}` is not part of the join tree",
                instance, parent
            )));
        }
        let join = self
            .query_tools
            .model_cubes()
            .find_join(parent.target(), instance.segment())?
            .ok_or_else(|| {
                CubeError::internal(format!(
                    "Cube `{}` declares no join `{}`",
                    parent.target(),
                    instance.segment()
                ))
            })?;
        let on_sql = self
            .utils
            .compile_instance_join_condition(instance, &join)?;
        // Only the instance and the cubes it is reached through are in the
        // tree when its ON SQL is rendered, members read on the way included
        let mut cubes = collect_cube_names_from_symbols(&on_sql.get_dependencies())?;
        cubes.extend(on_sql.get_cube_refs().iter().map(|r| r.cube_id().clone()));
        let in_tree = |cube: &CubeId| {
            let mut current = Some(instance);
            while let Some(id) = current {
                if id == cube {
                    return true;
                }
                current = id.parent();
            }
            false
        };
        if let Some(outside) = cubes.iter().find(|cube| !in_tree(cube)) {
            return Err(CubeError::user(format!(
                "The join `{}` of cube `{}` reads `{}`, which is neither side of the join",
                instance.segment(),
                parent.target(),
                outside
            )));
        }
        if !cubes.contains(instance) {
            return Err(CubeError::user(format!(
                "The join `{}` of cube `{}` never reads the joined cube. Refer to it as `{{{}}}`",
                instance.segment(),
                parent.target(),
                instance.segment()
            )));
        }
        let relationship = join.definition().static_data().relationship.clone();
        Ok(JoinTreeItem::new(
            self.utils.cube_from_path(instance)?,
            parent.clone(),
            on_sql,
            relationship.clone(),
            relationship_splits_rows(&relationship),
        ))
    }

    fn multiplication_factor(hints: &JoinHints, joins: &[JoinTreeItem]) -> HashMap<CubeId, bool> {
        let edges = joins
            .iter()
            .map(|item| MultiplicationEdge {
                from: item.original_from(),
                to: item.cube().cube_id(),
                relationship: item.relationship(),
            })
            .collect::<Vec<_>>();
        hints
            .iter()
            .filter_map(|hint| match hint {
                JoinHint::Single(cube) => Some(cube),
                JoinHint::Vector(path) => path.last(),
            })
            .map(|cube| (cube.clone(), is_multiplied(cube, &edges)))
            .collect()
    }
}

/// Whether joining the `to` side of an edge with this relationship splits one row
/// of the `from` side into several. Only many-to-one and one-to-one keep the row
/// count, and a relationship arrives either normalized or in one of its model
/// spellings, so every spelling of those two is listed. Anything unrecognized
/// counts as splitting: requiring a primary key that is not needed only refuses a
/// usable pre-aggregation, while omitting a needed one serves collapsed rows.
fn relationship_splits_rows(relationship: &str) -> bool {
    !matches!(
        relationship,
        "belongsTo"
            | "belongs_to"
            | "many_to_one"
            | "manyToOne"
            | "hasOne"
            | "has_one"
            | "one_to_one"
            | "oneToOne"
    )
}

#[cfg(test)]
mod tests {
    use super::relationship_splits_rows;

    #[test]
    fn splits_rows_covers_every_relationship_spelling() {
        for keeps_row_count in ["belongsTo", "many_to_one", "hasOne", "one_to_one"] {
            assert!(
                !relationship_splits_rows(keeps_row_count),
                "`{keeps_row_count}` joins at most one row"
            );
        }
        for splits in ["hasMany", "one_to_many"] {
            assert!(
                relationship_splits_rows(splits),
                "`{splits}` joins many rows"
            );
        }
        assert!(
            relationship_splits_rows("something_else"),
            "an unrecognized relationship has to be treated as splitting"
        );
    }
}
