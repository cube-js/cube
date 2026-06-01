use super::CommonUtils;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::planner::query_tools::QueryTools;
use crate::planner::{JoinTree, JoinTreeItem};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Resolves a `JoinDefinition` into a `JoinTree`: looks up each cube
/// and compiles its ON SQL once, so downstream planning can reuse the
/// compiled conditions instead of recompiling them on every use.
pub struct JoinTreeBuilder {
    utils: CommonUtils,
}

impl JoinTreeBuilder {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self {
            utils: CommonUtils::new(query_tools),
        }
    }

    pub fn build(&self, join: Rc<dyn JoinDefinition>) -> Result<Rc<JoinTree>, CubeError> {
        let root = self.utils.cube_from_path(join.static_data().root.clone())?;
        let mut joins = vec![];
        for join_definition in join.joins()?.iter() {
            let static_data = join_definition.static_data();
            let cube = self.utils.cube_from_path(static_data.original_to.clone())?;
            let on_sql = self.utils.compile_join_condition(join_definition.clone())?;
            joins.push(JoinTreeItem::new(
                cube,
                static_data.original_from.clone(),
                on_sql,
            ));
        }
        Ok(JoinTree::new(
            root,
            joins,
            join.static_data().multiplication_factor.clone(),
        ))
    }
}
