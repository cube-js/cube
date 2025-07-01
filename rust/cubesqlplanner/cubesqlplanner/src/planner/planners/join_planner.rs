use super::CommonUtils;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::join_item::JoinItem;
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlCall;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct JoinPlanner {
    utils: CommonUtils,
    query_tools: Rc<QueryTools>,
}

impl JoinPlanner {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self {
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
        }
    }

    pub fn make_join_logical_plan_with_join_hints(
        &self,
        join_hints: Vec<JoinHintItem>,
    ) -> Result<Rc<LogicalJoin>, CubeError> {
        let join = self.query_tools.join_graph().build_join(join_hints)?;
        self.make_join_logical_plan(join)
    }

    pub fn make_join_logical_plan(
        &self,
        join: Rc<dyn JoinDefinition>,
    ) -> Result<Rc<LogicalJoin>, CubeError> {
        let root_definition = self.utils.cube_from_path(join.static_data().root.clone())?;
        let root = Cube::new(root_definition);
        let joins_definitions = join.joins()?;
        let mut joins = vec![];
        for join_definition in joins_definitions.iter() {
            let cube_definition = self
                .utils
                .cube_from_path(join_definition.static_data().original_to.clone())?;
            let cube = Cube::new(cube_definition);
            let on_sql = self.compile_join_condition(join_definition.clone())?;
            joins.push(LogicalJoinItem::CubeJoinItem(CubeJoinItem { cube, on_sql }));
        }

        Ok(Rc::new(LogicalJoin { root, joins }))
    }

    pub fn compile_join_condition(
        &self,
        join_item: Rc<dyn JoinItem>,
    ) -> Result<Rc<SqlCall>, CubeError> {
        let definition = join_item.join()?;
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        evaluator_compiler
            .compile_sql_call(&join_item.static_data().original_from, definition.sql()?)
    }
}
