use super::{CommonUtils, DimensionSubqueryPlanner};
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::join_item::JoinItem;
use crate::plan::{From, JoinBuilder, JoinCondition};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlCall;
use crate::planner::SqlJoinCondition;
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

    pub fn make_join_node_with_prefix_and_join_hints(
        &self,
        alias_prefix: &Option<String>, /*TODO dimensions for subqueries*/
        join_hints: Vec<JoinHintItem>,
        dimension_subquery_planner: &DimensionSubqueryPlanner,
    ) -> Result<Rc<From>, CubeError> {
        let join = self.query_tools.join_graph().build_join(join_hints)?;
        self.make_join_node_impl(alias_prefix, join, dimension_subquery_planner)
    }

    pub fn make_join_node_impl(
        &self,
        alias_prefix: &Option<String>,
        join: Rc<dyn JoinDefinition>,
        dimension_subquery_planner: &DimensionSubqueryPlanner,
    ) -> Result<Rc<From>, CubeError> {
        let root = self.utils.cube_from_path(join.static_data().root.clone())?;
        let joins = join.joins()?;
        if joins.is_empty() && dimension_subquery_planner.is_empty() {
            Ok(From::new_from_cube(root, None))
        } else {
            let mut join_builder = JoinBuilder::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(alias_prefix)),
            );
            dimension_subquery_planner.add_joins_for_cube(&mut join_builder, root.name())?;
            for join in joins.iter() {
                let sql_call = self.compile_join_condition(join.clone())?;
                let on = JoinCondition::new_base_join(SqlJoinCondition::try_new(
                    self.query_tools.clone(),
                    sql_call,
                )?);
                let cube = self
                    .utils
                    .cube_from_path(join.static_data().original_to.clone())?;
                join_builder.left_join_cube(
                    cube.clone(),
                    Some(cube.default_alias_with_prefix(alias_prefix)),
                    on,
                );
                dimension_subquery_planner.add_joins_for_cube(&mut join_builder, cube.name())?;
            }
            let result = From::new_from_join(join_builder.build());
            Ok(result)
        }
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
