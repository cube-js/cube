use super::CommonUtils;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::plan::{From, JoinBuilder, JoinCondition};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::EvaluationNode;
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

    pub fn make_join_node_with_prefix(
        &self,
        alias_prefix: &Option<String>, /*TODO dimensions for subqueries*/
    ) -> Result<From, CubeError> {
        let join = self.query_tools.cached_data().join()?.clone();
        let root = self.utils.cube_from_path(join.static_data().root.clone())?;
        let joins = join.joins()?;
        if joins.items().is_empty() {
            Ok(From::new_from_cube(root, None))
        } else {
            let mut join_builder = JoinBuilder::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(alias_prefix)),
            );
            for join in joins.items().iter() {
                let definition = join.join()?;
                let evaluator = self
                    .compile_join_condition(&join.static_data().original_from, definition.sql()?)?;
                let on = JoinCondition::new_base_join(SqlJoinCondition::try_new(
                    self.query_tools.clone(),
                    evaluator,
                )?);
                let cube = self
                    .utils
                    .cube_from_path(join.static_data().original_to.clone())?;
                join_builder.left_join_cube(
                    cube.clone(),
                    Some(cube.default_alias_with_prefix(alias_prefix)),
                    on,
                );
            }
            let result = From::new_from_join(join_builder.build());
            Ok(result)
        }
    }

    pub fn make_join_node(&self) -> Result<From, CubeError> {
        self.make_join_node_with_prefix(&None)
    }

    fn compile_join_condition(
        &self,
        cube_name: &String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        evaluator_compiler.add_join_condition_evaluator(cube_name.clone(), sql)
    }
}
