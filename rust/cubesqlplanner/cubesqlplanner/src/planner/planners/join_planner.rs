use super::CommonUtils;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::plan::{From, FromSource, Join, JoinItem, JoinSource};
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

    pub fn make_join_node(&self, /*TODO dimensions for subqueries*/) -> Result<From, CubeError> {
        let join = self.query_tools.cached_data().join()?.clone();
        let root = self.utils.cube_from_path(join.static_data().root.clone())?;
        let joins = join.joins()?;
        if joins.items().is_empty() {
            Ok(From::new_from_cube(root))
        } else {
            let join_items = joins
                .items()
                .iter()
                .map(|join| {
                    let definition = join.join()?;
                    let evaluator = self.compile_join_condition(
                        &join.static_data().original_from,
                        definition.sql()?,
                    )?;
                    Ok(JoinItem {
                        from: JoinSource::new_from_cube(
                            self.utils
                                .cube_from_path(join.static_data().original_to.clone())?,
                        ),
                        on: SqlJoinCondition::try_new(self.query_tools.clone(), evaluator)?,
                        is_inner: false,
                    })
                })
                .collect::<Result<Vec<_>, CubeError>>()?;
            let result = From::new(FromSource::Join(Rc::new(Join {
                root: JoinSource::new_from_cube(root),
                joins: join_items,
            })));
            Ok(result)
        }
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
