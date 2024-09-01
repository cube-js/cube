use super::query_tools::QueryTools;
use super::sql_evaluator::{default_evaluate, EvaluationNode, MemberEvaluator};
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use cubenativeutils::CubeError;
use std::rc::Rc;
pub struct BaseJoinCondition {
    cube_name: String,
    member_evaluator: Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
}
impl BaseJoinCondition {
    pub fn try_new(
        cube_name: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            cube_name,
            member_evaluator,
            query_tools,
        }))
    }

    pub fn to_sql(&self) -> Result<String, CubeError> {
        default_evaluate(&self.member_evaluator, self.query_tools.clone())
    }
}
