use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{EvaluationNode, SqlEvaluatorVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait SqlNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError>;
}
