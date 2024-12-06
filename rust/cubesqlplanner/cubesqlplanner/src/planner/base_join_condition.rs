use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::{evaluate_with_context, VisitorContext};
use crate::plan::Schema;
use cubenativeutils::CubeError;
use std::rc::Rc;
pub trait BaseJoinCondition {
    fn to_sql(&self, context: Rc<VisitorContext>, schema: Rc<Schema>) -> Result<String, CubeError>;
}
pub struct SqlJoinCondition {
    member_evaluator: Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
}
impl SqlJoinCondition {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            member_evaluator,
            query_tools,
        }))
    }
}

impl BaseJoinCondition for SqlJoinCondition {
    fn to_sql(&self, context: Rc<VisitorContext>, schema: Rc<Schema>) -> Result<String, CubeError> {
        evaluate_with_context(
            &self.member_evaluator,
            self.query_tools.clone(),
            context,
            schema,
        )
    }
}
