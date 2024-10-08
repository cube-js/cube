use super::sql_evaluator::EvaluationNode;
use super::VisitorContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait BaseMember {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError>;
    fn alias_name(&self) -> Result<String, CubeError>;
    fn member_evaluator(&self) -> Rc<EvaluationNode>;
}
