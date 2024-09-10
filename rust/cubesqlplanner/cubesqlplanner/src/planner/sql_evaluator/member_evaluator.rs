use super::dependecy::Dependency;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
pub trait MemberEvaluator {
    fn evaluate(&self, tools: Rc<QueryTools>) -> Result<String, CubeError>;
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
}

pub trait MemberEvaluatorFactory: Sized {
    type Result;
    fn try_new(full_name: String, cube_evaluator: Rc<dyn CubeEvaluator>)
        -> Result<Self, CubeError>;
    fn cube_name(&self) -> &String;
    fn deps_names(&self) -> Result<Vec<String>, CubeError>;
    fn member_sql(&self) -> Option<Rc<dyn MemberSql>>;
    fn build(self, deps: Vec<Dependency>) -> Result<Rc<Self::Result>, CubeError>;
}
