use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
pub trait MemberEvaluator {
    fn eveluate(&self, tools: Rc<QueryTools>) -> Result<String, CubeError>;
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
}

pub trait MemberEvaluatorFactory: Sized {
    type Result;
    fn try_new(full_name: String, cube_evaluator: Rc<dyn CubeEvaluator>)
        -> Result<Self, CubeError>;
    fn cube_name(&self) -> &String;
    fn deps_names(&self) -> Result<Vec<String>, CubeError>;
    fn build(self, deps: Vec<Rc<dyn MemberEvaluator>>) -> Result<Rc<Self::Result>, CubeError>;
}
