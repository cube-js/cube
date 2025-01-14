use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{MemberSymbol, SqlEvaluatorVisitor};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub trait SqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError>;

    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;

    fn childs(&self) -> Vec<Rc<dyn SqlNode>>;
}

pub trait CubeNameNode {
    fn to_sql(&self, cube_name: &String) -> Result<String, CubeError>;
}
