use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct EvaluateSqlNode {}

impl EvaluateSqlNode {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {})
    }
}

impl SqlNode for EvaluateSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                ev.evaluate_sql(visitor, node_processor.clone(), query_tools.clone())
            }
            MemberSymbol::Measure(ev) => {
                ev.evaluate_sql(visitor, node_processor.clone(), query_tools.clone())
            }
            MemberSymbol::CubeTable(ev) => {
                ev.evaluate_sql(visitor, node_processor.clone(), query_tools.clone())
            }
            MemberSymbol::CubeName(ev) => ev.evaluate_sql(),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
