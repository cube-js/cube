use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::visitor::EvaluatorVisitor;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
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
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        _query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let args = visitor.evaluate_deps(node)?;
        match node.symbol() {
            MemberSymbolType::Dimension(ev) => ev.evaluate_sql(args),
            MemberSymbolType::Measure(ev) => ev.evaluate_sql(args),
            MemberSymbolType::CubeTable(ev) => ev.evaluate_sql(args),
            MemberSymbolType::CubeName(ev) => ev.evaluate_sql(args),
            MemberSymbolType::SimpleSql(ev) => ev.evaluate_sql(args),
        }
    }
}
