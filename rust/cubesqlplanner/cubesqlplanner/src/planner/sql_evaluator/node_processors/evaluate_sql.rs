use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::default_visitor::DefaultEvaluatorVisitor;
use crate::planner::sql_evaluator::default_visitor::NodeProcessorItem;
use crate::planner::sql_evaluator::visitor::EvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct EvaluateSqlProcessor {}

impl EvaluateSqlProcessor {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {})
    }
}

impl NodeProcessorItem for EvaluateSqlProcessor {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        _query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let args = visitor.evaluate_deps(node)?;
        match node.symbol() {
            MemberSymbolType::Dimension(ev) => ev.evaluate_sql(args),
            MemberSymbolType::Measure(ev) => ev.evaluate_sql(args),
            MemberSymbolType::CubeTable(ev) => ev.evaluate_sql(args),
            MemberSymbolType::CubeName(ev) => ev.evaluate_sql(args),
            MemberSymbolType::JoinCondition(ev) => ev.evaluate_sql(args),
            MemberSymbolType::MeasureFilter(ev) => ev.evaluate_sql(args),
        }
    }
}
