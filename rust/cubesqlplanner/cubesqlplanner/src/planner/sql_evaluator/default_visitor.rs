use super::visitor::EvaluatorVisitor;
use super::EvaluationNode;
use super::{CubeNameEvaluator, DimensionEvaluator, MeasureEvaluator, MemberEvaluatorType};
use crate::cube_bridge::memeber_sql::MemberSqlArg;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct DefaultEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
}

impl DefaultEvaluatorVisitor {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools }
    }
}

impl EvaluatorVisitor for DefaultEvaluatorVisitor {
    fn evaluate_sql(
        &mut self,
        node: &Rc<EvaluationNode>,
        args: Vec<MemberSqlArg>,
    ) -> Result<String, cubenativeutils::CubeError> {
        match node.evaluator() {
            MemberEvaluatorType::Dimension(ev) => {
                ev.default_evaluate_sql(args, self.query_tools.clone())
            }
            MemberEvaluatorType::Measure(ev) => {
                ev.default_evaluate_sql(args, self.query_tools.clone())
            }
            MemberEvaluatorType::CubeName(ev) => ev.default_evaluate_sql(self.query_tools.clone()),
        }
    }
}

pub fn default_evaluate(
    node: &Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
) -> Result<String, CubeError> {
    let mut visitor = DefaultEvaluatorVisitor::new(query_tools.clone());
    visitor.apply(node)
}
