use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::default_visitor::DefaultEvaluatorVisitor;
use crate::planner::sql_evaluator::default_visitor::NodeProcessorItem;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct FinalMeasureNodeProcessor {
    input: Rc<dyn NodeProcessorItem>,
}

impl FinalMeasureNodeProcessor {
    pub fn new(input: Rc<dyn NodeProcessorItem>) -> Rc<Self> {
        Rc::new(Self { input })
    }
}

impl NodeProcessorItem for FinalMeasureNodeProcessor {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Measure(ev) => {
                let input = self.input.process(visitor, node, query_tools.clone())?;

                if ev.is_calculated() {
                    input
                } else {
                    let measure_type = ev.measure_type();
                    format!("{}({})", measure_type, input)
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Measure filter node processor called for wrong node",
                )));
            }
        };
        Ok(res)
    }
}
