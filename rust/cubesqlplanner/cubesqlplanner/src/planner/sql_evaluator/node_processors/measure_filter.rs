use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::default_visitor::DefaultEvaluatorVisitor;
use crate::planner::sql_evaluator::default_visitor::NodeProcessorItem;
use crate::planner::sql_evaluator::visitor::EvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MeasureFilterNodeProcessor {
    input: Rc<dyn NodeProcessorItem>,
}

impl MeasureFilterNodeProcessor {
    pub fn new(input: Rc<dyn NodeProcessorItem>) -> Rc<Self> {
        Rc::new(Self { input })
    }
}

impl NodeProcessorItem for MeasureFilterNodeProcessor {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let input = self.input.process(visitor, node, query_tools.clone())?;
        let res = match node.symbol() {
            MemberSymbolType::Measure(ev) => {
                let measure_filters = ev.measure_filters();
                if !measure_filters.is_empty() {
                    let filters = measure_filters
                        .iter()
                        .map(|filter| -> Result<String, CubeError> {
                            Ok(format!("({})", visitor.apply(filter)?))
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" AND ");
                    //return `CASE WHEN ${where} THEN ${evaluateSql === '*' ? '1' : evaluateSql} END`;
                    let result = if input.as_str() == "*" {
                        "1".to_string()
                    } else {
                        input
                    };
                    format!("CASE WHEN {} THEN {} END", filters, result)
                } else {
                    input
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
