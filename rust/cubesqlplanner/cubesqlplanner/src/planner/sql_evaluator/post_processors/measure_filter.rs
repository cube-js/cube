use crate::cube_bridge::memeber_sql::{ContextSymbolArg, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::default_visitor::DefaultEvaluatorVisitor;
use crate::planner::sql_evaluator::default_visitor::PostProcesNodeProcessorItem;
use crate::planner::sql_evaluator::dependecy::ContextSymbolDep;
use crate::planner::sql_evaluator::visitor::EvaluatorVisitor;
use crate::planner::sql_evaluator::{
    CubeNameEvaluator, DimensionEvaluator, EvaluationNode, MeasureEvaluator, MemberEvaluator,
    MemberEvaluatorType,
};
use cubenativeutils::CubeError;
use std::boxed::Box;
use std::rc::Rc;

pub struct MeasureFilterNodeProcessor {
    measure_processor: Option<Rc<dyn PostProcesNodeProcessorItem>>,
}

impl MeasureFilterNodeProcessor {
    pub fn new(measure_processor: Option<Rc<dyn PostProcesNodeProcessorItem>>) -> Rc<Self> {
        Rc::new(Self { measure_processor })
    }
}

impl PostProcesNodeProcessorItem for MeasureFilterNodeProcessor {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
        result: String,
    ) -> Result<String, CubeError> {
        let prefix = visitor.cube_alias_prefix();
        let res = match node.evaluator() {
            MemberEvaluatorType::Measure(ev) => {
                let measure_filters = ev.measure_filters();
                let result = if !measure_filters.is_empty() {
                    let filters = measure_filters
                        .iter()
                        .map(|filter| -> Result<String, CubeError> {
                            Ok(format!("({})", visitor.apply(filter)?))
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" AND ");
                    //return `CASE WHEN ${where} THEN ${evaluateSql === '*' ? '1' : evaluateSql} END`;
                    let result = if result.as_str() == "*" {
                        "1".to_string()
                    } else {
                        result
                    };
                    format!("CASE WHEN {} THEN {} END", filters, result)
                } else {
                    result
                };
                if let Some(measure_processor) = &self.measure_processor {
                    measure_processor.process(visitor, node, query_tools, result)?
                } else {
                    result
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
