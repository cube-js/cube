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

pub struct FinalMeasureNodeProcessor {}

impl FinalMeasureNodeProcessor {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {})
    }
}

impl PostProcesNodeProcessorItem for FinalMeasureNodeProcessor {
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
                if ev.is_calculated() {
                    result
                } else {
                    let measure_type = ev.measure_type();
                    format!("{}({})", measure_type, result)
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
