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

pub struct RootNodeProcessor {
    dimension_processor: Option<Rc<dyn PostProcesNodeProcessorItem>>,
    measure_processor: Option<Rc<dyn PostProcesNodeProcessorItem>>,
}

impl RootNodeProcessor {
    pub fn new(
        dimension_processor: Option<Rc<dyn PostProcesNodeProcessorItem>>,
        measure_processor: Option<Rc<dyn PostProcesNodeProcessorItem>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            dimension_processor,
            measure_processor,
        })
    }
}

impl PostProcesNodeProcessorItem for RootNodeProcessor {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
        result: String,
    ) -> Result<String, CubeError> {
        let prefix = visitor.cube_alias_prefix();
        let res = match node.evaluator() {
            MemberEvaluatorType::Dimension(ev) => {
                let result =
                    query_tools.auto_prefix_with_cube_name(&ev.cube_name(), &result, prefix);
                if let Some(dimension_processor) = &self.dimension_processor {
                    dimension_processor.process(visitor, node, query_tools, result)?
                } else {
                    result
                }
            }
            MemberEvaluatorType::Measure(ev) => {
                let result =
                    query_tools.auto_prefix_with_cube_name(&ev.cube_name(), &result, prefix);
                if let Some(measure_processor) = &self.measure_processor {
                    measure_processor.process(visitor, node, query_tools, result)?
                } else {
                    result
                }
            }
            MemberEvaluatorType::CubeTable(ev) => result,
            MemberEvaluatorType::CubeName(ev) => {
                query_tools.escape_column_name(&query_tools.cube_alias_name(&result, prefix))
            }
            MemberEvaluatorType::JoinCondition(ev) => result,
            MemberEvaluatorType::MeasureFilter(_) => result,
        };
        Ok(res)
    }
}
