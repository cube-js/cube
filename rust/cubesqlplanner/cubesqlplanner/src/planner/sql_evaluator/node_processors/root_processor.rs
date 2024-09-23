use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::default_visitor::DefaultEvaluatorVisitor;
use crate::planner::sql_evaluator::default_visitor::NodeProcessorItem;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct RootNodeProcessor {
    dimension_processor: Rc<dyn NodeProcessorItem>,
    measure_processor: Rc<dyn NodeProcessorItem>,
    cube_name_processor: Rc<dyn NodeProcessorItem>,
    default_processor: Rc<dyn NodeProcessorItem>,
}

impl RootNodeProcessor {
    pub fn new(
        dimension_processor: Rc<dyn NodeProcessorItem>,
        measure_processor: Rc<dyn NodeProcessorItem>,
        cube_name_processor: Rc<dyn NodeProcessorItem>,
        default_processor: Rc<dyn NodeProcessorItem>,
    ) -> Rc<Self> {
        Rc::new(Self {
            dimension_processor,
            measure_processor,
            cube_name_processor,
            default_processor,
        })
    }
}

impl NodeProcessorItem for RootNodeProcessor {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Dimension(_) => {
                self.dimension_processor
                    .process(visitor, node, query_tools.clone())?
            }
            MemberSymbolType::Measure(_) => {
                self.measure_processor
                    .process(visitor, node, query_tools.clone())?
            }
            MemberSymbolType::CubeName(_) => {
                self.cube_name_processor
                    .process(visitor, node, query_tools.clone())?
            }
            _ => self
                .default_processor
                .process(visitor, node, query_tools.clone())?,
        };
        Ok(res)
    }
}
