use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::default_visitor::DefaultEvaluatorVisitor;
use crate::planner::sql_evaluator::default_visitor::NodeProcessorItem;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct RenderReferencesNodeProcessor {
    references: HashMap<String, String>,
    input: Rc<dyn NodeProcessorItem>,
}

impl RenderReferencesNodeProcessor {
    pub fn new(references: HashMap<String, String>, input: Rc<dyn NodeProcessorItem>) -> Rc<Self> {
        Rc::new(Self { references, input })
    }
}

impl NodeProcessorItem for RenderReferencesNodeProcessor {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let reference = match node.symbol() {
            MemberSymbolType::Dimension(ev) => self.references.get(&ev.full_name()).cloned(),
            MemberSymbolType::Measure(ev) => self.references.get(&ev.full_name()).cloned(),
            _ => None,
        };

        if let Some(reference) = reference {
            Ok(reference)
        } else {
            self.input.process(visitor, node, query_tools.clone())
        }
    }
}
