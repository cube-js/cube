use super::query_tools::QueryTools;
use super::sql_evaluator::node_processors::default_node_processor;
use super::sql_evaluator::EvaluationNode;
use crate::planner::sql_evaluator::default_visitor::{DefaultEvaluatorVisitor, NodeProcessorItem};
use crate::planner::sql_evaluator::visitor::EvaluatorVisitor;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct Context {
    cube_alias_prefix: Option<String>,
    node_processor: Rc<dyn NodeProcessorItem>,
}

impl Context {
    pub fn new(
        cube_alias_prefix: Option<String>,
        node_processor: Rc<dyn NodeProcessorItem>,
    ) -> Rc<Self> {
        Rc::new(Self {
            cube_alias_prefix,
            node_processor,
        })
    }

    pub fn new_with_cube_alias_prefix(cube_alias_prefix: String) -> Rc<Self> {
        Self::new(Some(cube_alias_prefix), default_node_processor())
    }

    pub fn default() -> Rc<Self> {
        Self::new(Default::default(), default_node_processor())
    }

    pub fn make_visitor(&self, query_tools: Rc<QueryTools>) -> DefaultEvaluatorVisitor {
        DefaultEvaluatorVisitor::new(
            query_tools,
            self.cube_alias_prefix.clone(),
            self.node_processor.clone(),
        )
    }

    pub fn cube_alias_prefix(&self) -> &Option<String> {
        &self.cube_alias_prefix
    }
}

pub fn evaluate_with_context(
    node: &Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
    context: Rc<Context>,
) -> Result<String, CubeError> {
    let mut visitor = context.make_visitor(query_tools);
    visitor.apply(node)
}
