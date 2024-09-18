use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use crate::planner::sql_evaluator::default_visitor::{
    DefaultEvaluatorVisitor, PostProcesNodeProcessorItem, ReplaceNodeProcessorItem,
};
use crate::planner::sql_evaluator::post_processors::default_post_processor;
use crate::planner::sql_evaluator::visitor::EvaluatorVisitor;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct Context {
    cube_alias_prefix: Option<String>,
    replace_visitors: Vec<Rc<dyn ReplaceNodeProcessorItem>>,
    post_process_visitors: Vec<Rc<dyn PostProcesNodeProcessorItem>>,
}

impl Context {
    pub fn new(
        cube_alias_prefix: Option<String>,
        replace_visitors: Vec<Rc<dyn ReplaceNodeProcessorItem>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            cube_alias_prefix,
            replace_visitors,
            post_process_visitors: vec![default_post_processor()],
        })
    }

    pub fn new_with_cube_alias_prefix(cube_alias_prefix: String) -> Rc<Self> {
        Self::new(Some(cube_alias_prefix), vec![])
    }

    pub fn default() -> Rc<Self> {
        Self::new(Default::default(), Default::default())
    }

    pub fn make_visitor(&self, query_tools: Rc<QueryTools>) -> DefaultEvaluatorVisitor {
        DefaultEvaluatorVisitor::new(
            query_tools,
            self.cube_alias_prefix.clone(),
            self.replace_visitors.clone(),
            self.post_process_visitors.clone(),
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
