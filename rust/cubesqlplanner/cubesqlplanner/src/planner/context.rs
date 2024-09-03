use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use crate::planner::sql_evaluator::default_visitor::{
    DefaultEvaluatorVisitor, PostProcesVisitorItem, ReplaceVisitorItem,
};
use crate::planner::sql_evaluator::visitor::EvaluatorVisitor;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct Context {
    cube_alias_prefix: Option<String>,
    replace_visitors: Vec<Rc<dyn ReplaceVisitorItem>>,
    post_process_visitors: Vec<Rc<dyn PostProcesVisitorItem>>,
}

impl Context {
    pub fn new(
        cube_alias_prefix: Option<String>,
        replace_visitors: Vec<Rc<dyn ReplaceVisitorItem>>,
        post_process_visitors: Vec<Rc<dyn PostProcesVisitorItem>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            cube_alias_prefix,
            replace_visitors,
            post_process_visitors,
        })
    }

    pub fn new_with_cube_alias_prefix(cube_alias_prefix: String) -> Rc<Self> {
        Self::new(Some(cube_alias_prefix), vec![], vec![])
    }

    pub fn default() -> Rc<Self> {
        Rc::new(Self {
            cube_alias_prefix: Default::default(),
            replace_visitors: Default::default(),
            post_process_visitors: Default::default(),
        })
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
