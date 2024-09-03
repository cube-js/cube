use super::visitor::EvaluatorVisitor;
use super::EvaluationNode;
use super::{CubeNameEvaluator, DimensionEvaluator, MeasureEvaluator, MemberEvaluatorType};
use crate::cube_bridge::memeber_sql::MemberSqlArg;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::boxed::Box;
use std::rc::Rc;

pub trait ReplaceVisitorItem {
    fn replace_if_needed(&self, node: &Rc<EvaluationNode>) -> Result<Option<String>, CubeError>;
}

pub trait PostProcesVisitorItem {
    fn process(&self, node: &Rc<EvaluationNode>, result: String) -> Result<String, CubeError>;
}

#[derive(Clone)]
pub struct DefaultEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
    cube_alias_prefix: Option<String>,
    replace_visitors: Vec<Rc<dyn ReplaceVisitorItem>>,
    post_process_visitors: Vec<Rc<dyn PostProcesVisitorItem>>,
}

impl DefaultEvaluatorVisitor {
    pub fn new(
        query_tools: Rc<QueryTools>,
        cube_alias_prefix: Option<String>,
        replace_visitors: Vec<Rc<dyn ReplaceVisitorItem>>,
        post_process_visitors: Vec<Rc<dyn PostProcesVisitorItem>>,
    ) -> Self {
        Self {
            query_tools,
            cube_alias_prefix,
            replace_visitors,
            post_process_visitors,
        }
    }

    pub fn add_replace_visitor(&mut self, visitor: Rc<dyn ReplaceVisitorItem>) {
        self.replace_visitors.push(visitor);
    }

    pub fn add_post_process_visitor(&mut self, visitor: Rc<dyn PostProcesVisitorItem>) {
        self.post_process_visitors.push(visitor);
    }

    pub fn cube_alias_prefix(&self) -> &Option<String> {
        &self.cube_alias_prefix
    }
}

impl EvaluatorVisitor for DefaultEvaluatorVisitor {
    fn evaluate_sql(
        &mut self,
        node: &Rc<EvaluationNode>,
        args: Vec<MemberSqlArg>,
    ) -> Result<String, cubenativeutils::CubeError> {
        match node.evaluator() {
            MemberEvaluatorType::Dimension(ev) => {
                ev.default_evaluate_sql(&self, args, self.query_tools.clone())
            }
            MemberEvaluatorType::Measure(ev) => {
                ev.default_evaluate_sql(&self, args, self.query_tools.clone())
            }
            MemberEvaluatorType::CubeTable(ev) => {
                ev.default_evaluate_sql(args, self.query_tools.clone())
            }
            MemberEvaluatorType::CubeName(ev) => {
                ev.default_evaluate_sql(&self, self.query_tools.clone())
            }
            MemberEvaluatorType::JoinCondition(ev) => {
                ev.default_evaluate_sql(args, self.query_tools.clone())
            }
        }
    }

    fn apply(&mut self, node: &Rc<EvaluationNode>) -> Result<String, CubeError> {
        self.on_node_enter(node)?;
        for replacer in self.replace_visitors.iter_mut() {
            if let Some(replace) = replacer.replace_if_needed(node)? {
                return Ok(replace);
            }
        }
        let deps = self.evaluate_deps(node)?;
        let result = self.evaluate_sql(node, deps)?;
        self.post_process(node, result)
    }

    fn post_process(
        &mut self,
        node: &Rc<EvaluationNode>,
        result: String,
    ) -> Result<String, CubeError> {
        let mut result = result;
        for processor in self.post_process_visitors.iter_mut() {
            result = processor.process(node, result)?;
        }

        Ok(result)
    }
}

pub fn default_evaluate(
    node: &Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
) -> Result<String, CubeError> {
    let mut visitor = DefaultEvaluatorVisitor::new(query_tools.clone(), None, vec![], vec![]);
    visitor.apply(node)
}
