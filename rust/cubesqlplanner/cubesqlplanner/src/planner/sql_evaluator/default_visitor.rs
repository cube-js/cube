use super::dependecy::ContextSymbolDep;
use super::visitor::EvaluatorVisitor;
use super::EvaluationNode;
use super::{CubeNameEvaluator, DimensionEvaluator, MeasureEvaluator, MemberEvaluatorType};
use crate::cube_bridge::memeber_sql::{ContextSymbolArg, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::boxed::Box;
use std::rc::Rc;

pub trait ReplaceNodeProcessorItem {
    fn replace_if_needed(&self, node: &Rc<EvaluationNode>) -> Result<Option<String>, CubeError>;
}

pub trait PostProcesNodeProcessorItem {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
        result: String,
    ) -> Result<String, CubeError>;
}

#[derive(Clone)]
pub struct DefaultEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
    cube_alias_prefix: Option<String>,
    replace_visitors: Vec<Rc<dyn ReplaceNodeProcessorItem>>,
    post_process_visitors: Vec<Rc<dyn PostProcesNodeProcessorItem>>,
}

impl DefaultEvaluatorVisitor {
    pub fn new(
        query_tools: Rc<QueryTools>,
        cube_alias_prefix: Option<String>,
        replace_visitors: Vec<Rc<dyn ReplaceNodeProcessorItem>>,
        post_process_visitors: Vec<Rc<dyn PostProcesNodeProcessorItem>>,
    ) -> Self {
        Self {
            query_tools,
            cube_alias_prefix,
            replace_visitors,
            post_process_visitors,
        }
    }

    pub fn add_replace_visitor(&mut self, visitor: Rc<dyn ReplaceNodeProcessorItem>) {
        self.replace_visitors.push(visitor);
    }

    pub fn add_post_process_visitor(&mut self, visitor: Rc<dyn PostProcesNodeProcessorItem>) {
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
            MemberEvaluatorType::Dimension(ev) => ev.evaluate_sql(args),
            MemberEvaluatorType::Measure(ev) => ev.evaluate_sql(args),
            MemberEvaluatorType::CubeTable(ev) => ev.evaluate_sql(args),
            MemberEvaluatorType::CubeName(ev) => ev.evaluate_sql(args),
            MemberEvaluatorType::JoinCondition(ev) => ev.evaluate_sql(args),
            MemberEvaluatorType::MeasureFilter(ev) => ev.evaluate_sql(args),
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

    fn apply_context_symbol(
        &mut self,
        context_symbol: &ContextSymbolDep,
    ) -> Result<MemberSqlArg, CubeError> {
        let res = match context_symbol {
            ContextSymbolDep::SecurityContext => {
                MemberSqlArg::ContextSymbol(ContextSymbolArg::SecurityContext(
                    self.query_tools.base_tools().security_context_for_rust()?,
                ))
            }
            ContextSymbolDep::FilterParams => MemberSqlArg::ContextSymbol(
                ContextSymbolArg::FilterParams(self.query_tools.base_tools().filters_proxy()?),
            ),
            ContextSymbolDep::FilterGroup => {
                MemberSqlArg::ContextSymbol(ContextSymbolArg::FilterGroup(
                    self.query_tools.base_tools().filter_group_function()?,
                ))
            }
        };
        Ok(res)
    }

    fn post_process(
        &mut self,
        node: &Rc<EvaluationNode>,
        result: String,
    ) -> Result<String, CubeError> {
        let mut result = result;
        let processors = self.post_process_visitors.clone();
        for processor in processors.into_iter() {
            result = processor.process(self, node, self.query_tools.clone(), result)?;
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
