use super::dependecy::ContextSymbolDep;
use super::visitor::EvaluatorVisitor;
use super::EvaluationNode;
use crate::cube_bridge::memeber_sql::{ContextSymbolArg, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait NodeProcessorItem {
    fn process(
        &self,
        visitor: &mut DefaultEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError>;
}

#[derive(Clone)]
pub struct DefaultEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
    cube_alias_prefix: Option<String>,
    node_processor: Rc<dyn NodeProcessorItem>,
}

impl DefaultEvaluatorVisitor {
    pub fn new(
        query_tools: Rc<QueryTools>,
        cube_alias_prefix: Option<String>,
        node_processor: Rc<dyn NodeProcessorItem>,
    ) -> Self {
        Self {
            query_tools,
            cube_alias_prefix,
            node_processor,
        }
    }

    pub fn cube_alias_prefix(&self) -> &Option<String> {
        &self.cube_alias_prefix
    }
}

impl EvaluatorVisitor for DefaultEvaluatorVisitor {
    fn apply(&mut self, node: &Rc<EvaluationNode>) -> Result<String, CubeError> {
        self.on_node_enter(node)?;
        let processor = self.node_processor.clone();
        let result = processor.process(self, node, self.query_tools.clone())?;
        Ok(result)
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
}
