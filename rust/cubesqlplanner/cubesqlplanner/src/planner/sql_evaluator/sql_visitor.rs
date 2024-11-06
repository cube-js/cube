use super::dependecy::ContextSymbolDep;
use super::sql_nodes::SqlNode;
use super::visitor::EvaluatorVisitor;
use super::EvaluationNode;
use crate::cube_bridge::memeber_sql::{ContextSymbolArg, MemberSqlArg};
use crate::plan::Schema;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct SqlEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
    cube_alias_prefix: Option<String>,
    node_processor: Rc<dyn SqlNode>,
    source_schema: Rc<Schema>,
}

impl SqlEvaluatorVisitor {
    pub fn new(
        query_tools: Rc<QueryTools>,
        cube_alias_prefix: Option<String>,
        node_processor: Rc<dyn SqlNode>,
        source_schema: Rc<Schema>,
    ) -> Self {
        Self {
            query_tools,
            cube_alias_prefix,
            node_processor,
            source_schema,
        }
    }

    pub fn cube_alias_prefix(&self) -> &Option<String> {
        &self.cube_alias_prefix
    }

    pub fn source_schema(&self) -> &Rc<Schema> {
        &self.source_schema
    }
}

impl EvaluatorVisitor for SqlEvaluatorVisitor {
    fn apply(&mut self, node: &Rc<EvaluationNode>) -> Result<String, CubeError> {
        self.on_node_enter(node)?;
        let node_processor = self.node_processor.clone();
        let result = node_processor.to_sql(self, node, self.query_tools.clone())?;
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
