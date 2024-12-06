use super::dependecy::{ContextSymbolDep, Dependency};
use super::sql_nodes::SqlNode;
use super::EvaluationNode;
use crate::cube_bridge::memeber_sql::{ContextSymbolArg, MemberSqlArg, MemberSqlStruct};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct SqlEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
}

impl SqlEvaluatorVisitor {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools }
    }

    pub fn apply(
        &mut self,
        node: &Rc<EvaluationNode>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let result =
            node_processor.to_sql(self, node, self.query_tools.clone(), node_processor.clone())?;
        Ok(result)
    }

    pub fn apply_context_symbol(
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

    pub fn evaluate_deps(
        &mut self,
        node: &Rc<EvaluationNode>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<Vec<MemberSqlArg>, CubeError> {
        node.deps()
            .iter()
            .map(|d| self.evaluate_single_dep(&d, node_processor.clone()))
            .collect()
    }

    fn evaluate_single_dep(
        &mut self,
        dep: &Dependency,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<MemberSqlArg, CubeError> {
        match dep {
            Dependency::SingleDependency(dep) => Ok(MemberSqlArg::String(
                self.apply(dep, node_processor.clone())?,
            )),
            Dependency::StructDependency(dep) => {
                let mut res = MemberSqlStruct::default();
                if let Some(sql_fn) = &dep.sql_fn {
                    res.sql_fn = Some(self.apply(sql_fn, node_processor.clone())?);
                }
                if let Some(to_string_fn) = &dep.to_string_fn {
                    res.to_string_fn = Some(self.apply(to_string_fn, node_processor.clone())?);
                }
                for (k, v) in dep.properties.iter() {
                    match v {
                        Dependency::SingleDependency(dep) => {
                            res.properties
                                .insert(k.clone(), self.apply(dep, node_processor.clone())?);
                        }
                        Dependency::StructDependency(_) => unimplemented!(),
                        Dependency::ContextDependency(_) => unimplemented!(),
                    }
                }
                Ok(MemberSqlArg::Struct(res))
            }
            Dependency::ContextDependency(contex_symbol) => {
                self.apply_context_symbol(contex_symbol)
            }
        }
    }
}
