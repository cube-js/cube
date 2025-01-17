use super::dependecy::{ContextSymbolDep, CubeDepProperty, CubeDependency, Dependency};
use super::sql_nodes::SqlNode;
use super::{symbols::MemberSymbol, SqlEvaluatorVisitor};
use crate::cube_bridge::memeber_sql::{ContextSymbolArg, MemberSql, MemberSqlArg, MemberSqlStruct};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct SqlCall {
    member_sql: Rc<dyn MemberSql>,
    deps: Vec<Dependency>,
}

impl SqlCall {
    pub fn new(member_sql: Rc<dyn MemberSql>, deps: Vec<Dependency>) -> Self {
        Self { member_sql, deps }
    }

    pub fn eval(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let args = self
            .deps
            .iter()
            .map(|d| {
                self.evaluate_single_dep(&d, visitor, node_processor.clone(), query_tools.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.member_sql.call(args)
    }

    pub fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        for dep in self.deps.iter() {
            match dep {
                Dependency::SymbolDependency(dep) => result.push(dep.clone()),
                Dependency::CubeDependency(cube_dep) => {
                    self.extract_symbol_deps_from_cube_dep(cube_dep, result)
                }
                Dependency::ContextDependency(_) => {}
            }
        }
    }

    pub fn extract_cube_deps(&self, result: &mut Vec<String>) {
        for dep in self.deps.iter() {
            match dep {
                Dependency::SymbolDependency(_) => {}
                Dependency::CubeDependency(cube_dep) => {
                    self.extract_cube_deps_from_cube_dep(cube_dep, result)
                }
                Dependency::ContextDependency(_) => {}
            }
        }
    }

    fn extract_symbol_deps_from_cube_dep(
        &self,
        cube_dep: &CubeDependency,
        result: &mut Vec<Rc<MemberSymbol>>,
    ) {
        for (_, v) in cube_dep.properties.iter() {
            match v {
                CubeDepProperty::SymbolDependency(dep) => result.push(dep.clone()),
                CubeDepProperty::CubeDependency(cube_dep) => {
                    self.extract_symbol_deps_from_cube_dep(cube_dep, result)
                }
            };
        }
    }

    fn extract_cube_deps_from_cube_dep(&self, cube_dep: &CubeDependency, result: &mut Vec<String>) {
        result.push(cube_dep.cube_symbol.name());

        for (_, v) in cube_dep.properties.iter() {
            match v {
                CubeDepProperty::CubeDependency(cube_dep) => {
                    self.extract_cube_deps_from_cube_dep(cube_dep, result)
                }
                _ => {}
            };
        }
    }

    fn evaluate_single_dep(
        &self,
        dep: &Dependency,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<MemberSqlArg, CubeError> {
        match dep {
            Dependency::SymbolDependency(dep) => Ok(MemberSqlArg::String(
                visitor.apply(dep, node_processor.clone())?,
            )),
            Dependency::CubeDependency(dep) => {
                self.evaluate_cube_dep(dep, visitor, node_processor.clone(), query_tools.clone())
            }
            Dependency::ContextDependency(contex_symbol) => {
                self.apply_context_symbol(contex_symbol, query_tools.clone())
            }
        }
    }

    fn evaluate_cube_dep(
        &self,
        dep: &CubeDependency,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<MemberSqlArg, CubeError> {
        let mut res = MemberSqlStruct::default();
        if let Some(sql_fn) = &dep.sql_fn {
            res.sql_fn = Some(visitor.apply(sql_fn, node_processor.clone())?);
        }
        if let Some(to_string_fn) = &dep.to_string_fn {
            res.to_string_fn = Some(visitor.apply(to_string_fn, node_processor.clone())?);
        }
        for (k, v) in dep.properties.iter() {
            let prop_res = match v {
                CubeDepProperty::SymbolDependency(dep) => {
                    MemberSqlArg::String(visitor.apply(&dep, node_processor.clone())?)
                }
                CubeDepProperty::CubeDependency(dep) => self.evaluate_cube_dep(
                    &dep,
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                )?,
            };
            res.properties.insert(k.clone(), prop_res);
        }
        Ok(MemberSqlArg::Struct(res))
    }

    pub fn apply_context_symbol(
        &self,
        context_symbol: &ContextSymbolDep,
        query_tools: Rc<QueryTools>,
    ) -> Result<MemberSqlArg, CubeError> {
        let res = match context_symbol {
            ContextSymbolDep::SecurityContext => {
                MemberSqlArg::ContextSymbol(ContextSymbolArg::SecurityContext(
                    query_tools.base_tools().security_context_for_rust()?,
                ))
            }
            ContextSymbolDep::FilterParams => MemberSqlArg::ContextSymbol(
                ContextSymbolArg::FilterParams(query_tools.base_tools().filters_proxy()?),
            ),
            ContextSymbolDep::FilterGroup => MemberSqlArg::ContextSymbol(
                ContextSymbolArg::FilterGroup(query_tools.base_tools().filter_group_function()?),
            ),
        };
        Ok(res)
    }
}
