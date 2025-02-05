use super::dependecy::{ContextSymbolDep, CubeDepProperty, CubeDependency, Dependency};
use super::sql_nodes::SqlNode;
use super::{symbols::MemberSymbol, SqlEvaluatorVisitor};
use crate::cube_bridge::member_sql::{ContextSymbolArg, MemberSql, MemberSqlArg, MemberSqlStruct};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
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
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let args = self
            .deps
            .iter()
            .map(|d| {
                self.evaluate_single_dep(
                    &d,
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                    templates,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.member_sql.call(args)
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = Vec::new();
        self.extract_symbol_deps(&mut deps);
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = Vec::new();
        self.extract_symbol_deps_with_path(&mut deps);
        deps
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

    pub fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        for dep in self.deps.iter() {
            match dep {
                Dependency::SymbolDependency(dep) => result.push((dep.clone(), vec![])),
                Dependency::CubeDependency(cube_dep) => {
                    self.extract_symbol_deps_with_path_from_cube_dep(cube_dep, vec![], result)
                }
                Dependency::ContextDependency(_) => {}
            }
        }
    }

    pub fn get_dependent_cubes(&self) -> Vec<String> {
        let mut deps = Vec::new();
        self.extract_cube_deps(&mut deps);
        deps
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

    fn extract_symbol_deps_with_path_from_cube_dep(
        &self,
        cube_dep: &CubeDependency,
        mut path: Vec<String>,
        result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>,
    ) {
        path.push(cube_dep.cube_symbol.cube_name());
        if let Some(sql_fn) = &cube_dep.sql_fn {
            result.push((sql_fn.clone(), path.clone()));
        }
        if let Some(to_string_fn) = &cube_dep.to_string_fn {
            result.push((to_string_fn.clone(), path.clone()));
        }
        for (_, v) in cube_dep.properties.iter() {
            match v {
                CubeDepProperty::SymbolDependency(dep) => result.push((dep.clone(), path.clone())),
                CubeDepProperty::CubeDependency(cube_dep) => {
                    self.extract_symbol_deps_with_path_from_cube_dep(cube_dep, path.clone(), result)
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
        templates: &PlanSqlTemplates,
    ) -> Result<MemberSqlArg, CubeError> {
        match dep {
            Dependency::SymbolDependency(dep) => Ok(MemberSqlArg::String(visitor.apply(
                dep,
                node_processor.clone(),
                templates,
            )?)),
            Dependency::CubeDependency(dep) => self.evaluate_cube_dep(
                dep,
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            ),
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
        templates: &PlanSqlTemplates,
    ) -> Result<MemberSqlArg, CubeError> {
        let mut res = MemberSqlStruct::default();
        if let Some(sql_fn) = &dep.sql_fn {
            res.sql_fn = Some(visitor.apply(sql_fn, node_processor.clone(), templates)?);
        }
        if let Some(to_string_fn) = &dep.to_string_fn {
            res.to_string_fn =
                Some(visitor.apply(to_string_fn, node_processor.clone(), templates)?);
        }
        for (k, v) in dep.properties.iter() {
            let prop_res = match v {
                CubeDepProperty::SymbolDependency(dep) => {
                    MemberSqlArg::String(visitor.apply(&dep, node_processor.clone(), templates)?)
                }
                CubeDepProperty::CubeDependency(dep) => self.evaluate_cube_dep(
                    &dep,
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                    templates,
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
            ContextSymbolDep::SqlUtils => MemberSqlArg::ContextSymbol(ContextSymbolArg::SqlUtils(
                query_tools.base_tools().sql_utils_for_rust()?,
            )),
        };
        Ok(res)
    }
}
