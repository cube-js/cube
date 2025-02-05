use super::symbols::MemberSymbol;
use super::Compiler;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::{CallDep, CubeEvaluator};
use crate::cube_bridge::member_sql::{ContextSymbolArg, MemberSql, MemberSqlArgForResolve};
use crate::cube_bridge::proxy::{
    CubeDepsCollector, CubeDepsCollectorProp, CubeDepsCollectorProxyHandler,
};
use cubenativeutils::wrappers::NativeContextHolderRef;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone)]
pub enum CubeDepProperty {
    CubeDependency(CubeDependency),
    SymbolDependency(Rc<MemberSymbol>),
}

#[derive(Clone)]
pub struct CubeDependency {
    pub cube_symbol: Rc<MemberSymbol>,
    pub sql_fn: Option<Rc<MemberSymbol>>,
    pub to_string_fn: Option<Rc<MemberSymbol>>,
    pub properties: HashMap<String, CubeDepProperty>,
}

impl CubeDependency {
    pub fn new(
        cube_symbol: Rc<MemberSymbol>,
        sql_fn: Option<Rc<MemberSymbol>>,
        to_string_fn: Option<Rc<MemberSymbol>>,
        properties: HashMap<String, CubeDepProperty>,
    ) -> Self {
        CubeDependency {
            cube_symbol,
            sql_fn,
            to_string_fn,
            properties,
        }
    }
}

#[derive(Clone)]
pub enum ContextSymbolDep {
    SecurityContext,
    FilterParams,
    FilterGroup,
    SqlUtils,
}

#[derive(Clone)]
pub enum Dependency {
    SymbolDependency(Rc<MemberSymbol>),
    CubeDependency(CubeDependency),
    ContextDependency(ContextSymbolDep),
}

pub struct DependenciesBuilder<'a> {
    compiler: &'a mut Compiler,
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    context_holder_ref: Rc<dyn NativeContextHolderRef>,
}

impl<'a> DependenciesBuilder<'a> {
    pub fn new(
        compiler: &'a mut Compiler,
        cube_evaluator: Rc<dyn CubeEvaluator>,
        base_tools: Rc<dyn BaseTools>,
        context_holder_ref: Rc<dyn NativeContextHolderRef>,
    ) -> Self {
        DependenciesBuilder {
            compiler,
            cube_evaluator,
            base_tools,
            context_holder_ref,
        }
    }

    pub fn build(
        mut self,
        cube_name: String,
        member_sql: Rc<dyn MemberSql>,
    ) -> Result<Vec<Dependency>, CubeError> {
        let call_deps = if member_sql.need_deps_resolve() {
            self.cube_evaluator
                .resolve_symbols_call_deps(cube_name.clone(), member_sql.clone())?
        } else {
            vec![]
        };

        let arg_names = member_sql.args_names();
        let mut deps_to_resolve = Vec::new();
        for arg_name in arg_names {
            if let Some(context_arg) = self.build_context_resolve_arg_tmp(arg_name)? {
                deps_to_resolve.push(MemberSqlArgForResolve::ContextSymbol(context_arg));
            } else if self
                .cube_evaluator
                .is_name_of_symbol_in_cube(cube_name.clone(), arg_name.clone())?
            {
                deps_to_resolve.push(MemberSqlArgForResolve::String("".to_string()));
            } else if self.is_current_cube(&arg_name)
                || self.cube_evaluator.is_name_of_cube(arg_name.clone())?
            {
                let new_cube_name = if self.is_current_cube(&arg_name) {
                    cube_name.clone()
                } else {
                    arg_name.clone()
                };
                let collector = CubeDepsCollector::try_new(
                    new_cube_name,
                    self.cube_evaluator.clone(),
                    self.base_tools.clone(),
                    self.context_holder_ref.clone(),
                )?;
                let proxy = CubeDepsCollectorProxyHandler::new(collector, self.base_tools.clone());
                deps_to_resolve.push(MemberSqlArgForResolve::CubeProxy(proxy));
            } else {
                return Err(CubeError::internal(format!(
                    "Undefinded dependency {}",
                    arg_name
                )));
            }
        }
        member_sql.deps_resolve(deps_to_resolve.clone())?;

        let mut result = Vec::new();

        for (dep, arg_name) in deps_to_resolve.into_iter().zip(arg_names.iter()) {
            match dep {
                MemberSqlArgForResolve::String(_) => {
                    result.push(Dependency::SymbolDependency(
                        self.build_evaluator(&cube_name, &arg_name)?,
                    ));
                }
                MemberSqlArgForResolve::CubeProxy(proxy_handler) => {
                    result.push(Dependency::CubeDependency(
                        self.build_cube_dependency(proxy_handler.clone())?,
                    ));
                }
                MemberSqlArgForResolve::ContextSymbol(context_symbol_arg) => {
                    let dep = match context_symbol_arg {
                        ContextSymbolArg::SecurityContext(_rc) => {
                            Dependency::ContextDependency(ContextSymbolDep::SecurityContext)
                        }
                        ContextSymbolArg::SqlUtils(_rc) => {
                            Dependency::ContextDependency(ContextSymbolDep::SqlUtils)
                        }
                        ContextSymbolArg::FilterParams(_rc) => {
                            Dependency::ContextDependency(ContextSymbolDep::FilterParams)
                        }
                        ContextSymbolArg::FilterGroup(_rc) => {
                            Dependency::ContextDependency(ContextSymbolDep::FilterGroup)
                        }
                    };
                    result.push(dep);
                }
            };
        }

        /* let mut childs = Vec::new();
        for (i, dep) in call_deps.iter().enumerate() {
            childs.push(vec![]);
            if let Some(parent) = dep.parent {
                childs[parent].push(i);
            }
        }

        let mut result = Vec::new();

        for (i, dep) in call_deps.iter().enumerate() {
            if dep.parent.is_some() {
                continue;
            }
            if let Some(context_dep) = self.build_context_dep(&dep.name) {
                result.push(context_dep);
                continue;
            }
            if childs[i].is_empty() {
                result.push(Dependency::SymbolDependency(
                    self.build_evaluator(&cube_name, &dep.name)?,
                ));
            } else {
                let dep = self.build_cube_dependency(&cube_name, i, &call_deps, &childs)?;
                result.push(Dependency::CubeDependency(dep));
            }
        } */

        Ok(result)
    }

    fn build_cube_dependency(
        &mut self,
        proxy_handler: Rc<CubeDepsCollectorProxyHandler>,
    ) -> Result<CubeDependency, CubeError> {
        let collector = proxy_handler.get_collector();
        let cube_symbol = self
            .compiler
            .add_cube_table_evaluator(collector.cube_name().clone())?;
        let sql_fn = if collector.has_sql_fn() {
            Some(
                self.compiler
                    .add_cube_table_evaluator(collector.cube_name().clone())?,
            )
        } else {
            None
        };

        let to_string_fn = if collector.has_to_string_fn() {
            Some(
                self.compiler
                    .add_cube_name_evaluator(collector.cube_name().clone())?,
            )
        } else {
            None
        };

        let mut properties = HashMap::new();
        for dep in collector.deps().iter() {
            match dep {
                CubeDepsCollectorProp::Symbol(name) => {
                    let prop = CubeDepProperty::SymbolDependency(
                        self.build_evaluator(collector.cube_name(), name)?,
                    );
                    properties.insert(name.clone(), prop);
                }
                CubeDepsCollectorProp::Cube(rc) => {
                    let prop =
                        CubeDepProperty::CubeDependency(self.build_cube_dependency(rc.clone())?);
                    let prop_name = rc.get_collector().cube_name().clone();
                    properties.insert(prop_name, prop);
                }
            };
        }
        /* let dep = &call_deps[dep_index];
        let new_cube_name = if self.is_current_cube(&dep.name) {
            cube_name.clone()
        } else {
            dep.name.clone()
        };
        let mut sql_fn = None;
        let mut to_string_fn: Option<Rc<MemberSymbol>> = None;
        let mut properties = HashMap::new();
        let cube_symbol = self
            .compiler
            .add_cube_table_evaluator(new_cube_name.clone())?;
        for child_ind in call_childs[dep_index].iter() {
            let name = &call_deps[*child_ind].name;
            if name.as_str() == "sql" {
                sql_fn = Some(
                    self.compiler
                        .add_cube_table_evaluator(new_cube_name.clone())?,
                );
            } else if name.as_str() == "toString" {
                to_string_fn = Some(
                    self.compiler
                        .add_cube_name_evaluator(new_cube_name.clone())?,
                );
            } else {
                let child_dep = if call_childs[*child_ind].is_empty() {
                    CubeDepProperty::SymbolDependency(self.build_evaluator(&new_cube_name, &name)?)
                } else {
                    CubeDepProperty::CubeDependency(self.build_cube_dependency(
                        &new_cube_name,
                        *child_ind,
                        call_deps,
                        call_childs,
                    )?)
                };
                properties.insert(name.clone(), child_dep);
            }
        } */
        Ok(CubeDependency::new(
            cube_symbol,
            sql_fn,
            to_string_fn,
            properties,
        ))
    }

    fn build_context_dep(&self, name: &str) -> Option<Dependency> {
        match name {
            "USER_CONTEXT" | "SECURITY_CONTEXT" => Some(Dependency::ContextDependency(
                ContextSymbolDep::SecurityContext,
            )),
            "FILTER_PARAMS" => Some(Dependency::ContextDependency(
                ContextSymbolDep::FilterParams,
            )),
            "FILTER_GROUP" => Some(Dependency::ContextDependency(ContextSymbolDep::FilterGroup)),
            "SQL_UTILS" => Some(Dependency::ContextDependency(ContextSymbolDep::SqlUtils)),
            _ => None,
        }
    }

    fn build_context_resolve_arg_tmp(
        &self,
        name: &str,
    ) -> Result<Option<ContextSymbolArg>, CubeError> {
        let res = match name {
            "USER_CONTEXT" | "SECURITY_CONTEXT" => Some(ContextSymbolArg::SecurityContext(
                self.base_tools.security_context_for_rust()?,
            )),
            "FILTER_PARAMS" => Some(ContextSymbolArg::FilterParams(
                self.base_tools.filters_proxy()?,
            )),
            "FILTER_GROUP" => Some(ContextSymbolArg::FilterGroup(
                self.base_tools.filter_group_function()?,
            )),
            "SQL_UTILS" => Some(ContextSymbolArg::SqlUtils(
                self.base_tools.sql_utils_for_rust()?,
            )),
            _ => None,
        };
        Ok(res)
    }

    //FIXME may be should be moved to BaseTools
    fn is_current_cube(&self, name: &str) -> bool {
        match name {
            "CUBE" | "TABLE" => true,
            _ => false,
        }
    }

    fn build_evaluator(
        &mut self,
        cube_name: &String,
        name: &String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let dep_full_name = format!("{}.{}", cube_name, name);
        //FIXME avoid cloning
        let dep_path = vec![cube_name.clone(), name.clone()];
        if self.cube_evaluator.is_measure(dep_path.clone())? {
            Ok(self.compiler.add_measure_evaluator(dep_full_name)?)
        } else if self.cube_evaluator.is_dimension(dep_path.clone())? {
            Ok(self.compiler.add_dimension_evaluator(dep_full_name)?)
        } else {
            Err(CubeError::internal(format!(
                "Cannot resolve dependency {} of member {}.{}",
                name, cube_name, name
            )))
        }
    }
}
