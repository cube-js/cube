use super::symbols::MemberSymbol;
use super::Compiler;
use crate::cube_bridge::evaluator::{CallDep, CubeEvaluator};
use crate::cube_bridge::member_sql::MemberSql;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub enum CubeDepProperty {
    CubeDependency(CubeDependency),
    SymbolDependency(Rc<MemberSymbol>),
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub enum ContextSymbolDep {
    SecurityContext,
    FilterParams,
    FilterGroup,
    SqlUtils,
}

#[derive(Clone, Debug)]
pub enum Dependency {
    SymbolDependency(Rc<MemberSymbol>),
    CubeDependency(CubeDependency),
    ContextDependency(ContextSymbolDep),
}

pub struct DependenciesBuilder<'a> {
    compiler: &'a mut Compiler,
    cube_evaluator: Rc<dyn CubeEvaluator>,
}

impl<'a> DependenciesBuilder<'a> {
    pub fn new(compiler: &'a mut Compiler, cube_evaluator: Rc<dyn CubeEvaluator>) -> Self {
        DependenciesBuilder {
            compiler,
            cube_evaluator,
        }
    }

    pub fn build(
        mut self,
        cube_name: String,
        member_sql: Rc<dyn MemberSql>,
    ) -> Result<Vec<Dependency>, CubeError> {
        let call_deps = if member_sql.need_deps_resolve() {
            self.cube_evaluator
                .resolve_symbols_call_deps(cube_name.clone(), member_sql)?
        } else {
            vec![]
        };

        let childs = self.deduplicate_deps_and_make_childs_tree(&call_deps)?;
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
        }

        Ok(result)
    }

    fn deduplicate_deps_and_make_childs_tree(
        &self,
        call_deps: &Vec<CallDep>,
    ) -> Result<Vec<Vec<usize>>, CubeError> {
        let mut childs_tree = Vec::new();
        let mut deduplicate_index_map = HashMap::<usize, usize>::new();
        let mut deduplicate_map = HashMap::<CallDep, usize>::new();
        for (i, dep) in call_deps.iter().enumerate() {
            //If subcube is used twice in function, then call_deps can hold duplicated dependencies
            //(for exampls in function ${Orders.ProductsAlt.name} || '_' || ${Orders.ProductsAlt.ProductCategories.name} ProductsAlt appeared twice in call_deps))
            let self_index = if let Some(exists_index) = deduplicate_map.get(&dep) {
                deduplicate_index_map.insert(i, *exists_index);
                *exists_index
            } else {
                deduplicate_map.insert(dep.clone(), i);
                i
            };

            childs_tree.push(vec![]);
            if let Some(parent) = dep.parent {
                let deduplecated_parent = deduplicate_index_map.get(&parent).unwrap_or(&parent);
                childs_tree[*deduplecated_parent].push(self_index);
            }
        }

        Ok(childs_tree)
    }

    fn build_cube_dependency(
        &mut self,
        cube_name: &String,
        dep_index: usize,
        call_deps: &Vec<CallDep>,
        call_childs: &Vec<Vec<usize>>,
    ) -> Result<CubeDependency, CubeError> {
        let dep = &call_deps[dep_index];
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
        }
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
