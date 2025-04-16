use super::symbols::MemberSymbol;
use super::Compiler;
use crate::cube_bridge::evaluator::{CallDep, CubeEvaluator};
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::sql_evaluator::TimeDimensionSymbol;
use crate::planner::GranularityHelper;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub enum CubeDepProperty {
    CubeDependency(CubeDependency),
    SymbolDependency(Rc<MemberSymbol>),
    TimeDimensionDependency(TimeDimensionDependency),
}

#[derive(Clone, Debug)]
pub struct TimeDimensionDependency {
    pub base_symbol: Rc<MemberSymbol>,
    pub granularities: HashMap<String, Rc<MemberSymbol>>,
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
    TimeDimensionDependency(TimeDimensionDependency),
    ContextDependency(ContextSymbolDep),
}

pub struct DependenciesBuilder<'a> {
    compiler: &'a mut Compiler,
    cube_evaluator: Rc<dyn CubeEvaluator>,
    timezone: Tz,
}

impl<'a> DependenciesBuilder<'a> {
    pub fn new(
        compiler: &'a mut Compiler,
        cube_evaluator: Rc<dyn CubeEvaluator>,
        timezone: Tz,
    ) -> Self {
        DependenciesBuilder {
            compiler,
            cube_evaluator,
            timezone,
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
            } else if self.check_cube_exists(&dep.name)? {
                let dep = self.build_cube_dependency(&cube_name, i, &call_deps, &childs)?;
                result.push(Dependency::CubeDependency(dep));
            } else {
                //Assuming this is a time dimension with an explicit granularity
                let dep =
                    self.build_time_dimension_dependency(&cube_name, i, &call_deps, &childs)?;
                result.push(Dependency::TimeDimensionDependency(dep));
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

    fn check_cube_exists(&self, cube_name: &String) -> Result<bool, CubeError> {
        if self.is_current_cube(cube_name) {
            Ok(true)
        } else {
            self.cube_evaluator.cube_exists(cube_name.clone())
        }
    }

    fn build_time_dimension_dependency(
        &mut self,
        cube_name: &String,
        dep_index: usize,
        call_deps: &Vec<CallDep>,
        call_childs: &Vec<Vec<usize>>,
    ) -> Result<TimeDimensionDependency, CubeError> {
        let dep = &call_deps[dep_index];
        let base_evaluator = self.build_evaluator(cube_name, &dep.name)?;
        let mut granularities = HashMap::new();
        for child_ind in call_childs[dep_index].iter() {
            let granularity = &call_deps[*child_ind].name;
            if let Some(granularity_obj) = GranularityHelper::make_granularity_obj(
                self.cube_evaluator.clone(),
                self.timezone.clone(),
                cube_name,
                &dep.name,
                Some(granularity.clone()),
            )? {
                let member_evaluator =
                    Rc::new(MemberSymbol::TimeDimension(TimeDimensionSymbol::new(
                        base_evaluator.clone(),
                        Some(granularity.clone()),
                        Some(granularity_obj),
                    )));
                granularities.insert(granularity.clone(), member_evaluator);
            } else {
                return Err(CubeError::user(format!(
                    "Undefined granularity {} for time dimension {}",
                    granularity, dep.name
                )));
            }
        }
        let result = TimeDimensionDependency {
            base_symbol: base_evaluator,
            granularities,
        };
        Ok(result)
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
                } else if self.check_cube_exists(name)? {
                    CubeDepProperty::CubeDependency(self.build_cube_dependency(
                        &new_cube_name,
                        *child_ind,
                        call_deps,
                        call_childs,
                    )?)
                } else {
                    let dep = self.build_time_dimension_dependency(
                        &new_cube_name,
                        *child_ind,
                        call_deps,
                        call_childs,
                    )?;
                    CubeDepProperty::TimeDimensionDependency(dep)
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
        self.compiler
            .add_auto_resolved_member_evaluator(dep_full_name)
    }
}
