use super::{Compiler, EvaluationNode};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::MemberSql;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct StructDependency {
    pub sql_fn: Option<Rc<EvaluationNode>>,
    pub to_string_fn: Option<Rc<EvaluationNode>>,
    pub properties: HashMap<String, Dependency>,
}

impl StructDependency {
    pub fn new(
        sql_fn: Option<Rc<EvaluationNode>>,
        to_string_fn: Option<Rc<EvaluationNode>>,
        properties: HashMap<String, Dependency>,
    ) -> Self {
        StructDependency {
            sql_fn,
            to_string_fn,
            properties,
        }
    }
}

pub enum ContextSymbolDep {
    SecurityContext,
    FilterParams,
    FilterGroup,
}

pub enum Dependency {
    SingleDependency(Rc<EvaluationNode>),
    StructDependency(StructDependency),
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
        member_sql: Option<Rc<dyn MemberSql>>,
    ) -> Result<Vec<Dependency>, CubeError> {
        let call_deps = if let Some(member_sql) = member_sql {
            self.cube_evaluator
                .resolve_symbols_call_deps(cube_name.clone(), member_sql)?
        } else {
            vec![]
        };

        let mut childs = Vec::new();
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
                result.push(Dependency::SingleDependency(
                    self.build_evaluator(&cube_name, &dep.name)?,
                ));
            } else {
                let new_cube_name = if self.is_current_cube(&dep.name) {
                    cube_name.clone()
                } else {
                    dep.name.clone()
                };
                let mut sql_fn = None;
                let mut to_string_fn: Option<Rc<EvaluationNode>> = None;
                let mut properties = HashMap::new();
                for child_ind in childs[i].iter() {
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
                        properties.insert(
                            name.clone(),
                            Dependency::SingleDependency(
                                self.build_evaluator(&new_cube_name, &name)?,
                            ),
                        );
                    }
                }
                result.push(Dependency::StructDependency(StructDependency::new(
                    sql_fn,
                    to_string_fn,
                    properties,
                )));
            }
        }

        Ok(result)
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
    ) -> Result<Rc<EvaluationNode>, CubeError> {
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
