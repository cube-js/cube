use super::dependecy::{
    ContextSymbolDep, CubeDepProperty, CubeDependency, Dependency, TimeDimensionDependency,
};
use super::sql_nodes::SqlNode;
use super::{symbols::MemberSymbol, SqlEvaluatorVisitor};
use crate::cube_bridge::base_query_options::FilterItem as NativeFilterItem;
use crate::cube_bridge::member_sql::{ContextSymbolArg, MemberSql, MemberSqlArg, MemberSqlStruct};
use crate::plan::{Filter, FilterItem};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
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

    pub fn is_direct_reference(&self) -> Result<bool, CubeError> {
        let dependencies = self.get_dependencies();
        if dependencies.len() != 1 {
            return Ok(false);
        }

        let reference_candidate = dependencies[0].clone();

        let args = self
            .deps
            .iter()
            .map(|d| self.evaluate_single_dep_for_ref_check(&d))
            .collect::<Result<Vec<_>, _>>()?;
        let eval_result = self.member_sql.call(args)?;

        Ok(eval_result.trim() == &reference_candidate.full_name())
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
                Dependency::TimeDimensionDependency(dep) => {
                    for (_, granularity) in dep.granularities.iter() {
                        result.push(granularity.clone());
                    }
                }
                Dependency::ContextDependency(_) => {}
            }
        }
    }

    pub fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        for dep in self.deps.iter() {
            match dep {
                Dependency::SymbolDependency(dep) => result.push((dep.clone(), vec![])),
                Dependency::TimeDimensionDependency(dep) => {
                    for (_, granularity) in dep.granularities.iter() {
                        result.push((granularity.clone(), vec![]));
                    }
                }
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
                Dependency::TimeDimensionDependency(_) => {}
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
                CubeDepProperty::TimeDimensionDependency(dep) => {
                    for (_, granularity) in dep.granularities.iter() {
                        result.push(granularity.clone());
                    }
                }
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
                CubeDepProperty::TimeDimensionDependency(dep) => {
                    for (_, granularity) in dep.granularities.iter() {
                        result.push((granularity.clone(), path.clone()));
                    }
                }
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

    //TODO temporary solution, should be removed after refactoring
    fn evaluate_single_dep_for_ref_check(
        &self,
        dep: &Dependency,
    ) -> Result<MemberSqlArg, CubeError> {
        match dep {
            Dependency::SymbolDependency(dep) => Ok(MemberSqlArg::String(dep.full_name())),
            Dependency::TimeDimensionDependency(dep) => {
                self.evaluate_time_dimesion_dep_for_ref_check(dep)
            }
            Dependency::CubeDependency(dep) => self.evaluate_cube_dep_for_ref_check(dep),
            Dependency::ContextDependency(_) => Ok(MemberSqlArg::String(format!("Context Symbol"))),
        }
    }

    //TODO temporary solution, should be removed after refactoring
    fn evaluate_cube_dep_for_ref_check(
        &self,
        dep: &CubeDependency,
    ) -> Result<MemberSqlArg, CubeError> {
        let mut res = MemberSqlStruct::default();
        if let Some(sql_fn) = &dep.sql_fn {
            res.sql_fn = Some(sql_fn.full_name());
        }
        if let Some(to_string_fn) = &dep.to_string_fn {
            res.to_string_fn = Some(to_string_fn.full_name());
        }
        for (k, v) in dep.properties.iter() {
            let prop_res = match v {
                CubeDepProperty::SymbolDependency(dep) => MemberSqlArg::String(dep.full_name()),

                CubeDepProperty::TimeDimensionDependency(dep) => {
                    self.evaluate_time_dimesion_dep_for_ref_check(dep)?
                }

                CubeDepProperty::CubeDependency(dep) => {
                    self.evaluate_cube_dep_for_ref_check(&dep)?
                }
            };
            res.properties.insert(k.clone(), prop_res);
        }
        Ok(MemberSqlArg::Struct(res))
    }

    fn evaluate_time_dimesion_dep_for_ref_check(
        &self,
        dep: &TimeDimensionDependency,
    ) -> Result<MemberSqlArg, CubeError> {
        let mut res = MemberSqlStruct::default();
        for (k, v) in dep.granularities.iter() {
            let arg = MemberSqlArg::String(v.full_name());
            res.properties.insert(k.clone(), arg);
        }
        Ok(MemberSqlArg::Struct(res))
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
            Dependency::TimeDimensionDependency(dep) => {
                self.evaluate_time_dimesion_dep(dep, visitor, node_processor.clone(), templates)
            }
            Dependency::CubeDependency(dep) => self.evaluate_cube_dep(
                dep,
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            ),
            Dependency::ContextDependency(contex_symbol) => {
                self.apply_context_symbol(visitor, contex_symbol, query_tools.clone())
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

                CubeDepProperty::TimeDimensionDependency(dep) => self.evaluate_time_dimesion_dep(
                    dep,
                    visitor,
                    node_processor.clone(),
                    templates,
                )?,

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

    fn evaluate_time_dimesion_dep(
        &self,
        dep: &TimeDimensionDependency,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<MemberSqlArg, CubeError> {
        let mut res = MemberSqlStruct::default();
        for (k, v) in dep.granularities.iter() {
            let arg = MemberSqlArg::String(visitor.apply(&v, node_processor.clone(), templates)?);
            res.properties.insert(k.clone(), arg);
        }
        Ok(MemberSqlArg::Struct(res))
    }

    pub fn apply_context_symbol(
        &self,
        visitor: &SqlEvaluatorVisitor,
        context_symbol: &ContextSymbolDep,
        query_tools: Rc<QueryTools>,
    ) -> Result<MemberSqlArg, CubeError> {
        let res = match context_symbol {
            ContextSymbolDep::SecurityContext => {
                MemberSqlArg::ContextSymbol(ContextSymbolArg::SecurityContext(
                    query_tools.base_tools().security_context_for_rust()?,
                ))
            }
            ContextSymbolDep::FilterParams => {
                let filters = visitor.all_filters();
                let native_filters = self.filters_to_native_filter_item(filters);
                let r = query_tools
                    .base_tools()
                    .filters_proxy_for_rust(native_filters)?;
                MemberSqlArg::ContextSymbol(ContextSymbolArg::FilterParams(r))
            }
            ContextSymbolDep::FilterGroup => {
                let filters = visitor.all_filters();
                let native_filters = self.filters_to_native_filter_item(filters);
                let r = query_tools
                    .base_tools()
                    .filter_group_function_for_rust(native_filters)?;
                MemberSqlArg::ContextSymbol(ContextSymbolArg::FilterGroup(r))
            }
            ContextSymbolDep::SqlUtils => MemberSqlArg::ContextSymbol(ContextSymbolArg::SqlUtils(
                query_tools.base_tools().sql_utils_for_rust()?,
            )),
        };
        Ok(res)
    }

    fn filters_to_native_filter_item(
        &self,
        filter: Option<Filter>,
    ) -> Option<Vec<NativeFilterItem>> {
        if let Some(filter) = filter {
            let mut res = Vec::new();
            for item in filter.items.iter() {
                if let Some(item) = self.filters_to_native_filter_item_impl(item) {
                    res.push(item);
                }
            }
            Some(res)
        } else {
            None
        }
    }

    fn filters_to_native_filter_item_impl(
        &self,
        filter_item: &FilterItem,
    ) -> Option<NativeFilterItem> {
        match filter_item {
            FilterItem::Group(group) => {
                let mut native_items = Vec::new();
                for itm in group.items.iter() {
                    if let Some(item) = self.filters_to_native_filter_item_impl(itm) {
                        native_items.push(item);
                    }
                }
                let (or, and) = match group.operator {
                    crate::plan::filter::FilterGroupOperator::Or => (Some(native_items), None),
                    crate::plan::filter::FilterGroupOperator::And => (None, Some(native_items)),
                };
                Some(NativeFilterItem {
                    or,
                    and,
                    member: None,
                    dimension: None,
                    operator: None,
                    values: None,
                })
            }
            FilterItem::Item(filter) => {
                if filter.use_raw_values() {
                    None
                } else {
                    Some(NativeFilterItem {
                        or: None,
                        and: None,
                        member: Some(filter.member_name()),
                        dimension: None,
                        operator: Some(filter.filter_operator().to_string()),
                        values: Some(filter.values().clone()),
                    })
                }
            }
            FilterItem::Segment(_) => None,
        }
    }
}
