use super::dependency::{
    ContextSymbolDep, CubeDepProperty, CubeDependency, Dependency, TimeDimensionDependency,
};
use super::sql_nodes::SqlNode;
use super::{symbols::MemberSymbol, SqlEvaluatorVisitor};
use crate::cube_bridge::base_query_options::FilterItem as NativeFilterItem;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::member_sql::{
    ContextSymbolArg, MemberSql, MemberSqlArg, MemberSqlStruct, SecutityContextProps,
};
use crate::plan::{Filter, FilterItem};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::rc::Rc;
use typed_builder::TypedBuilder;

pub struct SqlCallArg;

impl SqlCallArg {
    pub fn dependency(i: usize) -> String {
        format!("{{arg:{}}}", i)
    }
    pub fn filter_param(i: usize) -> String {
        format!("{{fp:{}}}", i)
    }
    pub fn filter_group(i: usize) -> String {
        format!("{{fg:{}}}", i)
    }
    pub fn security_value(i: usize) -> String {
        format!("{{sv:{}}}", i)
    }
}

#[derive(Debug, Clone)]
pub struct SqlCallDependency {
    pub path: Vec<String>,
    pub symbol: Rc<MemberSymbol>,
}

#[derive(Debug, Clone)]
pub struct SqlCallFilterParamsItem {
    pub filter_symbol: Rc<MemberSymbol>,
    pub column: String,
}

#[derive(Clone, Debug)]
pub struct SqlCallFilterGroupItem {
    pub filter_params: Vec<SqlCallFilterParamsItem>,
}

#[derive(Clone, TypedBuilder)]
pub struct SqlCallNew {
    template: String,
    deps: Vec<SqlCallDependency>,
    filter_params: Vec<SqlCallFilterParamsItem>,
    filter_groups: Vec<SqlCallFilterGroupItem>,
    security_context: SecutityContextProps,
}

impl SqlCallNew {
    pub fn eval(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let filter_params = self
            .filter_params
            .iter()
            .map(|itm| {
                Self::eval_filter_group(&vec![itm.clone()], visitor, query_tools.clone(), templates)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let filter_groups = self
            .filter_groups
            .iter()
            .map(|itm| {
                Self::eval_filter_group(&itm.filter_params, visitor, query_tools.clone(), templates)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let deps = self
            .deps
            .iter()
            .map(|dep| visitor.apply(&dep.symbol, node_processor.clone(), templates))
            .collect::<Result<Vec<_>, _>>()?;

        let context_values = self.eval_security_context_values(&query_tools);

        todo!()
    }

    fn eval_filter_group(
        items: &[SqlCallFilterParamsItem],
        visitor: &SqlEvaluatorVisitor,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(all_filters) = visitor.all_filters() {
            if let Some(filter_item) = all_filters.to_filter_item() {
                let symbols = items
                    .iter()
                    .map(|itm| itm.filter_symbol.clone())
                    .collect_vec();
                if let Some(subtree) = filter_item.find_subtree_for_members(&symbols) {
                    let mut context_factory = SqlNodesFactory::new();
                    for itm in items {
                        context_factory.add_render_reference(
                            itm.filter_symbol.full_name(),
                            itm.column.clone(),
                        );
                    }
                    let context = Rc::new(VisitorContext::new(
                        query_tools.clone(),
                        &context_factory,
                        None,
                    ));
                    return subtree.to_sql(templates, context);
                }
            }
        }
        templates.always_true()
    }

    fn eval_security_context_values(&self, query_tools: &Rc<QueryTools>) -> Vec<String> {
        self.security_context
            .values
            .iter()
            .map(|itm| query_tools.allocate_param(itm))
            .collect()
    }

    pub fn is_direct_reference(&self) -> bool {
        self.dependencies_count() == 1 && self.template == "{arg:0}"
    }

    pub fn resolve_direct_reference(&self) -> Option<Rc<MemberSymbol>> {
        if self.is_direct_reference() {
            Some(self.deps[0].symbol.clone())
        } else {
            None
        }
    }

    pub fn dependencies_count(&self) -> usize {
        self.deps.iter().filter(|d| !d.symbol.is_cube()).count()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        self.deps
            .iter()
            .filter_map(|d| {
                if d.symbol.is_cube() {
                    None
                } else {
                    Some(d.symbol.clone())
                }
            })
            .collect()
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        self.deps
            .iter()
            .filter_map(|d| {
                if d.symbol.is_cube() {
                    None
                } else {
                    Some((d.symbol.clone(), d.path.clone()))
                }
            })
            .collect()
    }

    pub fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        for dep in self.deps.iter() {
            if !dep.symbol.is_cube() {
                result.push(dep.symbol.clone())
            }
        }
    }

    pub fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        for dep in self.deps.iter() {
            if !dep.symbol.is_cube() {
                result.push((dep.symbol.clone(), dep.path.clone()))
            }
        }
    }

    pub fn apply_recursive<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Rc<Self>, CubeError> {
        let mut result = self.clone();
        for dep in result.deps.iter_mut() {
            dep.symbol = dep.symbol.apply_recursive(f)?;
        }
        Ok(Rc::new(result))
    }
}
