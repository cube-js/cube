use super::dependency::{
    ContextSymbolDep, CubeDepProperty, CubeDependency, Dependency, TimeDimensionDependency,
};
use super::sql_nodes::SqlNode;
use super::{symbols::MemberSymbol, SqlEvaluatorVisitor};
use crate::cube_bridge::base_query_options::FilterItem as NativeFilterItem;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::member_sql::{ContextSymbolArg, MemberSql, MemberSqlArg, MemberSqlStruct};
use crate::plan::{Filter, FilterItem};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FilterParamsItem {
    pub cube_name: String,
    pub name: String,
    pub column: String,
}

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct FilterGroupItem {
    pub filter_params: Vec<FilterParamsItem>,
}

#[derive(Default, Clone, Debug)]
pub struct SecutityContextProps {
    pub values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SqlCallDependency {
    pub path: Vec<String>,
    pub symbol: Rc<MemberSymbol>,
}

#[derive(Clone, TypedBuilder)]
pub struct SqlCallNew {
    template: String,
    deps: Vec<SqlCallDependency>,
    filter_params: Vec<FilterParamsItem>,
    filter_groups: Vec<FilterGroupItem>,
    security_context: SecutityContextProps,
}

impl SqlCallNew {
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
}
