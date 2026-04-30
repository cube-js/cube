mod case_dimension;
mod geo;
mod regular;
mod switch;

pub use case_dimension::*;
pub use geo::*;
pub use regular::*;
pub use switch::*;

use super::common::DimensionType;
use super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, CubeRef, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum DimensionKind {
    Regular(RegularDimension),
    Geo(GeoDimension),
    Switch(SwitchDimension),
    Case(CaseDimension),
}

impl DimensionKind {
    pub fn evaluate_sql(
        &self,
        name: &str,
        full_name: &str,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match self {
            Self::Regular(r) => r.evaluate_sql(visitor, node_processor, query_tools, templates),
            Self::Geo(_) => Err(CubeError::internal(format!(
                "Geo dimension {} doesn't support evaluate_sql directly",
                full_name
            ))),
            Self::Switch(s) => {
                s.evaluate_sql(name, visitor, node_processor, query_tools, templates)
            }
            Self::Case(c) => {
                c.evaluate_sql(full_name, visitor, node_processor, query_tools, templates)
            }
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        match self {
            Self::Regular(r) => r.get_dependencies(),
            Self::Geo(g) => g.get_dependencies(),
            Self::Switch(s) => s.get_dependencies(),
            Self::Case(c) => c.get_dependencies(),
        }
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(match self {
            Self::Regular(r) => Self::Regular(r.apply_to_deps(f)?),
            Self::Geo(g) => Self::Geo(g.apply_to_deps(f)?),
            Self::Switch(s) => Self::Switch(s.apply_to_deps(f)?),
            Self::Case(c) => Self::Case(c.apply_to_deps(f)?),
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match self {
            Self::Regular(r) => r.iter_sql_calls(),
            Self::Geo(g) => g.iter_sql_calls(),
            Self::Switch(s) => s.iter_sql_calls(),
            Self::Case(c) => c.iter_sql_calls(),
        }
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        match self {
            Self::Regular(r) => r.get_cube_refs(),
            Self::Geo(g) => g.get_cube_refs(),
            Self::Switch(s) => s.get_cube_refs(),
            Self::Case(c) => c.get_cube_refs(),
        }
    }

    pub fn is_owned_by_cube(&self) -> bool {
        match self {
            Self::Regular(r) => r.is_owned_by_cube(),
            Self::Geo(g) => g.is_owned_by_cube(),
            Self::Switch(s) => s.is_owned_by_cube(),
            Self::Case(c) => c.is_owned_by_cube(),
        }
    }

    pub fn is_time(&self) -> bool {
        match self {
            Self::Regular(r) => *r.dimension_type() == DimensionType::Time,
            Self::Case(c) => *c.dimension_type() == DimensionType::Time,
            _ => false,
        }
    }

    pub fn is_geo(&self) -> bool {
        matches!(self, Self::Geo(_))
    }

    pub fn is_switch(&self) -> bool {
        matches!(self, Self::Switch(_))
    }

    pub fn is_case(&self) -> bool {
        matches!(self, Self::Case(_))
    }

    pub fn is_calc_group(&self) -> bool {
        match self {
            Self::Switch(s) => s.is_calc_group(),
            _ => false,
        }
    }

    pub fn dimension_type_str(&self) -> &str {
        match self {
            Self::Regular(r) => r.dimension_type().as_str(),
            Self::Geo(_) => "geo",
            Self::Switch(_) => "switch",
            Self::Case(c) => c.dimension_type().as_str(),
        }
    }
}
