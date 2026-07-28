mod case_dimension;
mod geo;
mod regular;
mod switch;

pub use case_dimension::*;
pub use geo::*;
pub use regular::*;
pub use switch::*;

use super::common::DimensionType;
use super::deps::{DepVisitor, DepVisitorMut, SymbolDeps};
use crate::planner::SqlCall;
use cubenativeutils::CubeError;
use std::ops::ControlFlow;
use std::rc::Rc;

/// Form of a dimension's value, classified from its data-model
/// definition.
///
/// - `Regular` — plain dimension (a single `sql` expression).
/// - `Geo` — `type: geo` dimension with `latitude` / `longitude`.
/// - `Switch` — `type: switch` dimension; without a `sql` it becomes
///   a **calc group** — an abstract enumeration cross-joined into the
///   query rather than read from a column.
/// - `Case` — dimension defined via a `case` body, classic or
///   switch-style.
#[derive(Clone)]
pub enum DimensionKind {
    Regular(RegularDimension),
    Geo(GeoDimension),
    Switch(SwitchDimension),
    Case(CaseDimension),
}

impl SymbolDeps for DimensionKind {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        match self {
            Self::Regular(r) => r.visit_deps(visitor),
            Self::Geo(g) => g.visit_deps(visitor),
            Self::Switch(s) => s.visit_deps(visitor),
            Self::Case(c) => c.visit_deps(visitor),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Self::Regular(r) => r.visit_deps_mut(visitor),
            Self::Geo(g) => g.visit_deps_mut(visitor),
            Self::Switch(s) => s.visit_deps_mut(visitor),
            Self::Case(c) => c.visit_deps_mut(visitor),
        }
    }
}

impl DimensionKind {
    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match self {
            Self::Regular(r) => r.iter_sql_calls(),
            Self::Geo(g) => g.iter_sql_calls(),
            Self::Switch(s) => s.iter_sql_calls(),
            Self::Case(c) => c.iter_sql_calls(),
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

    /// True for a `Switch` dimension declared without a `sql` — a
    /// calc group: an abstract enumeration cross-joined into the query
    /// rather than read from a column.
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
