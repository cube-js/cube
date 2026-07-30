use super::super::deps::{symbol_deps, DepVisitor, DepVisitorMut, SymbolDeps};
use crate::planner::SqlCall;
use cubenativeutils::CubeError;
use std::ops::ControlFlow;
use std::rc::Rc;

/// Source of a `Count` measure's SQL.
///
/// - `Auto` — no explicit `sql` was declared; the count falls back
///   to the cube's primary-key expressions.
/// - `Explicit` — `sql` was declared on the measure.
#[derive(Clone)]
pub enum CountSql {
    Auto(Vec<Rc<SqlCall>>),
    Explicit(Rc<SqlCall>),
}

impl SymbolDeps for CountSql {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        match self {
            Self::Auto(pk_sqls) => pk_sqls.visit_deps(visitor),
            Self::Explicit(sql) => sql.visit_deps(visitor),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Self::Auto(pk_sqls) => pk_sqls.visit_deps_mut(visitor),
            Self::Explicit(sql) => sql.visit_deps_mut(visitor),
        }
    }
}

/// `Count` measure kind: counts rows of the underlying source.
/// Without an explicit `sql` falls back to counting the cube's
/// primary-key tuples.
#[derive(Clone)]
pub struct CountMeasure {
    sql: CountSql,
}

symbol_deps! {
    CountMeasure {
        sql: dep,
    }
}

impl CountMeasure {
    pub fn new(sql: CountSql) -> Self {
        Self { sql }
    }

    pub fn sql(&self) -> &CountSql {
        &self.sql
    }

    /// True when this count can be rendered as a key-distinct count to
    /// survive row multiplication: it falls back to the cube's primary
    /// keys (`Auto`) and at least one key is available.
    pub fn convertible_to_distinct(&self) -> bool {
        matches!(&self.sql, CountSql::Auto(pks) if !pks.is_empty())
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match &self.sql {
            CountSql::Explicit(sql) => Box::new(std::iter::once(sql)),
            CountSql::Auto(pk_sqls) => Box::new(pk_sqls.iter()),
        }
    }

    pub fn is_owned_by_cube(&self) -> bool {
        matches!(self.sql, CountSql::Auto(_))
    }
}
