use super::super::MemberSymbol;
use crate::planner::{CubeRef, SqlCall};
use cubenativeutils::CubeError;
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

/// `Count` measure kind: counts rows of the underlying source.
/// Without an explicit `sql` falls back to counting the cube's
/// primary-key tuples.
#[derive(Clone)]
pub struct CountMeasure {
    sql: CountSql,
}

impl CountMeasure {
    pub fn new(sql: CountSql) -> Self {
        Self { sql }
    }

    pub fn sql(&self) -> &CountSql {
        &self.sql
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        match &self.sql {
            CountSql::Explicit(sql) => sql.extract_symbol_deps(&mut deps),
            CountSql::Auto(pk_sqls) => {
                for pk in pk_sqls {
                    pk.extract_symbol_deps(&mut deps);
                }
            }
        }
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let sql = match &self.sql {
            CountSql::Explicit(sql) => CountSql::Explicit(sql.apply_recursive(f)?),
            CountSql::Auto(pk_sqls) => {
                let new_pks = pk_sqls
                    .iter()
                    .map(|pk| pk.apply_recursive(f))
                    .collect::<Result<Vec<_>, _>>()?;
                CountSql::Auto(new_pks)
            }
        };
        Ok(Self { sql })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match &self.sql {
            CountSql::Explicit(sql) => Box::new(std::iter::once(sql)),
            CountSql::Auto(pk_sqls) => Box::new(pk_sqls.iter()),
        }
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        match &self.sql {
            CountSql::Explicit(sql) => sql.extract_cube_refs(&mut refs),
            CountSql::Auto(pk_sqls) => {
                for pk in pk_sqls {
                    pk.extract_cube_refs(&mut refs);
                }
            }
        }
        refs
    }

    pub fn is_owned_by_cube(&self) -> bool {
        matches!(self.sql, CountSql::Auto(_))
    }
}
