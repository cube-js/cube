use super::super::MemberSymbol;
use crate::planner::{CubeRef, SqlCall};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// `type: switch` dimension from the data model: an enum with a
/// fixed list of allowed string values. With a `sql` — an ordinary
/// enum dimension reading from a real column. Without a `sql` — a
/// **calc group**: an abstract enumeration cross-joined into the
/// query as a virtual table of values.
#[derive(Clone)]
pub struct SwitchDimension {
    values: Vec<String>,
    member_sql: Option<Rc<SqlCall>>,
}

impl SwitchDimension {
    pub fn new(values: Vec<String>, member_sql: Option<Rc<SqlCall>>) -> Self {
        Self { values, member_sql }
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        self.member_sql.as_ref()
    }

    /// True when the switch dimension was declared without a `sql` —
    /// a calc group: an abstract enumeration cross-joined into the
    /// query rather than read from a column.
    pub fn is_calc_group(&self) -> bool {
        self.member_sql.is_none()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let member_sql = if let Some(sql) = &self.member_sql {
            Some(sql.apply_recursive(f)?)
        } else {
            None
        };
        Ok(Self {
            values: self.values.clone(),
            member_sql,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter())
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_cube_refs(&mut refs);
        }
        refs
    }

    pub fn is_owned_by_cube(&self) -> bool {
        false
    }
}
