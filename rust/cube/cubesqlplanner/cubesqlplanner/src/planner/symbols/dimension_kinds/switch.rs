use super::super::deps::symbol_deps;
use crate::planner::SqlCall;
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

symbol_deps! {
    SwitchDimension {
        values: skip,
        member_sql: dep,
    }
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

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter())
    }

    pub fn is_owned_by_cube(&self) -> bool {
        false
    }
}
