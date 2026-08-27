use super::super::common::{Case, DimensionType};
use super::super::deps::symbol_deps;
use crate::planner::SqlCall;
use std::rc::Rc;

/// Dimension whose value is defined via the `case` field of the
/// data-model definition. Comes in classic and switch-style forms;
/// see `Case`.
#[derive(Clone)]
pub struct CaseDimension {
    dimension_type: DimensionType,
    case: Case,
    member_sql: Option<Rc<SqlCall>>,
}

symbol_deps! {
    CaseDimension {
        dimension_type: skip,
        member_sql: dep,
        case: dep,
    }
}

impl CaseDimension {
    pub fn new(dimension_type: DimensionType, case: Case, member_sql: Option<Rc<SqlCall>>) -> Self {
        Self {
            dimension_type,
            case,
            member_sql,
        }
    }

    pub fn dimension_type(&self) -> &DimensionType {
        &self.dimension_type
    }

    pub fn case(&self) -> &Case {
        &self.case
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        self.member_sql.as_ref()
    }

    pub fn replace_case(&self, new_case: Case) -> Self {
        Self {
            dimension_type: self.dimension_type,
            case: new_case,
            member_sql: self.member_sql.clone(),
        }
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter().chain(self.case.iter_sql_calls()))
    }

    pub fn is_owned_by_cube(&self) -> bool {
        let mut owned = false;
        if let Some(sql) = &self.member_sql {
            owned |= sql.is_owned_by_cube();
        }
        owned |= self.case.is_owned_by_cube();
        owned
    }
}
