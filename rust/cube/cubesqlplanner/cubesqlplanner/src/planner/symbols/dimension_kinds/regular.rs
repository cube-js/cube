use super::super::common::DimensionType;
use super::super::deps::symbol_deps;
use crate::planner::SqlCall;
use std::rc::Rc;

/// Plain dimension from the data model — a single `sql` expression
/// typed by `DimensionType`.
#[derive(Clone)]
pub struct RegularDimension {
    dimension_type: DimensionType,
    member_sql: Rc<SqlCall>,
}

symbol_deps! {
    RegularDimension {
        dimension_type: skip,
        member_sql: dep,
    }
}

impl RegularDimension {
    pub fn new(dimension_type: DimensionType, member_sql: Rc<SqlCall>) -> Self {
        Self {
            dimension_type,
            member_sql,
        }
    }

    pub fn dimension_type(&self) -> &DimensionType {
        &self.dimension_type
    }

    pub fn member_sql(&self) -> &Rc<SqlCall> {
        &self.member_sql
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(std::iter::once(&self.member_sql))
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.member_sql.is_owned_by_cube()
    }
}
