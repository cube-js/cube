use super::PrimitiveType;
use crate::planner::sql_evaluator::SqlCall;
use std::rc::Rc;

/// Represents a simple dimension with a primitive type
#[derive(Clone)]
pub struct SimpleDimension {
    primitive_type: PrimitiveType,
    member_sql: Rc<SqlCall>,
}

impl SimpleDimension {
    pub fn new(primitive_type: PrimitiveType, member_sql: Rc<SqlCall>) -> Self {
        Self {
            primitive_type,
            member_sql,
        }
    }

    pub fn primitive_type(&self) -> PrimitiveType {
        self.primitive_type
    }

    pub fn member_sql(&self) -> &Rc<SqlCall> {
        &self.member_sql
    }
}

