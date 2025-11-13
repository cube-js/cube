use super::{MemberSymbol, PrimitiveType};
use crate::planner::sql_evaluator::SqlCall;
use cubenativeutils::CubeError;
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

    pub fn get_dependencies(&self, deps: &mut Vec<Rc<MemberSymbol>>) {
        self.member_sql.extract_symbol_deps(deps);
    }

    pub fn get_dependencies_with_path(&self, deps: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        self.member_sql.extract_symbol_deps_with_path(deps);
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            primitive_type: self.primitive_type,
            member_sql: self.member_sql.apply_recursive(f)?,
        })
    }
}

