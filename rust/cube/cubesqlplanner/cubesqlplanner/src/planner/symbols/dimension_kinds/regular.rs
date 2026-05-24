use super::super::common::DimensionType;
use super::super::MemberSymbol;
use crate::planner::{CubeRef, SqlCall};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plain dimension from the data model — a single `sql` expression
/// typed by `DimensionType`.
#[derive(Clone)]
pub struct RegularDimension {
    dimension_type: DimensionType,
    member_sql: Rc<SqlCall>,
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

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        self.member_sql.extract_symbol_deps(&mut deps);
        deps
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        self.member_sql.get_cube_refs()
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            dimension_type: self.dimension_type,
            member_sql: self.member_sql.apply_recursive(f)?,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(std::iter::once(&self.member_sql))
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.member_sql.is_owned_by_cube()
    }
}
