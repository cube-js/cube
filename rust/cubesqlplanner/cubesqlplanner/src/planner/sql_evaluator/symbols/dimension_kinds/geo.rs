use super::super::MemberSymbol;
use crate::planner::sql_evaluator::{CubeRef, SqlCall};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct GeoDimension {
    latitude: Rc<SqlCall>,
    longitude: Rc<SqlCall>,
}

impl GeoDimension {
    pub fn new(latitude: Rc<SqlCall>, longitude: Rc<SqlCall>) -> Self {
        Self {
            latitude,
            longitude,
        }
    }

    pub fn latitude(&self) -> &Rc<SqlCall> {
        &self.latitude
    }

    pub fn longitude(&self) -> &Rc<SqlCall> {
        &self.longitude
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        self.latitude.extract_symbol_deps(&mut deps);
        self.longitude.extract_symbol_deps(&mut deps);
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            latitude: self.latitude.apply_recursive(f)?,
            longitude: self.longitude.apply_recursive(f)?,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(std::iter::once(&self.latitude).chain(std::iter::once(&self.longitude)))
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        self.latitude.extract_cube_refs(&mut refs);
        self.longitude.extract_cube_refs(&mut refs);
        refs
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.latitude.is_owned_by_cube() || self.longitude.is_owned_by_cube()
    }
}
