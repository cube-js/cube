use super::MemberSymbol;
use crate::planner::sql_evaluator::SqlCall;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Represents a geo dimension with latitude and longitude
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

    pub fn get_dependencies(&self, deps: &mut Vec<Rc<MemberSymbol>>) {
        self.latitude.extract_symbol_deps(deps);
        self.longitude.extract_symbol_deps(deps);
    }

    pub fn get_dependencies_with_path(&self, deps: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        self.latitude.extract_symbol_deps_with_path(deps);
        self.longitude.extract_symbol_deps_with_path(deps);
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
}