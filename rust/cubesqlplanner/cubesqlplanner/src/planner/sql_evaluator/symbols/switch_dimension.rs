use super::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Represents a switch dimension with predefined values
#[derive(Clone)]
pub struct SwitchDimension {
    values: Vec<String>,
}

impl SwitchDimension {
    pub fn new(values: Vec<String>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &Vec<String> {
        &self.values
    }

    pub fn get_dependencies(&self, _deps: &mut Vec<Rc<MemberSymbol>>) {
        // Switch dimension has no SQL dependencies
    }

    pub fn get_dependencies_with_path(&self, _deps: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        // Switch dimension has no SQL dependencies
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        _f: &F,
    ) -> Result<Self, CubeError> {
        // Switch dimension has no SQL dependencies, return clone
        Ok(self.clone())
    }
}