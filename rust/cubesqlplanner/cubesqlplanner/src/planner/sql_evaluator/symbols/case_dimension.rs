use super::{Case, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Represents a case dimension with conditional logic
#[derive(Clone)]
pub struct CaseDimension {
    case: Case,
}

impl CaseDimension {
    pub fn new(case: Case) -> Self {
        Self { case }
    }

    pub fn case(&self) -> &Case {
        &self.case
    }

    pub fn get_dependencies(&self, deps: &mut Vec<Rc<MemberSymbol>>) {
        self.case.extract_symbol_deps(deps);
    }

    pub fn get_dependencies_with_path(&self, deps: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        self.case.extract_symbol_deps_with_path(deps);
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            case: self.case.apply_to_deps(f)?,
        })
    }
}