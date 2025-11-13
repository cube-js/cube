use super::Case;

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
}