use super::MemberSymbol;
use std::rc::Rc;

pub struct TimeDimensionSymbol {
    base_symbol: Rc<MemberSymbol>,
    full_name: String,
    granularity: Option<String>,
}

impl TimeDimensionSymbol {
    pub fn new(base_symbol: Rc<MemberSymbol>, granularity: Option<String>) -> Self {
        let name_suffix = if let Some(granularity) = &granularity {
            granularity.clone()
        } else {
            "day".to_string()
        };
        let full_name = format!("{}_{}", base_symbol.full_name(), name_suffix);
        Self {
            base_symbol,
            granularity,
            full_name,
        }
    }

    pub fn base_symbol(&self) -> &Rc<MemberSymbol> {
        &self.base_symbol
    }

    pub fn granularity(&self) -> &Option<String> {
        &self.granularity
    }

    pub fn full_name(&self) -> String {
        self.full_name.clone()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        self.base_symbol.get_dependencies()
    }

    pub fn get_dependent_cubes(&self) -> Vec<String> {
        self.base_symbol.get_dependent_cubes()
    }

    pub fn cube_name(&self) -> String {
        self.base_symbol.cube_name()
    }

    pub fn name(&self) -> String {
        self.base_symbol.name()
    }
}
