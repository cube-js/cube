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
}