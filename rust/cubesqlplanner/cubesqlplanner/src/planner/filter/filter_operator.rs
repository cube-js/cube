use cubenativeutils::CubeError;
use std::str::FromStr;
pub enum FilterOperator {
    Equal,
    NotEqual,
}

impl FromStr for FilterOperator {
    type Err = CubeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "equals" => Ok(Self::Equal),
            "notequals" => Ok(Self::NotEqual),
            _ => Err(CubeError::user(format!("Unknown filter operator {}", s))),
        }
    }
}
