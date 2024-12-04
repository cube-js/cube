use cubenativeutils::CubeError;
use std::str::FromStr;

#[derive(Clone, PartialEq, Debug)]
pub enum FilterOperator {
    Equal,
    NotEqual,
    InDateRange,
    InDateRangeExtended,
    In,
    NotIn,
    Set,
    NotSet,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    NotContains,
    StartsWith,
    NotStartsWith,
    NotEndsWith,
    EndsWith,
}

impl FromStr for FilterOperator {
    type Err = CubeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "equals" => Ok(Self::Equal),
            "notequals" => Ok(Self::NotEqual),
            "indaterange" => Ok(Self::InDateRange),
            "in" => Ok(Self::In),
            "notin" => Ok(Self::NotIn),
            "set" => Ok(Self::Set),
            "gt" => Ok(Self::Gt),
            "gte" => Ok(Self::Gte),
            "lt" => Ok(Self::Lt),
            "lte" => Ok(Self::Lte),
            "contains" => Ok(Self::Contains),
            "notcontains" => Ok(Self::NotContains),
            "startswith" => Ok(Self::StartsWith),
            "notstartswith" => Ok(Self::NotStartsWith),
            "endswith" => Ok(Self::EndsWith),
            "notendswith" => Ok(Self::NotEndsWith),

            _ => Err(CubeError::user(format!("Unknown filter operator {}", s))),
        }
    }
}
