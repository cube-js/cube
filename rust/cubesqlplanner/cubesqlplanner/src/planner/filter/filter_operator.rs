use cubenativeutils::CubeError;
use std::str::FromStr;

#[derive(Clone, PartialEq, Debug)]
pub enum FilterOperator {
    Equal,
    NotEqual,
    InDateRange,
    NotInDateRange,
    BeforeDate,
    BeforeOrOnDate,
    AfterDate,
    AfterOrOnDate,
    RegularRollingWindowDateRange,
    ToDateRollingWindowDateRange,
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
    MeasureFilter,
}

impl FromStr for FilterOperator {
    type Err = CubeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace("_", "").as_str() {
            "equals" => Ok(Self::Equal),
            "notequals" => Ok(Self::NotEqual),
            "indaterange" => Ok(Self::InDateRange),
            "onthedate" => Ok(Self::InDateRange),
            "beforedate" => Ok(Self::BeforeDate),
            "beforeorondate" => Ok(Self::BeforeOrOnDate),
            "afterdate" => Ok(Self::AfterDate),
            "afterorondate" => Ok(Self::AfterOrOnDate),
            "notindaterange" => Ok(Self::NotInDateRange),
            "in" => Ok(Self::In),
            "notin" => Ok(Self::NotIn),
            "set" => Ok(Self::Set),
            "notset" => Ok(Self::NotSet),
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
            "measurefilter" => Ok(Self::MeasureFilter),

            _ => Err(CubeError::user(format!("Unknown filter operator {}", s))),
        }
    }
}

impl ToString for FilterOperator {
    fn to_string(&self) -> String {
        let str = match self {
            FilterOperator::Equal => "equals",
            FilterOperator::NotEqual => "notEquals",
            FilterOperator::InDateRange => "inDateRange",
            FilterOperator::NotInDateRange => "notInDateRange",
            FilterOperator::BeforeDate => "beforeDate",
            FilterOperator::BeforeOrOnDate => "beforeOrOnDate",
            FilterOperator::AfterDate => "afterDate",
            FilterOperator::AfterOrOnDate => "afterOrOnDate",
            FilterOperator::RegularRollingWindowDateRange => "inDateRange",
            FilterOperator::ToDateRollingWindowDateRange => "inDateRange",
            FilterOperator::In => "in",
            FilterOperator::NotIn => "notIn",
            FilterOperator::Set => "set",
            FilterOperator::NotSet => "notSet",
            FilterOperator::Gt => "gt",
            FilterOperator::Gte => "gte",
            FilterOperator::Lt => "lt",
            FilterOperator::Lte => "lte",
            FilterOperator::Contains => "contains",
            FilterOperator::NotContains => "notContains",
            FilterOperator::StartsWith => "startsWith",
            FilterOperator::NotStartsWith => "notStartsWith",
            FilterOperator::NotEndsWith => "notEndsWith",
            FilterOperator::EndsWith => "endsWith",
            FilterOperator::MeasureFilter => "measureFilter",
        };
        str.to_string()
    }
}
