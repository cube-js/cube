use super::case::CaseVariant;
use super::expression::Expression;
use super::measure::TimeShiftSpec;
use super::path::MemberPath;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DimensionType {
    String,
    Number,
    Time,
    Boolean,
    Geo,
}

impl DimensionType {
    pub fn parse(raw: &str) -> Result<Self, cubenativeutils::CubeError> {
        match raw {
            "string" => Ok(Self::String),
            "number" => Ok(Self::Number),
            "time" => Ok(Self::Time),
            "boolean" => Ok(Self::Boolean),
            "geo" => Ok(Self::Geo),
            other => Err(cubenativeutils::CubeError::user(format!(
                "Unknown dimension type: {other}"
            ))),
        }
    }
}

#[derive(Clone)]
pub struct Dimension {
    pub path: MemberPath,
    pub dimension_type: DimensionType,

    pub sql: Option<Expression>,
    pub case: Option<CaseVariant>,
    pub mask_sql: Option<Expression>,

    /// For `geo` dimensions; ignored for others.
    pub latitude: Option<Expression>,
    pub longitude: Option<Expression>,

    pub primary_key: bool,
    pub owned_by_cube: bool,

    pub sub_query: bool,
    pub propagate_filters_to_sub_query: bool,

    /// Ordered enumeration of possible values (used by string-typed
    /// dimensions with `values: [...]`).
    pub values: Vec<String>,

    pub multi_stage: bool,
    pub add_group_by: Vec<MemberPath>,
    pub time_shifts: Vec<TimeShiftSpec>,

    /// Custom granularities for time-typed dimensions. Predefined
    /// granularities (year/quarter/month/...) are not stored — they are
    /// resolved by name at query time.
    pub granularities: HashMap<String, Granularity>,

    pub alias_member: Option<MemberPath>,
}

/// Custom granularity attached to a time dimension. Either constructed
/// from `interval` (+ optional `offset` / `origin`), or fully specified
/// by a `sql` expression.
#[derive(Clone)]
pub struct Granularity {
    pub name: String,
    pub interval: Option<String>,
    pub offset: Option<String>,
    pub origin: Option<String>,
    pub sql: Option<Expression>,
}
