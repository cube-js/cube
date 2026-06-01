use super::case::CaseVariant;
use super::expression::Expression;
use super::path::MemberPath;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MeasureType {
    Count,
    CountDistinct,
    CountDistinctApprox,
    Sum,
    Avg,
    Min,
    Max,
    RunningTotal,
    Number,
    NumberAgg,
    Rank,
    String,
    Time,
    Boolean,
}

impl MeasureType {
    /// Accepts both camelCase (production JS shape after schema-compiler)
    /// and snake_case (YAML schemas). `numberAgg` and `rank` are only
    /// valid on multi-stage measures per the JS validator, but we accept
    /// them unconditionally here — the multi-stage kind is decided
    /// separately in `build_multi_stage_spec`.
    pub fn parse(raw: &str) -> Result<Self, cubenativeutils::CubeError> {
        match raw {
            "count" => Ok(Self::Count),
            "countDistinct" | "count_distinct" => Ok(Self::CountDistinct),
            "countDistinctApprox" | "count_distinct_approx" => Ok(Self::CountDistinctApprox),
            "sum" => Ok(Self::Sum),
            "avg" => Ok(Self::Avg),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "runningTotal" | "running_total" => Ok(Self::RunningTotal),
            "number" => Ok(Self::Number),
            "numberAgg" | "number_agg" => Ok(Self::NumberAgg),
            "rank" => Ok(Self::Rank),
            "string" => Ok(Self::String),
            "time" => Ok(Self::Time),
            "boolean" => Ok(Self::Boolean),
            other => Err(cubenativeutils::CubeError::user(format!(
                "Unknown measure type: {other}"
            ))),
        }
    }
}

#[derive(Clone)]
pub struct Measure {
    pub path: MemberPath,
    pub measure_type: MeasureType,

    pub sql: Option<Expression>,
    pub case: Option<CaseVariant>,
    pub mask_sql: Option<Expression>,

    pub owned_by_cube: bool,
    pub primary_key: bool,

    pub multi_stage: Option<MultiStageSpec>,
    pub rolling_window: Option<RollingWindowSpec>,
    pub time_shifts: Vec<TimeShiftSpec>,

    pub filters: Vec<Expression>,
    pub drill_filters: Vec<Expression>,
    pub order_by: Vec<MeasureOrderBy>,

    /// `aliasMember` — set if this measure is an alias of another member.
    pub alias_member: Option<MemberPath>,
}

#[derive(Clone, Debug)]
pub struct MeasureOrderBy {
    pub sql: Expression,
    pub direction: OrderDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug)]
pub struct MultiStageSpec {
    pub kind: MultiStageKind,
    pub reduce_by: Vec<MemberPath>,
    pub group_by: Vec<MemberPath>,
    pub add_group_by: Vec<MemberPath>,
    pub time_shifts: Vec<TimeShiftSpec>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MultiStageKind {
    Aggregating,
    Filtering,
}

#[derive(Clone, Debug)]
pub struct RollingWindowSpec {
    pub trailing: Option<String>,
    pub leading: Option<String>,
    pub offset: Option<String>,
    pub kind: Option<RollingWindowKind>,
    pub granularity: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollingWindowKind {
    Time,
    Row,
}

#[derive(Clone, Debug)]
pub struct TimeShiftSpec {
    pub interval: Option<String>,
    pub name: Option<String>,
    pub direction: Option<TimeShiftDirection>,
    pub time_dimension: Option<MemberPath>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeShiftDirection {
    Next,
    Prior,
}
