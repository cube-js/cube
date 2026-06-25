use super::expression::Expression;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreAggregationKind {
    Rollup,
    OriginalSql,
    RollupJoin,
    RollupLambda,
    AutoRollup,
}

impl PreAggregationKind {
    /// Accepts both camelCase (production) and snake_case (YAML schemas).
    pub fn parse(raw: &str) -> Result<Self, cubenativeutils::CubeError> {
        match raw {
            "rollup" => Ok(Self::Rollup),
            "originalSql" | "original_sql" => Ok(Self::OriginalSql),
            "rollupJoin" | "rollup_join" => Ok(Self::RollupJoin),
            "rollupLambda" | "rollup_lambda" => Ok(Self::RollupLambda),
            "autoRollup" | "auto_rollup" => Ok(Self::AutoRollup),
            other => Err(cubenativeutils::CubeError::user(format!(
                "Unknown pre-aggregation kind: {other}"
            ))),
        }
    }

    pub fn is_rollup_family(&self) -> bool {
        matches!(
            self,
            Self::Rollup | Self::RollupJoin | Self::RollupLambda | Self::AutoRollup
        )
    }
}

#[derive(Clone)]
pub struct PreAggregation {
    pub name: String,
    pub kind: PreAggregationKind,

    pub sql_alias: Option<String>,
    pub external: Option<bool>,
    pub scheduled_refresh: Option<bool>,
    pub refresh_key: Option<RefreshKey>,
    pub use_original_sql_pre_aggregations: bool,
    pub allow_non_strict_date_range_match: bool,
    pub indexes: HashMap<String, Index>,
    pub owned_by_cube: bool,

    /// Set for `Rollup`/`RollupJoin`/`RollupLambda`/`AutoRollup`.
    pub rollup: Option<RollupSpec>,

    /// Set for `OriginalSql`.
    pub original_sql: Option<OriginalSqlSpec>,

    pub build_range_start: Option<Expression>,
    pub build_range_end: Option<Expression>,
}

/// Reference callables on a rollup-style pre-aggregation. Each
/// `*_references` field is a JS function that, when evaluated with
/// proxy arguments, returns the list of qualified member paths it
/// targets. We keep them as `Expression` here — evaluation is the
/// planner's responsibility (it already does it through
/// `evaluator.evaluate_rollup_references`).
#[derive(Clone)]
pub struct RollupSpec {
    pub measures: Option<Expression>,
    pub dimensions: Option<Expression>,
    pub segments: Option<Expression>,
    pub rollups: Option<Expression>,
    pub time_dimensions: Vec<RollupTimeDimension>,
    /// Legacy single-granularity form; new schemas put granularity
    /// inside each `time_dimensions` entry.
    pub granularity: Option<String>,
}

#[derive(Clone)]
pub struct RollupTimeDimension {
    /// Callable that resolves to a `Cube.dimension` reference at
    /// planner time.
    pub dimension: Expression,
    pub granularity: String,
}

#[derive(Clone)]
pub struct OriginalSqlSpec {
    pub partition_granularity: Option<String>,
    /// Legacy `time_dimension`/`timeDimensionReference` — callable
    /// returning a single time dimension reference.
    pub time_dimension: Option<Expression>,
}

#[derive(Clone)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub kind: Option<IndexKind>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Regular,
    Aggregate,
}

impl IndexKind {
    pub fn parse(raw: &str) -> Result<Self, cubenativeutils::CubeError> {
        match raw {
            "regular" => Ok(Self::Regular),
            "aggregate" => Ok(Self::Aggregate),
            other => Err(cubenativeutils::CubeError::user(format!(
                "Unknown index kind: {other}"
            ))),
        }
    }
}

#[derive(Clone)]
pub enum RefreshKey {
    Sql {
        sql: Expression,
        every: Option<EveryInterval>,
    },
    Every {
        every: EveryInterval,
        timezone: Option<String>,
        incremental: bool,
        update_window: Option<EveryInterval>,
    },
    Immutable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EveryInterval(pub String);
