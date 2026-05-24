mod aggregated;
mod calculated;
mod count;

pub use aggregated::*;
pub use calculated::*;
pub use count::*;

use super::common::AggregationType;
use super::MemberSymbol;
use crate::planner::{CubeRef, SqlCall};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// How a measure kind wraps its inner SQL when rendered: no wrapper
/// at all, a named SQL aggregate function, or one of the distinct-
/// count special forms.
pub enum AggregateWrap<'a> {
    PassThrough,
    Function(&'a str),
    CountDistinct,
    CountDistinctApprox,
}

/// Form of a measure's aggregation, classified from the data-model
/// `type`.
///
/// - `Count` — `type: count`. Counts rows; falls back to the cube's
///   primary keys when no explicit `sql` is given.
/// - `Aggregated` — built-in aggregation (`sum`, `avg`, `min`, `max`,
///   `count_distinct`, `count_distinct_approx`, `number_agg`,
///   `running_total`).
/// - `Calculated` — `type: number / string / time / boolean`. A
///   plain expression with no aggregation wrapper.
/// - `Rank` — `type: rank`. Window-function rank, no `sql`.
#[derive(Clone)]
pub enum MeasureKind {
    Count(CountMeasure),
    Aggregated(AggregatedMeasure),
    Calculated(CalculatedMeasure),
    Rank,
}

impl MeasureKind {
    pub fn from_type_str(
        measure_type: &str,
        member_sql: Option<Rc<SqlCall>>,
        pk_sqls: Vec<Rc<SqlCall>>,
    ) -> Result<Self, CubeError> {
        if measure_type == "count" {
            Ok(match member_sql {
                Some(sql) => Self::Count(CountMeasure::new(CountSql::Explicit(sql))),
                None => Self::Count(CountMeasure::new(CountSql::Auto(pk_sqls))),
            })
        } else if measure_type == "rank" {
            Ok(Self::Rank)
        } else if let Some(calc_type) = CalculatedMeasureType::from_str(measure_type) {
            Ok(if let Some(sql) = member_sql {
                Self::Calculated(CalculatedMeasure::new(calc_type, sql))
            } else {
                Self::Calculated(CalculatedMeasure::new_without_sql(calc_type))
            })
        } else if let Ok(agg_type) = AggregationType::from_str(measure_type) {
            Ok(if let Some(sql) = member_sql {
                Self::Aggregated(AggregatedMeasure::new(agg_type, sql))
            } else {
                Self::Aggregated(AggregatedMeasure::new_without_sql(agg_type))
            })
        } else {
            Err(CubeError::user(format!(
                "Unknown measure type: '{}'",
                measure_type
            )))
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        match self {
            Self::Count(c) => c.get_dependencies(),
            Self::Aggregated(a) => a.get_dependencies(),
            Self::Calculated(c) => c.get_dependencies(),
            Self::Rank => vec![],
        }
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(match self {
            Self::Count(c) => Self::Count(c.apply_to_deps(f)?),
            Self::Aggregated(a) => Self::Aggregated(a.apply_to_deps(f)?),
            Self::Calculated(c) => Self::Calculated(c.apply_to_deps(f)?),
            Self::Rank => Self::Rank,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match self {
            Self::Count(c) => c.iter_sql_calls(),
            Self::Aggregated(a) => a.iter_sql_calls(),
            Self::Calculated(c) => c.iter_sql_calls(),
            Self::Rank => Box::new(std::iter::empty()),
        }
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        match self {
            Self::Count(c) => c.get_cube_refs(),
            Self::Aggregated(a) => a.get_cube_refs(),
            Self::Calculated(c) => c.get_cube_refs(),
            Self::Rank => vec![],
        }
    }

    pub fn is_owned_by_cube(&self) -> bool {
        match self {
            Self::Count(c) => c.is_owned_by_cube(),
            Self::Aggregated(a) => a.is_owned_by_cube(),
            Self::Calculated(c) => c.is_owned_by_cube(),
            Self::Rank => false,
        }
    }

    pub fn is_calculated(&self) -> bool {
        matches!(self, Self::Calculated(_))
    }

    /// True if the kind's aggregation distributes over row union.
    /// Counts are always additive; aggregated measures delegate to
    /// their `AggregationType`. Calculated and rank are not additive.
    pub fn is_additive(&self) -> bool {
        match self {
            Self::Count(_) => true,
            Self::Aggregated(a) => a.agg_type().is_additive(),
            _ => false,
        }
    }

    pub fn measure_type_str(&self) -> &str {
        match self {
            Self::Count(_) => "count",
            Self::Aggregated(a) => a.agg_type().as_str(),
            Self::Calculated(c) => c.calc_type().as_str(),
            Self::Rank => "rank",
        }
    }

    /// True when `new_type` is a compatible aggregation replacement.
    /// Only `Aggregated` measures can have their type replaced, and
    /// only within compatibility groups: `sum`/`avg`/`min`/`max` are
    /// interchangeable among themselves, distinct counts among
    /// themselves.
    pub fn can_replace_type_with(&self, new_type: &str) -> bool {
        match self {
            Self::Aggregated(a) => {
                let target_ok = matches!(
                    new_type,
                    "sum" | "avg" | "min" | "max" | "count_distinct" | "count_distinct_approx"
                );
                match a.agg_type() {
                    AggregationType::Sum
                    | AggregationType::Avg
                    | AggregationType::Min
                    | AggregationType::Max => target_ok,
                    AggregationType::CountDistinct | AggregationType::CountDistinctApprox => {
                        matches!(new_type, "count_distinct" | "count_distinct_approx")
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    /// True if extra `measure_filters` can be merged into the kind.
    /// Counts and the basic aggregations support it; `number_agg`,
    /// `running_total`, calculated and rank measures do not.
    pub fn supports_additional_filters(&self) -> bool {
        match self {
            Self::Count(_) => true,
            Self::Aggregated(a) => matches!(
                a.agg_type(),
                AggregationType::Sum
                    | AggregationType::Avg
                    | AggregationType::Min
                    | AggregationType::Max
                    | AggregationType::CountDistinct
                    | AggregationType::CountDistinctApprox
            ),
            _ => false,
        }
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        match self {
            Self::Count(c) => match c.sql() {
                CountSql::Explicit(sql) => Some(sql),
                CountSql::Auto(_) => None,
            },
            Self::Aggregated(a) => a.member_sql(),
            Self::Calculated(c) => c.member_sql(),
            Self::Rank => None,
        }
    }

    /// How the kind wraps its inner SQL when rendered as a top-level
    /// query measure. `is_multiplied` is true when the join below the
    /// measure can produce duplicate rows — non-distinct counts then
    /// switch to `count_distinct` over primary keys to stay correct.
    pub fn aggregate_wrap(&self, is_multiplied: bool) -> AggregateWrap<'_> {
        match self {
            Self::Calculated(_) => AggregateWrap::PassThrough,
            Self::Aggregated(a) => match a.agg_type() {
                AggregationType::NumberAgg => AggregateWrap::PassThrough,
                AggregationType::CountDistinctApprox => AggregateWrap::CountDistinctApprox,
                AggregationType::CountDistinct => AggregateWrap::CountDistinct,
                AggregationType::RunningTotal => AggregateWrap::Function("sum"),
                _ => AggregateWrap::Function(a.agg_type().as_str()),
            },
            Self::Count(_) => {
                if is_multiplied {
                    AggregateWrap::CountDistinct
                } else {
                    AggregateWrap::Function("count")
                }
            }
            Self::Rank => AggregateWrap::PassThrough,
        }
    }

    /// How the kind wraps its inner SQL when rolled up from a
    /// pre-aggregation. Counts and most aggregations roll up via
    /// `sum`; `min` / `max` preserve themselves; calculated string /
    /// time / boolean values roll up via `max`.
    pub fn pre_aggregate_wrap(&self) -> AggregateWrap<'_> {
        match self {
            Self::Count(_) => AggregateWrap::Function("sum"),
            Self::Aggregated(a) => match a.agg_type() {
                AggregationType::CountDistinctApprox => AggregateWrap::CountDistinctApprox,
                AggregationType::Min => AggregateWrap::Function("min"),
                AggregationType::Max => AggregateWrap::Function("max"),
                _ => AggregateWrap::Function("sum"),
            },
            Self::Calculated(c) => match c.calc_type() {
                CalculatedMeasureType::Number => AggregateWrap::Function("sum"),
                _ => AggregateWrap::Function("max"),
            },
            _ => AggregateWrap::Function("sum"),
        }
    }

    pub fn with_new_type(&self, new_type: &str) -> Result<Self, CubeError> {
        let member_sql = self.member_sql().cloned();
        let pk_sqls = match self {
            Self::Count(c) => match c.sql() {
                CountSql::Auto(pks) => pks.clone(),
                _ => vec![],
            },
            _ => vec![],
        };
        Self::from_type_str(new_type, member_sql, pk_sqls)
    }
}
