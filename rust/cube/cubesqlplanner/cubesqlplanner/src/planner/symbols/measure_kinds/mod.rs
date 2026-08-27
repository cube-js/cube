mod aggregated;
mod calculated;
mod count;

pub use aggregated::*;
pub use calculated::*;
pub use count::*;

use super::common::AggregationType;
use super::deps::{DepVisitor, DepVisitorMut, SymbolDeps};
use crate::planner::SqlCall;
use cubenativeutils::CubeError;
use std::ops::ControlFlow;
use std::rc::Rc;

/// How a measure kind wraps its inner SQL when rendered: no wrapper
/// at all, a named SQL aggregate function, or one of the distinct-
/// count special forms. `CountDistinctApproxState` is the mergeable
/// intermediate form of `CountDistinctApprox` — an HLL state instead
/// of a cardinality.
pub enum AggregateWrap<'a> {
    PassThrough,
    Function(&'a str),
    CountDistinct,
    CountDistinctApprox,
    CountDistinctApproxState,
}

/// Form of a measure's aggregation, classified from the data-model
/// `type`.
///
/// - `Count` — `type: count`. Counts rows; falls back to the cube's
///   primary keys when no explicit `sql` is given.
/// - `Aggregated` — built-in aggregation (`sum`, `avg`, `min`, `max`,
///   `count_distinct`, `count_distinct_approx`, `number_agg`).
/// - `Calculated` — `type: number / string / time / boolean`. A
///   plain expression with no aggregation wrapper.
/// - `Rank` — `type: rank`. Window-function rank, no `sql`.
#[derive(Clone)]
pub enum MeasureKind {
    Count(CountMeasure),
    /// A `Count` that sits under a row-multiplying join and must be
    /// rendered as a distinct count to stay correct. Identical to
    /// `Count` in every respect except the final aggregate wrap.
    MultipliedCount(CountMeasure),
    Aggregated(AggregatedMeasure),
    /// An `Aggregated` that renders as a mergeable intermediate state
    /// (an HLL state for `count_distinct_approx`) instead of a final
    /// value, because an outer aggregation merges it — a rolling
    /// window over a multi-stage leaf, or a rollup read from a
    /// pre-aggregation. Constructed only via [`Self::as_state`].
    AggregatedState(AggregatedMeasure),
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

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match self {
            Self::Count(c) | Self::MultipliedCount(c) => c.iter_sql_calls(),
            Self::Aggregated(a) | Self::AggregatedState(a) => a.iter_sql_calls(),
            Self::Calculated(c) => c.iter_sql_calls(),
            Self::Rank => Box::new(std::iter::empty()),
        }
    }

    pub fn is_owned_by_cube(&self) -> bool {
        match self {
            Self::Count(c) | Self::MultipliedCount(c) => c.is_owned_by_cube(),
            Self::Aggregated(a) | Self::AggregatedState(a) => a.is_owned_by_cube(),
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
            Self::Count(_) | Self::MultipliedCount(_) => true,
            Self::Aggregated(a) | Self::AggregatedState(a) => a.agg_type().is_additive(),
            Self::Calculated(_) | Self::Rank => false,
        }
    }

    /// True if a rollup stores this kind as a mergeable sketch rather than the
    /// final value — either it already is that state form, or it is the kind
    /// that has one. Derived from [`Self::as_state`] so both answers cannot
    /// drift apart.
    pub fn is_stored_as_state(&self) -> bool {
        matches!(self, Self::AggregatedState(_)) || self.as_state().is_some()
    }

    pub fn measure_type_str(&self) -> &str {
        match self {
            Self::Count(_) | Self::MultipliedCount(_) => "count",
            Self::Aggregated(a) | Self::AggregatedState(a) => a.agg_type().as_str(),
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
                    AggregationType::NumberAgg => false,
                }
            }
            Self::Count(_)
            | Self::MultipliedCount(_)
            | Self::AggregatedState(_)
            | Self::Calculated(_)
            | Self::Rank => false,
        }
    }

    /// True if extra `measure_filters` can be merged into the kind.
    /// Counts and the basic aggregations support it; `number_agg`,
    /// calculated and rank measures do not.
    pub fn supports_additional_filters(&self) -> bool {
        match self {
            Self::Count(_) | Self::MultipliedCount(_) => true,
            Self::Aggregated(a) => matches!(
                a.agg_type(),
                AggregationType::Sum
                    | AggregationType::Avg
                    | AggregationType::Min
                    | AggregationType::Max
                    | AggregationType::CountDistinct
                    | AggregationType::CountDistinctApprox
            ),
            Self::AggregatedState(_) | Self::Calculated(_) | Self::Rank => false,
        }
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        match self {
            Self::Count(c) | Self::MultipliedCount(c) => match c.sql() {
                CountSql::Explicit(sql) => Some(sql),
                CountSql::Auto(_) => None,
            },
            Self::Aggregated(a) | Self::AggregatedState(a) => a.member_sql(),
            Self::Calculated(c) => c.member_sql(),
            Self::Rank => None,
        }
    }

    /// How the kind wraps its inner SQL when rendered as a top-level
    /// query measure. `MultipliedCount` switches to `count_distinct`
    /// over primary keys to stay correct under row multiplication.
    pub fn aggregate_wrap(&self) -> AggregateWrap<'_> {
        match self {
            Self::Calculated(_) => AggregateWrap::PassThrough,
            Self::Aggregated(a) => match a.agg_type() {
                AggregationType::NumberAgg => AggregateWrap::PassThrough,
                AggregationType::CountDistinctApprox => AggregateWrap::CountDistinctApprox,
                AggregationType::CountDistinct => AggregateWrap::CountDistinct,
                AggregationType::Sum
                | AggregationType::Avg
                | AggregationType::Min
                | AggregationType::Max => AggregateWrap::Function(a.agg_type().as_str()),
            },
            Self::AggregatedState(_) => AggregateWrap::CountDistinctApproxState,
            Self::Count(_) => AggregateWrap::Function("count"),
            Self::MultipliedCount(_) => AggregateWrap::CountDistinct,
            Self::Rank => AggregateWrap::PassThrough,
        }
    }

    /// How the kind wraps its inner SQL when rolled up from a
    /// pre-aggregation. Counts and most aggregations roll up via
    /// `sum`; `min` / `max` preserve themselves; calculated string /
    /// time / boolean values roll up via `max`.
    pub fn pre_aggregate_wrap(&self) -> AggregateWrap<'_> {
        match self {
            Self::Count(_) | Self::MultipliedCount(_) => AggregateWrap::Function("sum"),
            Self::AggregatedState(_) => AggregateWrap::CountDistinctApproxState,
            Self::Aggregated(a) => match a.agg_type() {
                AggregationType::CountDistinctApprox => AggregateWrap::CountDistinctApprox,
                AggregationType::Min => AggregateWrap::Function("min"),
                AggregationType::Max => AggregateWrap::Function("max"),
                AggregationType::Sum
                | AggregationType::Avg
                | AggregationType::CountDistinct
                | AggregationType::NumberAgg => AggregateWrap::Function("sum"),
            },
            Self::Calculated(c) => match c.calc_type() {
                CalculatedMeasureType::Number => AggregateWrap::Function("sum"),
                CalculatedMeasureType::String
                | CalculatedMeasureType::Time
                | CalculatedMeasureType::Boolean => AggregateWrap::Function("max"),
            },
            Self::Rank => AggregateWrap::Function("sum"),
        }
    }

    pub fn with_new_type(&self, new_type: &str) -> Result<Self, CubeError> {
        let member_sql = self.member_sql().cloned();
        let pk_sqls = match self {
            Self::Count(c) | Self::MultipliedCount(c) => match c.sql() {
                CountSql::Explicit(_) => vec![],
                CountSql::Auto(pks) => pks.clone(),
            },
            Self::Aggregated(_) | Self::AggregatedState(_) | Self::Calculated(_) | Self::Rank => {
                vec![]
            }
        };
        Self::from_type_str(new_type, member_sql, pk_sqls)
    }

    /// Render form when this kind sits under a row-multiplying join:
    /// a `Count` becomes a distinct `MultipliedCount`; every other
    /// kind is unchanged (only counts switch wrap under multiplication).
    pub fn into_multiplied(&self) -> Self {
        match self {
            Self::Count(c) => Self::MultipliedCount(c.clone()),
            Self::MultipliedCount(_)
            | Self::Aggregated(_)
            | Self::AggregatedState(_)
            | Self::Calculated(_)
            | Self::Rank => self.clone(),
        }
    }

    /// `Some(render form)` when the kind's aggregation can render as a
    /// mergeable intermediate state for an outer aggregation to merge:
    /// only `count_distinct_approx` has such a form (an HLL state).
    /// `None` when the kind has no state form or is already in it.
    pub fn as_state(&self) -> Option<Self> {
        match self {
            Self::Aggregated(a) if a.agg_type() == AggregationType::CountDistinctApprox => {
                Some(Self::AggregatedState(a.clone()))
            }
            Self::Count(_)
            | Self::MultipliedCount(_)
            | Self::Aggregated(_)
            | Self::AggregatedState(_)
            | Self::Calculated(_)
            | Self::Rank => None,
        }
    }

    /// `Some(render form)` when this kind, under a row-multiplying join,
    /// is still safe to compute directly in the main query: a key-based
    /// count rolls up as a distinct `MultipliedCount`, distinct
    /// aggregations are already immune and stay as-is. `None` otherwise.
    pub fn regular_in_multiplied(&self) -> Option<Self> {
        match self {
            Self::Count(c) if c.convertible_to_distinct() => Some(Self::MultipliedCount(c.clone())),
            Self::Aggregated(a) | Self::AggregatedState(a) if a.agg_type().is_distinct() => {
                Some(self.clone())
            }
            Self::Count(_)
            | Self::MultipliedCount(_)
            | Self::Aggregated(_)
            | Self::AggregatedState(_)
            | Self::Calculated(_)
            | Self::Rank => None,
        }
    }
}

impl SymbolDeps for MeasureKind {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        match self {
            Self::Count(c) | Self::MultipliedCount(c) => c.visit_deps(visitor),
            Self::Aggregated(a) | Self::AggregatedState(a) => a.visit_deps(visitor),
            Self::Calculated(c) => c.visit_deps(visitor),
            Self::Rank => ControlFlow::Continue(()),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Self::Count(c) | Self::MultipliedCount(c) => c.visit_deps_mut(visitor),
            Self::Aggregated(a) | Self::AggregatedState(a) => a.visit_deps_mut(visitor),
            Self::Calculated(c) => c.visit_deps_mut(visitor),
            Self::Rank => Ok(()),
        }
    }
}
