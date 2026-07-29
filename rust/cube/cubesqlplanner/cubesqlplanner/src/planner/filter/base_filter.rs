use super::filter_operator::FilterOperator;
use super::typed_filter::{resolve_base_symbol, TypedFilter};
use crate::cube_bridge::base_query_options::FilterValue;
use crate::planner::Compiler;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// Classifies a filter by the kind of member it targets. Drives where
/// the filter is placed when the query is rendered (WHERE vs HAVING).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterType {
    Dimension,
    Measure,
}

/// Filter on a single member; thin wrapper over `TypedFilter`.
// TODO: temporary compatibility proxy — collapse into TypedFilter
// and update FilterItem consumers.
#[derive(Clone)]
pub struct BaseFilter {
    typed_filter: TypedFilter,
}

impl PartialEq for BaseFilter {
    fn eq(&self, other: &Self) -> bool {
        self.typed_filter.filter_type() == other.typed_filter.filter_type()
            && self.typed_filter.operator() == other.typed_filter.operator()
            && self.typed_filter.values() == other.typed_filter.values()
    }
}

impl BaseFilter {
    // FIXME: late compilation. `compiler` is threaded through purely so a
    // to_date rolling-window granularity can be (re)compiled while the filter
    // is built during planning. With early compilation this disappears.
    pub fn try_new(
        query_tools: Rc<crate::planner::query_tools::QueryTools>,
        member_evaluator: Rc<MemberSymbol>,
        filter_type: FilterType,
        filter_operator: FilterOperator,
        values: Option<Vec<FilterValue>>,
        compiler: Option<&mut Compiler>,
    ) -> Result<Rc<Self>, CubeError> {
        let typed_filter = TypedFilter::builder()
            .query_tools(query_tools)
            .member_evaluator(member_evaluator)
            .filter_type(filter_type)
            .operator(filter_operator)
            .values(values)
            .build(compiler)?;

        Ok(Rc::new(Self { typed_filter }))
    }

    // FIXME: late compilation — see `try_new`. `compiler` only feeds the
    // rolling-window granularity recompute.
    pub fn change_operator(
        &self,
        filter_operator: FilterOperator,
        values: Vec<FilterValue>,
        use_raw_values: bool,
        query_tools: Rc<crate::planner::query_tools::QueryTools>,
        compiler: Option<&mut Compiler>,
    ) -> Result<Rc<Self>, CubeError> {
        let typed_filter = self
            .typed_filter
            .to_builder()
            .query_tools(query_tools)
            .operator(filter_operator)
            .values(Some(values))
            .use_raw_values(use_raw_values)
            .build(compiler)?;

        Ok(Rc::new(Self { typed_filter }))
    }

    /// Member this filter applies to, with `TimeDimension` wrappers
    /// peeled off to the underlying base dimension. Use
    /// `raw_member_evaluator` to keep the wrapper.
    pub fn member_evaluator(&self) -> Rc<MemberSymbol> {
        resolve_base_symbol(self.typed_filter.member_evaluator())
    }

    /// Member this filter applies to, exactly as it was given —
    /// `TimeDimension` wrappers are kept. See `member_evaluator`
    /// for the unwrapped form.
    pub fn raw_member_evaluator(&self) -> Rc<MemberSymbol> {
        self.typed_filter.member_evaluator().clone()
    }

    pub fn raw_member_evaluator_ref(&self) -> &Rc<MemberSymbol> {
        self.typed_filter.member_evaluator()
    }

    pub fn with_member_evaluator(
        &self,
        member_evaluator: Rc<MemberSymbol>,
    ) -> Result<Rc<Self>, CubeError> {
        // No compiler here (called from static-filter symbol rewriting, which
        // has none). A member swap keeps the same operator/values, so the only
        // branch that would need a compiler is a to_date rolling window — not
        // reachable from this path. FIXME: removed once granularities are
        // resolved during early compilation rather than at filter-build time.
        let typed_filter = self
            .typed_filter
            .to_builder()
            .member_evaluator(member_evaluator)
            // A member swap leaves operator/values unchanged, so the operation
            // is identical — carry it over so build() needs no Compiler.
            .carry_op(self.typed_filter.operation().clone())
            .build(None)?;

        Ok(Rc::new(Self { typed_filter }))
    }

    /// The filtered member as a time-dimension symbol when it is one;
    /// `None` otherwise.
    pub fn time_dimension_symbol(&self) -> Option<Rc<MemberSymbol>> {
        if self
            .typed_filter
            .member_evaluator()
            .as_time_dimension()
            .is_ok()
        {
            Some(self.typed_filter.member_evaluator().clone())
        } else {
            None
        }
    }

    pub fn values(&self) -> &Vec<FilterValue> {
        self.typed_filter.values()
    }

    /// Raw filter operator enum, matching the value declared in the
    /// data model (`equals`, `in`, `inDateRange`, ...). See
    /// `operation` for the decoded form ready for rendering.
    pub fn filter_operator(&self) -> &FilterOperator {
        self.typed_filter.operator()
    }

    /// Decoded, typed form of the filter operation, ready for
    /// rendering. See `filter_operator` for the raw enum.
    pub fn operation(&self) -> &super::typed_filter::FilterOp {
        self.typed_filter.operation()
    }

    pub fn use_raw_values(&self) -> bool {
        self.typed_filter.use_raw_values()
    }

    pub fn typed_filter(&self) -> &TypedFilter {
        &self.typed_filter
    }

    pub fn member_name(&self) -> String {
        self.member_evaluator().full_name()
    }

    /// True when the filter compares its member to exactly one value
    /// with the `Equal` operator.
    pub fn is_single_value_equal(&self) -> bool {
        self.typed_filter.values().len() == 1
            && *self.typed_filter.operator() == FilterOperator::Equal
    }

    /// Concrete allowed values when the operator is `In` or `Equal`,
    /// otherwise `None`. `NULL`s in the value list are discarded.
    pub fn get_value_restrictions(&self) -> Option<Vec<String>> {
        if *self.typed_filter.operator() == FilterOperator::In
            || *self.typed_filter.operator() == FilterOperator::Equal
        {
            Some(
                self.typed_filter
                    .values()
                    .iter()
                    .filter_map(|v| v.to_param_string())
                    .collect_vec(),
            )
        } else {
            None
        }
    }
}
