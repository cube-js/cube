use crate::cube_bridge::base_query_options::FilterValue;
use crate::planner::query_tools::QueryTools;
use crate::planner::Compiler;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::base_filter::FilterType;
use super::operators::comparison::{ComparisonKind, ComparisonOp};
use super::operators::date_range::{DateRangeKind, DateRangeOp};
use super::operators::date_single::{DateSingleKind, DateSingleOp};
use super::operators::equality::EqualityOp;
use super::operators::in_list::InListOp;
use super::operators::like::LikeOp;
use super::operators::measure_filter::MeasureFilterOp;
use super::operators::nullability::NullabilityOp;
use super::operators::rolling_window::{RegularRollingWindowOp, RollingWindowOffsetOp};
use super::operators::to_date_rolling_window::ToDateRollingWindowOp;
use super::FilterOperator;
use crate::planner::GranularityHelper;

/// Resolves TimeDimension to its base dimension symbol; returns as-is for other kinds.
pub fn resolve_base_symbol(symbol: &Rc<MemberSymbol>) -> Rc<MemberSymbol> {
    if let Ok(td) = symbol.as_time_dimension() {
        td.base_symbol().clone()
    } else {
        symbol.clone()
    }
}

/// Typed, ready-to-render filter operation. Decoded once at filter
/// construction from a `FilterOperator` plus its value list, each
/// variant carrying exactly the parameters its rendering needs
/// (compared value, date bounds, granularity, etc.).
#[derive(Clone, Debug)]
pub enum FilterOp {
    Comparison(ComparisonOp),
    DateRange(DateRangeOp),
    DateSingle(DateSingleOp),
    Equality(EqualityOp),
    InList(InListOp),
    Like(LikeOp),
    MeasureFilter(MeasureFilterOp),
    Nullability(NullabilityOp),
    RegularRollingWindow(RegularRollingWindowOp),
    RollingWindowOffset(RollingWindowOffsetOp),
    ToDateRollingWindow(ToDateRollingWindowOp),
}

/// Filter bound to a member and decoded into a typed `FilterOp`.
/// Carries both the raw operator + value list (the form that came
/// from the data model) and the decoded form, so renderers can pick
/// whichever view they need.
#[derive(Clone)]
pub struct TypedFilter {
    member_evaluator: Rc<MemberSymbol>,
    filter_type: FilterType,
    operator: FilterOperator,
    values: Vec<FilterValue>,
    use_raw_values: bool,
    op: FilterOp,
}

impl TypedFilter {
    pub fn builder() -> TypedFilterBuilder {
        TypedFilterBuilder::default()
    }

    pub fn to_builder(&self) -> TypedFilterBuilder {
        TypedFilter::builder()
            .member_evaluator(self.member_evaluator.clone())
            .filter_type(self.filter_type.clone())
            .operator(self.operator.clone())
            .values(Some(self.values.clone()))
            .use_raw_values(self.use_raw_values)
    }

    pub fn member_evaluator(&self) -> &Rc<MemberSymbol> {
        &self.member_evaluator
    }

    pub fn filter_type(&self) -> &FilterType {
        &self.filter_type
    }

    pub fn operator(&self) -> &FilterOperator {
        &self.operator
    }

    pub fn values(&self) -> &Vec<FilterValue> {
        &self.values
    }

    pub fn operation(&self) -> &FilterOp {
        &self.op
    }

    pub fn use_raw_values(&self) -> bool {
        self.use_raw_values
    }
}

#[derive(Default)]
pub struct TypedFilterBuilder {
    query_tools: Option<Rc<QueryTools>>,
    member_evaluator: Option<Rc<MemberSymbol>>,
    filter_type: Option<FilterType>,
    operator: Option<FilterOperator>,
    values: Option<Vec<FilterValue>>,
    use_raw_values: bool,
    /// Pre-computed operation carried over from an existing filter. When set,
    /// `build` reuses it instead of recomputing from operator/values — which is
    /// what lets a member-only rewrite (`with_member_evaluator`) avoid touching
    /// the Compiler for a to_date rolling-window granularity.
    op: Option<FilterOp>,
}

impl TypedFilterBuilder {
    pub fn query_tools(mut self, v: Rc<QueryTools>) -> Self {
        self.query_tools = Some(v);
        self
    }

    pub fn member_evaluator(mut self, v: Rc<MemberSymbol>) -> Self {
        self.member_evaluator = Some(v);
        self
    }

    pub fn filter_type(mut self, v: FilterType) -> Self {
        self.filter_type = Some(v);
        self
    }

    pub fn operator(mut self, v: FilterOperator) -> Self {
        self.operator = Some(v);
        self
    }

    pub fn use_raw_values(mut self, v: bool) -> Self {
        self.use_raw_values = v;
        self
    }

    /// Carries an already-computed `FilterOp` so `build` skips recomputation
    /// (and thus the Compiler dependency). Valid only when operator and values
    /// are unchanged — i.e. a pure member swap.
    pub fn carry_op(mut self, op: FilterOp) -> Self {
        self.op = Some(op);
        self
    }

    pub fn values(mut self, v: Option<Vec<FilterValue>>) -> Self {
        self.values = v;
        self
    }

    fn resolve_member_type(member_evaluator: &Rc<MemberSymbol>) -> Option<String> {
        let symbol = resolve_base_symbol(member_evaluator);
        match symbol.as_ref() {
            MemberSymbol::Dimension(d) => Some(d.dimension_type().to_string()),
            // The cast type drives how a bound comparison value is wrapped.
            // Aggregations (count, sum, ...) and number measures compare as
            // numbers. String/time measures are non-numeric scalars and must
            // not be coerced to a number; date comparisons take the dedicated
            // date operators instead. min/max carry their operand type, which
            // isn't known here, so they fall through to the numeric default.
            MemberSymbol::Measure(m) => match m.measure_type() {
                "boolean" => Some("boolean".to_string()),
                "string" | "time" => None,
                _ => Some("number".to_string()),
            },
            _ => None,
        }
    }

    fn first_non_null_value(values: &[FilterValue]) -> Result<FilterValue, CubeError> {
        values
            .iter()
            .find(|v| !v.is_null())
            .cloned()
            .ok_or_else(|| CubeError::user("Expected one parameter but nothing found".to_string()))
    }

    /// First non-null value rendered as its parameter string. Used by the
    /// date/interval operators, which operate on the string form.
    fn first_non_null_string(values: &[FilterValue]) -> Result<String, CubeError> {
        // `first_non_null_value` already rejects an all-null/empty list, and a
        // non-null `FilterValue` always renders to `Some`, so the fallback here
        // is unreachable.
        Ok(Self::first_non_null_value(values)?
            .to_param_string()
            .unwrap_or_default())
    }

    // FIXME: late compilation. `compiler` and the builder's `query_tools` are
    // consumed only to (re)compile a custom rolling-window granularity during
    // planning (the ToDateRollingWindowDateRange branch below); neither is
    // stored on the built filter. Once granularities are resolved in an early
    // compile phase, both inputs should go away.
    pub fn build(self, compiler: Option<&mut Compiler>) -> Result<TypedFilter, CubeError> {
        let query_tools = self.query_tools;
        let member_evaluator = self
            .member_evaluator
            .ok_or_else(|| CubeError::internal("member_evaluator is required".to_string()))?;
        let filter_type = self
            .filter_type
            .ok_or_else(|| CubeError::internal("filter_type is required".to_string()))?;
        let operator = self
            .operator
            .ok_or_else(|| CubeError::internal("operator is required".to_string()))?;
        let values = self.values.unwrap_or_default();
        let values_snapshot = values.clone();

        let op = if let Some(op) = self.op {
            op
        } else {
            let member_type = Self::resolve_member_type(&member_evaluator);
            match operator {
                FilterOperator::Equal | FilterOperator::NotEqual => {
                    let negated = matches!(operator, FilterOperator::NotEqual);
                    let has_null = values.iter().any(|v| v.is_null());
                    if values.len() > 1 {
                        FilterOp::InList(InListOp::new(negated, values, member_type))
                    } else if has_null {
                        // equals null → IS NULL, notEquals null → IS NOT NULL
                        FilterOp::Nullability(NullabilityOp::new(!negated))
                    } else if let Some(value) = values.into_iter().next() {
                        FilterOp::Equality(EqualityOp::new(negated, value, member_type))
                    } else {
                        return Err(CubeError::user(
                            "Expected at least one value for equals/notEquals filter".to_string(),
                        ));
                    }
                }
                FilterOperator::In => FilterOp::InList(InListOp::new(false, values, member_type)),
                FilterOperator::NotIn => FilterOp::InList(InListOp::new(true, values, member_type)),
                FilterOperator::Gt
                | FilterOperator::Gte
                | FilterOperator::Lt
                | FilterOperator::Lte => {
                    let kind = match operator {
                        FilterOperator::Gt => ComparisonKind::Gt,
                        FilterOperator::Gte => ComparisonKind::Gte,
                        FilterOperator::Lt => ComparisonKind::Lt,
                        FilterOperator::Lte => ComparisonKind::Lte,
                        _ => unreachable!(),
                    };
                    let value = Self::first_non_null_value(&values)?;
                    FilterOp::Comparison(ComparisonOp::new(kind, value, member_type))
                }
                FilterOperator::Set => FilterOp::Nullability(NullabilityOp::new(false)),
                FilterOperator::NotSet => FilterOp::Nullability(NullabilityOp::new(true)),
                FilterOperator::InDateRange | FilterOperator::NotInDateRange => {
                    let from = Self::first_non_null_string(&values)?;
                    let to = values
                        .get(1)
                        .and_then(|v| v.to_param_string())
                        .ok_or_else(|| {
                            CubeError::user("2 arguments expected for date range".to_string())
                        })?;
                    let kind = if matches!(operator, FilterOperator::InDateRange) {
                        DateRangeKind::InRange
                    } else {
                        DateRangeKind::NotInRange
                    };
                    FilterOp::DateRange(DateRangeOp::new(kind, from, to))
                }
                FilterOperator::BeforeDate => {
                    let value = Self::first_non_null_string(&values)?;
                    FilterOp::DateSingle(DateSingleOp::new(DateSingleKind::Before, value))
                }
                FilterOperator::BeforeOrOnDate => {
                    let value = Self::first_non_null_string(&values)?;
                    FilterOp::DateSingle(DateSingleOp::new(DateSingleKind::BeforeOrOn, value))
                }
                FilterOperator::AfterDate => {
                    let value = Self::first_non_null_string(&values)?;
                    FilterOp::DateSingle(DateSingleOp::new(DateSingleKind::After, value))
                }
                FilterOperator::AfterOrOnDate => {
                    let value = Self::first_non_null_string(&values)?;
                    FilterOp::DateSingle(DateSingleOp::new(DateSingleKind::AfterOrOn, value))
                }
                FilterOperator::RegularRollingWindowDateRange => {
                    let trailing = values.get(2).and_then(|v| v.to_param_string());
                    let leading = values.get(3).and_then(|v| v.to_param_string());
                    FilterOp::RegularRollingWindow(RegularRollingWindowOp::new(trailing, leading))
                }
                FilterOperator::RollingWindowOffsetDateRange => {
                    let from = values.first().and_then(|v| v.to_param_string());
                    let to = values.get(1).and_then(|v| v.to_param_string());
                    let trailing = values.get(2).and_then(|v| v.to_param_string());
                    let leading = values.get(3).and_then(|v| v.to_param_string());
                    let offset = values
                        .get(4)
                        .and_then(|v| v.to_param_string())
                        .unwrap_or_else(|| "end".to_string());
                    FilterOp::RollingWindowOffset(RollingWindowOffsetOp::new(
                        from, to, trailing, leading, offset,
                    ))
                }
                FilterOperator::ToDateRollingWindowDateRange => {
                    let granularity_name = values
                        .get(2)
                        .and_then(|v| v.to_param_string())
                        .ok_or_else(|| {
                            CubeError::user(
                                "Granularity required for to_date rolling window".to_string(),
                            )
                        })?;

                    let resolved = resolve_base_symbol(&member_evaluator);
                    let evaluator_compiler = compiler.ok_or_else(|| {
                        CubeError::internal(
                            "Compiler is required to resolve a to_date rolling-window granularity"
                                .to_string(),
                        )
                    })?;
                    let query_tools = query_tools.as_ref().ok_or_else(|| {
                        CubeError::internal(
                            "query_tools is required to resolve a to_date rolling-window granularity"
                                .to_string(),
                        )
                    })?;

                    let granularity_obj = GranularityHelper::make_granularity_obj(
                        query_tools.cube_evaluator().clone(),
                        evaluator_compiler,
                        &resolved.cube_name(),
                        &resolved.name(),
                        Some(granularity_name.clone()),
                    )?
                    .ok_or_else(|| {
                        CubeError::internal(format!(
                            "Rolling window granularity '{}' is not found in time dimension '{}'",
                            granularity_name,
                            resolved.name()
                        ))
                    })?;

                    FilterOp::ToDateRollingWindow(ToDateRollingWindowOp::new(granularity_obj))
                }
                FilterOperator::Contains
                | FilterOperator::NotContains
                | FilterOperator::StartsWith
                | FilterOperator::NotStartsWith
                | FilterOperator::EndsWith
                | FilterOperator::NotEndsWith => {
                    let has_null = values.iter().any(|v| v.is_null());
                    let non_null_values: Vec<String> =
                        values.iter().filter_map(|v| v.to_param_string()).collect();
                    let (negated, start_wild, end_wild) = match operator {
                        FilterOperator::Contains => (false, true, true),
                        FilterOperator::NotContains => (true, true, true),
                        FilterOperator::StartsWith => (false, false, true),
                        FilterOperator::NotStartsWith => (true, false, true),
                        FilterOperator::EndsWith => (false, true, false),
                        FilterOperator::NotEndsWith => (true, true, false),
                        _ => unreachable!(),
                    };
                    FilterOp::Like(LikeOp::new(
                        negated,
                        start_wild,
                        end_wild,
                        non_null_values,
                        has_null,
                        member_type,
                    ))
                }
                FilterOperator::MeasureFilter => FilterOp::MeasureFilter(MeasureFilterOp::new()),
            }
        };

        Ok(TypedFilter {
            member_evaluator,
            filter_type,
            operator,
            values: values_snapshot,
            use_raw_values: self.use_raw_values,
            op,
        })
    }
}
