use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
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
use super::operators::rolling_window::RegularRollingWindowOp;
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
    ToDateRollingWindow(ToDateRollingWindowOp),
}

#[derive(Clone)]
pub struct TypedFilter {
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<MemberSymbol>,
    filter_type: FilterType,
    operator: FilterOperator,
    values: Vec<Option<String>>,
    use_raw_values: bool,
    op: FilterOp,
}

impl TypedFilter {
    pub fn builder() -> TypedFilterBuilder {
        TypedFilterBuilder::default()
    }

    pub fn to_builder(&self) -> TypedFilterBuilder {
        TypedFilter::builder()
            .query_tools(self.query_tools.clone())
            .member_evaluator(self.member_evaluator.clone())
            .filter_type(self.filter_type.clone())
            .operator(self.operator.clone())
            .values(Some(self.values.clone()))
            .use_raw_values(self.use_raw_values)
    }

    pub fn query_tools(&self) -> &Rc<QueryTools> {
        &self.query_tools
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

    pub fn values(&self) -> &Vec<Option<String>> {
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
    values: Option<Vec<Option<String>>>,
    use_raw_values: bool,
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

    pub fn values(mut self, v: Option<Vec<Option<String>>>) -> Self {
        self.values = v;
        self
    }

    fn resolve_member_type(member_evaluator: &Rc<MemberSymbol>) -> Option<String> {
        let symbol = resolve_base_symbol(member_evaluator);
        match symbol.as_ref() {
            MemberSymbol::Dimension(d) => Some(d.dimension_type().to_string()),
            MemberSymbol::Measure(m) => Some(m.measure_type().to_string()),
            _ => None,
        }
    }

    fn first_non_null_value(values: &[Option<String>]) -> Result<String, CubeError> {
        values
            .iter()
            .find_map(|v| v.as_ref().cloned())
            .ok_or_else(|| CubeError::user("Expected one parameter but nothing found".to_string()))
    }

    pub fn build(self) -> Result<TypedFilter, CubeError> {
        let query_tools = self
            .query_tools
            .ok_or_else(|| CubeError::internal("query_tools is required".to_string()))?;
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

        let member_type = Self::resolve_member_type(&member_evaluator);

        let op = match operator {
            FilterOperator::Equal | FilterOperator::NotEqual => {
                let negated = matches!(operator, FilterOperator::NotEqual);
                let has_null = values.iter().any(|v| v.is_none());
                if values.len() > 1 {
                    FilterOp::InList(InListOp::new(negated, values, member_type))
                } else if has_null {
                    // equals null → IS NULL, notEquals null → IS NOT NULL
                    FilterOp::Nullability(NullabilityOp::new(!negated))
                } else if let Some(Some(value)) = values.into_iter().next() {
                    FilterOp::Equality(EqualityOp::new(negated, value, member_type))
                } else {
                    return Err(CubeError::user(
                        "Expected at least one value for equals/notEquals filter".to_string(),
                    ));
                }
            }
            FilterOperator::In => FilterOp::InList(InListOp::new(false, values, member_type)),
            FilterOperator::NotIn => FilterOp::InList(InListOp::new(true, values, member_type)),
            FilterOperator::Gt | FilterOperator::Gte | FilterOperator::Lt | FilterOperator::Lte => {
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
                let from = Self::first_non_null_value(&values)?;
                let to = values
                    .get(1)
                    .and_then(|v| v.as_ref().cloned())
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
                let value = Self::first_non_null_value(&values)?;
                FilterOp::DateSingle(DateSingleOp::new(DateSingleKind::Before, value))
            }
            FilterOperator::BeforeOrOnDate => {
                let value = Self::first_non_null_value(&values)?;
                FilterOp::DateSingle(DateSingleOp::new(DateSingleKind::BeforeOrOn, value))
            }
            FilterOperator::AfterDate => {
                let value = Self::first_non_null_value(&values)?;
                FilterOp::DateSingle(DateSingleOp::new(DateSingleKind::After, value))
            }
            FilterOperator::AfterOrOnDate => {
                let value = Self::first_non_null_value(&values)?;
                FilterOp::DateSingle(DateSingleOp::new(DateSingleKind::AfterOrOn, value))
            }
            FilterOperator::RegularRollingWindowDateRange => {
                let trailing = values.get(2).and_then(|v| v.clone());
                let leading = values.get(3).and_then(|v| v.clone());
                FilterOp::RegularRollingWindow(RegularRollingWindowOp::new(trailing, leading))
            }
            FilterOperator::ToDateRollingWindowDateRange => {
                let granularity_name = values
                    .get(2)
                    .and_then(|v| v.as_ref())
                    .ok_or_else(|| {
                        CubeError::user(
                            "Granularity required for to_date rolling window".to_string(),
                        )
                    })?
                    .clone();

                let resolved = resolve_base_symbol(&member_evaluator);
                let evaluator_compiler_cell = query_tools.evaluator_compiler().clone();
                let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

                let granularity_obj = GranularityHelper::make_granularity_obj(
                    query_tools.cube_evaluator().clone(),
                    &mut evaluator_compiler,
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
                let has_null = values.iter().any(|v| v.is_none());
                let non_null_values: Vec<String> =
                    values.iter().filter_map(|v| v.clone()).collect();
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
        };

        Ok(TypedFilter {
            query_tools,
            member_evaluator,
            filter_type,
            operator,
            values: values_snapshot,
            use_raw_values: self.use_raw_values,
            op,
        })
    }
}
