use crate::cube_bridge::member_sql::FilterParamsColumn;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{evaluate_with_context, FiltersContext, VisitorContext};
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
use super::operators::{FilterOperationSql, FilterSqlContext};
use super::FilterOperator;
use crate::planner::GranularityHelper;

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

    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let FilterOp::MeasureFilter(op) = &self.op {
            return op.to_sql(
                &self.member_evaluator,
                &self.query_tools,
                &context,
                plan_templates,
            );
        }

        let resolved = if let Ok(td) = self.member_evaluator.as_time_dimension() {
            td.base_symbol().clone()
        } else {
            self.member_evaluator.clone()
        };
        let member_sql = evaluate_with_context(&resolved, context.clone(), plan_templates)?;

        let filters_context = context.filters_context();
        let ctx = FilterSqlContext {
            member_sql: &member_sql,
            query_tools: &self.query_tools,
            plan_templates,
            use_db_time_zone: !filters_context.use_local_tz,
        };

        self.dispatch_to_sql(&ctx)
    }

    pub fn to_sql_for_filter_params(
        &self,
        column: &FilterParamsColumn,
        plan_templates: &PlanSqlTemplates,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;

        match column {
            FilterParamsColumn::String(column_sql) => {
                let ctx = FilterSqlContext {
                    member_sql: column_sql,
                    query_tools: &self.query_tools,
                    plan_templates,
                    use_db_time_zone,
                };
                self.dispatch_to_sql(&ctx)
            }
            FilterParamsColumn::Callback(callback) => {
                let args = match &self.op {
                    FilterOp::DateRange(_) | FilterOp::DateSingle(_) => {
                        let ctx = FilterSqlContext {
                            member_sql: "",
                            query_tools: &self.query_tools,
                            plan_templates,
                            use_db_time_zone,
                        };
                        let from = self
                            .values
                            .first()
                            .and_then(|v| v.as_ref())
                            .map(|v| ctx.format_and_allocate_from_date(v))
                            .transpose()?;
                        let to = self
                            .values
                            .get(1)
                            .and_then(|v| v.as_ref())
                            .map(|v| ctx.format_and_allocate_to_date(v))
                            .transpose()?;
                        [from, to].into_iter().flatten().collect()
                    }
                    _ => self
                        .values
                        .iter()
                        .filter_map(|v| {
                            v.as_ref()
                                .map(|v| self.query_tools.allocate_param(v))
                        })
                        .collect::<Vec<_>>(),
                };
                callback.call(&args)
            }
        }
    }

    fn dispatch_to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        match &self.op {
            FilterOp::Comparison(op) => op.to_sql(ctx),
            FilterOp::DateRange(op) => op.to_sql(ctx),
            FilterOp::DateSingle(op) => op.to_sql(ctx),
            FilterOp::Equality(op) => op.to_sql(ctx),
            FilterOp::InList(op) => op.to_sql(ctx),
            FilterOp::Like(op) => op.to_sql(ctx),
            FilterOp::MeasureFilter(_) => {
                unreachable!("MeasureFilter is handled in TypedFilter::to_sql")
            }
            FilterOp::Nullability(op) => op.to_sql(ctx),
            FilterOp::RegularRollingWindow(op) => op.to_sql(ctx),
            FilterOp::ToDateRollingWindow(op) => op.to_sql(ctx),
        }
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

    fn resolve_member_evaluator(member_evaluator: &Rc<MemberSymbol>) -> Rc<MemberSymbol> {
        if let Ok(td) = member_evaluator.as_time_dimension() {
            td.base_symbol().clone()
        } else {
            member_evaluator.clone()
        }
    }

    fn resolve_member_type(member_evaluator: &Rc<MemberSymbol>) -> Option<String> {
        let symbol = if let Ok(td) = member_evaluator.as_time_dimension() {
            td.base_symbol().clone()
        } else {
            member_evaluator.clone()
        };
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
            .ok_or_else(|| {
                CubeError::user("Expected one parameter but nothing found".to_string())
            })
    }

    // FIXME: return TypedFilter directly once all operators are migrated from BaseFilter
    pub fn build(self) -> Result<Option<TypedFilter>, CubeError> {
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
                        CubeError::user(
                            "2 arguments expected for date range".to_string(),
                        )
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

                let resolved = Self::resolve_member_evaluator(&member_evaluator);
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
            FilterOperator::Contains => {
                let non_null_values: Vec<String> =
                    values.iter().filter_map(|v| v.clone()).collect();
                FilterOp::Like(LikeOp::new(false, true, true, non_null_values, member_type))
            }
            FilterOperator::NotContains => {
                let non_null_values: Vec<String> =
                    values.iter().filter_map(|v| v.clone()).collect();
                FilterOp::Like(LikeOp::new(true, true, true, non_null_values, member_type))
            }
            FilterOperator::StartsWith => {
                let non_null_values: Vec<String> =
                    values.iter().filter_map(|v| v.clone()).collect();
                FilterOp::Like(LikeOp::new(false, false, true, non_null_values, member_type))
            }
            FilterOperator::NotStartsWith => {
                let non_null_values: Vec<String> =
                    values.iter().filter_map(|v| v.clone()).collect();
                FilterOp::Like(LikeOp::new(true, false, true, non_null_values, member_type))
            }
            FilterOperator::EndsWith => {
                let non_null_values: Vec<String> =
                    values.iter().filter_map(|v| v.clone()).collect();
                FilterOp::Like(LikeOp::new(false, true, false, non_null_values, member_type))
            }
            FilterOperator::NotEndsWith => {
                let non_null_values: Vec<String> =
                    values.iter().filter_map(|v| v.clone()).collect();
                FilterOp::Like(LikeOp::new(true, true, false, non_null_values, member_type))
            }
            FilterOperator::MeasureFilter => FilterOp::MeasureFilter(MeasureFilterOp::new()),
        };

        Ok(Some(TypedFilter {
            query_tools,
            member_evaluator,
            filter_type,
            operator,
            values: values_snapshot,
            use_raw_values: self.use_raw_values,
            op,
        }))
    }
}
