use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{evaluate_with_context, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::base_filter::FilterType;
use super::operators::comparison::{ComparisonKind, ComparisonOp};
use super::operators::date_range::{DateRangeKind, DateRangeOp};
use super::operators::date_single::{DateSingleKind, DateSingleOp};
use super::operators::equality::EqualityOp;
use super::operators::in_list::InListOp;
use super::operators::nullability::NullabilityOp;
use super::operators::rolling_window::RegularRollingWindowOp;
use super::operators::to_date_rolling_window::ToDateRollingWindowOp;
use super::operators::{FilterOperationSql, FilterSqlContext};
use super::FilterOperator;

#[derive(Clone, Debug)]
pub enum FilterOp {
    Comparison(ComparisonOp),
    DateRange(DateRangeOp),
    DateSingle(DateSingleOp),
    Equality(EqualityOp),
    InList(InListOp),
    Nullability(NullabilityOp),
    RegularRollingWindow(RegularRollingWindowOp),
    ToDateRollingWindow(ToDateRollingWindowOp),
}

#[derive(Clone)]
pub struct TypedFilter {
    #[allow(dead_code)]
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<MemberSymbol>,
    #[allow(dead_code)]
    filter_type: FilterType,
    op: FilterOp,
}

impl TypedFilter {
    pub fn builder() -> TypedFilterBuilder {
        TypedFilterBuilder::default()
    }

    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
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

        match &self.op {
            FilterOp::Comparison(op) => op.to_sql(&ctx),
            FilterOp::DateRange(op) => op.to_sql(&ctx),
            FilterOp::DateSingle(op) => op.to_sql(&ctx),
            FilterOp::Equality(op) => op.to_sql(&ctx),
            FilterOp::InList(op) => op.to_sql(&ctx),
            FilterOp::Nullability(op) => op.to_sql(&ctx),
            FilterOp::RegularRollingWindow(op) => op.to_sql(&ctx),
            FilterOp::ToDateRollingWindow(op) => op.to_sql(&ctx),
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
            _ => return Ok(None),
        };

        Ok(Some(TypedFilter {
            query_tools,
            member_evaluator,
            filter_type,
            op,
        }))
    }
}
