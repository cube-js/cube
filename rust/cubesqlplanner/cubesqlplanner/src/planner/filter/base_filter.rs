use super::filter_operator::FilterOperator;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::filter::FilterTemplates;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::QueryDateTimeHelper;
use crate::planner::{evaluate_with_context, FiltersContext, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

const FROM_PARTITION_RANGE: &'static str = "__FROM_PARTITION_RANGE";

const TO_PARTITION_RANGE: &'static str = "__TO_PARTITION_RANGE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterType {
    Dimension,
    Measure,
}

pub struct BaseFilter {
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<MemberSymbol>,
    #[allow(dead_code)]
    filter_type: FilterType,
    filter_operator: FilterOperator,
    values: Vec<Option<String>>,
    use_raw_values: bool,
    templates: FilterTemplates,
}

impl PartialEq for BaseFilter {
    fn eq(&self, other: &Self) -> bool {
        self.filter_type == other.filter_type
            && self.filter_operator == other.filter_operator
            && self.values == other.values
    }
}

impl BaseFilter {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<MemberSymbol>,
        filter_type: FilterType,
        filter_operator: FilterOperator,
        values: Option<Vec<Option<String>>>,
    ) -> Result<Rc<Self>, CubeError> {
        let templates = FilterTemplates::new(query_tools.templates_render());
        let values = if let Some(values) = values {
            values
        } else {
            vec![]
        };
        Ok(Rc::new(Self {
            query_tools,
            member_evaluator,
            filter_type,
            filter_operator,
            values,
            templates,
            use_raw_values: false,
        }))
    }

    pub fn change_operator(
        &self,
        filter_operator: FilterOperator,
        values: Vec<Option<String>>,
        use_raw_values: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            query_tools: self.query_tools.clone(),
            member_evaluator: self.member_evaluator.clone(),
            filter_type: self.filter_type.clone(),
            filter_operator,
            values,
            templates: self.templates.clone(),
            use_raw_values,
        })
    }

    pub fn member_evaluator(&self) -> &Rc<MemberSymbol> {
        if let Ok(time_dimension) = self.member_evaluator.as_time_dimension() {
            time_dimension.base_symbol()
        } else {
            &self.member_evaluator
        }
    }

    //FIXME Not very good solution, but suitable for check time dimension filters in pre-aggregations
    pub fn time_dimension_symbol(&self) -> Option<Rc<MemberSymbol>> {
        if self.member_evaluator.as_time_dimension().is_ok() {
            Some(self.member_evaluator.clone())
        } else {
            None
        }
    }

    pub fn values(&self) -> &Vec<Option<String>> {
        &self.values
    }

    pub fn filter_operator(&self) -> &FilterOperator {
        &self.filter_operator
    }

    pub fn use_raw_values(&self) -> bool {
        self.use_raw_values
    }

    pub fn member_name(&self) -> String {
        self.member_evaluator().full_name()
    }

    pub fn is_single_value_equal(&self) -> bool {
        self.values.len() == 1 && self.filter_operator == FilterOperator::Equal
    }

    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if matches!(self.filter_operator, FilterOperator::MeasureFilter) {
            self.measure_filter_where(context, plan_templates)
        } else {
            let symbol = self.member_evaluator();
            let member_sql = evaluate_with_context(
                &symbol,
                self.query_tools.clone(),
                context.clone(),
                plan_templates,
            )?;
            let filters_context = context.filters_context();

            let res = match self.filter_operator {
                FilterOperator::Equal => self.equals_where(&member_sql, filters_context)?,
                FilterOperator::NotEqual => self.not_equals_where(&member_sql, filters_context)?,
                FilterOperator::InDateRange => self.in_date_range(&member_sql, filters_context)?,
                FilterOperator::BeforeDate => self.before_date(&member_sql, filters_context)?,
                FilterOperator::BeforeOrOnDate => {
                    self.before_or_on_date(&member_sql, filters_context)?
                }
                FilterOperator::AfterDate => self.after_date(&member_sql, filters_context)?,
                FilterOperator::AfterOrOnDate => {
                    self.after_or_on_date(&member_sql, filters_context)?
                }
                FilterOperator::NotInDateRange => {
                    self.not_in_date_range(&member_sql, filters_context)?
                }
                FilterOperator::RegularRollingWindowDateRange => {
                    self.regular_rolling_window_date_range(&member_sql, filters_context)?
                }
                FilterOperator::ToDateRollingWindowDateRange => {
                    self.to_date_rolling_window_date_range(&member_sql, filters_context)?
                }
                FilterOperator::In => self.in_where(&member_sql, filters_context)?,
                FilterOperator::NotIn => self.not_in_where(&member_sql, filters_context)?,
                FilterOperator::Set => self.set_where(&member_sql, filters_context)?,
                FilterOperator::NotSet => self.not_set_where(&member_sql, filters_context)?,
                FilterOperator::Gt => self.gt_where(&member_sql, filters_context)?,
                FilterOperator::Gte => self.gte_where(&member_sql, filters_context)?,
                FilterOperator::Lt => self.lt_where(&member_sql, filters_context)?,
                FilterOperator::Lte => self.lte_where(&member_sql, filters_context)?,
                FilterOperator::Contains => self.contains_where(&member_sql, filters_context)?,
                FilterOperator::NotContains => {
                    self.not_contains_where(&member_sql, filters_context)?
                }
                FilterOperator::StartsWith => {
                    self.starts_with_where(&member_sql, filters_context)?
                }
                FilterOperator::NotStartsWith => {
                    self.not_starts_with_where(&member_sql, filters_context)?
                }
                FilterOperator::EndsWith => self.ends_with_where(&member_sql, filters_context)?,
                FilterOperator::NotEndsWith => {
                    self.not_ends_with_where(&member_sql, filters_context)?
                }
                FilterOperator::MeasureFilter => {
                    return Err(CubeError::internal(format!(
                        "Measure filter should be processed separately"
                    )));
                }
            };
            Ok(res)
        }
    }

    fn measure_filter_where(
        &self,
        context: Rc<VisitorContext>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match self.member_evaluator.as_ref() {
            MemberSymbol::Measure(measure_symbol) => {
                if measure_symbol.measure_filters().is_empty()
                    && measure_symbol.measure_drill_filters().is_empty()
                {
                    self.templates.always_true()?
                } else {
                    let visitor = context.make_visitor(self.query_tools.clone());
                    let node_processor = context.node_processor();

                    measure_symbol
                        .measure_filters()
                        .iter()
                        .chain(measure_symbol.measure_drill_filters().iter())
                        .map(|filter| -> Result<String, CubeError> {
                            Ok(format!(
                                "({})",
                                filter.eval(
                                    &visitor,
                                    node_processor.clone(),
                                    self.query_tools.clone(),
                                    plan_templates
                                )?
                            ))
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" AND ")
                }
            }
            _ => self.templates.always_true()?,
        };
        Ok(res)
    }

    fn equals_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let need_null_check = self.is_need_null_chek(false);
        if self.is_array_value() {
            self.templates.in_where(
                member_sql.to_string(),
                self.filter_and_allocate_values(),
                need_null_check,
            )
        } else if self.is_values_contains_null() {
            self.templates.not_set_where(member_sql.to_string())
        } else {
            self.templates
                .equals(member_sql.to_string(), self.first_param()?, need_null_check)
        }
    }

    fn not_equals_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let need_null_check = self.is_need_null_chek(true);
        if self.is_array_value() {
            self.templates.not_in_where(
                member_sql.to_string(),
                self.filter_and_allocate_values(),
                need_null_check,
            )
        } else if self.is_values_contains_null() {
            self.templates.set_where(member_sql.to_string())
        } else {
            self.templates
                .not_equals(member_sql.to_string(), self.first_param()?, need_null_check)
        }
    }

    fn in_date_range(
        &self,
        member_sql: &str,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;
        let (from, to) = self.allocate_date_params(use_db_time_zone, false)?;
        self.templates
            .time_range_filter(member_sql.to_string(), from, to)
    }

    fn not_in_date_range(
        &self,
        member_sql: &str,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;
        let (from, to) = self.allocate_date_params(use_db_time_zone, false)?;
        self.templates
            .time_not_in_range_filter(member_sql.to_string(), from, to)
    }

    fn before_date(
        &self,
        member_sql: &str,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;
        let value = self.first_timestamp_param(use_db_time_zone, false)?;

        self.templates.lt(member_sql.to_string(), value)
    }

    fn before_or_on_date(
        &self,
        member_sql: &str,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;
        let value = self.first_timestamp_param(use_db_time_zone, false)?;

        self.templates.lte(member_sql.to_string(), value)
    }

    fn after_date(
        &self,
        member_sql: &str,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;
        let value = self.first_timestamp_param(use_db_time_zone, false)?;

        self.templates.gt(member_sql.to_string(), value)
    }

    fn after_or_on_date(
        &self,
        member_sql: &str,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;
        let value = self.first_timestamp_param(use_db_time_zone, false)?;

        self.templates.gte(member_sql.to_string(), value)
    }

    fn extend_date_range_bound(
        &self,
        date: String,
        interval: &Option<String>,
        is_sub: bool,
    ) -> Result<Option<String>, CubeError> {
        if let Some(interval) = interval {
            if interval != "unbounded" {
                if is_sub {
                    Ok(Some(
                        self.query_tools
                            .base_tools()
                            .subtract_interval(date, interval.clone())?,
                    ))
                } else {
                    Ok(Some(
                        self.query_tools
                            .base_tools()
                            .add_interval(date, interval.clone())?,
                    ))
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(Some(date.to_string()))
        }
    }

    fn regular_rolling_window_date_range(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let (from, to) = self.allocate_date_params(false, true)?;

        let from = if self.values.len() >= 3 {
            self.extend_date_range_bound(from, &self.values[2], true)?
        } else {
            Some(from)
        };

        let to = if self.values.len() >= 4 {
            self.extend_date_range_bound(to, &self.values[3], false)?
        } else {
            Some(to)
        };

        let date_field = self
            .query_tools
            .base_tools()
            .convert_tz(member_sql.to_string())?;
        if let (Some(from), Some(to)) = (&from, &to) {
            self.templates
                .time_range_filter(date_field, from.clone(), to.clone())
        } else if let Some(from) = &from {
            self.templates.gte(date_field, from.clone())
        } else if let Some(to) = &to {
            self.templates.lte(date_field, to.clone())
        } else {
            self.templates.always_true()
        }
    }

    fn to_date_rolling_window_date_range(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let (from, to) = self.allocate_date_params(false, true)?;

        let from = if self.values.len() >= 3 {
            if let Some(granularity) = &self.values[2] {
                self.query_tools
                    .base_tools()
                    .time_grouped_column(granularity.clone(), from)?
            } else {
                return Err(CubeError::user(format!(
                    "Granularity required for to_date rolling window"
                )));
            }
        } else {
            return Err(CubeError::user(format!(
                "Granularity required for to_date rolling window"
            )));
        };

        let date_field = self
            .query_tools
            .base_tools()
            .convert_tz(member_sql.to_string())?;
        self.templates.time_range_filter(date_field, from, to)
    }

    fn in_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let need_null_check = self.is_need_null_chek(false);
        self.templates.in_where(
            member_sql.to_string(),
            self.filter_and_allocate_values(),
            need_null_check,
        )
    }

    fn not_in_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let need_null_check = self.is_need_null_chek(true);
        self.templates.not_in_where(
            member_sql.to_string(),
            self.filter_and_allocate_values(),
            need_null_check,
        )
    }

    fn set_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.templates.set_where(member_sql.to_string())
    }

    fn not_set_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.templates.not_set_where(member_sql.to_string())
    }

    fn gt_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.templates
            .gt(member_sql.to_string(), self.first_param()?)
    }

    fn gte_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.templates
            .gte(member_sql.to_string(), self.first_param()?)
    }

    fn lt_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.templates
            .lt(member_sql.to_string(), self.first_param()?)
    }

    fn lte_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.templates
            .lte(member_sql.to_string(), self.first_param()?)
    }

    fn contains_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.like_or_where(member_sql, false, true, true)
    }

    fn not_contains_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.like_or_where(member_sql, true, true, true)
    }

    fn starts_with_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.like_or_where(member_sql, false, false, true)
    }

    fn not_starts_with_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.like_or_where(member_sql, true, false, true)
    }

    fn ends_with_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.like_or_where(member_sql, false, true, false)
    }

    fn not_ends_with_where(
        &self,
        member_sql: &str,
        _filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        self.like_or_where(member_sql, true, true, false)
    }

    fn like_or_where(
        &self,
        member_sql: &str,
        not: bool,
        start_wild: bool,
        end_wild: bool,
    ) -> Result<String, CubeError> {
        let values = self.filter_and_allocate_values();
        let like_parts = values
            .into_iter()
            .map(|v| {
                self.templates
                    .ilike(member_sql, &v, start_wild, end_wild, not)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let logical_symbol = if not { " AND " } else { " OR " };
        let null_check = if self.is_need_null_chek(not) {
            self.templates.or_is_null_check(member_sql.to_string())?
        } else {
            "".to_string()
        };
        Ok(format!(
            "({}){}",
            like_parts.join(logical_symbol),
            null_check
        ))
    }

    fn from_date_in_db_time_zone(
        &self,
        value: &String,
        use_db_time_zone: bool,
    ) -> Result<String, CubeError> {
        if self.use_raw_values {
            return Ok(value.clone());
        }
        let from = self.format_from_date(value)?;

        let res = if use_db_time_zone && &from != FROM_PARTITION_RANGE {
            self.query_tools.base_tools().in_db_time_zone(from)?
        } else {
            from
        };
        Ok(res)
    }

    fn to_date_in_db_time_zone(
        &self,
        value: &String,
        use_db_time_zone: bool,
    ) -> Result<String, CubeError> {
        if self.use_raw_values {
            return Ok(value.clone());
        }
        let from = self.format_to_date(value)?;

        let res = if use_db_time_zone && &from != TO_PARTITION_RANGE {
            self.query_tools.base_tools().in_db_time_zone(from)?
        } else {
            from
        };
        Ok(res)
    }

    fn allocate_date_params(
        &self,
        use_db_time_zone: bool,
        as_date_time: bool,
    ) -> Result<(String, String), CubeError> {
        if self.values.len() >= 2 {
            let from = if let Some(from_str) = &self.values[0] {
                self.from_date_in_db_time_zone(from_str, use_db_time_zone)?
            } else {
                return Err(CubeError::user(format!(
                    "Arguments for date range is not valid"
                )));
            };

            let to = if let Some(to_str) = &self.values[1] {
                self.to_date_in_db_time_zone(to_str, use_db_time_zone)?
            } else {
                return Err(CubeError::user(format!(
                    "Arguments for date range is not valid"
                )));
            };
            let from = self.allocate_timestamp_param(&from, as_date_time)?;
            let to = self.allocate_timestamp_param(&to, as_date_time)?;
            Ok((from, to))
        } else {
            Err(CubeError::user(format!(
                "2 arguments expected for date range, got {}",
                self.values.len()
            )))
        }
    }

    fn format_from_date(&self, date: &str) -> Result<String, CubeError> {
        QueryDateTimeHelper::format_from_date(date, self.query_tools.clone())
    }

    fn format_to_date(&self, date: &str) -> Result<String, CubeError> {
        QueryDateTimeHelper::format_to_date(date, self.query_tools.clone())
    }

    fn allocate_param(&self, param: &str) -> String {
        self.query_tools.allocate_param(param)
    }

    fn allocate_timestamp_param(
        &self,
        param: &str,
        as_date_time: bool,
    ) -> Result<String, CubeError> {
        if self.use_raw_values {
            return Ok(param.to_string());
        }
        let placeholder = self.query_tools.allocate_param(param);
        if as_date_time {
            self.query_tools.base_tools().date_time_cast(placeholder)
        } else {
            self.query_tools.base_tools().time_stamp_cast(placeholder)
        }
    }

    fn first_param(&self) -> Result<String, CubeError> {
        if self.values.is_empty() {
            Err(CubeError::user(format!(
                "Expected one parameter but nothing found"
            )))
        } else {
            if let Some(value) = &self.values[0] {
                Ok(self.allocate_param(value))
            } else {
                Ok("NULL".to_string())
            }
        }
    }

    fn first_timestamp_param(
        &self,
        use_db_time_zone: bool,
        as_date_time: bool,
    ) -> Result<String, CubeError> {
        if self.values.is_empty() {
            Err(CubeError::user(format!(
                "Expected at least one parameter but nothing found"
            )))
        } else {
            if let Some(value) = &self.values[0] {
                self.allocate_timestamp_param(
                    &self.from_date_in_db_time_zone(value, use_db_time_zone)?,
                    as_date_time,
                )
            } else {
                return Err(CubeError::user(format!(
                    "Arguments for timestamp parameter for operator {} is not valid",
                    self.filter_operator().to_string()
                )));
            }
        }
    }

    fn is_need_null_chek(&self, is_not: bool) -> bool {
        let contains_null = self.is_values_contains_null();
        if is_not {
            !contains_null
        } else {
            contains_null
        }
    }

    fn is_values_contains_null(&self) -> bool {
        self.values.iter().any(|v| v.is_none())
    }

    fn is_array_value(&self) -> bool {
        self.values.len() > 1
    }

    fn filter_and_allocate_values(&self) -> Vec<String> {
        self.values
            .iter()
            .filter_map(|v| v.as_ref().map(|v| self.allocate_param(&v)))
            .collect::<Vec<_>>()
    }
}
