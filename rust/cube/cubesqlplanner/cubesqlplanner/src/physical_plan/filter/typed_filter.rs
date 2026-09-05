use super::operators::{FilterOperationSql, FilterSqlContext};
use super::ToSql;
use crate::cube_bridge::member_sql::FilterParamsColumn;
use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::filter::typed_filter::{resolve_base_symbol, FilterOp, TypedFilter};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_call::SqlCallFilterParamsItem;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::CalendarDimensionTimeShift;
use crate::planner::FiltersContext;
use crate::planner::SqlInterval;
use cubenativeutils::CubeError;
use std::rc::Rc;

impl ToSql for TypedFilter {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
        if let FilterOp::MeasureFilter(op) = self.operation() {
            return op.to_sql(
                self.member_evaluator(),
                visitor,
                node_processor,
                query_tools,
                templates,
            );
        }

        let resolved = resolve_base_symbol(self.member_evaluator());
        let member_sql = visitor.apply_for_filter(&resolved, node_processor, templates)?;

        let ctx = FilterSqlContext::new(
            &member_sql,
            &query_tools,
            templates,
            !filters_ctx.use_local_tz,
            self.use_raw_values(),
        );

        dispatch_to_sql(self.operation(), &ctx)
    }
}

/// The time shift active on the stage a FILTER_PARAMS column is rendered into.
/// An interval is arithmetic the column can carry. A calendar shift can be
/// either: declared as an interval it is arithmetic too, but declared with
/// `sql` it resolves through a mapping column on the calendar cube, which has
/// no expression over the fact's own column.
pub enum FilterParamsTimeShift<'a> {
    Interval(&'a SqlInterval),
    Calendar(&'a CalendarDimensionTimeShift),
}

impl TypedFilter {
    pub fn to_sql_for_filter_params(
        &self,
        item: &SqlCallFilterParamsItem,
        time_shift: Option<FilterParamsTimeShift<'_>>,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: &Rc<QueryTools>,
        plan_templates: &PlanSqlTemplates,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;

        match &item.column {
            FilterParamsColumn::String(column_sql) => {
                // Inside a time-shifted CTE the FILTER_PARAMS column must carry the
                // same shift as the regular time-dimension filter, otherwise its
                // current-period bounds contradict the shifted predicate and empty
                // the CTE.
                let shifted_column;
                let member_sql = match &time_shift {
                    Some(FilterParamsTimeShift::Interval(interval)) => {
                        shifted_column = format!(
                            "({})",
                            plan_templates
                                .add_timestamp_interval(column_sql.clone(), interval.to_sql())?
                        );
                        shifted_column.as_str()
                    }
                    // The inverse is not symmetric with the arm above:
                    // `extract_time_shifts` inverts before storing into
                    // `TimeShiftState`, while the calendar map keeps the
                    // declaration as written and the calendar node inverts at
                    // render. Taking `to_sql()` directly here would shift the
                    // pushed-down bounds the wrong way.
                    Some(FilterParamsTimeShift::Calendar(shift)) if shift.sql.is_none() => {
                        match &shift.interval {
                            Some(interval) => {
                                shifted_column = format!(
                                    "({})",
                                    plan_templates.add_timestamp_interval(
                                        column_sql.clone(),
                                        interval.inverse().to_sql()
                                    )?
                                );
                                shifted_column.as_str()
                            }
                            // The calendar node renders the dimension unshifted
                            // too, so there is nothing to carry.
                            None => column_sql.as_str(),
                        }
                    }
                    Some(FilterParamsTimeShift::Calendar(shift)) => {
                        return Err(CubeError::user(format!(
                            concat!(
                                "FILTER_PARAMS column for `{}` is a string, so it cannot carry ",
                                "the `{}` calendar time shift this query applies: that shift ",
                                "resolves through a mapping column on the calendar cube rather ",
                                "than offsetting the column, and no expression over `{}` stands ",
                                "for it. Pass the column as a callback instead - it receives the ",
                                "query's own bounds and can widen the pushed-down range to cover ",
                                "the shifted periods."
                            ),
                            item.filter_symbol_name,
                            shift.name.as_deref().unwrap_or("<unnamed>"),
                            column_sql,
                        )));
                    }
                    None => column_sql.as_str(),
                };
                let ctx = FilterSqlContext::new(
                    member_sql,
                    query_tools,
                    plan_templates,
                    use_db_time_zone,
                    self.use_raw_values(),
                );
                dispatch_to_sql(self.operation(), &ctx)
            }
            FilterParamsColumn::Compiled(compiled) => {
                // An INTERVAL shift is carried by offsetting the column, which
                // only a string column allows. A named calendar shift is carried
                // by the callback itself: it is handed the query's own bounds and
                // decides what range around them to push down, which is the only
                // place that mapping can be expressed.
                if matches!(time_shift, Some(FilterParamsTimeShift::Interval(_))) {
                    return Err(CubeError::user(format!(
                        "FILTER_PARAMS column for `{}` is a callback, which cannot carry the time \
                         shift the surrounding query applies; pass the column as a string instead",
                        item.filter_symbol_name
                    )));
                }
                let values =
                    self.filter_param_values(query_tools, plan_templates, use_db_time_zone)?;
                // A column applies what its filter supplies, and nothing when the
                // filter cannot supply what the column takes — a `set` or `notSet`
                // operator carries no values at all, and a one-sided date operator
                // carries one where the column takes both bounds. The filter still
                // reaches the query on its own; only its restatement inside this
                // SQL is dropped, which is narrower than binding a bound the
                // filter never gave.
                if values.len() < compiled.value_params_count {
                    return plan_templates.always_true();
                }
                let Some(call) = &item.compiled_call else {
                    return Err(CubeError::internal(format!(
                        "Compiled filter params column for `{}` has no call",
                        item.filter_symbol_name
                    )));
                };
                call.eval_with_filter_values(
                    visitor,
                    node_processor,
                    query_tools.clone(),
                    plan_templates,
                    &values,
                )
            }
            FilterParamsColumn::Callback(callback) => {
                // A callback column is opaque SQL produced by user code, so a
                // time shift can't be wrapped around it; it is rendered as-is.
                let args =
                    self.filter_param_values(query_tools, plan_templates, use_db_time_zone)?;
                callback.call(&args)
            }
        }
    }

    // The filter's values, formatted the way a `FILTER_PARAMS` column expects to
    // receive them.
    fn filter_param_values(
        &self,
        query_tools: &Rc<QueryTools>,
        plan_templates: &PlanSqlTemplates,
        use_db_time_zone: bool,
    ) -> Result<Vec<String>, CubeError> {
        let args = match self.operation() {
            // RollingWindowOffset carries [from, to, trailing, leading, offset];
            // only the from/to dates are filter-param args for the callback.
            FilterOp::DateRange(_) | FilterOp::DateSingle(_) | FilterOp::RollingWindowOffset(_) => {
                let ctx = FilterSqlContext::new(
                    "",
                    query_tools,
                    plan_templates,
                    use_db_time_zone,
                    self.use_raw_values(),
                );
                let from = self
                    .values()
                    .first()
                    .and_then(|v| v.to_param_string())
                    .map(|v| ctx.format_and_allocate_from_date_no_cast(&v))
                    .transpose()?;
                let to = self
                    .values()
                    .get(1)
                    .and_then(|v| v.to_param_string())
                    .map(|v| ctx.format_and_allocate_to_date_no_cast(&v))
                    .transpose()?;
                [from, to].into_iter().flatten().collect()
            }
            _ => self
                .values()
                .iter()
                .filter_map(|v| v.to_param_string())
                .map(|v| query_tools.allocate_param(&v))
                .collect::<Vec<_>>(),
        };
        Ok(args)
    }
}

fn dispatch_to_sql(op: &FilterOp, ctx: &FilterSqlContext) -> Result<String, CubeError> {
    match op {
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
        FilterOp::RollingWindowOffset(op) => op.to_sql(ctx),
        FilterOp::ToDateRollingWindow(op) => op.to_sql(ctx),
    }
}
