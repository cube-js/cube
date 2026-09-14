use super::operators::{FilterOperationSql, FilterSqlContext};
use super::ToSql;
use crate::cube_bridge::member_sql::FilterParamsColumn;
use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::filter::operators::rolling_window::{
    RegularRollingWindowOp, RollingWindowOffsetOp,
};
use crate::planner::filter::operators::to_date_rolling_window::ToDateRollingWindowOp;
use crate::planner::filter::typed_filter::{resolve_base_symbol, FilterOp, TypedFilter};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_call::SqlCallFilterParamsItem;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use crate::planner::QueryDateTime;
use crate::planner::QueryDateTimeHelper;
use crate::planner::SqlInterval;
use chrono::Duration;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::rc::Rc;
use std::str::FromStr;

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

impl TypedFilter {
    pub fn to_sql_for_filter_params(
        &self,
        item: &SqlCallFilterParamsItem,
        time_shift: Option<&SqlInterval>,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: &Rc<QueryTools>,
        plan_templates: &PlanSqlTemplates,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        // A binding written for a time shift restates a band the stage reads,
        // which takes both of its bounds. An operator supplying fewer describes
        // no band to restate, and the binding would emit a predicate holding
        // whatever its unfilled bounds happened to render as. A shifted stage
        // can equally be a rolling window's base scan, and a rolling filter
        // carries the same two bounds a date range does.
        if let Some(shift_name) = &item.time_shift_name {
            if !matches!(
                self.operation(),
                FilterOp::DateRange(_)
                    | FilterOp::RegularRollingWindow(_)
                    | FilterOp::RollingWindowOffset(_)
                    | FilterOp::ToDateRollingWindow(_)
            ) {
                return Err(CubeError::user(format!(
                    "FILTER_PARAMS binding for time shift `{}` of `{}` needs a date-range filter, \
                     but the query filters that member with `{:?}`",
                    shift_name,
                    item.filter_symbol_name,
                    self.operator()
                )));
            }
        }

        let use_db_time_zone = !filters_context.use_local_tz;

        match &item.column {
            FilterParamsColumn::String(column_sql) => {
                // Inside a time-shifted CTE the FILTER_PARAMS column must carry the
                // same shift as the regular time-dimension filter, otherwise its
                // current-period bounds contradict the shifted predicate and empty
                // the CTE.
                let shifted_column;
                let member_sql = if let Some(interval) = time_shift {
                    shifted_column = format!(
                        "({})",
                        plan_templates
                            .add_timestamp_interval(column_sql.clone(), interval.to_sql())?
                    );
                    shifted_column.as_str()
                } else {
                    column_sql.as_str()
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
                if time_shift.is_some() {
                    return Err(CubeError::user(format!(
                        "FILTER_PARAMS column for `{}` is a callback, which cannot carry the time \
                         shift the surrounding query applies; pass the column as a string instead",
                        item.filter_symbol_name
                    )));
                }
                // A column applies what its filter supplies, and nothing when the
                // filter cannot supply what the column takes — a `set` or `notSet`
                // operator carries no values at all, a one-sided date operator
                // carries one where the column takes both bounds, and a rolling
                // window whose band is not derivable can state no band at all. The
                // filter still reaches the query on its own; only its restatement
                // inside this SQL is dropped, which is narrower than binding a
                // bound the filter never gave.
                let Some(values) =
                    self.filter_param_values(query_tools, plan_templates, use_db_time_zone)?
                else {
                    return plan_templates.always_true();
                };
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
                let Some(args) =
                    self.filter_param_values(query_tools, plan_templates, use_db_time_zone)?
                else {
                    return plan_templates.always_true();
                };
                callback.call(&args)
            }
        }
    }

    // The filter's values, formatted the way a `FILTER_PARAMS` column expects to
    // receive them. `None` where the filter states a band the column cannot be
    // given, which leaves the binding to widen rather than restate the band
    // narrower than it is.
    fn filter_param_values(
        &self,
        query_tools: &Rc<QueryTools>,
        plan_templates: &PlanSqlTemplates,
        use_db_time_zone: bool,
    ) -> Result<Option<Vec<String>>, CubeError> {
        let ctx = FilterSqlContext::new(
            "",
            query_tools,
            plan_templates,
            use_db_time_zone,
            self.use_raw_values(),
        );
        let args = match self.operation() {
            FilterOp::DateRange(_) | FilterOp::DateSingle(_) => {
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
            // A rolling window reads a band wider than the period reported: its
            // own frame, and for a to_date window the period it counts from. A
            // column restating the reported period instead would cut the scan
            // to less than the window sums, and every window would undercount
            // its tail. The band's own bounds are what the column gets.
            FilterOp::RegularRollingWindow(_)
            | FilterOp::RollingWindowOffset(_)
            | FilterOp::ToDateRollingWindow(_) => {
                let Some((from, to)) = self.rolling_window_band(query_tools.timezone())? else {
                    return Ok(None);
                };
                vec![
                    ctx.format_and_allocate_from_date_no_cast(&from)?,
                    ctx.format_and_allocate_to_date_no_cast(&to)?,
                ]
            }
            _ => self
                .values()
                .iter()
                .filter_map(|v| v.to_param_string())
                .map(|v| query_tools.allocate_param(&v))
                .collect::<Vec<_>>(),
        };
        Ok(Some(args))
    }

    /// The band a rolling window's base scan reads, as `[from, to]` dates.
    /// `None` where either end is not derivable here — an unbounded side has no
    /// bound to state, and a window whose series is only known at run time
    /// carries no dates to shift.
    fn rolling_window_band(&self, tz: Tz) -> Result<Option<(String, String)>, CubeError> {
        match self.operation() {
            FilterOp::ToDateRollingWindow(ToDateRollingWindowOp { window_range, .. }) => {
                Ok(window_range.clone())
            }
            FilterOp::RegularRollingWindow(RegularRollingWindowOp {
                trailing,
                leading,
                series_range,
            }) => {
                let Some((series_from, series_to)) = series_range else {
                    return Ok(None);
                };
                let from = shift_bound(tz, series_from, trailing, true)?;
                let to = shift_bound(tz, series_to, leading, false)?;
                Ok(from.zip(to))
            }
            // Without a granularity the window is anchored by one end of the
            // date range rather than by a series, and both its bounds are that
            // anchor shifted by the frame.
            FilterOp::RollingWindowOffset(RollingWindowOffsetOp {
                from,
                to,
                trailing,
                leading,
                offset,
            }) => {
                let precision = 3;
                let anchor = if offset == "start" {
                    from.as_deref()
                        .map(|v| QueryDateTimeHelper::format_from_date(v, precision))
                } else {
                    to.as_deref()
                        .map(|v| QueryDateTimeHelper::format_to_date(v, precision))
                };
                let Some(anchor) = anchor.transpose()? else {
                    return Ok(None);
                };
                let lower = shift_bound(tz, &anchor, trailing, true)?;
                let upper = shift_bound(tz, &anchor, leading, false)?;
                Ok(lower.zip(upper))
            }
            _ => Ok(None),
        }
    }
}

/// `date` moved by `interval`, or `None` for an `unbounded` side — which has no
/// bound to state. A side with no interval keeps the date as it is.
fn shift_bound(
    tz: Tz,
    date: &str,
    interval: &Option<String>,
    subtract: bool,
) -> Result<Option<String>, CubeError> {
    let interval = match interval.as_deref() {
        Some("unbounded") => return Ok(None),
        Some(interval) => SqlInterval::from_str(interval)?,
        None => return Ok(Some(date.to_string())),
    };
    let interval = if subtract {
        interval.inverse()
    } else {
        interval
    };
    let anchor = QueryDateTime::from_date_str(tz, date)?;
    Ok(Some(
        shift_wall_clock(&anchor, &interval)?.format("%Y-%m-%dT%H:%M:%S%.3f"),
    ))
}

/// `anchor` moved by `interval` on the wall clock.
///
/// The band describes a span over the series' own points, and those are wall
/// clock — so an hour of interval has to move the bound by an hour of wall
/// clock, the way the stage's own SQL moves it. `add_interval` switches to
/// absolute arithmetic for an interval carrying no date part, which across a
/// daylight-saving transition lands an offset away from where the stage reads.
fn shift_wall_clock(
    anchor: &QueryDateTime,
    interval: &SqlInterval,
) -> Result<QueryDateTime, CubeError> {
    let carries_date = interval.year != 0
        || interval.quarter != 0
        || interval.month != 0
        || interval.week != 0
        || interval.day != 0;
    if carries_date {
        return anchor.add_interval(interval);
    }
    anchor.add_duration(
        Duration::hours(interval.hour as i64)
            + Duration::minutes(interval.minute as i64)
            + Duration::seconds(interval.second as i64),
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn shifted(date: &str, interval: &str, subtract: bool) -> String {
        shift_bound(
            Tz::America__Los_Angeles,
            date,
            &Some(interval.to_string()),
            subtract,
        )
        .unwrap()
        .unwrap()
    }

    // An hour of interval is an hour of wall clock, on both sides of a
    // spring-forward. Absolute arithmetic would answer 17:00 / 13:00 here,
    // putting the band an hour inside the one the stage's SQL reads.
    #[test]
    fn a_sub_day_shift_moves_the_wall_clock_across_a_dst_boundary() {
        assert_eq!(
            shifted("2024-03-10T12:00:00.000", "18 hour", true),
            "2024-03-09T18:00:00.000"
        );
        assert_eq!(
            shifted("2024-03-09T18:00:00.000", "18 hour", false),
            "2024-03-10T12:00:00.000"
        );
    }

    // A day is a calendar day, which the spring-forward makes 23 hours long.
    #[test]
    fn a_day_shift_stays_on_the_same_clock_time() {
        assert_eq!(
            shifted("2024-03-11T09:00:00.000", "1 day", true),
            "2024-03-10T09:00:00.000"
        );
    }

    #[test]
    fn an_unbounded_side_states_no_bound() {
        assert_eq!(
            shift_bound(
                Tz::America__Los_Angeles,
                "2024-03-10T12:00:00.000",
                &Some("unbounded".to_string()),
                true
            )
            .unwrap(),
            None
        );
    }
}
