use std::{
    cmp::{max, min},
    str::FromStr,
    sync::Arc,
};

use chrono::{DateTime, Datelike, Days, Months, NaiveDate, Timelike, Utc};
use datafusion::{
    arrow::datatypes::{ArrowPrimitiveType, IntervalDayTimeType, IntervalMonthDayNanoType},
    error::DataFusionError,
    logical_plan::{Expr, Operator},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use egg::Id;

use crate::{
    compile::rewrite::{rewriter::CubeEGraph, BinaryExprOp, LiteralExprValue, LogicalPlanLanguage},
    transport::SqlTemplates,
    CubeError,
};

type IntervalDayTime = <IntervalDayTimeType as ArrowPrimitiveType>::Native;
type IntervalMonthDayNano = <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native;

// TODO merge these with date_part and date_trunc on new arrow
// See https://github.com/apache/arrow-rs/blob/63a6209b87d9fb2d06265fa5d4c72817b6f47394/arrow-arith/src/temporal.rs#

#[derive(Debug)]
pub enum DatePartToken {
    Delta(DeltaTimeUnitToken),
    Special(SpecialTimeUnitToken),
}

impl FromStr for DatePartToken {
    // TODO proper type for err
    type Err = String;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        input
            .parse::<DeltaTimeUnitToken>()
            .map(DatePartToken::Delta)
            .or_else(|_| {
                input
                    .parse::<SpecialTimeUnitToken>()
                    .map(DatePartToken::Special)
            })
            .map_err(|_| format!("Unexpected value for DatePartToken: {input}"))
    }
}

impl DatePartToken {
    // Must return standard representations where it can
    pub fn as_str(&self) -> &str {
        use DatePartToken::*;
        // Each representation chosen as it is documented
        // https://www.postgresql.org/docs/16/functions-datetime.html
        match self {
            Delta(token) => token.as_str(),
            Special(token) => token.as_str(),
        }
    }

    pub fn delta_for_trunc(&self) -> Option<DeltaTimeUnitToken> {
        // Get suitable argument for date_trunc, so we could turn date_part(self, ...) to date_part(date_trunc(result, ...))
        // Not every first arg of date_part is suitable for this
        // For example, date_part supports 'epoch', while for date_trunc it does not make sense
        // See https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
        // See https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC

        use DatePartToken::*;
        use DeltaTimeUnitToken::*;
        use SpecialTimeUnitToken::*;

        match self {
            Delta(Microseconds) => Some(Microseconds),
            Delta(Milliseconds) => Some(Milliseconds),
            Delta(Second) => Some(Second),
            Delta(Minute) => Some(Minute),
            Delta(Hour) => Some(Hour),
            Delta(Day) => Some(Day),
            Delta(Week) => Some(Week),
            Delta(Month) => Some(Month),
            Delta(Quarter) => Some(Quarter),
            Delta(Year) => Some(Year),
            Delta(Decade) => Some(Decade),
            Delta(Century) => Some(Century),
            Delta(Millennium) => Some(Millennium),

            // Does not really make sense
            Delta(Timezone) => None,
            Delta(TimezoneHour) => None,
            Delta(TimezoneMinute) => None,

            Special(DayOfWeek) => Some(Day),
            Special(DayOfYear) => Some(Day),
            // No possible truncation here
            Special(Epoch) => None,
            Special(IsoDayOfWeek) => Some(Day),
            Special(IsoDayOfYear) => Some(Day),
            // Results of extract(julian from ...) can be fractional, no possible truncation
            Special(Julian) => None,
        }
    }
}

// Special delta token
// Adapted from datetktbl in PostgreSQL src/backend/utils/adt/datetime.c
// Only entries with type UNITS or value DTK_EPOCH are present, and that are not covered by DeltaTimeUnitToken
// Only those are relevant to date_part and date_trunc
#[derive(Debug)]
pub enum SpecialTimeUnitToken {
    DayOfWeek,
    DayOfYear,
    Epoch,
    IsoDayOfWeek,
    IsoDayOfYear,
    Julian,
}

impl SpecialTimeUnitToken {
    // Must return standard representations where it can
    pub fn as_str(&self) -> &str {
        use SpecialTimeUnitToken::*;
        // Each representation chosen as it is documented
        // https://www.postgresql.org/docs/16/functions-datetime.html
        match self {
            DayOfWeek => "dow",
            DayOfYear => "doy",
            Epoch => "epoch",
            IsoDayOfWeek => "isodow",
            IsoDayOfYear => "isoyear",
            Julian => "julian",
        }
    }
}

impl FromStr for SpecialTimeUnitToken {
    // TODO proper type for err
    type Err = String;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        use SpecialTimeUnitToken::*;

        // TODO try to remove allocation and use direct parsing
        let lower = input.to_ascii_lowercase();

        // See https://github.com/postgres/postgres/blob/ca6fde92258a328a98c1d9e41da5462b73da8529/src/backend/utils/adt/datetime.c#L92-L179
        Ok(match lower.as_str() {
            "dow" => DayOfWeek,
            "doy" => DayOfYear,
            "epoch" => Epoch,
            "isodow" => IsoDayOfWeek,
            "isoyear" => IsoDayOfYear,
            "j" => Julian,
            "jd" => Julian,
            "julian" => Julian,

            _ => {
                return Err(format!(
                    "Unexpected value for SpecialTimeUnitToken: {input}"
                ))
            }
        })
    }
}

// Time delta token
// Adapted from deltatktbl in PostgreSQL src/backend/utils/adt/datetime.c
// Only entries with type UNITS are present
// Only those are relevant to date_part and date_trunc
// Use this for date_trunc arguments
#[derive(Debug)]
pub enum DeltaTimeUnitToken {
    Microseconds,
    Milliseconds,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    Decade,
    Century,
    Millennium,

    Timezone,
    TimezoneHour,
    TimezoneMinute,
}

impl FromStr for DeltaTimeUnitToken {
    // TODO proper type for err
    type Err = String;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // postgres does support non-standard granularities, but doesn't document it
        // See https://github.com/postgres/postgres/blob/ca6fde92258a328a98c1d9e41da5462b73da8529/src/backend/utils/adt/datetime.c#L227-L228
        // See https://github.com/postgres/postgres/blob/ca6fde92258a328a98c1d9e41da5462b73da8529/src/backend/utils/adt/timestamp.c#L4698-L4700
        // See https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC

        use DeltaTimeUnitToken::*;

        // TODO try to remove allocation and use direct parsing
        let lower = input.to_ascii_lowercase();

        // Beware that postgres truncates identifiers before comparison
        // TODO implement truncation for more complete behavior match
        // See https://github.com/postgres/postgres/blob/ca6fde92258a328a98c1d9e41da5462b73da8529/src/backend/utils/adt/datetime.c#L183-L250
        Ok(match lower.as_str() {
            "c" => Century,
            "cent" => Century,
            "centuries" => Century,
            "century" => Century,
            "d" => Day,
            "day" => Day,
            "days" => Day,
            "dec" => Decade,
            "decade" => Decade,
            "decades" => Decade,
            "decs" => Decade,
            "h" => Hour,
            "hour" => Hour,
            "hours" => Hour,
            "hr" => Hour,
            "hrs" => Hour,
            "m" => Minute,
            "microseconds" => Microseconds,
            "mil" => Millennium,
            "millennia" => Millennium,
            "millennium" => Millennium,
            "milliseconds" => Milliseconds,
            "mils" => Millennium,
            "min" => Minute,
            "mins" => Minute,
            "minute" => Minute,
            "minutes" => Minute,
            "mon" => Month,
            "mons" => Month,
            "month" => Month,
            "months" => Month,
            "ms" => Milliseconds,
            "msec" => Milliseconds,
            "msecond" => Milliseconds,
            "mseconds" => Milliseconds,
            "msecs" => Milliseconds,
            "qtr" => Quarter,
            "quarter" => Quarter,
            "s" => Second,
            "sec" => Second,
            "second" => Second,
            "seconds" => Second,
            "secs" => Second,
            "timezone" => Timezone,
            "timezone_hour" => TimezoneHour,
            "timezone_minute" => TimezoneMinute,
            "us" => Microseconds,
            "usec" => Microseconds,
            "usecond" => Microseconds,
            "useconds" => Microseconds,
            "usecs" => Microseconds,
            "w" => Week,
            "week" => Week,
            "weeks" => Week,
            "y" => Year,
            "year" => Year,
            "years" => Year,
            "yr" => Year,
            "yrs" => Year,

            _ => return Err(format!("Unexpected value for DeltaTimeUnitToken: {input}")),
        })
    }
}

impl DeltaTimeUnitToken {
    // Must return standard representations where it can
    pub fn as_str(&self) -> &str {
        use DeltaTimeUnitToken::*;
        // Each representation chosen as it is documented
        // https://www.postgresql.org/docs/16/functions-datetime.html
        match self {
            Microseconds => "microseconds",
            Milliseconds => "milliseconds",
            Second => "second",
            Minute => "minute",
            Hour => "hour",
            Day => "day",
            Week => "week",
            Month => "month",
            Quarter => "quarter",
            Year => "year",
            Decade => "decade",
            Century => "century",
            Millennium => "millennium",
            Timezone => "timezone",
            TimezoneHour => "timezone_hour",
            TimezoneMinute => "timezone_minute",
        }
    }
}

pub fn parse_granularity_string(granularity: &str, to_normalize: bool) -> Option<String> {
    if to_normalize {
        match granularity.to_lowercase().as_str() {
            "dow" | "doy" => Some("day".to_string()),
            "qtr" => Some("quarter".to_string()),
            _ => Some(granularity.to_lowercase()),
        }
    } else {
        match granularity.to_lowercase().as_str() {
            "qtr" => Some("quarter".to_string()),
            _ => Some(granularity.to_lowercase()),
        }
    }
}

pub fn parse_granularity(granularity: &ScalarValue, to_normalize: bool) -> Option<String> {
    match granularity {
        ScalarValue::Utf8(Some(granularity)) => {
            parse_granularity_string(&granularity, to_normalize)
        }
        _ => None,
    }
}

pub fn granularity_scalar_to_interval(granularity: &ScalarValue) -> Option<ScalarValue> {
    if let Some(granularity) = parse_granularity(granularity, false) {
        return granularity_to_interval(&granularity);
    }
    None
}

pub fn granularity_str_to_interval(granularity: &str) -> Option<ScalarValue> {
    if let Some(granularity) = parse_granularity_string(granularity, false) {
        return granularity_to_interval(&granularity);
    }
    None
}

fn granularity_to_interval(granularity: &str) -> Option<ScalarValue> {
    let interval = match granularity.to_lowercase().as_str() {
        "millisecond" | "min_unit" => ScalarValue::IntervalDayTime(Some(1)),
        "second" => ScalarValue::IntervalDayTime(Some(1000)),
        "minute" => ScalarValue::IntervalDayTime(Some(60000)),
        "hour" => ScalarValue::IntervalDayTime(Some(3600000)),
        "day" => ScalarValue::IntervalDayTime(Some(4294967296)),
        "week" => ScalarValue::IntervalDayTime(Some(30064771072)),
        "month" => ScalarValue::IntervalYearMonth(Some(1)),
        "quarter" => ScalarValue::IntervalYearMonth(Some(3)),
        "year" => ScalarValue::IntervalYearMonth(Some(12)),
        _ => return None,
    };
    Some(interval)
}

pub fn min_max_granularity(
    left: &ScalarValue,
    right: &ScalarValue,
    is_max: bool,
    week_as_day: Option<bool>,
) -> Option<String> {
    let left = parse_granularity(left, false)?;
    let right = parse_granularity(right, false)?;

    let left = granularity_str_to_int_order(&left, week_as_day)?;
    let right = granularity_str_to_int_order(&right, week_as_day)?;

    let result = if is_max {
        max(left, right)
    } else {
        min(left, right)
    };
    granularity_int_order_to_str(result, week_as_day)
}

pub fn granularity_str_to_int_order(granularity: &str, week_as_day: Option<bool>) -> Option<i32> {
    match granularity.to_lowercase().as_str() {
        "second" => Some(0),
        "minute" => Some(1),
        "hour" => Some(2),
        "day" => Some(3),
        // Week-month offsets may lead to incorrect results. `week_as_day` controls
        // the result of week granularity conversion.
        "week" => match week_as_day {
            Some(true) => Some(3),
            Some(false) => Some(4),
            None => None,
        },
        "month" => Some(5),
        "quarter" => Some(6),
        "year" => Some(7),
        _ => None,
    }
}

fn granularity_int_order_to_str(granularity: i32, week_as_day: Option<bool>) -> Option<String> {
    match granularity {
        0 => Some("second"),
        1 => Some("minute"),
        2 => Some("hour"),
        3 => Some("day"),
        // Week-month offsets may lead to incorrect results. `week_as_day` controls
        // the result of week granularity conversion.
        4 => match week_as_day {
            Some(true) => Some("day"),
            Some(false) => Some("week"),
            None => None,
        },
        5 => Some("month"),
        6 => Some("quarter"),
        7 => Some("year"),
        _ => None,
    }
    .map(|g| g.to_string())
}

pub fn negated_cube_filter_op(op: &str) -> Option<&'static str> {
    macro_rules! define_op_eq {
        ($($EXPR:expr => $NEG:expr,)*) => {
            match op {
                $(
                    $EXPR => $NEG,
                    $NEG => $EXPR,
                )*
                _ => return None,
            }
        }
    }

    let negated = define_op_eq![
        "equals" => "notEquals",
        "contains" => "notContains",
        "startsWith" => "notStartsWith",
        "endsWith" => "notEndsWith",
        "gt" => "lte",
        "lt" => "gte",
        "set" => "notSet",
        "inDateRange" => "notInDateRange",
    ];

    Some(negated)
}

pub fn reaggragate_fun(cube_fun: &str) -> Option<AggregateFunction> {
    Some(match cube_fun {
        "count" | "sum" => AggregateFunction::Sum,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        _ => return None,
    })
}

pub fn is_literal_date_trunced(ns: i64, granularity: &str) -> Option<bool> {
    let granularity = parse_granularity_string(granularity, false)?;
    let ns_in_seconds = 1_000_000_000;
    if ns % ns_in_seconds > 0 {
        return Some(false);
    }
    let seconds = ns / ns_in_seconds;
    let dt = DateTime::from_timestamp(seconds, 0)?;

    let is_minute_trunced = |dt: DateTime<Utc>| dt.second() == 0;
    let is_hour_trunced = |dt| is_minute_trunced(dt) && dt.minute() == 0;
    let is_day_trunced = |dt| is_hour_trunced(dt) && dt.hour() == 0;
    let is_week_trunced = |dt| is_day_trunced(dt) && dt.weekday().num_days_from_monday() == 0;
    let is_month_trunced = |dt| is_day_trunced(dt) && dt.day() == 1;
    let is_quarter_trunced = |dt| is_month_trunced(dt) && dt.month0() % 3 == 0;
    let is_year_trunced = |dt| is_month_trunced(dt) && dt.month() == 1;

    Some(match granularity.as_str() {
        "second" => true,
        "minute" => is_minute_trunced(dt),
        "hour" => is_hour_trunced(dt),
        "day" => is_day_trunced(dt),
        "week" => is_week_trunced(dt),
        "month" => is_month_trunced(dt),
        "quarter" => is_quarter_trunced(dt),
        "year" => is_year_trunced(dt),
        _ => return None,
    })
}

#[derive(Clone, Copy)]
pub struct DecomposedDayTime {
    pub days: i32,
    pub millis: i32,
}

impl DecomposedDayTime {
    const DAY_LABEL: &'static str = "DAY";
    const MILLIS_LABEL: &'static str = "MILLISECOND";

    pub fn from_raw_interval_value(interval: IntervalDayTime) -> Self {
        let (days, millis) = IntervalDayTimeType::to_parts(interval);

        Self { days, millis }
    }

    pub fn is_single_part(&self) -> bool {
        self.days == 0 || self.millis == 0
    }

    pub fn millis_scalar(&self) -> ScalarValue {
        let value = Some(IntervalDayTimeType::make_value(0, self.millis));
        ScalarValue::IntervalDayTime(value)
    }

    pub fn days_scalar(&self) -> ScalarValue {
        let value = Some(IntervalDayTimeType::make_value(self.days, 0));
        ScalarValue::IntervalDayTime(value)
    }

    pub fn create_decomposed_expr(self) -> Expr {
        match (self.days, self.millis) {
            (0, _) => Expr::Literal(self.millis_scalar()),
            (_, 0) => Expr::Literal(self.days_scalar()),
            _ => Expr::BinaryExpr {
                left: Box::new(Expr::Literal(self.days_scalar())),
                op: Operator::Plus,
                right: Box::new(Expr::Literal(self.millis_scalar())),
            },
        }
    }

    pub fn add_decomposed_to_egraph(self, egraph: &mut CubeEGraph) -> Id {
        let add_literal = |egraph: &mut CubeEGraph, scalar| {
            let id = egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                scalar,
            )));
            egraph.add(LogicalPlanLanguage::LiteralExpr([id]))
        };

        match (self.days, self.millis) {
            (0, _) => add_literal(egraph, self.millis_scalar()),
            (_, 0) => add_literal(egraph, self.days_scalar()),
            _ => {
                let op = Operator::Plus;
                let op = egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(op)));
                let left = add_literal(egraph, self.days_scalar());
                let right = add_literal(egraph, self.millis_scalar());

                egraph.add(LogicalPlanLanguage::BinaryExpr([left, op, right]))
            }
        }
    }

    pub fn generate_interval_sql(
        &self,
        templates: &Arc<SqlTemplates>,
    ) -> Result<String, DataFusionError> {
        let single = !templates.contains_template("expressions/interval");
        match single {
            true => self.generate_simple_interval_sql(templates),
            _ => self.generate_composite_interval_sql(templates),
        }
        .map_err(|e| DataFusionError::Internal(format!("Can't generate SQL for interval: {}", e)))
    }

    fn generate_simple_interval_sql(
        &self,
        templates: &Arc<SqlTemplates>,
    ) -> Result<String, CubeError> {
        match (self.days as i64, self.millis as i64) {
            (0, millis) => templates.interval_single_expr(millis, Self::MILLIS_LABEL),
            (days, 0) => templates.interval_single_expr(days, Self::DAY_LABEL),
            (days, millis) => Err(CubeError::internal(format!(
                "Expected simple interval, found composite: (days: {days};  millis: {millis})"
            ))),
        }
    }
    fn generate_composite_interval_sql(
        &self,
        templates: &Arc<SqlTemplates>,
    ) -> Result<String, CubeError> {
        const MILLIS: &str = DecomposedDayTime::MILLIS_LABEL;
        const DAY: &str = DecomposedDayTime::DAY_LABEL;
        match (self.days, self.millis) {
            (0, millis) => templates.interval_expr(format!("{millis} {MILLIS}")),
            (days, 0) => templates.interval_expr(format!("{days} {DAY}")),
            (days, millis) => templates.interval_expr(format!("{days} {DAY} {millis} {MILLIS}")),
        }
    }
}

pub struct DecomposedMonthDayNano {
    pub months: i32,
    pub days: i32,
    pub millis: i64,
}

impl DecomposedMonthDayNano {
    const MONTH: &'static str = "MONTH";
    const DAY: &'static str = "DAY";
    const MILLIS: &'static str = "MILLISECOND";

    const NANOS_IN_MILLI: i64 = 1_000_000;

    pub fn from_raw_interval_value(interval: IntervalMonthDayNano) -> Self {
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(interval);
        // TODO: precision loss
        let millis = nanos / Self::NANOS_IN_MILLI;
        DecomposedMonthDayNano {
            months,
            days,
            millis,
        }
    }

    pub fn is_single_part(&self) -> bool {
        if self.months == 0 {
            self.days == 0 || self.millis == 0
        } else {
            self.days == 0 && self.millis == 0
        }
    }

    pub fn millis_scalar(&self) -> ScalarValue {
        let value = Some(IntervalMonthDayNanoType::make_value(
            0,
            0,
            self.millis * Self::NANOS_IN_MILLI,
        ));
        ScalarValue::IntervalMonthDayNano(value)
    }

    pub fn days_scalar(&self) -> ScalarValue {
        let value = Some(IntervalMonthDayNanoType::make_value(0, self.days, 0));
        ScalarValue::IntervalMonthDayNano(value)
    }

    pub fn months_scalar(&self) -> ScalarValue {
        let value = Some(IntervalMonthDayNanoType::make_value(self.months, 0, 0));
        ScalarValue::IntervalMonthDayNano(value)
    }

    pub fn create_decomposed_expr(self) -> Expr {
        let bin = |l, r| Expr::BinaryExpr {
            left: Box::new(Expr::Literal(l)),
            op: Operator::Plus,
            right: Box::new(Expr::Literal(r)),
        };

        match (self.months, self.days, self.millis) {
            (0, 0, _) => Expr::Literal(self.millis_scalar()),
            (0, _, 0) => Expr::Literal(self.days_scalar()),
            (_, 0, 0) => Expr::Literal(self.months_scalar()),

            (0, _, _) => bin(self.days_scalar(), self.millis_scalar()),
            (_, 0, _) => bin(self.months_scalar(), self.millis_scalar()),
            (_, _, 0) => bin(self.months_scalar(), self.days_scalar()),

            _ => Expr::BinaryExpr {
                left: Box::new(Expr::Literal(self.months_scalar())),
                op: Operator::Plus,
                right: Box::new(bin(self.days_scalar(), self.millis_scalar())),
            },
        }
    }

    pub fn add_decomposed_to_egraph(self, egraph: &mut CubeEGraph) -> Id {
        let add_literal = |egraph: &mut CubeEGraph, scalar| {
            let id = egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                scalar,
            )));
            egraph.add(LogicalPlanLanguage::LiteralExpr([id]))
        };
        let add_binary = |egraph: &mut CubeEGraph, l, r| {
            let op = Operator::Plus;
            let op = egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(op)));
            let left = add_literal(egraph, l);
            let right = add_literal(egraph, r);

            egraph.add(LogicalPlanLanguage::BinaryExpr([left, op, right]))
        };

        match (self.months, self.days, self.millis) {
            (0, 0, _) => add_literal(egraph, self.millis_scalar()),
            (0, _, 0) => add_literal(egraph, self.days_scalar()),
            (_, 0, 0) => add_literal(egraph, self.months_scalar()),

            (0, _, _) => add_binary(egraph, self.days_scalar(), self.millis_scalar()),
            (_, 0, _) => add_binary(egraph, self.months_scalar(), self.millis_scalar()),
            (_, _, 0) => add_binary(egraph, self.months_scalar(), self.days_scalar()),

            _ => {
                let op = Operator::Plus;
                let op = egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(op)));
                let left = add_literal(egraph, self.months_scalar());
                let right = add_binary(egraph, self.days_scalar(), self.millis_scalar());

                egraph.add(LogicalPlanLanguage::BinaryExpr([left, op, right]))
            }
        }
    }

    pub fn generate_interval_sql(
        &self,
        templates: &Arc<SqlTemplates>,
    ) -> Result<String, DataFusionError> {
        let single = !templates.contains_template("expressions/interval");
        match single {
            true => self.generate_simple_interval_sql(templates),
            _ => self.generate_composite_interval_sql(templates),
        }
        .map_err(|e| DataFusionError::Internal(format!("Can't generate SQL for interval: {}", e)))
    }

    fn generate_simple_interval_sql(
        &self,
        templates: &Arc<SqlTemplates>,
    ) -> Result<String, CubeError> {
        match (self.months as i64, self.days as i64, self.millis) {
            (0, 0, millis) => templates.interval_single_expr(millis, Self::MILLIS),
            (0, days, 0) => templates.interval_single_expr(days, Self::DAY),
            (mons, 0, 0) => templates.interval_single_expr(mons, Self::MONTH),
            (mons, days, millis) => Err(CubeError::internal(format!(
                "Expected simple interval, found composite (months: {mons};  days: {days};  millis: {millis})"
            ))),
        }
    }

    fn generate_composite_interval_sql(
        &self,
        templates: &Arc<SqlTemplates>,
    ) -> Result<String, CubeError> {
        const MILLIS: &str = DecomposedMonthDayNano::MILLIS;
        const DAY: &str = DecomposedMonthDayNano::DAY;
        const MONTH: &str = DecomposedMonthDayNano::MONTH;

        let gen_two_parts = |num1, date_part1, num2, date_part2| {
            templates.interval_expr(format!("{num1} {date_part1} {num2} {date_part2}"))
        };

        match (self.months as i64, self.days as i64, self.millis) {
            (0, 0, millis) => templates.interval_expr(format!("{millis} {MILLIS}")),
            (0, days, 0) => templates.interval_expr(format!("{days} {DAY}")),
            (mons, 0, 0) => templates.interval_expr(format!("{mons} {MONTH}")),
            (0, days, millis) => gen_two_parts(days, DAY, millis, MILLIS),
            (mons, 0, millis) => gen_two_parts(mons, MONTH, millis, MILLIS),
            (mons, days, 0) => gen_two_parts(mons, MONTH, days, DAY),
            (mons, days, millis) => {
                templates.interval_expr(format!("{mons} {MONTH} {days} {DAY} {millis} {MILLIS}"))
            }
        }
    }
}

/// Try to merge a date range with a date part extraction filter.
///
/// This function calculates the date range for a specific date part (month, quarter, week)
/// within the given year, constrained by the provided start and end dates.
pub fn try_merge_range_with_date_part(
    start_date: NaiveDate,
    end_date: NaiveDate,
    granularity: &str,
    value: i64,
) -> Option<(NaiveDate, NaiveDate)> {
    // Check that the range only covers one year
    let year = start_date.year();
    if year != end_date.year() {
        return None;
    }

    match granularity {
        "month" => {
            // Month value must be valid
            if !(1..=12).contains(&value) {
                return None;
            }

            // Obtain the new range
            let new_start_date = NaiveDate::from_ymd_opt(year, value as u32, 1)?;

            let new_end_date = new_start_date
                .checked_add_months(Months::new(1))
                .and_then(|date| date.checked_sub_days(Days::new(1)))?;

            // If the resulting range is outside of the original range, we can't merge
            // the filters
            if new_start_date > end_date || new_end_date < start_date {
                return None;
            }

            // Preserves existing constraints, for example:
            // inDataRange: order_date >= '2019-02-15' AND order_date < '2019-03-10'
            // filter: EXTRACT(MONTH FROM order_date) = 2 (February)
            let new_start_date = max(new_start_date, start_date);
            let new_end_date = min(new_end_date, end_date);

            Some((new_start_date, new_end_date))
        }
        "quarter" | "qtr" => {
            // Quarter value must be valid
            if !(1..=4).contains(&value) {
                return None;
            }

            let quarter_start_month = (value - 1) * 3 + 1;

            // Obtain the new range
            let new_start_date = NaiveDate::from_ymd_opt(year, quarter_start_month as u32, 1)?;

            let new_end_date = new_start_date
                .checked_add_months(Months::new(3))
                .and_then(|date| date.checked_sub_days(Days::new(1)))?;

            // Paranoid check, If the resulting range is outside of the original range, we can't merge
            // the filters
            if new_start_date > end_date || new_end_date < start_date {
                return None;
            }

            // Preserves existing constraints, for example:
            // inDataRange: order_date >= '2019-04-15' AND order_date < '2019-12-31'
            // filter: EXTRACT(QUARTER FROM order_date) = 2
            let new_start_date = max(new_start_date, start_date);
            let new_end_date = min(new_end_date, end_date);

            Some((new_start_date, new_end_date))
        }
        // Following ISO 8601
        "week" => {
            // Week value must be valid
            if !(1..=53).contains(&value) {
                return None;
            }

            // For ISO weeks, we need to find the year that contains this week number
            // Try with the start_date year first
            let year = start_date.year();

            // Get January 4th of the year (which is always in week 1)
            let jan_4 = NaiveDate::from_ymd_opt(year, 1, 4)?;

            // Get the Monday of week 1
            let iso_week = jan_4.iso_week();
            let week_1_year = iso_week.year();

            // Check if we're looking at the right ISO year
            // The ISO year might differ from calendar year for dates near year boundaries
            if week_1_year != year {
                // This can happen when January 1-3 belong to the previous year's last week
                // For now, we'll require that the range is within a single ISO year
                return None;
            }

            // Calculate the date of Monday of the requested week
            // ISO week 1 starts on the Monday of the week containing January 4th
            let days_from_week_1 = (value - 1) * 7;
            let week_1_monday = jan_4 - Days::new(jan_4.weekday().num_days_from_monday() as u64);

            let week_start = week_1_monday.checked_add_days(Days::new(days_from_week_1 as u64))?;

            let week_end = week_start.checked_add_days(Days::new(6))?;

            // Verify this week actually exists in this year (week 53 doesn't always exist)
            if week_start.iso_week().week() != value as u32 {
                return None;
            }

            // Paranoid check, If the resulting range is outside of the original range, we can't merge
            // the filters
            if week_start > end_date || week_end < start_date {
                return None;
            }

            // Preserves existing constraints, for example:
            // inDataRange: order_date >= '2019-04-09' AND order_date <= '2019-04-12'
            // filter: EXTRACT(WEEK FROM date) = 15
            let new_start_date = max(week_start, start_date);
            let new_end_date = min(week_end, end_date);

            Some((new_start_date, new_end_date))
        }
        // TODO: handle more granularities
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_week_range() -> Result<(), CubeError> {
        let start = NaiveDate::from_ymd_opt(2019, 1, 1).expect("Invalid date");
        let end = NaiveDate::from_ymd_opt(2019, 12, 31).expect("Invalid date");

        // Test week 1 of 2019 (Dec 31, 2018 - Jan 6, 2019)
        // But constrained by our range starting Jan 1
        let (week_start, week_end) =
            try_merge_range_with_date_part(start, end, "week", 1).expect("Expected week range");
        assert_eq!(
            week_start,
            NaiveDate::from_ymd_opt(2019, 1, 1).expect("Invalid date")
        );
        assert_eq!(
            week_end,
            NaiveDate::from_ymd_opt(2019, 1, 6).expect("Invalid date")
        );

        // Test week 15 of 2019 (Apr 8-14)
        let (week_start, week_end) =
            try_merge_range_with_date_part(start, end, "week", 15).expect("Expected week range");
        assert_eq!(
            week_start,
            NaiveDate::from_ymd_opt(2019, 4, 8).expect("Invalid date")
        );
        assert_eq!(
            week_end,
            NaiveDate::from_ymd_opt(2019, 4, 14).expect("Invalid date")
        );

        // Test week 52 of 2019 (Dec 23-29)
        let (week_start, week_end) =
            try_merge_range_with_date_part(start, end, "week", 52).expect("Expected week range");
        assert_eq!(
            week_start,
            NaiveDate::from_ymd_opt(2019, 12, 23).expect("Invalid date")
        );
        assert_eq!(
            week_end,
            NaiveDate::from_ymd_opt(2019, 12, 29).expect("Invalid date")
        );

        // Test invalid week number
        assert_eq!(try_merge_range_with_date_part(start, end, "week", 0), None);
        assert_eq!(try_merge_range_with_date_part(start, end, "week", 54), None);

        // Test week 53 (which doesn't exist in 2019)
        let result = try_merge_range_with_date_part(start, end, "week", 53);
        assert!(result.is_none());

        // Test partial overlap
        let start = NaiveDate::from_ymd_opt(2019, 4, 10).expect("Invalid date");
        let end = NaiveDate::from_ymd_opt(2019, 4, 12).expect("Invalid date");
        let (week_start, week_end) =
            try_merge_range_with_date_part(start, end, "week", 15).expect("Expected week range");
        assert_eq!(
            week_start,
            NaiveDate::from_ymd_opt(2019, 4, 10).expect("Invalid date")
        );
        assert_eq!(
            week_end,
            NaiveDate::from_ymd_opt(2019, 4, 12).expect("Invalid date")
        );

        // Test no overlap
        let start = NaiveDate::from_ymd_opt(2019, 5, 1).expect("Invalid date");
        let end = NaiveDate::from_ymd_opt(2019, 12, 31).expect("Invalid date");
        let result = try_merge_range_with_date_part(start, end, "week", 15);
        assert!(result.is_none());

        Ok(())
    }
}
