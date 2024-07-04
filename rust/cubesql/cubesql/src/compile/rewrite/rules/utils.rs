use std::{
    cmp::{max, min},
    sync::Arc,
};

use chrono::{Datelike, NaiveDateTime, Timelike};
use datafusion::{
    error::DataFusionError,
    logical_plan::{Expr, Operator},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use egg::{EGraph, Id};

use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, BinaryExprOp, LiteralExprValue, LogicalPlanLanguage,
    },
    transport::SqlTemplates,
    CubeError,
};

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
    let dt = NaiveDateTime::from_timestamp_opt(seconds, 0)?;

    let is_minute_trunced = |dt: NaiveDateTime| dt.second() == 0;
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
    const _DAY_BITS: i32 = 32;
    const MILLIS_BITS: i32 = 32;

    const DAY_LABEL: &'static str = "DAY";
    const MILLIS_LABEL: &'static str = "MILLISECOND";

    pub fn from_raw_interval_value(interval: i64) -> Self {
        Self {
            days: (interval >> Self::MILLIS_BITS) as i32,
            millis: interval as i32,
        }
    }

    pub fn is_single_part(&self) -> bool {
        self.days == 0 || self.millis == 0
    }

    pub fn millis_scalar(&self) -> ScalarValue {
        let value = Some(self.millis as i64);
        ScalarValue::IntervalDayTime(value)
    }

    pub fn days_scalar(&self) -> ScalarValue {
        let value = Some((self.days as i64) << Self::MILLIS_BITS);
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

    pub fn add_decomposed_to_egraph(
        self,
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) -> Id {
        let add_literal = |egraph: &mut EGraph<_, _>, scalar| {
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
    const _MONTHS_MASK: u128 = 0xFFFF_FFFF_0000_0000_0000_0000_0000_0000;
    const DAYS_MASK: u128 = 0x0000_0000_FFFF_FFFF_0000_0000_0000_0000;
    const NANOS_MASK: u128 = 0x0000_0000_0000_0000_FFFF_FFFF_FFFF_FFFF;
    const _MONTHS_BITS: i32 = 32;
    const DAYS_BITS: i32 = 32;
    const NANOS_BITS: i32 = 64;
    const DAYS_OFFSET: i32 = Self::NANOS_BITS;
    const MONTHS_OFFSET: i32 = Self::DAYS_OFFSET + Self::DAYS_BITS;

    const MONTH: &'static str = "MONTH";
    const DAY: &'static str = "DAY";
    const MILLIS: &'static str = "MILLISECOND";

    const NANOS_IN_MILLI: i64 = 1_000_000;

    pub fn from_raw_interval_value(interval: i128) -> Self {
        let interval = interval as u128;
        let months = (interval >> Self::MONTHS_OFFSET) as i32;
        let days = (interval >> Self::DAYS_OFFSET) as i32;
        let nanos = interval as i64;
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
        let value = Some(((self.millis * Self::NANOS_IN_MILLI) as u128 & Self::NANOS_MASK) as i128);
        ScalarValue::IntervalMonthDayNano(value)
    }

    pub fn days_scalar(&self) -> ScalarValue {
        let value = Some((((self.days as u128) << Self::DAYS_OFFSET) & Self::DAYS_MASK) as i128);
        ScalarValue::IntervalMonthDayNano(value)
    }

    pub fn months_scalar(&self) -> ScalarValue {
        let value = Some(((self.months as u128) << Self::MONTHS_OFFSET) as i128);
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

    pub fn add_decomposed_to_egraph(
        self,
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) -> Id {
        let add_literal = |egraph: &mut EGraph<_, _>, scalar| {
            let id = egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                scalar,
            )));
            egraph.add(LogicalPlanLanguage::LiteralExpr([id]))
        };
        let add_binary = |egraph: &mut EGraph<_, _>, l, r| {
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
