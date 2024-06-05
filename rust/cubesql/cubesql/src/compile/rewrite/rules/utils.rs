use std::cmp::{max, min};

use chrono::{Datelike, NaiveDateTime, Timelike};
use datafusion::{
    logical_plan::{Expr, Operator},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};

use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, BinaryExprOp, LiteralExprValue, LogicalPlanLanguage,
};

use egg::{EGraph, Id};

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
    pub ms: i32,
}
impl DecomposedDayTime {
    /// DAY_BITS | MS_BITS
    const MS_BITS: i32 = 32;

    /// # Args
    /// * `interval` is value from `ScalarValue::IntervalDayTime(Some(interval))`
    pub fn from_interval(interval: i64) -> Self {
        Self {
            days: (interval >> Self::MS_BITS) as i32,
            ms: interval as i32,
        }
    }

    pub fn is_single_part(&self) -> bool {
        self.days == 0 || self.ms == 0
    }
    pub fn ms_scalar(&self) -> ScalarValue {
        let value = Some(self.ms as i64);
        ScalarValue::IntervalDayTime(value)
    }
    pub fn days_scalar(&self) -> ScalarValue {
        let value = Some((self.days as i64) << Self::MS_BITS);
        ScalarValue::IntervalDayTime(value)
    }

    pub fn create_decomposed_expr(self) -> Expr {
        match (self.days, self.ms) {
            (0, _) => Expr::Literal(self.ms_scalar()),
            (_, 0) => Expr::Literal(self.days_scalar()),
            _ => Expr::BinaryExpr {
                left: Box::new(Expr::Literal(self.days_scalar())),
                op: Operator::Plus,
                right: Box::new(Expr::Literal(self.ms_scalar())),
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

        match (self.days, self.ms) {
            (0, _) => add_literal(egraph, self.ms_scalar()),
            (_, 0) => add_literal(egraph, self.days_scalar()),
            _ => {
                let op = Operator::Plus;
                let op = egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(op)));
                let left = add_literal(egraph, self.days_scalar());
                let right = add_literal(egraph, self.ms_scalar());

                egraph.add(LogicalPlanLanguage::BinaryExpr([left, op, right]))
            }
        }
    }
}

pub struct DecomposedMonthDayNano {
    pub months: i32,
    pub days: i32,
    pub ms: i64,
}
impl DecomposedMonthDayNano {
    // MONTH_BITS | DAY_BITS | MS_BITS
    // const MONTH_BITS: i32 = 32;
    const DAY_BITS: i32 = 32;
    const MS_BITS: i32 = 64;
    const MONTH_SH: i32 = Self::DAY_BITS + Self::MS_BITS;
    const DAY_SH: i32 = Self::MS_BITS;
    const MS_TO_NS: i64 = 1_000_000;

    /// # Args
    /// * `interval` is value from `ScalarValue::IntervalMonthDayNano(Some(interval))`
    pub fn from_interval(interval: i128) -> Self {
        let interval = interval as u128;
        let months = (interval >> Self::MONTH_SH) as i32;
        let days = (interval >> Self::DAY_SH) as i32;
        let ns = interval as i64;
        let ms = ns / Self::MS_TO_NS;
        DecomposedMonthDayNano { months, days, ms }
    }

    pub fn is_single_part(&self) -> bool {
        if self.months == 0 {
            self.days == 0 || self.ms == 0
        } else {
            self.days == 0 && self.ms == 0
        }
    }
    pub fn ms_scalar(&self) -> ScalarValue {
        let value = Some((self.ms * Self::MS_TO_NS) as i128);
        ScalarValue::IntervalMonthDayNano(value)
    }
    pub fn days_scalar(&self) -> ScalarValue {
        let value = Some((self.days as i128) << Self::DAY_SH);
        ScalarValue::IntervalMonthDayNano(value)
    }
    pub fn months_scalar(&self) -> ScalarValue {
        let value = Some((self.months as i128) << Self::MONTH_SH);
        ScalarValue::IntervalMonthDayNano(value)
    }

    pub fn create_decomposed_expr(self) -> Expr {
        let bin = |l, r| Expr::BinaryExpr {
            left: Box::new(Expr::Literal(l)),
            op: Operator::Plus,
            right: Box::new(Expr::Literal(r)),
        };

        match (self.months, self.days, self.ms) {
            (0, 0, _) => Expr::Literal(self.ms_scalar()),
            (0, _, 0) => Expr::Literal(self.days_scalar()),
            (_, 0, 0) => Expr::Literal(self.months_scalar()),

            (0, _, _) => bin(self.days_scalar(), self.ms_scalar()),
            (_, 0, _) => bin(self.months_scalar(), self.ms_scalar()),
            (_, _, 0) => bin(self.months_scalar(), self.days_scalar()),

            _ => Expr::BinaryExpr {
                left: Box::new(Expr::Literal(self.months_scalar())),
                op: Operator::Plus,
                right: Box::new(bin(self.days_scalar(), self.ms_scalar())),
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

        match (self.months, self.days, self.ms) {
            (0, 0, _) => add_literal(egraph, self.ms_scalar()),
            (0, _, 0) => add_literal(egraph, self.days_scalar()),
            (_, 0, 0) => add_literal(egraph, self.months_scalar()),

            (0, _, _) => add_binary(egraph, self.days_scalar(), self.ms_scalar()),
            (_, 0, _) => add_binary(egraph, self.months_scalar(), self.ms_scalar()),
            (_, _, 0) => add_binary(egraph, self.months_scalar(), self.days_scalar()),

            _ => {
                let op = Operator::Plus;
                let op = egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(op)));
                let left = add_literal(egraph, self.months_scalar());
                let right = add_binary(egraph, self.days_scalar(), self.ms_scalar());

                egraph.add(LogicalPlanLanguage::BinaryExpr([left, op, right]))
            }
        }
    }
}
