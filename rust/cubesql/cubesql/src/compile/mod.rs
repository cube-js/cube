use std::sync::Arc;
use std::{backtrace::Backtrace, fmt};

use chrono::{prelude::*, Duration};

use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::SqlToRel;
use datafusion::variable::VarType;
use datafusion::{logical_plan::LogicalPlan, prelude::*};
use log::{debug, trace, warn};
use serde::Serialize;
use serde_json::json;
use sqlparser::ast::{self, DateTimeField, Ident, ObjectName};

use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};

use crate::mysql::dataframe;
pub use crate::schema::ctx::*;
use crate::schema::V1CubeMetaExt;

use crate::CubeError;
use crate::{
    compile::builder::QueryBuilder,
    schema::{ctx, V1CubeMetaDimensionExt, V1CubeMetaMeasureExt, V1CubeMetaSegmentExt},
};
use msql_srv::{ColumnFlags, ColumnType, StatusFlags};

use self::builder::*;
use self::context::*;
use self::engine::context::SystemVar;
use self::engine::provider::CubeContext;
use self::engine::udf::{
    create_connection_id_udf, create_convert_tz_udf, create_current_user_udf, create_db_udf,
    create_if_udf, create_instr_udf, create_isnull_udf, create_least_udf, create_locate_udf,
    create_time_format_udf, create_timediff_udf, create_ucase_udf, create_user_udf,
    create_version_udf,
};
use self::parser::parse_sql_to_statement;

pub mod builder;
pub mod context;
pub mod engine;
pub mod parser;

#[derive(Debug, PartialEq)]
pub enum CompilationError {
    Internal(String),
    User(String),
    Unsupported(String),
    Unknown(String),
}

pub type CompilationResult<T> = std::result::Result<T, CompilationError>;

impl fmt::Display for CompilationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CompilationError::User(message) => {
                write!(f, "SQLCompilationError: Internal {}", message)
            }
            CompilationError::Internal(message) => {
                write!(f, "SQLCompilationError: User {}", message)
            }
            CompilationError::Unsupported(message) => {
                write!(f, "SQLCompilationError: Unsupported {}", message)
            }
            CompilationError::Unknown(message) => {
                write!(f, "SQLCompilationError: Unknown {}", message)
            }
        }
    }
}

impl From<regex::Error> for CompilationError {
    fn from(v: regex::Error) -> Self {
        CompilationError::Internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<serde_json::Error> for CompilationError {
    fn from(v: serde_json::Error) -> Self {
        CompilationError::Internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

fn compile_select_expr(
    expr: &ast::Expr,
    ctx: &mut QueryContext,
    builder: &mut QueryBuilder,
    mb_alias: Option<String>,
) -> CompilationResult<()> {
    let selection =
        ctx.compile_selection_from_projection(expr)?
            .ok_or(CompilationError::Unknown(format!(
                "Unknown expression in SELECT statement: {}",
                expr.to_string()
            )))?;

    match selection {
        Selection::TimeDimension(dimension, granularity) => {
            if let Some(alias) = mb_alias.clone() {
                ctx.with_alias(
                    alias,
                    Selection::TimeDimension(dimension.clone(), granularity.clone()),
                );
            };

            builder.with_time_dimension(
                V1LoadRequestQueryTimeDimension {
                    dimension: dimension.name.clone(),
                    granularity: Some(granularity),
                    date_range: None,
                },
                CompiledQueryFieldMeta {
                    column_from: dimension.name.clone(),
                    column_to: mb_alias.unwrap_or(dimension.get_real_name()),
                    column_type: ColumnType::MYSQL_TYPE_STRING,
                },
            );
        }
        Selection::Measure(measure) => {
            if let Some(alias) = mb_alias.clone() {
                ctx.with_alias(alias, Selection::Measure(measure.clone()));
            };

            builder.with_measure(
                measure.name.clone(),
                CompiledQueryFieldMeta {
                    column_from: measure.name.clone(),
                    column_to: mb_alias.unwrap_or(measure.get_real_name()),
                    column_type: measure.get_mysql_type(),
                },
            );
        }
        Selection::Dimension(dimension) => {
            if let Some(alias) = mb_alias.clone() {
                ctx.with_alias(alias, Selection::Dimension(dimension.clone()));
            };

            builder.with_dimension(
                dimension.name.clone(),
                CompiledQueryFieldMeta {
                    column_from: dimension.name.clone(),
                    column_to: mb_alias.unwrap_or(dimension.get_real_name()),
                    column_type: match dimension._type.as_str() {
                        "number" => ColumnType::MYSQL_TYPE_DOUBLE,
                        _ => ColumnType::MYSQL_TYPE_STRING,
                    },
                },
            );
        }
        Selection::Segment(s) => {
            return Err(CompilationError::User(format!(
                "Unable to use segment '{}' as column in SELECT statement",
                s.get_real_name()
            )))
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
struct IntervalLiteral {
    negative: bool,
    seconds: u32,
    minutes: u32,
    hours: u32,
    days: u32,
    months: u32,
    years: u32,
}

#[derive(Debug, Clone, PartialEq)]
enum CompiledExpression {
    Selection(Selection),
    StringLiteral(String),
    DateLiteral(DateTime<Utc>),
    IntervalLiteral(IntervalLiteral),
    NumberLiteral(String, bool),
    BooleanLiteral(bool),
}

impl CompiledExpression {
    pub fn to_date_literal(&self) -> Option<CompiledExpression> {
        let date = self.to_date();
        date.map(CompiledExpression::DateLiteral)
    }

    pub fn to_date(&self) -> Option<DateTime<Utc>> {
        match self {
            CompiledExpression::DateLiteral(date) => Some(*date),
            CompiledExpression::StringLiteral(s) => {
                if let Ok(datetime) = Utc.datetime_from_str(s.as_str(), "%Y-%m-%d %H:%M:%S.%f") {
                    return Some(datetime);
                };

                if let Ok(datetime) = DateTime::parse_from_rfc3339(s.as_str()) {
                    return Some(datetime.into());
                };

                if let Ok(ref date) = NaiveDate::parse_from_str(s.as_str(), "%Y-%m-%d") {
                    return Some(
                        Utc.ymd(date.year(), date.month(), date.day())
                            .and_hms_nano(0, 0, 0, 0),
                    );
                }

                None
            }
            _ => None,
        }
    }

    pub fn to_value_as_str(&self) -> CompilationResult<String> {
        match &self {
            CompiledExpression::BooleanLiteral(v) => Ok(if *v {
                "true".to_string()
            } else {
                "false".to_string()
            }),
            CompiledExpression::StringLiteral(v) => Ok(v.clone()),
            CompiledExpression::DateLiteral(date) => {
                Ok(date.to_rfc3339_opts(SecondsFormat::Millis, true))
            }
            CompiledExpression::NumberLiteral(n, is_negative) => {
                Ok(format!("{}{}", if *is_negative { "-" } else { "" }, n))
            }
            _ => Err(CompilationError::Internal(format!(
                "Unable to convert CompiledExpression to String: {:?}",
                self
            ))),
        }
    }
}

fn compile_argument(argument: &ast::Expr) -> CompilationResult<CompiledExpression> {
    match argument {
        ast::Expr::Value(value) => match value {
            ast::Value::SingleQuotedString(format) => {
                Ok(CompiledExpression::StringLiteral(format.clone()))
            }
            _ => Err(CompilationError::Unsupported(format!(
                "Unable to compile argument: {:?}",
                argument
            ))),
        },
        _ => Err(CompilationError::Unsupported(format!(
            "Unable to compile argument: {:?}",
            argument
        ))),
    }
}

fn function_arguments_unpack2<'a>(
    f: &'a ast::Function,
    fn_name: String,
) -> CompilationResult<(&'a ast::Expr, &'a ast::Expr)> {
    match f.args.as_slice() {
        [ast::FunctionArg::Unnamed(arg1), ast::FunctionArg::Unnamed(arg2)] => Ok((arg1, arg2)),
        _ => Err(CompilationError::User(format!(
            "Unsupported signature for {} function: {:?}",
            fn_name, f
        ))),
    }
}

fn str_to_date_function(f: &ast::Function) -> CompilationResult<CompiledExpression> {
    let (date_expr, format_expr) = function_arguments_unpack2(&f, "STR_TO_DATE".to_string())?;

    let date = match compile_argument(date_expr)? {
        CompiledExpression::StringLiteral(str) => str,
        _ => {
            return Err(CompilationError::User(format!(
                "Wrong type of argument (date), must be StringLiteral: {:?}",
                f
            )))
        }
    };
    let format = match compile_argument(format_expr)? {
        CompiledExpression::StringLiteral(str) => str,
        _ => {
            return Err(CompilationError::User(format!(
                "Wrong type of argument (format), must be StringLiteral: {:?}",
                f
            )))
        }
    };

    if !format.eq("%Y-%m-%d %H:%i:%s.%f") {
        return Err(CompilationError::User(format!(
            "Wrong type of argument: {:?}",
            f
        )));
    }

    let parsed_date = Utc
        .datetime_from_str(date.as_str(), "%Y-%m-%d %H:%M:%S.%f")
        .map_err(|e| {
            CompilationError::User(format!("Unable to parse {}, err: {}", date, e.to_string(),))
        })?;

    Ok(CompiledExpression::DateLiteral(parsed_date))
}

// DATE(expr)
// Extracts the date part of the date or datetime expression expr.
fn date_function(f: &ast::Function, ctx: &QueryContext) -> CompilationResult<CompiledExpression> {
    let date_expr = match f.args.as_slice() {
        [ast::FunctionArg::Unnamed(date_expr)] => date_expr,
        _ => {
            return Err(CompilationError::User(format!(
                "Unsupported signature for DATE function: {:?}",
                f
            )));
        }
    };

    let compiled = compile_expression(&date_expr, &ctx)?;
    match compiled {
        date @ CompiledExpression::DateLiteral(_) => Ok(date),
        CompiledExpression::StringLiteral(ref input) => {
            let parsed_date = Utc
                .datetime_from_str(input.as_str(), "%Y-%m-%d %H:%M:%S.%f")
                .map_err(|e| {
                    CompilationError::User(format!(
                        "Unable to parse {}, err: {}",
                        input,
                        e.to_string(),
                    ))
                })?;

            Ok(CompiledExpression::DateLiteral(parsed_date))
        }
        _ => {
            return Err(CompilationError::User(format!(
                "Wrong type of argument (date), must be DateLiteral, actual: {:?}",
                f
            )))
        }
    }
}

fn now_function(f: &ast::Function) -> CompilationResult<CompiledExpression> {
    if f.args.len() > 1 {
        return Err(CompilationError::User(format!(
            "Unsupported signature for NOW function: {:?}",
            f
        )));
    };

    Ok(CompiledExpression::DateLiteral(Utc::now()))
}

fn date_add_function(
    f: &ast::Function,
    ctx: &QueryContext,
) -> CompilationResult<CompiledExpression> {
    let (left_expr, right_expr) = function_arguments_unpack2(&f, "DATE_ADD".to_string())?;

    let date = match compile_expression(&left_expr, &ctx)? {
        CompiledExpression::DateLiteral(str) => str,
        _ => {
            return Err(CompilationError::User(format!(
                "Wrong type of argument (date), must be DateLiteral: {:?}",
                f
            )))
        }
    };

    let interval = match compile_expression(&right_expr, &ctx)? {
        CompiledExpression::IntervalLiteral(str) => str,
        _ => {
            return Err(CompilationError::User(format!(
                "Wrong type of argument (interval), must be IntervalLiteral: {:?}",
                f
            )))
        }
    };

    let duration = if interval.seconds > 0 {
        Duration::seconds(interval.seconds as i64)
    } else if interval.minutes > 0 {
        Duration::minutes(interval.minutes as i64)
    } else if interval.hours > 0 {
        Duration::hours(interval.hours as i64)
    } else if interval.days > 0 {
        Duration::days(interval.days as i64)
    } else if interval.months > 0 {
        // @todo use real days
        Duration::days((interval.months * 30) as i64)
    } else if interval.years > 0 {
        // @todo use real years
        Duration::days((interval.years * 365) as i64)
    } else {
        return Err(CompilationError::Unsupported(format!(
            "Unsupported manipulation with interval",
        )));
    };

    Ok(CompiledExpression::DateLiteral(if interval.negative {
        date - duration
    } else {
        date + duration
    }))
}

fn compile_expression(
    expr: &ast::Expr,
    ctx: &QueryContext,
) -> CompilationResult<CompiledExpression> {
    match expr {
        ast::Expr::Identifier(ident) => {
            if let Some(selection) = ctx.find_selection_for_identifier(&ident.value, true) {
                Ok(CompiledExpression::Selection(selection))
            } else {
                Err(CompilationError::User(format!(
                    "Unable to find selection for: {:?}",
                    ident
                )))
            }
        }
        ast::Expr::CompoundIdentifier(i) => {
            // @todo We need a context with main table rel
            let identifier = if i.len() == 2 {
                i[1].value.to_string()
            } else {
                return Err(CompilationError::Unsupported(format!(
                    "Unsupported compound identifier in argument: {}",
                    expr.to_string()
                )));
            };

            if let Some(selection) = ctx.find_selection_for_identifier(&identifier, true) {
                Ok(CompiledExpression::Selection(selection))
            } else {
                Err(CompilationError::User(format!(
                    "Unable to find selection for: {:?}",
                    identifier
                )))
            }
        }
        ast::Expr::UnaryOp { expr, op } => match op {
            ast::UnaryOperator::Minus => match *expr.clone() {
                ast::Expr::Value(value) => match value {
                    ast::Value::Number(v, _) => Ok(CompiledExpression::NumberLiteral(v, true)),
                    _ => Err(CompilationError::User(format!(
                        "Unsupported value: {:?}",
                        value
                    ))),
                },
                _ => Err(CompilationError::Unsupported(format!(
                    "Unable to compile Unary Op: {:?}",
                    expr
                ))),
            },
            _ => Err(CompilationError::Unsupported(format!(
                "Unable to compile Unary Op: {:?}",
                expr
            ))),
        },
        ast::Expr::Value(val) => match val {
            ast::Value::SingleQuotedString(v) => Ok(CompiledExpression::StringLiteral(v.clone())),
            ast::Value::Number(v, _) => Ok(CompiledExpression::NumberLiteral(v.clone(), false)),
            ast::Value::Boolean(v) => Ok(CompiledExpression::BooleanLiteral(*v)),
            ast::Value::Interval {
                value,
                leading_field,
                ..
            } => {
                let (interval_value, interval_negative) = match compile_expression(&value, &ctx)? {
                    CompiledExpression::NumberLiteral(n, is_negative) => {
                        let n = n.to_string().parse::<u32>().map_err(|e| {
                            CompilationError::Unsupported(format!(
                                "Unable to parse interval value: {}",
                                e.to_string()
                            ))
                        })?;

                        (n, is_negative)
                    }
                    _ => {
                        return Err(CompilationError::User(format!(
                            "Unsupported type of Interval value, must be NumberLiteral: {:?}",
                            value
                        )))
                    }
                };

                let mut interval = IntervalLiteral {
                    negative: false,
                    seconds: 0,
                    minutes: 0,
                    hours: 0,
                    days: 0,
                    months: 0,
                    years: 0,
                };

                interval.negative = interval_negative;

                match leading_field.clone().unwrap_or(DateTimeField::Second) {
                    DateTimeField::Second => {
                        interval.seconds = interval_value;
                    }
                    DateTimeField::Minute => {
                        interval.minutes = interval_value;
                    }
                    DateTimeField::Hour => {
                        interval.hours = interval_value;
                    }
                    DateTimeField::Day => {
                        interval.days = interval_value;
                    }
                    DateTimeField::Month => {
                        interval.months = interval_value;
                    }
                    DateTimeField::Year => {
                        interval.years = interval_value;
                    }
                    _ => {
                        return Err(CompilationError::User(format!(
                            "Unsupported type of Interval, actual: {:?}",
                            leading_field
                        )))
                    }
                };

                Ok(CompiledExpression::IntervalLiteral(interval))
            }
            _ => Err(CompilationError::User(format!(
                "Unsupported value: {:?}",
                val
            ))),
        },
        ast::Expr::Function(f) => match f.name.to_string().to_lowercase().as_str() {
            "str_to_date" => str_to_date_function(&f),
            "date" => date_function(&f, &ctx),
            "date_add" => date_add_function(&f, &ctx),
            "now" => now_function(&f),
            _ => Err(CompilationError::User(format!(
                "Unsupported function: {:?}",
                f
            ))),
        },
        _ => Err(CompilationError::Unsupported(format!(
            "Unable to compile expression: {:?}",
            expr
        ))),
    }
}

fn compiled_binary_op_expr(
    left: &Box<ast::Expr>,
    op: &ast::BinaryOperator,
    right: &Box<ast::Expr>,
    ctx: &QueryContext,
) -> CompilationResult<CompiledFilterTree> {
    let left = compile_expression(left, ctx)?;
    let right = compile_expression(right, ctx)?;

    // Group selection to left, expr for filtering to right
    let (selection_to_filter, filter_expr) = match (left, right) {
        (CompiledExpression::Selection(selection), non_selection) => (selection, non_selection),
        (non_selection, CompiledExpression::Selection(selection)) => (selection, non_selection),
        // CubeSQL doesnt support BinaryExpression with literals in both sides
        (l, r) => {
            return Err(CompilationError::Unsupported(format!(
                "Unable to compile binary expression (unbound expr): ({:?}, {:?})",
                l, r
            )))
        }
    };

    let member = match selection_to_filter.clone() {
        Selection::TimeDimension(d, _) => d.name,
        Selection::Dimension(d) => d.name,
        Selection::Measure(m) => m.name,
        Selection::Segment(m) => m.name,
    };

    let compiled_filter = match selection_to_filter {
        // Compile to CompiledFilter::Filter
        Selection::Dimension(dim) => {
            let filter_expr = if dim.is_time() {
                let date = filter_expr.to_date();
                if let Some(dt) = date {
                    CompiledExpression::DateLiteral(dt)
                } else {
                    return Err(CompilationError::User(format!(
                        "Unable to compare time dimension \"{}\" with not a date value: {}",
                        dim.get_real_name(),
                        filter_expr.to_value_as_str()?
                    )));
                }
            } else {
                filter_expr
            };

            let (value, operator) = match op {
                ast::BinaryOperator::NotLike => (filter_expr, "notContains".to_string()),
                ast::BinaryOperator::Like => (filter_expr, "contains".to_string()),
                ast::BinaryOperator::Eq => (filter_expr, "equals".to_string()),
                ast::BinaryOperator::NotEq => (filter_expr, "notEquals".to_string()),
                // >=
                ast::BinaryOperator::GtEq => match filter_expr {
                    CompiledExpression::DateLiteral(_) => (filter_expr, "afterDate".to_string()),
                    _ => (filter_expr, "gte".to_string()),
                },
                // >
                ast::BinaryOperator::Gt => match filter_expr {
                    CompiledExpression::DateLiteral(dt) => (
                        CompiledExpression::DateLiteral(dt + Duration::milliseconds(1)),
                        "afterDate".to_string(),
                    ),
                    _ => (filter_expr, "gt".to_string()),
                },
                // <
                ast::BinaryOperator::Lt => match filter_expr {
                    CompiledExpression::DateLiteral(dt) => (
                        CompiledExpression::DateLiteral(dt - Duration::milliseconds(1)),
                        "beforeDate".to_string(),
                    ),
                    _ => (filter_expr, "lt".to_string()),
                },
                // <=
                ast::BinaryOperator::LtEq => match filter_expr {
                    CompiledExpression::DateLiteral(_) => (filter_expr, "beforeDate".to_string()),
                    _ => (filter_expr, "lte".to_string()),
                },
                _ => {
                    return Err(CompilationError::Unsupported(format!(
                        "Operator in binary expression: {:?}",
                        op
                    )))
                }
            };

            CompiledFilter::Filter {
                member,
                operator,
                values: Some(vec![value.to_value_as_str()?]),
            }
        }
        // Compile to CompiledFilter::SegmentFilter (it will be pushed to segments via optimization)
        Selection::Segment(_) => match op {
            ast::BinaryOperator::Eq => match filter_expr {
                CompiledExpression::BooleanLiteral(v) => {
                    if v {
                        CompiledFilter::SegmentFilter { member }
                    } else {
                        return Err(CompilationError::Unsupported(
                            "Unable to use false as value for filtering segment".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(CompilationError::Unsupported(format!(
                        "Unable to use value {:?} as value for filtering segment",
                        filter_expr
                    )));
                }
            },
            _ => {
                return Err(CompilationError::Unsupported(format!(
                    "Unable to use operator {} with segment",
                    op
                )));
            }
        },
        _ => {
            return Err(CompilationError::Unsupported(format!(
                "Unable to compile binary expression: {:?}",
                op
            )))
        }
    };

    Ok(CompiledFilterTree::Filter(compiled_filter))
}

fn binary_op_create_node_and(
    left: CompiledFilterTree,
    right: CompiledFilterTree,
) -> CompilationResult<CompiledFilterTree> {
    match [&left, &right] {
        [CompiledFilterTree::Filter(left_f), CompiledFilterTree::Filter(right_f)] => {
            match [left_f, right_f] {
                [CompiledFilter::Filter {
                    member: l_member,
                    operator: l_op,
                    values: l_v,
                }, CompiledFilter::Filter {
                    member: r_member,
                    operator: r_op,
                    values: r_v,
                }] => {
                    if l_member.eq(r_member)
                        && ((l_op.eq(&"beforeDate".to_string())
                            && r_op.eq(&"afterDate".to_string()))
                            || (l_op.eq(&"afterDate".to_string())
                                && r_op.eq(&"beforeDate".to_string())))
                    {
                        return Ok(CompiledFilterTree::Filter(CompiledFilter::Filter {
                            member: l_member.clone(),
                            operator: "inDateRange".to_string(),
                            values: Some(vec![
                                l_v.as_ref().unwrap().first().unwrap().to_string(),
                                r_v.as_ref().unwrap().first().unwrap().to_string(),
                            ]),
                        }));
                    };
                }
                _ => {}
            }
        }
        _ => {}
    };

    Ok(CompiledFilterTree::And(Box::new(left), Box::new(right)))
}

fn compiled_binary_op_logical(
    left: &Box<ast::Expr>,
    op: &ast::BinaryOperator,
    right: &Box<ast::Expr>,
    ctx: &QueryContext,
) -> CompilationResult<CompiledFilterTree> {
    let left = compile_where_expression(left, ctx)?;
    let right = compile_where_expression(right, ctx)?;

    match op {
        ast::BinaryOperator::And => Ok(binary_op_create_node_and(left, right)?),
        ast::BinaryOperator::Or => Ok(CompiledFilterTree::Or(Box::new(left), Box::new(right))),
        _ => Err(CompilationError::Unsupported(format!(
            "Unable to compiled_binary_op_logical: BinaryOp({:?}, {:?}, {:?})",
            left, op, right
        ))),
    }
}

fn compile_where_expression(
    expr: &ast::Expr,
    ctx: &QueryContext,
) -> CompilationResult<CompiledFilterTree> {
    match expr {
        // Unwrap from brackets
        ast::Expr::Nested(nested) => compile_where_expression(nested, ctx),
        ast::Expr::BinaryOp { left, right, op } => match op {
            ast::BinaryOperator::And | ast::BinaryOperator::Or => {
                compiled_binary_op_logical(left, op, right, ctx)
            }
            _ => compiled_binary_op_expr(left, op, right, ctx),
        },
        ast::Expr::IsNull(expr) => {
            let compiled_expr = compile_expression(expr, ctx)?;
            let column_for_filter = match &compiled_expr {
                CompiledExpression::Selection(selection) => match selection {
                    Selection::TimeDimension(t, _) => Ok(t),
                    Selection::Dimension(d) => Ok(d),
                    Selection::Segment(_) | Selection::Measure(_) => {
                        Err(CompilationError::User(format!(
                            "Column for IsNull must be a Dimension or TimeDimension, actual: {:?}",
                            compiled_expr
                        )))
                    }
                },
                _ => Err(CompilationError::User(format!(
                    "Column for IsNull must be a Dimension or TimeDimension, actual: {:?}",
                    compiled_expr
                ))),
            }?;

            Ok(CompiledFilterTree::Filter(CompiledFilter::Filter {
                member: column_for_filter.name.clone(),
                operator: "notSet".to_string(),
                values: None,
            }))
        }
        ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let compiled_expr = compile_expression(expr, ctx)?;
            let column_for_filter = match &compiled_expr {
                CompiledExpression::Selection(Selection::TimeDimension(t, _)) => Ok(t),
                CompiledExpression::Selection(Selection::Dimension(d)) => {
                    if d.is_time() {
                        Ok(d)
                    } else {
                        Err(CompilationError::User(format!(
                            "Column for Between must be a time dimension, actual: {:?}",
                            compiled_expr
                        )))
                    }
                }
                _ => Err(CompilationError::User(format!(
                    "Column for Between must be a time dimension, actual: {:?}",
                    compiled_expr
                ))),
            }?;

            let low_compiled = compile_expression(low, ctx)?;
            let low_compiled_date =
                low_compiled
                    .to_date_literal()
                    .ok_or(CompilationError::User(format!(
                        "Unable to compare time dimension \"{}\" with not a date value: {}",
                        column_for_filter.get_real_name(),
                        low_compiled.to_value_as_str()?
                    )))?;

            let high_compiled = compile_expression(high, ctx)?;
            let high_compiled_date =
                high_compiled
                    .to_date_literal()
                    .ok_or(CompilationError::User(format!(
                        "Unable to compare time dimension \"{}\" with not a date value: {}",
                        column_for_filter.get_real_name(),
                        high_compiled.to_value_as_str()?
                    )))?;

            Ok(CompiledFilterTree::Filter(CompiledFilter::Filter {
                member: column_for_filter.name.clone(),
                operator: if *negated {
                    "notInDateRange".to_string()
                } else {
                    "inDateRange".to_string()
                },
                values: Some(vec![
                    low_compiled_date.to_value_as_str()?,
                    high_compiled_date.to_value_as_str()?,
                ]),
            }))
        }
        ast::Expr::IsNotNull(expr) => {
            let compiled_expr = compile_expression(expr, ctx)?;
            let column_for_filter = match &compiled_expr {
                CompiledExpression::Selection(selection) => match selection {
                    Selection::TimeDimension(t, _) => Ok(t),
                    Selection::Dimension(d) => Ok(d),
                    Selection::Segment(_) | Selection::Measure(_) => {
                        Err(CompilationError::User(format!(
                            "Column for IsNull must be a Dimension or TimeDimension, actual: {:?}",
                            compiled_expr
                        )))
                    }
                },
                _ => Err(CompilationError::User(format!(
                    "Column for IsNull must be a Dimension or TimeDimension, actual: {:?}",
                    compiled_expr
                ))),
            }?;

            Ok(CompiledFilterTree::Filter(CompiledFilter::Filter {
                member: column_for_filter.name.clone(),
                operator: "set".to_string(),
                values: None,
            }))
        }
        ast::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let compiled_expr = compile_expression(expr, ctx)?;
            let column_for_filter = match &compiled_expr {
                CompiledExpression::Selection(selection) => match selection {
                    Selection::TimeDimension(t, _) => Ok(t),
                    Selection::Dimension(d) => Ok(d),
                    Selection::Segment(_) | Selection::Measure(_) => {
                        Err(CompilationError::User(format!(
                            "Column for InExpr must be a Dimension or TimeDimension, actual: {:?}",
                            compiled_expr
                        )))
                    }
                },
                _ => Err(CompilationError::User(format!(
                    "Column for InExpr must be a Dimension or TimeDimension, actual: {:?}",
                    compiled_expr
                ))),
            }?;

            fn compile_value(value: &ast::Expr, ctx: &QueryContext) -> CompilationResult<String> {
                compile_expression(value, ctx)?.to_value_as_str()
            }

            let values = list
                .iter()
                .map(|value| compile_value(value, ctx))
                .take_while(Result::is_ok)
                .map(Result::unwrap)
                .collect();

            Ok(CompiledFilterTree::Filter(CompiledFilter::Filter {
                member: column_for_filter.name.clone(),
                operator: if *negated {
                    "notEquals".to_string()
                } else {
                    "equals".to_string()
                },
                values: Some(values),
            }))
        }
        _ => Err(CompilationError::Unsupported(format!(
            "Unable to compile expression: {:?}",
            expr
        ))),
    }
}

fn optimize_where_inner_filter(
    tree: Box<CompiledFilterTree>,
    builder: &mut QueryBuilder,
) -> Option<Box<CompiledFilterTree>> {
    match *tree {
        CompiledFilterTree::Filter(ref filter) => match filter {
            CompiledFilter::Filter {
                member,
                operator,
                values,
            } => {
                if operator.eq(&"inDateRange".to_string()) {
                    let filter_pushdown = builder.push_date_range_for_time_dimension(
                        member,
                        json!(values.as_ref().unwrap()),
                    );
                    if filter_pushdown {
                        None
                    } else {
                        debug!("Unable to push down {}", member);

                        Some(tree)
                    }
                } else {
                    Some(tree)
                }
            }
            CompiledFilter::SegmentFilter { member } => {
                builder.with_segment(member.clone());

                None
            }
        },
        _ => Some(tree),
    }
}

fn optimize_where_filters(
    parent: Option<CompiledFilterTree>,
    current: CompiledFilterTree,
    builder: &mut QueryBuilder,
) -> Option<CompiledFilterTree> {
    if parent.is_none() {
        match current {
            CompiledFilterTree::And(left, right) => {
                let left_recompile = optimize_where_inner_filter(left, builder);
                let right_recompile = optimize_where_inner_filter(right, builder);

                match (left_recompile, right_recompile) {
                    (Some(l), Some(r)) => {
                        return Some(CompiledFilterTree::And(l, r));
                    }
                    (Some(l), None) => {
                        return Some(*l);
                    }
                    (None, Some(r)) => {
                        return Some(*r);
                    }
                    (None, None) => {
                        return None;
                    }
                }
            }
            CompiledFilterTree::Filter(ref filter) => {
                match filter {
                    CompiledFilter::Filter {
                        member,
                        operator,
                        values,
                    } => {
                        if operator.eq(&"inDateRange".to_string()) {
                            let filter_pushdown = builder.push_date_range_for_time_dimension(
                                member,
                                json!(values.as_ref().unwrap()),
                            );
                            if filter_pushdown {
                                return None;
                            } else {
                                debug!("Unable to push down {}", member)
                            }
                        }
                    }
                    CompiledFilter::SegmentFilter { member } => {
                        builder.with_segment(member.clone());

                        return None;
                    }
                };
            }
            _ => {}
        };
    };

    Some(current)
}

fn convert_where_filters(
    node: CompiledFilterTree,
) -> CompilationResult<Vec<V1LoadRequestQueryFilterItem>> {
    match node {
        // It's a special case for the root of CompiledFilterTree to simplify and operator without using logical and
        CompiledFilterTree::And(left, right) => {
            let mut l = convert_where_filters_unnest_and(*left)?;
            let mut r = convert_where_filters_unnest_and(*right)?;

            l.append(&mut r);

            Ok(l)
        }
        _ => Ok(vec![convert_where_filters_base(node)?]),
    }
}

fn convert_where_filters_unnest_and(
    node: CompiledFilterTree,
) -> CompilationResult<Vec<V1LoadRequestQueryFilterItem>> {
    match node {
        CompiledFilterTree::And(left, right) => {
            let mut l = convert_where_filters_unnest_and(*left)?;
            let mut r = convert_where_filters_unnest_and(*right)?;

            l.append(&mut r);

            Ok(l)
        }
        _ => Ok(vec![convert_where_filters_base(node)?]),
    }
}

fn convert_where_filters_unnest_or(
    node: CompiledFilterTree,
) -> CompilationResult<Vec<V1LoadRequestQueryFilterItem>> {
    match node {
        CompiledFilterTree::Or(left, right) => {
            let mut l = convert_where_filters_unnest_or(*left)?;
            let mut r = convert_where_filters_unnest_or(*right)?;

            l.append(&mut r);

            Ok(l)
        }
        _ => Ok(vec![convert_where_filters_base(node)?]),
    }
}

fn convert_where_filters_base(
    node: CompiledFilterTree,
) -> CompilationResult<V1LoadRequestQueryFilterItem> {
    match node {
        CompiledFilterTree::Filter(filter) => match filter {
            CompiledFilter::Filter {
                member,
                operator,
                values,
            } => Ok(V1LoadRequestQueryFilterItem {
                member: Some(member),
                operator: Some(operator),
                values,
                or: None,
                and: None,
            }),
            CompiledFilter::SegmentFilter { member: _ } => Err(CompilationError::Internal(
                "Unable to compile segments, it should be pushed down to segments".to_string(),
            )),
        },
        CompiledFilterTree::And(left, right) => {
            let mut l = convert_where_filters_unnest_and(*left)?;
            let mut r = convert_where_filters_unnest_and(*right)?;

            l.append(&mut r);

            Ok(V1LoadRequestQueryFilterItem {
                member: None,
                operator: None,
                values: None,
                or: None,
                and: Some(l.iter().map(|filter| json!(filter)).collect::<_>()),
            })
        }
        CompiledFilterTree::Or(left, right) => {
            let mut l = convert_where_filters_unnest_or(*left)?;
            let mut r = convert_where_filters_unnest_or(*right)?;

            l.append(&mut r);

            Ok(V1LoadRequestQueryFilterItem {
                member: None,
                operator: None,
                values: None,
                or: Some(l.iter().map(|filter| json!(filter)).collect::<_>()),
                and: None,
            })
        }
    }
}

#[derive(Debug, Clone)]
enum CompiledFilter {
    Filter {
        member: String,
        operator: String,
        values: Option<Vec<String>>,
    },
    SegmentFilter {
        member: String,
    },
}

#[derive(Debug, Clone)]
enum CompiledFilterTree {
    Filter(CompiledFilter),
    And(Box<CompiledFilterTree>, Box<CompiledFilterTree>),
    Or(Box<CompiledFilterTree>, Box<CompiledFilterTree>),
}

fn compile_group(
    grouping: &Vec<ast::Expr>,
    ctx: &QueryContext,
    _builder: &mut QueryBuilder,
) -> CompilationResult<()> {
    for group in grouping.iter() {
        match &group {
            ast::Expr::Identifier(i) => {
                if let Some(selection) = ctx.find_selection_for_identifier(&i.to_string(), true) {
                    match selection {
                        Selection::Segment(s) => {
                            return Err(CompilationError::User(format!(
                                "Unable to use segment '{}' in GROUP BY",
                                s.get_real_name()
                            )));
                        }
                        _ => {}
                    }
                };
            }
            _ => {}
        }
    }

    Ok(())
}

fn compile_where(
    selection: &ast::Expr,
    ctx: &QueryContext,
    builder: &mut QueryBuilder,
) -> CompilationResult<()> {
    let filters = match &selection {
        binary @ ast::Expr::BinaryOp { left, right, op } => match op {
            ast::BinaryOperator::Like
            | ast::BinaryOperator::NotLike
            | ast::BinaryOperator::Lt
            | ast::BinaryOperator::LtEq
            | ast::BinaryOperator::Gt
            | ast::BinaryOperator::GtEq
            | ast::BinaryOperator::Eq
            | ast::BinaryOperator::NotEq => compile_where_expression(binary, ctx)?,
            ast::BinaryOperator::And => {
                let left_compiled = compile_where_expression(left, ctx)?;
                let right_compiled = compile_where_expression(right, ctx)?;

                binary_op_create_node_and(left_compiled, right_compiled)?
            }
            ast::BinaryOperator::Or => {
                let left_compiled = compile_where_expression(left, ctx)?;
                let right_compiled = compile_where_expression(right, ctx)?;

                CompiledFilterTree::Or(Box::new(left_compiled), Box::new(right_compiled))
            }
            _ => {
                return Err(CompilationError::Unsupported(format!(
                    "Operator for binary expression in WHERE clause: {:?}",
                    selection
                )));
            }
        },
        ast::Expr::Nested(nested) => compile_where_expression(nested, ctx)?,
        inlist @ ast::Expr::InList { .. } => compile_where_expression(inlist, ctx)?,
        isnull @ ast::Expr::IsNull { .. } => compile_where_expression(isnull, ctx)?,
        isnotnull @ ast::Expr::IsNotNull { .. } => compile_where_expression(isnotnull, ctx)?,
        between @ ast::Expr::Between { .. } => compile_where_expression(between, ctx)?,
        _ => {
            return Err(CompilationError::Unsupported(format!(
                "Expression in WHERE clause: {:?}",
                selection
            )));
        }
    };

    trace!("Filters (before optimization): {:?}", filters);

    let filters = optimize_where_filters(None, filters, builder);
    trace!("Filters (after optimization): {:?}", filters);

    if let Some(optimized_filter) = filters {
        builder.with_filters(convert_where_filters(optimized_filter)?);
    }

    Ok(())
}

fn compile_order(
    order_by: &Vec<ast::OrderByExpr>,
    ctx: &QueryContext,
    builder: &mut QueryBuilder,
) -> CompilationResult<()> {
    if order_by.is_empty() {
        return Ok(());
    };

    for order_expr in order_by.iter() {
        let order_selection = ctx
            .compile_selection(&order_expr.expr.clone())?
            .ok_or_else(|| {
                CompilationError::Unsupported(format!(
                    "Unsupported expression in order: {:?}",
                    order_expr.expr
                ))
            })?;

        let direction_as_str = if let Some(direction) = order_expr.asc {
            if direction {
                "asc".to_string()
            } else {
                "desc".to_string()
            }
        } else {
            "asc".to_string()
        };

        match order_selection {
            Selection::Dimension(d) => builder.with_order(vec![d.name.clone(), direction_as_str]),
            Selection::Measure(m) => builder.with_order(vec![m.name.clone(), direction_as_str]),
            Selection::TimeDimension(t, _) => {
                builder.with_order(vec![t.name.clone(), direction_as_str])
            }
            Selection::Segment(s) => {
                return Err(CompilationError::User(format!(
                    "Unable to use segment '{}' in ORDER BY",
                    s.get_real_name()
                )));
            }
        };
    }

    Ok(())
}

fn compile_select(expr: &ast::Select, ctx: &mut QueryContext) -> CompilationResult<QueryBuilder> {
    let mut builder = QueryBuilder::new();

    if !expr.projection.is_empty() {
        for projection in expr.projection.iter() {
            match projection {
                ast::SelectItem::Wildcard => {
                    for dimension in ctx.meta.dimensions.iter() {
                        builder.with_dimension(
                            dimension.name.clone(),
                            CompiledQueryFieldMeta {
                                column_from: dimension.name.clone(),
                                column_to: dimension.get_real_name(),
                                column_type: match dimension._type.as_str() {
                                    "number" => ColumnType::MYSQL_TYPE_DOUBLE,
                                    _ => ColumnType::MYSQL_TYPE_STRING,
                                },
                            },
                        )
                    }
                }
                ast::SelectItem::UnnamedExpr(expr) => {
                    compile_select_expr(expr, ctx, &mut builder, None)?
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    compile_select_expr(expr, ctx, &mut builder, Some(alias.value.to_string()))?
                }
                _ => {
                    return Err(CompilationError::Unsupported(format!(
                        "Unsupported expression in projection: {:?}",
                        projection
                    )));
                }
            }
        }
    }

    Ok(builder)
}

#[derive(Debug)]
pub struct QueryPlannerExecutionProps {
    connection_id: u32,
    user: Option<String>,
    database: Option<String>,
}

impl QueryPlannerExecutionProps {
    pub fn new(connection_id: u32, user: Option<String>, database: Option<String>) -> Self {
        Self {
            connection_id,
            user,
            database,
        }
    }

    pub fn set_user(&mut self, user: Option<String>) {
        self.user = user;
    }
}

impl QueryPlannerExecutionProps {
    pub fn connection_id(&self) -> u32 {
        self.connection_id
    }
}

struct QueryPlanner {
    context: Arc<ctx::TenantContext>,
}

impl QueryPlanner {
    pub fn new(context: Arc<ctx::TenantContext>) -> Self {
        Self { context }
    }

    /// Common case for both planners: meta & olap
    /// This method tries to detect what planner to use as earlier as possible
    /// and forward context to correct planner
    pub fn select_to_plan(
        &self,
        stmt: &ast::Statement,
        q: &Box<ast::Query>,
        props: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        let select = match &q.body {
            sqlparser::ast::SetExpr::Select(select) => select,
            _ => {
                return Err(CompilationError::Unsupported(
                    "Unsupported Query".to_string(),
                ));
            }
        };

        let from_table = if select.from.len() == 1 {
            &select.from[0]
        } else {
            return self.create_df_logical_plan(stmt.clone(), props);
        };

        let (schema_name, table_name) = match &from_table.relation {
            ast::TableFactor::Table { name, .. } => match name {
                ast::ObjectName(identifiers) => {
                    if identifiers.len() == 2 {
                        // db.`KibanaSampleDataEcommerce`
                        (identifiers[0].value.clone(), identifiers[1].value.clone())
                    } else if identifiers.len() == 1 {
                        // `KibanaSampleDataEcommerce`
                        ("db".to_string(), identifiers[0].value.clone())
                    } else {
                        return Err(CompilationError::Unsupported(
                            "Query with multiple tables in from".to_string(),
                        ));
                    }
                }
            },
            factor => {
                return Err(CompilationError::Unsupported(format!(
                    "table factor: {:?}",
                    factor
                )));
            }
        };

        if schema_name.to_lowercase() == "information_schema"
            || schema_name.to_lowercase() == "performance_schema"
        {
            return self.create_df_logical_plan(stmt.clone(), props);
        }

        if !select.from[0].joins.is_empty() {
            return Err(CompilationError::Unsupported(
                "Query with JOIN instruction(s)".to_string(),
            ));
        }

        if q.with.is_some() {
            return Err(CompilationError::Unsupported(
                "Query with CTE instruction(s)".to_string(),
            ));
        }

        if !select.cluster_by.is_empty() {
            return Err(CompilationError::Unsupported(
                "Query with CLUSTER BY instruction(s)".to_string(),
            ));
        }

        if !select.distribute_by.is_empty() {
            return Err(CompilationError::Unsupported(
                "Query with DISTRIBUTE BY instruction(s)".to_string(),
            ));
        }

        if select.having.is_some() {
            return Err(CompilationError::Unsupported(
                "Query with HAVING instruction(s)".to_string(),
            ));
        }

        if schema_name.to_lowercase() != "db" {
            return Err(CompilationError::Unsupported(format!(
                "Unable to access schema {}",
                schema_name
            )));
        }

        // @todo Better solution?
        // Metabase
        if q.to_string()
            == format!(
                "SELECT true AS `_` FROM `{}` WHERE 1 <> 1 LIMIT 0",
                table_name
            )
        {
            return Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Arc::new(dataframe::DataFrame::new(
                    vec![dataframe::Column::new(
                        "_".to_string(),
                        ColumnType::MYSQL_TYPE_TINY,
                        ColumnFlags::empty(),
                    )],
                    vec![],
                )),
            ));
        };

        if let Some(cube) = self.context.find_cube_with_name(table_name.clone()) {
            let mut ctx = QueryContext::new(&cube);
            let mut builder = compile_select(select, &mut ctx)?;

            if let Some(limit_expr) = &q.limit {
                let limit = limit_expr.to_string().parse::<i32>().map_err(|e| {
                    CompilationError::Unsupported(format!(
                        "Unable to parse limit: {}",
                        e.to_string()
                    ))
                })?;

                builder.with_limit(limit);
            }

            if let Some(offset_expr) = &q.offset {
                let offset = offset_expr.value.to_string().parse::<i32>().map_err(|e| {
                    CompilationError::Unsupported(format!(
                        "Unable to parse offset: {}",
                        e.to_string()
                    ))
                })?;

                builder.with_offset(offset);
            }

            compile_group(&select.group_by, &ctx, &mut builder)?;
            compile_order(&q.order_by, &ctx, &mut builder)?;

            if let Some(selection) = &select.selection {
                compile_where(selection, &ctx, &mut builder)?;
            }

            Ok(QueryPlan::CubeSelect(StatusFlags::empty(), builder.build()))
        } else {
            Err(CompilationError::Unknown(format!(
                "Unknown cube '{}'. Please ensure your schema files are valid.",
                table_name,
            )))
        }
    }

    pub fn plan(
        &self,
        stmt: &ast::Statement,
        props: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        match stmt {
            ast::Statement::Query(q) => self.select_to_plan(stmt, q, props),
            ast::Statement::SetTransaction { .. } => Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Arc::new(dataframe::DataFrame::new(vec![], vec![])),
            )),
            ast::Statement::SetNames { charset_name, .. } => {
                if !(charset_name.eq_ignore_ascii_case("utf8")
                    || charset_name.eq_ignore_ascii_case("utf8mb4"))
                {
                    warn!(
                        "SET NAME does not support non utf8 charsets, input: {}",
                        charset_name
                    );
                };

                Ok(QueryPlan::MetaTabular(
                    StatusFlags::empty(),
                    Arc::new(dataframe::DataFrame::new(vec![], vec![])),
                ))
            }
            ast::Statement::Kill { .. } => Ok(QueryPlan::MetaOk(StatusFlags::empty())),
            ast::Statement::SetVariable { .. } => Ok(QueryPlan::MetaOk(StatusFlags::empty())),
            ast::Statement::ShowVariable { variable } => {
                self.show_variable_to_plan(variable, props)
            }
            ast::Statement::ShowVariables { filter } => self.show_variables_to_plan(&filter, props),
            ast::Statement::ShowCreate { obj_name, obj_type } => {
                self.show_create_to_plan(&obj_name, &obj_type)
            }
            ast::Statement::ShowColumns { .. } => self.create_df_logical_plan(stmt.clone(), props),
            _ => Err(CompilationError::Unsupported(format!(
                "Unsupported query type: {}",
                stmt.to_string()
            ))),
        }
    }

    fn show_variable_to_plan(
        &self,
        variable: &Vec<Ident>,
        props: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        let name = ObjectName(variable.to_vec()).to_string();
        if name.eq_ignore_ascii_case("databases") || name.eq_ignore_ascii_case("schemas") {
            Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Arc::new(dataframe::DataFrame::new(
                    vec![dataframe::Column::new(
                        "Database".to_string(),
                        ColumnType::MYSQL_TYPE_STRING,
                        ColumnFlags::empty(),
                    )],
                    vec![
                        dataframe::Row::new(vec![dataframe::TableValue::String("db".to_string())]),
                        dataframe::Row::new(vec![dataframe::TableValue::String(
                            "information_schema".to_string(),
                        )]),
                        dataframe::Row::new(vec![dataframe::TableValue::String(
                            "mysql".to_string(),
                        )]),
                        dataframe::Row::new(vec![dataframe::TableValue::String(
                            "performance_schema".to_string(),
                        )]),
                        dataframe::Row::new(vec![dataframe::TableValue::String("sys".to_string())]),
                    ],
                )),
            ))
        } else if name.eq_ignore_ascii_case("warnings") {
            Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Arc::new(dataframe::DataFrame::new(
                    vec![
                        dataframe::Column::new(
                            "Level".to_string(),
                            ColumnType::MYSQL_TYPE_VAR_STRING,
                            ColumnFlags::NOT_NULL_FLAG,
                        ),
                        dataframe::Column::new(
                            "Code".to_string(),
                            ColumnType::MYSQL_TYPE_LONG,
                            ColumnFlags::NOT_NULL_FLAG | ColumnFlags::UNSIGNED_FLAG,
                        ),
                        dataframe::Column::new(
                            "Message".to_string(),
                            ColumnType::MYSQL_TYPE_VAR_STRING,
                            ColumnFlags::NOT_NULL_FLAG,
                        ),
                    ],
                    vec![],
                )),
            ))
        } else {
            self.create_df_logical_plan(
                ast::Statement::ShowVariable {
                    variable: variable.clone(),
                },
                props,
            )
        }
    }

    fn show_variables_to_plan(
        &self,
        filter: &Option<ast::ShowStatementFilter>,
        props: &QueryPlannerExecutionProps,
    ) -> Result<QueryPlan, CompilationError> {
        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE VARIABLE_NAME {}", stmt.to_string())
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                return Err(CompilationError::Unsupported(format!(
                    "Show variable doesnt support WHERE statement: {}",
                    stmt
                )))
            }
            Some(stmt @ ast::ShowStatementFilter::ILike(_)) => {
                return Err(CompilationError::User(format!(
                    "Show variable doesnt define ILIKE statement: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let stmt = parse_sql_to_statement(
            &format!("SELECT VARIABLE_NAME as Variable_name, VARIABLE_VALUE as Value FROM performance_schema.session_variables {} ORDER BY Variable_name DESC", filter)
        )?;

        self.create_df_logical_plan(stmt, props)
    }

    fn show_create_to_plan(
        &self,
        obj_name: &ObjectName,
        obj_type: &ast::ShowCreateObject,
    ) -> Result<QueryPlan, CompilationError> {
        match obj_type {
            ast::ShowCreateObject::Table => {}
            _ => {
                return Err(CompilationError::User(format!(
                    "SHOW CREATE doesn't support type: {}",
                    obj_type
                )))
            }
        };

        let table_name_filter = if obj_name.0.len() == 2 {
            &obj_name.0[1].value
        } else {
            &obj_name.0[0].value
        };

        self.context.cubes.iter().find(|c| c.name.eq(table_name_filter)).map(|cube| {
            let mut fields: Vec<String> = vec![];

            for column in &cube.get_columns() {
                fields.push(format!(
                    "`{}` {}{}",
                    column.get_name(),
                    column.get_column_type(),
                    if column.mysql_can_be_null() { " NOT NULL" } else { "" }
                ));
            }

            QueryPlan::MetaTabular(StatusFlags::empty(), Arc::new(dataframe::DataFrame::new(
                vec![
                    dataframe::Column::new(
                        "Table".to_string(),
                        ColumnType::MYSQL_TYPE_STRING,
                        ColumnFlags::empty(),
                    ),
                    dataframe::Column::new(
                        "Create Table".to_string(),
                        ColumnType::MYSQL_TYPE_STRING,
                        ColumnFlags::empty(),
                    )
                ],
                vec![dataframe::Row::new(vec![
                    dataframe::TableValue::String(cube.name.clone()),
                    dataframe::TableValue::String(
                        format!("CREATE TABLE `{}` (\r\n  {}\r\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", cube.name, fields.join(",\r\n  "))
                    ),
                ])]
            )))
        }).ok_or(
            CompilationError::User(format!(
                "Unknown table: {}",
                table_name_filter
            ))
        )
    }

    fn create_df_logical_plan(
        &self,
        stmt: ast::Statement,
        props: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        let mut ctx =
            ExecutionContext::with_config(ExecutionConfig::new().with_information_schema(false));

        let variable_provider = SystemVar::new();
        ctx.register_variable(VarType::System, Arc::new(variable_provider));

        ctx.register_udf(create_version_udf());
        ctx.register_udf(create_db_udf("database".to_string(), props));
        ctx.register_udf(create_db_udf("schema".to_string(), props));
        ctx.register_udf(create_connection_id_udf(props));
        ctx.register_udf(create_user_udf(props));
        ctx.register_udf(create_current_user_udf(props));
        ctx.register_udf(create_instr_udf());
        ctx.register_udf(create_ucase_udf());
        ctx.register_udf(create_isnull_udf());
        ctx.register_udf(create_if_udf());
        ctx.register_udf(create_least_udf());
        ctx.register_udf(create_convert_tz_udf());
        ctx.register_udf(create_timediff_udf());
        ctx.register_udf(create_time_format_udf());
        ctx.register_udf(create_locate_udf());

        let state = ctx.state.lock().unwrap().clone();
        let cube_ctx = CubeContext::new(&state, &self.context.cubes);
        let df_query_planner = SqlToRel::new(&cube_ctx);

        let plan = df_query_planner
            .statement_to_plan(&DFStatement::Statement(stmt))
            .map_err(|err| {
                CompilationError::Internal(format!("Initial planning error: {}", err))
            })?;

        let optimized_plan = ctx.optimize(&plan).map_err(|err| {
            CompilationError::Internal(format!("Planning optimization error: {}", err))
        })?;

        Ok(QueryPlan::DataFushionSelect(
            StatusFlags::empty(),
            optimized_plan,
            ctx,
        ))
    }
}

pub fn convert_statement_to_cube_query(
    stmt: &ast::Statement,
    tenant_ctx: Arc<ctx::TenantContext>,
    props: &QueryPlannerExecutionProps,
) -> CompilationResult<QueryPlan> {
    let planner = QueryPlanner::new(tenant_ctx);
    planner.plan(stmt, props)
}

#[derive(Debug, PartialEq, Serialize)]
pub struct CompiledQuery {
    pub request: V1LoadRequestQuery,
    pub meta: Vec<CompiledQueryFieldMeta>,
}

pub enum QueryPlan {
    // Meta will not be executed in DF,
    // we already knows how respond to it
    MetaOk(StatusFlags),
    MetaTabular(StatusFlags, Arc<dataframe::DataFrame>),
    // Query will be executed via Data Fusion
    DataFushionSelect(StatusFlags, LogicalPlan, ExecutionContext),
    // Query will be executed by direct request in Cube.js
    CubeSelect(StatusFlags, CompiledQuery),
}

impl QueryPlan {
    pub fn print(&self, pretty: bool) -> Result<String, CubeError> {
        match self {
            QueryPlan::DataFushionSelect(_, plan, _) => {
                if pretty {
                    Ok(plan.display_indent().to_string())
                } else {
                    Ok(plan.display().to_string())
                }
            }
            QueryPlan::CubeSelect(_, compiled_query) => {
                if pretty {
                    Ok(serde_json::to_string_pretty(&compiled_query)?)
                } else {
                    Ok(serde_json::to_string(&compiled_query)?)
                }
            }
            QueryPlan::MetaOk(_) | QueryPlan::MetaTabular(_, _) => Ok(
                "This query doesnt have a plan, because it already has values for response"
                    .to_string(),
            ),
        }
    }
}

pub fn convert_sql_to_cube_query(
    query: &String,
    tenant: Arc<ctx::TenantContext>,
    props: &QueryPlannerExecutionProps,
) -> CompilationResult<QueryPlan> {
    // @todo Support without workarounds
    // metabase
    let query = query.clone().replace("IF(TABLE_TYPE='BASE TABLE' or TABLE_TYPE='SYSTEM VERSIONED', 'TABLE', TABLE_TYPE) as TABLE_TYPE", "TABLE_TYPE");
    let query = query.replace("ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME", "");
    // @todo Implement CONVERT function
    let query = query.replace("CONVERT (CASE DATA_TYPE WHEN 'year' THEN NUMERIC_SCALE WHEN 'tinyint' THEN 0 ELSE NUMERIC_SCALE END, UNSIGNED INTEGER)", "0");
    // @todo Case intensive mode
    let query = query.replace("CASE data_type", "CASE DATA_TYPE");
    let query = query.replace("CASE update_rule    WHEN", "CASE UPDATE_RULE WHEN");
    // @todo problem with parser, space in types
    let query = query.replace("signed integer", "bigint");
    let query = query.replace("SIGNED INTEGER", "bigint");
    let query = query.replace("unsigned integer", "bigint");
    let query = query.replace("UNSIGNED INTEGER", "bigint");

    let stmt = parse_sql_to_statement(&query)?;
    convert_statement_to_cube_query(&stmt, tenant, props)
}

#[cfg(test)]
mod tests {
    use cubeclient::models::{
        V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment,
    };

    use crate::mysql::dataframe::batch_to_dataframe;
    use datafusion::execution::dataframe_impl::DataFrameImpl;

    use super::*;
    use pretty_assertions::assert_eq;

    fn get_test_meta() -> Vec<V1CubeMeta> {
        vec![
            V1CubeMeta {
                name: "KibanaSampleDataEcommerce".to_string(),
                title: None,
                dimensions: vec![
                    V1CubeMetaDimension {
                        name: "KibanaSampleDataEcommerce.order_date".to_string(),
                        _type: "time".to_string(),
                    },
                    V1CubeMetaDimension {
                        name: "KibanaSampleDataEcommerce.customer_gender".to_string(),
                        _type: "string".to_string(),
                    },
                    V1CubeMetaDimension {
                        name: "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        _type: "number".to_string(),
                    },
                ],
                measures: vec![
                    V1CubeMetaMeasure {
                        name: "KibanaSampleDataEcommerce.count".to_string(),
                        title: None,
                        _type: "number".to_string(),
                        agg_type: Some("count".to_string()),
                    },
                    V1CubeMetaMeasure {
                        name: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                        title: None,
                        _type: "number".to_string(),
                        agg_type: Some("max".to_string()),
                    },
                    V1CubeMetaMeasure {
                        name: "KibanaSampleDataEcommerce.minPrice".to_string(),
                        title: None,
                        _type: "number".to_string(),
                        agg_type: Some("min".to_string()),
                    },
                    V1CubeMetaMeasure {
                        name: "KibanaSampleDataEcommerce.avgPrice".to_string(),
                        title: None,
                        _type: "number".to_string(),
                        agg_type: Some("avg".to_string()),
                    },
                ],
                segments: vec![
                    V1CubeMetaSegment {
                        name: "KibanaSampleDataEcommerce.is_male".to_string(),
                        title: "Ecommerce Male".to_string(),
                        short_title: "Male".to_string(),
                    },
                    V1CubeMetaSegment {
                        name: "KibanaSampleDataEcommerce.is_female".to_string(),
                        title: "Ecommerce Female".to_string(),
                        short_title: "Female".to_string(),
                    },
                ],
            },
            V1CubeMeta {
                name: "Logs".to_string(),
                title: None,
                dimensions: vec![],
                measures: vec![
                    V1CubeMetaMeasure {
                        name: "Logs.agentCount".to_string(),
                        title: None,
                        _type: "number".to_string(),
                        agg_type: Some("countDistinct".to_string()),
                    },
                    V1CubeMetaMeasure {
                        name: "Logs.agentCountApprox".to_string(),
                        title: None,
                        _type: "number".to_string(),
                        agg_type: Some("countDistinctApprox".to_string()),
                    },
                ],
                segments: vec![],
            },
        ]
    }

    fn get_test_tenant_ctx() -> Arc<ctx::TenantContext> {
        Arc::new(ctx::TenantContext {
            cubes: get_test_meta(),
        })
    }

    fn convert_simple_select(query: String) -> CompiledQuery {
        let query = convert_sql_to_cube_query(
            &query,
            get_test_tenant_ctx(),
            &QueryPlannerExecutionProps {
                connection_id: 8,
                user: Some("ovr".to_string()),
                database: None,
            },
        );
        match query.unwrap() {
            QueryPlan::CubeSelect(_, query) => query,
            _ => panic!("Must return CubeSelect instead of DF plan"),
        }
    }

    #[test]
    fn test_select_measure_via_function() {
        let query = convert_simple_select(
            "SELECT MEASURE(maxPrice), MEASURE(minPrice), MEASURE(avgPrice) FROM KibanaSampleDataEcommerce".to_string(),
        );

        assert_eq!(
            query,
            CompiledQuery {
                request: V1LoadRequestQuery {
                    measures: Some(vec![
                        "KibanaSampleDataEcommerce.maxPrice".to_string(),
                        "KibanaSampleDataEcommerce.minPrice".to_string(),
                        "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    ]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                },
                meta: vec![
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                        column_to: "maxPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    },
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.minPrice".to_string(),
                        column_to: "minPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    },
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.avgPrice".to_string(),
                        column_to: "avgPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    },
                ]
            }
        )
    }

    #[test]
    fn test_select_compound_identifiers() {
        let query = convert_simple_select(
            "SELECT MEASURE(`KibanaSampleDataEcommerce`.`maxPrice`) AS maxPrice, `KibanaSampleDataEcommerce`.`minPrice` AS minPrice FROM KibanaSampleDataEcommerce".to_string(),
        );

        assert_eq!(
            query,
            CompiledQuery {
                request: V1LoadRequestQuery {
                    measures: Some(vec![
                        "KibanaSampleDataEcommerce.maxPrice".to_string(),
                        "KibanaSampleDataEcommerce.minPrice".to_string(),
                    ]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                },
                meta: vec![
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                        column_to: "maxPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    },
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.minPrice".to_string(),
                        column_to: "minPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    }
                ]
            }
        )
    }

    #[test]
    fn test_select_measure_aggregate_functions() {
        let query = convert_simple_select(
            "SELECT MAX(maxPrice), MIN(minPrice), AVG(avgPrice) FROM KibanaSampleDataEcommerce"
                .to_string(),
        );

        assert_eq!(
            query,
            CompiledQuery {
                request: V1LoadRequestQuery {
                    measures: Some(vec![
                        "KibanaSampleDataEcommerce.maxPrice".to_string(),
                        "KibanaSampleDataEcommerce.minPrice".to_string(),
                        "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    ]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                },
                meta: vec![
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                        column_to: "maxPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    },
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.minPrice".to_string(),
                        column_to: "minPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    },
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.avgPrice".to_string(),
                        column_to: "avgPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    },
                ]
            }
        )
    }

    #[test]
    fn test_order_alias_for_measure_default() {
        let query = convert_simple_select(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce ORDER BY cnt".to_string(),
        );

        assert_eq!(
            query.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[test]
    fn test_order_by() {
        let supported_orders = vec![
            // test_order_alias_for_dimension_default
            (
                "SELECT taxful_total_price as total_price FROM KibanaSampleDataEcommerce ORDER BY total_price".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_indentifier_default
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_compound_identifier_default
            (
                "SELECT taxful_total_price FROM `KibanaSampleDataEcommerce` ORDER BY `KibanaSampleDataEcommerce`.`taxful_total_price`".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_indentifier_asc
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price ASC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "asc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_indentifier_desc
            (
                "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_identifer_alias_ident_no_escape
            (
                "SELECT taxful_total_price as alias1 FROM KibanaSampleDataEcommerce ORDER BY alias1 DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
            // test_order_identifer_alias_ident_escape
            (
                "SELECT taxful_total_price as `alias1` FROM KibanaSampleDataEcommerce ORDER BY `alias1` DESC".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                        "desc".to_string(),
                    ]]),
                    limit: None,
                    offset: None,
                    filters: None
                }
            ),
        ];

        for (sql, expected_request) in supported_orders.iter() {
            let query = convert_simple_select(sql.to_string());

            assert_eq!(&query.request, expected_request)
        }
    }

    #[test]
    fn test_order_function_date() {
        let query = convert_simple_select(
            "SELECT DATE(order_date) FROM KibanaSampleDataEcommerce ORDER BY DATE(order_date) DESC"
                .to_string(),
        );

        assert_eq!(
            query.request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_owned()),
                    date_range: None
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[test]
    fn test_select_all_fields_by_asterisk_limit_100() {
        let query =
            convert_simple_select("SELECT * FROM KibanaSampleDataEcommerce LIMIT 100".to_string());

        assert_eq!(
            query.request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: Some(100),
                offset: None,
                filters: None
            }
        )
    }

    #[test]
    fn test_select_all_fields_by_asterisk_limit_100_offset_50() {
        let query = convert_simple_select(
            "SELECT * FROM KibanaSampleDataEcommerce LIMIT 100 OFFSET 50".to_string(),
        );

        assert_eq!(
            query.request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: Some(100),
                offset: Some(50),
                filters: None
            }
        )
    }

    #[test]
    fn test_select_two_fields() {
        let query = convert_simple_select(
            "SELECT order_date, customer_gender FROM KibanaSampleDataEcommerce".to_string(),
        );

        assert_eq!(
            query.request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[test]
    fn test_select_fields_alias() {
        let query = convert_simple_select(
            "SELECT order_date as order_date, customer_gender as customer_gender FROM KibanaSampleDataEcommerce"
                .to_string(),
        );

        assert_eq!(
            query,
            CompiledQuery {
                request: V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.order_date".to_string(),
                        "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    ]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
                meta: vec![
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.order_date".to_string(),
                        column_to: "order_date".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_STRING,
                    },
                    CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.customer_gender".to_string(),
                        column_to: "customer_gender".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_STRING,
                    }
                ]
            }
        )
    }

    #[test]
    fn test_select_aggregations() {
        let variants = vec![
            (
                "SELECT COUNT(*) FROM KibanaSampleDataEcommerce".to_string(),
                CompiledQuery {
                    request: V1LoadRequestQuery {
                        measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                        dimensions: Some(vec![]),
                        segments: Some(vec![]),
                        time_dimensions: None,
                        order: None,
                        limit: None,
                        offset: None,
                        filters: None,
                    },
                    meta: vec![CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.count".to_string(),
                        column_to: "count".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_LONGLONG,
                    }],
                },
            ),
            (
                "SELECT COUNT(DISTINCT agentCount) FROM Logs".to_string(),
                CompiledQuery {
                    request: V1LoadRequestQuery {
                        measures: Some(vec!["Logs.agentCount".to_string()]),
                        dimensions: Some(vec![]),
                        segments: Some(vec![]),
                        time_dimensions: None,
                        order: None,
                        limit: None,
                        offset: None,
                        filters: None,
                    },
                    meta: vec![CompiledQueryFieldMeta {
                        column_from: "Logs.agentCount".to_string(),
                        column_to: "agentCount".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    }],
                },
            ),
            (
                "SELECT COUNT(DISTINCT agentCountApprox) FROM Logs".to_string(),
                CompiledQuery {
                    request: V1LoadRequestQuery {
                        measures: Some(vec!["Logs.agentCountApprox".to_string()]),
                        dimensions: Some(vec![]),
                        segments: Some(vec![]),
                        time_dimensions: None,
                        order: None,
                        limit: None,
                        offset: None,
                        filters: None,
                    },
                    meta: vec![CompiledQueryFieldMeta {
                        column_from: "Logs.agentCountApprox".to_string(),
                        column_to: "agentCountApprox".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    }],
                },
            ),
            (
                "SELECT MAX(`maxPrice`) FROM KibanaSampleDataEcommerce".to_string(),
                CompiledQuery {
                    request: V1LoadRequestQuery {
                        measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                        dimensions: Some(vec![]),
                        segments: Some(vec![]),
                        time_dimensions: None,
                        order: None,
                        limit: None,
                        offset: None,
                        filters: None,
                    },
                    meta: vec![CompiledQueryFieldMeta {
                        column_from: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                        column_to: "maxPrice".to_string(),
                        column_type: ColumnType::MYSQL_TYPE_DOUBLE,
                    }],
                },
            ),
        ];

        for (input_query, expected_query) in variants.iter() {
            let query = convert_simple_select(input_query.clone());

            assert_eq!(&query, expected_query)
        }
    }

    #[test]
    fn test_select_error() {
        let variants = vec![
            (
                "SELECT MAX(*) FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::User("Unable to use '*' as argument to aggregation function 'MAX()' (only COUNT() supported)".to_string()),
            ),
            (
                "SELECT MAX(order_date) FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::User("Dimension 'order_date' was used with the aggregate function 'MAX()'. Please use a measure instead".to_string()),
            ),
            (
                "SELECT MAX(minPrice) FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::User("Measure aggregation type doesn't match. The aggregation type for 'minPrice' is 'MIN()' but 'MAX()' was provided".to_string()),
            ),
            (
                "SELECT MAX(unknownIdentifier) FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::User("Unable to find measure with name 'unknownIdentifier' for MAX(unknownIdentifier)".to_string()),
            ),
            // Check restrictions for segments usage
            (
                "SELECT is_male FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::User("Unable to use segment 'is_male' as column in SELECT statement".to_string()),
            ),
            (
                "SELECT COUNT(*) FROM KibanaSampleDataEcommerce GROUP BY is_male".to_string(),
                CompilationError::User("Unable to use segment 'is_male' in GROUP BY".to_string()),
            ),
            (
                "SELECT COUNT(*) FROM KibanaSampleDataEcommerce ORDER BY is_male DESC".to_string(),
                CompilationError::User("Unable to use segment 'is_male' in ORDER BY".to_string()),
            ),
        ];

        for (input_query, expected_error) in variants.iter() {
            let query = convert_sql_to_cube_query(
                &input_query,
                get_test_tenant_ctx(),
                &QueryPlannerExecutionProps {
                    connection_id: 8,
                    user: Some("ovr".to_string()),
                    database: None,
                },
            );

            match &query {
                Ok(_) => panic!("Query ({}) should return error", input_query),
                Err(e) => assert_eq!(e, expected_error),
            }
        }
    }

    #[test]
    fn test_group_by_date_trunc() {
        let supported_granularities = vec![
            // all variants
            [
                "DATE_TRUNC('second', order_date)".to_string(),
                "second".to_string(),
            ],
            [
                "DATE_TRUNC('minute', order_date)".to_string(),
                "minute".to_string(),
            ],
            [
                "DATE_TRUNC('hour', order_date)".to_string(),
                "hour".to_string(),
            ],
            [
                "DATE_TRUNC('week', order_date)".to_string(),
                "week".to_string(),
            ],
            [
                "DATE_TRUNC('month', order_date)".to_string(),
                "month".to_string(),
            ],
            [
                "DATE_TRUNC('quarter', order_date)".to_string(),
                "quarter".to_string(),
            ],
            [
                "DATE_TRUNC('year', order_date)".to_string(),
                "year".to_string(),
            ],
            // with escaping
            [
                "DATE_TRUNC('second', `order_date`)".to_string(),
                "second".to_string(),
            ],
        ];

        for [subquery, expected_granularity] in supported_granularities.iter() {
            let query = convert_simple_select(
                format!("SELECT COUNT(*), {} AS __timestamp FROM KibanaSampleDataEcommerce GROUP BY __timestamp", subquery)
            );

            assert_eq!(
                query,
                CompiledQuery {
                    request: V1LoadRequestQuery {
                        measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                        dimensions: Some(vec![]),
                        segments: Some(vec![]),
                        time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                            dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                            granularity: Some(expected_granularity.to_string()),
                            date_range: None,
                        }]),
                        order: None,
                        limit: None,
                        offset: None,
                        filters: None
                    },
                    meta: vec![
                        CompiledQueryFieldMeta {
                            column_from: "KibanaSampleDataEcommerce.count".to_string(),
                            column_to: "count".to_string(),
                            column_type: ColumnType::MYSQL_TYPE_LONGLONG,
                        },
                        CompiledQueryFieldMeta {
                            column_from: "KibanaSampleDataEcommerce.order_date".to_string(),
                            column_to: "__timestamp".to_string(),
                            column_type: ColumnType::MYSQL_TYPE_STRING,
                        }
                    ]
                }
            )
        }
    }

    #[test]
    fn test_group_by_date_granularity_superset() {
        let supported_granularities = vec![
            // With MAKEDATE
            ["MAKEDATE(YEAR(order_date), 1) + INTERVAL QUARTER(order_date) QUARTER - INTERVAL 1 QUARTER".to_string(), "quarter".to_string()],
            // With DATE
            ["DATE(DATE_SUB(order_date, INTERVAL DAYOFWEEK(DATE_SUB(order_date, INTERVAL 1 DAY)) - 1 DAY))".to_string(), "week".to_string()],
            // With escaping by `
            ["DATE(DATE_SUB(`order_date`, INTERVAL DAYOFWEEK(DATE_SUB(`order_date`, INTERVAL 1 DAY)) - 1 DAY))".to_string(), "week".to_string()],
            // @todo enable support when cube.js will support it
            // ["DATE(DATE_SUB(order_date, INTERVAL DAYOFWEEK(order_date) - 1 DAY))".to_string(), "week".to_string()],
            ["DATE(DATE_SUB(order_date, INTERVAL DAYOFMONTH(order_date) - 1 DAY))".to_string(), "month".to_string()],
            ["DATE(DATE_SUB(order_date, INTERVAL DAYOFYEAR(order_date) - 1 DAY))".to_string(), "year".to_string()],
            // Simple DATE
            ["DATE(order_date)".to_string(), "day".to_string()],
            ["DATE(`order_date`)".to_string(), "day".to_string()],
            ["DATE(`KibanaSampleDataEcommerce`.`order_date`)".to_string(), "day".to_string()],
            // With DATE_ADD
            ["DATE_ADD(DATE(order_date), INTERVAL HOUR(order_date) HOUR)".to_string(), "hour".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL HOUR(`order_date`) HOUR)".to_string(), "hour".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL (HOUR(order_date) * 60 + MINUTE(order_date)) MINUTE)".to_string(), "minute".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL (HOUR(`order_date`) * 60 + MINUTE(`order_date`)) MINUTE)".to_string(), "minute".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL (HOUR(order_date) * 60 * 60 + MINUTE(order_date) * 60 + SECOND(order_date)) SECOND)".to_string(), "second".to_string()],
            ["DATE_ADD(DATE(order_date), INTERVAL (HOUR(`order_date`) * 60 * 60 + MINUTE(`order_date`) * 60 + SECOND(`order_date`)) SECOND)".to_string(), "second".to_string()],
        ];

        for [subquery, expected_granularity] in supported_granularities.iter() {
            let query = convert_simple_select(
                format!("SELECT COUNT(*), {} AS __timestamp FROM KibanaSampleDataEcommerce GROUP BY __timestamp", subquery)
            );

            assert_eq!(
                query,
                CompiledQuery {
                    request: V1LoadRequestQuery {
                        measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                        dimensions: Some(vec![]),
                        segments: Some(vec![]),
                        time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                            dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                            granularity: Some(expected_granularity.to_string()),
                            date_range: None,
                        }]),
                        order: None,
                        limit: None,
                        offset: None,
                        filters: None
                    },
                    meta: vec![
                        CompiledQueryFieldMeta {
                            column_from: "KibanaSampleDataEcommerce.count".to_string(),
                            column_to: "count".to_string(),
                            column_type: ColumnType::MYSQL_TYPE_LONGLONG,
                        },
                        CompiledQueryFieldMeta {
                            column_from: "KibanaSampleDataEcommerce.order_date".to_string(),
                            column_to: "__timestamp".to_string(),
                            column_type: ColumnType::MYSQL_TYPE_STRING,
                        }
                    ]
                }
            )
        }
    }

    #[test]
    fn test_where_filter_daterange() {
        let to_check = vec![
            // Filter push down to TD (day) - Superset
            (
                "COUNT(*), DATE(order_date) AS __timestamp".to_string(),
                "order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Filter push down to TD (day) - Superset
            (
                "COUNT(*), DATE(order_date) AS __timestamp".to_string(),
                // Now replaced with exact date
                "`KibanaSampleDataEcommerce`.`order_date` >= date(date_add(date('2021-09-30 00:00:00.000000'), INTERVAL -30 day)) AND `KibanaSampleDataEcommerce`.`order_date` < date('2021-09-07 00:00:00.000000')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Create a new TD (dateRange filter pushdown)
            (
                "COUNT(*)".to_string(),
                "order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // Create a new TD (dateRange filter pushdown from right side of CompiledFilterTree::And)
            (
                "COUNT(*)".to_string(),
                "customer_gender = 'FEMALE' AND (order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f'))".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
            // similar as below but from left side
            (
                "COUNT(*)".to_string(),
                "(order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')) AND customer_gender = 'FEMALE'".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
        ];

        for (sql_projection, sql_filter, expected_tdm) in to_check.iter() {
            let query = convert_simple_select(format!(
                "SELECT
                {}
                FROM KibanaSampleDataEcommerce
                WHERE {}
                GROUP BY __timestamp",
                sql_projection, sql_filter
            ));

            assert_eq!(query.request.time_dimensions, *expected_tdm)
        }
    }

    #[test]
    fn test_where_filter_or() {
        let query = convert_simple_select(
            "SELECT
                COUNT(*), DATE(order_date) AS __timestamp
                FROM KibanaSampleDataEcommerce
                WHERE order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') OR order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')
                GROUP BY __timestamp"
            .to_string()
        );

        assert_eq!(
            query.request.filters,
            Some(vec![V1LoadRequestQueryFilterItem {
                member: None,
                operator: None,
                values: None,
                or: Some(vec![
                    json!(V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("afterDate".to_string()),
                        values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
                        or: None,
                        and: None,
                    }),
                    json!(V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("beforeDate".to_string()),
                        values: Some(vec!["2021-09-06T23:59:59.999Z".to_string()]),
                        or: None,
                        and: None,
                    })
                ]),
                and: None,
            },])
        )
    }

    #[test]
    fn test_where_filter_simple() {
        let to_check = vec![
            (
                "customer_gender = 'FEMALE'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["FEMALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price > 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price >= 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("gte".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price < 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("lt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price <= 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("lte".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price = -1".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["-1".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "taxful_total_price <> -1".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["-1".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // IN
            (
                "customer_gender IN ('FEMALE', 'MALE')".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["FEMALE".to_string(), "MALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT IN ('FEMALE', 'MALE')".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["FEMALE".to_string(), "MALE".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // NULL
            (
                "customer_gender IS NULL".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notSet".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender IS NOT NULL".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Date
            (
                "order_date = '2021-08-31'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "order_date <> '2021-08-31'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("notEquals".to_string()),
                    values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // BETWEEN
            (
                "order_date BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
                // This filter will be pushed to time_dimension
                None,
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-07T00:00:00.000Z".to_string(),
                    ])),
                }]),
            ),
            (
                "order_date NOT BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("notInDateRange".to_string()),
                    values: Some(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-07T00:00:00.000Z".to_string(),
                    ]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // SIMILAR as BETWEEN but manually
            (
                "order_date >= '2021-08-31' AND order_date < '2021-09-07'".to_string(),
                // This filter will be pushed to time_dimension
                None,
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        // -1 milleseconds hack for cube.js
                        "2021-09-06T23:59:59.999Z".to_string(),
                    ])),
                }]),
            ),
            //  SIMILAR as BETWEEN but without -1 nanosecond because <=
            (
                "order_date >= '2021-08-31' AND order_date <= '2021-09-07'".to_string(),
                None,
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        // without -1 because <=
                        "2021-09-07T00:00:00.000Z".to_string(),
                    ])),
                }]),
            ),
            // LIKE
            (
                "customer_gender LIKE 'female'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "customer_gender NOT LIKE 'male'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("notContains".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Segment
            (
                "is_male = true".to_string(),
                // This filter will be pushed to segments
                None,
                None,
            ),
            (
                "is_male = true AND is_female = true".to_string(),
                // This filters will be pushed to segments
                None,
                None,
            ),
        ];

        for (sql, expected_fitler, expected_time_dimensions) in to_check.iter() {
            let query = convert_simple_select(format!(
                "SELECT
                COUNT(*)
                FROM KibanaSampleDataEcommerce
                WHERE {}
                GROUP BY __timestamp",
                sql
            ));

            assert_eq!(
                query.request.filters, *expected_fitler,
                "Filters for {}",
                sql
            );
            assert_eq!(
                query.request.time_dimensions, *expected_time_dimensions,
                "Time dimensions for {}",
                sql
            );
        }
    }

    #[test]
    fn test_filter_error() {
        let to_check = vec![
            // Binary expr
            (
                "order_date >= 'WRONG_DATE'".to_string(),
                CompilationError::User("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <= 'WRONG_DATE'".to_string(),
                CompilationError::User("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date < 'WRONG_DATE'".to_string(),
                CompilationError::User("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <= 'WRONG_DATE'".to_string(),
                CompilationError::User("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date = 'WRONG_DATE'".to_string(),
                CompilationError::User("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <> 'WRONG_DATE'".to_string(),
                CompilationError::User("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            // Between
            (
                "order_date BETWEEN 'WRONG_DATE' AND '2021-01-01'".to_string(),
                CompilationError::User("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date BETWEEN '2021-01-01' AND 'WRONG_DATE'".to_string(),
                CompilationError::User("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
        ];

        for (sql, expected_error) in to_check.iter() {
            let query = convert_sql_to_cube_query(
                &format!(
                    "SELECT
                    COUNT(*), DATE(order_date) AS __timestamp
                    FROM KibanaSampleDataEcommerce
                    WHERE {}
                    GROUP BY __timestamp",
                    sql
                ),
                get_test_tenant_ctx(),
                &QueryPlannerExecutionProps {
                    connection_id: 8,
                    user: Some("ovr".to_string()),
                    database: None,
                },
            );

            match &query {
                Ok(_) => panic!("Query ({}) should return error", sql),
                Err(e) => assert_eq!(e, expected_error, "{}", sql),
            }
        }
    }

    #[test]
    fn test_where_filter_complex() {
        let to_check = vec![
            (
                "customer_gender = 'FEMALE' AND customer_gender = 'MALE'".to_string(),
                vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["FEMALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["MALE".to_string()]),
                        or: None,
                        and: None,
                    }
                ],
            ),
            (
                "customer_gender = 'FEMALE' OR customer_gender = 'MALE'".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["MALE".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' AND customer_gender = 'MALE' AND customer_gender = 'UNKNOWN'".to_string(),
                vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["FEMALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["MALE".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["UNKNOWN".to_string()]),
                        or: None,
                        and: None,
                    }
                ],
            ),
            (
                "customer_gender = 'FEMALE' OR customer_gender = 'MALE' OR customer_gender = 'UNKNOWN'".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["MALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["UNKNOWN".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' OR (customer_gender = 'MALE' AND taxful_total_price > 5)".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                                    operator: Some("equals".to_string()),
                                    values: Some(vec!["MALE".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("gt".to_string()),
                                    values: Some(vec!["5".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ]),
                        })
                    ]),
                    and: None,
                }],
            ),
            (
                "customer_gender = 'FEMALE' OR (customer_gender = 'MALE' AND taxful_total_price > 5 AND taxful_total_price < 100)".to_string(),
                vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("equals".to_string()),
                            values: Some(vec!["FEMALE".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                                    operator: Some("equals".to_string()),
                                    values: Some(vec!["MALE".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("gt".to_string()),
                                    values: Some(vec!["5".to_string()]),
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                                    operator: Some("lt".to_string()),
                                    values: Some(vec!["100".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ]),
                        })
                    ]),
                    and: None,
                }]
            ),
        ];

        for (sql, expected_fitler) in to_check.iter() {
            let query = convert_simple_select(format!(
                "SELECT
                COUNT(*), DATE(order_date) AS __timestamp
                FROM KibanaSampleDataEcommerce
                WHERE {}
                GROUP BY __timestamp",
                sql
            ));

            assert_eq!(query.request.filters, Some(expected_fitler.clone()))
        }
    }

    fn parse_expr_from_projection(query: &String) -> ast::Expr {
        let stmt = parse_sql_to_statement(&query).unwrap();
        match stmt {
            ast::Statement::Query(query) => match &query.body {
                ast::SetExpr::Select(select) => {
                    if select.projection.len() == 1 {
                        match &select.projection[0] {
                            ast::SelectItem::UnnamedExpr(expr) => {
                                return expr.clone();
                            }
                            ast::SelectItem::ExprWithAlias { expr, .. } => {
                                return expr.clone();
                            }
                            _ => panic!("err"),
                        };
                    } else {
                        panic!("err");
                    }
                }
                _ => panic!("err"),
            },
            _ => panic!("err"),
        }
    }

    #[test]
    fn test_str_to_date() {
        let compiled = compile_expression(
            &parse_expr_from_projection(
                &"SELECT STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')"
                    .to_string(),
            ),
            &QueryContext::new(&get_test_meta()[0]),
        )
        .unwrap();

        match compiled {
            CompiledExpression::DateLiteral(date) => {
                assert_eq!(date.to_string(), "2021-08-31 00:00:00 UTC".to_string())
            }
            _ => panic!("Must be DateLiteral"),
        };
    }

    #[test]
    fn test_now_expr() {
        let compiled = compile_expression(
            &parse_expr_from_projection(&"SELECT NOW()".to_string()),
            &QueryContext::new(&get_test_meta()[0]),
        )
        .unwrap();

        match compiled {
            CompiledExpression::DateLiteral(_) => {}
            _ => panic!("Must be DateLiteral"),
        };
    }

    #[test]
    fn test_date_date_add_interval_expr() {
        let to_check = vec![
            // positive
            (
                "date_add(date('2021-01-01 00:00:00.000000'), INTERVAL 1 second)".to_string(),
                "2021-01-01 00:00:01 UTC",
            ),
            (
                "date_add(date('2021-01-01 00:00:00.000000'), INTERVAL 1 minute)".to_string(),
                "2021-01-01 00:01:00 UTC",
            ),
            (
                "date_add(date('2021-01-01 00:00:00.000000'), INTERVAL 1 hour)".to_string(),
                "2021-01-01 01:00:00 UTC",
            ),
            (
                "date_add(date('2021-01-01 00:00:00.000000'), INTERVAL 1 day)".to_string(),
                "2021-01-02 00:00:00 UTC",
            ),
            // @todo we need to support exact +1 month
            (
                "date_add(date('2021-01-01 00:00:00.000000'), INTERVAL 1 month)".to_string(),
                "2021-01-31 00:00:00 UTC",
            ),
            // @todo we need to support exact +1 year
            (
                "date_add(date('2021-01-01 00:00:00.000000'), INTERVAL 1 year)".to_string(),
                "2022-01-01 00:00:00 UTC",
            ),
            // negative
            (
                "date_add(date('2021-08-31 00:00:00.000000'), INTERVAL -30 day)".to_string(),
                "2021-08-01 00:00:00 UTC",
            ),
        ];

        for (sql, expected_date) in to_check.iter() {
            let compiled = compile_expression(
                &parse_expr_from_projection(&format!("SELECT {}", sql)),
                &QueryContext::new(&get_test_meta()[0]),
            )
            .unwrap();

            match compiled {
                CompiledExpression::DateLiteral(date) => {
                    assert_eq!(date.to_string(), expected_date.to_string())
                }
                _ => panic!("Must be DateLiteral"),
            };
        }
    }

    #[test]
    fn test_str_literal_to_date() {
        let d = CompiledExpression::StringLiteral("2021-08-31".to_string())
            .to_date_literal()
            .unwrap();
        assert_eq!(
            d.to_value_as_str().unwrap(),
            "2021-08-31T00:00:00.000Z".to_string()
        );

        let d = CompiledExpression::StringLiteral("2021-08-31 00:00:00.000000".to_string())
            .to_date_literal()
            .unwrap();
        assert_eq!(
            d.to_value_as_str().unwrap(),
            "2021-08-31T00:00:00.000Z".to_string()
        );

        let d = CompiledExpression::StringLiteral("2021-08-31T00:00:00+00:00".to_string())
            .to_date_literal()
            .unwrap();
        assert_eq!(
            d.to_value_as_str().unwrap(),
            "2021-08-31T00:00:00.000Z".to_string()
        );

        // JS date.toIsoString()
        let d = CompiledExpression::StringLiteral("2021-08-31T00:00:00.000Z".to_string())
            .to_date_literal()
            .unwrap();
        assert_eq!(
            d.to_value_as_str().unwrap(),
            "2021-08-31T00:00:00.000Z".to_string()
        );
    }

    async fn execute_query(query: String) -> Result<String, CubeError> {
        let query = convert_sql_to_cube_query(
            &query,
            get_test_tenant_ctx(),
            &QueryPlannerExecutionProps {
                connection_id: 8,
                user: Some("ovr".to_string()),
                database: None,
            },
        );
        match query.unwrap() {
            QueryPlan::DataFushionSelect(_, plan, ctx) => {
                let df = DataFrameImpl::new(ctx.state, &plan);
                let batches = df.collect().await?;
                let frame = batch_to_dataframe(&batches)?;

                return Ok(frame.print());
            }
            QueryPlan::MetaTabular(_, frame) => {
                return Ok(frame.print());
            }
            _ => panic!("Unknown execution method"),
        }
    }

    #[tokio::test]
    async fn test_show_create_table() -> Result<(), CubeError> {
        let exepected =
            "+---------------------------+-----------------------------------------------+\n\
        | Table                     | Create Table                                  |\n\
        +---------------------------+-----------------------------------------------+\n\
        | KibanaSampleDataEcommerce | CREATE TABLE `KibanaSampleDataEcommerce` (\r    |\n\
        |                           |   `count` int,\r                                |\n\
        |                           |   `maxPrice` int,\r                             |\n\
        |                           |   `minPrice` int,\r                             |\n\
        |                           |   `avgPrice` int,\r                             |\n\
        |                           |   `order_date` datetime NOT NULL,\r             |\n\
        |                           |   `customer_gender` varchar(255) NOT NULL,\r    |\n\
        |                           |   `taxful_total_price` varchar(255) NOT NULL,\r |\n\
        |                           |   `is_male` boolean,\r                          |\n\
        |                           |   `is_female` boolean\r                         |\n\
        |                           | ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4       |\n\
        +---------------------------+-----------------------------------------------+";

        assert_eq!(
            execute_query("show create table KibanaSampleDataEcommerce;".to_string()).await?,
            exepected.clone()
        );

        assert_eq!(
            execute_query("show create table `db`.`KibanaSampleDataEcommerce`;".to_string())
                .await?,
            exepected
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_tables() -> Result<(), CubeError> {
        assert_eq!(
            execute_query("SELECT * FROM information_schema.tables".to_string()).await?,
            "+---------------+--------------------+---------------------------+------------+--------+---------+------------+-------------+----------------+-------------+-----------------+--------------+-----------+----------------+-------------+-------------+------------+-----------------+----------+----------------+---------------+\n\
            | TABLE_CATALOG | TABLE_SCHEMA       | TABLE_NAME                | TABLE_TYPE | ENGINE | VERSION | ROW_FORMAT | TABLES_ROWS | AVG_ROW_LENGTH | DATA_LENGTH | MAX_DATA_LENGTH | INDEX_LENGTH | DATA_FREE | AUTO_INCREMENT | CREATE_TIME | UPDATE_TIME | CHECK_TIME | TABLE_COLLATION | CHECKSUM | CREATE_OPTIONS | TABLE_COMMENT |\n\
            +---------------+--------------------+---------------------------+------------+--------+---------+------------+-------------+----------------+-------------+-----------------+--------------+-----------+----------------+-------------+-------------+------------+-----------------+----------+----------------+---------------+\n\
            | def           | information_schema | tables                    | BASE TABLE | InnoDB | 10      | Dynamic    | 0           | 0              | 16384       |                 |              |           |                |             |             |            |                 |          |                |               |\n\
            | def           | information_schema | columns                   | BASE TABLE | InnoDB | 10      | Dynamic    | 0           | 0              | 16384       |                 |              |           |                |             |             |            |                 |          |                |               |\n\
            | def           | information_schema | key_column_usage          | BASE TABLE | InnoDB | 10      | Dynamic    | 0           | 0              | 16384       |                 |              |           |                |             |             |            |                 |          |                |               |\n\
            | def           | information_schema | referential_constraints   | BASE TABLE | InnoDB | 10      | Dynamic    | 0           | 0              | 16384       |                 |              |           |                |             |             |            |                 |          |                |               |\n\
            | def           | performance_schema | session_variables         | BASE TABLE | InnoDB | 10      | Dynamic    | 0           | 0              | 16384       |                 |              |           |                |             |             |            |                 |          |                |               |\n\
            | def           | performance_schema | global_variables          | BASE TABLE | InnoDB | 10      | Dynamic    | 0           | 0              | 16384       |                 |              |           |                |             |             |            |                 |          |                |               |\n\
            | def           | db                 | KibanaSampleDataEcommerce | BASE TABLE | InnoDB | 10      | Dynamic    | 0           | 0              | 16384       |                 |              |           |                |             |             |            |                 |          |                |               |\n\
            | def           | db                 | Logs                      | BASE TABLE | InnoDB | 10      | Dynamic    | 0           | 0              | 16384       |                 |              |           |                |             |             |            |                 |          |                |               |\n\
            +---------------+--------------------+---------------------------+------------+--------+---------+------------+-------------+----------------+-------------+-----------------+--------------+-----------+----------------+-------------+-------------+------------+-----------------+----------+----------------+---------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_columns() -> Result<(), CubeError> {
        assert_eq!(
            execute_query("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = 'db'".to_string()).await?,
            "+---------------+--------------+---------------------------+--------------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+--------------+-------------------+--------------------+------------+-------+----------------+-----------------------+--------+\n\
            | TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME                | COLUMN_NAME        | ORDINAL_POSITION | COLUMN_DEFAULT | IS_NULLABLE | DATA_TYPE | CHARACTER_MAXIMUM_LENGTH | CHARACTER_OCTET_LENGTH | COLUMN_TYPE  | NUMERIC_PRECISION | DATETIME_PRECISION | COLUMN_KEY | EXTRA | COLUMN_COMMENT | GENERATION_EXPRESSION | SRS_ID |\n\
            +---------------+--------------+---------------------------+--------------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+--------------+-------------------+--------------------+------------+-------+----------------+-----------------------+--------+\n\
            | def           | db           | KibanaSampleDataEcommerce | count              | 0                |                | NO          | int       | NULL                     | NULL                   | int          | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | KibanaSampleDataEcommerce | maxPrice           | 0                |                | NO          | int       | NULL                     | NULL                   | int          | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | KibanaSampleDataEcommerce | minPrice           | 0                |                | NO          | int       | NULL                     | NULL                   | int          | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | KibanaSampleDataEcommerce | avgPrice           | 0                |                | NO          | int       | NULL                     | NULL                   | int          | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | KibanaSampleDataEcommerce | order_date         | 0                |                | YES         | datetime  | NULL                     | NULL                   | datetime     | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | KibanaSampleDataEcommerce | customer_gender    | 0                |                | YES         | varchar   | NULL                     | NULL                   | varchar(255) | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | KibanaSampleDataEcommerce | taxful_total_price | 0                |                | YES         | varchar   | NULL                     | NULL                   | varchar(255) | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | KibanaSampleDataEcommerce | is_male            | 0                |                | NO          | boolean   | NULL                     | NULL                   | boolean      | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | KibanaSampleDataEcommerce | is_female          | 0                |                | NO          | boolean   | NULL                     | NULL                   | boolean      | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | Logs                      | agentCount         | 0                |                | NO          | int       | NULL                     | NULL                   | int          | NULL              | NULL               |            |       |                |                       |        |\n\
            | def           | db           | Logs                      | agentCountApprox   | 0                |                | NO          | int       | NULL                     | NULL                   | int          | NULL              | NULL               |            |       |                |                       |        |\n\
            +---------------+--------------+---------------------------+--------------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+--------------+-------------------+--------------------+------------+-------+----------------+-----------------------+--------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_schemata() -> Result<(), CubeError> {
        assert_eq!(
            execute_query("SELECT * FROM information_schema.schemata".to_string()).await?,
            "+--------------+--------------------+----------------------------+------------------------+----------+--------------------+\n\
            | CATALOG_NAME | SCHEMA_NAME        | DEFAULT_CHARACTER_SET_NAME | DEFAULT_COLLATION_NAME | SQL_PATH | DEFAULT_ENCRYPTION |\n\
            +--------------+--------------------+----------------------------+------------------------+----------+--------------------+\n\
            | def          | information_schema | utf8                       | utf8_general_ci        | NULL     | NO                 |\n\
            | def          | mysql              | utf8mb4                    | utf8mb4_0900_ai_ci     | NULL     | NO                 |\n\
            | def          | performance_schema | utf8mb4                    | utf8mb4_0900_ai_ci     | NULL     | NO                 |\n\
            | def          | sys                | utf8mb4                    | utf8mb4_0900_ai_ci     | NULL     | NO                 |\n\
            | def          | test               | utf8mb4                    | utf8mb4_0900_ai_ci     | NULL     | NO                 |\n\
            +--------------+--------------------+----------------------------+------------------------+----------+--------------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_stats_for_columns() -> Result<(), CubeError> {
        // This query is used by metabase for introspection
        assert_eq!(
            execute_query("
            SELECT
                A.TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, A.TABLE_NAME, A.COLUMN_NAME, B.SEQ_IN_INDEX KEY_SEQ, B.INDEX_NAME PK_NAME
            FROM INFORMATION_SCHEMA.COLUMNS A, INFORMATION_SCHEMA.STATISTICS B
            WHERE A.COLUMN_KEY in ('PRI','pri') AND B.INDEX_NAME='PRIMARY'  AND (ISNULL(database()) OR (A.TABLE_SCHEMA = database())) AND (ISNULL(database()) OR (B.TABLE_SCHEMA = database())) AND A.TABLE_NAME = 'OutlierFingerprints'  AND B.TABLE_NAME = 'OutlierFingerprints'  AND A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME
            ORDER BY A.COLUMN_NAME".to_string()).await?,
            "++\n++\n++"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_performance_schema_variables() -> Result<(), CubeError> {
        assert_eq!(
            execute_query("SELECT * FROM performance_schema.session_variables WHERE VARIABLE_NAME = 'max_allowed_packet'".to_string()).await?,
            "+--------------------+----------------+\n\
            | VARIABLE_NAME      | VARIABLE_VALUE |\n\
            +--------------------+----------------+\n\
            | max_allowed_packet | 67108864       |\n\
            +--------------------+----------------+"
        );

        assert_eq!(
            execute_query("SELECT * FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'max_allowed_packet'".to_string()).await?,
            "+--------------------+----------------+\n\
            | VARIABLE_NAME      | VARIABLE_VALUE |\n\
            +--------------------+----------------+\n\
            | max_allowed_packet | 67108864       |\n\
            +--------------------+----------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_if() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                r#"select
                if(null, true, false) as r1,
                if(true, false, true) as r2,
                if(true, 'true', 'false') as r3,
                if(true, CAST(1 as int), CAST(2 as bigint)) as c1,
                if(false, CAST(1 as int), CAST(2 as bigint)) as c2,
                if(true, CAST(1 as bigint), CAST(2 as int)) as c3
            "#
                .to_string()
            )
            .await?,
            "+-------+-------+------+----+----+----+\n\
            | r1    | r2    | r3   | c1 | c2 | c3 |\n\
            +-------+-------+------+----+----+----+\n\
            | false | false | true | 1  | 2  | 1  |\n\
            +-------+-------+------+----+----+----+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_least() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                least(1, 2) as r1, \
                least(2, 1) as r2, \
                least(null, 1) as r3, \
                least(1, null) as r4
            "
                .to_string()
            )
            .await?,
            "+----+----+------+------+\n\
            | r1 | r2 | r3   | r4   |\n\
            +----+----+------+------+\n\
            | 1  | 1  | NULL | NULL |\n\
            +----+----+------+------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ucase() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                ucase('super stroka') as r1
            "
                .to_string()
            )
            .await?,
            "+--------------+\n\
            | r1           |\n\
            +--------------+\n\
            | SUPER STROKA |\n\
            +--------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_convert_tz() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select convert_tz('2021-12-08T15:50:14.337Z'::timestamp, @@GLOBAL.time_zone, '+00:00') as r1;".to_string()
            )
            .await?,
            "+--------------------------+\n\
            | r1                       |\n\
            +--------------------------+\n\
            | 2021-12-08T15:50:14.337Z |\n\
            +--------------------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_timediff() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                    timediff('1994-11-26T13:25:00.000Z'::timestamp, '1994-11-26T13:25:00.000Z'::timestamp) as r1
                ".to_string()
            )
            .await?,
            "+------------------------------------------------+\n\
            | r1                                             |\n\
            +------------------------------------------------+\n\
            | 0 years 0 mons 0 days 0 hours 0 mins 0.00 secs |\n\
            +------------------------------------------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_instr() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                    instr('rust is killing me', 'r') as r1,
                    instr('rust is killing me', 'e') as r2,
                    instr('Rust is killing me', 'unknown') as r3;
                "
                .to_string()
            )
            .await?,
            "+----+----+----+\n\
            | r1 | r2 | r3 |\n\
            +----+----+----+\n\
            | 1  | 18 | 0  |\n\
            +----+----+----+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_locate() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                    locate('r', 'rust is killing me') as r1,
                    locate('e', 'rust is killing me') as r2,
                    locate('unknown', 'Rust is killing me') as r3
                "
                .to_string()
            )
            .await?,
            "+----+----+----+\n\
            | r1 | r2 | r3 |\n\
            +----+----+----+\n\
            | 1  | 18 | 0  |\n\
            +----+----+----+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_select_variables() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                // This query I saw in Google Data Studio
                "/* mysql-connector-java-5.1.49 ( Revision: ad86f36e100e104cd926c6b81c8cab9565750116 ) */
                SELECT  \
                    @@session.auto_increment_increment AS auto_increment_increment, \
                    @@character_set_client AS character_set_client, \
                    @@character_set_connection AS character_set_connection, \
                    @@character_set_results AS character_set_results, \
                    @@character_set_server AS character_set_server, \
                    @@collation_server AS collation_server, \
                    @@collation_connection AS collation_connection, \
                    @@init_connect AS init_connect, \
                    @@interactive_timeout AS interactive_timeout, \
                    @@license AS license, \
                    @@lower_case_table_names AS lower_case_table_names, \
                    @@max_allowed_packet AS max_allowed_packet, \
                    @@net_buffer_length AS net_buffer_length, \
                    @@net_write_timeout AS net_write_timeout, \
                    @@sql_mode AS sql_mode, \
                    @@system_time_zone AS system_time_zone, \
                    @@time_zone AS time_zone, \
                    @@transaction_isolation AS transaction_isolation, \
                    @@wait_timeout AS wait_timeout
                "
                .to_string()
            )
            .await?,
            "+--------------------------+----------------------+--------------------------+-----------------------+----------------------+--------------------+----------------------+--------------+---------------------+----------+------------------------+--------------------+-------------------+-------------------+-----------------------------------------------------------------------------------------------------------------------+------------------+-----------+-----------------------+--------------+\n\
            | auto_increment_increment | character_set_client | character_set_connection | character_set_results | character_set_server | collation_server   | collation_connection | init_connect | interactive_timeout | license  | lower_case_table_names | max_allowed_packet | net_buffer_length | net_write_timeout | sql_mode                                                                                                              | system_time_zone | time_zone | transaction_isolation | wait_timeout |\n\
            +--------------------------+----------------------+--------------------------+-----------------------+----------------------+--------------------+----------------------+--------------+---------------------+----------+------------------------+--------------------+-------------------+-------------------+-----------------------------------------------------------------------------------------------------------------------+------------------+-----------+-----------------------+--------------+\n\
            | 1                        | utf8mb4              | utf8mb4                  | utf8mb4               | utf8mb4              | utf8mb4_0900_ai_ci | utf8mb4_general_ci   |              | 28800               | Apache 2 | 0                      | 67108864           | 16384             | 600               | ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION | UTC              | SYSTEM    | REPEATABLE-READ       | 28800        |\n\
            +--------------------------+----------------------+--------------------------+-----------------------+----------------------+--------------------+----------------------+--------------+---------------------+----------+------------------------+--------------------+-------------------+-------------------+-----------------------------------------------------------------------------------------------------------------------+------------------+-----------+-----------------------+--------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_variable() -> Result<(), CubeError> {
        // LIKE
        assert_eq!(
            execute_query(
                "show variables like 'sql_mode';"
                .to_string()
            )
            .await?,
            "+---------------+-----------------------------------------------------------------------------------------------------------------------+\n\
            | Variable_name | Value                                                                                                                 |\n\
            +---------------+-----------------------------------------------------------------------------------------------------------------------+\n\
            | sql_mode      | ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION |\n\
            +---------------+-----------------------------------------------------------------------------------------------------------------------+"
        );

        // LIKE pattern
        assert_eq!(
            execute_query(
                "show variables like '%_mode';"
                .to_string()
            )
            .await?,
            "+---------------+-----------------------------------------------------------------------------------------------------------------------+\n\
            | Variable_name | Value                                                                                                                 |\n\
            +---------------+-----------------------------------------------------------------------------------------------------------------------+\n\
            | sql_mode      | ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION |\n\
            +---------------+-----------------------------------------------------------------------------------------------------------------------+"
        );

        // Negative test, we dont define this variable
        assert_eq!(
            execute_query("show variables like 'aurora_version';".to_string()).await?,
            "++\n++\n++"
        );

        // All variables
        assert_eq!(
            execute_query(
                "show variables;"
                .to_string()
            )
            .await?,
            "+------------------------+-----------------------------------------------------------------------------------------------------------------------+\n\
            | Variable_name          | Value                                                                                                                 |\n\
            +------------------------+-----------------------------------------------------------------------------------------------------------------------+\n\
            | sql_mode               | ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION |\n\
            | max_allowed_packet     | 67108864                                                                                                              |\n\
            | lower_case_table_names | 0                                                                                                                     |\n\
            +------------------------+-----------------------------------------------------------------------------------------------------------------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "SELECT \
                    @@GLOBAL.time_zone AS global_tz, \
                    @@system_time_zone AS system_tz, time_format(   timediff(      now(), convert_tz(now(), @@GLOBAL.time_zone, '+00:00')   ),   '%H:%i' ) AS 'offset'
                ".to_string()
            )
            .await?,
            "+-----------+-----------+--------+\n\
            | global_tz | system_tz | offset |\n\
            +-----------+-----------+--------+\n\
            | SYSTEM    | UTC       | 00:00  |\n\
            +-----------+-----------+--------+"
        );

        assert_eq!(
            execute_query(
                "SELECT \
                TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, \
                CASE data_type WHEN 'bit' THEN -7 WHEN 'tinyblob' THEN -3 WHEN 'mediumblob' THEN -4 WHEN 'longblob' THEN -4 WHEN 'blob' THEN -4 WHEN 'tinytext' THEN 12 WHEN 'mediumtext' THEN -1 WHEN 'longtext' THEN -1 WHEN 'text' THEN -1 WHEN 'date' THEN 91 WHEN 'datetime' THEN 93 WHEN 'decimal' THEN 3 WHEN 'double' THEN 8 WHEN 'enum' THEN 12 WHEN 'float' THEN 7 WHEN 'int' THEN IF( COLUMN_TYPE like '%unsigned%', 4,4) WHEN 'bigint' THEN -5 WHEN 'mediumint' THEN 4 WHEN 'null' THEN 0 WHEN 'set' THEN 12 WHEN 'smallint' THEN IF( COLUMN_TYPE like '%unsigned%', 5,5) WHEN 'varchar' THEN 12 WHEN 'varbinary' THEN -3 WHEN 'char' THEN 1 WHEN 'binary' THEN -2 WHEN 'time' THEN 92 WHEN 'timestamp' THEN 93 WHEN 'tinyint' THEN IF(COLUMN_TYPE like 'tinyint(1)%',-7,-6)  WHEN 'year' THEN 91 ELSE 1111 END  DATA_TYPE, IF(COLUMN_TYPE like 'tinyint(1)%', 'BIT',  UCASE(IF( COLUMN_TYPE LIKE '%(%)%', CONCAT(SUBSTRING( COLUMN_TYPE,1, LOCATE('(',COLUMN_TYPE) - 1 ), SUBSTRING(COLUMN_TYPE ,1+locate(')', COLUMN_TYPE))), COLUMN_TYPE))) TYPE_NAME,  CASE DATA_TYPE  WHEN 'time' THEN IF(DATETIME_PRECISION = 0, 10, CAST(11 + DATETIME_PRECISION as signed integer))  WHEN 'date' THEN 10  WHEN 'datetime' THEN IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))  WHEN 'timestamp' THEN IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))  ELSE   IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,2147483647), NUMERIC_PRECISION)  END COLUMN_SIZE, \
                65535 BUFFER_LENGTH, \
                CONVERT (CASE DATA_TYPE WHEN 'year' THEN NUMERIC_SCALE WHEN 'tinyint' THEN 0 ELSE NUMERIC_SCALE END, UNSIGNED INTEGER) DECIMAL_DIGITS, 10 NUM_PREC_RADIX, \
                IF(IS_NULLABLE = 'yes',1,0) NULLABLE,
                COLUMN_COMMENT REMARKS, \
                COLUMN_DEFAULT COLUMN_DEF, \
                0 SQL_DATA_TYPE, \
                0 SQL_DATETIME_SUB, \
                LEAST(CHARACTER_OCTET_LENGTH,2147483647) CHAR_OCTET_LENGTH, \
                ORDINAL_POSITION, \
                IS_NULLABLE, \
                NULL SCOPE_CATALOG, \
                NULL SCOPE_SCHEMA, \
                NULL SCOPE_TABLE, \
                NULL SOURCE_DATA_TYPE, \
                IF(EXTRA = 'auto_increment','YES','NO') IS_AUTOINCREMENT, \
                IF(EXTRA in ('VIRTUAL', 'PERSISTENT', 'VIRTUAL GENERATED', 'STORED GENERATED') ,'YES','NO') IS_GENERATEDCOLUMN \
                FROM INFORMATION_SCHEMA.COLUMNS  WHERE (ISNULL(database()) OR (TABLE_SCHEMA = database())) AND TABLE_NAME = 'KibanaSampleDataEcommerce' \
                ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;".to_string()
            )
            .await?,
            "+-----------+-------------+---------------------------+--------------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+\n\
            | TABLE_CAT | TABLE_SCHEM | TABLE_NAME                | COLUMN_NAME        | DATA_TYPE | TYPE_NAME | COLUMN_SIZE | BUFFER_LENGTH | DECIMAL_DIGITS | NUM_PREC_RADIX | NULLABLE | REMARKS | COLUMN_DEF | SQL_DATA_TYPE | SQL_DATETIME_SUB | CHAR_OCTET_LENGTH | ORDINAL_POSITION | IS_NULLABLE | SCOPE_CATALOG | SCOPE_SCHEMA | SCOPE_TABLE | SOURCE_DATA_TYPE | IS_AUTOINCREMENT | IS_GENERATEDCOLUMN |\n\
            +-----------+-------------+---------------------------+--------------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+\n\
            | db        | NULL        | KibanaSampleDataEcommerce | count              | 4         | INT       | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | NO          | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            | db        | NULL        | KibanaSampleDataEcommerce | maxPrice           | 4         | INT       | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | NO          | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            | db        | NULL        | KibanaSampleDataEcommerce | minPrice           | 4         | INT       | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | NO          | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            | db        | NULL        | KibanaSampleDataEcommerce | avgPrice           | 4         | INT       | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | NO          | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            | db        | NULL        | KibanaSampleDataEcommerce | order_date         | 93        | DATETIME  | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | YES         | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            | db        | NULL        | KibanaSampleDataEcommerce | customer_gender    | 12        | VARCHAR   | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | YES         | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            | db        | NULL        | KibanaSampleDataEcommerce | taxful_total_price | 12        | VARCHAR   | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | YES         | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            | db        | NULL        | KibanaSampleDataEcommerce | is_male            | 1111      | BOOLEAN   | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | NO          | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            | db        | NULL        | KibanaSampleDataEcommerce | is_female          | 1111      | BOOLEAN   | NULL        | 65535         | 0              | 10             | 0        |         |            | 0             | 0                | NULL              | 0                | NO          | NULL          | NULL         | NULL        | NULL             | NO               | NO                 |\n\
            +-----------+-------------+---------------------------+--------------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+"
        );

        assert_eq!(
            execute_query(
                "SELECT
                    KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT,
                    NULL PKTABLE_SCHEM,
                    KCU.REFERENCED_TABLE_NAME PKTABLE_NAME,
                    KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME,
                    KCU.TABLE_SCHEMA FKTABLE_CAT,
                    NULL FKTABLE_SCHEM,
                    KCU.TABLE_NAME FKTABLE_NAME,
                    KCU.COLUMN_NAME FKCOLUMN_NAME,
                    KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,
                    CASE update_rule    WHEN 'RESTRICT' THEN 1   WHEN 'NO ACTION' THEN 3   WHEN 'CASCADE' THEN 0   WHEN 'SET NULL' THEN 2   WHEN 'SET DEFAULT' THEN 4 END UPDATE_RULE,
                    CASE DELETE_RULE WHEN 'RESTRICT' THEN 1  WHEN 'NO ACTION' THEN 3  WHEN 'CASCADE' THEN 0  WHEN 'SET NULL' THEN 2  WHEN 'SET DEFAULT' THEN 4 END DELETE_RULE,
                    RC.CONSTRAINT_NAME FK_NAME,
                    NULL PK_NAME,
                    7 DEFERRABILITY
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
                INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
                WHERE (ISNULL(database()) OR (KCU.TABLE_SCHEMA = database())) AND  KCU.TABLE_NAME = 'SlackMessages' ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ
                ".to_string()
            )
            .await?,
            "++\n\
            ++\n\
            ++"
        );

        Ok(())
    }
}
