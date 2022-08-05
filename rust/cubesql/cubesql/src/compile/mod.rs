use core::fmt;
use std::{
    backtrace::Backtrace, collections::HashMap, env, fmt::Formatter, future::Future, pin::Pin,
    sync::Arc,
};

use chrono::{prelude::*, Duration};

use datafusion::{
    arrow::datatypes::DataType,
    execution::context::{
        default_session_builder, SessionConfig as DFSessionConfig,
        SessionContext as DFSessionContext,
    },
    logical_plan::{
        plan::{Analyze, Explain, Extension, Projection, ToStringifiedPlan},
        DFField, DFSchema, DFSchemaRef, Expr, LogicalPlan, PlanType, ToDFSchema,
    },
    prelude::*,
    scalar::ScalarValue,
    sql::{parser::Statement as DFStatement, planner::SqlToRel},
    variable::VarType,
};
use itertools::Itertools;
use log::{debug, trace, warn};
use serde::Serialize;
use serde_json::json;
use sqlparser::ast::{self, escape_single_quote_string, DateTimeField, Ident, ObjectName};

use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};

pub use crate::transport::ctx::*;

use self::{
    builder::*,
    context::*,
    engine::{
        context::VariablesProvider,
        df::{
            planner::CubeQueryPlanner,
            scan::{CubeScanNode, MemberField},
        },
        information_schema::mysql::ext::CubeColumnMySqlExt,
        provider::CubeContext,
        udf::{
            create_array_lower_udf, create_array_upper_udf, create_connection_id_udf,
            create_convert_tz_udf, create_cube_regclass_cast_udf, create_current_schema_udf,
            create_current_schemas_udf, create_current_timestamp_udf, create_current_user_udf,
            create_date_add_udf, create_date_sub_udf, create_date_udf, create_dayofmonth_udf,
            create_dayofweek_udf, create_dayofyear_udf, create_db_udf, create_format_type_udf,
            create_generate_series_udtf, create_generate_subscripts_udtf,
            create_has_schema_privilege_udf, create_hour_udf, create_if_udf, create_instr_udf,
            create_isnull_udf, create_json_build_object_udf, create_least_udf, create_locate_udf,
            create_makedate_udf, create_measure_udaf, create_minute_udf, create_pg_backend_pid_udf,
            create_pg_datetime_precision_udf, create_pg_expandarray_udtf,
            create_pg_get_constraintdef_udf, create_pg_get_expr_udf,
            create_pg_get_serial_sequence_udf, create_pg_get_userbyid_udf,
            create_pg_is_other_temp_schema, create_pg_my_temp_schema,
            create_pg_numeric_precision_udf, create_pg_numeric_scale_udf,
            create_pg_table_is_visible_udf, create_pg_total_relation_size_udf,
            create_pg_truetypid_udf, create_pg_truetypmod_udf, create_pg_type_is_visible_udf,
            create_quarter_udf, create_second_udf, create_session_user_udf, create_str_to_date_udf,
            create_time_format_udf, create_timediff_udf, create_to_char_udf, create_ucase_udf,
            create_unnest_udtf, create_user_udf, create_version_udf, create_year_udf,
        },
    },
    parser::parse_sql_to_statement,
    rewrite::converter::LogicalPlanToLanguageConverter,
};
use crate::{
    compile::engine::df::scan::CubeScanOptions,
    sql::{
        database_variables::{DatabaseVariable, DatabaseVariablesToUpdate},
        dataframe,
        session::DatabaseProtocol,
        statement::{CastReplacer, ToTimestampReplacer, UdfWildcardArgReplacer},
        types::{CommandCompletion, StatusFlags},
        ColumnFlags, ColumnType, Session, SessionManager, SessionState,
    },
    transport::{
        df_data_type_by_column_type, V1CubeMetaDimensionExt, V1CubeMetaExt, V1CubeMetaMeasureExt,
        V1CubeMetaSegmentExt,
    },
    CubeError, CubeErrorCauseType,
};

pub mod builder;
pub mod context;
pub mod engine;
pub mod parser;
pub mod rewrite;
pub mod service;

#[derive(thiserror::Error, Debug)]
pub enum CompilationError {
    #[error("SQLCompilationError: Internal: {0}")]
    Internal(String, Backtrace, Option<HashMap<String, String>>),
    #[error("SQLCompilationError: User: {0}")]
    User(String, Option<HashMap<String, String>>),
    #[error("SQLCompilationError: Unsupported: {0}")]
    Unsupported(String, Option<HashMap<String, String>>),
}

impl PartialEq for CompilationError {
    fn eq(&self, other: &Self) -> bool {
        match &self {
            CompilationError::Internal(left, _, _) => match other {
                CompilationError::Internal(right, _, _) => left == right,
                _ => false,
            },
            CompilationError::User(left, _) => match other {
                CompilationError::User(right, _) => left == right,
                _ => false,
            },
            CompilationError::Unsupported(left, _) => match other {
                CompilationError::Unsupported(right, _) => left == right,
                _ => false,
            },
        }
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl CompilationError {
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            CompilationError::Internal(_, bt, _) => Some(bt),
            CompilationError::User(_, _) => None,
            CompilationError::Unsupported(_, _) => None,
        }
    }

    pub fn to_backtrace(self) -> Option<Backtrace> {
        match self {
            CompilationError::Internal(_, bt, _) => Some(bt),
            CompilationError::User(_, _) => None,
            CompilationError::Unsupported(_, _) => None,
        }
    }
}

impl CompilationError {
    pub fn internal(message: String) -> Self {
        Self::Internal(message, Backtrace::capture(), None)
    }

    pub fn internal_with_bt(message: String, bt: Backtrace) -> Self {
        Self::Internal(message, bt, None)
    }

    pub fn user(message: String) -> Self {
        Self::User(message, None)
    }

    pub fn unsupported(message: String) -> Self {
        Self::Unsupported(message, None)
    }
}

impl CompilationError {
    pub fn message(&self) -> String {
        match self {
            CompilationError::Internal(msg, _, _)
            | CompilationError::User(msg, _)
            | CompilationError::Unsupported(msg, _) => msg.clone(),
        }
    }

    pub fn with_message(self, msg: String) -> Self {
        match self {
            CompilationError::Internal(_, bts, meta) => CompilationError::Internal(msg, bts, meta),
            CompilationError::User(_, meta) => CompilationError::User(msg, meta),
            CompilationError::Unsupported(_, meta) => CompilationError::Unsupported(msg, meta),
        }
    }
}

impl CompilationError {
    pub fn with_meta(self, meta: Option<HashMap<String, String>>) -> Self {
        match self {
            CompilationError::Internal(msg, bts, _) => CompilationError::Internal(msg, bts, meta),
            CompilationError::User(msg, _) => CompilationError::User(msg, meta),
            CompilationError::Unsupported(msg, _) => CompilationError::Unsupported(msg, meta),
        }
    }
}

pub type CompilationResult<T> = std::result::Result<T, CompilationError>;

impl From<regex::Error> for CompilationError {
    fn from(v: regex::Error) -> Self {
        CompilationError::internal(format!("{:?}", v))
    }
}

impl From<serde_json::Error> for CompilationError {
    fn from(v: serde_json::Error) -> Self {
        CompilationError::internal(format!("{:?}", v))
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
            .ok_or(CompilationError::unsupported(format!(
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
                    column_type: ColumnType::String,
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
                    column_type: measure.get_sql_type(),
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
                        "number" => ColumnType::Double,
                        _ => ColumnType::String,
                    },
                },
            );
        }
        Selection::Segment(s) => {
            return Err(CompilationError::user(format!(
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
            _ => Err(CompilationError::internal(format!(
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
            _ => Err(CompilationError::unsupported(format!(
                "Unable to compile argument: {:?}",
                argument
            ))),
        },
        _ => Err(CompilationError::unsupported(format!(
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
        [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg1)), ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg2))] => {
            Ok((&arg1, &arg2))
        }
        _ => Err(CompilationError::user(format!(
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
            return Err(CompilationError::user(format!(
                "Wrong type of argument (date), must be StringLiteral: {:?}",
                f
            )))
        }
    };
    let format = match compile_argument(format_expr)? {
        CompiledExpression::StringLiteral(str) => str,
        _ => {
            return Err(CompilationError::user(format!(
                "Wrong type of argument (format), must be StringLiteral: {:?}",
                f
            )))
        }
    };

    if !format.eq("%Y-%m-%d %H:%i:%s.%f") {
        return Err(CompilationError::user(format!(
            "Wrong type of argument: {:?}",
            f
        )));
    }

    let parsed_date = Utc
        .datetime_from_str(date.as_str(), "%Y-%m-%d %H:%M:%S.%f")
        .map_err(|e| {
            CompilationError::user(format!("Unable to parse {}, err: {}", date, e.to_string(),))
        })?;

    Ok(CompiledExpression::DateLiteral(parsed_date))
}

// DATE(expr)
// Extracts the date part of the date or datetime expression expr.
fn date_function(f: &ast::Function, ctx: &QueryContext) -> CompilationResult<CompiledExpression> {
    let date_expr = match f.args.as_slice() {
        [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(date_expr))] => date_expr,
        _ => {
            return Err(CompilationError::user(format!(
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
                    CompilationError::user(format!(
                        "Unable to parse {}, err: {}",
                        input,
                        e.to_string(),
                    ))
                })?;

            Ok(CompiledExpression::DateLiteral(parsed_date))
        }
        _ => {
            return Err(CompilationError::user(format!(
                "Wrong type of argument (date), must be DateLiteral, actual: {:?}",
                f
            )))
        }
    }
}

fn now_function(f: &ast::Function) -> CompilationResult<CompiledExpression> {
    if f.args.len() > 1 {
        return Err(CompilationError::user(format!(
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
            return Err(CompilationError::user(format!(
                "Wrong type of argument (date), must be DateLiteral: {:?}",
                f
            )))
        }
    };

    let interval = match compile_expression(&right_expr, &ctx)? {
        CompiledExpression::IntervalLiteral(str) => str,
        _ => {
            return Err(CompilationError::user(format!(
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
        return Err(CompilationError::unsupported(format!(
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
                Err(CompilationError::user(format!(
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
                return Err(CompilationError::unsupported(format!(
                    "Unsupported compound identifier in argument: {}",
                    expr.to_string()
                )));
            };

            if let Some(selection) = ctx.find_selection_for_identifier(&identifier, true) {
                Ok(CompiledExpression::Selection(selection))
            } else {
                Err(CompilationError::user(format!(
                    "Unable to find selection for: {:?}",
                    identifier
                )))
            }
        }
        ast::Expr::UnaryOp { expr, op } => match op {
            ast::UnaryOperator::Minus => match *expr.clone() {
                ast::Expr::Value(value) => match value {
                    ast::Value::Number(v, _) => Ok(CompiledExpression::NumberLiteral(v, true)),
                    _ => Err(CompilationError::user(format!(
                        "Unsupported value: {:?}",
                        value
                    ))),
                },
                _ => Err(CompilationError::unsupported(format!(
                    "Unable to compile Unary Op: {:?}",
                    expr
                ))),
            },
            _ => Err(CompilationError::unsupported(format!(
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
                            CompilationError::unsupported(format!(
                                "Unable to parse interval value: {}",
                                e.to_string()
                            ))
                        })?;

                        (n, is_negative)
                    }
                    _ => {
                        return Err(CompilationError::user(format!(
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
                        return Err(CompilationError::user(format!(
                            "Unsupported type of Interval, actual: {:?}",
                            leading_field
                        )))
                    }
                };

                Ok(CompiledExpression::IntervalLiteral(interval))
            }
            _ => Err(CompilationError::user(format!(
                "Unsupported value: {:?}",
                val
            ))),
        },
        ast::Expr::Function(f) => match f.name.to_string().to_lowercase().as_str() {
            "str_to_date" => str_to_date_function(&f),
            "date" => date_function(&f, &ctx),
            "date_add" => date_add_function(&f, &ctx),
            "now" => now_function(&f),
            _ => Err(CompilationError::user(format!(
                "Unsupported function: {:?}",
                f
            ))),
        },
        _ => Err(CompilationError::unsupported(format!(
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
    let left_ce = compile_expression(left, ctx)?;
    let right_ce = compile_expression(right, ctx)?;

    // Group selection to left, expr for filtering to right
    let (selection_to_filter, filter_expr) = match (left_ce, right_ce) {
        (CompiledExpression::Selection(selection), non_selection) => (selection, non_selection),
        (non_selection, CompiledExpression::Selection(selection)) => (selection, non_selection),
        // CubeSQL doesnt support BinaryExpression with literals in both sides
        (l, r) => {
            return Err(CompilationError::unsupported(format!(
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
        Selection::Measure(_measure) => {
            let (value, operator) = match op {
                ast::BinaryOperator::NotLike => (filter_expr, "notContains".to_string()),
                ast::BinaryOperator::Like => (filter_expr, "contains".to_string()),
                ast::BinaryOperator::Eq => (filter_expr, "equals".to_string()),
                ast::BinaryOperator::NotEq => (filter_expr, "notEquals".to_string()),
                ast::BinaryOperator::GtEq => (filter_expr, "gte".to_string()),
                ast::BinaryOperator::Gt => (filter_expr, "gt".to_string()),
                ast::BinaryOperator::Lt => (filter_expr, "lt".to_string()),
                ast::BinaryOperator::LtEq => (filter_expr, "lte".to_string()),
                _ => {
                    return Err(CompilationError::unsupported(format!(
                        "Operator in binary expression for measure: {} {} {}",
                        left, op, right
                    )))
                }
            };

            CompiledFilter::Filter {
                member,
                operator,
                values: Some(vec![value.to_value_as_str()?]),
            }
        }
        // Compile to CompiledFilter::Filter
        Selection::Dimension(dim) => {
            let filter_expr = if dim.is_time() {
                let date = filter_expr.to_date();
                if let Some(dt) = date {
                    CompiledExpression::DateLiteral(dt)
                } else {
                    return Err(CompilationError::user(format!(
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
                ast::BinaryOperator::GtEq => match filter_expr {
                    CompiledExpression::DateLiteral(_) => (filter_expr, "afterDate".to_string()),
                    _ => (filter_expr, "gte".to_string()),
                },
                ast::BinaryOperator::Gt => match filter_expr {
                    CompiledExpression::DateLiteral(dt) => (
                        CompiledExpression::DateLiteral(dt + Duration::milliseconds(1)),
                        "afterDate".to_string(),
                    ),
                    _ => (filter_expr, "gt".to_string()),
                },
                ast::BinaryOperator::Lt => match filter_expr {
                    CompiledExpression::DateLiteral(dt) => (
                        CompiledExpression::DateLiteral(dt - Duration::milliseconds(1)),
                        "beforeDate".to_string(),
                    ),
                    _ => (filter_expr, "lt".to_string()),
                },
                ast::BinaryOperator::LtEq => match filter_expr {
                    CompiledExpression::DateLiteral(_) => (filter_expr, "beforeDate".to_string()),
                    _ => (filter_expr, "lte".to_string()),
                },
                _ => {
                    return Err(CompilationError::unsupported(format!(
                        "Operator in binary expression for dimension: {} {} {}",
                        left, op, right
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
                        return Err(CompilationError::unsupported(
                            "Unable to use false as value for filtering segment".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(CompilationError::unsupported(format!(
                        "Unable to use value {:?} as value for filtering segment",
                        filter_expr
                    )));
                }
            },
            _ => {
                return Err(CompilationError::user(format!(
                    "Unable to use operator {} with segment: {} {} {}",
                    op, left, op, right
                )));
            }
        },
        _ => {
            return Err(CompilationError::unsupported(format!(
                "Binary expression: {} {} {}",
                left, op, right
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
        _ => Err(CompilationError::unsupported(format!(
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
                        Err(CompilationError::user(format!(
                            "Column for IsNull must be a Dimension or TimeDimension, actual: {:?}",
                            compiled_expr
                        )))
                    }
                },
                _ => Err(CompilationError::user(format!(
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
                        Err(CompilationError::user(format!(
                            "Column for Between must be a time dimension, actual: {:?}",
                            compiled_expr
                        )))
                    }
                }
                _ => Err(CompilationError::user(format!(
                    "Column for Between must be a time dimension, actual: {:?}",
                    compiled_expr
                ))),
            }?;

            let low_compiled = compile_expression(low, ctx)?;
            let low_compiled_date =
                low_compiled
                    .to_date_literal()
                    .ok_or(CompilationError::user(format!(
                        "Unable to compare time dimension \"{}\" with not a date value: {}",
                        column_for_filter.get_real_name(),
                        low_compiled.to_value_as_str()?
                    )))?;

            let high_compiled = compile_expression(high, ctx)?;
            let high_compiled_date =
                high_compiled
                    .to_date_literal()
                    .ok_or(CompilationError::user(format!(
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
                        Err(CompilationError::user(format!(
                            "Column for IsNull must be a Dimension or TimeDimension, actual: {:?}",
                            compiled_expr
                        )))
                    }
                },
                _ => Err(CompilationError::user(format!(
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
                        Err(CompilationError::user(format!(
                            "Column for InExpr must be a Dimension or TimeDimension, actual: {:?}",
                            compiled_expr
                        )))
                    }
                },
                _ => Err(CompilationError::user(format!(
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
        _ => Err(CompilationError::unsupported(format!(
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
            CompiledFilter::SegmentFilter { member: _ } => Err(CompilationError::internal(
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
                            return Err(CompilationError::user(format!(
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
                return Err(CompilationError::unsupported(format!(
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
            return Err(CompilationError::unsupported(format!(
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
                CompilationError::unsupported(format!(
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
                return Err(CompilationError::user(format!(
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
                                    "number" => ColumnType::Double,
                                    _ => ColumnType::String,
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
                    return Err(CompilationError::unsupported(format!(
                        "Unsupported expression in projection: {:?}",
                        projection
                    )));
                }
            }
        }
    }

    Ok(builder)
}

#[derive(Clone)]
struct QueryPlanner {
    state: Arc<SessionState>,
    meta: Arc<MetaContext>,
    session_manager: Arc<SessionManager>,
}

impl QueryPlanner {
    pub fn new(
        state: Arc<SessionState>,
        meta: Arc<MetaContext>,
        session_manager: Arc<SessionManager>,
    ) -> Self {
        Self {
            state,
            meta,
            session_manager,
        }
    }

    /// Common case for both planners: meta & olap
    /// This method tries to detect what planner to use as earlier as possible
    /// and forward context to correct planner
    async fn select_to_plan(
        &self,
        stmt: &ast::Statement,
        q: &Box<ast::Query>,
    ) -> CompilationResult<QueryPlan> {
        // TODO move CUBESQL_REWRITE_ENGINE env to config
        let rewrite_engine = env::var("CUBESQL_REWRITE_ENGINE")
            .ok()
            .map(|v| v.parse::<bool>().unwrap())
            .unwrap_or(self.state.protocol == DatabaseProtocol::PostgreSQL);
        if rewrite_engine {
            return self.create_df_logical_plan(stmt.clone()).await;
        }

        let select = match &q.body {
            sqlparser::ast::SetExpr::Select(select) => select,
            _ => {
                return Err(CompilationError::unsupported(
                    "Unsupported Query".to_string(),
                ));
            }
        };

        if select.into.is_some() {
            return Err(CompilationError::unsupported(
                "Unsupported query type: SELECT INTO".to_string(),
            ));
        }

        let from_table = if select.from.len() == 1 {
            &select.from[0]
        } else {
            return self.create_df_logical_plan(stmt.clone()).await;
        };

        let (db_name, schema_name, table_name) = match &from_table.relation {
            ast::TableFactor::Table { name, .. } => match name {
                ast::ObjectName(identifiers) => {
                    match identifiers.len() {
                        // db.`KibanaSampleDataEcommerce`
                        2 => match self.state.protocol {
                            DatabaseProtocol::MySQL => (
                                identifiers[0].value.clone(),
                                "public".to_string(),
                                identifiers[1].value.clone(),
                            ),
                            DatabaseProtocol::PostgreSQL => (
                                "db".to_string(),
                                identifiers[0].value.clone(),
                                identifiers[1].value.clone(),
                            ),
                        },
                        // `KibanaSampleDataEcommerce`
                        1 => match self.state.protocol {
                            DatabaseProtocol::MySQL => (
                                "db".to_string(),
                                "public".to_string(),
                                identifiers[0].value.clone(),
                            ),
                            DatabaseProtocol::PostgreSQL => (
                                "db".to_string(),
                                "public".to_string(),
                                identifiers[0].value.clone(),
                            ),
                        },
                        _ => {
                            return Err(CompilationError::unsupported(format!(
                                "Table identifier: {:?}",
                                identifiers
                            )));
                        }
                    }
                }
            },
            factor => {
                return Err(CompilationError::unsupported(format!(
                    "table factor: {:?}",
                    factor
                )));
            }
        };

        match self.state.protocol {
            DatabaseProtocol::MySQL => {
                if db_name.to_lowercase() == "information_schema"
                    || db_name.to_lowercase() == "performance_schema"
                {
                    return self.create_df_logical_plan(stmt.clone()).await;
                }
            }
            DatabaseProtocol::PostgreSQL => {
                if schema_name.to_lowercase() == "information_schema"
                    || schema_name.to_lowercase() == "performance_schema"
                    || schema_name.to_lowercase() == "pg_catalog"
                {
                    return self.create_df_logical_plan(stmt.clone()).await;
                }
            }
        };

        if db_name.to_lowercase() != "db" {
            return Err(CompilationError::unsupported(format!(
                "Unable to access database {}",
                db_name
            )));
        }

        if !select.from[0].joins.is_empty() {
            return Err(CompilationError::unsupported(
                "Query with JOIN instruction(s)".to_string(),
            ));
        }

        if q.with.is_some() {
            return Err(CompilationError::unsupported(
                "Query with CTE instruction(s)".to_string(),
            ));
        }

        if !select.cluster_by.is_empty() {
            return Err(CompilationError::unsupported(
                "Query with CLUSTER BY instruction(s)".to_string(),
            ));
        }

        if !select.distribute_by.is_empty() {
            return Err(CompilationError::unsupported(
                "Query with DISTRIBUTE BY instruction(s)".to_string(),
            ));
        }

        if select.having.is_some() {
            return Err(CompilationError::unsupported(
                "Query with HAVING instruction(s)".to_string(),
            ));
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
                Box::new(dataframe::DataFrame::new(
                    vec![dataframe::Column::new(
                        "_".to_string(),
                        ColumnType::Int8,
                        ColumnFlags::empty(),
                    )],
                    vec![],
                )),
            ));
        };

        if let Some(cube) = self.meta.find_cube_with_name(&table_name) {
            let mut ctx = QueryContext::new(&cube);
            let mut builder = compile_select(select, &mut ctx)?;

            if let Some(limit_expr) = &q.limit {
                let limit = limit_expr.to_string().parse::<i32>().map_err(|e| {
                    CompilationError::unsupported(format!(
                        "Unable to parse limit: {}",
                        e.to_string()
                    ))
                })?;

                builder.with_limit(limit);
            }

            if let Some(offset_expr) = &q.offset {
                let offset = offset_expr.value.to_string().parse::<i32>().map_err(|e| {
                    CompilationError::unsupported(format!(
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

            let query = builder.build();
            let schema = query.meta_as_df_schema();

            let projection_expr = query.meta_as_df_projection_expr();
            let projection_schema = query.meta_as_df_projection_schema();

            let scan_node = LogicalPlan::Extension(Extension {
                node: Arc::new(CubeScanNode::new(
                    schema.clone(),
                    schema
                        .fields()
                        .iter()
                        .map(|f| MemberField::Member(f.name().to_string()))
                        .collect(),
                    query.request,
                    // @todo Remove after split!
                    self.state.auth_context().unwrap(),
                    CubeScanOptions { change_user: None },
                )),
            });
            let logical_plan = LogicalPlan::Projection(Projection {
                expr: projection_expr,
                input: Arc::new(scan_node),
                schema: projection_schema,
                alias: None,
            });

            let ctx = self.create_execution_ctx();
            Ok(QueryPlan::DataFusionSelect(
                StatusFlags::empty(),
                logical_plan,
                ctx,
            ))
        } else {
            Err(CompilationError::user(format!(
                "Unknown cube '{}'. Please ensure your schema files are valid.",
                table_name,
            )))
        }
    }

    pub async fn plan(&self, stmt: &ast::Statement) -> CompilationResult<QueryPlan> {
        match (stmt, &self.state.protocol) {
            (ast::Statement::Query(q), _) => self.select_to_plan(stmt, q).await,
            (ast::Statement::SetTransaction { .. }, _) => Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Box::new(dataframe::DataFrame::new(vec![], vec![])),
            )),
            (ast::Statement::SetNames { charset_name, .. }, DatabaseProtocol::MySQL) => {
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
                    Box::new(dataframe::DataFrame::new(vec![], vec![])),
                ))
            }
            (ast::Statement::Kill { .. }, DatabaseProtocol::MySQL) => Ok(QueryPlan::MetaOk(
                StatusFlags::empty(),
                CommandCompletion::Select(0),
            )),
            (ast::Statement::SetVariable { key_values }, _) => {
                self.set_variable_to_plan(&key_values)
            }
            (ast::Statement::ShowVariable { variable }, _) => {
                self.show_variable_to_plan(variable).await
            }
            (ast::Statement::ShowVariables { filter }, DatabaseProtocol::MySQL) => {
                self.show_variables_to_plan(&filter).await
            }
            (ast::Statement::ShowCreate { obj_name, obj_type }, DatabaseProtocol::MySQL) => {
                self.show_create_to_plan(&obj_name, &obj_type)
            }
            (
                ast::Statement::ShowColumns {
                    extended,
                    full,
                    filter,
                    table_name,
                },
                DatabaseProtocol::MySQL,
            ) => {
                self.show_columns_to_plan(*extended, *full, &filter, &table_name)
                    .await
            }
            (
                ast::Statement::ShowTables {
                    extended,
                    full,
                    filter,
                    db_name,
                },
                DatabaseProtocol::MySQL,
            ) => {
                self.show_tables_to_plan(*extended, *full, &filter, &db_name)
                    .await
            }
            (ast::Statement::ShowCollation { filter }, DatabaseProtocol::MySQL) => {
                self.show_collation_to_plan(&filter).await
            }
            (ast::Statement::ExplainTable { table_name, .. }, DatabaseProtocol::MySQL) => {
                self.explain_table_to_plan(&table_name).await
            }
            (
                ast::Statement::Explain {
                    statement,
                    verbose,
                    analyze,
                    ..
                },
                _,
            ) => self.explain_to_plan(&statement, *verbose, *analyze).await,
            (ast::Statement::Use { db_name }, DatabaseProtocol::MySQL) => {
                self.use_to_plan(&db_name)
            }
            (ast::Statement::StartTransaction { .. }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Real support
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Begin,
                ))
            }
            (ast::Statement::Commit { .. }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Real support
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Commit,
                ))
            }
            (ast::Statement::Rollback { .. }, DatabaseProtocol::PostgreSQL) => {
                // TODO: Real support
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Rollback,
                ))
            }
            (ast::Statement::Discard { object_type }, DatabaseProtocol::PostgreSQL) => {
                Ok(QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Discard(object_type.to_string()),
                ))
            }
            _ => Err(CompilationError::unsupported(format!(
                "Unsupported query type: {}",
                stmt.to_string()
            ))),
        }
    }

    async fn show_variable_to_plan(&self, variable: &Vec<Ident>) -> CompilationResult<QueryPlan> {
        let name = variable.to_vec()[0].value.clone();
        if self.state.protocol == DatabaseProtocol::PostgreSQL {
            let full_variable = variable.iter().map(|v| v.value.to_lowercase()).join("_");
            let full_variable = match full_variable.as_str() {
                "transaction_isolation_level" => "transaction_isolation",
                x => x,
            };
            let stmt = if name.eq_ignore_ascii_case("all") {
                parse_sql_to_statement(
                    &"SELECT name, setting, short_desc as description FROM pg_catalog.pg_settings"
                        .to_string(),
                    self.state.protocol.clone(),
                )?
            } else {
                parse_sql_to_statement(
                    // TODO: column name might be expected to match variable name
                    &format!(
                        "SELECT setting FROM pg_catalog.pg_settings where name = '{}'",
                        escape_single_quote_string(full_variable),
                    ),
                    self.state.protocol.clone(),
                )?
            };

            self.create_df_logical_plan(stmt).await
        } else if name.eq_ignore_ascii_case("databases") || name.eq_ignore_ascii_case("schemas") {
            Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Box::new(dataframe::DataFrame::new(
                    vec![dataframe::Column::new(
                        "Database".to_string(),
                        ColumnType::String,
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
        } else if name.eq_ignore_ascii_case("processlist") {
            let stmt = parse_sql_to_statement(
                &"SELECT * FROM information_schema.processlist".to_string(),
                self.state.protocol.clone(),
            )?;

            self.create_df_logical_plan(stmt).await
        } else if name.eq_ignore_ascii_case("warnings") {
            Ok(QueryPlan::MetaTabular(
                StatusFlags::empty(),
                Box::new(dataframe::DataFrame::new(
                    vec![
                        dataframe::Column::new(
                            "Level".to_string(),
                            ColumnType::VarStr,
                            ColumnFlags::NOT_NULL,
                        ),
                        dataframe::Column::new(
                            "Code".to_string(),
                            ColumnType::Int32,
                            ColumnFlags::NOT_NULL | ColumnFlags::UNSIGNED,
                        ),
                        dataframe::Column::new(
                            "Message".to_string(),
                            ColumnType::VarStr,
                            ColumnFlags::NOT_NULL,
                        ),
                    ],
                    vec![],
                )),
            ))
        } else {
            self.create_df_logical_plan(ast::Statement::ShowVariable {
                variable: variable.clone(),
            })
            .await
        }
    }

    async fn show_variables_to_plan(
        &self,
        filter: &Option<ast::ShowStatementFilter>,
    ) -> Result<QueryPlan, CompilationError> {
        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE VARIABLE_NAME {}", stmt.to_string())
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                return Err(CompilationError::unsupported(format!(
                    "Show variable doesnt support WHERE statement: {}",
                    stmt
                )))
            }
            Some(stmt @ ast::ShowStatementFilter::ILike(_)) => {
                return Err(CompilationError::user(format!(
                    "Show variable doesnt define ILIKE statement: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let stmt = parse_sql_to_statement(
            &format!("SELECT VARIABLE_NAME as Variable_name, VARIABLE_VALUE as Value FROM performance_schema.session_variables {} ORDER BY Variable_name DESC", filter),
            self.state.protocol.clone(),
        )?;

        self.create_df_logical_plan(stmt).await
    }

    fn show_create_to_plan(
        &self,
        obj_name: &ObjectName,
        obj_type: &ast::ShowCreateObject,
    ) -> Result<QueryPlan, CompilationError> {
        match obj_type {
            ast::ShowCreateObject::Table => {}
            _ => {
                return Err(CompilationError::user(format!(
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

        self.meta.cubes.iter().find(|c| c.name.eq(table_name_filter)).map(|cube| {
            let mut fields: Vec<String> = vec![];

            for column in &cube.get_columns() {
                fields.push(format!(
                    "`{}` {}{}",
                    column.get_name(),
                    column.get_mysql_column_type(),
                    if column.sql_can_be_null() { " NOT NULL" } else { "" }
                ));
            }

            QueryPlan::MetaTabular(StatusFlags::empty(), Box::new(dataframe::DataFrame::new(
                vec![
                    dataframe::Column::new(
                        "Table".to_string(),
                        ColumnType::String,
                        ColumnFlags::empty(),
                    ),
                    dataframe::Column::new(
                        "Create Table".to_string(),
                        ColumnType::String,
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
            CompilationError::user(format!(
                "Unknown table: {}",
                table_name_filter
            ))
        )
    }

    async fn show_columns_to_plan(
        &self,
        extended: bool,
        full: bool,
        filter: &Option<ast::ShowStatementFilter>,
        table_name: &ast::ObjectName,
    ) -> Result<QueryPlan, CompilationError> {
        let extended = match extended {
            false => "".to_string(),
            // The planner is unable to correctly process queries with UNION ALL in subqueries as of writing this.
            // Uncomment this to enable EXTENDED support once such queries can be processed.
            /*true => {
                let extended_columns = "'' AS `Type`, NULL AS `Collation`, 'NO' AS `Null`, '' AS `Key`, NULL AS `Default`, '' AS `Extra`, 'select' AS `Privileges`, '' AS `Comment`";
                format!("UNION ALL SELECT 'DB_TRX_ID' AS `Field`, 2 AS `Order`, {} UNION ALL SELECT 'DB_ROLL_PTR' AS `Field`, 3 AS `Order`, {}", extended_columns, extended_columns)
            }*/
            true => {
                return Err(CompilationError::unsupported(
                    "SHOW COLUMNS: EXTENDED is not implemented".to_string(),
                ))
            }
        };

        let columns = match full {
            false => "`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`",
            true => "`Field`, `Type`, `Collation`, `Null`, `Key`, `Default`, `Extra`, `Privileges`, `Comment`",
        };

        let mut object_name = table_name.0.clone();
        let table_name = match object_name.pop() {
            Some(table_name) => escape_single_quote_string(&table_name.value).to_string(),
            None => {
                return Err(CompilationError::internal(format!(
                    "Unexpected lack of table name"
                )))
            }
        };
        let db_name = match object_name.pop() {
            Some(db_name) => escape_single_quote_string(&db_name.value).to_string(),
            None => self.state.database().unwrap_or("db".to_string()).clone(),
        };

        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE `Field` {}", stmt.to_string())
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                format!("{}", stmt.to_string())
            }
            Some(stmt) => {
                return Err(CompilationError::user(format!(
                    "SHOW COLUMNS doesn't support requested filter: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let information_schema_sql = format!("SELECT `COLUMN_NAME` AS `Field`, 1 AS `Order`, `COLUMN_TYPE` AS `Type`, IF(`DATA_TYPE` = 'varchar', 'utf8mb4_0900_ai_ci', NULL) AS `Collation`, `IS_NULLABLE` AS `Null`, `COLUMN_KEY` AS `Key`, NULL AS `Default`, `EXTRA` AS `Extra`, 'select' AS `Privileges`, `COLUMN_COMMENT` AS `Comment` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = '{}' AND `TABLE_SCHEMA` = '{}' {}", table_name, db_name, extended);
        let stmt = parse_sql_to_statement(
            &format!(
                "SELECT {} FROM ({}) AS `COLUMNS` {}",
                columns, information_schema_sql, filter
            ),
            self.state.protocol.clone(),
        )?;

        self.create_df_logical_plan(stmt).await
    }

    async fn show_tables_to_plan(
        &self,
        // EXTENDED is accepted but does not alter the result
        _extended: bool,
        full: bool,
        filter: &Option<ast::ShowStatementFilter>,
        db_name: &Option<ast::Ident>,
    ) -> Result<QueryPlan, CompilationError> {
        let db_name = match db_name {
            Some(db_name) => db_name.clone(),
            None => Ident::new(self.state.database().unwrap_or("db".to_string())),
        };

        let column_name = format!("Tables_in_{}", db_name.value);
        let column_name = match db_name.quote_style {
            Some(quote_style) => Ident::with_quote(quote_style, column_name),
            None => Ident::new(column_name),
        };

        let columns = match full {
            false => format!("{}", column_name),
            true => format!("{}, `Table_type`", column_name),
        };

        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE {} {}", column_name, stmt)
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                format!("{}", stmt)
            }
            Some(stmt) => {
                return Err(CompilationError::user(format!(
                    "SHOW TABLES doesn't support requested filter: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let information_schema_sql = format!(
            "SELECT `TABLE_NAME` AS {}, `TABLE_TYPE` AS `Table_type` FROM `information_schema`.`TABLES`
WHERE `TABLE_SCHEMA` = '{}'",
            column_name,
            escape_single_quote_string(&db_name.value),
        );
        let stmt = parse_sql_to_statement(
            &format!(
                "SELECT {} FROM ({}) AS `TABLES` {}",
                columns, information_schema_sql, filter
            ),
            self.state.protocol.clone(),
        )?;

        self.create_df_logical_plan(stmt).await
    }

    async fn show_collation_to_plan(
        &self,
        filter: &Option<ast::ShowStatementFilter>,
    ) -> Result<QueryPlan, CompilationError> {
        let filter = match filter {
            Some(stmt @ ast::ShowStatementFilter::Like(_)) => {
                format!("WHERE `Collation` {}", stmt)
            }
            Some(stmt @ ast::ShowStatementFilter::Where(_)) => {
                format!("{}", stmt)
            }
            Some(stmt) => {
                return Err(CompilationError::user(format!(
                    "SHOW COLLATION doesn't support requested filter: {}",
                    stmt
                )))
            }
            None => "".to_string(),
        };

        let information_schema_sql = "SELECT `COLLATION_NAME` AS `Collation`, `CHARACTER_SET_NAME` AS `Charset`, `ID` AS `Id`, `IS_DEFAULT` AS `Default`, `IS_COMPILED` AS `Compiled`, `SORTLEN` AS `Sortlen`, `PAD_ATTRIBUTE` AS `Pad_attribute` FROM `information_schema`.`COLLATIONS` ORDER BY `Collation`";
        let stmt = parse_sql_to_statement(
            &format!(
                "SELECT * FROM ({}) AS `COLLATIONS` {}",
                information_schema_sql, filter
            ),
            self.state.protocol.clone(),
        )?;

        self.create_df_logical_plan(stmt).await
    }

    async fn explain_table_to_plan(
        &self,
        table_name: &ast::ObjectName,
    ) -> Result<QueryPlan, CompilationError> {
        // EXPLAIN <table> matches the SHOW COLUMNS output exactly, reuse the plan
        self.show_columns_to_plan(false, false, &None, table_name)
            .await
    }

    fn explain_to_plan(
        &self,
        statement: &Box<ast::Statement>,
        verbose: bool,
        analyze: bool,
    ) -> Pin<Box<dyn Future<Output = Result<QueryPlan, CompilationError>> + Send + Sync>> {
        let self_cloned = self.clone();

        let statement = statement.clone();
        // This Boxing construct here because of recursive call to self.plan()
        Box::pin(async move {
            let plan = self_cloned.plan(&statement).await?;

            match plan {
                QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => Ok(QueryPlan::MetaTabular(
                    StatusFlags::empty(),
                    Box::new(dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "Execution Plan".to_string(),
                            ColumnType::String,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![dataframe::TableValue::String(
                            "This query doesnt have a plan, because it already has values for response"
                                .to_string(),
                        )])],
                    )),
                )),
                QueryPlan::DataFusionSelect(flags, plan, context) => {
                    let plan = Arc::new(plan);
                    let schema = LogicalPlan::explain_schema();
                    let schema = schema.to_dfschema_ref().map_err(|err| {
                        CompilationError::internal(format!(
                            "Unable to get DF schema for explain plan: {}",
                            err
                        ))
                    })?;

                    let explain_plan = if analyze {
                        LogicalPlan::Analyze(Analyze {
                            verbose,
                            input: plan,
                            schema,
                        })
                    } else {
                        let stringified_plans = vec![plan.to_stringified(PlanType::InitialLogicalPlan)];

                        LogicalPlan::Explain(Explain {
                            verbose,
                            plan,
                            stringified_plans,
                            schema,
                        })
                    };

                    Ok(QueryPlan::DataFusionSelect(flags, explain_plan, context))
                }
            }
        })
    }

    fn use_to_plan(&self, db_name: &ast::Ident) -> Result<QueryPlan, CompilationError> {
        self.state.set_database(Some(db_name.value.clone()));

        Ok(QueryPlan::MetaOk(
            StatusFlags::empty(),
            CommandCompletion::Use,
        ))
    }

    fn set_variable_to_plan(
        &self,
        key_values: &Vec<ast::SetVariableKeyValue>,
    ) -> Result<QueryPlan, CompilationError> {
        let mut flags = StatusFlags::SERVER_STATE_CHANGED;

        let mut session_columns_to_update =
            DatabaseVariablesToUpdate::with_capacity(key_values.len());
        let mut global_columns_to_update =
            DatabaseVariablesToUpdate::with_capacity(key_values.len());

        match self.state.protocol {
            DatabaseProtocol::PostgreSQL => {
                for key_value in key_values.iter() {
                    let value: String = match &key_value.value[0] {
                        ast::Expr::Identifier(ident) => ident.value.to_string(),
                        ast::Expr::Value(val) => match val {
                            ast::Value::SingleQuotedString(single_quoted_str) => {
                                single_quoted_str.to_string()
                            }
                            ast::Value::DoubleQuotedString(double_quoted_str) => {
                                double_quoted_str.to_string()
                            }
                            ast::Value::Number(number, _) => number.to_string(),
                            _ => {
                                return Err(CompilationError::user(format!(
                                    "invalid {} variable format",
                                    key_value.key.value
                                )))
                            }
                        },
                        _ => {
                            return Err(CompilationError::user(format!(
                                "invalid {} variable format",
                                key_value.key.value
                            )))
                        }
                    };

                    session_columns_to_update.push(DatabaseVariable::system(
                        key_value.key.value.to_lowercase(),
                        ScalarValue::Utf8(Some(value.clone())),
                        None,
                    ));
                }
            }
            DatabaseProtocol::MySQL => {
                for key_value in key_values.iter() {
                    if key_value.key.value.to_lowercase() == "autocommit".to_string() {
                        flags |= StatusFlags::AUTOCOMMIT;

                        break;
                    }

                    let symbols: Vec<char> = key_value.key.value.chars().collect();
                    if symbols.len() < 2 {
                        continue;
                    }

                    let is_user_defined_var = symbols[0] == '@' && symbols[1] != '@';
                    let is_global_var =
                        (symbols[0] == '@' && symbols[1] == '@') || symbols[0] != '@';

                    let value: String = match &key_value.value[0] {
                        ast::Expr::Identifier(ident) => ident.value.to_string(),
                        ast::Expr::Value(val) => match val {
                            ast::Value::SingleQuotedString(single_quoted_str) => {
                                single_quoted_str.to_string()
                            }
                            ast::Value::DoubleQuotedString(double_quoted_str) => {
                                double_quoted_str.to_string()
                            }
                            ast::Value::Number(number, _) => number.to_string(),
                            _ => {
                                return Err(CompilationError::user(format!(
                                    "invalid {} variable format",
                                    key_value.key.value
                                )))
                            }
                        },
                        _ => {
                            return Err(CompilationError::user(format!(
                                "invalid {} variable format",
                                key_value.key.value
                            )))
                        }
                    };

                    if is_global_var {
                        let key = if symbols[0] == '@' {
                            key_value.key.value[2..].to_lowercase()
                        } else {
                            key_value.key.value.to_lowercase()
                        };
                        global_columns_to_update.push(DatabaseVariable::system(
                            key.to_lowercase(),
                            ScalarValue::Utf8(Some(value.clone())),
                            None,
                        ));
                    } else if is_user_defined_var {
                        let key = key_value.key.value[1..].to_lowercase();
                        session_columns_to_update.push(DatabaseVariable::user_defined(
                            key.to_lowercase(),
                            ScalarValue::Utf8(Some(value.clone())),
                            None,
                        ));
                    }
                }
            }
        }

        if !session_columns_to_update.is_empty() {
            self.state.set_variables(session_columns_to_update);
        }

        if !global_columns_to_update.is_empty() {
            self.session_manager
                .server
                .set_variables(global_columns_to_update, self.state.protocol.clone());
        }

        match self.state.protocol {
            DatabaseProtocol::PostgreSQL => Ok(QueryPlan::MetaOk(flags, CommandCompletion::Set)),
            // TODO: Verify that it's possible to use MetaOk too...
            DatabaseProtocol::MySQL => Ok(QueryPlan::MetaTabular(
                flags,
                Box::new(dataframe::DataFrame::new(vec![], vec![])),
            )),
        }
    }

    fn create_execution_ctx(&self) -> DFSessionContext {
        let query_planner = Arc::new(CubeQueryPlanner::new(
            self.session_manager.server.transport.clone(),
            self.state.get_load_request_meta(),
        ));
        let mut ctx = DFSessionContext::with_state(
            default_session_builder(
                DFSessionConfig::new()
                    .create_default_catalog_and_schema(false)
                    .with_information_schema(false)
                    .with_default_catalog_and_schema("db", "public"),
            )
            .with_query_planner(query_planner),
        );

        if self.state.protocol == DatabaseProtocol::MySQL {
            let system_variable_provider =
                VariablesProvider::new(self.state.clone(), self.session_manager.server.clone());
            let user_defined_variable_provider =
                VariablesProvider::new(self.state.clone(), self.session_manager.server.clone());

            ctx.register_variable(VarType::System, Arc::new(system_variable_provider));
            ctx.register_variable(
                VarType::UserDefined,
                Arc::new(user_defined_variable_provider),
            );
        }

        // udf
        if self.state.protocol == DatabaseProtocol::MySQL {
            ctx.register_udf(create_version_udf("8.0.25".to_string()));
            ctx.register_udf(create_db_udf("database".to_string(), self.state.clone()));
            ctx.register_udf(create_db_udf("schema".to_string(), self.state.clone()));
            ctx.register_udf(create_current_user_udf(
                self.state.clone(),
                "current_user",
                true,
            ));
            ctx.register_udf(create_user_udf(self.state.clone()));
        } else if self.state.protocol == DatabaseProtocol::PostgreSQL {
            ctx.register_udf(create_version_udf(
                "PostgreSQL 14.1 on x86_64-cubesql".to_string(),
            ));
            ctx.register_udf(create_db_udf(
                "current_database".to_string(),
                self.state.clone(),
            ));
            ctx.register_udf(create_db_udf(
                "current_schema".to_string(),
                self.state.clone(),
            ));
            ctx.register_udf(create_current_user_udf(
                self.state.clone(),
                "current_user",
                false,
            ));
            ctx.register_udf(create_current_user_udf(self.state.clone(), "user", false));
            ctx.register_udf(create_session_user_udf(self.state.clone()));
        }

        ctx.register_udf(create_connection_id_udf(self.state.clone()));
        ctx.register_udf(create_pg_backend_pid_udf(self.state.clone()));
        ctx.register_udf(create_instr_udf());
        ctx.register_udf(create_ucase_udf());
        ctx.register_udf(create_isnull_udf());
        ctx.register_udf(create_if_udf());
        ctx.register_udf(create_least_udf());
        ctx.register_udf(create_convert_tz_udf());
        ctx.register_udf(create_timediff_udf());
        ctx.register_udf(create_time_format_udf());
        ctx.register_udf(create_locate_udf());
        ctx.register_udf(create_date_udf());
        ctx.register_udf(create_makedate_udf());
        ctx.register_udf(create_year_udf());
        ctx.register_udf(create_quarter_udf());
        ctx.register_udf(create_hour_udf());
        ctx.register_udf(create_minute_udf());
        ctx.register_udf(create_second_udf());
        ctx.register_udf(create_dayofweek_udf());
        ctx.register_udf(create_dayofmonth_udf());
        ctx.register_udf(create_dayofyear_udf());
        ctx.register_udf(create_date_sub_udf());
        ctx.register_udf(create_date_add_udf());
        ctx.register_udf(create_str_to_date_udf());
        ctx.register_udf(create_current_timestamp_udf());
        ctx.register_udf(create_current_schema_udf());
        ctx.register_udf(create_current_schemas_udf());
        ctx.register_udf(create_format_type_udf());
        ctx.register_udf(create_pg_datetime_precision_udf());
        ctx.register_udf(create_pg_numeric_precision_udf());
        ctx.register_udf(create_pg_numeric_scale_udf());
        ctx.register_udf(create_pg_get_userbyid_udf(self.state.clone()));
        ctx.register_udf(create_pg_get_expr_udf());
        ctx.register_udf(create_pg_table_is_visible_udf());
        ctx.register_udf(create_pg_type_is_visible_udf());
        ctx.register_udf(create_pg_get_constraintdef_udf());
        ctx.register_udf(create_pg_truetypid_udf());
        ctx.register_udf(create_pg_truetypmod_udf());
        ctx.register_udf(create_to_char_udf());
        ctx.register_udf(create_array_lower_udf());
        ctx.register_udf(create_array_upper_udf());
        ctx.register_udf(create_pg_my_temp_schema());
        ctx.register_udf(create_pg_is_other_temp_schema());
        ctx.register_udf(create_has_schema_privilege_udf(self.state.clone()));
        ctx.register_udf(create_pg_total_relation_size_udf());
        ctx.register_udf(create_cube_regclass_cast_udf());
        ctx.register_udf(create_pg_get_serial_sequence_udf());
        ctx.register_udf(create_json_build_object_udf());

        // udaf
        ctx.register_udaf(create_measure_udaf());

        // udtf
        ctx.register_udtf(create_generate_series_udtf());
        ctx.register_udtf(create_unnest_udtf());
        ctx.register_udtf(create_generate_subscripts_udtf());
        ctx.register_udtf(create_pg_expandarray_udtf());

        ctx
    }

    async fn create_df_logical_plan(&self, stmt: ast::Statement) -> CompilationResult<QueryPlan> {
        match &stmt {
            ast::Statement::Query(query) => match &query.body {
                ast::SetExpr::Select(select) if select.into.is_some() => {
                    return Err(CompilationError::unsupported(
                        "Unsupported query type: SELECT INTO".to_string(),
                    ))
                }
                _ => (),
            },
            _ => (),
        }

        let ctx = self.create_execution_ctx();

        let df_state = Arc::new(ctx.state.write().clone());
        let cube_ctx = CubeContext::new(
            df_state,
            self.meta.clone(),
            self.session_manager.clone(),
            self.state.clone(),
        );
        let df_query_planner = SqlToRel::new_with_options(&cube_ctx, true);

        let plan = df_query_planner
            .statement_to_plan(DFStatement::Statement(Box::new(stmt.clone())))
            .map_err(|err| {
                CompilationError::internal(format!("Initial planning error: {}", err))
            })?;

        let optimized_plan = plan;
        // ctx.optimize(&plan).map_err(|err| {
        //    CompilationError::Internal(format!("Planning optimization error: {}", err))
        // })?;

        let mut converter = LogicalPlanToLanguageConverter::new(Arc::new(cube_ctx));
        let root = converter
            .add_logical_plan(&optimized_plan)
            .map_err(|e| CompilationError::internal(e.to_string()))?;
        let result = converter
            .take_rewriter()
            .find_best_plan(root, self.state.auth_context().unwrap())
            .await
            .map_err(|e| match e.cause {
                CubeErrorCauseType::Internal(_) => CompilationError::Internal(
                    format!(
                        "Error during rewrite: {}. Please check logs for additional information.",
                        e.message
                    ),
                    e.to_backtrace().unwrap_or_else(|| Backtrace::capture()),
                    None,
                ),
                CubeErrorCauseType::User(_) => CompilationError::user(format!(
                    "Error during rewrite: {}. Please check logs for additional information.",
                    e.message
                )),
            });

        if let Err(_) = &result {
            log::error!("It may be this query is not supported yet. Please post an issue on GitHub https://github.com/cube-js/cube.js/issues/new?template=sql_api_query_issue.md or ask about it in Slack https://slack.cube.dev.");
        }

        let rewrite_plan = result?;

        log::debug!("Rewrite: {:#?}", rewrite_plan);

        Ok(QueryPlan::DataFusionSelect(
            StatusFlags::empty(),
            rewrite_plan,
            ctx,
        ))
    }
}

pub async fn convert_statement_to_cube_query(
    stmt: &ast::Statement,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> CompilationResult<QueryPlan> {
    let stmt = CastReplacer::new().replace(stmt);
    let stmt = ToTimestampReplacer::new().replace(&stmt);
    let stmt = UdfWildcardArgReplacer::new().replace(&stmt);

    let planner = QueryPlanner::new(session.state.clone(), meta, session.session_manager.clone());
    planner.plan(&stmt).await
}

#[derive(Debug, PartialEq, Serialize)]
pub struct CompiledQuery {
    pub request: V1LoadRequestQuery,
    pub meta: Vec<CompiledQueryFieldMeta>,
}

impl CompiledQuery {
    pub fn meta_as_df_projection_expr(&self) -> Vec<Expr> {
        let mut projection = Vec::new();

        for meta_field in self.meta.iter() {
            projection.push(Expr::Alias(
                Box::new(Expr::Column(Column {
                    relation: None,
                    name: meta_field.column_from.clone(),
                })),
                meta_field.column_to.clone(),
            ));
        }

        projection
    }

    pub fn meta_as_df_projection_schema(&self) -> Arc<DFSchema> {
        let mut fields: Vec<DFField> = Vec::new();

        for meta_field in self.meta.iter() {
            fields.push(DFField::new(
                None,
                meta_field.column_to.as_str(),
                df_data_type_by_column_type(meta_field.column_type.clone()),
                false,
            ));
        }

        DFSchemaRef::new(DFSchema::new_with_metadata(fields, HashMap::new()).unwrap())
    }

    pub fn meta_as_df_schema(&self) -> Arc<DFSchema> {
        let mut fields: Vec<DFField> = Vec::new();

        for meta_field in self.meta.iter() {
            let exists = fields
                .iter()
                .any(|field| field.name() == &meta_field.column_from);
            if !exists {
                fields.push(DFField::new(
                    None,
                    meta_field.column_from.as_str(),
                    match meta_field.column_type {
                        ColumnType::Int32 | ColumnType::Int64 => DataType::Int64,
                        ColumnType::String => DataType::Utf8,
                        ColumnType::Double => DataType::Float64,
                        ColumnType::Int8 => DataType::Boolean,
                        _ => panic!("Unimplemented support for {:?}", meta_field.column_type),
                    },
                    false,
                ));
            }
        }

        DFSchemaRef::new(DFSchema::new_with_metadata(fields, HashMap::new()).unwrap())
    }
}

pub enum QueryPlan {
    // Meta will not be executed in DF,
    // we already knows how respond to it
    MetaOk(StatusFlags, CommandCompletion),
    MetaTabular(StatusFlags, Box<dataframe::DataFrame>),
    // Query will be executed via Data Fusion
    DataFusionSelect(StatusFlags, LogicalPlan, DFSessionContext),
}

impl fmt::Debug for QueryPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryPlan::MetaOk(flags, completion) => {
                f.write_str(&format!(
                    "MetaOk(StatusFlags: {:?}, CommandCompletion: {:?})", flags, completion
                ))
            },
            QueryPlan::MetaTabular(flags, _) => {
                f.write_str(&format!(
                    "MetaTabular(StatusFlags: {:?}, DataFrame: hidden)",
                    flags
                ))
            },
            QueryPlan::DataFusionSelect(flags, _, _) => {
                f.write_str(&format!(
                    "DataFusionSelect(StatusFlags: {:?}, LogicalPlan: hidden, DFSessionContext: hidden)",
                    flags
                ))
            },
        }
    }
}

impl QueryPlan {
    pub fn as_logical_plan(self) -> LogicalPlan {
        match self {
            QueryPlan::DataFusionSelect(_, plan, _) => plan,
            QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => {
                panic!("This query doesnt have a plan, because it already has values for response")
            }
        }
    }

    pub fn print(&self, pretty: bool) -> Result<String, CubeError> {
        match self {
            QueryPlan::DataFusionSelect(_, plan, _) => {
                if pretty {
                    Ok(plan.display_indent().to_string())
                } else {
                    Ok(plan.display().to_string())
                }
            }
            QueryPlan::MetaOk(_, _) | QueryPlan::MetaTabular(_, _) => Ok(
                "This query doesnt have a plan, because it already has values for response"
                    .to_string(),
            ),
        }
    }
}

pub async fn convert_sql_to_cube_query(
    query: &String,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> CompilationResult<QueryPlan> {
    let stmt = parse_sql_to_statement(&query, session.state.protocol.clone())?;
    convert_statement_to_cube_query(&stmt, meta, session).await
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cubeclient::models::{
        V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment, V1LoadResponse,
    };
    use datafusion::dataframe::DataFrame as DFDataFrame;
    use pretty_assertions::assert_eq;
    use regex::Regex;

    use super::*;
    use crate::{
        sql::{
            dataframe::batch_to_dataframe, types::StatusFlags, AuthContextRef,
            AuthenticateResponse, HttpAuthContext, ServerManager, SqlAuthService,
        },
        transport::{LoadRequestMeta, TransportService},
    };
    use datafusion::logical_plan::PlanVisitor;
    use log::Level;
    use simple_logger::SimpleLogger;

    lazy_static! {
        pub static ref TEST_LOGGING_INITIALIZED: std::sync::RwLock<bool> =
            std::sync::RwLock::new(false);
    }

    fn init_logger() {
        let mut initialized = TEST_LOGGING_INITIALIZED.write().unwrap();
        if !*initialized {
            let log_level = Level::Trace;
            let logger = SimpleLogger::new()
                .with_level(Level::Error.to_level_filter())
                .with_module_level("cubeclient", log_level.to_level_filter())
                .with_module_level("cubesql", log_level.to_level_filter())
                .with_module_level("datafusion", Level::Warn.to_level_filter())
                .with_module_level("pg-srv", Level::Warn.to_level_filter());

            log::set_boxed_logger(Box::new(logger)).unwrap();
            log::set_max_level(log_level.to_level_filter());
            *initialized = true;
        }
    }

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
                    V1CubeMetaDimension {
                        name: "KibanaSampleDataEcommerce.has_subscription".to_string(),
                        _type: "boolean".to_string(),
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

    fn get_test_tenant_ctx() -> Arc<MetaContext> {
        Arc::new(MetaContext::new(get_test_meta()))
    }

    fn get_test_session(protocol: DatabaseProtocol) -> Arc<Session> {
        let server = Arc::new(ServerManager::new(
            get_test_auth(),
            get_test_transport(),
            None,
        ));

        let session_manager = Arc::new(SessionManager::new(server.clone()));
        let session = session_manager.create_session(protocol, "127.0.0.1".to_string());

        // Populate like shims
        session.state.set_database(Some("db".to_string()));
        session.state.set_user(Some("ovr".to_string()));

        let auth_ctx = HttpAuthContext {
            access_token: "access_token".to_string(),
            base_path: "base_path".to_string(),
        };

        session.state.set_auth_context(Some(Arc::new(auth_ctx)));

        session
    }

    fn get_test_auth() -> Arc<dyn SqlAuthService> {
        #[derive(Debug)]
        struct TestSqlAuth {}

        #[async_trait]
        impl SqlAuthService for TestSqlAuth {
            async fn authenticate(
                &self,
                _user: Option<String>,
            ) -> Result<AuthenticateResponse, CubeError> {
                Ok(AuthenticateResponse {
                    context: Arc::new(HttpAuthContext {
                        access_token: "fake".to_string(),
                        base_path: "fake".to_string(),
                    }),
                    password: None,
                })
            }
        }

        Arc::new(TestSqlAuth {})
    }

    fn get_test_transport() -> Arc<dyn TransportService> {
        #[derive(Debug)]
        struct TestConnectionTransport {}

        #[async_trait]
        impl TransportService for TestConnectionTransport {
            // Load meta information about cubes
            async fn meta(&self, _ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
                panic!("It's a fake transport");
            }

            // Execute load query
            async fn load(
                &self,
                _query: V1LoadRequestQuery,
                _ctx: AuthContextRef,
                _meta_fields: LoadRequestMeta,
            ) -> Result<V1LoadResponse, CubeError> {
                panic!("It's a fake transport");
            }
        }

        Arc::new(TestConnectionTransport {})
    }

    async fn convert_select_to_query_plan(query: String, db: DatabaseProtocol) -> QueryPlan {
        let query =
            convert_sql_to_cube_query(&query, get_test_tenant_ctx(), get_test_session(db)).await;

        query.unwrap()
    }

    fn find_cube_scan_deep_search(parent: Arc<LogicalPlan>) -> CubeScanNode {
        pub struct FindCubeScanNodeVisitor(Option<CubeScanNode>);

        impl PlanVisitor for FindCubeScanNodeVisitor {
            type Error = CubeError;

            fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
                if let LogicalPlan::Extension(ext) = plan {
                    if let Some(scan_node) = ext.node.as_any().downcast_ref::<CubeScanNode>() {
                        self.0 = Some(scan_node.clone());
                    }
                }
                Ok(true)
            }
        }

        let mut visitor = FindCubeScanNodeVisitor(None);
        parent.accept(&mut visitor).unwrap();
        visitor.0.expect("No CubeScanNode was found in plan")
    }
    trait LogicalPlanTestUtils {
        fn find_projection_schema(&self) -> DFSchemaRef;

        fn find_cube_scan(&self) -> CubeScanNode;
    }

    impl LogicalPlanTestUtils for LogicalPlan {
        fn find_projection_schema(&self) -> DFSchemaRef {
            match self {
                LogicalPlan::Projection(proj) => proj.schema.clone(),
                _ => panic!("Root plan node is not projection!"),
            }
        }

        fn find_cube_scan(&self) -> CubeScanNode {
            find_cube_scan_deep_search(Arc::new(self.clone()))
        }
    }

    #[tokio::test]
    async fn test_select_measure_via_function() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(maxPrice), MEASURE(minPrice), MEASURE(avgPrice) FROM KibanaSampleDataEcommerce".to_string(),
        DatabaseProtocol::MySQL).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
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
            }
        );
    }

    #[tokio::test]
    async fn test_select_compound_identifiers() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MEASURE(`KibanaSampleDataEcommerce`.`maxPrice`) AS maxPrice, MEASURE(`KibanaSampleDataEcommerce`.`minPrice`) AS minPrice FROM KibanaSampleDataEcommerce".to_string(), DatabaseProtocol::MySQL
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
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
            }
        );
    }

    #[tokio::test]
    async fn test_select_measure_aggregate_functions() {
        let query_plan = convert_select_to_query_plan(
            "SELECT MAX(maxPrice), MIN(minPrice), AVG(avgPrice) FROM KibanaSampleDataEcommerce"
                .to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
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
            }
        );

        assert_eq!(
            logical_plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
            vec![DataType::Float64, DataType::Float64, DataType::Float64]
        );
    }

    #[tokio::test]
    async fn test_change_user_via_filter() {
        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher'"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_change_user_via_filter_and() {
        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher' AND customer_gender = 'male'".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
            .await;

        let cube_scan = query_plan.as_logical_plan().find_cube_scan();

        assert_eq!(cube_scan.options.change_user, Some("gopher".to_string()));

        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["male".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn test_change_user_via_filter_or() {
        // OR is not allowed for __user
        let query =
            convert_sql_to_cube_query(
                &"SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce WHERE __user = 'gopher' OR customer_gender = 'male'".to_string(),
                get_test_tenant_ctx(),
                get_test_session(DatabaseProtocol::PostgreSQL)
            ).await;

        // TODO: We need to propagate error to result, to assert message
        query.unwrap_err();
    }

    #[tokio::test]
    async fn test_order_alias_for_measure_default() {
        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(*) as cnt FROM KibanaSampleDataEcommerce ORDER BY cnt".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
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

    #[tokio::test]
    async fn test_order_by() {
        init_logger();

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
            (
                "SELECT COUNT(*) count, customer_gender, order_date FROM KibanaSampleDataEcommerce GROUP BY customer_gender, order_date ORDER BY order_date".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.customer_gender".to_string(),
                        "KibanaSampleDataEcommerce.order_date".to_string(),
                    ]),
                    time_dimensions: None,
                    order: Some(vec![vec![
                        "KibanaSampleDataEcommerce.order_date".to_string(),
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
                "SELECT taxful_total_price FROM `db`.`KibanaSampleDataEcommerce` ORDER BY `KibanaSampleDataEcommerce`.`taxful_total_price`".to_string(),
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
            let query_plan =
                convert_select_to_query_plan(sql.to_string(), DatabaseProtocol::MySQL).await;

            assert_eq!(
                &query_plan.as_logical_plan().find_cube_scan().request,
                expected_request
            )
        }
    }

    #[tokio::test]
    async fn test_order_function_date() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE(order_date) FROM KibanaSampleDataEcommerce ORDER BY DATE(order_date) DESC"
                .to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
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
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE(order_date) FROM KibanaSampleDataEcommerce GROUP BY DATE(order_date) ORDER BY DATE(order_date) DESC"
                .to_string(),
            DatabaseProtocol::MySQL,
        ).await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
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
        );
    }

    #[tokio::test]
    async fn test_select_all_fields_by_asterisk_limit_100() {
        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce LIMIT 100".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .dimensions,
            Some(vec![
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                "KibanaSampleDataEcommerce.has_subscription".to_string(),
            ])
        )
    }

    #[tokio::test]
    async fn test_select_all_fields_by_asterisk_limit_100_offset_50() {
        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce LIMIT 100 OFFSET 50".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .dimensions,
            Some(vec![
                "KibanaSampleDataEcommerce.order_date".to_string(),
                "KibanaSampleDataEcommerce.customer_gender".to_string(),
                "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                "KibanaSampleDataEcommerce.has_subscription".to_string(),
            ])
        )
    }

    #[tokio::test]
    async fn test_select_two_fields() {
        let query_plan = convert_select_to_query_plan(
            "SELECT order_date, customer_gender FROM KibanaSampleDataEcommerce".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await;

        assert_eq!(
            query_plan.as_logical_plan().find_cube_scan().request,
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

    #[tokio::test]
    async fn test_select_fields_alias() {
        let query_plan = convert_select_to_query_plan(
            "SELECT order_date as order_date, customer_gender as customer_gender FROM KibanaSampleDataEcommerce"
                .to_string(), DatabaseProtocol::MySQL
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
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
        );

        // assert_eq!(
        //     logical_plan.schema().clone(),
        //     Arc::new(
        //         DFSchema::new_with_metadata(
        //             vec![
        //                 DFField::new(None, "order_date", DataType::Utf8, false),
        //                 DFField::new(None, "customer_gender", DataType::Utf8, false),
        //             ],
        //             HashMap::new()
        //         )
        //         .unwrap()
        //     ),
        // );
    }

    #[tokio::test]
    async fn test_select_where_false() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT * FROM KibanaSampleDataEcommerce WHERE 1 = 0".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    "KibanaSampleDataEcommerce.has_subscription".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: Some(1),
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_projection_with_casts() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \
             CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS \"customer_gender\",\
             \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",\
             \"KibanaSampleDataEcommerce\".\"maxPrice\" AS \"maxPrice\",\
             \"KibanaSampleDataEcommerce\".\"minPrice\" AS \"minPrice\",\
             \"KibanaSampleDataEcommerce\".\"avgPrice\" AS \"avgPrice\",\
             \"KibanaSampleDataEcommerce\".\"order_date\" AS \"order_date\",\
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price1\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price2\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price3\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price4\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price5\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price6\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price7\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price8\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price9\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price10\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price11\",
             \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price12\"
             FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_min_max() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MIN(\"KibanaSampleDataEcommerce\".\"order_date\") AS \"tmn:timestamp:min\", MAX(\"KibanaSampleDataEcommerce\".\"order_date\") AS \"tmn:timestamp:max\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_min_max_number() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT MIN(\"KibanaSampleDataEcommerce\".\"taxful_total_price\") AS \"tmn:timestamp:min\", MAX(\"KibanaSampleDataEcommerce\".\"taxful_total_price\") AS \"tmn:timestamp:max\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_filter_and_group_by() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE (CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) = 'female') GROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn tableau_having_count_on_cube_without_count() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(DISTINCT \"Logs\".\"agentCount\") AS \"sum:count:ok\" FROM \"public\".\"Logs\" \"Logs\" HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["Logs.agentCount".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_boolean_filter_inplace_where() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE \"KibanaSampleDataEcommerce\".\"is_female\" HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec!["KibanaSampleDataEcommerce.is_female".to_string()]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["0".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE NOT(\"KibanaSampleDataEcommerce\".\"has_subscription\") HAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.has_subscription".to_string()),
                        operator: Some("equals".to_string()),
                        values: Some(vec!["false".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.count".to_string()),
                        operator: Some("gt".to_string()),
                        values: Some(vec!["0".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
            }
        );
    }

    #[tokio::test]
    async fn tableau_not_null_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT \"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS \"taxful_total_price\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE (NOT (\"KibanaSampleDataEcommerce\".\"taxful_total_price\" IS NULL)) GROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn tableau_current_timestamp() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS \"COL\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = &query_plan.print(true).unwrap();

        let re = Regex::new(r"TimestampNanosecond\(\d+, None\)").unwrap();
        let logical_plan = re
            .replace_all(logical_plan, "TimestampNanosecond(0, None)")
            .as_ref()
            .to_string();

        assert_eq!(
            logical_plan,
            "Projection: TimestampNanosecond(0, None) AS COL\
            \n  EmptyRelation",
        );
    }

    #[tokio::test]
    async fn tableau_time_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\" FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\" WHERE ((\"KibanaSampleDataEcommerce\".\"order_date\" >= (TIMESTAMP '2020-12-25 22:48:48.000')) AND (\"KibanaSampleDataEcommerce\".\"order_date\" <= (TIMESTAMP '2022-04-01 00:00:00.000')))".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2020-12-25 22:48:48.000".to_string(),
                        "2022-04-01 00:00:00.000".to_string()
                    ]))
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn superset_pg_time_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT DATE_TRUNC('week', \"order_date\") AS __timestamp,
               count(count) AS \"COUNT(count)\"
FROM public.\"KibanaSampleDataEcommerce\"
WHERE \"order_date\" >= TO_TIMESTAMP('2021-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND \"order_date\" < TO_TIMESTAMP('2022-05-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY DATE_TRUNC('week', \"order_date\")
ORDER BY \"COUNT(count)\" DESC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: Some(json!(vec![
                        "2021-05-15T00:00:00.000Z".to_string(),
                        "2022-05-14T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "desc".to_string()
                ]]),
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn power_bi_dimension_only() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"_\".\"customer_gender\"\r\nfrom \r\n(\r\n    select \"rows\".\"customer_gender\" as \"customer_gender\"\r\n    from \r\n    (\r\n        select \"customer_gender\"\r\n        from \"public\".\"KibanaSampleDataEcommerce\" \"$Table\"\r\n    ) \"rows\"\r\n    group by \"customer_gender\"\r\n) \"_\"\r\norder by \"_\".\"customer_gender\"\r\nlimit 1001".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    "asc".to_string(),
                ],],),
                limit: Some(1001),
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn power_bi_is_not_empty() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select sum(\"rows\".\"count\") as \"a0\" from (select \"_\".\"count\" from \"public\".\"KibanaSampleDataEcommerce\" \"_\" where (not \"_\".\"customer_gender\" is null and not \"_\".\"customer_gender\" = '' or not (not \"_\".\"customer_gender\" is null))) \"rows\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: None,
                            operator: None,
                            values: None,
                            or: None,
                            and: Some(vec![
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("set".to_string()),
                                    values: None,
                                    or: None,
                                    and: None,
                                }),
                                json!(V1LoadRequestQueryFilterItem {
                                    member: Some(
                                        "KibanaSampleDataEcommerce.customer_gender".to_string()
                                    ),
                                    operator: Some("notEquals".to_string()),
                                    values: Some(vec!["".to_string()]),
                                    or: None,
                                    and: None,
                                })
                            ])
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
            }
        );
    }

    #[tokio::test]
    async fn non_cube_filters_cast_kept() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT id FROM information_schema.testing_dataset WHERE id > CAST('0' AS INTEGER)"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.print(true).unwrap();
        assert!(
            logical_plan.contains("CAST"),
            "{:?} doesn't contain CAST",
            logical_plan
        );
    }

    #[tokio::test]
    async fn tableau_default_having() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:count:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nHAVING (COUNT(1) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        let cube_scan = logical_plan.find_cube_scan();
        assert_eq!(
            cube_scan.request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["0".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );

        assert_eq!(
            cube_scan
                .schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>(),
            vec!["sum:count:ok".to_string(),]
        );
        assert_eq!(
            &cube_scan.member_fields,
            &vec![MemberField::Member(
                "KibanaSampleDataEcommerce.count".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn tableau_group_by_month() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT COUNT(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:bytesBilled:ok\",\n  DATE_TRUNC( 'MONTH', CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS TIMESTAMP) ) AS \"tmn:timestamp:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 2".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_group_by_month_and_dimension() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS \"query\",\n  SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:bytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_extract_year() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(TRUNC(EXTRACT(YEAR FROM \"KibanaSampleDataEcommerce\".\"order_date\")) AS INTEGER) AS \"yr:timestamp:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST(TRUNC(EXTRACT(YEAR FROM \"KibanaSampleDataEcommerce\".\"order_date\")) AS INTEGER) AS \"yr:timestamp:ok\", SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:teraBytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_week() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT CAST((DATE_TRUNC( 'day', CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS DATE) ) + (-EXTRACT(DOW FROM \"KibanaSampleDataEcommerce\".\"order_date\") * INTERVAL '1 DAY')) AS DATE) AS \"yr:timestamp:ok\", SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:teraBytesBilled:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nGROUP BY 1".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn tableau_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:freeCount:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nWHERE (CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) = 'female')".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn tableau_contains_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "SELECT SUM(\"KibanaSampleDataEcommerce\".\"count\") AS \"sum:freeCount:ok\"\nFROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"\nWHERE (STRPOS(CAST(LOWER(CAST(CAST(\"KibanaSampleDataEcommerce\".\"customer_gender\" AS TEXT) AS TEXT)) AS TEXT),CAST('fem' AS TEXT)) > 0)".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn measure_used_on_dimension() {
        init_logger();

        let create_query = convert_sql_to_cube_query(
            &"SELECT MEASURE(customer_gender) FROM \"public\".\"KibanaSampleDataEcommerce\" \"KibanaSampleDataEcommerce\"".to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL),
        ).await;

        assert_eq!(
            create_query.err().unwrap().message(),
            "Error during rewrite: Dimension 'customer_gender' was used with the aggregate function 'MEASURE()'. Please use a measure instead. Please check logs for additional information.",
        );
    }

    #[tokio::test]
    async fn powerbi_contains_filter() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"rows\".\"customer_gender\" as \"customer_gender\",
\n    sum(\"rows\".\"count\") as \"a0\"\
\nfrom\
\n(\
\n    select \"_\".\"count\",\
\n        \"_\".\"customer_gender\"\
\n    from \"public\".\"KibanaSampleDataEcommerce\" \"_\"\
\n    where strpos((case\
\n        when \"_\".\"customer_gender\" is not null\
\n        then \"_\".\"customer_gender\"\
\n        else ''\
\n    end), 'fem') > 0\
\n) \"rows\"\
\ngroup by \"customer_gender\"\
\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(50000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn powerbi_inner_wrapped_dates() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"_\".\"created_at_day\",\
\n    \"_\".\"a0\"\
\nfrom \
\n(\
\n    select \"rows\".\"created_at_day\" as \"created_at_day\",\
\n        sum(\"rows\".\"cnt\") as \"a0\"\
\n    from \
\n    (\
\n        select count(*) cnt,date_trunc('day', order_date) as created_at_day, date_trunc('month', order_date) as created_at_month from public.KibanaSampleDataEcommerce group by 2, 3\
\n    ) \"rows\"\
\n    group by \"created_at_day\"\
\n) \"_\"\
\nwhere not \"_\".\"a0\" is null\
\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: Some(50000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.count".to_string()),
                    operator: Some("set".to_string()),
                    values: None,
                    or: None,
                    and: None,
                }]),
            }
        );
    }

    #[tokio::test]
    async fn powerbi_inner_wrapped_asterisk() {
        init_logger();

        let query_plan = convert_select_to_query_plan(
            "select \"rows\".\"customer_gender\" as \"customer_gender\",\
\n    \"rows\".\"created_at_month\" as \"created_at_month\"\
\nfrom \
\n(\
\n    select \"_\".\"count\",\
\n        \"_\".\"minPrice\",\
\n        \"_\".\"maxPrice\",\
\n        \"_\".\"avgPrice\",\
\n        \"_\".\"order_date\",\
\n        \"_\".\"customer_gender\",\
\n        \"_\".\"created_at_day\",\
\n        \"_\".\"created_at_month\"\
\n    from \
\n    (\
\n        select *, date_trunc('day', order_date) created_at_day, date_trunc('month', order_date) created_at_month from public.KibanaSampleDataEcommerce\
\n    ) \"_\"\
\n    where \"_\".\"created_at_month\" < timestamp '2022-06-13 00:00:00' and \"_\".\"created_at_month\" >= timestamp '2021-12-16 00:00:00'\
\n) \"rows\"\
\ngroup by \"customer_gender\",\
\n    \"created_at_month\"\
\nlimit 1000001"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: Some(json!(vec![
                        "2021-12-16 00:00:00".to_string(),
                        "2022-06-13 00:00:00".to_string()
                    ])),
                }]),
                order: None,
                limit: Some(50000),
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn test_select_aggregations() {
        let variants = vec![
            (
                "SELECT COUNT(*) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(*) FROM db.KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(1) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(count) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(DISTINCT agentCount) FROM Logs".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["Logs.agentCount".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT COUNT(DISTINCT agentCountApprox) FROM Logs".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["Logs.agentCountApprox".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
            (
                "SELECT MAX(`maxPrice`) FROM KibanaSampleDataEcommerce".to_string(),
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                },
            ),
        ];

        for (input_query, expected_request) in variants.iter() {
            let logical_plan =
                convert_select_to_query_plan(input_query.clone(), DatabaseProtocol::MySQL)
                    .await
                    .as_logical_plan();

            assert_eq!(&logical_plan.find_cube_scan().request, expected_request);
        }
    }

    #[tokio::test]
    async fn test_select_error() {
        let rewrite_engine = env::var("CUBESQL_REWRITE_ENGINE")
            .ok()
            .map(|v| v.parse::<bool>().unwrap())
            .unwrap_or(false);
        let variants = if rewrite_engine {
            vec![
                (
                    "SELECT COUNT(maxPrice) FROM KibanaSampleDataEcommerce".to_string(),
                    CompilationError::user("Error during rewrite: Measure aggregation type doesn't match. The aggregation type for 'maxPrice' is 'MAX()' but 'COUNT()' was provided. Please check logs for additional information.".to_string()),
                ),
                (
                    "SELECT COUNT(order_date) FROM KibanaSampleDataEcommerce".to_string(),
                    CompilationError::user("Error during rewrite: Dimension 'order_date' was used with the aggregate function 'COUNT()'. Please use a measure instead. Please check logs for additional information.".to_string()),
                ),
            ]
        } else {
            vec![
                // Count agg fn
                (
                    "SELECT COUNT(maxPrice) FROM KibanaSampleDataEcommerce".to_string(),
                    CompilationError::user("Measure aggregation type doesn't match. The aggregation type for 'maxPrice' is 'MAX()' but 'COUNT()' was provided".to_string()),
                ),
                (
                    "SELECT COUNT(order_date) FROM KibanaSampleDataEcommerce".to_string(),
                    CompilationError::user("Dimension 'order_date' was used with the aggregate function 'COUNT()'. Please use a measure instead".to_string()),
                ),
                // (
                //     "SELECT COUNT(2) FROM KibanaSampleDataEcommerce".to_string(),
                //     CompilationError::user("Unable to use number '2' as argument to aggregation function".to_string()),
                // ),
                // (
                //     "SELECT COUNT(unknownIdentifier) FROM KibanaSampleDataEcommerce".to_string(),
                //     CompilationError::user("Unable to find measure with name 'unknownIdentifier' which is used as argument to aggregation function 'COUNT()'".to_string()),
                // ),
                // Another aggregation functions
                // (
                //     "SELECT COUNT(DISTINCT *) FROM KibanaSampleDataEcommerce".to_string(),
                //     CompilationError::user("Unable to use '*' as argument to aggregation function 'COUNT()' (only COUNT() supported)".to_string()),
                // ),
                // (
                //     "SELECT MAX(*) FROM KibanaSampleDataEcommerce".to_string(),
                //     CompilationError::user("Unable to use '*' as argument to aggregation function 'MAX()' (only COUNT() supported)".to_string()),
                // ),
                // (
                //     "SELECT MAX(order_date) FROM KibanaSampleDataEcommerce".to_string(),
                //     CompilationError::user("Dimension 'order_date' was used with the aggregate function 'MAX()'. Please use a measure instead".to_string()),
                // ),
                // (
                //     "SELECT MAX(minPrice) FROM KibanaSampleDataEcommerce".to_string(),
                //     CompilationError::user("Measure aggregation type doesn't match. The aggregation type for 'minPrice' is 'MIN()' but 'MAX()' was provided".to_string()),
                // ),
                // (
                //     "SELECT MAX(unknownIdentifier) FROM KibanaSampleDataEcommerce".to_string(),
                //     CompilationError::user("Unable to find measure with name 'unknownIdentifier' which is used as argument to aggregation function 'MAX()'".to_string()),
                // ),
                // Check restrictions for segments usage
                // (
                //     "SELECT is_male FROM KibanaSampleDataEcommerce".to_string(),
                //     CompilationError::user("Unable to use segment 'is_male' as column in SELECT statement".to_string()),
                // ),
                // (
                //     "SELECT COUNT(*) FROM KibanaSampleDataEcommerce GROUP BY is_male".to_string(),
                //     CompilationError::user("Unable to use segment 'is_male' in GROUP BY".to_string()),
                // ),
                // (
                //     "SELECT COUNT(*) FROM KibanaSampleDataEcommerce ORDER BY is_male DESC".to_string(),
                //     CompilationError::user("Unable to use segment 'is_male' in ORDER BY".to_string()),
                // ),
            ]
        };

        for (input_query, expected_error) in variants.iter() {
            let query = convert_sql_to_cube_query(
                &input_query,
                get_test_tenant_ctx(),
                get_test_session(DatabaseProtocol::MySQL),
            )
            .await;

            match query {
                Ok(_) => panic!("Query ({}) should return error", input_query),
                Err(e) => assert_eq!(&e.with_meta(None), expected_error, "for {}", input_query),
            }
        }
    }

    #[tokio::test]
    async fn test_group_by_date_trunc() {
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
            let logical_plan = convert_select_to_query_plan(
                format!("SELECT COUNT(*), {} AS __timestamp FROM KibanaSampleDataEcommerce GROUP BY __timestamp", subquery), DatabaseProtocol::MySQL
            ).await.as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
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
                }
            );

            // assert_eq!(
            //     logical_plan
            //         .find_cube_scan()
            //         .schema
            //         .fields()
            //         .iter()
            //         .map(|f| f.name().to_string())
            //         .collect::<Vec<_>>(),
            //     vec!["COUNT(UInt8(1))", "__timestamp"]
            // );

            // assert_eq!(
            //     logical_plan.find_cube_scan().member_fields,
            //     vec![
            //         "KibanaSampleDataEcommerce.count",
            //         &format!(
            //             "KibanaSampleDataEcommerce.order_date.{}",
            //             expected_granularity
            //         )
            //     ]
            // );
        }
    }

    #[tokio::test]
    async fn test_group_by_date_granularity_superset() {
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
            let logical_plan = convert_select_to_query_plan(
                format!("SELECT COUNT(*), {} AS __timestamp FROM KibanaSampleDataEcommerce GROUP BY __timestamp", subquery), DatabaseProtocol::MySQL
            ).await.as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
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
                }
            )
        }
    }

    #[tokio::test]
    async fn test_date_part_quarter_granularity() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT CAST(TRUNC(EXTRACT(QUARTER FROM KibanaSampleDataEcommerce.order_date)) AS INTEGER)
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("quarter".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_where_filter_daterange() {
        init_logger();

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
            // Column precedence vs projection alias
            (
                "COUNT(*), DATE(order_date) AS order_date".to_string(),
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
            // Stacked chart
            (
                "COUNT(*), customer_gender, DATE(order_date) AS __timestamp".to_string(),
                "customer_gender = 'FEMALE' AND (order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f'))".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00.000Z".to_string(),
                        "2021-09-06T23:59:59.999Z".to_string()
                    ])),
                }])
            ),
        ];

        for (sql_projection, sql_filter, expected_tdm) in to_check.iter() {
            let query = format!(
                "SELECT
                {}
                FROM KibanaSampleDataEcommerce
                WHERE {}
                {}",
                sql_projection,
                sql_filter,
                if sql_projection.contains("__timestamp")
                    && sql_projection.contains("customer_gender")
                {
                    "GROUP BY customer_gender, __timestamp"
                } else if sql_projection.contains("__timestamp") {
                    "GROUP BY __timestamp"
                } else if sql_projection.contains("order_date") {
                    "GROUP BY DATE(order_date)"
                } else {
                    ""
                }
            );
            let logical_plan = convert_select_to_query_plan(query, DatabaseProtocol::MySQL)
                .await
                .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.time_dimensions,
                *expected_tdm
            )
        }
    }

    #[tokio::test]
    async fn test_where_filter_or() {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                COUNT(*), DATE(order_date) AS __timestamp
                FROM KibanaSampleDataEcommerce
                WHERE order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') OR order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')
                GROUP BY __timestamp"
            .to_string(), DatabaseProtocol::MySQL
        ).await;

        assert_eq!(
            query_plan
                .as_logical_plan()
                .find_cube_scan()
                .request
                .filters,
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

    #[tokio::test]
    async fn test_where_filter_simple() {
        let to_check = vec![
            // Binary expression with Measures
            (
                "maxPrice = 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            (
                "maxPrice > 5".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["5".to_string()]),
                    or: None,
                    and: None,
                }]),
                None,
            ),
            // Binary expression with Dimensions
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
            // (
            //     "order_date = '2021-08-31'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("equals".to_string()),
            //         values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // (
            //     "order_date <> '2021-08-31'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("notEquals".to_string()),
            //         values: Some(vec!["2021-08-31T00:00:00.000Z".to_string()]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // BETWEEN
            // (
            //     "order_date BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
            //     // This filter will be pushed to time_dimension
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ])),
            //     }]),
            // ),
            // (
            //     "order_date NOT BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
            //     Some(vec![V1LoadRequestQueryFilterItem {
            //         member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            //         operator: Some("notInDateRange".to_string()),
            //         values: Some(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ]),
            //         or: None,
            //         and: None,
            //     }]),
            //     None,
            // ),
            // SIMILAR as BETWEEN but manually
            // (
            //     "order_date >= '2021-08-31' AND order_date < '2021-09-07'".to_string(),
            //     // This filter will be pushed to time_dimension
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             // -1 milleseconds hack for cube.js
            //             "2021-09-06T23:59:59.999Z".to_string(),
            //         ])),
            //     }]),
            // ),
            // //  SIMILAR as BETWEEN but without -1 nanosecond because <=
            // (
            //     "order_date >= '2021-08-31' AND order_date <= '2021-09-07'".to_string(),
            //     None,
            //     Some(vec![V1LoadRequestQueryTimeDimension {
            //         dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
            //         granularity: None,
            //         date_range: Some(json!(vec![
            //             "2021-08-31T00:00:00.000Z".to_string(),
            //             // without -1 because <=
            //             "2021-09-07T00:00:00.000Z".to_string(),
            //         ])),
            //     }]),
            // ),
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
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT
                COUNT(*)
                FROM KibanaSampleDataEcommerce
                WHERE {}",
                    sql
                ),
                DatabaseProtocol::MySQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.filters,
                *expected_fitler,
                "Filters for {}",
                sql
            );
            assert_eq!(
                logical_plan.find_cube_scan().request.time_dimensions,
                *expected_time_dimensions,
                "Time dimensions for {}",
                sql
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_filter_error() {
        let to_check = vec![
            // Binary expr
            (
                "order_date >= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date < 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <= 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date = 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date <> 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            // Between
            (
                "order_date BETWEEN 'WRONG_DATE' AND '2021-01-01'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
            ),
            (
                "order_date BETWEEN '2021-01-01' AND 'WRONG_DATE'".to_string(),
                CompilationError::user("Unable to compare time dimension \"order_date\" with not a date value: WRONG_DATE".to_string()),
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
                get_test_session(DatabaseProtocol::MySQL),
            )
            .await;

            match &query {
                Ok(_) => panic!("Query ({}) should return error", sql),
                Err(e) => assert_eq!(e, expected_error, "{}", sql),
            }
        }
    }

    #[tokio::test]
    async fn test_where_filter_complex() {
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
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT
                COUNT(*), DATE(order_date) AS __timestamp
                FROM KibanaSampleDataEcommerce
                WHERE {}
                GROUP BY __timestamp",
                    sql
                ),
                DatabaseProtocol::MySQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request.filters,
                Some(expected_fitler.clone())
            )
        }
    }

    fn parse_expr_from_projection(query: &String, db: DatabaseProtocol) -> ast::Expr {
        let stmt = parse_sql_to_statement(&query, db).unwrap();
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

    #[tokio::test]
    async fn test_str_to_date() {
        let compiled = compile_expression(
            &parse_expr_from_projection(
                &"SELECT STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')"
                    .to_string(),
                DatabaseProtocol::MySQL,
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

    #[tokio::test]
    async fn test_now_expr() {
        let compiled = compile_expression(
            &parse_expr_from_projection(&"SELECT NOW()".to_string(), DatabaseProtocol::MySQL),
            &QueryContext::new(&get_test_meta()[0]),
        )
        .unwrap();

        match compiled {
            CompiledExpression::DateLiteral(_) => {}
            _ => panic!("Must be DateLiteral"),
        };
    }

    #[tokio::test]
    async fn test_date_date_add_interval_expr() {
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
                &parse_expr_from_projection(&format!("SELECT {}", sql), DatabaseProtocol::MySQL),
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

    #[tokio::test]
    async fn test_date_add_sub_postgres() {
        async fn check_fun(name: &str, t: &str, i: &str, expected: &str) {
            assert_eq!(
                execute_query(
                    format!(
                        "SELECT {}(Str_to_date('{}', '%Y-%m-%d %H:%i:%s'), INTERVAL '{}') as result",
                        name, t, i
                    ),
                    DatabaseProtocol::PostgreSQL
                )
                .await
                .unwrap(),
                format!(
                    "+-------------------------+\n\
                | result                  |\n\
                +-------------------------+\n\
                | {} |\n\
                +-------------------------+",
                    expected
                )
            );
        }

        async fn check_adds_to(t: &str, i: &str, expected: &str) {
            check_fun("DATE_ADD", t, i, expected).await
        }

        async fn check_subs_to(t: &str, i: &str, expected: &str) {
            check_fun("DATE_SUB", t, i, expected).await
        }

        check_adds_to("2021-01-01 00:00:00", "1 second", "2021-01-01T00:00:01.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 minute", "2021-01-01T00:01:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 hour", "2021-01-01T01:00:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "1 day", "2021-01-02T00:00:00.000").await;
        check_adds_to(
            "2021-01-01 00:00:00",
            "-1 second",
            "2020-12-31T23:59:59.000",
        )
        .await;
        check_adds_to(
            "2021-01-01 00:00:00",
            "-1 minute",
            "2020-12-31T23:59:00.000",
        )
        .await;
        check_adds_to("2021-01-01 00:00:00", "-1 hour", "2020-12-31T23:00:00.000").await;
        check_adds_to("2021-01-01 00:00:00", "-1 day", "2020-12-31T00:00:00.000").await;

        check_adds_to(
            "2021-01-01 00:00:00",
            "1 day 1 hour 1 minute 1 second",
            "2021-01-02T01:01:01.000",
        )
        .await;
        check_subs_to(
            "2021-01-02 01:01:01",
            "1 day 1 hour 1 minute 1 second",
            "2021-01-01T00:00:00.000",
        )
        .await;

        check_adds_to("2021-01-01 00:00:00", "1 month", "2021-02-01T00:00:00.000").await;

        check_adds_to("2021-01-01 00:00:00", "1 year", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 year", "2021-01-01T00:00:00.000").await;

        check_adds_to("2021-01-01 00:00:00", "13 month", "2022-02-01T00:00:00.000").await;
        check_subs_to("2022-02-01 00:00:00", "13 month", "2021-01-01T00:00:00.000").await;

        check_adds_to("2021-01-01 23:59:00", "1 minute", "2021-01-02T00:00:00.000").await;
        check_subs_to("2021-01-02 00:00:00", "1 minute", "2021-01-01T23:59:00.000").await;

        check_adds_to("2021-12-01 00:00:00", "1 month", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 month", "2021-12-01T00:00:00.000").await;

        check_adds_to("2021-12-31 00:00:00", "1 day", "2022-01-01T00:00:00.000").await;
        check_subs_to("2022-01-01 00:00:00", "1 day", "2021-12-31T00:00:00.000").await;

        // Feb 29 on leap and non-leap years.
        check_adds_to("2020-02-29 00:00:00", "1 day", "2020-03-01T00:00:00.000").await;
        check_subs_to("2020-03-01 00:00:00", "1 day", "2020-02-29T00:00:00.000").await;

        check_adds_to("2020-02-28 00:00:00", "1 day", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-02-29 00:00:00", "1 day", "2020-02-28T00:00:00.000").await;

        check_adds_to("2021-02-28 00:00:00", "1 day", "2021-03-01T00:00:00.000").await;
        check_subs_to("2021-03-01 00:00:00", "1 day", "2021-02-28T00:00:00.000").await;

        check_adds_to("2020-02-29 00:00:00", "1 year", "2021-02-28T00:00:00.000").await;
        check_subs_to("2020-02-29 00:00:00", "1 year", "2019-02-28T00:00:00.000").await;

        check_adds_to("2020-01-30 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-03-30 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;

        check_adds_to("2020-01-29 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;
        check_subs_to("2020-03-29 00:00:00", "1 month", "2020-02-29T00:00:00.000").await;

        check_adds_to("2021-01-29 00:00:00", "1 month", "2021-02-28T00:00:00.000").await;
        check_subs_to("2021-03-29 00:00:00", "1 month", "2021-02-28T00:00:00.000").await;
    }

    #[tokio::test]
    async fn test_str_literal_to_date() {
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

    async fn execute_query(query: String, db: DatabaseProtocol) -> Result<String, CubeError> {
        Ok(execute_query_with_flags(query, db).await?.0)
    }

    async fn execute_query_with_flags(
        query: String,
        db: DatabaseProtocol,
    ) -> Result<(String, StatusFlags), CubeError> {
        execute_queries_with_flags(vec![query], db).await
    }

    async fn execute_queries_with_flags(
        queries: Vec<String>,
        db: DatabaseProtocol,
    ) -> Result<(String, StatusFlags), CubeError> {
        let meta = get_test_tenant_ctx();
        let session = get_test_session(db);

        let mut output: Vec<String> = Vec::new();
        let mut output_flags = StatusFlags::empty();

        for query in queries {
            let query = convert_sql_to_cube_query(&query, meta.clone(), session.clone()).await;
            match query.unwrap() {
                QueryPlan::DataFusionSelect(flags, plan, ctx) => {
                    let df = DFDataFrame::new(ctx.state, &plan);
                    let batches = df.collect().await?;
                    let frame = batch_to_dataframe(&df.schema().into(), &batches)?;

                    output.push(frame.print());
                    output_flags = flags;
                }
                QueryPlan::MetaTabular(flags, frame) => {
                    output.push(frame.print());
                    output_flags = flags;
                }
                QueryPlan::MetaOk(flags, _) => {
                    output_flags = flags;
                }
            }
        }

        Ok((output.join("\n").to_string(), output_flags))
    }

    #[tokio::test]
    async fn test_show_create_table() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "show_create_table",
            execute_query(
                "show create table KibanaSampleDataEcommerce;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "show_create_table",
            execute_query(
                "show create table `db`.`KibanaSampleDataEcommerce`;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_tables_mysql() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_tables_mysql",
            execute_query(
                "SELECT * FROM information_schema.tables".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_role_table_grants_pg() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_role_table_grants_postgresql",
            execute_query(
                "SELECT * FROM information_schema.role_table_grants".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_observable() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "observable_grants",
            execute_query(
                "SELECT DISTINCT privilege_type
                FROM information_schema.role_table_grants
                WHERE grantee = user
                UNION
                SELECT DISTINCT privilege_type
                FROM information_schema.role_column_grants
                WHERE grantee = user
              "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_role_column_grants_pg() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_role_column_grants_postgresql",
            execute_query(
                "SELECT * FROM information_schema.role_column_grants".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_columns_mysql() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_columns_mysql",
            execute_query(
                "SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = 'db'".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_schemata() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_schemata",
            execute_query(
                "SELECT * FROM information_schema.schemata".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_stats_for_columns() -> Result<(), CubeError> {
        // This query is used by metabase for introspection
        insta::assert_snapshot!(
            "test_information_schema_stats_for_columns",
            execute_query("
            SELECT
                A.TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, A.TABLE_NAME, A.COLUMN_NAME, B.SEQ_IN_INDEX KEY_SEQ, B.INDEX_NAME PK_NAME
            FROM INFORMATION_SCHEMA.COLUMNS A, INFORMATION_SCHEMA.STATISTICS B
            WHERE A.COLUMN_KEY in ('PRI','pri') AND B.INDEX_NAME='PRIMARY'  AND (ISNULL(database()) OR (A.TABLE_SCHEMA = database())) AND (ISNULL(database()) OR (B.TABLE_SCHEMA = database())) AND A.TABLE_NAME = 'OutlierFingerprints'  AND B.TABLE_NAME = 'OutlierFingerprints'  AND A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME
            ORDER BY A.COLUMN_NAME".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_redshift_svv_tables() -> Result<(), CubeError> {
        // This query is used by metabase for introspection
        insta::assert_snapshot!(
            "redshift_svv_tables",
            execute_query(
                "SELECT * FROM svv_tables ORDER BY table_name DESC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_thought_spot_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "thought_spot_tables",
            execute_query(
                "SELECT * FROM (SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT, table_schema AS TABLE_SCHEM, table_name AS TABLE_NAME, CAST( CASE table_type WHEN 'BASE TABLE' THEN CASE WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM TABLE' WHEN table_schema = 'pg_toast' THEN 'SYSTEM TOAST TABLE' WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY TABLE' ELSE 'TABLE' END WHEN 'VIEW' THEN CASE WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM VIEW' WHEN table_schema = 'pg_toast' THEN NULL WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY VIEW' ELSE 'VIEW' END WHEN 'EXTERNAL TABLE' THEN 'EXTERNAL TABLE' END AS VARCHAR(124)) AS TABLE_TYPE, REMARKS, '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME,  '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM svv_tables) WHERE true  AND current_database() = 'dev' AND TABLE_TYPE IN ( 'TABLE', 'VIEW', 'EXTERNAL TABLE')  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_performance_schema_variables() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "performance_schema_session_variables",
            execute_query("SELECT * FROM performance_schema.session_variables WHERE VARIABLE_NAME = 'max_allowed_packet'".to_string(), DatabaseProtocol::MySQL).await?
        );

        insta::assert_snapshot!(
            "performance_schema_global_variables",
            execute_query("SELECT * FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'max_allowed_packet'".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_processlist() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "show_processlist",
            execute_query("SHOW processlist".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_warnings() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "show_warnings",
            execute_query("SHOW warnings".to_string(), DatabaseProtocol::MySQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_collations() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_collations",
            execute_query(
                "SELECT * FROM information_schema.collations".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_processlist() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_processlist",
            execute_query(
                "SELECT * FROM information_schema.processlist".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
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
                .to_string(),
                DatabaseProtocol::MySQL
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
                .to_string(),
                DatabaseProtocol::MySQL
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
                .to_string(),
                DatabaseProtocol::MySQL
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
                "select convert_tz('2021-12-08T15:50:14.337Z'::timestamp, @@GLOBAL.time_zone, '+00:00') as r1;".to_string(), DatabaseProtocol::MySQL
            )
            .await?,
            "+-------------------------+\n\
            | r1                      |\n\
            +-------------------------+\n\
            | 2021-12-08T15:50:14.337 |\n\
            +-------------------------+"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_timediff() -> Result<(), CubeError> {
        assert_eq!(
            execute_query(
                "select \
                    timediff('1994-11-26T13:25:00.000Z'::timestamp, '1994-11-26T13:25:00.000Z'::timestamp) as r1
                ".to_string(), DatabaseProtocol::MySQL
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
                .to_string(),
                DatabaseProtocol::MySQL
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
                .to_string(),
                DatabaseProtocol::MySQL
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
    async fn test_gdata_studio() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "test_gdata_studio",
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
                .to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_variable() -> Result<(), CubeError> {
        // LIKE
        insta::assert_snapshot!(
            "show_variables_like_sql_mode",
            execute_query(
                "show variables like 'sql_mode';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // LIKE pattern
        insta::assert_snapshot!(
            "show_variables_like",
            execute_query(
                "show variables like '%_mode';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Negative test, we dont define this variable
        insta::assert_snapshot!(
            "show_variables_like_aurora",
            execute_query(
                "show variables like 'aurora_version';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // All variables
        insta::assert_snapshot!(
            "show_variables",
            execute_query("show variables;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // Postgres escaped with quotes
        insta::assert_snapshot!(
            "show_variable_quoted",
            execute_query(
                "show \"max_allowed_packet\";".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_columns() -> Result<(), CubeError> {
        // Simplest syntax
        insta::assert_snapshot!(
            "show_columns",
            execute_query(
                "show columns from KibanaSampleDataEcommerce;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // FULL
        insta::assert_snapshot!(
            "show_columns_full",
            execute_query(
                "show full columns from KibanaSampleDataEcommerce;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // LIKE
        insta::assert_snapshot!(
            "show_columns_like",
            execute_query(
                "show columns from KibanaSampleDataEcommerce like '%ice%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // WHERE
        insta::assert_snapshot!(
            "show_columns_where",
            execute_query(
                "show columns from KibanaSampleDataEcommerce where Type = 'int';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // FROM db FROM tbl
        insta::assert_snapshot!(
            "show_columns_from_db",
            execute_query(
                "show columns from KibanaSampleDataEcommerce from db like 'count';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Everything
        insta::assert_snapshot!(
            "show_columns_everything",
            execute_query(
                "show full columns from KibanaSampleDataEcommerce from db like '%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_tables() -> Result<(), CubeError> {
        // Simplest syntax
        insta::assert_snapshot!(
            "show_tables_simple",
            execute_query("show tables;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // FULL
        insta::assert_snapshot!(
            "show_tables_full",
            execute_query("show full tables;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // LIKE
        insta::assert_snapshot!(
            "show_tables_like",
            execute_query(
                "show tables like '%ban%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // WHERE
        insta::assert_snapshot!(
            "show_tables_where",
            execute_query(
                "show tables where Tables_in_db = 'Logs';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // FROM db
        insta::assert_snapshot!(
            "show_tables_from_db",
            execute_query("show tables from db;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // Everything
        insta::assert_snapshot!(
            "show_tables_everything",
            execute_query(
                "show full tables from db like '%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_tableau() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_table_name_column_name_query",
            execute_query(
                "SELECT `table_name`, `column_name`
                FROM `information_schema`.`columns`
                WHERE `data_type`='enum' AND `table_schema`='db'"
                    .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_null_text_query",
            execute_query(
                "
                SELECT
                    NULL::text AS PKTABLE_CAT,
                    pkn.nspname AS PKTABLE_SCHEM,
                    pkc.relname AS PKTABLE_NAME,
                    pka.attname AS PKCOLUMN_NAME,
                    NULL::text AS FKTABLE_CAT,
                    fkn.nspname AS FKTABLE_SCHEM,
                    fkc.relname AS FKTABLE_NAME,
                    fka.attname AS FKCOLUMN_NAME,
                    pos.n AS KEY_SEQ,
                    CASE con.confupdtype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS UPDATE_RULE,
                    CASE con.confdeltype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS DELETE_RULE,
                    con.conname AS FK_NAME,
                    pkic.relname AS PK_NAME,
                    CASE
                        WHEN con.condeferrable AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                    END AS DEFERRABILITY
                FROM
                    pg_catalog.pg_namespace pkn,
                    pg_catalog.pg_class pkc,
                    pg_catalog.pg_attribute pka,
                    pg_catalog.pg_namespace fkn,
                    pg_catalog.pg_class fkc,
                    pg_catalog.pg_attribute fka,
                    pg_catalog.pg_constraint con,
                    pg_catalog.generate_series(1, 32) pos(n),
                    pg_catalog.pg_class pkic
                WHERE
                    pkn.oid = pkc.relnamespace AND
                    pkc.oid = pka.attrelid AND
                    pka.attnum = con.confkey[pos.n] AND
                    con.confrelid = pkc.oid AND
                    fkn.oid = fkc.relnamespace AND
                    fkc.oid = fka.attrelid AND
                    fka.attnum = con.conkey[pos.n] AND
                    con.conrelid = fkc.oid AND
                    con.contype = 'f' AND
                    (pkic.relkind = 'i' OR pkic.relkind = 'I') AND
                    pkic.oid = con.conindid AND
                    fkn.nspname = 'public' AND
                    fkc.relname = 'payment'
                ORDER BY
                    pkn.nspname,
                    pkc.relname,
                    con.conname,
                    pos.n
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_table_cat_query",
            execute_query(
                "
                SELECT
                    result.TABLE_CAT,
                    result.TABLE_SCHEM,
                    result.TABLE_NAME,
                    result.COLUMN_NAME,
                    result.KEY_SEQ,
                    result.PK_NAME
                FROM
                    (
                        SELECT
                            NULL AS TABLE_CAT,
                            n.nspname AS TABLE_SCHEM,
                            ct.relname AS TABLE_NAME,
                            a.attname AS COLUMN_NAME,
                            (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
                            ci.relname AS PK_NAME,
                            information_schema._pg_expandarray(i.indkey) AS KEYS,
                            a.attnum AS A_ATTNUM
                        FROM pg_catalog.pg_class ct
                        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
                        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                        JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
                        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                        WHERE
                            true AND
                            n.nspname = 'public' AND
                            ct.relname = 'payment' AND
                            i.indisprimary
                    ) result
                    where result.A_ATTNUM = (result.KEYS).x
                ORDER BY
                    result.table_name,
                    result.pk_name,
                    result.key_seq;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_excel() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "excel_select_db_query",
            execute_query(
                "
                SELECT
                    'db' as Database,
                    ns.nspname as Schema,
                    relname as Name,
                    CASE
                        WHEN ns.nspname Like E'pg\\_catalog' then 'Catalog'
                        WHEN ns.nspname Like E'information\\_schema' then 'Information'
                        WHEN relkind = 'f' then 'Foreign'
                        ELSE 'User'
                    END as TableType,
                    pg_get_userbyid(relowner) AS definer,
                    rel.oid as Oid,
                    relacl as ACL,
                    true as HasOids,
                    relhassubclass as HasSubtables,
                    reltuples as RowNumber,
                    description as Comment,
                    relnatts as ColumnNumber,
                    relhastriggers as TriggersNumber,
                    conname as Constraint,
                    conkey as ColumnConstrainsIndexes
                FROM pg_class rel
                INNER JOIN pg_namespace ns ON relnamespace = ns.oid
                LEFT OUTER JOIN pg_description des ON
                    des.objoid = rel.oid AND
                    des.objsubid = 0
                LEFT OUTER JOIN pg_constraint c ON
                    c.conrelid = rel.oid AND
                    c.contype = 'p'
                WHERE
                    (
                        (relkind = 'r') OR
                        (relkind = 's') OR
                        (relkind = 'f')
                    ) AND
                    NOT ns.nspname LIKE E'pg\\_temp\\_%%' AND
                    NOT ns.nspname like E'pg\\_%' AND
                    NOT ns.nspname like E'information\\_schema' AND
                    ns.nspname::varchar like E'public' AND
                    relname::varchar like '%' AND
                    pg_get_userbyid(relowner)::varchar like '%'
                ORDER BY relname
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_typname_big_query",
            execute_query(
                "
                SELECT
                    typname as name,
                    n.nspname as Schema,
                    pg_get_userbyid(typowner) as Definer,
                    typlen as Length,
                    t.oid as oid,
                    typbyval as IsReferenceType,
                    case
                        when typtype = 'b' then 'base'
                        when typtype = 'd' then 'domain'
                        when typtype = 'c' then 'composite'
                        when typtype = 'd' then 'pseudo'
                    end as Type,
                    case
                        when typalign = 'c' then 'char'
                        when typalign = 's' then 'short'
                        when typalign = 'i' then 'int'
                        else 'double'
                    end as alignment,
                    case
                        when typstorage = 'p' then 'plain'
                        when typstorage = 'e' then 'secondary'
                        when typstorage = 'm' then 'compressed inline'
                        else 'secondary or compressed inline'
                    end as ValueStorage,
                    typdefault as DefaultValue,
                    description as comment
                FROM pg_type t
                LEFT OUTER JOIN
                    pg_description des ON des.objoid = t.oid,
                    pg_namespace n
                WHERE
                    t.typnamespace = n.oid and
                    t.oid::varchar like E'1033' and
                    typname like E'%' and
                    n.nspname like E'%' and
                    pg_get_userbyid(typowner)::varchar like E'%' and
                    typtype::varchar like E'c'
                ORDER BY name
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_typname_aclitem_query",
            execute_query(
                "
                SELECT
                    typname as name,
                    t.oid as oid,
                    typtype as Type,
                    typelem as TypeElement
                FROM pg_type t
                WHERE
                    t.oid::varchar like '1034' and
                    typtype::varchar like 'b' and
                    typelem != 0
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_pg_constraint_query",
            execute_query(
                "
                SELECT
                    a.conname as Name,
                    ns.nspname as Schema,
                    mycl.relname as Table,
                    b.conname as ReferencedKey,
                    frns.nspname as ReferencedSchema,
                    frcl.relname as ReferencedTable,
                    a.oid as Oid,
                    a.conkey as ColumnIndexes,
                    a.confkey as ForeignColumnIndexes,
                    a.confupdtype as UpdateActionCode,
                    a.confdeltype as DeleteActionCode,
                    a.confmatchtype as ForeignKeyMatchType,
                    a.condeferrable as IsDeferrable,
                    a.condeferred as Iscondeferred
                FROM pg_constraint a
                inner join pg_constraint b on (
                    a.confrelid = b.conrelid AND
                    a.confkey = b.conkey
                )
                INNER JOIN pg_namespace ns ON a.connamespace = ns.oid
                INNER JOIN pg_class mycl ON a.conrelid = mycl.oid
                LEFT OUTER JOIN pg_class frcl ON a.confrelid = frcl.oid
                INNER JOIN pg_namespace frns ON frcl.relnamespace = frns.oid
                WHERE
                    a.contype = 'f' AND
                    (
                        b.contype = 'p' OR
                        b.contype = 'u'
                    ) AND
                    a.oid::varchar like '%' AND
                    a.conname like '%' AND
                    ns.nspname like E'public' AND
                    mycl.relname like E'KibanaSampleDataEcommerce' AND
                    frns.nspname like '%' AND
                    frcl.relname like '%'
                ORDER BY 1
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_pg_attribute_query",
            execute_query(
                "
                SELECT DISTINCT
                    attname AS Name,
                    attnum
                FROM pg_attribute
                JOIN pg_class ON oid = attrelid
                INNER JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    pg_namespace.nspname like 'public' AND
                    relname like 'KibanaSampleDataEcommerce' AND
                    attnum in (2)
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_fkey_query",
            execute_query(
                "
                SELECT
                    nspname as Schema,
                    cl.relname as Table,
                    clr.relname as RefTableName,
                    conname as Name,
                    conkey as ColumnIndexes,
                    confkey as ColumnRefIndexes
                FROM pg_constraint
                INNER JOIN pg_namespace ON connamespace = pg_namespace.oid
                INNER JOIN pg_class cl ON conrelid = cl.oid
                INNER JOIN pg_class clr ON confrelid = clr.oid
                WHERE
                    contype = 'f' AND
                    conname like E'sample\\_fkey' AND
                    nspname like E'public' AND
                    cl.relname like E'KibanaSampleDataEcommerce'
                order by 1
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_large_select_query",
            execute_query(
                "
                SELECT
                    na.nspname as Schema,
                    cl.relname as Table,
                    att.attname AS Name,
                    att.attnum as Position,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'true'
                        ELSE 'false'
                    END as Nullable,
                    CASE
                        WHEN
                            ty.typname Like 'bit' OR
                            ty.typname Like 'varbit' and
                            att.atttypmod > 0
                        THEN att.atttypmod
                        WHEN ty.typname Like 'interval' THEN -1
                        WHEN att.atttypmod > 0 THEN att.atttypmod - 4
                        ELSE att.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS DatetimeLength,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'false'
                        ELSE 'true'
                    END as IsUnique,
                    att.atthasdef as HasDefaultValue,
                    att.attisdropped as IsDropped,
                    att.attinhcount as ancestorCount,
                    att.attndims as Dimension,
                    CASE
                        WHEN attndims > 0 THEN true
                        ELSE false
                    END AS isarray,
                    CASE
                        WHEN ty.typname = 'bpchar' THEN 'char'
                        WHEN ty.typname = '_bpchar' THEN '_char'
                        ELSE ty.typname
                    END as TypeName,
                    tn.nspname as TypeSchema,
                    et.typname as elementaltypename,
                    description as Comment,
                    cs.relname AS sername,
                    ns.nspname AS serschema,
                    att.attidentity as IdentityMode,
                    CAST(pg_get_expr(def.adbin, def.adrelid) AS varchar) as DefaultValue,
                    (SELECT count(1) FROM pg_type t2 WHERE t2.typname=ty.typname) > 1 AS isdup
                FROM pg_attribute att
                JOIN pg_type ty ON ty.oid=atttypid
                JOIN pg_namespace tn ON tn.oid=ty.typnamespace
                JOIN pg_class cl ON
                    cl.oid=attrelid AND
                    (
                        (cl.relkind = 'r') OR
                        (cl.relkind = 's') OR
                        (cl.relkind = 'v') OR
                        (cl.relkind = 'm') OR
                        (cl.relkind = 'f')
                    )
                JOIN pg_namespace na ON na.oid=cl.relnamespace
                LEFT OUTER JOIN pg_type et ON et.oid=ty.typelem
                LEFT OUTER JOIN pg_attrdef def ON
                    adrelid=attrelid AND
                    adnum=attnum
                LEFT OUTER JOIN pg_description des ON
                    des.objoid=attrelid AND
                    des.objsubid=attnum
                LEFT OUTER JOIN (
                    pg_depend
                    JOIN pg_class cs ON
                        objid=cs.oid AND
                        cs.relkind='S' AND
                        classid='pg_class'::regclass::oid
                ) ON
                    refobjid=attrelid AND
                    refobjsubid=attnum
                LEFT OUTER JOIN pg_namespace ns ON ns.oid=cs.relnamespace
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    cl.relname like E'KibanaSampleDataEcommerce' AND
                    na.nspname like E'public' AND
                    att.attname like '%'
                ORDER BY attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "excel_exists_query",
            execute_query(
                "
                SELECT
                    a.attname as fieldname,
                    a.attnum  as fieldordinal,
                    a.atttypid as datatype,
                    a.atttypmod as fieldmod,
                    a.attnotnull as isnull,
                    c.relname as tablename,
                    n.nspname as schema,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c1
                            where
                                c1.conrelid = c.oid and
                                c1.contype = 'p' and
                                a.attnum = ANY (c1.conkey)
                        ) THEN true
                        ELSE false
                    END as iskey,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c1
                            where
                                c1.conrelid = c.oid and
                                c1.contype = 'u' and
                                a.attnum = ANY (c1.conkey)
                        ) THEN true
                        ELSE false
                    END as isunique,
                    CAST(pg_get_expr(d.adbin, d.adrelid) AS varchar) as defvalue,
                    CASE
                        WHEN t.typtype = 'd' THEN t.typbasetype
                        ELSE a.atttypid
                    END as basetype,
                    CASE
                        WHEN a.attidentity = 'a' THEN true
                        ELSE false
                    END as IsAutoIncrement,
                    CASE
                        WHEN
                            t.typname Like 'bit' OR
                            t.typname Like 'varbit' and
                            a.atttypmod > 0
                        THEN a.atttypmod
                        WHEN
                            t.typname Like 'interval' OR
                            t.typname Like 'timestamp' OR
                            t.typname Like 'timestamptz' OR
                            t.typname Like 'time' OR
                            t.typname Like 'timetz'
                        THEN -1
                        WHEN a.atttypmod > 0 THEN a.atttypmod - 4
                        ELSE a.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS DatetimePrecision
                FROM pg_namespace n
                INNER JOIN pg_class c ON c.relnamespace = n.oid
                INNER JOIN pg_attribute a on c.oid = a.attrelid
                LEFT JOIN pg_attrdef d on
                    d.adrelid = a.attrelid and
                    d.adnum =a.attnum
                LEFT JOIN pg_type t on t.oid = a.atttypid
                WHERE
                    a.attisdropped = false AND
                    (
                        (c.relkind = 'r') OR
                        (c.relkind = 's') OR
                        (c.relkind = 'v') OR
                        (c.relkind = 'm') OR
                        (c.relkind = 'f')
                    ) AND
                    a.attnum > 0 AND
                    ((
                        c.relname LIKE 'KibanaSampleDataEcommerce' AND
                        n.nspname LIKE 'public'
                    ))
                ORDER BY
                    tablename,
                    fieldordinal
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_explain_table() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            execute_query(
                "explain KibanaSampleDataEcommerce;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_use_db() -> Result<(), CubeError> {
        assert_eq!(
            execute_query("use db;".to_string(), DatabaseProtocol::MySQL).await?,
            "".to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_variable() -> Result<(), CubeError> {
        assert_eq!(
            execute_query_with_flags("set autocommit=1;".to_string(), DatabaseProtocol::MySQL)
                .await?,
            (
                "++\n++\n++".to_string(),
                StatusFlags::SERVER_STATE_CHANGED | StatusFlags::AUTOCOMMIT
            )
        );

        assert_eq!(
            execute_query_with_flags(
                "set character_set_results = utf8;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            ("++\n++\n++".to_string(), StatusFlags::SERVER_STATE_CHANGED)
        );

        assert_eq!(
            execute_query_with_flags(
                "set autocommit=1, sql_mode = concat(@@sql_mode,',strict_trans_tables');"
                    .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?,
            (
                "++\n++\n++".to_string(),
                StatusFlags::SERVER_STATE_CHANGED | StatusFlags::AUTOCOMMIT
            )
        );

        insta::assert_snapshot!(
            "pg_set_app_show",
            execute_queries_with_flags(
                vec![
                    "set application_name = 'testing app'".to_string(),
                    "show application_name".to_string()
                ],
                DatabaseProtocol::PostgreSQL
            )
            .await?
            .0
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_backend_pid() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_backend_pid",
            execute_query(
                "select pg_backend_pid();".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_show_collation() -> Result<(), CubeError> {
        // Simplest syntax
        insta::assert_snapshot!(
            "show_collation",
            execute_query("show collation;".to_string(), DatabaseProtocol::MySQL).await?
        );

        // LIKE
        insta::assert_snapshot!(
            "show_collation_like",
            execute_query(
                "show collation like '%unicode%';".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // WHERE
        insta::assert_snapshot!(
            "show_collation_where",
            execute_query(
                "show collation where Id between 255 and 260;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Superset query
        insta::assert_snapshot!(
            "show_collation_superset",
            execute_query(
                "show collation where charset = 'utf8mb4' and collation = 'utf8mb4_bin';"
                    .to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_explain() -> Result<(), CubeError> {
        // SELECT with no tables (inline eval)
        insta::assert_snapshot!(
            execute_query("EXPLAIN SELECT 1+1;".to_string(), DatabaseProtocol::MySQL).await?
        );

        insta::assert_snapshot!(
            execute_query(
                "EXPLAIN VERBOSE SELECT 1+1;".to_string(),
                DatabaseProtocol::MySQL
            )
            .await?
        );

        // Execute without asserting with fixture, because metrics can change
        execute_query(
            "EXPLAIN ANALYZE SELECT 1+1;".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await?;

        // SELECT with table and specific columns
        execute_query(
            "EXPLAIN SELECT count, avgPrice FROM KibanaSampleDataEcommerce;".to_string(),
            DatabaseProtocol::MySQL,
        )
        .await?;

        // EXPLAIN for Postgres
        execute_query(
            "EXPLAIN SELECT 1+1;".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            execute_query(
                "SELECT \
                    @@GLOBAL.time_zone AS global_tz, \
                    @@system_time_zone AS system_tz, time_format(   timediff(      now(), convert_tz(now(), @@GLOBAL.time_zone, '+00:00')   ),   '%H:%i' ) AS 'offset'
                ".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        insta::assert_snapshot!(
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
                ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        insta::assert_snapshot!(
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
                ".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_tables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_tables_postgres",
            execute_query(
                "SELECT * FROM information_schema.tables".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_columns_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_columns_postgres",
            execute_query(
                "SELECT * FROM information_schema.columns".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_character_sets_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_character_sets_postgres",
            execute_query(
                "SELECT * FROM information_schema.character_sets".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_key_column_usage_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_key_column_usage_postgres",
            execute_query(
                "SELECT * FROM information_schema.key_column_usage".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_referential_constraints_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_referential_constraints_postgres",
            execute_query(
                "SELECT * FROM information_schema.referential_constraints".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_information_schema_table_constraints_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "information_schema_table_constraints_postgres",
            execute_query(
                "SELECT * FROM information_schema.table_constraints".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgtables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgtables_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_tables".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgtype_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgtype_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_type ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgroles_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgroles_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_roles ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgnamespace_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgnamespace_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_namespace".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_am_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgam_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_am".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_regclass() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "dynamic_regclass_postgres_utf8",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT 'pg_class' as a
                    UNION ALL
                    SELECT NULL
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dynamic_regclass_postgres_int32",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT CAST(83 as int) as a
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dynamic_regclass_postgres_int64",
            execute_query(
                "SELECT cast(r.a as regclass) FROM (
                    SELECT 83 as a
                ) as r"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_sequence_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgsequence_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_sequence".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgrange_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgrange_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_range".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgattrdef_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgattrdef_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_attrdef".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgattribute_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgattribute_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_attribute".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgindex_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgindex_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_index".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgclass_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgclass_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_class".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgproc_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgproc_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_proc".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdescription_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdescription_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_description".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgconstraint_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgconstraint_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_constraint".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdepend_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdepend_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_depend ORDER BY refclassid ASC, refobjid ASC"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgenum_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgenum_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_enum".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgmatviews_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgmatviews_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_matviews".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgdatabase_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgdatabase_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_database ORDER BY oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pgcatalog_pgstatiousertables_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pgcatalog_pgstatiousertables_postgres",
            execute_query(
                "SELECT * FROM pg_catalog.pg_statio_user_tables ORDER BY relid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_constraint_column_usage_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "constraint_column_usage_postgres",
            execute_query(
                "SELECT * FROM information_schema.constraint_column_usage".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_views_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "views_postgres",
            execute_query(
                "SELECT * FROM information_schema.views".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_current_schema_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "current_schema_postgres",
            execute_query(
                "SELECT current_schema()".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_rust_client() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "rust_client_types",
            execute_query(
                r#"SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
                FROM pg_catalog.pg_type t
                LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
                INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                WHERE t.oid = 25"#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_current_schemas_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "current_schemas_postgres",
            execute_query(
                "SELECT current_schemas(false)".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "current_schemas_including_implicit_postgres",
            execute_query(
                "SELECT current_schemas(true)".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_format_type_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "format_type",
            execute_query(
                "
                SELECT
                    t.oid,
                    t.typname,
                    format_type(t.oid, 20) ft20,
                    format_type(t.oid, 5) ft5,
                    format_type(t.oid, 4) ft4,
                    format_type(t.oid, 0) ft0,
                    format_type(t.oid, -1) ftneg,
                    format_type(t.oid, NULL::bigint) ftnull
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_datetime_precision_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_datetime_precision_simple",
            execute_query(
                "SELECT information_schema._pg_datetime_precision(1184, 3) p".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_datetime_precision_types",
            execute_query(
                "
                SELECT t.oid, information_schema._pg_datetime_precision(t.oid, 3) p
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_numeric_precision_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_numeric_precision_simple",
            execute_query(
                "SELECT information_schema._pg_numeric_precision(1700, 3);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_numeric_precision_types",
            execute_query(
                "
                SELECT t.oid, information_schema._pg_numeric_precision(t.oid, 3) p
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_numeric_scale_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_numeric_scale_simple",
            execute_query(
                "SELECT information_schema._pg_numeric_scale(1700, 50);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_numeric_scale_types",
            execute_query(
                "
                SELECT t.oid, information_schema._pg_numeric_scale(t.oid, 10) s
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_get_userbyid_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_get_userbyid",
            execute_query(
                "
                SELECT pg_get_userbyid(t.id)
                FROM information_schema.testing_dataset t
                WHERE t.id < 15;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unnest_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "unnest_i64_from_table",
            execute_query(
                "SELECT unnest(r.a) FROM (SELECT ARRAY[1,2,3,4] as a UNION ALL SELECT ARRAY[5,6,7,8] as a) as r;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "unnest_str_from_table",
            execute_query(
                "SELECT unnest(r.a) FROM (SELECT ARRAY['1', '2'] as a UNION ALL SELECT ARRAY['3', '4'] as a) as r;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "unnest_i64_scalar",
            execute_query(
                "SELECT unnest(ARRAY[1,2,3,4,5]);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_series_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "generate_series_i64_1",
            execute_query(
                "SELECT generate_series(-5, 5);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_f64_2",
            execute_query(
                "SELECT generate_series(-5, 5, 3);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_f64_1",
            execute_query(
                "SELECT generate_series(-5, 5, 0.5);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_empty_1",
            execute_query(
                "SELECT generate_series(-5, -10, 3);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_empty_2",
            execute_query(
                "SELECT generate_series(1, 5, 0);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_catalog_generate_series_i64",
            execute_query(
                "SELECT pg_catalog.generate_series(1, 5);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "generate_series_from_table",
            execute_query(
                "select generate_series(1, oid) from pg_catalog.pg_type where oid in (16,17);"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_get_expr_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_get_expr_1",
            execute_query(
                "
                SELECT
                    attrelid,
                    attname,
                    pg_catalog.pg_get_expr(attname, attrelid) default
                FROM pg_catalog.pg_attribute
                ORDER BY
                    attrelid ASC,
                    attname ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        insta::assert_snapshot!(
            "pg_get_expr_2",
            execute_query(
                "
                SELECT
                    attrelid,
                    attname,
                    pg_catalog.pg_get_expr(attname, attrelid, true) default
                FROM pg_catalog.pg_attribute
                ORDER BY
                    attrelid ASC,
                    attname ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_subscripts_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_generate_subscripts_1",
            execute_query(
                "SELECT generate_subscripts(r.a, 1) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_generate_subscripts_2_forward",
            execute_query(
                "SELECT generate_subscripts(r.a, 1, false) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_generate_subscripts_2_reverse",
            execute_query(
                "SELECT generate_subscripts(r.a, 1, true) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_generate_subscripts_3",
            execute_query(
                "SELECT generate_subscripts(r.a, 2) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_expandarray_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_expandarray_value",
            execute_query(
                "SELECT (information_schema._pg_expandarray(t.a)).x FROM pg_catalog.pg_class c, (SELECT ARRAY[5, 10, 15] a) t;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_expandarray_index",
            execute_query(
                "SELECT (information_schema._pg_expandarray(t.a)).n FROM pg_catalog.pg_class c, (SELECT ARRAY[5, 10, 15] a) t;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_type_is_visible_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_type_is_visible",
            execute_query(
                "
                SELECT t.oid, t.typname, n.nspname, pg_catalog.pg_type_is_visible(t.oid) is_visible
                FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n
                WHERE t.typnamespace = n.oid
                ORDER BY t.oid ASC;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_get_constraintdef_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_get_constraintdef_1",
            execute_query(
                "select pg_catalog.pg_get_constraintdef(r.oid, true) from pg_catalog.pg_constraint r;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "pg_get_constraintdef_2",
            execute_query(
                "select pg_catalog.pg_get_constraintdef(r.oid) from pg_catalog.pg_constraint r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_date_part_quarter() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "date_part_quarter",
            execute_query(
                "
                SELECT
                    t.d,
                    date_part('quarter', t.d) q
                FROM (
                    SELECT TIMESTAMP '2000-01-05 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2005-05-20 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2010-08-02 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2020-10-01 00:00:00+00:00' d
                ) t
                ORDER BY t.d ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_array_lower() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "array_lower_scalar",
            execute_query(
                "
                SELECT
                    array_lower(ARRAY[1,2,3,4,5]) v1,
                    array_lower(ARRAY[5,4,3,2,1]) v2,
                    array_lower(ARRAY[5,4,3,2,1], 1) v3
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "array_lower_column",
            execute_query(
                "
                SELECT
                    array_lower(t.v) q
                FROM (
                    SELECT ARRAY[1,2,3,4,5] as v UNION ALL
                    SELECT ARRAY[5,4,3,2,1] as v
                ) t
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_array_upper() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "array_upper_scalar",
            execute_query(
                "
                SELECT
                    array_upper(ARRAY[1,2,3,4,5]) v1,
                    array_upper(ARRAY[5,4,3]) v2,
                    array_upper(ARRAY[5,4], 1) v3
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "array_upper_column",
            execute_query(
                "
                SELECT
                    array_upper(t.v) q
                FROM (
                    SELECT ARRAY[1,2,3,4,5] as v
                    UNION ALL
                    SELECT ARRAY[5,4,3,2] as v
                    UNION ALL
                    SELECT ARRAY[5,4,3] as v
                ) t
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_catalog_udf_search_path() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_catalog_udf_search_path",
            execute_query(
                "SELECT version() UNION ALL SELECT pg_catalog.version();".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_has_schema_privilege_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "has_schema_privilege",
            execute_query(
                "SELECT
                    nspname,
                    has_schema_privilege('ovr', nspname, 'CREATE') create,
                    has_schema_privilege('ovr', nspname, 'USAGE') usage
                FROM pg_namespace
                ORDER BY nspname ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_total_relation_size() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_total_relation_size",
            execute_query(
                "SELECT
                    oid,
                    relname,
                    pg_total_relation_size(oid) relsize
                FROM pg_class
                ORDER BY oid ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_discard_postgres() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "discard_postgres_all",
            execute_query("DISCARD ALL;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );
        insta::assert_snapshot!(
            "discard_postgres_plans",
            execute_query("DISCARD PLANS;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );
        insta::assert_snapshot!(
            "discard_postgres_sequences",
            execute_query(
                "DISCARD SEQUENCES;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        insta::assert_snapshot!(
            "discard_postgres_temporary",
            execute_query(
                "DISCARD TEMPORARY;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );
        insta::assert_snapshot!(
            "discard_postgres_temp",
            execute_query("DISCARD TEMP;".to_string(), DatabaseProtocol::PostgreSQL).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn superset_meta_queries() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "superset_attname_query",
            execute_query(
                r#"SELECT a.attname
                FROM pg_attribute a JOIN (
                SELECT unnest(ix.indkey) attnum,
                generate_subscripts(ix.indkey, 1) ord
                FROM pg_index ix
                WHERE ix.indrelid = 13449 AND ix.indisprimary
                ) k ON a.attnum=k.attnum
                WHERE a.attrelid = 13449
                ORDER BY k.ord
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        // TODO should be pg_get_expr instead of format_type
        insta::assert_snapshot!(
            "superset_subquery",
            execute_query(
                "
                SELECT
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    (
                        SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                        FROM pg_catalog.pg_attrdef d
                        WHERE
                            d.adrelid = a.attrelid AND
                            d.adnum = a.attnum AND
                            a.atthasdef
                    ) AS DEFAULT,
                    a.attnotnull,
                    a.attnum,
                    a.attrelid as table_oid,
                    pgd.description as comment,
                    a.attgenerated as generated
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_description pgd ON (
                    pgd.objoid = a.attrelid AND
                    pgd.objsubid = a.attnum
                )
                WHERE
                    a.attrelid = 18000
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                ORDER BY a.attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "superset_visible_query",
            execute_query(
                r#"
                SELECT
                    t.typname as "name",
                    pg_catalog.pg_type_is_visible(t.oid) as "visible",
                    n.nspname as "schema",
                    e.enumlabel as "label"
                FROM pg_catalog.pg_type t
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                LEFT JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
                WHERE t.typtype = 'e'
                ORDER BY
                    "schema",
                    "name",
                    e.oid
                ;
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "superset_attype_query",
            execute_query(
                r#"SELECT
                    t.typname as "name",
                    pg_catalog.format_type(t.typbasetype, t.typtypmod) as "attype",
                    not t.typnotnull as "nullable",
                    t.typdefault as "default",
                    pg_catalog.pg_type_is_visible(t.oid) as "visible",
                    n.nspname as "schema"
                FROM pg_catalog.pg_type t
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typtype = 'd'
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn superset_conname_query() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "superset_conname_query",
            execute_query(
                r#"SELECT r.conname,
                pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
                n.nspname as conschema
                FROM  pg_catalog.pg_constraint r,
                pg_namespace n,
                pg_class c
                WHERE r.conrelid = 13449 AND
                r.contype = 'f' AND
                c.oid = confrelid AND
                n.oid = c.relnamespace
                ORDER BY 1
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // https://github.com/sqlalchemy/sqlalchemy/blob/6104c163eb58e35e46b0bb6a237e824ec1ee1d15/lib/sqlalchemy/dialects/postgresql/base.py
    #[tokio::test]
    async fn sqlalchemy_new_conname_query() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "sqlalchemy_new_conname_query",
            execute_query(
                r#"SELECT
                a.attname,
                pg_catalog.format_type(a.atttypid, a.atttypmod),
                (
                    SELECT
                        pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                    FROM
                        pg_catalog.pg_attrdef AS d
                    WHERE
                        d.adrelid = a.attrelid
                        AND d.adnum = a.attnum
                        AND a.atthasdef
                ) AS DEFAULT,
                a.attnotnull,
                a.attrelid AS table_oid,
                pgd.description AS comment,
                a.attgenerated AS generated,
                (
                    SELECT
                        json_build_object(
                            'always',
                            a.attidentity = 'a',
                            'start',
                            s.seqstart,
                            'increment',
                            s.seqincrement,
                            'minvalue',
                            s.seqmin,
                            'maxvalue',
                            s.seqmax,
                            'cache',
                            s.seqcache,
                            'cycle',
                            s.seqcycle
                        )
                    FROM
                        pg_catalog.pg_sequence AS s
                        JOIN pg_catalog.pg_class AS c ON s.seqrelid = c."oid"
                    WHERE
                        c.relkind = 'S'
                        AND a.attidentity <> ''
                        AND s.seqrelid = CAST(
                            pg_catalog.pg_get_serial_sequence(
                                CAST(CAST(a.attrelid AS REGCLASS) AS TEXT),
                                a.attname
                            ) AS REGCLASS
                        )
                ) AS identity_options
            FROM
                pg_catalog.pg_attribute AS a
                LEFT JOIN pg_catalog.pg_description AS pgd ON (
                    pgd.objoid = a.attrelid
                    AND pgd.objsubid = a.attnum
                )
            WHERE
                a.attrelid = 18000
                AND a.attnum > 0
                AND NOT a.attisdropped
            ORDER BY
                a.attnum"#
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn pgcli_queries() -> Result<(), CubeError> {
        init_logger();

        insta::assert_snapshot!(
            "pgcli_queries_d",
            execute_query(
                r#"SELECT n.nspname as "Schema",
                    c.relname as "Name",
                    CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
                    pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
                    FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
                    WHERE c.relkind IN ('r','p','v','m','S','f','')
                    AND n.nspname <> 'pg_catalog'
                    AND n.nspname !~ '^pg_toast'
                    AND n.nspname <> 'information_schema'
                    AND pg_catalog.pg_table_is_visible(c.oid)
                "#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_desktop_constraints() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_desktop_constraints",
            execute_query(
                "select	'test'::name as PKTABLE_CAT,
                n2.nspname as PKTABLE_SCHEM,
                c2.relname as PKTABLE_NAME,
                a2.attname as PKCOLUMN_NAME,
                'test'::name as FKTABLE_CAT,
                n1.nspname as FKTABLE_SCHEM,
                c1.relname as FKTABLE_NAME,
                a1.attname as FKCOLUMN_NAME,
                i::int2 as KEY_SEQ,
                case ref.confupdtype
                    when 'c' then 0::int2
                    when 'n' then 2::int2
                    when 'd' then 4::int2
                    when 'r' then 1::int2
                    else 3::int2
                end as UPDATE_RULE,
                case ref.confdeltype
                    when 'c' then 0::int2
                    when 'n' then 2::int2
                    when 'd' then 4::int2
                    when 'r' then 1::int2
                    else 3::int2
                end as DELETE_RULE,
                ref.conname as FK_NAME,
                cn.conname as PK_NAME,
                case
                    when ref.condeferrable then
                        case
                        when ref.condeferred then 5::int2
                        else 6::int2
                        end
                    else 7::int2
                end as DEFERRABLITY
             from
             ((((((( (select cn.oid, conrelid, conkey, confrelid, confkey,
                 generate_series(array_lower(conkey, 1), array_upper(conkey, 1)) as i,
                 confupdtype, confdeltype, conname,
                 condeferrable, condeferred
              from pg_catalog.pg_constraint cn,
                pg_catalog.pg_class c,
                pg_catalog.pg_namespace n
              where contype = 'f'
               and  conrelid = c.oid
               and  relname = 'KibanaSampleDataEcommerce'
               and  n.oid = c.relnamespace
               and  n.nspname = 'public'
             ) ref
             inner join pg_catalog.pg_class c1
              on c1.oid = ref.conrelid)
             inner join pg_catalog.pg_namespace n1
              on  n1.oid = c1.relnamespace)
             inner join pg_catalog.pg_attribute a1
              on  a1.attrelid = c1.oid
              and  a1.attnum = conkey[i])
             inner join pg_catalog.pg_class c2
              on  c2.oid = ref.confrelid)
             inner join pg_catalog.pg_namespace n2
              on  n2.oid = c2.relnamespace)
             inner join pg_catalog.pg_attribute a2
              on  a2.attrelid = c2.oid
              and  a2.attnum = confkey[i])
             left outer join pg_catalog.pg_constraint cn
              on cn.conrelid = ref.confrelid
              and cn.contype = 'p')
              order by ref.oid, ref.i;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_desktop_columns() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_desktop_table_columns",
            execute_query(
                "select
                    n.nspname,
                    c.relname,
                    a.attname,
                    a.atttypid,
                    t.typname,
                    a.attnum,
                    a.attlen,
                    a.atttypmod,
                    a.attnotnull,
                    c.relhasrules,
                    c.relkind,
                    c.oid,
                    pg_get_expr(d.adbin, d.adrelid),
                    case
                        t.typtype
                        when 'd' then t.typbasetype
                        else 0
                    end,
                    t.typtypmod,
                    c.relhasoids
                from
                    (
                        (
                            (
                                pg_catalog.pg_class c
                                inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                                and c.oid = 18000
                            )
                            inner join pg_catalog.pg_attribute a on (not a.attisdropped)
                            and a.attnum > 0
                            and a.attrelid = c.oid
                        )
                        inner join pg_catalog.pg_type t on t.oid = a.atttypid
                    )
                    /* Attention, We have hack for on a.atthasdef */
                    left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum
                order by
                    n.nspname,
                    c.relname,
                    attnum;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_desktop_indexes",
            execute_query(
                "SELECT
                    ta.attname,
                    ia.attnum,
                    ic.relname,
                    n.nspname,
                    tc.relname
                FROM
                    pg_catalog.pg_attribute ta,
                    pg_catalog.pg_attribute ia,
                    pg_catalog.pg_class tc,
                    pg_catalog.pg_index i,
                    pg_catalog.pg_namespace n,
                    pg_catalog.pg_class ic
                WHERE
                    tc.relname = 'KibanaSampleDataEcommerce'
                    AND n.nspname = 'public'
                    AND tc.oid = i.indrelid
                    AND n.oid = tc.relnamespace
                    AND i.indisprimary = 't'
                    AND ia.attrelid = i.indexrelid
                    AND ta.attrelid = i.indrelid
                    AND ta.attnum = i.indkey [ia.attnum-1]
                    AND (NOT ta.attisdropped)
                    AND (NOT ia.attisdropped)
                    AND ic.oid = i.indexrelid
                ORDER BY
                    ia.attnum;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_desktop_pkeys",
            execute_query(
                "SELECT
                    ta.attname,
                    ia.attnum,
                    ic.relname,
                    n.nspname,
                    tc.relname
                FROM
                    pg_catalog.pg_attribute ta,
                    pg_catalog.pg_attribute ia,
                    pg_catalog.pg_class tc,
                    pg_catalog.pg_index i,
                    pg_catalog.pg_namespace n,
                    pg_catalog.pg_class ic
                WHERE
                    tc.relname = 'KibanaSampleDataEcommerce'
                    AND n.nspname = 'public'
                    AND tc.oid = i.indrelid
                    AND n.oid = tc.relnamespace
                    AND i.indisprimary = 't'
                    AND ia.attrelid = i.indexrelid
                    AND ta.attrelid = i.indrelid
                    AND ta.attnum = i.indkey [ia.attnum-1]
                    AND (NOT ta.attisdropped)
                    AND (NOT ia.attisdropped)
                    AND ic.oid = i.indexrelid
                ORDER BY
                    ia.attnum;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "tableau_desktop_tables",
            execute_query(
                "select
                    relname,
                    nspname,
                    relkind
                from
                    pg_catalog.pg_class c,
                    pg_catalog.pg_namespace n
                where
                    relkind in ('r', 'v', 'm', 'f')
                    and nspname not in (
                        'pg_catalog',
                        'information_schema',
                        'pg_toast',
                        'pg_temp_1'
                    )
                    and n.oid = relnamespace
                order by
                    nspname,
                    relname"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_get_expr_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_get_expr_query",
            execute_query(
                "SELECT c.oid, a.attnum, a.attname, c.relname, n.nspname, a.attnotnull OR ( t.typtype = 'd' AND t.typnotnull ), a.attidentity != '' OR pg_catalog.Pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval(%'
                FROM   pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n
                    ON ( c.relnamespace = n.oid )
                JOIN pg_catalog.pg_attribute a
                    ON ( c.oid = a.attrelid )
                JOIN pg_catalog.pg_type t
                    ON ( a.atttypid = t.oid )
                LEFT JOIN pg_catalog.pg_attrdef d
                    ON ( d.adrelid = a.attrelid AND d.adnum = a.attnum )
                JOIN (SELECT 2615 AS oid, 2 AS attnum UNION ALL SELECT 1259, 2 UNION ALL SELECT 2609, 4) vals
                ON ( c.oid = vals.oid AND a.attnum = vals.attnum );"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn datagrip_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "datagrip_introspection",
            execute_query(
                "select current_database(), current_schema(), current_user;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn dbeaver_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "dbeaver_introspection_init",
            execute_query(
                "SELECT current_schema(), session_user;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dbeaver_introspection_databases",
            execute_query(
                "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname = 'db'"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dbeaver_introspection_namespaces",
            execute_query(
                "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n
                LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass
                ORDER BY nspname".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "dbeaver_introspection_types",
            execute_query(
                "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description
                FROM pg_catalog.pg_type t
                LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem
                LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid
                LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid
                WHERE t.typname IS NOT NULL
                AND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')
                ORDER BY t.oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn postico1_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "postico1_schemas",
            execute_query(
                "SELECT
                    oid,
                    nspname,
                    nspname = ANY (current_schemas(true)) AS is_on_search_path,
                    oid = pg_my_temp_schema() AS is_my_temp_schema,
                    pg_is_other_temp_schema(oid) AS is_other_temp_schema
                FROM pg_namespace"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_regclass_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "tableau_regclass_query",
            execute_query(
                "SELECT NULL          AS TABLE_CAT,
                n.nspname     AS TABLE_SCHEM,
                c.relname     AS TABLE_NAME,
                CASE n.nspname ~ '^pg_'
                      OR n.nspname = 'information_schema'
                  WHEN true THEN
                    CASE
                      WHEN n.nspname = 'pg_catalog'
                            OR n.nspname = 'information_schema' THEN
                        CASE c.relkind
                          WHEN 'r' THEN 'SYSTEM TABLE'
                          WHEN 'v' THEN 'SYSTEM VIEW'
                          WHEN 'i' THEN 'SYSTEM INDEX'
                          ELSE NULL
                        end
                      WHEN n.nspname = 'pg_toast' THEN
                        CASE c.relkind
                          WHEN 'r' THEN 'SYSTEM TOAST TABLE'
                          WHEN 'i' THEN 'SYSTEM TOAST INDEX'
                          ELSE NULL
                        end
                      ELSE
                        CASE c.relkind
                          WHEN 'r' THEN 'TEMPORARY TABLE'
                          WHEN 'p' THEN 'TEMPORARY TABLE'
                          WHEN 'i' THEN 'TEMPORARY INDEX'
                          WHEN 'S' THEN 'TEMPORARY SEQUENCE'
                          WHEN 'v' THEN 'TEMPORARY VIEW'
                          ELSE NULL
                        end
                    end
                  WHEN false THEN
                    CASE c.relkind
                      WHEN 'r' THEN 'TABLE'
                      WHEN 'p' THEN 'PARTITIONED TABLE'
                      WHEN 'i' THEN 'INDEX'
                      WHEN 'P' THEN 'PARTITIONED INDEX'
                      WHEN 'S' THEN 'SEQUENCE'
                      WHEN 'v' THEN 'VIEW'
                      WHEN 'c' THEN 'TYPE'
                      WHEN 'f' THEN 'FOREIGN TABLE'
                      WHEN 'm' THEN 'MATERIALIZED VIEW'
                      ELSE NULL
                    end
                  ELSE NULL
                end           AS TABLE_TYPE,
                d.description AS REMARKS,
                ''            AS TYPE_CAT,
                ''            AS TYPE_SCHEM,
                ''            AS TYPE_NAME,
                ''            AS SELF_REFERENCING_COL_NAME,
                ''            AS REF_GENERATION
            FROM   pg_catalog.pg_namespace n,
                pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_description d
                       ON ( c.oid = d.objoid
                            AND d.objsubid = 0
                            AND d.classoid = 'pg_class' :: regclass )
            WHERE  c.relnamespace = n.oid
                AND ( false
                       OR ( c.relkind = 'f' )
                       OR ( c.relkind = 'm' )
                       OR ( c.relkind = 'p'
                            AND n.nspname !~ '^pg_'
                            AND n.nspname <> 'information_schema' )
                       OR ( c.relkind = 'r'
                            AND n.nspname !~ '^pg_'
                            AND n.nspname <> 'information_schema' )
                       OR ( c.relkind = 'v'
                            AND n.nspname <> 'pg_catalog'
                            AND n.nspname <> 'information_schema' ) )
            ORDER BY TABLE_SCHEM ASC, TABLE_NAME ASC
            ;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn powerbi_introspection() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "powerbi_supported_types",
            execute_query(
                "/*** Load all supported types ***/
                SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
                CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
                CASE
                  WHEN pg_proc.proname='array_recv' THEN a.typelem
                  WHEN a.typtype='r' THEN rngsubtype
                  ELSE 0
                END AS elemoid,
                CASE
                  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */
                  WHEN a.typtype='r' THEN 2                                        /* Ranges before */
                  WHEN a.typtype='d' THEN 1                                        /* Domains before */
                  ELSE 0                                                           /* Base types first */
                END AS ord
                FROM pg_type AS a
                JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
                JOIN pg_proc ON pg_proc.oid = a.typreceive
                LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
                LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
                LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
                LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)
                WHERE
                  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */
                  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */
                  (pg_proc.proname='array_recv' AND (
                    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */
                    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */
                    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */
                  )) OR
                  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */
                /* changed for stable sort ORDER BY ord */
                ORDER BY a.typname"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_composite_types",
            execute_query(
                "/*** Load field definitions for (free-standing) composite types ***/
                SELECT typ.oid, att.attname, att.atttypid
                FROM pg_type AS typ
                JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
                JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
                JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)
                WHERE
                    (typ.typtype = 'c' AND cls.relkind='c') AND
                attnum > 0 AND     /* Don't load system attributes */
                NOT attisdropped
                ORDER BY typ.oid, att.attnum"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_enums",
            execute_query(
                "/*** Load enum fields ***/
                SELECT pg_type.oid, enumlabel
                FROM pg_enum
                JOIN pg_type ON pg_type.oid=enumtypid
                ORDER BY oid, enumsortorder"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_table_columns",
            execute_query(
                "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE
                from INFORMATION_SCHEMA.columns
                where TABLE_SCHEMA = 'public' and TABLE_NAME = 'KibanaSampleDataEcommerce'
                order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_schemas",
            execute_query(
                "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                from INFORMATION_SCHEMA.tables
                where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')
                order by TABLE_SCHEMA, TABLE_NAME"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_from_subquery",
            execute_query(
                "
                select
                    pkcol.COLUMN_NAME as PK_COLUMN_NAME,
                    fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA,
                    fkcol.TABLE_NAME AS FK_TABLE_NAME,
                    fkcol.COLUMN_NAME as FK_COLUMN_NAME,
                    fkcol.ORDINAL_POSITION as ORDINAL,
                    fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME || '_' || 'users' || '_' || fkcon.CONSTRAINT_NAME as FK_NAME
                from
                    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name
                        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon
                        inner join
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol
                        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA
                        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME
                        inner join
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol
                        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA
                        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME
                where pkcol.TABLE_SCHEMA = 'public' and pkcol.TABLE_NAME = 'users'
                        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION
                order by FK_NAME, fkcol.ORDINAL_POSITION
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "powerbi_uppercase_alias",
            execute_query(
                "
                select
                    i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME,
                    ii.COLUMN_NAME,
                    ii.ORDINAL_POSITION,
                    case
                        when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y'
                        else 'N'
                    end as PRIMARY_KEY
                from INFORMATION_SCHEMA.table_constraints i
                inner join INFORMATION_SCHEMA.key_column_usage ii on
                    i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and
                    i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME and
                    i.TABLE_SCHEMA = ii.TABLE_SCHEMA and
                    i.TABLE_NAME = ii.TABLE_NAME
                where
                    i.TABLE_SCHEMA = 'public' and
                    i.TABLE_NAME = 'KibanaSampleDataEcommerce' and
                    i.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')
                order by
                    i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME,
                    ii.TABLE_SCHEMA,
                    ii.TABLE_NAME,
                    ii.ORDINAL_POSITION
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn tableau_temporary_tables() {
        let create_query = convert_sql_to_cube_query(
            &"
            CREATE LOCAL TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_2_Connect_C\" (
                \"COL\" INTEGER
            ) ON COMMIT PRESERVE ROWS
            ".to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL),
        ).await;
        match create_query {
            Err(CompilationError::Unsupported(msg, _)) => assert_eq!(msg, "Unsupported query type: CREATE LOCAL TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_2_Connect_C\" (\"COL\" INT) ON COMMIT PRESERVE ROWS"),
            _ => panic!("CREATE TABLE should throw CompilationError::Unsupported"),
        };

        let select_into_query = convert_sql_to_cube_query(
            &"
            SELECT *
            INTO TEMPORARY TABLE \"#Tableau_91262_83C81E14-EFF9-4FBD-AA5C-A9D7F5634757_1_Connect_C\"
            FROM (SELECT 1 AS COL) AS CHECKTEMP
            LIMIT 1
            "
            .to_string(),
            get_test_tenant_ctx(),
            get_test_session(DatabaseProtocol::PostgreSQL),
        )
        .await;
        match select_into_query {
            Err(CompilationError::Unsupported(msg, _)) => {
                assert_eq!(msg, "Unsupported query type: SELECT INTO")
            }
            _ => panic!("SELECT INTO should throw CompilationError::unsupported"),
        }
    }

    // This tests asserts that our DF fork contains support for IS TRUE|FALSE
    #[tokio::test]
    async fn df_is_boolean() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_is_boolean",
            execute_query(
                "SELECT r.v, r.v IS TRUE as is_true, r.v IS FALSE as is_false
                 FROM (SELECT true as v UNION ALL SELECT false as v) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for >> && <<
    #[tokio::test]
    async fn df_is_bitwise_shit() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_fork_bitwise_shit",
            execute_query(
                "SELECT 2 << 10 as t1, 2048 >> 10 as t2;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for escaped single quoted strings
    #[tokio::test]
    async fn df_escaped_strings() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_escaped_strings",
            execute_query(
                "SELECT 'test' LIKE e'%' as v1, 'payment_p2020_01' LIKE E'payment\\_p2020\\_01' as v2;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    // This tests asserts that our DF fork contains support for string-boolean coercion and cast
    #[tokio::test]
    async fn db_string_boolean_comparison() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "df_string_boolean_comparison",
            execute_query(
                "SELECT TRUE = 't' t, FALSE <> 'f' f;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_truetyp() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "pg_truetypid_truetypmod",
            execute_query(
                "
                SELECT
                    a.attrelid,
                    a.attname,
                    t.typname,
                    information_schema._pg_truetypid(a.*, t.*) typid,
                    information_schema._pg_truetypmod(a.*, t.*) typmod,
                    information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(a.*, t.*),
                        information_schema._pg_truetypmod(a.*, t.*)
                    ) as_arg
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                ORDER BY a.attrelid ASC, a.attnum ASC
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_to_char_udf() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "to_char_1",
            execute_query(
                "SELECT to_char(x, 'YYYY-MM-DD HH24:MI:SS.MS TZ') FROM (SELECT Str_to_date('2021-08-31 11:05:10.400000', '%Y-%m-%d %H:%i:%s.%f') x) e".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        insta::assert_snapshot!(
            "to_char_2",
            execute_query(
                "
                SELECT to_char(x, 'YYYY-MM-DD HH24:MI:SS.MS TZ')
                FROM  (
                        SELECT Str_to_date('2021-08-31 11:05:10.400000', '%Y-%m-%d %H:%i:%s.%f') x
                    UNION ALL
                        SELECT str_to_date('2021-08-31 11:05', '%Y-%m-%d %H:%i') x
                ) e
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_to_char_query() -> Result<(), CubeError> {
        execute_query(
            "select to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS.MS TZ')".to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_table_exists() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_table_exists",
            execute_query(
                r#"SELECT TRUE AS "_" FROM "public"."KibanaSampleDataEcommerce" WHERE 1 <> 1 LIMIT 0;"#
                    .to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_substring() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                    \"source\".\"substring1\" AS \"substring2\",
                    \"source\".\"count\" AS \"count\"
                FROM (
                    SELECT
                        \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",
                        SUBSTRING(\"KibanaSampleDataEcommerce\".\"customer_gender\" FROM 1 FOR 1234) AS \"substring1\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                ) AS \"source\"".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_doy() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                \"source\".\"order_date\" AS \"order_date\",
                \"source\".\"count\" AS \"count\"
            FROM
                (
                    SELECT
                        (
                            CAST(
                                extract(
                                    doy
                                    from
                                        \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                                ) AS integer
                            )
                        ) AS \"order_date\",
                        count(*) AS \"count\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                    GROUP BY CAST(
                        extract(
                            doy
                            from
                                \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                        ) AS integer
                    )
                    ORDER BY CAST(
                        extract(
                            doy
                            from
                                \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                        ) AS integer
                    ) ASC
                ) \"source\"
            WHERE
                \"source\".\"count\" IS NOT NULL
            ORDER BY
                \"source\".\"count\" ASC
            LIMIT
                100"
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_binary_expr_projection_split() -> Result<(), CubeError> {
        let operators = ["+", "-", "*", "/"];

        for operator in operators {
            let query_plan = convert_select_to_query_plan(
                format!("SELECT
                    (
                        CAST(
                             \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" AS integer
                        ) {} 100
                    ) AS \"taxful_total_price\"
                FROM
                    \"public\".\"KibanaSampleDataEcommerce\"", operator),
                DatabaseProtocol::PostgreSQL,
            )
                .await;

            let logical_plan = query_plan.as_logical_plan();
            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    segments: Some(vec![]),
                    dimensions: Some(vec![
                        "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                    ]),
                    time_dimensions: None,
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None,
                }
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_dow() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "SELECT
                \"source\".\"order_date\" AS \"order_date\",
                \"source\".\"count\" AS \"count\"
            FROM
                (
                    SELECT
                        (
                            CAST(
                                extract(
                                    dow
                                    from
                                        \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                                ) AS integer
                            ) + 1
                        ) AS \"order_date\",
                        count(*) AS \"count\"
                    FROM
                        \"public\".\"KibanaSampleDataEcommerce\"
                    GROUP BY (
                        CAST(
                            extract(
                                dow
                                from
                                    \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                            ) AS integer
                        ) + 1
                    )
                    ORDER BY (
                        CAST(
                            extract(
                                dow
                                from
                                    \"public\".\"KibanaSampleDataEcommerce\".\"order_date\"
                            ) AS integer
                        ) + 1
                    ) ASC
                ) \"source\"
            WHERE
                \"source\".\"count\" IS NOT NULL
            ORDER BY
                \"source\".\"count\" ASC
            LIMIT
                100"
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("day".to_string()),
                    date_range: None,
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_subquery_with_same_name_excel() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "subquery_with_same_name_excel",
            execute_query(
                "SELECT oid, (SELECT oid FROM pg_type WHERE typname like 'geography') as dd FROM pg_type WHERE typname like 'geometry'".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_join_where_and_or() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "join_where_and_or",
            execute_query(
                "
                SELECT
                    att.attname,
                    att.attnum,
                    cl.oid
                FROM pg_attribute att
                JOIN pg_class cl ON
                    cl.oid = attrelid AND (
                        cl.relkind = 's' OR
                        cl.relkind = 'r'
                    )
                ORDER BY
                    cl.oid ASC,
                    att.attnum ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_type_any_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_type_any",
            execute_query(
                "SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n
                ON t.typnamespace = n.oid WHERE t.oid = 25;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_regproc_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_regproc_query",
            execute_query(
                "SELECT typinput='array_in'::regproc as is_array, typtype, typname, pg_type.oid
                FROM pg_catalog.pg_type
                LEFT JOIN (
                    select
                        ns.oid as nspoid,
                        ns.nspname,
                        r.r
                    from pg_namespace as ns
                    join (
                        select
                            s.r,
                            (current_schemas(false))[s.r] as nspname
                        from generate_series(1, array_upper(current_schemas(false), 1)) as s(r)
                    ) as r
                    using ( nspname )
                ) as sp
                ON sp.nspoid = typnamespace
                /* I've changed oid = to oid IN to verify is_array column */
                WHERE pg_type.oid IN (25, 1016)
                ORDER BY sp.r, pg_type.oid DESC;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_namespace_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_namespace",
            execute_query(
                "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG
                FROM pg_catalog.pg_namespace
                WHERE nspname <> 'pg_toast'
                AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1])
                AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))
                ORDER BY TABLE_SCHEM;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pg_class_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pg_class_query",
            execute_query(
                "
                SELECT *
                    FROM (
                        SELECT  n.nspname,
                                c.relname,
                                a.attname,
                                a.atttypid,
                                a.attnotnull or (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
                                a.atttypmod,
                                a.attlen,
                                t.typtypmod,
                                row_number() OVER (partition BY a.attrelid ORDER BY a.attnum) AS attnum,
                                NULLIF(a.attidentity, '') AS attidentity,
                                pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
                                dsc.description,
                                t.typbasetype,
                                t.typtype
                            FROM pg_catalog.pg_namespace n
                            JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
                            JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
                            JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
                            LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
                            LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
                            LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
                            LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
                        WHERE c.relkind IN ('r', 'p', 'v', 'f', 'm') AND a.attnum > 0 AND NOT a.attisdropped AND n.nspname LIKE 'public' AND c.relname LIKE 'KibanaSampleDataEcommerce') c
                WHERE true
                ORDER BY nspname, c.relname, attnum;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_table_cat_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_table_cat_query",
            execute_query(
                "
                SELECT  result.table_cat,
                        result.table_schem,
                        result.table_name,
                        result.column_name,
                        result.key_seq,
                        result.pk_name
                    FROM (
                        SELECT  NULL AS table_cat,
                                n.nspname AS table_schem,
                                ct.relname AS table_name,
                                a.attname AS column_name,
                                (information_schema._pg_expandarray(i.indkey)).n as key_seq,
                                ci.relname AS pk_name,
                                information_schema._pg_expandarray(i.indkey) AS keys,
                                a.attnum AS a_attnum
                            FROM   pg_catalog.pg_class ct
                            JOIN   pg_catalog.pg_attribute a ON(ct.oid = a.attrelid)
                            JOIN   pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                            JOIN   pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
                            JOIN   pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                        WHERE true AND ct.relname = 'actor' AND i.indisprimary) result
                WHERE result.a_attnum = (result.keys).x
                ORDER BY result.table_name, result.pk_name, result.key_seq;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metabase_pktable_cat_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "metabase_pktable_cat_query",
            execute_query(
                "
                SELECT  NULL::text  AS pktable_cat,
                        pkn.nspname AS pktable_schem,
                        pkc.relname AS pktable_name,
                        pka.attname AS pkcolumn_name,
                        NULL::text  AS fktable_cat,
                        fkn.nspname AS fktable_schem,
                        fkc.relname AS fktable_name,
                        fka.attname AS fkcolumn_name,
                        pos.n       AS key_seq,
                        CASE con.confupdtype
                            WHEN 'c' THEN 0
                            WHEN 'n' THEN 2
                            WHEN 'd' THEN 4
                            WHEN 'r' THEN 1
                            WHEN 'p' THEN 1
                            WHEN 'a' THEN 3
                            ELSE NULL
                        END AS update_rule,
                        CASE con.confdeltype
                            WHEN 'c' THEN 0
                            WHEN 'n' THEN 2
                            WHEN 'd' THEN 4
                            WHEN 'r' THEN 1
                            WHEN 'p' THEN 1
                            WHEN 'a' THEN 3
                            ELSE NULL
                        END AS delete_rule,
                        con.conname  AS fk_name,
                        pkic.relname AS pk_name,
                        CASE
                            WHEN con.condeferrable AND con.condeferred THEN 5
                            WHEN con.condeferrable THEN 6
                            ELSE 7
                        END AS deferrability
                    FROM    pg_catalog.pg_namespace pkn,
                            pg_catalog.pg_class pkc,
                            pg_catalog.pg_attribute pka,
                            pg_catalog.pg_namespace fkn,
                            pg_catalog.pg_class fkc,
                            pg_catalog.pg_attribute fka,
                            pg_catalog.pg_constraint con,
                            pg_catalog.generate_series(1, 32) pos(n),
                            pg_catalog.pg_class pkic
                WHERE   pkn.oid = pkc.relnamespace
                AND     pkc.oid = pka.attrelid
                AND     pka.attnum = con.confkey[pos.n]
                AND     con.confrelid = pkc.oid
                AND     fkn.oid = fkc.relnamespace
                AND     fkc.oid = fka.attrelid
                AND     fka.attnum = con.conkey[pos.n]
                AND     con.conrelid = fkc.oid
                AND     con.contype = 'f'
                AND     (pkic.relkind = 'i' OR pkic.relkind = 'I')
                AND     pkic.oid = con.conindid
                AND     fkn.nspname = 'public'
                AND     fkc.relname = 'actor'
                ORDER BY pkn.nspname, pkc.relname, con.conname, pos.n;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_computing_ilike_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sigma_computing_ilike_query",
            execute_query(
                "
                select distinct table_schema
                from information_schema.tables
                where
                    table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN', 'FOREIGN TABLE') and
                    table_schema NOT IN ('pg_catalog', 'information_schema') and
                    table_schema ilike '%'
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_computing_pg_matviews_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sigma_computing_pg_matviews_query",
            execute_query(
                "
                SELECT table_name FROM (
                    select table_name
                    from information_schema.tables
                    where
                        table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN', 'FOREIGN TABLE') and
                        table_schema = 'public'
                    UNION
                    select matviewname as table_name
                    from pg_catalog.pg_matviews
                    where schemaname = 'public'
                ) t
                ORDER BY table_name ASC
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_computing_array_subquery_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sigma_computing_array_subquery_query",
            execute_query(
                r#"
                select
                    cl.relname as "source_table",
                    array(
                        select (
                            select attname::text
                            from pg_attribute
                            where
                                attrelid = con.conrelid and
                                attnum = con.conkey[i]
                        )
                        from generate_series(array_lower(con.conkey, 1), array_upper(con.conkey, 1)) i
                    ) as "source_keys",
                    (
                        select nspname
                        from pg_namespace ns2
                        join pg_class cl2 on ns2.oid = cl2.relnamespace
                        where cl2.oid = con.confrelid
                    ) as "target_schema",
                    (
                        select relname
                        from pg_class
                        where oid = con.confrelid
                    ) as "target_table",
                    array(
                        select (
                            select attname::text
                            from pg_attribute
                            where
                                attrelid = con.confrelid and
                                attnum = con.confkey[i]
                        )
                        from generate_series(array_lower(con.confkey, 1), array_upper(con.confkey, 1)) i
                    ) as "target_keys"
                from pg_class cl
                join pg_namespace ns on cl.relnamespace = ns.oid
                join pg_constraint con on con.conrelid = cl.oid
                where
                    ns.nspname = 'public' and
                    cl.relname >= 'A' and
                    cl.relname <= 'z' and
                    con.contype = 'f'
                order by
                    "source_table",
                    con.conname
                ;
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sigma_computing_with_subquery_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "sigma_computing_with_subquery_query",
            execute_query(
                "
                with
                    nsp as (
                        select oid
                        from pg_catalog.pg_namespace
                        where nspname = 'public'
                    ),
                    tbl as (
                        select oid
                        from pg_catalog.pg_class
                        where
                            relname = 'KibanaSampleDataEcommerce' and
                            relnamespace = (select oid from nsp)
                    )
                select
                    attname,
                    typname,
                    description
                from pg_attribute a
                join pg_type on atttypid = pg_type.oid
                left join pg_description on
                    attrelid = objoid and
                    attnum = objsubid
                where
                    attnum > 0 and
                    attrelid = (select oid from tbl)
                order by attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_google_sheets_pg_database_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "google_sheets_pg_database_query",
            execute_query(
                "
                SELECT
                    cl.relname as Table,
                    att.attname AS Name,
                    att.attnum as Position,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'true'
                        ELSE 'false'
                    END as Nullable,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c
                            where
                                c.conrelid = cl.oid and
                                c.contype = 'p' and
                                att.attnum = ANY (c.conkey)
                        ) THEN true
                        ELSE false
                    END as IsKey,
                    CASE
                        WHEN cs.relname IS NULL THEN 'false'
                        ELSE 'true'
                    END as IsAutoIncrement,
                    CASE
                        WHEN ty.typname = 'bpchar' THEN 'char'
                        WHEN ty.typname = '_bpchar' THEN '_char'
                        ELSE ty.typname
                    END as TypeName,
                    CASE
                        WHEN
                            ty.typname Like 'bit' OR
                            ty.typname Like 'varbit' and
                            att.atttypmod > 0
                        THEN att.atttypmod
                        WHEN
                            ty.typname Like 'interval' OR
                            ty.typname Like 'timestamp' OR
                            ty.typname Like 'timestamptz' OR
                            ty.typname Like 'time' OR
                            ty.typname Like 'timetz' THEN -1
                        WHEN att.atttypmod > 0 THEN att.atttypmod - 4
                        ELSE att.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS DatetimeLength
                FROM pg_attribute att
                JOIN pg_type ty ON ty.oid = atttypid
                JOIN pg_namespace tn ON tn.oid = ty.typnamespace
                JOIN pg_class cl ON
                    cl.oid = attrelid AND
                    (
                        (cl.relkind = 'r') OR
                        (cl.relkind = 's') OR
                        (cl.relkind = 'v') OR
                        (cl.relkind = 'm') OR
                        (cl.relkind = 'f')
                    )
                JOIN pg_namespace na ON na.oid = cl.relnamespace
                LEFT OUTER JOIN (
                    pg_depend
                    JOIN pg_class cs ON
                        objid = cs.oid AND
                        cs.relkind = 'S' AND
                        classid = 'pg_class'::regclass::oid
                ) ON
                    refobjid = attrelid AND
                    refobjsubid = attnum
                LEFT JOIN pg_database db ON db.datname = current_database()
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    na.nspname = 'public' AND
                    cl.relname = 'KibanaSampleDataEcommerce'
                ORDER BY attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_has_schema_privilege_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_has_schema_privilege_query",
            execute_query(
                "
                SELECT nspname AS schema_name
                FROM pg_namespace
                WHERE
                    (
                        has_schema_privilege('ovr', nspname, 'USAGE') = TRUE OR
                        has_schema_privilege('ovr', nspname, 'CREATE') = TRUE
                    ) AND
                    nspname NOT IN ('pg_catalog', 'information_schema') AND
                    nspname NOT LIKE 'pg_toast%' AND
                    nspname NOT LIKE 'pg_temp_%'
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_pktable_cat_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_pktable_cat_query",
            execute_query(
                "
                SELECT
                    NULL::text AS PKTABLE_CAT,
                    pkn.nspname AS PKTABLE_SCHEM,
                    pkc.relname AS PKTABLE_NAME,
                    pka.attname AS PKCOLUMN_NAME,
                    NULL::text AS FKTABLE_CAT,
                    fkn.nspname AS FKTABLE_SCHEM,
                    fkc.relname AS FKTABLE_NAME,
                    fka.attname AS FKCOLUMN_NAME,
                    pos.n AS KEY_SEQ,
                    CASE con.confupdtype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS UPDATE_RULE,
                    CASE con.confdeltype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS DELETE_RULE,
                    con.conname AS FK_NAME,
                    pkic.relname AS PK_NAME,
                    CASE
                        WHEN con.condeferrable AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                    END AS DEFERRABILITY
                FROM
                    pg_catalog.pg_namespace pkn,
                    pg_catalog.pg_class pkc,
                    pg_catalog.pg_attribute pka,
                    pg_catalog.pg_namespace fkn,
                    pg_catalog.pg_class fkc,
                    pg_catalog.pg_attribute fka,
                    pg_catalog.pg_constraint con,
                    pg_catalog.generate_series(1, 32) pos(n),
                    pg_catalog.pg_depend dep,
                    pg_catalog.pg_class pkic
                WHERE
                    pkn.oid = pkc.relnamespace AND
                    pkc.oid = pka.attrelid AND
                    pka.attnum = con.confkey[pos.n] AND
                    con.confrelid = pkc.oid AND
                    fkn.oid = fkc.relnamespace AND
                    fkc.oid = fka.attrelid AND
                    fka.attnum = con.conkey[pos.n] AND
                    con.conrelid = fkc.oid AND
                    con.contype = 'f' AND
                    con.oid = dep.objid AND
                    pkic.oid = dep.refobjid AND
                    pkic.relkind = 'i' AND
                    dep.classid = 'pg_constraint'::regclass::oid AND
                    dep.refclassid = 'pg_class'::regclass::oid AND
                    fkn.nspname = 'public' AND
                    fkc.relname = 'TABLENAME'
                ORDER BY
                    pkn.nspname,
                    pkc.relname,
                    con.conname,
                    pos.n
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_quicksight_table_cat_query() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "quicksight_table_cat_query",
            execute_query(
                "
                SELECT
                    NULL AS TABLE_CAT,
                    n.nspname AS TABLE_SCHEM,
                    ct.relname AS TABLE_NAME,
                    a.attname AS COLUMN_NAME,
                    (i.keys).n AS KEY_SEQ,
                    ci.relname AS PK_NAME
                FROM pg_catalog.pg_class ct
                JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
                JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                JOIN (
                    SELECT
                        i.indexrelid,
                        i.indrelid,
                        i.indisprimary,
                        information_schema._pg_expandarray(i.indkey) AS keys
                    FROM pg_catalog.pg_index i
                ) i ON (
                    a.attnum = (i.keys).x AND
                    a.attrelid = i.indrelid
                )
                JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                WHERE
                    true AND
                    n.nspname = 'public' AND
                    ct.relname = 'Orders' AND
                    i.indisprimary
                ORDER BY
                    table_name,
                    pk_name,
                    key_seq
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cast_decimal_default_precision() -> Result<(), CubeError> {
        insta::assert_snapshot!(
            "cast_decimal_default_precision",
            execute_query(
                "
                SELECT \"rows\".b as \"plan\", count(1) as \"a0\"
                FROM (SELECT * FROM (select 1 \"teamSize\", 2 b UNION ALL select 1011 \"teamSize\", 3 b) \"_\"
                WHERE ((CAST(\"_\".\"teamSize\" as DECIMAL) = CAST(1011 as DECIMAL)))) \"rows\"
                GROUP BY \"plan\";
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

        let query_plan = convert_select_to_query_plan(
            "SELECT count FROM KibanaSampleDataEcommerce WHERE (CAST(maxPrice AS Decimal) = CAST(100 AS Decimal));"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();
        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec!["100".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_triple_ident() -> Result<(), CubeError> {
        let query_plan = convert_select_to_query_plan(
            "select count
            from \"public\".\"KibanaSampleDataEcommerce\"
            where (\"public\".\"KibanaSampleDataEcommerce\".\"maxPrice\" > 100 and \"public\".\"KibanaSampleDataEcommerce\".\"maxPrice\" < 150);
            ".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await;

        let logical_plan = query_plan.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string(),]),
                segments: Some(vec![]),
                dimensions: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("gt".to_string()),
                        values: Some(vec!["100".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                        operator: Some("lt".to_string()),
                        values: Some(vec!["150".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn metabase_interval_date_range_filter() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT COUNT(*) 
            FROM KibanaSampleDataEcommerce 
            WHERE KibanaSampleDataEcommerce.order_date >= CAST((CAST(now() AS timestamp) + (INTERVAL '-30 day')) AS date);
            ".to_string(), 
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        let filters = logical_plan
            .find_cube_scan()
            .request
            .filters
            .unwrap_or_default();
        let filter_vals = if filters.len() > 0 {
            filters[0].values.clone()
        } else {
            None
        };

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("afterDate".to_string()),
                    values: filter_vals,
                    or: None,
                    and: None,
                },])
            }
        )
    }

    #[tokio::test]
    async fn superset_timeout_reached() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "
            SELECT \"KibanaSampleDataEcommerce\".\"count\" AS \"count\",\
             \"KibanaSampleDataEcommerce\".\"order_date\" AS \"order_date\", \
             \"KibanaSampleDataEcommerce\".\"is_male\" AS \"is_male\",\
             \"KibanaSampleDataEcommerce\".\"is_female\" AS \"is_female\",\
             \"KibanaSampleDataEcommerce\".\"maxPrice\" AS \"maxPrice\",\
             \"KibanaSampleDataEcommerce\".\"minPrice\" AS \"minPrice\",\
             \"KibanaSampleDataEcommerce\".\"avgPrice\" AS \"avgPrice\"\
             FROM public.\"KibanaSampleDataEcommerce\" WHERE \"order_date\" >= str_to_date('2021-06-30 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US') AND \"order_date\" < str_to_date('2022-06-30 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US') AND \"is_male\" = true ORDER BY \"order_date\" DESC LIMIT 10000
            ".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![
                    "KibanaSampleDataEcommerce.count".to_string(),
                    "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    "KibanaSampleDataEcommerce.minPrice".to_string(),
                    "KibanaSampleDataEcommerce.avgPrice".to_string(),
                ]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.order_date".to_string()]),
                segments: Some(vec!["KibanaSampleDataEcommerce.is_male".to_string()]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2021-06-30T00:00:00.000Z".to_string(),
                        "2022-06-29T23:59:59.999Z".to_string()
                    ]))
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "desc".to_string(),
                ]]),
                limit: Some(10000),
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn superset_ilike() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT customer_gender AS customer_gender FROM public.\"KibanaSampleDataEcommerce\" WHERE customer_gender ILIKE '%fem%' GROUP BY customer_gender LIMIT 1000".to_string(),
            DatabaseProtocol::PostgreSQL,
        ).await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1000),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["fem".to_string()]),
                    or: None,
                    and: None,
                }]),
            }
        )
    }

    #[tokio::test]
    async fn metabase_limit_0() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
            "SELECT true AS \"_\" FROM \"public\".\"KibanaSampleDataEcommerce\" WHERE 1 <> 1 LIMIT 0".to_string(),
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(1),
                offset: None,
                filters: None
            }
        )
    }

    #[tokio::test]
    async fn test_outer_aggr_simple_count() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT CAST(TRUNC(EXTRACT(YEAR FROM order_date)) AS INTEGER), Count(1) FROM KibanaSampleDataEcommerce GROUP BY 1
            ".to_string(), 
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("year".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn metabase_date_filters() {
        init_logger();

        let now = "str_to_date('2022-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')";
        let cases = vec![
            // last 30 days
            [
                format!("CAST(({} + (INTERVAL '-30 day')) AS date)", now),
                format!("CAST({} AS date)", now),
                "2021-12-02T00:00:00.000Z".to_string(),
                "2021-12-31T23:59:59.999Z".to_string(),
            ],
            // last 30 weeks
            [
                format!("(CAST(date_trunc('week', (({} + (INTERVAL '-30 week')) + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                format!("(CAST(date_trunc('week', ({} + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                "2021-05-30T00:00:00.000Z".to_string(),
                "2021-12-25T23:59:59.999Z".to_string(),
            ],
            // last 30 quarters
            [
                format!("date_trunc('quarter', ({} + (INTERVAL '-90 month')))", now),
                format!("date_trunc('quarter', {})", now),
                "2014-07-01T00:00:00.000Z".to_string(),
                "2021-12-31T23:59:59.999Z".to_string(),
            ],
            // this year
            [
                format!("date_trunc('year', {})", now),
                format!("date_trunc('year', ({} + (INTERVAL '1 year')))", now),
                "2022-01-01T00:00:00.000Z".to_string(),
                "2022-12-31T23:59:59.999Z".to_string(),
            ],
            // next 2 years including current
            [
                format!("date_trunc('year', {})", now),
                format!("date_trunc('year', ({} + (INTERVAL '3 year')))", now),
                "2022-01-01T00:00:00.000Z".to_string(),
                "2024-12-31T23:59:59.999Z".to_string(),
            ],
        ];
        for [lte, gt, from, to] in cases {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT count FROM (SELECT count FROM KibanaSampleDataEcommerce
                    WHERE (order_date >= {} AND order_date < {})) source",
                    lte, gt
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: None,
                        date_range: Some(json!(vec![from, to])),
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            );
        }

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"source\".\"count\" AS \"count\" 
            FROM (
                    SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" FROM \"public\".\"KibanaSampleDataEcommerce\"
                    WHERE \"public\".\"KibanaSampleDataEcommerce\".\"order_date\" 
                    BETWEEN timestamp with time zone '2022-06-13 12:30:00.000Z' 
                    AND timestamp with time zone '2022-06-29 12:30:00.000Z'
            ) 
            \"source\""
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: None,
                    date_range: Some(json!(vec![
                        "2022-06-13 12:30:00.000Z".to_string(),
                        "2022-06-29 12:30:00.000Z".to_string()
                    ]))
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None
            }
        );

        let cases = vec![
            // prev 5 days starting 4 weeks ago
            [
                "(INTERVAL '4 week')".to_string(),
                format!("CAST(({} + (INTERVAL '-5 day')) AS date)", now),
                format!("CAST({} AS date)", now),
                "2021-11-29T00:00:00.000Z".to_string(),
                "2021-12-04T00:00:00.000Z".to_string(),
            ],
            // prev 5 weeks starting 4 weeks ago
            [
                "(INTERVAL '4 week')".to_string(),
                format!("(CAST(date_trunc('week', (({} + (INTERVAL '-5 week')) + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                format!("(CAST(date_trunc('week', ({} + (INTERVAL '1 day'))) AS timestamp) + (INTERVAL '-1 day'))", now),
                "2021-10-24T00:00:00.000Z".to_string(),
                "2021-11-28T00:00:00.000Z".to_string(),
            ],
            // prev 5 months starting 4 months ago
            [
                "(INTERVAL '4 month')".to_string(),
                format!("date_trunc('month', ({} + (INTERVAL '-5 month')))", now),
                format!("date_trunc('month', {})", now),
                "2021-04-01T00:00:00.000Z".to_string(),
                "2021-09-01T00:00:00.000Z".to_string(),
            ],
        ];

        for [interval, lowest, highest, from, to] in cases {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT \"source\".\"count\" AS \"count\" 
                    FROM (
                        SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" FROM \"public\".\"KibanaSampleDataEcommerce\"
                        WHERE (\"public\".\"KibanaSampleDataEcommerce\".\"order_date\" + {}) BETWEEN {} AND {}
                    ) 
                    \"source\"",
                    interval, lowest, highest
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: None,
                        date_range: Some(json!(vec![from, to])),
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            );
        }

        let logical_plan = convert_select_to_query_plan(
            format!(
                "SELECT \"source\".\"order_date\" AS \"order_date\", \"source\".\"max\" AS \"max\" 
                FROM (SELECT date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\") AS \"order_date\", max(\"KibanaSampleDataEcommerce\".\"maxPrice\") AS \"max\" FROM \"KibanaSampleDataEcommerce\"
                GROUP BY date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\")
                ORDER BY date_trunc('month', \"KibanaSampleDataEcommerce\".\"order_date\") ASC) \"source\"
                WHERE (CAST(date_trunc('month', \"source\".\"order_date\") AS timestamp) + (INTERVAL '60 minute')) BETWEEN date_trunc('minute', ({} + (INTERVAL '-30 minute')))
                AND date_trunc('minute', {})", 
                now, now
            ),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.maxPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                }]),
                order: Some(vec![vec![
                    "KibanaSampleDataEcommerce.order_date".to_string(),
                    "asc".to_string(),
                ]]),
                limit: None,
                offset: None,
                filters: None
            }
        );
    }

    #[tokio::test]
    async fn test_metabase_bins() {
        let logical_plan = convert_select_to_query_plan(
            "
            SELECT ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1) AS \"taxful_total_price\", count(*) AS \"count\"
            FROM \"public\".\"KibanaSampleDataEcommerce\"
            GROUP BY ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1)
            ORDER BY ((floor(((\"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" - 1.1) / 0.025)) * 0.025) + 1.1) ASC;
            ".to_string(), 
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![
                    "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                ]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }

    #[tokio::test]
    async fn metabase_contains_str_filters() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\"
                FROM \"public\".\"KibanaSampleDataEcommerce\"
                WHERE (lower(\"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\") like '%female%')
                LIMIT 10"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("contains".to_string()),
                    values: Some(vec!["female".to_string()]),
                    or: None,
                    and: None,
                },]),
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\"
            FROM \"public\".\"KibanaSampleDataEcommerce\"
            WHERE (NOT (lower(\"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\") like '%female%') OR \"public\".\"KibanaSampleDataEcommerce\".\"customer_gender\" IS NULL)
            LIMIT 10"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notContains".to_string()),
                            values: Some(vec!["female".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                            operator: Some("notSet".to_string()),
                            values: None,
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
            }
        );
    }

    #[tokio::test]
    async fn metabase_between_numbers_filters() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" 
                FROM \"public\".\"KibanaSampleDataEcommerce\" 
                WHERE \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" BETWEEN 1 AND 2
                LIMIT 10"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10),
                offset: None,
                filters: Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                        operator: Some("gte".to_string()),
                        values: Some(vec!["1".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                        operator: Some("lte".to_string()),
                        values: Some(vec!["2".to_string()]),
                        or: None,
                        and: None,
                    }
                ]),
            }
        );

        let logical_plan = convert_select_to_query_plan(
            "SELECT \"public\".\"KibanaSampleDataEcommerce\".\"count\" AS \"count\" 
            FROM \"public\".\"KibanaSampleDataEcommerce\" 
            WHERE \"public\".\"KibanaSampleDataEcommerce\".\"taxful_total_price\" NOT BETWEEN 1 AND 2
            LIMIT 10"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: None,
                order: None,
                limit: Some(10),
                offset: None,
                filters: Some(vec![V1LoadRequestQueryFilterItem {
                    member: None,
                    operator: None,
                    values: None,
                    or: Some(vec![
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("lt".to_string()),
                            values: Some(vec!["1".to_string()]),
                            or: None,
                            and: None,
                        }),
                        json!(V1LoadRequestQueryFilterItem {
                            member: Some(
                                "KibanaSampleDataEcommerce.taxful_total_price".to_string()
                            ),
                            operator: Some("gt".to_string()),
                            values: Some(vec!["2".to_string()]),
                            or: None,
                            and: None,
                        })
                    ]),
                    and: None,
                },]),
            }
        );
    }

    #[tokio::test]
    async fn metabase_aggreagte_by_week_of_year() {
        init_logger();

        let logical_plan = convert_select_to_query_plan(
                "SELECT ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0)) AS \"order_date\", 
                               min(\"KibanaSampleDataEcommerce\".\"minPrice\") AS \"min\"
                FROM \"KibanaSampleDataEcommerce\"
                GROUP BY ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0))
                ORDER BY ceil((CAST(extract(doy from CAST(date_trunc('week', \"KibanaSampleDataEcommerce\".\"order_date\") AS timestamp)) AS integer) / 7.0)) ASC"
                .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await
        .as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec!["KibanaSampleDataEcommerce.minPrice".to_string()]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("week".to_string()),
                    date_range: None,
                },]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        );
    }

    #[tokio::test]
    async fn datastudio_date_aggregations() {
        let supported_granularities = vec![
            // date
            [
                "CAST(DATE_TRUNC('SECOND', \"order_date\") AS DATE)",
                "second",
            ],
            // date, time
            ["DATE_TRUNC('SECOND', \"order_date\")", "second"],
            // date, hour, minute
            [
                "DATE_TRUNC('MINUTE', DATE_TRUNC('SECOND', \"order_date\"))",
                "minute",
            ],
            // month
            [
                "EXTRACT(MONTH FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "month",
            ],
            // minute
            [
                "EXTRACT(MINUTE FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "minute",
            ],
            // hour
            [
                "EXTRACT(HOUR FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "hour",
            ],
            // day of month
            [
                "EXTRACT(DAY FROM DATE_TRUNC('SECOND', \"order_date\"))::integer",
                "day",
            ],
            // iso week / iso year / day of year
            ["DATE_TRUNC('SECOND', \"order_date\")", "second"],
            // month, day
            // TODO: support TO_CHAR aggregation
            // [
            //     "CAST(TO_CHAR(DATE_TRUNC('SECOND', \"order_date\"), 'MMDD') AS BIGINT)",
            //     "second",
            // ],
            // date, hour, minute
            [
                "DATE_TRUNC('MINUTE', DATE_TRUNC('SECOND', \"order_date\"))",
                "minute",
            ],
            // date, hour
            [
                "DATE_TRUNC('HOUR', DATE_TRUNC('SECOND', \"order_date\"))",
                "hour",
            ],
            // year, month
            [
                "CAST(DATE_TRUNC('MONTH', DATE_TRUNC('SECOND', \"order_date\")) AS DATE)",
                "month",
            ],
            // year
            [
                "CAST(DATE_TRUNC('YEAR', DATE_TRUNC('SECOND', \"order_date\")) AS DATE)",
                "year",
            ],
        ];

        for [expr, expected_granularity] in supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT {} AS \"qt_u3dj8wr1vc\", COUNT(1) AS \"__record_count\" FROM KibanaSampleDataEcommerce GROUP BY \"qt_u3dj8wr1vc\"",
                    expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
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
                }
            )
        }
    }

    #[tokio::test]
    async fn test_extract_date_trunc_week() {
        let supported_granularities = vec![
            (
                "EXTRACT(WEEK FROM DATE_TRUNC('MONTH', \"order_date\"))::integer",
                "month",
            ),
            (
                "EXTRACT(MONTH FROM DATE_TRUNC('WEEK', \"order_date\"))::integer",
                "week",
            ),
        ];

        for (expr, granularity) in supported_granularities {
            let logical_plan = convert_select_to_query_plan(
                format!(
                    "SELECT {} AS \"qt_u3dj8wr1vc\" FROM KibanaSampleDataEcommerce GROUP BY \"qt_u3dj8wr1vc\"",
                    expr
                ),
                DatabaseProtocol::PostgreSQL,
            )
            .await
            .as_logical_plan();

            assert_eq!(
                logical_plan.find_cube_scan().request,
                V1LoadRequestQuery {
                    measures: Some(vec![]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                        dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                        granularity: Some(granularity.to_string()),
                        date_range: None,
                    }]),
                    order: None,
                    limit: None,
                    offset: None,
                    filters: None
                }
            )
        }
    }

    #[tokio::test]
    async fn test_metabase_unwrap_date_cast() {
        let logical_plan = convert_select_to_query_plan(
            "SELECT max(CAST(\"KibanaSampleDataEcommerce\".\"order_date\" AS date)) AS \"max\" FROM \"KibanaSampleDataEcommerce\"".to_string(), 
            DatabaseProtocol::PostgreSQL
        ).await.as_logical_plan();

        assert_eq!(
            logical_plan.find_cube_scan().request,
            V1LoadRequestQuery {
                measures: Some(vec![]),
                dimensions: Some(vec![]),
                segments: Some(vec![]),
                time_dimensions: Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_owned(),
                    granularity: Some("month".to_owned()),
                    date_range: None
                }]),
                order: None,
                limit: None,
                offset: None,
                filters: None,
            }
        )
    }
}
