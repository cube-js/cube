use std::sync::Arc;
use std::{backtrace::Backtrace, fmt};

use chrono::{prelude::*, Duration};

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::catalog::MemoryCatalogProvider;
use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};

use datafusion::datasource::MemTable;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::SqlToRel;
use datafusion::variable::VarType;
use datafusion::{logical_plan::LogicalPlan, prelude::*};
use log::{debug, trace};
use serde::Serialize;
use serde_json::json;
use sqlparser::ast::{self, Ident, ObjectName};

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
use msql_srv::ColumnType;

use self::builder::*;
use self::context::*;
use self::engine::context::SystemVar;
use self::engine::udf::{
    create_connection_id_udf, create_current_user_udf, create_db_udf, create_user_udf,
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
    if let Some(selection) = ctx.find_selection_for_expr(expr)? {
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
                    "Unable to use segment {} as column in SELECT",
                    s.get_real_name()
                )))
            }
        }
    } else {
        return Err(CompilationError::Unknown(format!(
            "Expression in selection: {}",
            expr.to_string()
        )));
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
enum CompiledExpression {
    Selection(Selection),
    StringLiteral(String),
    DateLiteral(DateTime<Utc>),
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
            CompiledExpression::DateLiteral(date) => Ok(date.to_rfc3339()),
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
        ast::Expr::Value(value) => match value {
            ast::Value::SingleQuotedString(v) => Ok(CompiledExpression::StringLiteral(v.clone())),
            ast::Value::Number(v, _) => Ok(CompiledExpression::NumberLiteral(v.clone(), false)),
            ast::Value::Boolean(v) => Ok(CompiledExpression::BooleanLiteral(*v)),
            _ => Err(CompilationError::User(format!(
                "Unsupported value: {:?}",
                value
            ))),
        },
        ast::Expr::Function(f) => {
            match f.name.to_string().to_lowercase().as_str() {
                //
                "str_to_date" => match f.args.as_slice() {
                    [ast::FunctionArg::Unnamed(date_expr), ast::FunctionArg::Unnamed(format_expr)] =>
                    {
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
                                CompilationError::User(format!(
                                    "Unable to parse {}, err: {}",
                                    date,
                                    e.to_string(),
                                ))
                            })?;

                        Ok(CompiledExpression::DateLiteral(parsed_date))
                    }
                    _ => Err(CompilationError::User(format!(
                        "Unsupported function: {:?}",
                        f
                    ))),
                },
                _ => Err(CompilationError::User(format!(
                    "Unsupported function: {:?}",
                    f
                ))),
            }
        }
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
            let (value, operator) = match op {
                ast::BinaryOperator::NotLike => (filter_expr, "notContains".to_string()),
                ast::BinaryOperator::Like => (filter_expr, "contains".to_string()),
                ast::BinaryOperator::Eq => (filter_expr, "equals".to_string()),
                ast::BinaryOperator::NotEq => (filter_expr, "notEquals".to_string()),
                ast::BinaryOperator::Gt => match filter_expr {
                    CompiledExpression::DateLiteral(dt) => (
                        CompiledExpression::DateLiteral(dt + Duration::milliseconds(1)),
                        "afterDate".to_string(),
                    ),
                    _ => (filter_expr, "gt".to_string()),
                },
                ast::BinaryOperator::GtEq => match filter_expr {
                    CompiledExpression::DateLiteral(_) => (filter_expr, "afterDate".to_string()),
                    _ => {
                        if dim.is_time() {
                            let casted_filter_expr = filter_expr.to_date();
                            if let Some(dt) = casted_filter_expr {
                                (CompiledExpression::DateLiteral(dt), "afterDate".to_string())
                            } else {
                                return Err(CompilationError::User(format!(
                                    "Unable to compare time dimension \"{}\" with not a date value: {}",
                                    dim.get_real_name(),
                                    filter_expr.to_value_as_str()?
                                )));
                            }
                        } else {
                            (filter_expr, "gte".to_string())
                        }
                    }
                },
                ast::BinaryOperator::Lt => match filter_expr {
                    CompiledExpression::DateLiteral(dt) => (
                        CompiledExpression::DateLiteral(dt - Duration::milliseconds(1)),
                        "beforeDate".to_string(),
                    ),
                    _ => {
                        if dim.is_time() {
                            let casted_filter_expr = filter_expr.to_date();
                            if let Some(dt) = casted_filter_expr {
                                (
                                    CompiledExpression::DateLiteral(dt - Duration::milliseconds(1)),
                                    "beforeDate".to_string(),
                                )
                            } else {
                                return Err(CompilationError::User(format!(
                                    "Unable to compare time dimension \"{}\" with not a date value: {}",
                                    dim.get_real_name(),
                                    filter_expr.to_value_as_str()?
                                )));
                            }
                        } else {
                            (filter_expr, "lt".to_string())
                        }
                    }
                },
                ast::BinaryOperator::LtEq => match filter_expr {
                    CompiledExpression::DateLiteral(_) => (filter_expr, "beforeDate".to_string()),
                    _ => (filter_expr, "lte".to_string()),
                },
                _ => {
                    return Err(CompilationError::Unsupported(format!(
                        "Unable to compile operator: {:?}",
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
                    let filter_pushdown = builder.push_date_range_for_time_dimenssion(
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
                            let filter_pushdown = builder.push_date_range_for_time_dimenssion(
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
                                "Unable to use segment {} in GROUP BY",
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
    if !order_by.is_empty() {
        for order_expr in order_by.iter() {
            match &order_expr.expr {
                ast::Expr::Identifier(i) => {
                    if let Some(selection) = ctx.find_selection_for_identifier(&i.to_string(), true)
                    {
                        let direction_as_str = if let Some(direction) = order_expr.asc {
                            if direction {
                                "asc".to_string()
                            } else {
                                "desc".to_string()
                            }
                        } else {
                            "asc".to_string()
                        };

                        match selection {
                            Selection::Dimension(d) => {
                                builder.with_order(vec![d.name.clone(), direction_as_str])
                            }
                            Selection::Measure(m) => {
                                builder.with_order(vec![m.name.clone(), direction_as_str])
                            }
                            Selection::TimeDimension(t, _) => {
                                builder.with_order(vec![t.name.clone(), direction_as_str])
                            }
                            Selection::Segment(s) => {
                                return Err(CompilationError::User(format!(
                                    "Unable to use segment {} in ORDER BY",
                                    s.get_real_name()
                                )));
                            }
                        };
                    } else {
                        return Err(CompilationError::Unknown(format!(
                            "Unknown dimension: {}",
                            i.to_string()
                        )));
                    }
                }
                _ => {
                    return Err(CompilationError::Unsupported(format!(
                        "Unsupported projection: {:?}",
                        order_expr.expr
                    )));
                }
            }
        }
    }

    Ok(())
}

fn compile_select(expr: &ast::Select, ctx: &mut QueryContext) -> CompilationResult<QueryBuilder> {
    let mut builder = QueryBuilder::new();

    if !expr.projection.is_empty() {
        for projection in expr.projection.iter() {
            // println!("{:?}", projection);

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
                    compile_select_expr(expr, ctx, &mut builder, Some(alias.to_string()))?
                }
                _ => {
                    return Err(CompilationError::Unsupported(format!(
                        "Unsupported projection: {:?}",
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

    pub fn plan(
        &self,
        stmt: &ast::Statement,
        props: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        let (query, select) = match stmt {
            ast::Statement::Query(q) => {
                if q.with.is_some() {
                    return Err(CompilationError::Unsupported(
                        "Query with CTE instruction(s)".to_string(),
                    ));
                }

                match &q.body {
                    sqlparser::ast::SetExpr::Select(select) => (q, select),
                    _ => {
                        return Err(CompilationError::Unsupported(
                            "Unsupported Query".to_string(),
                        ));
                    }
                }
            }
            ast::Statement::SetVariable { .. } => {
                return Ok(QueryPlan::Meta(Arc::new(dataframe::DataFrame::new(
                    vec![],
                    vec![],
                ))));
            }
            ast::Statement::ShowVariable { variable } => {
                return self.show_variable_to_plan(variable, props);
            }
            // Proxy some queries to DF
            ast::Statement::ShowColumns { .. } => {
                return self.create_df_logical_plan(stmt.clone(), props);
            }
            _ => {
                return Err(CompilationError::Unsupported(
                    "Unsupported query type".to_string(),
                ));
            }
        };

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

        let from_table = if select.from.len() == 1 {
            if !select.from[0].joins.is_empty() {
                return Err(CompilationError::Unsupported(
                    "Query with JOIN instruction(s)".to_string(),
                ));
            }

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

        if schema_name.to_lowercase() == "information_schema" {
            return self.create_df_logical_plan(stmt.clone(), props);
        }

        if schema_name.to_lowercase() != "db" {
            return Err(CompilationError::Unsupported(format!(
                "Unable to access schema {}",
                schema_name
            )));
        }

        if let Some(cube) = self.context.find_cube_with_name(table_name.clone()) {
            // println!("{:?}", select.projection);
            let mut ctx = QueryContext::new(&cube);
            let mut builder = compile_select(select, &mut ctx)?;

            if let Some(limit_expr) = &query.limit {
                let limit = limit_expr.to_string().parse::<i32>().map_err(|e| {
                    CompilationError::Unsupported(format!(
                        "Unable to parse limit: {}",
                        e.to_string()
                    ))
                })?;

                builder.with_limit(limit);
            }

            if let Some(offset_expr) = &query.offset {
                let offset = offset_expr.value.to_string().parse::<i32>().map_err(|e| {
                    CompilationError::Unsupported(format!(
                        "Unable to parse offset: {}",
                        e.to_string()
                    ))
                })?;

                builder.with_offset(offset);
            }

            compile_group(&select.group_by, &ctx, &mut builder)?;
            compile_order(&query.order_by, &ctx, &mut builder)?;

            if let Some(selection) = &select.selection {
                compile_where(selection, &ctx, &mut builder)?;
            }

            Ok(QueryPlan::CubeSelect(builder.build()))
        } else {
            return Err(CompilationError::Unknown(format!(
                "Unknown cube: {}",
                table_name
            )));
        }
    }

    fn show_variable_to_plan(
        &self,
        variable: &Vec<Ident>,
        props: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        let name = ObjectName(variable.to_vec()).to_string();
        if name.eq_ignore_ascii_case("databases") || name.eq_ignore_ascii_case("schemas") {
            Ok(QueryPlan::Meta(Arc::new(dataframe::DataFrame::new(
                vec![dataframe::Column::new(
                    "Database".to_string(),
                    ColumnType::MYSQL_TYPE_STRING,
                )],
                vec![
                    dataframe::Row::new(vec![dataframe::TableValue::String("db".to_string())]),
                    dataframe::Row::new(vec![dataframe::TableValue::String(
                        "information_schema".to_string(),
                    )]),
                    dataframe::Row::new(vec![dataframe::TableValue::String("mysql".to_string())]),
                    dataframe::Row::new(vec![dataframe::TableValue::String(
                        "performance_schema".to_string(),
                    )]),
                    dataframe::Row::new(vec![dataframe::TableValue::String("sys".to_string())]),
                ],
            ))))
        } else if name.eq_ignore_ascii_case("warnings") {
            Ok(QueryPlan::Meta(Arc::new(dataframe::DataFrame::new(
                vec![
                    dataframe::Column::new("Level".to_string(), ColumnType::MYSQL_TYPE_STRING),
                    dataframe::Column::new("Code".to_string(), ColumnType::MYSQL_TYPE_LONGLONG),
                    dataframe::Column::new("Message".to_string(), ColumnType::MYSQL_TYPE_STRING),
                ],
                vec![],
            ))))
        } else if name.eq_ignore_ascii_case("variables like 'aurora\\_version'") {
            Ok(QueryPlan::Meta(Arc::new(dataframe::DataFrame::new(
                vec![
                    dataframe::Column::new(
                        "Variable_name".to_string(),
                        ColumnType::MYSQL_TYPE_STRING,
                    ),
                    dataframe::Column::new("Value".to_string(), ColumnType::MYSQL_TYPE_LONGLONG),
                ],
                vec![dataframe::Row::new(vec![])],
            ))))
        } else {
            self.create_df_logical_plan(
                ast::Statement::ShowVariable {
                    variable: variable.clone(),
                },
                props,
            )
        }
    }

    fn create_df_logical_plan(
        &self,
        stmt: ast::Statement,
        props: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        let mut ctx =
            ExecutionContext::with_config(ExecutionConfig::new().with_information_schema(true));

        let variable_provider = SystemVar::new();
        ctx.register_variable(VarType::System, Arc::new(variable_provider));

        ctx.register_udf(create_version_udf());
        ctx.register_udf(create_db_udf("database".to_string(), props));
        ctx.register_udf(create_db_udf("schema".to_string(), props));
        ctx.register_udf(create_connection_id_udf(props));
        ctx.register_udf(create_user_udf(props));
        ctx.register_udf(create_current_user_udf(props));

        {
            let schema_provider = MemorySchemaProvider::new();

            for cube in &self.context.cubes {
                let mut schema_fields = vec![];

                for column in cube.get_columns() {
                    let data_type = match column.mysql_type_as_str().as_str() {
                        "int" => DataType::Int64,
                        "time" => DataType::Timestamp(TimeUnit::Nanosecond, None),
                        _ => DataType::Utf8,
                    };

                    schema_fields.push(Field::new(
                        column.get_name(),
                        data_type,
                        column.mysql_can_be_null(),
                    ));
                }

                let schema = Arc::new(Schema::new(schema_fields));
                let provider = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();

                schema_provider
                    .register_table(cube.name.clone(), Arc::new(provider))
                    .map_err(|err| {
                        CompilationError::Internal(format!(
                            "Unable to register table provider for {}: {}",
                            cube.name.clone(),
                            err
                        ))
                    })?;
            }

            let catalog_provider = MemoryCatalogProvider::new();
            catalog_provider.register_schema("db", Arc::new(schema_provider));

            ctx.register_catalog("db", Arc::new(catalog_provider));
        }

        let state = ctx.state.lock().unwrap().clone();
        let df_query_planner = SqlToRel::new(&state);

        let plan = df_query_planner
            .statement_to_plan(&DFStatement::Statement(stmt))
            .map_err(|err| {
                CompilationError::Internal(format!("Initial planning error: {}", err))
            })?;

        let optimized_plan = ctx.optimize(&plan).map_err(|err| {
            CompilationError::Internal(format!("Planning optimization error: {}", err))
        })?;

        Ok(QueryPlan::DataFushionSelect(optimized_plan, ctx))
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
    // Query will not be executed, we already knows how respond to it
    Meta(Arc<dataframe::DataFrame>),
    // Query will be executed via Data Fusion
    DataFushionSelect(LogicalPlan, ExecutionContext),
    // Query will be executed by direct request in Cube.js
    CubeSelect(CompiledQuery),
}

impl QueryPlan {
    pub fn print(&self, pretty: bool) -> Result<String, CubeError> {
        match self {
            QueryPlan::DataFushionSelect(plan, _) => {
                if pretty {
                    Ok(plan.display_indent().to_string())
                } else {
                    Ok(plan.display().to_string())
                }
            }
            QueryPlan::CubeSelect(compiled_query) => {
                if pretty {
                    Ok(serde_json::to_string_pretty(&compiled_query)?)
                } else {
                    Ok(serde_json::to_string(&compiled_query)?)
                }
            }
            QueryPlan::Meta(_) => Ok(
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
    let stmt = parse_sql_to_statement(query)?;
    convert_statement_to_cube_query(&stmt, tenant, props)
}

#[cfg(test)]
mod tests {
    use cubeclient::models::{
        V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment,
    };

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
            QueryPlan::CubeSelect(query) => query,
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
    fn test_order_alias_for_dimension_default() {
        let query = convert_simple_select(
            "SELECT taxful_total_price as total_price FROM KibanaSampleDataEcommerce ORDER BY total_price"
                .to_string(),
        );

        assert_eq!(
            query.request,
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
        )
    }

    #[test]
    fn test_order_indentifier_default() {
        let query = convert_simple_select(
            "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price"
                .to_string(),
        );

        assert_eq!(
            query.request,
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
        )
    }

    #[test]
    fn test_order_indentifier_asc() {
        let query = convert_simple_select(
            "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price ASC".to_string(),
        );

        assert_eq!(
            query.request,
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
        )
    }

    #[test]
    fn test_order_indentifier_desc() {
        let query = convert_simple_select(
            "SELECT taxful_total_price FROM KibanaSampleDataEcommerce ORDER BY taxful_total_price DESC".to_string(),
        );

        assert_eq!(
            query.request,
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
                CompilationError::User("Unable to use * as argument to aggregation function (only count supported)".to_string()),
            ),
            (
                "SELECT MAX(order_date) FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::User("Unable to use dimension order_date as measure in aggregation function MAX(order_date)".to_string()),
            ),
            (
                "SELECT MAX(minPrice) FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::User("Unable to use measure minPrice of type min as argument in aggregate function MAX(). Aggregate function must match the type of measure.".to_string()),
            ),
            // Check restrictions for segments usage
            (
                "SELECT is_male FROM KibanaSampleDataEcommerce".to_string(),
                CompilationError::User("Unable to use segment is_male as column in SELECT".to_string()),
            ),
            (
                "SELECT COUNT(*) FROM KibanaSampleDataEcommerce GROUP BY is_male".to_string(),
                CompilationError::User("Unable to use segment is_male in GROUP BY".to_string()),
            ),
            (
                "SELECT COUNT(*) FROM KibanaSampleDataEcommerce ORDER BY is_male DESC".to_string(),
                CompilationError::User("Unable to use segment is_male in ORDER BY".to_string()),
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
            ["DATE(order_date)".to_string(), "day".to_string()],
            // With escaping by `
            ["DATE(`order_date`)".to_string(), "day".to_string()],
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
            // // Filter push down to TD (day)
            (
                "COUNT(*), DATE(order_date) AS __timestamp".to_string(),
                "order_date >= STR_TO_DATE('2021-08-31 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f') AND order_date < STR_TO_DATE('2021-09-07 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f')".to_string(),
                Some(vec![V1LoadRequestQueryTimeDimension {
                    dimension: "KibanaSampleDataEcommerce.order_date".to_string(),
                    granularity: Some("day".to_string()),
                    date_range: Some(json!(vec![
                        "2021-08-31T00:00:00+00:00".to_string(),
                        "2021-09-06T23:59:59.999+00:00".to_string()
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
                        "2021-08-31T00:00:00+00:00".to_string(),
                        "2021-09-06T23:59:59.999+00:00".to_string()
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
                        "2021-08-31T00:00:00+00:00".to_string(),
                        "2021-09-06T23:59:59.999+00:00".to_string()
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
                        "2021-08-31T00:00:00+00:00".to_string(),
                        "2021-09-06T23:59:59.999+00:00".to_string()
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
                        values: Some(vec!["2021-08-31T00:00:00+00:00".to_string()]),
                        or: None,
                        and: None,
                    }),
                    json!(V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("beforeDate".to_string()),
                        values: Some(vec!["2021-09-06T23:59:59.999+00:00".to_string()]),
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
            ),
            (
                "taxful_total_price > -1".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some("gt".to_string()),
                    values: Some(vec!["-1".to_string()]),
                    or: None,
                    and: None,
                }]),
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
            ),
            // BETWEEN
            (
                "order_date BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
                None,
                // This filter will be pushed to time_dimension
                // V1LoadRequestQueryFilterItem {
                //     member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                //     operator: Some("inDateRange".to_string()),
                //     values: Some(vec!["2021-08-31".to_string(), "2021-09-07".to_string()]),
                //     or: None,
                //     and: None,
                // },
            ),
            (
                "order_date NOT BETWEEN '2021-08-31' AND '2021-09-07'".to_string(),
                Some(vec![V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                    operator: Some("notInDateRange".to_string()),
                    values: Some(vec![
                        "2021-08-31T00:00:00+00:00".to_string(),
                        "2021-09-07T00:00:00+00:00".to_string(),
                    ]),
                    or: None,
                    and: None,
                }]),
            ),
            // SIMILAR as BETWEEN but manually
            (
                "order_date >= '2021-08-31' AND order_date < '2021-09-07'".to_string(),
                None,
                // This filter will be pushed to time_dimension
                // Some(vec![V1LoadRequestQueryFilterItem {
                //     member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                //     operator: Some("inDateRange".to_string()),
                //     values: Some(vec!["2021-08-31".to_string(), "2021-09-07".to_string()]),
                //     or: None,
                //     and: None,
                // }]),
            ),
            //  SIMILAR as BETWEEN but without -1 nanosecond because <=
            (
                "order_date >= '2021-08-31' AND order_date <= '2021-09-07'".to_string(),
                Some(vec![
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("afterDate".to_string()),
                        values: Some(vec!["2021-08-31T00:00:00+00:00".to_string()]),
                        or: None,
                        and: None,
                    },
                    V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                        operator: Some("lte".to_string()),
                        values: Some(vec!["2021-09-07".to_string()]),
                        or: None,
                        and: None,
                    },
                ]),
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
            ),
            // Segment
            (
                "is_male = true".to_string(),
                // This filter will be pushed to segments
                None,
            ),
            (
                "is_male = true AND is_female = true".to_string(),
                // This filters will be pushed to segments
                None,
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

            assert_eq!(query.request.filters, *expected_fitler)
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
                "order_date < 'WRONG_DATE'".to_string(),
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
                Err(e) => assert_eq!(e, expected_error),
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
    fn test_str_literal_to_date() {
        let d = CompiledExpression::StringLiteral("2021-08-31".to_string())
            .to_date()
            .unwrap();
        assert_eq!(d.to_rfc3339(), "2021-08-31T00:00:00+00:00".to_string());

        let d = CompiledExpression::StringLiteral("2021-08-31 00:00:00.000000".to_string())
            .to_date()
            .unwrap();
        assert_eq!(d.to_rfc3339(), "2021-08-31T00:00:00+00:00".to_string());

        let d = CompiledExpression::StringLiteral("2021-08-31T00:00:00+00:00".to_string())
            .to_date()
            .unwrap();
        assert_eq!(d.to_rfc3339(), "2021-08-31T00:00:00+00:00".to_string());
    }
}
