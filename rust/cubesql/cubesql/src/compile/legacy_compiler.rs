use crate::{
    compile::{
        builder::{CompiledQueryFieldMeta, QueryBuilder},
        context::{QueryContext, Selection},
        error::CompilationError,
        CompilationResult,
    },
    sql::ColumnType,
    transport::{V1CubeMetaDimensionExt, V1CubeMetaMeasureExt, V1CubeMetaSegmentExt},
};
use chrono::{DateTime, Datelike, Duration, NaiveDate, SecondsFormat, TimeZone, Utc};
use cubeclient::models::{V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension};
use log::{debug, trace};
use serde_json::json;
use sqlparser::ast;

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
                        Utc.with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
                            .unwrap(),
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

                match leading_field.clone().unwrap_or(ast::DateTimeField::Second) {
                    ast::DateTimeField::Second => {
                        interval.seconds = interval_value;
                    }
                    ast::DateTimeField::Minute => {
                        interval.minutes = interval_value;
                    }
                    ast::DateTimeField::Hour => {
                        interval.hours = interval_value;
                    }
                    ast::DateTimeField::Day => {
                        interval.days = interval_value;
                    }
                    ast::DateTimeField::Month => {
                        interval.months = interval_value;
                    }
                    ast::DateTimeField::Year => {
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
                ast::BinaryOperator::Eq => (filter_expr, "equals".to_string()),
                ast::BinaryOperator::NotEq => (filter_expr, "notEquals".to_string()),
                ast::BinaryOperator::GtEq => match filter_expr {
                    CompiledExpression::DateLiteral(_) => {
                        (filter_expr, "afterOrOnDate".to_string())
                    }
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
                    CompiledExpression::DateLiteral(_) => {
                        (filter_expr, "beforeOrOnDate".to_string())
                    }
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

fn compiled_like_expr(
    negated: bool,
    expr: &Box<ast::Expr>,
    pattern: &Box<ast::Expr>,
    ctx: &QueryContext,
) -> CompilationResult<CompiledFilterTree> {
    let expr_ce = compile_expression(expr, ctx)?;
    let pattern_ce = compile_expression(pattern, ctx)?;

    // Group selection to left, expr for filtering to right
    let (selection_to_filter, filter_expr) = match (expr_ce, pattern_ce) {
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
            let (value, operator) = match negated {
                true => (filter_expr, "notContains".to_string()),
                false => (filter_expr, "contains".to_string()),
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

            let (value, operator) = match negated {
                true => (filter_expr, "notContains".to_string()),
                false => (filter_expr, "contains".to_string()),
            };

            CompiledFilter::Filter {
                member,
                operator,
                values: Some(vec![value.to_value_as_str()?]),
            }
        }
        // Compile to CompiledFilter::SegmentFilter (it will be pushed to segments via optimization)
        Selection::Segment(_) => {
            return Err(CompilationError::user(format!(
                "Unable to use LIKE with segment: {} {}LIKE {}",
                expr,
                if negated { "NOT " } else { "" },
                pattern
            )))
        }
        _ => {
            return Err(CompilationError::unsupported(format!(
                "Binary expression: {} {} {}",
                expr,
                if negated { "NOT " } else { "" },
                pattern
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
                                && r_op.eq(&"beforeDate".to_string()))
                            || (l_op.eq(&"afterOrOnDate".to_string())
                                && r_op.eq(&"beforeOrOnDate".to_string()))
                            || (l_op.eq(&"beforeOrOnDate".to_string())
                                && r_op.eq(&"afterOrOnDate".to_string())))
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
        binary @ ast::Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            if escape_char.is_some() {
                return Err(CompilationError::user(format!(
                    "LIKE doesn't support ESCAPE: {:?}",
                    binary
                )));
            }
            compiled_like_expr(*negated, expr, pattern, ctx)
        }
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

pub fn compile_group(
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

pub fn compile_where(
    selection: &ast::Expr,
    ctx: &QueryContext,
    builder: &mut QueryBuilder,
) -> CompilationResult<()> {
    let filters = match &selection {
        binary @ ast::Expr::BinaryOp { left, right, op } => match op {
            ast::BinaryOperator::Lt
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
        binary @ ast::Expr::Like { .. } => compile_where_expression(binary, ctx)?,
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

pub fn compile_order(
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

pub fn compile_select(
    expr: &ast::Select,
    ctx: &mut QueryContext,
) -> CompilationResult<QueryBuilder> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compile::{parser::parse_sql_to_statement, test::get_test_meta},
        sql::session::DatabaseProtocol,
    };

    fn parse_expr_from_projection(query: &String, db: DatabaseProtocol) -> ast::Expr {
        let stmt = parse_sql_to_statement(&query, db, &mut None).unwrap();
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
}
