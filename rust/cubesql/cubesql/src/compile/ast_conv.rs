use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    iter::{self, Peekable},
    str::Chars,
    sync::Arc,
};

use cubeclient::models::{V1CubeMetaDimension, V1CubeMetaMeasure, V1LoadRequestQueryFilterItem};
use datafusion::{
    error::{DataFusionError, Result as DFResult},
    logical_plan::{LogicalPlan, PlanVisitor},
};
use sqlparser::{ast, dialect::PostgreSqlDialect, parser::Parser};

use crate::{
    compile::{
        convert_sql_to_cube_query,
        engine::df::{
            scan::CubeScanNode,
            wrapper::{CubeScanWrappedSqlNode, CubeScanWrapperNode},
        },
        CompilationError,
    },
    sql::Session,
    transport::MetaContext,
    CubeError,
};

#[derive(Debug)]
enum ModifyAction {
    Add(V1LoadRequestQueryFilterItem),
    Remove(V1LoadRequestQueryFilterItem),
    Replace {
        old: V1LoadRequestQueryFilterItem,
        new: V1LoadRequestQueryFilterItem,
    },
}

impl ModifyAction {
    fn get_cube_and_member_name(
        filter: &V1LoadRequestQueryFilterItem,
    ) -> DFResult<(String, String)> {
        let member_name = filter
            .member
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("Filter must have a member".to_string()))?;
        let (cube, member) = member_name
            .split_once('.')
            .ok_or_else(|| DataFusionError::NotImplemented("Invalid member format".to_string()))?;
        Ok((cube.to_string(), member.to_string()))
    }

    /// Builds the comparison expression for a plain (non-group) filter.
    fn leaf_expr(
        filter: &V1LoadRequestQueryFilterItem,
        source: &MemberSource,
        meta_member: &MetaMember,
    ) -> DFResult<ast::Expr> {
        let expr = match source {
            MemberSource::CubeTable { relation_alias } => {
                let column_ident = ast::Ident::with_quote('"', meta_member.short_name());
                let column_expr =
                    ast::Expr::CompoundIdentifier(vec![relation_alias.clone(), column_ident]);
                match meta_member {
                    MetaMember::Dimension(_) => column_expr,
                    MetaMember::Measure(measure) => {
                        // Distinct aggregations come camelCase from the meta
                        // API and snake_case elsewhere in the transport, so
                        // both spell them; `countDistinctApprox` has no SQL form.
                        let func_name = match measure.agg_type.as_deref() {
                            Some("count") => "COUNT",
                            Some("count_distinct" | "countDistinct") => "COUNT",
                            Some("sum") => "SUM",
                            Some("avg") => "AVG",
                            Some("min") => "MIN",
                            Some("max") => "MAX",
                            _ => "MEASURE",
                        };
                        let distinct = matches!(
                            measure.agg_type.as_deref(),
                            Some("count_distinct" | "countDistinct")
                        );
                        ast::Expr::Function(ast::Function {
                            name: ast::ObjectName::from(vec![ast::Ident::new(func_name)]),
                            uses_odbc_syntax: false,
                            parameters: ast::FunctionArguments::None,
                            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                                duplicate_treatment: distinct
                                    .then_some(ast::DuplicateTreatment::Distinct),
                                args: vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                    column_expr,
                                ))],
                                clauses: vec![],
                            }),
                            filter: None,
                            null_treatment: None,
                            over: None,
                            within_group: vec![],
                            approximate: false,
                        })
                    }
                }
            }
            // The member is already computed as an output column of a derived
            // table / CTE reference, so it is referenced as a plain column
            MemberSource::DerivedColumn {
                relation_alias,
                column,
            } => ast::Expr::CompoundIdentifier(vec![relation_alias.clone(), column.clone()]),
        };

        match filter.operator.as_deref() {
            Some(op_name @ ("equals" | "notEquals")) => {
                let negated = op_name == "notEquals";
                let Some(values) = &filter.values else {
                    return Err(DataFusionError::Plan(format!(
                        "Filter values are required for \"{}\" operator",
                        op_name
                    )));
                };
                let values = values
                    .iter()
                    .map(|v| Self::value_to_expr_by_member_type(v, meta_member))
                    .collect::<DFResult<Vec<_>>>()?;
                match &values[..] {
                    [] => Err(DataFusionError::Plan(format!(
                        "At least one filter value is required for \"{}\" operator",
                        op_name
                    ))),
                    [value] => Ok(ast::Expr::BinaryOp {
                        left: Box::new(expr),
                        op: if negated {
                            ast::BinaryOperator::NotEq
                        } else {
                            ast::BinaryOperator::Eq
                        },
                        right: Box::new(value.clone()),
                    }),
                    _ => Ok(ast::Expr::InList {
                        expr: Box::new(expr),
                        list: values,
                        negated,
                    }),
                }
            }
            Some("set") => Ok(ast::Expr::IsNotNull(Box::new(expr))),
            Some("notSet") => Ok(ast::Expr::IsNull(Box::new(expr))),
            Some("inDateRange") => Self::date_range_expr(
                expr,
                filter,
                "inDateRange",
                ast::BinaryOperator::GtEq,
                ast::BinaryOperator::LtEq,
                ast::BinaryOperator::And,
            ),
            Some("beforeDate") => Self::single_string_cmp_expr(
                expr,
                filter.values.as_deref(),
                "beforeDate",
                ast::BinaryOperator::Lt,
            ),
            Some("beforeOrOnDate") => Self::single_string_cmp_expr(
                expr,
                filter.values.as_deref(),
                "beforeOrOnDate",
                ast::BinaryOperator::LtEq,
            ),
            Some("afterDate") => Self::single_string_cmp_expr(
                expr,
                filter.values.as_deref(),
                "afterDate",
                ast::BinaryOperator::Gt,
            ),
            Some("afterOrOnDate") => Self::single_string_cmp_expr(
                expr,
                filter.values.as_deref(),
                "afterOrOnDate",
                ast::BinaryOperator::GtEq,
            ),
            // A negated range is only recognized as `notInDateRange` when it
            // is written as NOT BETWEEN; the equivalent disjunction of
            // comparisons comes back as two separate bounds instead
            Some("notInDateRange") => Self::between_expr(expr, filter, "notInDateRange", true),
            Some("gt") => Self::numeric_cmp_expr(
                expr,
                filter.values.as_deref(),
                "gt",
                ast::BinaryOperator::Gt,
            ),
            Some("gte") => Self::numeric_cmp_expr(
                expr,
                filter.values.as_deref(),
                "gte",
                ast::BinaryOperator::GtEq,
            ),
            Some("lt") => Self::numeric_cmp_expr(
                expr,
                filter.values.as_deref(),
                "lt",
                ast::BinaryOperator::Lt,
            ),
            Some("lte") => Self::numeric_cmp_expr(
                expr,
                filter.values.as_deref(),
                "lte",
                ast::BinaryOperator::LtEq,
            ),
            Some("contains") => {
                Self::like_family_expr(&expr, filter, "contains", LikeShape::Contains, false)
            }
            Some("notContains") => {
                Self::like_family_expr(&expr, filter, "notContains", LikeShape::Contains, true)
            }
            Some("startsWith") => {
                Self::like_family_expr(&expr, filter, "startsWith", LikeShape::StartsWith, false)
            }
            Some("notStartsWith") => {
                Self::like_family_expr(&expr, filter, "notStartsWith", LikeShape::StartsWith, true)
            }
            Some("endsWith") => {
                Self::like_family_expr(&expr, filter, "endsWith", LikeShape::EndsWith, false)
            }
            Some("notEndsWith") => {
                Self::like_family_expr(&expr, filter, "notEndsWith", LikeShape::EndsWith, true)
            }
            _ => Err(DataFusionError::Plan(format!(
                "Unsupported filter operator: {:?}",
                filter.operator
            ))),
        }
    }

    fn numeric_cmp_expr(
        column_expr: ast::Expr,
        values: Option<&[String]>,
        op_name: &str,
        op: ast::BinaryOperator,
    ) -> DFResult<ast::Expr> {
        let value = Self::single_numeric_value(values, op_name)?;
        Ok(ast::Expr::BinaryOp {
            left: Box::new(column_expr),
            op,
            right: Box::new(Self::numeric_value_expr(value)?),
        })
    }

    /// Numeric literals are rendered verbatim, so the value must be validated
    /// to be a plain SQL numeric literal before it is placed into the AST.
    /// `f64` parsing alone would admit `inf`/`NaN` and surrounding whitespace,
    /// hence the character check.
    fn numeric_value_expr(value: &str) -> DFResult<ast::Expr> {
        if !Self::is_numeric_literal(value) {
            return Err(DataFusionError::Plan(format!(
                "Filter value must be numeric, got {:?}",
                value
            )));
        }
        Ok(ast::Expr::Value(
            ast::Value::Number(value.to_string(), false).into(),
        ))
    }

    fn is_numeric_literal(value: &str) -> bool {
        !value.is_empty()
            && value
                .chars()
                .all(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E'))
            && value.parse::<f64>().is_ok_and(|value| value.is_finite())
    }

    fn single_numeric_value<'a>(values: Option<&'a [String]>, op_name: &str) -> DFResult<&'a str> {
        let Some(values) = values else {
            return Err(DataFusionError::Plan(format!(
                "Filter values are required for \"{}\" operator",
                op_name
            )));
        };
        if values.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "Exactly one filter value is required for \"{}\" operator",
                op_name
            )));
        }
        let value = values[0].as_str();
        if !Self::is_numeric_literal(value) {
            return Err(DataFusionError::Plan(format!(
                "Filter value for \"{}\" operator must be numeric, got {:?}",
                op_name, value
            )));
        }
        Ok(value)
    }

    fn single_string_cmp_expr(
        column_expr: ast::Expr,
        values: Option<&[String]>,
        op_name: &str,
        op: ast::BinaryOperator,
    ) -> DFResult<ast::Expr> {
        let Some(values) = values else {
            return Err(DataFusionError::Plan(format!(
                "Filter values are required for \"{}\" operator",
                op_name
            )));
        };
        if values.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "Exactly one filter value is required for \"{}\" operator",
                op_name
            )));
        }
        Ok(ast::Expr::BinaryOp {
            left: Box::new(column_expr),
            op,
            right: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(values[0].clone()).into(),
            )),
        })
    }

    fn between_expr(
        column_expr: ast::Expr,
        filter: &V1LoadRequestQueryFilterItem,
        op_name: &str,
        negated: bool,
    ) -> DFResult<ast::Expr> {
        let values = Self::two_values(filter, op_name)?;
        Ok(ast::Expr::Between {
            expr: Box::new(column_expr),
            negated,
            low: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(values[0].clone()).into(),
            )),
            high: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(date_range_upper(&values[1])).into(),
            )),
        })
    }

    fn two_values<'a>(
        filter: &'a V1LoadRequestQueryFilterItem,
        op_name: &str,
    ) -> DFResult<&'a [String]> {
        let Some(values) = &filter.values else {
            return Err(DataFusionError::Plan(format!(
                "Filter values are required for \"{}\" operator",
                op_name
            )));
        };
        if values.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "Exactly two filter values are required for \"{}\" operator",
                op_name
            )));
        }
        Ok(values)
    }

    fn date_range_expr(
        column_expr: ast::Expr,
        filter: &V1LoadRequestQueryFilterItem,
        op_name: &str,
        lower_op: ast::BinaryOperator,
        upper_op: ast::BinaryOperator,
        join_op: ast::BinaryOperator,
    ) -> DFResult<ast::Expr> {
        let values = Self::two_values(filter, op_name)?;
        let lower = ast::Expr::BinaryOp {
            left: Box::new(column_expr.clone()),
            op: lower_op,
            right: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(values[0].clone()).into(),
            )),
        };
        let upper = ast::Expr::BinaryOp {
            left: Box::new(column_expr),
            op: upper_op,
            right: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(date_range_upper(&values[1])).into(),
            )),
        };
        Ok(ast::Expr::Nested(Box::new(ast::Expr::BinaryOp {
            left: Box::new(lower),
            op: join_op,
            right: Box::new(upper),
        })))
    }

    fn multi_value_join<F>(
        values: Option<&[String]>,
        op_name: &str,
        negated: bool,
        mut make_expr: F,
    ) -> DFResult<ast::Expr>
    where
        F: FnMut(&str) -> ast::Expr,
    {
        let Some(values) = values else {
            return Err(DataFusionError::Plan(format!(
                "Filter values are required for \"{}\" operator",
                op_name
            )));
        };
        if values.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "At least one filter value is required for \"{}\" operator",
                op_name
            )));
        }
        let join_op = if negated {
            ast::BinaryOperator::And
        } else {
            ast::BinaryOperator::Or
        };
        let mut value_exprs = values.iter().map(|v| make_expr(v.as_str()));
        let first = value_exprs.next().unwrap();
        let combined = value_exprs.fold(first, |acc, e| ast::Expr::BinaryOp {
            left: Box::new(acc),
            op: join_op.clone(),
            right: Box::new(e),
        });
        Ok(if values.len() > 1 {
            ast::Expr::Nested(Box::new(combined))
        } else {
            combined
        })
    }

    fn like_family_expr(
        column_expr: &ast::Expr,
        filter: &V1LoadRequestQueryFilterItem,
        op_name: &str,
        shape: LikeShape,
        negated: bool,
    ) -> DFResult<ast::Expr> {
        Self::multi_value_join(filter.values.as_deref(), op_name, negated, |v| {
            // Backslash is the escape character the LIKE pattern parser of the
            // filter rewrite rules assumes, so no ESCAPE clause: one would take
            // a rewrite path that produces no Cube filter
            let escaped = v
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            let pattern = match shape {
                LikeShape::Contains => format!("%{}%", escaped),
                LikeShape::StartsWith => format!("{}%", escaped),
                LikeShape::EndsWith => format!("%{}", escaped),
            };
            ast::Expr::ILike {
                negated,
                any: false,
                expr: Box::new(column_expr.clone()),
                pattern: Box::new(ast::Expr::Value(
                    ast::Value::SingleQuotedString(pattern).into(),
                )),
                escape_char: None,
            }
        })
    }

    fn value_to_expr_by_member_type(value: &str, meta_member: &MetaMember) -> DFResult<ast::Expr> {
        let kind = match meta_member {
            MetaMember::Dimension(dimension) => match dimension.r#type.as_str() {
                "number" => ValueKind::Numeric,
                "boolean" => ValueKind::Boolean,
                _ => ValueKind::String,
            },
            MetaMember::Measure(measure) => match measure.r#type.as_str() {
                "string" | "time" => ValueKind::String,
                "boolean" => ValueKind::Boolean,
                _ => ValueKind::Numeric,
            },
        };

        match kind {
            ValueKind::Numeric => Self::numeric_value_expr(value),
            // The filter rewrite rules read a boolean literal and render it
            // back as `true`/`false`, which is what the request carries
            ValueKind::Boolean => {
                let value = value.parse::<bool>().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "Filter value must be a boolean, got {:?}",
                        value
                    ))
                })?;
                Ok(ast::Expr::Value(ast::Value::Boolean(value).into()))
            }
            ValueKind::String => Ok(ast::Expr::Value(
                ast::Value::SingleQuotedString(value.to_string()).into(),
            )),
        }
    }
}

/// Aggregations a measure may be projected or filtered with.
const AGG_FUNCTIONS: [&str; 7] = ["COUNT", "SUM", "AVG", "MIN", "MAX", "MEASURE", "AGGREGATE"];

/// How a filter value is rendered, per the type of the member it filters.
#[derive(Debug, Clone, Copy)]
enum ValueKind {
    Numeric,
    Boolean,
    String,
}

#[derive(Debug, Clone, Copy)]
enum LikeShape {
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Debug)]
enum MetaMember {
    Dimension(V1CubeMetaDimension),
    Measure(V1CubeMetaMeasure),
}

impl MetaMember {
    fn get_from_ctx(ctx: &MetaContext, cube_name: &str, member_name: &str) -> DFResult<Self> {
        let full_member_name = format!("{}.{}", cube_name, member_name);
        if let Some(dimension) = ctx.find_dimension_with_name(&full_member_name) {
            return Ok(MetaMember::Dimension(dimension.clone()));
        }
        if let Some(measure) = ctx.find_measure_with_name(&full_member_name) {
            return Ok(MetaMember::Measure(measure.clone()));
        }
        Err(DataFusionError::Plan(format!(
            "Member \"{}\" not found in data model",
            full_member_name
        )))
    }

    /// Whether the member holds a time, which is where the planner
    /// canonicalizes the value a filter was written with.
    fn is_time(&self) -> bool {
        let kind = match self {
            MetaMember::Dimension(dimension) => &dimension.r#type,
            MetaMember::Measure(measure) => &measure.r#type,
        };
        kind == "time"
    }

    fn short_name(&self) -> String {
        let full_name = match self {
            MetaMember::Dimension(dimension) => &dimension.name,
            MetaMember::Measure(measure) => &measure.name,
        };
        full_name
            .split('.')
            .next_back()
            .unwrap_or(full_name)
            .to_string()
    }
}

/// Where the member column comes from in the outermost SELECT.
#[derive(Debug)]
enum MemberSource {
    /// The cube is referenced directly in the outermost FROM: dimensions are
    /// filtered in WHERE, measures are aggregated and filtered in HAVING.
    CubeTable { relation_alias: ast::Ident },
    /// The member is exposed as an output column of a derived table or a CTE
    /// reference in the outermost FROM: filtered as a plain column in WHERE.
    /// The column is carried as written, since quoting decides whether the
    /// identifier folds.
    DerivedColumn {
        relation_alias: ast::Ident,
        column: ast::Ident,
    },
}

fn parse_single_query(sql: &str) -> DFResult<Box<ast::Query>> {
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| DataFusionError::SQL(e))?;
    let mut ast_iter = ast.into_iter();
    let Some(statement) = ast_iter.next() else {
        return Err(DataFusionError::NotImplemented(
            "No SQL statement found".to_string(),
        ));
    };
    if ast_iter.next().is_some() {
        return Err(DataFusionError::NotImplemented(
            "Only one statement per input is supported".to_string(),
        ));
    }
    let ast::Statement::Query(query) = statement else {
        return Err(DataFusionError::NotImplemented(
            "Only SELECT statements are supported".to_string(),
        ));
    };
    Ok(query)
}

#[cfg(test)]
fn modify_sql_ast_many(
    sql: &str,
    actions: &[ModifyAction],
    ctx: &MetaContext,
    reported: &ReportedFilters,
) -> DFResult<(String, Vec<bool>)> {
    modify_parsed_query(sql, actions, ctx, reported)
}

/// The rewrite the endpoints use: a rewrite that fails is the caller's.
async fn rewrite_sql(
    sql: &str,
    actions: Vec<ModifyAction>,
    meta: Arc<MetaContext>,
    reported: ReportedFilters,
) -> Result<(String, Vec<bool>), CubeError> {
    modify_parsed_query(sql, &actions, &meta, &reported).map_err(|e| CubeError::user(e.to_string()))
}

fn modify_parsed_query(
    sql: &str,
    actions: &[ModifyAction],
    ctx: &MetaContext,
    reported: &ReportedFilters,
) -> DFResult<(String, Vec<bool>)> {
    let mut query = parse_single_query(sql)?;

    let mut applied = Vec::with_capacity(actions.len());
    let mut clause_keys = ClauseKeys::default();
    let mut clause_budget = ClauseBudget::of(outermost_select(&query)?)?;
    for action in actions {
        applied.push(apply_action_to_outermost_select(
            query.as_mut(),
            action,
            ctx,
            reported,
            &mut clause_keys,
            &mut clause_budget,
        )?);
    }

    let modified_sql = query.to_string();
    Ok((modified_sql, applied))
}

/// How many relations deep a chain of derived tables and CTE references is
/// followed when resolving a member; past it the member is unavailable. It
/// also refuses a self-referencing CTE.
const MAX_RELATION_DEPTH: usize = 25;

/// How deep a single expression is unwrapped: a group's parentheses, and the
/// truncations or extractions around the column a predicate filters.
const MAX_EXPR_NESTING: usize = 25;

/// The functions the engine evaluates when it plans, which a filter can be
/// written against: `CURRENT_DATE - INTERVAL '7 days'` is reported as the
/// date it came out to, so the SQL and the filter read back differ.
const PLANNING_TIME_FUNCTIONS: [&str; 7] = [
    "current_date",
    "current_timestamp",
    "localtimestamp",
    "now",
    "date_trunc",
    "to_date",
    "to_timestamp",
];

/// Whether the expression is a date the engine computes when it plans: only
/// literals, intervals and the functions above, no column reference anywhere.
/// Walked iteratively, with a node budget.
fn is_planning_time_date(expr: &ast::Expr) -> bool {
    let mut is_date = false;
    let mut pending = vec![expr];
    let mut visited = 0;

    while let Some(expr) = pending.pop() {
        visited += 1;
        if visited > MAX_EXPR_NODES {
            return false;
        }

        match expr {
            ast::Expr::Value(_) => {}
            ast::Expr::TypedString { .. } => is_date = true,
            ast::Expr::Interval(interval) => {
                is_date = true;
                pending.push(&interval.value);
            }
            ast::Expr::Nested(inner) | ast::Expr::Cast { expr: inner, .. } => pending.push(inner),
            ast::Expr::UnaryOp { op, expr: inner } => {
                if !matches!(op, ast::UnaryOperator::Minus | ast::UnaryOperator::Plus) {
                    return false;
                }
                pending.push(inner);
            }
            ast::Expr::BinaryOp { left, op, right } => {
                if !matches!(
                    op,
                    ast::BinaryOperator::Plus
                        | ast::BinaryOperator::Minus
                        | ast::BinaryOperator::Multiply
                        | ast::BinaryOperator::Divide
                ) {
                    return false;
                }
                pending.push(left);
                pending.push(right);
            }
            ast::Expr::Function(func) => {
                let ast::ObjectName(name_parts) = &func.name;
                let Some(name) = name_parts.last().and_then(|part| part.as_ident()) else {
                    return false;
                };
                if !PLANNING_TIME_FUNCTIONS
                    .iter()
                    .any(|known| name.value.eq_ignore_ascii_case(known))
                {
                    return false;
                }
                is_date = true;
                match &func.args {
                    ast::FunctionArguments::None => {}
                    ast::FunctionArguments::List(arg_list) => {
                        for arg in &arg_list.args {
                            let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)) = arg
                            else {
                                return false;
                            };
                            pending.push(arg);
                        }
                    }
                    _ => return false,
                }
            }
            _ => return false,
        }
    }

    is_date
}

/// The column a `DATE_TRUNC(unit, column)` truncates. The engine turns a
/// comparison against one into a range over the column itself, so it is that
/// column the predicate filters.
fn date_trunc_arg(func: &ast::Function) -> Option<&ast::Expr> {
    let ast::ObjectName(name_parts) = &func.name;
    let name = name_parts.last().and_then(|part| part.as_ident())?;
    if !name.value.eq_ignore_ascii_case("date_trunc") {
        return None;
    }
    let ast::FunctionArguments::List(arg_list) = &func.args else {
        return None;
    };
    let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(unit)), ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(column))] =
        &arg_list.args[..]
    else {
        return None;
    };
    is_literal(unit).then_some(column)
}

/// The column side of a predicate with the truncation or the extraction, if
/// any, peeled off. The engine turns a comparison against either into a range
/// over the column itself, so it is that column the predicate filters.
fn filter_column_expr(expr: &ast::Expr) -> &ast::Expr {
    let mut expr = expr;
    // A truncation of a truncation is still a filter on the innermost column,
    // and the loop keeps a nested one from costing a frame
    for _ in 0..MAX_EXPR_NESTING {
        match expr {
            ast::Expr::Function(func) => match date_trunc_arg(func) {
                Some(arg) => expr = arg,
                None => return expr,
            },
            ast::Expr::Extract { expr: inner, .. } => expr = inner,
            expr => return expr,
        }
    }
    expr
}

/// The value side of a filter predicate: a literal, or a date the engine
/// computes when it plans.
fn is_filter_value(expr: &ast::Expr) -> bool {
    is_literal(expr) || is_planning_time_date(expr)
}

fn is_literal(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Value(_) => true,
        // Negative numbers are parsed as a unary operation over a literal
        ast::Expr::UnaryOp { op, expr } => {
            matches!(op, ast::UnaryOperator::Minus | ast::UnaryOperator::Plus)
                && matches!(expr.as_ref(), ast::Expr::Value(_))
        }
        _ => false,
    }
}

/// Which clause of the outermost SELECT the filter belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ClauseKind {
    Where,
    Having,
}

fn clause_mut(select: &mut ast::Select, kind: ClauseKind) -> &mut Option<ast::Expr> {
    match kind {
        ClauseKind::Where => &mut select.selection,
        ClauseKind::Having => &mut select.having,
    }
}

/// Applies the action to the outermost SELECT only: the member must resolve
/// there, as a column of a cube in its FROM or as an output column a derived
/// table or CTE exposes directly. Computed columns do not qualify.
fn apply_action_to_outermost_select(
    query: &mut ast::Query,
    action: &ModifyAction,
    ctx: &MetaContext,
    reported: &ReportedFilters,
    clause_keys: &mut ClauseKeys,
    clause_budget: &mut ClauseBudget,
) -> DFResult<bool> {
    // `with` is only read, and it is a field of its own, so a split borrow
    // keeps the whole CTE list out of the per-action work
    let ast::Query { with, body, .. } = query;
    let with = with.as_ref();
    let ast::SetExpr::Select(select) = body.as_mut() else {
        return Err(DataFusionError::NotImplemented(
            "Only plain SELECT statements are supported at the outermost level".to_string(),
        ));
    };

    // A bare column can only be one relation's when the FROM holds one
    let single_relation = select.from.len() == 1 && select.from[0].joins.is_empty();
    let match_context = |filter: &V1LoadRequestQueryFilterItem, sole_reported| MatchContext {
        sole_reported,
        ignore_qualifier: single_relation,
        normalize_dates: filters_a_time_member(filter, ctx),
    };

    match action {
        ModifyAction::Add(filter) => {
            let (expr, kind) = require_filter_expr(filter, select, with, ctx)?;
            let match_ctx = match_context(filter, false);
            if clause_keys.holds(kind, match_ctx, clause_mut(select, kind), &expr) {
                return Ok(false);
            }
            clause_budget.charge(kind, &expr)?;
            clause_keys.note_appended(kind, &expr, single_relation);
            append_expr_to_clause(clause_mut(select, kind), expr);
            Ok(true)
        }
        ModifyAction::Remove(filter) => {
            // A filter whose member is not available in the outermost SELECT
            // can't be present in it either, so removal is a no-op
            let Some((expr, kind)) = resolve_filter_expr(filter, select, with, ctx)? else {
                return Ok(false);
            };
            let match_ctx = match_context(filter, reported.is_sole_on_member(filter));
            clause_keys.forget(kind);
            Ok(remove_expr_from_clause(clause_mut(select, kind), &expr, match_ctx) > 0)
        }
        ModifyAction::Replace { old, new } => {
            let Some((old_expr, old_kind)) = resolve_filter_expr(old, select, with, ctx)? else {
                return Ok(false);
            };
            let (new_expr, new_kind) = require_filter_expr(new, select, with, ctx)?;
            let match_ctx = match_context(old, reported.is_sole_on_member(old));
            // The new filter is nowhere in the query yet, so nothing about it
            // is reported; it is matched only against what the clause already
            // holds, under the member it stands on itself
            let new_match_ctx = match_context(new, false);
            clause_keys.forget(old_kind);
            // Charged whether it replaces in place or is appended: the count
            // only ever errs toward refusing
            clause_budget.charge(new_kind, &new_expr)?;
            if old_kind == new_kind {
                // Replace in place, preserving positions within the clause
                Ok(replace_expr_in_clause(
                    clause_mut(select, old_kind),
                    &old_expr,
                    new_expr,
                    match_ctx,
                    new_match_ctx,
                ) > 0)
            } else {
                // The filters belong to different clauses: remove, then add
                if remove_expr_from_clause(clause_mut(select, old_kind), &old_expr, match_ctx) == 0
                {
                    return Ok(false);
                }
                if !clause_keys.holds(
                    new_kind,
                    new_match_ctx,
                    clause_mut(select, new_kind),
                    &new_expr,
                ) {
                    clause_keys.note_appended(new_kind, &new_expr, single_relation);
                    append_expr_to_clause(clause_mut(select, new_kind), new_expr);
                }
                Ok(true)
            }
        }
    }
}

/// Same as [`resolve_filter_expr`], but a member that is not available in the
/// outermost SELECT is an error rather than `None`.
fn require_filter_expr(
    filter: &V1LoadRequestQueryFilterItem,
    select: &ast::Select,
    with: Option<&ast::With>,
    ctx: &MetaContext,
) -> DFResult<(ast::Expr, ClauseKind)> {
    resolve_filter_expr(filter, select, with, ctx)?.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Filter {} is not available in the outermost SELECT",
            filter_description(filter)
        ))
    })
}

/// Resolves a filter or an and/or filter group to the SQL expression to
/// filter with and the clause of the outermost SELECT it belongs to.
/// All filters of a group must belong to the same clause. Returns `None` if
/// a member of the filter is not available in the outermost SELECT.
fn resolve_filter_expr(
    filter: &V1LoadRequestQueryFilterItem,
    select: &ast::Select,
    with: Option<&ast::With>,
    ctx: &MetaContext,
) -> DFResult<Option<(ast::Expr, ClauseKind)>> {
    if filter.and.is_some() && filter.or.is_some() {
        return Err(DataFusionError::Plan(
            "Filter can't be both \"and\" and \"or\" group at once".to_string(),
        ));
    }

    let group = filter
        .and
        .as_ref()
        .map(|items| (items, ast::BinaryOperator::And))
        .or_else(|| {
            filter
                .or
                .as_ref()
                .map(|items| (items, ast::BinaryOperator::Or))
        });
    let Some((items, join_op)) = group else {
        // A plain filter
        let (cube_name, member_name) = ModifyAction::get_cube_and_member_name(filter)?;
        let meta_member = MetaMember::get_from_ctx(ctx, &cube_name, &member_name)?;
        let Some(source) =
            resolve_member_source(select, with, &cube_name, &member_name, &meta_member)
        else {
            return Ok(None);
        };
        let kind = match (&source, &meta_member) {
            (MemberSource::CubeTable { .. }, MetaMember::Measure(_)) => ClauseKind::Having,
            _ => ClauseKind::Where,
        };
        let expr = ModifyAction::leaf_expr(filter, &source, &meta_member)?;
        return Ok(Some((expr, kind)));
    };

    if filter.member.is_some() || filter.operator.is_some() || filter.values.is_some() {
        return Err(DataFusionError::Plan(
            "Filter group can't have member, operator or values".to_string(),
        ));
    }
    if items.is_empty() {
        return Err(DataFusionError::Plan(
            "Filter group must contain at least one filter".to_string(),
        ));
    }

    let mut exprs = Vec::with_capacity(items.len());
    let mut group_kind = None;
    for item in items {
        let item_filter: V1LoadRequestQueryFilterItem = serde_json::from_value(item.clone())
            .map_err(|e| {
                DataFusionError::Plan(format!("Invalid filter in a filter group: {}", e))
            })?;
        let Some((expr, kind)) = resolve_filter_expr(&item_filter, select, with, ctx)? else {
            return Ok(None);
        };
        match group_kind {
            None => group_kind = Some(kind),
            Some(group_kind) if group_kind != kind => {
                return Err(DataFusionError::Plan(
                    "Filter group can't mix dimension (WHERE) and measure (HAVING) filters"
                        .to_string(),
                ))
            }
            Some(_) => {}
        }
        exprs.push(expr);
    }

    let multiple = exprs.len() > 1;
    let mut exprs_iter = exprs.into_iter();
    let first = exprs_iter.next().unwrap();
    let combined = exprs_iter.fold(first, |acc, expr| ast::Expr::BinaryOp {
        left: Box::new(acc),
        op: join_op.clone(),
        right: Box::new(expr),
    });
    let expr = if multiple {
        ast::Expr::Nested(Box::new(combined))
    } else {
        combined
    };
    Ok(Some((expr, group_kind.unwrap())))
}

/// Resolves the member to a column available in the outermost SELECT.
/// Returns `None` if the member is not available there.
fn resolve_member_source(
    select: &ast::Select,
    with: Option<&ast::With>,
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
) -> Option<MemberSource> {
    let withs = with.into_iter().collect::<Vec<_>>();

    // Cube referenced directly in the outermost FROM
    if let Some(relation_alias) =
        alias_for_relation_in_from(cube_name, &select.from, &cte_names(&withs))
    {
        return Some(MemberSource::CubeTable { relation_alias });
    }

    // Derived tables and CTE references in the outermost FROM exposing the
    // member, directly or through relations of their own
    let budget = Cell::new(MAX_RELATION_EXPANSIONS);
    for table_with_joins in &select.from {
        let factors = iter::once(&table_with_joins.relation)
            .chain(table_with_joins.joins.iter().map(|join| &join.relation));
        for factor in factors {
            if let Some((relation_alias, column)) = member_column_in_table_factor(
                factor,
                &withs,
                cube_name,
                member_name,
                meta_member,
                0,
                &budget,
            ) {
                return Some(MemberSource::DerivedColumn {
                    relation_alias,
                    column,
                });
            }
        }
    }

    None
}

/// Names of the CTEs visible at a point. A relation with such a name refers
/// to the CTE, not to a cube of the same name.
fn cte_names(withs: &[&ast::With]) -> HashSet<String> {
    withs
        .iter()
        .flat_map(|with| with.cte_tables.iter())
        .map(|cte| cte.alias.name.value.to_ascii_lowercase())
        .collect()
}

/// Upper bound on the predicates a clause of the outermost SELECT may hold,
/// so that the walks over it are bounded work. An order of magnitude above
/// what [`MAX_FILTERS`] additions can build, so a query this API produced is
/// never one it then refuses.
const MAX_CLAUSE_PREDICATES: usize = 10_000;

/// Upper bound on the nodes one expression walk visits: a group needle holds
/// up to [`MAX_FILTERS`] leaves, so an expression can be as large as a clause
/// and shares its bound, named apart so such a walk reads as one.
const MAX_EXPR_NODES: usize = MAX_CLAUSE_PREDICATES;

/// Counts the predicates of a clause: its leaves under AND, OR and
/// parentheses. Iterative, as a clause may be as long as the bound allows.
fn clause_predicate_count(expr: &ast::Expr) -> usize {
    let mut count = 0;
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And | ast::BinaryOperator::Or,
                right,
            } => {
                pending.push(left);
                pending.push(right);
            }
            ast::Expr::Nested(inner) => pending.push(inner),
            _ => count += 1,
        }
    }
    count
}

/// The predicate counts of the outermost clauses, kept across a batch: the
/// bound is checked once on the query as parsed and then charged per append,
/// rather than recounted over the whole clause for every action.
struct ClauseBudget {
    counts: HashMap<ClauseKind, usize>,
}

impl ClauseBudget {
    /// Counts both clauses, refusing a SELECT already over the bound.
    fn of(select: &ast::Select) -> DFResult<Self> {
        let mut budget = Self {
            counts: HashMap::new(),
        };
        for (kind, clause) in [
            (ClauseKind::Where, select.selection.as_ref()),
            (ClauseKind::Having, select.having.as_ref()),
        ] {
            if let Some(clause) = clause {
                budget.counts.insert(kind, clause_predicate_count(clause));
                budget.assert_bounded(kind)?;
            }
        }
        Ok(budget)
    }

    /// Accounts for an expression about to be appended to the clause. A
    /// removal is not refunded: the count only ever errs toward refusing.
    fn charge(&mut self, kind: ClauseKind, expr: &ast::Expr) -> DFResult<()> {
        *self.counts.entry(kind).or_insert(0) += clause_predicate_count(expr);
        self.assert_bounded(kind)
    }

    fn assert_bounded(&self, kind: ClauseKind) -> DFResult<()> {
        let count = self.counts.get(&kind).copied().unwrap_or(0);
        if count > MAX_CLAUSE_PREDICATES {
            return Err(DataFusionError::Plan(format!(
                "A clause of the outermost SELECT has {} predicates, more than the {} supported",
                count, MAX_CLAUSE_PREDICATES
            )));
        }
        Ok(())
    }
}

/// The SELECT the actions apply to.
fn outermost_select(query: &ast::Query) -> DFResult<&ast::Select> {
    match query.body.as_ref() {
        ast::SetExpr::Select(select) => Ok(select),
        _ => Err(DataFusionError::NotImplemented(
            "Only plain SELECT statements are supported at the outermost level".to_string(),
        )),
    }
}

/// How many relations are followed in total while resolving a member or
/// deciding whether a column can be one. Depth alone doesn't bound the work:
/// a query whose relations each reference several others branches out.
const MAX_RELATION_EXPANSIONS: usize = 1000;

/// Consumes one relation expansion, returning whether there was budget left.
fn spend_expansion(budget: &Cell<usize>) -> bool {
    let left = budget.get();
    if left == 0 {
        return false;
    }
    budget.set(left - 1);
    true
}

/// If the table factor is a derived table or a CTE reference which exposes the
/// member as an output column, returns the relation alias to qualify the
/// column with and the output column name.
fn member_column_in_table_factor<'a>(
    factor: &'a ast::TableFactor,
    withs: &[&'a ast::With],
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
    depth: usize,
    budget: &Cell<usize>,
) -> Option<(ast::Ident, ast::Ident)> {
    match factor {
        ast::TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } if alias.columns.is_empty() => {
            let column = member_output_column_in_query(
                subquery,
                withs,
                cube_name,
                member_name,
                meta_member,
                depth,
                budget,
            )?;
            Some((alias.name.clone(), column))
        }
        ast::TableFactor::Table { name, alias, .. } => {
            // The table may be a reference to a CTE
            let ast::ObjectName(parts) = name;
            let [part] = &parts[..] else {
                return None;
            };
            let table_ident = part.as_ident()?;
            // Unquoted identifiers are case-insensitive; the same
            // approximation is made for cube names when matching relations
            let cte = withs.iter().find_map(|with| {
                with.cte_tables.iter().find(|cte| {
                    cte.alias
                        .name
                        .value
                        .eq_ignore_ascii_case(&table_ident.value)
                })
            })?;
            if !cte.alias.columns.is_empty() {
                return None;
            }
            let column = member_output_column_in_query(
                &cte.query,
                withs,
                cube_name,
                member_name,
                meta_member,
                depth,
                budget,
            )?;
            let relation_alias = match alias {
                Some(alias) if alias.columns.is_empty() => alias.name.clone(),
                Some(_) => return None,
                None => table_ident.clone(),
            };
            Some((relation_alias, column))
        }
        _ => None,
    }
}

/// The output column of a query that exposes the member: a direct column or
/// wildcard of a cube in its FROM, an aliased aggregation for a measure, or a
/// column forwarded unchanged from a relation that exposes it.
fn member_output_column_in_query<'a>(
    query: &'a ast::Query,
    withs: &[&'a ast::With],
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
    depth: usize,
    budget: &Cell<usize>,
) -> Option<ast::Ident> {
    if depth >= MAX_RELATION_DEPTH || !spend_expansion(budget) {
        return None;
    }

    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    // A query declares CTEs of its own on top of the ones already visible
    let withs = query
        .with
        .as_ref()
        .into_iter()
        .chain(withs.iter().copied())
        .collect::<Vec<_>>();

    // A bare column reference can't be attributed to a specific relation, so
    // it is only accepted when there is a single relation in FROM
    let allow_unqualified = relation_count(&select.from) == 1;

    // The cube itself in this query's FROM
    if let Some(cube_alias) =
        alias_for_relation_in_from(cube_name, &select.from, &cte_names(&withs))
    {
        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    if matches!(meta_member, MetaMember::Dimension(_))
                        && is_direct_member_ref(expr, &cube_alias, member_name, allow_unqualified)
                    {
                        // Output column name is the column name itself
                        return Some(ast::Ident::with_quote('"', member_name));
                    }
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    if is_member_expr(
                        expr,
                        &cube_alias,
                        member_name,
                        meta_member,
                        allow_unqualified,
                    ) {
                        return Some(alias.clone());
                    }
                }
                ast::SelectItem::Wildcard(_) => {
                    // `SELECT *` exposes raw cube columns: dimensions keep their name,
                    // measures stay unaggregated and cannot be filtered. Over a join a bare
                    // name may belong to any relation, so it is restricted as elsewhere.
                    if allow_unqualified && matches!(meta_member, MetaMember::Dimension(_)) {
                        return Some(ast::Ident::with_quote('"', member_name));
                    }
                }
                ast::SelectItem::QualifiedWildcard(
                    ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) => {
                    let ast::ObjectName(parts) = name;
                    if let [qualifier] = &parts[..] {
                        if qualifier
                            .as_ident()
                            .is_some_and(|i| i.value.eq_ignore_ascii_case(&cube_alias.value))
                            && matches!(meta_member, MetaMember::Dimension(_))
                        {
                            return Some(ast::Ident::with_quote('"', member_name));
                        }
                    }
                }
                _ => {}
            }
        }

        // The cube being in this FROM doesn't mean the projection reads the
        // member from it - another relation may expose it too, so the search
        // carries on below rather than ending here
    }

    // A relation of this query may expose it, in which case the projection
    // has to forward that column unchanged
    for table_with_joins in &select.from {
        let factors = iter::once(&table_with_joins.relation)
            .chain(table_with_joins.joins.iter().map(|join| &join.relation));
        for factor in factors {
            let Some((relation_alias, column)) = member_column_in_table_factor(
                factor,
                &withs,
                cube_name,
                member_name,
                meta_member,
                depth + 1,
                budget,
            ) else {
                continue;
            };
            if let Some(forwarded) =
                forwarded_output_column(select, &relation_alias, &column, allow_unqualified)
            {
                return Some(forwarded);
            }
        }
    }

    None
}

/// Finds the output column of a SELECT that forwards a column of one of its
/// relations unchanged, as opposed to computing something from it.
fn forwarded_output_column(
    select: &ast::Select,
    relation: &ast::Ident,
    column: &ast::Ident,
    allow_unqualified: bool,
) -> Option<ast::Ident> {
    let mut wildcard = None;
    for item in &select.projection {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                // The output name of an unaliased item is the column name, as
                // the reference wrote it
                if is_direct_member_ref(expr, relation, &column.value, allow_unqualified) {
                    return Some(match expr {
                        ast::Expr::CompoundIdentifier(idents) => idents.last()?.clone(),
                        _ => column.clone(),
                    });
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                if is_direct_member_ref(expr, relation, &column.value, allow_unqualified) {
                    return Some(alias.clone());
                }
            }
            ast::SelectItem::Wildcard(_) => {
                if allow_unqualified {
                    wildcard = Some(column.clone());
                }
            }
            ast::SelectItem::QualifiedWildcard(
                ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                _,
            ) => {
                let ast::ObjectName(parts) = name;
                if let [qualifier] = &parts[..] {
                    if qualifier
                        .as_ident()
                        .is_some_and(|i| i.value.eq_ignore_ascii_case(&relation.value))
                    {
                        wildcard = Some(column.clone());
                    }
                }
            }
            _ => {}
        }
    }

    wildcard
}

/// Whether the expression directly exposes the member: a direct column
/// reference for dimensions, an aggregation of the raw column for measures.
fn is_member_expr(
    expr: &ast::Expr,
    cube_alias: &ast::Ident,
    member_name: &str,
    meta_member: &MetaMember,
    allow_unqualified: bool,
) -> bool {
    match meta_member {
        MetaMember::Dimension(_) => {
            is_direct_member_ref(expr, cube_alias, member_name, allow_unqualified)
        }
        MetaMember::Measure(_) => match expr {
            ast::Expr::Function(func) => {
                let ast::ObjectName(name_parts) = &func.name;
                let Some(func_name) = name_parts.last().and_then(|part| part.as_ident()) else {
                    return false;
                };
                if !AGG_FUNCTIONS
                    .iter()
                    .any(|agg| func_name.value.eq_ignore_ascii_case(agg))
                {
                    return false;
                }
                let ast::FunctionArguments::List(arg_list) = &func.args else {
                    return false;
                };
                let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))] =
                    &arg_list.args[..]
                else {
                    return false;
                };
                is_direct_member_ref(arg, cube_alias, member_name, allow_unqualified)
            }
            _ => false,
        },
    }
}

/// Whether the expression is a reference to the member column of the cube.
/// An unqualified reference can only be attributed to the cube when it is the
/// sole relation of the FROM clause, hence `allow_unqualified`.
fn is_direct_member_ref(
    expr: &ast::Expr,
    cube_alias: &ast::Ident,
    member_name: &str,
    allow_unqualified: bool,
) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => {
            allow_unqualified && ident.value.eq_ignore_ascii_case(member_name)
        }
        ast::Expr::CompoundIdentifier(idents) => {
            let [qualifier, column] = &idents[..] else {
                return false;
            };
            // The relation itself was matched case-insensitively, so the
            // qualifier written on the column may differ in case from it
            qualifier.value.eq_ignore_ascii_case(&cube_alias.value)
                && column.value.eq_ignore_ascii_case(member_name)
        }
        _ => false,
    }
}

/// Number of relations in the FROM clause, joins included.
fn relation_count(from: &[ast::TableWithJoins]) -> usize {
    from.iter()
        .map(|table_with_joins| 1 + table_with_joins.joins.len())
        .sum()
}

fn alias_for_relation_in_from(
    relation_name: &str,
    from: &[ast::TableWithJoins],
    cte_names: &HashSet<String>,
) -> Option<ast::Ident> {
    for table_with_joins in from {
        if let Some(alias) =
            alias_for_relation_in_table_factor(relation_name, &table_with_joins.relation, cte_names)
        {
            return Some(alias);
        }
        for join in &table_with_joins.joins {
            if let Some(alias) =
                alias_for_relation_in_table_factor(relation_name, &join.relation, cte_names)
            {
                return Some(alias);
            }
        }
    }
    None
}

fn alias_for_relation_in_table_factor(
    relation_name: &str,
    table_factor: &ast::TableFactor,
    cte_names: &HashSet<String>,
) -> Option<ast::Ident> {
    match table_factor {
        ast::TableFactor::Table { name, alias, .. } => {
            let ast::ObjectName(parts) = name;
            let last_ident = parts.last()?.as_ident()?;
            let table_name = &last_ident.value;
            if !table_name.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            // A relation named after a cube may in fact be a CTE reference,
            // whose output columns have nothing to do with the cube's ones
            if parts.len() == 1 && cte_names.contains(&table_name.to_ascii_lowercase()) {
                return None;
            }
            let Some(alias) = alias else {
                return Some(last_ident.clone());
            };
            if !alias.columns.is_empty() {
                return None;
            }
            Some(alias.name.clone())
        }
        _ => None,
    }
}

/// Whether the expression is an AND chain, and so a grouping the conjunct
/// split may look through.
fn is_and_chain(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::And,
            ..
        }
    )
}

/// Splits a clause into the conjuncts of its top-level AND chain,
/// iteratively, as a clause holds a conjunct per filter. Parentheses around
/// the whole clause are kept, since a filter group is what they may be.
fn into_and_conjuncts_exact(expr: ast::Expr) -> Vec<ast::Expr> {
    into_and_conjuncts_with(expr, false)
}

/// The consuming split, with parenthesized AND chains looked through or not.
/// [`and_conjuncts_with`] is its borrowing twin and walks in the same order,
/// which [`with_keys`] relies on.
fn into_and_conjuncts_with(expr: ast::Expr, transparent: bool) -> Vec<ast::Expr> {
    let mut conjuncts = Vec::new();
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And,
                right,
            } => {
                pending.push(*right);
                pending.push(*left);
            }
            ast::Expr::Nested(inner) if transparent && is_and_chain(&inner) => pending.push(*inner),
            expr => conjuncts.push(expr),
        }
    }
    conjuncts
}

/// As [`into_and_conjuncts_exact`], with parenthesized AND chains looked
/// through: the rewrite engine flattens a top-level `and` group into sibling
/// filters, so its members have to be reachable as filters of their own.
fn into_and_conjuncts(expr: ast::Expr) -> Vec<ast::Expr> {
    into_and_conjuncts_with(expr, true)
}

/// The borrowing counterpart of [`into_and_conjuncts_exact`].
fn and_conjuncts_exact(expr: &ast::Expr) -> Vec<&ast::Expr> {
    and_conjuncts_with(expr, false)
}

/// The borrowing split; see [`into_and_conjuncts_with`].
fn and_conjuncts_with(expr: &ast::Expr, transparent: bool) -> Vec<&ast::Expr> {
    let mut conjuncts = Vec::new();
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And,
                right,
            } => {
                pending.push(right);
                pending.push(left);
            }
            ast::Expr::Nested(inner) if transparent && is_and_chain(inner) => pending.push(inner),
            expr => conjuncts.push(expr),
        }
    }
    conjuncts
}

/// The borrowing counterpart of [`into_and_conjuncts`].
fn and_conjuncts(expr: &ast::Expr) -> Vec<&ast::Expr> {
    and_conjuncts_with(expr, true)
}

/// Joins conjuncts back into a left-deep AND chain.
fn and_chain(conjuncts: Vec<ast::Expr>) -> Option<ast::Expr> {
    conjuncts
        .into_iter()
        .reduce(|left, right| ast::Expr::BinaryOp {
            left: Box::new(left),
            op: ast::BinaryOperator::And,
            right: Box::new(right),
        })
}

/// Whether the clause, keyed as `keys` by [`clause_key_set`], already holds
/// the expression: as itself, or as a group each of whose members it holds.
fn clause_holds(keys: &HashSet<FilterKey>, expr: &ast::Expr, ctx: MatchContext) -> bool {
    if keys.contains(&ctx.key(expr)) {
        return true;
    }
    let members = group_members(expr);
    !members.is_empty() && members.iter().all(|member| keys.contains(&ctx.key(member)))
}

/// The keys an appended expression adds to a clause's key set: itself under
/// both splits, and its members under the looking-through one.
fn appended_keys(expr: &ast::Expr, ctx: MatchContext) -> Vec<FilterKey> {
    iter::once(ctx.key(expr))
        .chain(group_members(expr).into_iter().map(|m| ctx.key(m)))
        .collect()
}

/// Appends the expression to the clause with AND. Whether it is already there
/// is decided by the caller against the clause's keys.
fn append_expr_to_clause(option_clause: &mut Option<ast::Expr>, expr: ast::Expr) {
    // The clause is taken rather than borrowed: it grows by a predicate per
    // addition, and cloning it each time would make a batch quadratic
    let Some(clause) = option_clause.take() else {
        *option_clause = Some(expr);
        return;
    };
    // The existing clause may be a top-level OR chain, and sqlparser rendering
    // is not precedence-aware: parentheses only survive as `Expr::Nested`.
    // Operators binding looser than AND have to be parenthesized before AND-ing.
    let existing = match &clause {
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::Or | ast::BinaryOperator::Xor,
            ..
        } => ast::Expr::Nested(Box::new(clause)),
        _ => clause,
    };
    *option_clause = Some(ast::Expr::BinaryOp {
        left: Box::new(existing),
        op: ast::BinaryOperator::And,
        right: Box::new(expr),
    });
}

/// How a filter this API writes is matched against the predicates a query
/// wrote itself.
#[derive(Debug, Clone, Copy)]
struct MatchContext {
    /// The plan reports the filter and nothing else on its member, so it may
    /// be matched by its column alone.
    sole_reported: bool,
    /// The outermost FROM holds one relation, so a bare column is the same
    /// column as a qualified one.
    ignore_qualifier: bool,
    /// The filter stands on a time member, whose dates the planner
    /// canonicalizes; on any other member `'2024-01-01'` and
    /// `'2024-01-01T00:00:00.000Z'` are two different strings.
    normalize_dates: bool,
}

impl MatchContext {
    fn key(&self, expr: &ast::Expr) -> FilterKey {
        expr_key(expr, self.ignore_qualifier, self.normalize_dates)
    }
}

/// A comparison key for a filter expression: its SQL rendering with the
/// identifier quoting reduced, as this API quotes what a query may write bare.
/// `drop_qualifiers` drops relation names; `normalize_dates` reduces dates.
fn expr_key(expr: &ast::Expr, drop_qualifiers: bool, normalize_dates: bool) -> String {
    // A group this API writes carries its parentheses, while a query holding
    // one predicate writes the same group without them
    let mut expr = expr;
    for _ in 0..MAX_EXPR_NESTING {
        let ast::Expr::Nested(inner) = expr else {
            break;
        };
        expr = inner;
    }

    let rendered = expr.to_string();
    let mut key = String::with_capacity(rendered.len());
    let mut word = String::new();
    let mut chars = rendered.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                push_word(&mut key, &mut word, Some('\''));
                // A quoted literal, in which a quote is written twice
                let literal = read_quoted(&mut chars, '\'');
                let literal = if normalize_dates {
                    normalize_filter_value(&literal)
                } else {
                    literal
                };
                key.push('\'');
                key.push_str(&literal.replace('\'', "''"));
                key.push('\'');
            }
            '"' => {
                push_word(&mut key, &mut word, Some('"'));
                let ident = read_quoted(&mut chars, '"');
                if drop_qualifiers && chars.peek() == Some(&'.') {
                    chars.next();
                    continue;
                }
                // cubesql resolves an identifier case-insensitively however
                // it is quoted, so the key spells it lower case, and bare
                // when it can be written bare
                let ident = ident.to_ascii_lowercase();
                if is_bare_identifier(&ident) {
                    key.push_str(&ident);
                } else {
                    key.push('"');
                    key.push_str(&ident.replace('"', "\"\""));
                    key.push('"');
                }
            }
            c if c.is_ascii_alphanumeric() || c == '_' => word.push(c),
            c => {
                let is_qualifier = c == '.' && starts_identifier(&word);
                if drop_qualifiers && is_qualifier {
                    word.clear();
                    continue;
                }
                push_word(&mut key, &mut word, Some(c));
                key.push(c);
            }
        }
    }
    push_word(&mut key, &mut word, None);

    key
}

/// Flushes a run of unquoted characters into the key, folded to lower case as
/// PostgreSQL does. `ILIKE` keys as `LIKE` and every aggregation as `measure`,
/// since a Cube filter carries neither the case sensitivity nor the
/// aggregation the query spelled.
fn push_word(key: &mut String, word: &mut String, followed_by: Option<char>) {
    if word.is_empty() {
        return;
    }
    let lowered = word.to_ascii_lowercase();
    let is_call = followed_by == Some('(');
    let normalized = if lowered == "ilike" {
        "like"
    } else if is_call
        && AGG_FUNCTIONS
            .iter()
            .any(|agg| agg.eq_ignore_ascii_case(&lowered))
    {
        "measure"
    } else {
        &lowered
    };
    key.push_str(normalized);
    word.clear();
}

/// Reads the rest of a quoted run, in which the quote character is written
/// twice to stand for itself.
fn read_quoted(chars: &mut Peekable<Chars>, quote: char) -> String {
    let mut quoted = String::new();
    loop {
        match chars.next() {
            Some(c) if c == quote => {
                if chars.peek() == Some(&quote) {
                    chars.next();
                    quoted.push(quote);
                } else {
                    break;
                }
            }
            Some(other) => quoted.push(other),
            None => break,
        }
    }
    quoted
}

/// Whether a run of unquoted characters is a name rather than a number, and
/// so can be the relation of a qualified column.
fn starts_identifier(word: &str) -> bool {
    word.chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
}

/// Whether an identifier can be written without quotes: nothing in it would
/// end a bare one. Case does not matter, as cubesql resolves an identifier
/// case-insensitively whichever way it is written.
fn is_bare_identifier(ident: &str) -> bool {
    let mut chars = ident.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The filters a needle stands for besides itself. The rewrite engine reports
/// a top-level `and` group as sibling filters, so the group is present when
/// each of its members is, and removing it removes each of them. Anything
/// else, an `or` group included, stands only for itself.
fn group_members(needle: &ast::Expr) -> Vec<&ast::Expr> {
    match needle {
        ast::Expr::Nested(inner) if is_and_chain(inner) => and_conjuncts(inner),
        _ => Vec::new(),
    }
}

/// The conjuncts of a clause under both splits: as the clause stands, so that
/// a filter group is seen as the group it is, and with nested AND chains
/// looked through, so that a member of one is seen on its own.
fn all_conjuncts<'a>(clause: &'a ast::Expr) -> Vec<&'a ast::Expr> {
    and_conjuncts_exact(clause)
        .into_iter()
        .chain(and_conjuncts(clause))
        .collect()
}

/// The keys of every conjunct of the clause, under both splits, which is
/// what an addition checks its filter against.
fn clause_key_set(clause: &Option<ast::Expr>, ctx: MatchContext) -> HashSet<FilterKey> {
    clause
        .iter()
        .flat_map(all_conjuncts)
        .map(|expr| ctx.key(expr))
        .collect()
}

/// The conjunct keys of the outermost clauses, kept across a batch so that
/// additions key a clause once. One set per clause and per way of keying;
/// every set of a clause is extended on append, forgotten on any other change.
#[derive(Default)]
struct ClauseKeys {
    sets: HashMap<(ClauseKind, bool), HashSet<FilterKey>>,
}

impl ClauseKeys {
    /// Whether the clause already holds the expression.
    fn holds(
        &mut self,
        kind: ClauseKind,
        ctx: MatchContext,
        clause: &Option<ast::Expr>,
        expr: &ast::Expr,
    ) -> bool {
        let keys = self
            .sets
            .entry((kind, ctx.normalize_dates))
            .or_insert_with(|| clause_key_set(clause, ctx));
        clause_holds(keys, expr, ctx)
    }

    /// Records an append in every set of the clause, each under its own way
    /// of keying, so that none of them goes stale.
    fn note_appended(&mut self, kind: ClauseKind, expr: &ast::Expr, ignore_qualifier: bool) {
        for ((known, normalize_dates), keys) in self.sets.iter_mut() {
            if *known != kind {
                continue;
            }
            let ctx = MatchContext {
                sole_reported: false,
                ignore_qualifier,
                normalize_dates: *normalize_dates,
            };
            keys.extend(appended_keys(expr, ctx));
        }
    }

    fn forget(&mut self, kind: ClauseKind) {
        self.sets.retain(|(known, _), _| *known != kind);
    }
}

/// Splits the clause the way that matches the needle: as it stands, so that a
/// group matches as a group, else with nested AND chains looked through.
/// `Err` hands the clause back untouched when the needle is in neither.
fn matching_conjuncts(
    clause: ast::Expr,
    needle_key: &FilterKey,
    ctx: MatchContext,
) -> Result<Vec<(FilterKey, ast::Expr)>, ast::Expr> {
    let exact_keys = keys_of(and_conjuncts_exact(&clause), ctx);
    if exact_keys.contains(needle_key) {
        return Ok(with_keys(exact_keys, into_and_conjuncts_exact(clause), ctx));
    }

    let keys = keys_of(and_conjuncts(&clause), ctx);
    if keys.contains(needle_key) {
        return Ok(with_keys(keys, into_and_conjuncts(clause), ctx));
    }

    Err(clause)
}

fn keys_of(conjuncts: Vec<&ast::Expr>, ctx: MatchContext) -> Vec<FilterKey> {
    conjuncts
        .into_iter()
        .map(|conjunct| ctx.key(conjunct))
        .collect()
}

/// Pairs each conjunct with the key worked out while the clause was borrowed,
/// so a removal keys a conjunct once. The two splits walk in the same order;
/// should they ever not, the keys are worked out again rather than misparied.
fn with_keys(
    keys: Vec<FilterKey>,
    conjuncts: Vec<ast::Expr>,
    ctx: MatchContext,
) -> Vec<(FilterKey, ast::Expr)> {
    if keys.len() != conjuncts.len() {
        return conjuncts
            .into_iter()
            .map(|conjunct| (ctx.key(&conjunct), conjunct))
            .collect();
    }

    keys.into_iter().zip(conjuncts).collect()
}

/// Removes all identical expressions from the top-level AND chain.
/// Returns the number of expressions removed.
fn remove_expr_from_clause(
    option_clause: &mut Option<ast::Expr>,
    needle: &ast::Expr,
    ctx: MatchContext,
) -> usize {
    let Some(clause) = option_clause.take() else {
        return 0;
    };

    // Which split matches is decided before the clause is taken apart, so a
    // needle that isn't there leaves it exactly as it was
    let needle_key = ctx.key(needle);
    let clause = match matching_conjuncts(clause, &needle_key, ctx) {
        Ok(conjuncts) => {
            let before = conjuncts.len();
            let kept = conjuncts
                .into_iter()
                .filter(|(key, _)| key != &needle_key)
                .map(|(_, conjunct)| conjunct)
                .collect::<Vec<_>>();
            let removed = before - kept.len();
            *option_clause = and_chain(kept);
            return removed;
        }
        Err(clause) => clause,
    };

    // A group the clause doesn't hold as one expression is still there when
    // each of its members is, and then it is those that are removed
    let members = group_members(needle);
    let member_keys = members.iter().map(|m| ctx.key(m)).collect::<Vec<_>>();
    let holds_every_member = !members.is_empty() && {
        let keys = all_conjuncts(&clause)
            .into_iter()
            .map(|expr| ctx.key(expr))
            .collect::<Vec<_>>();
        member_keys.iter().all(|member| keys.contains(member))
    };
    if holds_every_member {
        let conjuncts = into_and_conjuncts(clause);
        let before = conjuncts.len();
        let kept = conjuncts
            .into_iter()
            .filter(|conjunct| !member_keys.contains(&ctx.key(conjunct)))
            .collect::<Vec<_>>();
        let removed = before - kept.len();
        *option_clause = and_chain(kept);
        return removed;
    }

    remove_reported_column_predicates(option_clause, clause, needle, ctx)
}

/// Removes every predicate on the column the needle filters: a filter the
/// plan reports but the query does not spell out as this API writes it has no
/// predicates identifiable one by one. Only for one alone on its member.
fn remove_reported_column_predicates(
    option_clause: &mut Option<ast::Expr>,
    clause: ast::Expr,
    needle: &ast::Expr,
    ctx: MatchContext,
) -> usize {
    let needle_column = filter_column_key(needle, ctx).filter(|_| ctx.sole_reported);
    let Some(needle_column) = needle_column else {
        *option_clause = Some(clause);
        return 0;
    };

    let on_needle_column =
        |conjunct: &ast::Expr| filter_column_key(conjunct, ctx).as_ref() == Some(&needle_column);

    // Whether there is one is decided before the clause is taken apart, so a
    // clause holding none is left exactly as it was
    if !and_conjuncts(&clause).into_iter().any(on_needle_column) {
        *option_clause = Some(clause);
        return 0;
    }

    let conjuncts = into_and_conjuncts(clause);
    let before = conjuncts.len();
    let kept = conjuncts
        .into_iter()
        .filter(|conjunct| !on_needle_column(conjunct))
        .collect::<Vec<_>>();
    let removed = before - kept.len();
    *option_clause = and_chain(kept);
    removed
}

/// The single column a filter expression stands on, as a comparison key.
/// A filter over more than one column, a group of them included, has none.
fn filter_column_key(expr: &ast::Expr, ctx: MatchContext) -> Option<FilterKey> {
    let mut column: Option<String> = None;
    let mut pending = vec![expr];
    let mut visited = 0;

    while let Some(expr) = pending.pop() {
        visited += 1;
        if visited > MAX_EXPR_NODES {
            return None;
        }

        match expr {
            ast::Expr::Nested(inner) => pending.push(inner),
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And | ast::BinaryOperator::Or,
                right,
            } => {
                pending.push(left);
                pending.push(right);
            }
            expr => {
                let key = ctx.key(filter_column_expr(predicate_column(expr)?));
                match &column {
                    Some(seen) if seen != &key => return None,
                    Some(_) => {}
                    None => column = Some(key),
                }
            }
        }
    }

    column
}

/// The column side of a single predicate.
fn predicate_column(expr: &ast::Expr) -> Option<&ast::Expr> {
    match expr {
        ast::Expr::BinaryOp { left, op, right } => {
            if !is_comparison_op(op) {
                return None;
            }
            if is_filter_value(right) {
                Some(left)
            } else if is_filter_value(left) {
                Some(right)
            } else {
                None
            }
        }
        ast::Expr::InList { expr, .. }
        | ast::Expr::Between { expr, .. }
        | ast::Expr::Like { expr, .. }
        | ast::Expr::ILike { expr, .. }
        | ast::Expr::IsNull(expr)
        | ast::Expr::IsNotNull(expr) => Some(expr),
        _ => None,
    }
}

fn is_comparison_op(op: &ast::BinaryOperator) -> bool {
    matches!(
        op,
        ast::BinaryOperator::Eq
            | ast::BinaryOperator::NotEq
            | ast::BinaryOperator::Lt
            | ast::BinaryOperator::LtEq
            | ast::BinaryOperator::Gt
            | ast::BinaryOperator::GtEq
    )
}

/// Replaces all identical expressions in place within the top-level AND
/// chain, preserving their positions. Returns the number of replacements.
fn replace_expr_in_clause(
    option_clause: &mut Option<ast::Expr>,
    old: &ast::Expr,
    new: ast::Expr,
    ctx: MatchContext,
    new_ctx: MatchContext,
) -> usize {
    let Some(clause) = option_clause.take() else {
        return 0;
    };

    let old_key = ctx.key(old);
    let clause = match matching_conjuncts(clause, &old_key, ctx) {
        Ok(conjuncts) => {
            let mut replaced = 0;
            let conjuncts = conjuncts
                .into_iter()
                .map(|(key, conjunct)| {
                    if key == old_key {
                        replaced += 1;
                        new.clone()
                    } else {
                        conjunct
                    }
                })
                .collect::<Vec<_>>();
            *option_clause = and_chain(conjuncts);
            return replaced;
        }
        Err(clause) => clause,
    };

    // As for removal, a group the clause holds as separate members is
    // replaced by dropping those and appending the new filter. Its position
    // isn't kept, as the members had no single one to keep.
    *option_clause = Some(clause);
    let removed = remove_expr_from_clause(option_clause, old, ctx);
    if removed == 0 {
        return 0;
    }
    // The filter being added is matched as itself: what counts as the same
    // value is decided by the member it stands on, not by the one it replaces
    if !clause_holds(&clause_key_set(option_clause, new_ctx), &new, new_ctx) {
        append_expr_to_clause(option_clause, new);
    }
    removed
}

/// Reduces a time member's values back to the form they were written in; the
/// planner canonicalizes a date to `2020-01-01T00:00:00.000Z`. The member
/// decides rather than the operator: a time member carries a date under
/// `notEquals` too, and a string member never does.
fn report_filter(
    mut filter: V1LoadRequestQueryFilterItem,
    ctx: &MetaContext,
) -> V1LoadRequestQueryFilterItem {
    if let Some(items) = filter.and.take() {
        filter.and = Some(report_group_items(items, ctx));
        return filter;
    }
    if let Some(items) = filter.or.take() {
        filter.or = Some(report_group_items(items, ctx));
        return filter;
    }

    if !filters_a_time_member(&filter, ctx) {
        return filter;
    }

    filter.values = filter.values.map(|values| {
        values
            .iter()
            .map(|value| report_date_value(value))
            .collect()
    });
    filter
}

/// A date at the start or the end of its day is reported as the date alone,
/// which is how a query writes it and how the REST API reads a range bound. A
/// value carrying a real time is reported as it stands, down to its trailing
/// zeros, since shortening it would report something the query doesn't say.
fn report_date_value(value: &str) -> String {
    let normalized = normalize_filter_value(value);
    if normalized.len() == DATE_LENGTH {
        normalized
    } else {
        value.to_string()
    }
}

/// The upper bound of a date range as the REST API reads it: a bare date
/// stands for the whole of that day, which `/v1/load` renders as
/// `T23:59:59.999`. Written the same way here, so that the two paths filter
/// the same rows; [`report_date_value`] folds it back to the date.
fn date_range_upper(value: &str) -> String {
    if is_iso_date_like(value) && value.len() == DATE_LENGTH {
        format!("{}T23:59:59.999", value)
    } else {
        value.to_string()
    }
}

/// The length of an ISO date, `YYYY-MM-DD`.
const DATE_LENGTH: usize = 10;

fn report_group_items(items: Vec<serde_json::Value>, ctx: &MetaContext) -> Vec<serde_json::Value> {
    items
        .into_iter()
        .map(
            |item| match serde_json::from_value::<V1LoadRequestQueryFilterItem>(item.clone()) {
                Ok(item_filter) => {
                    serde_json::to_value(report_filter(item_filter, ctx)).unwrap_or(item)
                }
                Err(_) => item,
            },
        )
        .collect()
}

/// Extracts Cube filters from all CubeScan nodes of a logical plan, including
/// those of CTEs and subqueries. Time dimension date ranges are represented as
/// `inDateRange` filter items, and date values are reported in the form the
/// query wrote them.
pub fn extract_filters_from_plan(
    plan: &LogicalPlan,
    ctx: &MetaContext,
) -> Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    struct CollectFiltersVisitor<'ctx>(Vec<V1LoadRequestQueryFilterItem>, &'ctx MetaContext);

    impl PlanVisitor for CollectFiltersVisitor<'_> {
        type Error = CubeError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
            if let LogicalPlan::Extension(ext) = plan {
                if let Some(scan_node) = ext.node.as_any().downcast_ref::<CubeScanNode>() {
                    if let Some(filters) = &scan_node.request.filters {
                        let ctx = self.1;
                        self.0.extend(
                            filters
                                .iter()
                                .cloned()
                                .map(|filter| report_filter(filter, ctx)),
                        );
                    }
                    for time_dimension in scan_node.request.time_dimensions.iter().flatten() {
                        let Some(date_range) = &time_dimension.date_range else {
                            continue;
                        };
                        let values = match date_range {
                            serde_json::Value::Array(values) => values
                                .iter()
                                .filter_map(|value| value.as_str().map(|s| s.to_string()))
                                .collect(),
                            serde_json::Value::String(value) => vec![value.clone()],
                            _ => continue,
                        };
                        self.0.push(report_filter(
                            V1LoadRequestQueryFilterItem {
                                member: Some(time_dimension.dimension.clone()),
                                operator: Some("inDateRange".to_string()),
                                values: Some(values),
                                ..Default::default()
                            },
                            self.1,
                        ));
                    }
                } else if let Some(wrapper_node) =
                    ext.node.as_any().downcast_ref::<CubeScanWrapperNode>()
                {
                    wrapper_node.wrapped_plan.accept(self)?;
                } else if let Some(wrapper_node) =
                    ext.node.as_any().downcast_ref::<CubeScanWrappedSqlNode>()
                {
                    wrapper_node.wrapped_plan.accept(self)?;
                }
            }
            Ok(true)
        }
    }

    let mut visitor = CollectFiltersVisitor(Vec::new(), ctx);
    // The extracted set is used as the verification oracle, so a partial
    // collection must not be mistaken for a complete one
    plan.accept(&mut visitor)?;

    // Dedup while preserving order
    let mut seen = HashSet::new();
    Ok(visitor
        .0
        .into_iter()
        .filter(|filter| seen.insert(filter_key(filter)))
        .collect())
}

type FilterKey = String;

/// The filters a query reports, which is what decides whether a filter this
/// API cannot find by value in the query is one the query holds all the same.
struct ReportedFilters {
    keys: HashSet<FilterKey>,
    /// How many of the reported filters stand on each member, a group
    /// counting once for every member it holds.
    per_member: HashMap<String, usize>,
}

impl ReportedFilters {
    /// For a modification that matches nothing already in the query, and so
    /// has no use for what it reports.
    fn none() -> Self {
        Self {
            keys: HashSet::new(),
            per_member: HashMap::new(),
        }
    }

    fn of(filters: &[V1LoadRequestQueryFilterItem]) -> Self {
        let mut per_member = HashMap::new();
        for filter in filters {
            for member in filter_members(filter) {
                *per_member.entry(member).or_insert(0) += 1;
            }
        }
        Self {
            keys: filters.iter().map(exact_filter_key).collect(),
            per_member,
        }
    }

    /// Whether the query reports the filter and nothing else on its member, so
    /// that removing every predicate on the column removes this filter alone.
    fn is_sole_on_member(&self, filter: &V1LoadRequestQueryFilterItem) -> bool {
        if !self.keys.contains(&exact_filter_key(filter)) {
            return false;
        }
        let members = filter_members(filter);
        let [member] = &members.into_iter().collect::<Vec<_>>()[..] else {
            return false;
        };
        self.per_member.get(member) == Some(&1)
    }
}

/// Whether any member the filter stands on holds a time, and so carries
/// values the planner reports in a form the query need not have written.
fn filters_a_time_member(filter: &V1LoadRequestQueryFilterItem, ctx: &MetaContext) -> bool {
    filter_members(filter).iter().any(|member| {
        member
            .split_once('.')
            .and_then(|(cube, name)| MetaMember::get_from_ctx(ctx, cube, name).ok())
            .is_some_and(|member| member.is_time())
    })
}

/// The members a filter stands on, those of a group included.
fn filter_members(filter: &V1LoadRequestQueryFilterItem) -> HashSet<String> {
    let mut members = HashSet::new();
    let mut pending = vec![normalized_filter_json(filter)];
    let mut visited = 0;

    while let Some(item) = pending.pop() {
        visited += 1;
        if visited > MAX_FILTER_NODES {
            break;
        }
        if let Some(member) = item.get("member").and_then(|member| member.as_str()) {
            members.insert(member.to_string());
        }
        for group in ["and", "or"] {
            if let Some(items) = item.get(group).and_then(|items| items.as_array()) {
                pending.extend(items.iter().cloned());
            }
        }
    }

    members
}

/// The filters with every repeat of an earlier one dropped, first occurrence
/// kept in place.
fn dedupe_filters(filters: &[V1LoadRequestQueryFilterItem]) -> Vec<V1LoadRequestQueryFilterItem> {
    let mut seen = HashSet::new();
    filters
        .iter()
        .filter(|filter| seen.insert(filter_key(filter)))
        .cloned()
        .collect()
}

/// A filter exactly as written, for deciding whether it is the very filter a
/// query reports. Unlike [`filter_key`] this reduces nothing: another form of
/// a value is another value, which the column path has to be sure of.
fn exact_filter_key(filter: &V1LoadRequestQueryFilterItem) -> FilterKey {
    serde_json::to_string(filter).unwrap_or_default()
}

/// A canonical representation of a filter (or an and/or filter group)
/// used for perfect-match comparison.
fn filter_key(filter: &V1LoadRequestQueryFilterItem) -> FilterKey {
    normalized_filter_json(filter).to_string()
}

/// Keys that have to be present in the filters of a plan for the filter to
/// count as applied. A top-level `and` group is flattened by the rewrite
/// engine into sibling filters, so it is verified through its members.
fn verification_keys(filter: &V1LoadRequestQueryFilterItem) -> Vec<FilterKey> {
    let json = normalized_filter_json(filter);
    match json.get("and").and_then(|items| items.as_array()) {
        Some(items) => items.iter().map(|item| item.to_string()).collect(),
        None => vec![json.to_string()],
    }
}

/// LIKE-family operators, which take any number of values.
const LIKE_FAMILY_OPERATORS: [&str; 6] = [
    "contains",
    "notContains",
    "startsWith",
    "notStartsWith",
    "endsWith",
    "notEndsWith",
];

/// The canonical JSON of a filter as the rewrite engine reports one, at every
/// depth: a single-member group is its member, a multi-value LIKE-family
/// filter is a group of single-value ones (OR; AND when negated), same-kind
/// nesting is flattened.
fn normalized_filter_json(filter: &V1LoadRequestQueryFilterItem) -> serde_json::Value {
    let group = filter
        .and
        .as_ref()
        .map(|items| ("and", items))
        .or_else(|| filter.or.as_ref().map(|items| ("or", items)));

    if let Some((op, items)) = group {
        let items = items
            .iter()
            .map(|item| {
                match serde_json::from_value::<V1LoadRequestQueryFilterItem>(item.clone()) {
                    Ok(item_filter) => normalized_filter_json(&item_filter),
                    Err(_) => item.clone(),
                }
            })
            .collect::<Vec<_>>();

        if let [single] = &items[..] {
            return single.clone();
        }

        let mut flattened = Vec::with_capacity(items.len());
        for item in items {
            match item.get(op).and_then(|nested| nested.as_array()) {
                Some(nested) => flattened.extend(nested.iter().cloned()),
                None => flattened.push(item),
            }
        }
        return serde_json::json!({ op: flattened });
    }

    if let Some(expanded) = like_family_expansion(filter) {
        return expanded;
    }

    leaf_filter_json(filter)
}

/// Expands a LIKE-family filter of several values into the group of
/// single-value filters it is emitted as. Returns `None` for anything else.
fn like_family_expansion(filter: &V1LoadRequestQueryFilterItem) -> Option<serde_json::Value> {
    let operator = filter.operator.as_deref()?;
    let values = filter.values.as_ref()?;
    if values.len() < 2 || !LIKE_FAMILY_OPERATORS.contains(&operator) {
        return None;
    }

    let items = values
        .iter()
        .map(|value| {
            leaf_filter_json(&V1LoadRequestQueryFilterItem {
                member: filter.member.clone(),
                operator: Some(operator.to_string()),
                values: Some(vec![value.clone()]),
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();

    // The negated operators chain with AND, the plain ones with OR
    let op = if operator.starts_with("not") {
        "and"
    } else {
        "or"
    };
    Some(serde_json::json!({ op: items }))
}

fn leaf_filter_json(filter: &V1LoadRequestQueryFilterItem) -> serde_json::Value {
    let values = filter
        .values
        .iter()
        .flatten()
        .map(|value| normalize_filter_value(value))
        .collect::<Vec<_>>();
    serde_json::json!({
        "member": filter.member.as_deref().unwrap_or_default(),
        "operator": filter.operator.as_deref().unwrap_or_default(),
        "values": values,
    })
}

/// Reduces a date at the start or the end of its day, as the planner spells
/// `2024-01-01` and as this API pads a range's upper bound, to the date it
/// stands for. Anything not shaped like a date is left alone.
fn normalize_filter_value(value: &str) -> String {
    if !is_iso_date_like(value) {
        return value.to_string();
    }

    let value = strip_any_suffix(value, &["Z", "+00:00", "+0000"]);
    let value = strip_any_suffix(value, &[".000"]);
    let value = strip_any_suffix(
        value,
        &["T00:00:00", " 00:00:00", "T23:59:59.999", " 23:59:59.999"],
    );
    value.to_string()
}

fn strip_any_suffix<'a>(value: &'a str, suffixes: &[&str]) -> &'a str {
    suffixes
        .iter()
        .find_map(|suffix| value.strip_suffix(suffix))
        .unwrap_or(value)
}

/// Whether the value starts with an ISO-8601 date, optionally followed by a
/// time part.
fn is_iso_date_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 10
        && bytes[..10].iter().enumerate().all(|(i, b)| match i {
            4 | 7 => *b == b'-',
            _ => b.is_ascii_digit(),
        })
        && (bytes.len() == 10 || bytes[10] == b'T' || bytes[10] == b' ')
}

/// Upper bound on the filters a request may carry, counted in the leaves of
/// filter groups: a batch is one plan, so what it bounds is the predicate the
/// rewrite engine saturates over.
const MAX_FILTERS: usize = 500;

/// Upper bound on the nodes of one normalized filter tree: at most
/// [`MAX_FILTERS`] leaves, and a normalized group holds two children or more,
/// so fewer groups than leaves.
const MAX_FILTER_NODES: usize = 2 * MAX_FILTERS;

/// Refuses a request carrying more than [`MAX_FILTERS`] filters, counted in
/// the leaves of groups and across every array the request holds.
fn assert_filter_count<'a>(
    filters: impl IntoIterator<Item = &'a V1LoadRequestQueryFilterItem>,
) -> Result<(), CubeError> {
    let count = filters.into_iter().map(count_filter_leaves).sum::<usize>();
    if count > MAX_FILTERS {
        return Err(CubeError::user(format!(
            "At most {} filters are supported per request, got {}",
            MAX_FILTERS, count
        )));
    }
    Ok(())
}

fn count_filter_leaves(filter: &V1LoadRequestQueryFilterItem) -> usize {
    match filter.and.as_ref().or(filter.or.as_ref()) {
        Some(items) => items
            .iter()
            .map(count_filter_json_leaves)
            .sum::<usize>()
            .max(1),
        None => 1,
    }
}

fn count_filter_json_leaves(item: &serde_json::Value) -> usize {
    // A group is whichever of the two fields holds an array: `and` being
    // present but null is how a plain `or` group deserializes
    let items = item
        .get("and")
        .and_then(|items| items.as_array())
        .or_else(|| item.get("or").and_then(|items| items.as_array()));
    match items {
        Some(items) => items
            .iter()
            .map(count_filter_json_leaves)
            .sum::<usize>()
            .max(1),
        None => 1,
    }
}

/// The rewritten query and the filters its plan reports.
#[derive(Debug)]
pub struct SqlFiltersUpdate {
    pub sql: String,
    pub filters: Vec<V1LoadRequestQueryFilterItem>,
}

/// Extracts the Cube filters of a SQL query from its logical plan.
/// Time dimension date ranges are represented as `inDateRange` filter items.
pub async fn get_sql_filters(
    sql: &str,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    plan_and_extract_filters(sql, meta, session, "Failed to plan the query").await
}

/// A short human-readable filter description for error messages.
fn filter_description(filter: &V1LoadRequestQueryFilterItem) -> String {
    if let Some(member) = &filter.member {
        format!("on \"{}\"", member)
    } else if filter.and.is_some() {
        "group \"and\"".to_string()
    } else if filter.or.is_some() {
        "group \"or\"".to_string()
    } else {
        "".to_string()
    }
}

async fn plan_and_extract_filters(
    sql: &str,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
    error_prefix: &str,
) -> Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    // A planner fault of its own stays internal, so the gateway answers it as
    // one; a query that does not plan is the caller's
    let query_plan = convert_sql_to_cube_query(sql, meta.clone(), session)
        .await
        .map_err(|e| {
            let message = format!("{}: {}", error_prefix, e);
            match e {
                CompilationError::Internal(..) => CubeError::internal(message),
                _ => CubeError::user(message),
            }
        })?;
    // A statement that compiles but has no plan - SET, SHOW, BEGIN - is the
    // caller's to fix, like the parse error `add` gives the same input
    let logical_plan = query_plan
        .try_as_logical_plan()
        .map_err(|_| CubeError::user("Only SELECT queries are supported".to_string()))?;
    extract_filters_from_plan(logical_plan, &meta)
}

/// Adds the filters to the SQL query and verifies each of them is picked up
/// by the logical plan of the rewritten query.
async fn add_filters_and_verify(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    let actions = filters
        .iter()
        .map(|filter| ModifyAction::Add(filter.clone()))
        .collect::<Vec<_>>();
    // Only additions, which match nothing that is already there
    let (new_sql, _) = rewrite_sql(sql, actions, meta.clone(), ReportedFilters::none()).await?;

    verify_additions(sql, new_sql, filters, meta, session).await
}

/// Plans the rewritten query and verifies each added filter is picked up by
/// its plan. Only the rewritten query is planned: planning is the expensive
/// part of a request, and the original is planned on failure alone, to tell
/// a query that never planned from a rewrite that broke it.
async fn verify_additions(
    original_sql: &str,
    new_sql: String,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    let new_filters = match plan_and_extract_filters(
        &new_sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the rewritten query",
    )
    .await
    {
        Ok(filters) => filters,
        Err(rewritten_error) => {
            // Told apart from a query that never planned, which is reported
            // as such. A rewrite that stops planning is still the caller's:
            // a measure filter added to a query with no GROUP BY does that
            plan_and_extract_filters(original_sql, meta, session, "Failed to plan the query")
                .await?;
            return Err(rewritten_error);
        }
    };
    let new_keys = new_filters.iter().map(filter_key).collect::<HashSet<_>>();

    for filter in filters {
        if !verification_keys(filter)
            .iter()
            .all(|key| new_keys.contains(key))
        {
            return Err(CubeError::user(format!(
                "Filter {} was not applied to the query",
                filter_description(filter)
            )));
        }
    }

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: new_filters,
    })
}

/// Adds the filters to the outermost SELECT; ones already present are left as
/// is. Verified against the plan of the rewritten query, which is plan-wide,
/// so an equal filter in a CTE or a subquery satisfies it too.
pub async fn add_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    add_filters_and_verify(sql, filters, meta, session).await
}

/// Removes every filter the plan reports from the outermost WHERE and HAVING,
/// then adds the given ones. The plan decides what a filter is, so nothing
/// extraction cannot represent is removed; a reported filter the outermost
/// SELECT does not hold - a CTE's - stays, and stays reported.
pub async fn set_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    let reported_filters = plan_and_extract_filters(
        sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the query",
    )
    .await?;
    // The removals are as many as the plan reports, and each walks the clause,
    // so they are bounded the way a request's own filters are
    assert_filter_count(&reported_filters).map_err(|_| {
        CubeError::user(format!(
            "The query reports more than {} filters, which is more than set can replace",
            MAX_FILTERS
        ))
    })?;
    let reported = ReportedFilters::of(&reported_filters);
    let actions = set_actions(&reported_filters, filters);
    let (new_sql, _) = rewrite_sql(sql, actions, meta.clone(), reported).await?;

    verify_additions(sql, new_sql, filters, meta, session).await
}

/// The actions a `set` is made of: the removal of every reported filter,
/// then the additions.
fn set_actions(
    reported: &[V1LoadRequestQueryFilterItem],
    filters: &[V1LoadRequestQueryFilterItem],
) -> Vec<ModifyAction> {
    reported
        .iter()
        .map(|filter| ModifyAction::Remove(filter.clone()))
        .chain(
            filters
                .iter()
                .map(|filter| ModifyAction::Add(filter.clone())),
        )
        .collect()
}

/// Deletes the filters from the outermost SELECT: every equal filter goes, one
/// that is not there is a no-op. Decided on the AST, since plan-wide extraction
/// would false-alarm on a CTE's copy; the rewritten query is still planned.
pub async fn delete_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    // Make sure the original query plans before rewriting, and keep the
    // filters it reports: a filter among them that the query does not spell
    // out the way this API writes it is matched by the column it stands on
    let reported = ReportedFilters::of(
        &plan_and_extract_filters(
            sql,
            meta.clone(),
            session.clone(),
            "Failed to plan the query",
        )
        .await?,
    );

    let actions = filters
        .iter()
        .map(|filter| ModifyAction::Remove(filter.clone()))
        .collect::<Vec<_>>();
    let (new_sql, _) = rewrite_sql(sql, actions, meta.clone(), reported).await?;

    // Make sure the rewritten query still plans, and report its filters
    let new_filters = plan_and_extract_filters(
        &new_sql,
        meta,
        session,
        "Failed to plan the rewritten query",
    )
    .await?;

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: new_filters,
    })
}

/// Replaces one exact set of filters with another in the outermost SELECT:
/// every old filter must be present, all equal occurrences are replaced, and a
/// one-for-one replacement in one clause keeps its position. Verified as
/// [`add_sql_filters`] is; removal decided as [`delete_sql_filters`] does.
pub async fn replace_sql_filters(
    sql: &str,
    old_filters: &[V1LoadRequestQueryFilterItem],
    new_filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    if old_filters.is_empty() {
        return Err(CubeError::user(
            "At least one filter to replace is required".to_string(),
        ));
    }

    // One request, one bound: the old and the new filters together
    assert_filter_count(old_filters.iter().chain(new_filters))?;

    // A filter listed twice is one filter to find: the first removal takes
    // every occurrence, and the second would find nothing left
    let old_filters = &dedupe_filters(old_filters)[..];

    // As for a deletion, the filters the query reports decide which of them
    // are matched by the column they stand on rather than by their values
    let reported = ReportedFilters::of(
        &plan_and_extract_filters(
            sql,
            meta.clone(),
            session.clone(),
            "Failed to plan the query",
        )
        .await?,
    );

    let actions = if let ([old_filter], [new_filter]) = (old_filters, new_filters) {
        // One-to-one replacement is done in place to preserve positions
        vec![ModifyAction::Replace {
            old: old_filter.clone(),
            new: new_filter.clone(),
        }]
    } else {
        old_filters
            .iter()
            .map(|filter| ModifyAction::Remove(filter.clone()))
            .chain(
                new_filters
                    .iter()
                    .map(|filter| ModifyAction::Add(filter.clone())),
            )
            .collect()
    };
    let (new_sql, applied) = rewrite_sql(sql, actions, meta.clone(), reported).await?;

    // Every filter to replace has to be found in the outermost SELECT: the
    // removal (or in-place replacement) actions come first, one per old filter
    for (filter, applied) in old_filters.iter().zip(&applied) {
        if !applied {
            return Err(CubeError::user(format!(
                "Filter to replace {} was not found in the outermost SELECT",
                filter_description(filter)
            )));
        }
    }

    let extracted_filters = plan_and_extract_filters(
        &new_sql,
        meta,
        session,
        "Failed to plan the rewritten query",
    )
    .await?;
    let extracted_keys = extracted_filters
        .iter()
        .map(filter_key)
        .collect::<HashSet<_>>();

    for filter in new_filters {
        if !verification_keys(filter)
            .iter()
            .all(|key| extracted_keys.contains(key))
        {
            return Err(CubeError::user(format!(
                "Replacement filter {} was not applied to the query",
                filter_description(filter)
            )));
        }
    }

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: extracted_filters,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::test::get_test_tenant_ctx;
    use std::slice;

    /// Applies a single action; a convenience wrapper over [`modify_sql_ast_many`].
    fn modify_sql_ast(
        sql: &str,
        action: &ModifyAction,
        ctx: &MetaContext,
    ) -> DFResult<(String, bool)> {
        let (sql, applied) =
            modify_sql_ast_many(sql, slice::from_ref(action), ctx, &ReportedFilters::none())?;
        Ok((sql, applied[0]))
    }

    #[test]
    fn test_modify_sql_ast() -> DFResult<()> {
        let sql = r#"
            SELECT
                KibanaSampleDataEcommerce.customer_gender,
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price,
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            ORDER BY 1
        "#;

        // Test adding "equals" filter
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notEquals" filter with multiple values
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec![
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
            ]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('test1', 'test2', 'test3') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "notEquals" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec![
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
            ]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing non-existing filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec![
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
            ]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        // Make sure no modifications were made
        assert!(!applied);

        // Test adding "contains" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["abc".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "contains" filter with multiple values (OR-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["x".to_string(), "y%z_w\\v".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notContains" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notContains".to_string()),
            values: Some(vec!["foo".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notContains" filter with multiple values (AND-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notContains".to_string()),
            values: Some(vec!["bar".to_string(), "baz%_\\qux".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing single-value "contains" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["abc".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing multi-value "contains" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["x".to_string(), "y%z_w\\v".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing single-value "notContains" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notContains".to_string()),
            values: Some(vec!["foo".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing multi-value "notContains" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notContains".to_string()),
            values: Some(vec!["bar".to_string(), "baz%_\\qux".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "startsWith" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("startsWith".to_string()),
            values: Some(vec!["pre".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "startsWith" filter with multiple values (OR-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("startsWith".to_string()),
            values: Some(vec!["a".to_string(), "b%".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notStartsWith" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notStartsWith".to_string()),
            values: Some(vec!["foo".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notStartsWith" filter with multiple values (AND-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notStartsWith".to_string()),
            values: Some(vec!["x".to_string(), "_y".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "endsWith" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("endsWith".to_string()),
            values: Some(vec!["end".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "endsWith" filter with multiple values (OR-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("endsWith".to_string()),
            values: Some(vec!["m".to_string(), "n\\o".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notEndsWith" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEndsWith".to_string()),
            values: Some(vec!["tail".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%tail' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notEndsWith" filter with multiple values (AND-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEndsWith".to_string()),
            values: Some(vec!["p".to_string(), "q_r".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%tail' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%p' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%q\\_r') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing all four newly-added prefix/suffix filters in reverse insertion order
        let remove_ops: Vec<(&str, Vec<&str>)> = vec![
            ("notEndsWith", vec!["p", "q_r"]),
            ("notEndsWith", vec!["tail"]),
            ("endsWith", vec!["m", "n\\o"]),
            ("endsWith", vec!["end"]),
            ("notStartsWith", vec!["x", "_y"]),
            ("notStartsWith", vec!["foo"]),
            ("startsWith", vec!["a", "b%"]),
            ("startsWith", vec!["pre"]),
        ];
        let mut sql = modified_sql;
        for (op, values) in remove_ops {
            let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some(op.to_string()),
                values: Some(values.into_iter().map(|s| s.to_string()).collect()),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "remove {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        // Test adding "gt" filter (integer value) on a numeric dimension
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "gt" filter (decimal value) on same member, AND-combined
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["3.14".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing the integer "gt" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test "gt" with non-numeric value rejected
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["not_a_number".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("must be numeric"),
            "unexpected error: {}",
            err
        );

        // Test "gt" with wrong number of values rejected
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["1".to_string(), "2".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("Exactly one filter value"),
            "unexpected error: {}",
            err
        );

        // Test adding "gte", "lt", "lte" filters chained on the numeric dimension
        let add_ops: Vec<(&str, &str)> = vec![("gte", "5"), ("lt", "100"), ("lte", "10.5")];
        let mut sql = modified_sql;
        for (op, value) in add_ops {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "add {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" >= 5 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" < 100 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" <= 10.5 \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        // Test removing "gte", "lt", "lte", and remaining "gt" filters
        let remove_ops: Vec<(&str, &str)> =
            vec![("lte", "10.5"), ("lt", "100"), ("gte", "5"), ("gt", "3.14")];
        for (op, value) in remove_ops {
            let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "remove {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        // Test non-numeric value rejected for each of gte/lt/lte
        for op in ["gte", "lt", "lte"] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec!["not_a_number".to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let err = modify_sql_ast(&sql, &action, &ctx).unwrap_err();
            assert!(
                err.to_string().contains("must be numeric"),
                "unexpected error for {}: {}",
                op,
                err
            );
        }

        // Test adding "set" filter (no values)
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("set".to_string()),
            values: None,
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" IS NOT NULL \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "set" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("set".to_string()),
            values: None,
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notSet" filter (values ignored)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notSet".to_string()),
            values: Some(vec!["ignored".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" IS NULL \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "notSet" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notSet".to_string()),
            values: None,
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "inDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND (KibanaSampleDataEcommerce.\"order_date\" >= '2024-01-01' \
                    AND KibanaSampleDataEcommerce.\"order_date\" <= '2024-12-31T23:59:59.999') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "inDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test "inDateRange" with wrong number of values rejected
        for values in [
            vec![],
            vec!["2024-01-01".to_string()],
            vec![
                "2024-01-01".to_string(),
                "2024-06-01".to_string(),
                "2024-12-31".to_string(),
            ],
        ] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some("inDateRange".to_string()),
                values: Some(values),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
            assert!(
                err.to_string().contains("Exactly two filter values"),
                "unexpected error: {}",
                err
            );
        }

        // Test adding "notInDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("notInDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"order_date\" NOT BETWEEN '2024-01-01' AND '2024-12-31T23:59:59.999' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "notInDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("notInDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test "notInDateRange" with wrong number of values rejected
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("notInDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("Exactly two filter values"),
            "unexpected error: {}",
            err
        );

        // Test adding "beforeDate" filter
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"order_date\" < '2024-06-01' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "beforeDate" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test "beforeDate" with wrong number of values rejected
        for values in [vec![], vec!["a".to_string(), "b".to_string()]] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some("beforeDate".to_string()),
                values: Some(values),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
            assert!(
                err.to_string().contains("Exactly one filter value"),
                "unexpected error: {}",
                err
            );
        }

        // Test adding "beforeOrOnDate" filter
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeOrOnDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"order_date\" <= '2024-06-01' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "beforeOrOnDate" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeOrOnDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "afterDate" then "afterOrOnDate" filters; verify SQL operators
        let add_ops: Vec<(&str, &str, &str)> = vec![
            ("afterDate", "2024-06-01", ">"),
            ("afterOrOnDate", "2024-07-01", ">="),
        ];
        let mut sql = modified_sql;
        for (op, value, _) in &add_ops {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "add {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"order_date\" > '2024-06-01' \
                AND KibanaSampleDataEcommerce.\"order_date\" >= '2024-07-01' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        // Test removing both filters
        for (op, value, _) in add_ops.iter().rev() {
            let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "remove {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_cte_outermost_only() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "\
            WITH gendered AS (\
                SELECT \
                    KibanaSampleDataEcommerce.customer_gender AS gender, \
                    MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
                FROM KibanaSampleDataEcommerce \
                GROUP BY 1\
            ) \
            SELECT gender, max_price FROM gendered\
        ";

        // Dimension exposed by the CTE: filtered in the outermost WHERE,
        // the CTE itself is left untouched
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            WITH gendered AS (\
                SELECT \
                    KibanaSampleDataEcommerce.customer_gender AS gender, \
                    MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
                FROM KibanaSampleDataEcommerce \
                GROUP BY 1\
            ) \
            SELECT gender, max_price FROM gendered \
            WHERE gendered.gender = 'test'\
            "
        );
        assert!(applied);

        // Measure exposed by the CTE as an aggregation: filtered as a plain
        // column in the outermost WHERE
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            WITH gendered AS (\
                SELECT \
                    KibanaSampleDataEcommerce.customer_gender AS gender, \
                    MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
                FROM KibanaSampleDataEcommerce \
                GROUP BY 1\
            ) \
            SELECT gender, max_price FROM gendered \
            WHERE gendered.gender = 'test' AND gendered.max_price > 42\
            "
        );
        assert!(applied);

        // Removing both filters restores the original query
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert!(applied);
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&modified_sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            WITH gendered AS (\
                SELECT \
                    KibanaSampleDataEcommerce.customer_gender AS gender, \
                    MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
                FROM KibanaSampleDataEcommerce \
                GROUP BY 1\
            ) \
            SELECT gender, max_price FROM gendered\
            "
        );
        assert!(applied);

        // Member not exposed by the CTE is rejected
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_cte_filters_not_removed_from_cte() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        // The filter lives inside the CTE; removal only looks at the outermost
        // SELECT, so nothing is modified
        let sql = "\
            WITH gendered AS (\
                SELECT KibanaSampleDataEcommerce.customer_gender AS gender \
                FROM KibanaSampleDataEcommerce \
                WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
            ) \
            SELECT * FROM gendered\
        ";
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql);
        assert!(!applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_derived_table() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "\
            SELECT * FROM (\
                SELECT customer_gender FROM KibanaSampleDataEcommerce\
            ) AS t\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT * FROM (\
                SELECT customer_gender FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.\"customer_gender\" = 'test'\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_outermost_set_operation_rejected() {
        let ctx = get_test_tenant_ctx();
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            UNION ALL \
            SELECT customer_gender FROM KibanaSampleDataEcommerce\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(&sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("Only plain SELECT statements are supported at the outermost level"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_add_delete_sql_filters() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
        ";

        // The original query has no filters
        let filters = get_sql_filters(sql, meta.clone(), session.clone()).await?;
        assert!(filters.is_empty());

        // Add a filter
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1\
            "
        );
        assert!(result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&filters[0])));

        // The added filter is extracted from the logical plan
        let extracted = get_sql_filters(&result.sql, meta.clone(), session.clone()).await?;
        assert!(extracted
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&filters[0])));

        // Adding the same filter again is a no-op
        let noop_result =
            add_sql_filters(&result.sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(noop_result.sql, result.sql);

        // Delete the filter
        let result =
            delete_sql_filters(&result.sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
            "
        );
        assert!(result.filters.is_empty());

        // Deleting a filter that is not present is a no-op
        let result = delete_sql_filters(&result.sql, &filters, meta, session).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
            "
        );

        Ok(())
    }

    fn or_group(items: Vec<serde_json::Value>) -> V1LoadRequestQueryFilterItem {
        V1LoadRequestQueryFilterItem {
            or: Some(items),
            ..Default::default()
        }
    }

    fn and_group(items: Vec<serde_json::Value>) -> V1LoadRequestQueryFilterItem {
        V1LoadRequestQueryFilterItem {
            and: Some(items),
            ..Default::default()
        }
    }

    #[test]
    fn test_modify_sql_ast_filter_groups() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Add an "or" filter group
        let group = or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["male"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "contains",
                "values": ["vip"],
            }),
        ]);
        let action = ModifyAction::Add(group.clone());
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'male' \
                OR KibanaSampleDataEcommerce.\"notes\" ILIKE '%vip%') \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Removing a group with a different order of filters is not
        // a perfect match, so nothing is removed
        let sql = modified_sql;
        let reversed_group = or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "contains",
                "values": ["vip"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["male"],
            }),
        ]);
        let action = ModifyAction::Remove(reversed_group);
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql);
        assert!(!applied);

        // Removing a perfectly matching group works
        let action = ModifyAction::Remove(group);
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );
        assert!(applied);

        // Add a nested filter group: "and" group inside an "or" group
        let sql = modified_sql;
        let nested_group = or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "and": [
                    {
                        "member": "KibanaSampleDataEcommerce.taxful_total_price",
                        "operator": "gt",
                        "values": ["1"],
                    },
                    {
                        "member": "KibanaSampleDataEcommerce.taxful_total_price",
                        "operator": "lt",
                        "values": ["5"],
                    },
                ],
            }),
        ]);
        let action = ModifyAction::Add(nested_group.clone());
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
                OR (KibanaSampleDataEcommerce.\"taxful_total_price\" > 1 \
                    AND KibanaSampleDataEcommerce.\"taxful_total_price\" < 5)) \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Removing the perfectly matching nested group works
        let sql = modified_sql;
        let action = ModifyAction::Remove(nested_group);
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );
        assert!(applied);

        // A group mixing dimension (WHERE) and measure (HAVING) filters is rejected
        let mixed_group = or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.maxPrice",
                "operator": "gt",
                "values": ["10"],
            }),
        ]);
        let action = ModifyAction::Add(mixed_group);
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("can't mix"),
            "unexpected error: {}",
            err
        );

        // A filter that is both an "and" and an "or" group is rejected
        let mut invalid_group = and_group(vec![serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        })]);
        invalid_group.or = Some(vec![]);
        let action = ModifyAction::Add(invalid_group);
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("can't be both \"and\" and \"or\""),
            "unexpected error: {}",
            err
        );

        // An empty group is rejected
        let action = ModifyAction::Add(or_group(vec![]));
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("at least one filter"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_replace_filter() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1\
        ";

        // Replace a filter in place: the position within the clause is preserved
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["test".to_string()]),
                ..Default::default()
            },
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("notEquals".to_string()),
                values: Some(vec!["a".to_string(), "b".to_string()]),
                ..Default::default()
            },
        };
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('a', 'b') \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Replacing a filter that is not present is not applied
        let sql = modified_sql;
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["missing".to_string()]),
                ..Default::default()
            },
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("set".to_string()),
                ..Default::default()
            },
        };
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql);
        assert!(!applied);

        // Replace a plain filter with an "or" filter group in place
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some("gt".to_string()),
                values: Some(vec!["42".to_string()]),
                ..Default::default()
            },
            new: or_group(vec![
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.taxful_total_price",
                    "operator": "lt",
                    "values": ["10"],
                }),
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.taxful_total_price",
                    "operator": "gt",
                    "values": ["100"],
                }),
            ]),
        };
        let (modified_sql, applied) = modify_sql_ast(&modified_sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('a', 'b') \
                AND (KibanaSampleDataEcommerce.\"taxful_total_price\" < 10 \
                    OR KibanaSampleDataEcommerce.\"taxful_total_price\" > 100) \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Replace a dimension (WHERE) filter with a measure (HAVING) filter
        let sql = modified_sql;
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("notEquals".to_string()),
                values: Some(vec!["a".to_string(), "b".to_string()]),
                ..Default::default()
            },
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                operator: Some("gt".to_string()),
                values: Some(vec!["10".to_string()]),
                ..Default::default()
            },
        };
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"taxful_total_price\" < 10 \
                OR KibanaSampleDataEcommerce.\"taxful_total_price\" > 100) \
            GROUP BY 1 \
            HAVING MAX(KibanaSampleDataEcommerce.\"maxPrice\") > 10\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_delete_sql_filters_group() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Add an "or" filter group
        let filters = vec![or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ])];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
                OR KibanaSampleDataEcommerce.\"notes\" = 'y') \
            GROUP BY 1\
            "
        );
        assert!(result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&filters[0])));

        // Delete the group
        let result = delete_sql_filters(&result.sql, &filters, meta, session).await?;
        assert_eq!(
            result.sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );
        assert!(result.filters.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_sql_filters() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1\
        ";

        // Replace a plain filter with another plain filter
        let old_filter = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        };
        let new_filter = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec!["other".to_string()]),
            ..Default::default()
        };
        let result = replace_sql_filters(
            sql,
            slice::from_ref(&old_filter),
            slice::from_ref(&new_filter),
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
            GROUP BY 1\
            "
        );
        assert!(result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&new_filter)));
        assert!(!result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&old_filter)));

        // Replacing a filter that is not present fails
        let err = replace_sql_filters(
            &result.sql,
            slice::from_ref(&old_filter),
            slice::from_ref(&new_filter),
            meta.clone(),
            session.clone(),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("was not found in the outermost SELECT"),
            "unexpected error: {}",
            err
        );

        // Replace one set of filters with another
        let old_set = vec![new_filter];
        let new_set = vec![
            V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["a".to_string()]),
                ..Default::default()
            },
            V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some("gt".to_string()),
                values: Some(vec!["10".to_string()]),
                ..Default::default()
            },
        ];
        let result = replace_sql_filters(
            &result.sql,
            &old_set,
            &new_set,
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 10 \
            GROUP BY 1\
            "
        );
        for filter in &new_set {
            assert!(result
                .filters
                .iter()
                .any(|extracted| filter_key(extracted) == filter_key(filter)));
        }
        assert!(!result
            .filters
            .iter()
            .any(|extracted| filter_key(extracted) == filter_key(&old_set[0])));

        // An empty set of filters to replace is rejected
        let err = replace_sql_filters(&result.sql, &[], &new_set, meta, session)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("At least one filter"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_duplicate_filters() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        // The same filter expression appears twice in the WHERE clause
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
                AND KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1\
        ";

        // Deleting removes all equal filters
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Replacing replaces all equal filters, preserving positions
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["test".to_string()]),
                ..Default::default()
            },
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("notEquals".to_string()),
                values: Some(vec!["other".to_string()]),
                ..Default::default()
            },
        };
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
                AND KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_sql_filters() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1\
        ";

        // Set replaces all outermost filters with the new set
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["y".to_string()]),
            ..Default::default()
        }];
        let result = set_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'y' \
            GROUP BY 1\
            "
        );
        assert!(result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&filters[0])));
        assert!(!result
            .filters
            .iter()
            .any(|filter| filter.member.as_deref()
                == Some("KibanaSampleDataEcommerce.customer_gender")));

        // Setting an empty set clears all outermost filters
        let result = set_sql_filters(&result.sql, &[], meta, session).await?;
        assert_eq!(
            result.sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );
        assert!(result.filters.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_like_family_round_trip() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Several values are emitted as a boolean chain of one predicate per
        // value, which the planner reports as several single-value filters
        for operator in [
            "contains",
            "notContains",
            "startsWith",
            "notStartsWith",
            "endsWith",
            "notEndsWith",
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            let extracted = result
                .filters
                .iter()
                .map(filter_key)
                .collect::<HashSet<_>>();
            let expected = verification_keys(&filters[0]);
            assert!(
                expected.iter().all(|key| extracted.contains(key)),
                "{} filter did not round trip, wanted {:?}, got {:?}",
                operator,
                expected,
                result.filters
            );
            // The plain operators chain with OR and stay one group, the
            // negated ones chain with AND and are flattened into siblings
            assert_eq!(
                expected.len(),
                if operator.starts_with("not") { 3 } else { 1 },
                "unexpected verification keys for {}: {:?}",
                operator,
                expected
            );
        }

        // LIKE-family filters must survive the planner round trip, including
        // values with characters that have to be escaped in the pattern
        for (operator, value) in [
            ("contains", "abc"),
            ("startsWith", "pre"),
            ("endsWith", "end"),
            ("notContains", "x"),
            ("contains", "50%_off\\now"),
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&filters[0])),
                "{} filter on {:?} did not round trip, got {:?}",
                operator,
                value,
                result.filters
            );
        }

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_numeric_value_validation() {
        let ctx = get_test_tenant_ctx();
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Numeric literals are rendered verbatim, so anything that is not a
        // plain number must be rejected instead of reaching the SQL text
        for value in ["0 OR 1=1", "abc", "1; DROP TABLE x", "inf", "NaN", " 1", ""] {
            for operator in ["equals", "notEquals"] {
                let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some(operator.to_string()),
                    values: Some(vec![value.to_string()]),
                    ..Default::default()
                });
                let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
                assert!(
                    err.to_string().contains("must be numeric"),
                    "unexpected error for {} {:?}: {}",
                    operator,
                    value,
                    err
                );
            }
        }

        // Multi-value filters validate every value
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["1".to_string(), "2 OR 1=1".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("must be numeric"),
            "unexpected error: {}",
            err
        );

        // Well-formed numbers are still accepted
        for value in ["42", "-1", "3.14", "1e3"] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx).unwrap();
            assert!(applied);
            assert!(
                modified_sql.ends_with(&format!(
                    "WHERE KibanaSampleDataEcommerce.\"taxful_total_price\" = {} GROUP BY 1",
                    value
                )),
                "unexpected SQL for {:?}: {}",
                value,
                modified_sql
            );
        }
    }

    #[test]
    fn test_modify_sql_ast_or_clause_is_parenthesized() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        // AND-ing onto a top-level OR chain must not rebind the disjunction
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" = 'b' \
            GROUP BY 1\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" = 'b') \
                AND KibanaSampleDataEcommerce.\"notes\" = 'x' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_parenthesized_clause() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        // A redundantly parenthesized clause must not defeat lookup or removal
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
                AND KibanaSampleDataEcommerce.\"notes\" = 'x') \
            GROUP BY 1\
        ";
        let filter = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["a".to_string()]),
            ..Default::default()
        };

        // Already present: adding is a no-op
        let action = ModifyAction::Add(filter.clone());
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql.trim_end());
        assert!(!applied);

        // Removal descends into the parentheses. Those around the clause as a
        // whole say nothing about its structure, so they don't survive it
        let action = ModifyAction::Remove(filter);
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_cte_shadowing_cube_name() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        // The CTE is named after the cube, but exposes its own columns only
        let sql = "\
            WITH KibanaSampleDataEcommerce AS (\
                SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
            ) \
            SELECT gender FROM KibanaSampleDataEcommerce\
        ";

        // The cube's own column is not exposed by the CTE
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(&sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );

        // Deleting a filter whose member is not available is a no-op
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql.trim_end());
        assert!(!applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_unqualified_ref_in_join() {
        let ctx = get_test_tenant_ctx();
        // A bare column reference in a multi-relation FROM can't be attributed
        // to a specific cube, so it doesn't expose the member
        let sql = "\
            SELECT sub.customer_gender FROM (\
                SELECT customer_gender FROM KibanaSampleDataEcommerce \
                CROSS JOIN Logs\
            ) AS sub\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_delete_sql_filters_unresolvable_member() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // The member is not selected by the query, so there is nothing to
        // delete - that is a no-op rather than an error
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        }];
        let result = delete_sql_filters(sql, &filters, meta, session).await?;
        assert_eq!(result.sql, sql);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_measure_round_trip() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
        ";

        // Measure filters land in HAVING as a synthesized aggregation, which
        // has to be recognized by the filter rewrite rules
        for (member, operator, value) in [
            ("KibanaSampleDataEcommerce.maxPrice", "gt", "10"),
            ("KibanaSampleDataEcommerce.maxPrice", "equals", "42"),
            ("KibanaSampleDataEcommerce.count", "gte", "1"),
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some(member.to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert!(
                result.sql.contains("HAVING"),
                "{} {} filter did not produce a HAVING clause: {}",
                member,
                operator,
                result.sql
            );
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&filters[0])),
                "{} {} filter did not round trip, got {:?}",
                member,
                operator,
                result.filters
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_date_round_trip() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT order_date FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Date range filters are reconstructed from the time dimension of the
        // plan rather than from its filters, and their values are reshaped by
        // the planner
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter) == filter_key(&filters[0])),
            "inDateRange filter did not round trip, got {:?}",
            result.filters
        );

        // A negated range has to be emitted as NOT BETWEEN to be recognized
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("notInDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT order_date FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"order_date\" \
                NOT BETWEEN '2024-01-01' AND '2024-12-31T23:59:59.999' \
            GROUP BY 1\
            "
        );
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter) == filter_key(&filters[0])),
            "notInDateRange filter did not round trip, got {:?}",
            result.filters
        );

        // Single-bound date operators stay plain filters
        for (operator, value) in [
            ("beforeDate", "2024-06-01"),
            ("afterOrOnDate", "2024-07-01"),
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&filters[0])),
                "{} filter did not round trip, got {:?}",
                operator,
                result.filters
            );
        }

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_string_value_quoting() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Single quotes in string values must be doubled, so that the value
        // can't terminate the literal and inject SQL
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["O'Brien' OR 1=1 --".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'O''Brien'' OR 1=1 --' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // The rewritten SQL still parses as a single predicate
        let query = parse_single_query(&modified_sql)?;
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a plain SELECT");
        };
        assert!(matches!(
            select.selection,
            Some(ast::Expr::BinaryOp {
                op: ast::BinaryOperator::Eq,
                ..
            })
        ));

        // LIKE patterns quote the same way
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["O'Brien".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%O''Brien%' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_sql_filters_also_present_in_cte() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // The same filter is present both in the CTE and in the outermost
        // SELECT: deleting the outermost one is correct even though the plan
        // still carries the CTE's copy
        let sql = "\
            WITH recent AS (\
                SELECT customer_gender \
                FROM KibanaSampleDataEcommerce \
                WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
            ) \
            SELECT customer_gender FROM recent \
            WHERE recent.\"customer_gender\" = 'test' \
            GROUP BY 1\
        ";
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        }];
        let result = delete_sql_filters(sql, &filters, meta, session).await?;
        assert_eq!(
            result.sql,
            "\
            WITH recent AS (\
                SELECT customer_gender \
                FROM KibanaSampleDataEcommerce \
                WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
            ) \
            SELECT customer_gender FROM recent \
            GROUP BY 1\
            "
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_cte_name_case_mismatch() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        // Unquoted identifiers are case-insensitive: the CTE reference and its
        // declaration may differ in case
        let sql = "\
            WITH Gendered AS (\
                SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
            ) \
            SELECT gender FROM gendered\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            WITH Gendered AS (\
                SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
            ) \
            SELECT gender FROM gendered \
            WHERE gendered.gender = 'test'\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_removes_only_what_the_plan_reports() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // A filter the plan reports is what `set` removes
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' GROUP BY 1";
        let result = set_sql_filters(sql, &[], meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );

        // A predicate over a computed expression keeps the whole clause out
        // of the Cube query, so the plan reports no filter at all, and `set`
        // has nothing it may remove: the query keeps both predicates rather
        // than losing one on this API's own say-so
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE LOWER(customer_gender) = 'test' \
                   AND KibanaSampleDataEcommerce.\"customer_gender\" = 'test' GROUP BY 1";
        let result = set_sql_filters(sql, &[], meta, session).await?;
        assert_eq!(result.sql, sql);
        assert!(
            result.filters.is_empty(),
            "unexpected: {:?}",
            result.filters
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_identifier_case_mismatch() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        // The relation is matched case-insensitively, so the qualifier and the
        // column of the projection may be written in a different case
        let sql = "\
            SELECT customer_gender FROM (\
                SELECT KibanaSampleDataEcommerce.CUSTOMER_GENDER AS customer_gender \
                FROM kibanasampledataecommerce\
            ) AS t\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM (\
                SELECT KibanaSampleDataEcommerce.CUSTOMER_GENDER AS customer_gender \
                FROM kibanasampledataecommerce\
            ) AS t \
            WHERE t.customer_gender = 'test'\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_wildcard_over_join() {
        let ctx = get_test_tenant_ctx();
        // A wildcard over a join exposes columns of every relation, so a bare
        // dimension name can't be attributed to the cube
        let sql = "\
            SELECT customer_gender FROM (\
                SELECT * FROM KibanaSampleDataEcommerce CROSS JOIN Logs\
            ) AS sub \
            GROUP BY 1\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_modify_sql_ast_count_distinct_measure() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "SELECT content, COUNT(DISTINCT agentCount) FROM Logs GROUP BY 1";

        // `countDistinct` is how the meta API spells the aggregation
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("Logs.agentCount".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["10".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT content, COUNT(DISTINCT agentCount) FROM Logs \
            GROUP BY 1 \
            HAVING COUNT(DISTINCT Logs.\"agentCount\") > 10\
            "
        );
        assert!(applied);

        // `countDistinctApprox` has no exact SQL equivalent and stays on the
        // MEASURE path
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("Logs.agentCountApprox".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["10".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
        assert!(
            modified_sql.contains("HAVING MEASURE(Logs.\"agentCountApprox\") > 10"),
            "unexpected SQL: {}",
            modified_sql
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_delete_sql_filters_and_group() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // A top-level "and" group is flattened by the planner into sibling
        // filters, so it is verified through its members
        let filters = vec![and_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ])];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
                AND KibanaSampleDataEcommerce.\"notes\" = 'y') \
            GROUP BY 1\
            "
        );

        // A single-member "and" group is emitted as a plain filter
        let single = vec![and_group(vec![serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["z"],
        })])];
        let result = add_sql_filters(sql, &single, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
            GROUP BY 1\
            "
        );

        // A single-member "or" group is emitted as a plain filter too
        let single = vec![or_group(vec![serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["z"],
        })])];
        let result = add_sql_filters(sql, &single, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
            GROUP BY 1\
            "
        );

        // An "and" group nested inside an "or" survives as a group
        let nested = vec![or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "and": [
                    {
                        "member": "KibanaSampleDataEcommerce.notes",
                        "operator": "equals",
                        "values": ["y"],
                    },
                    {
                        "member": "KibanaSampleDataEcommerce.taxful_total_price",
                        "operator": "gt",
                        "values": ["1"],
                    },
                ],
            }),
        ])];
        let result = add_sql_filters(sql, &nested, meta, session).await?;
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter) == filter_key(&nested[0])),
            "nested group did not round trip, got {:?}",
            result.filters
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_multi_value_and_null_round_trip() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Multi-value equality becomes IN / NOT IN, and the null checks
        // become IS [NOT] NULL
        for (operator, values) in [
            ("equals", Some(vec!["a".to_string(), "b".to_string()])),
            ("notEquals", Some(vec!["a".to_string(), "b".to_string()])),
            ("set", None),
            ("notSet", None),
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some(operator.to_string()),
                values,
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&filters[0])),
                "{} filter did not round trip, got {:?}",
                operator,
                result.filters
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_nested_group_normalization() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // The normalizations that AST generation and the planner perform reach
        // every depth of a filter tree, not just its root
        let cases = vec![
            // A multi-value LIKE-family filter chains with OR, which the
            // engine collapses into the enclosing group
            (
                "multi-value contains inside an or",
                or_group(vec![
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.customer_gender",
                        "operator": "contains",
                        "values": ["a", "b"],
                    }),
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.notes",
                        "operator": "equals",
                        "values": ["y"],
                    }),
                ]),
            ),
            // The negated operators chain with AND, which survives as a group
            // of its own inside the enclosing or
            (
                "multi-value notContains inside an or",
                or_group(vec![
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.customer_gender",
                        "operator": "notContains",
                        "values": ["a", "b"],
                    }),
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.notes",
                        "operator": "equals",
                        "values": ["y"],
                    }),
                ]),
            ),
            // A single-member group is emitted as its member alone
            (
                "single-member and inside an or",
                or_group(vec![
                    serde_json::json!({
                        "and": [{
                            "member": "KibanaSampleDataEcommerce.notes",
                            "operator": "equals",
                            "values": ["y"],
                        }],
                    }),
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.customer_gender",
                        "operator": "equals",
                        "values": ["x"],
                    }),
                ]),
            ),
        ];

        for (label, filter) in cases {
            let filters = vec![filter];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            let extracted = result
                .filters
                .iter()
                .map(filter_key)
                .collect::<HashSet<_>>();
            let expected = verification_keys(&filters[0]);
            assert!(
                expected.iter().all(|key| extracted.contains(key)),
                "{} did not round trip, wanted {:?}, got {:?}",
                label,
                expected,
                result.filters
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_count_is_bounded() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let leaf = |i: usize| {
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": [format!("v{}", i)],
            })
        };

        // A group nests any number of leaves inside a single array entry, so
        // counting entries alone would let the bound be walked around
        let nested = vec![or_group((0..MAX_FILTERS + 1).map(leaf).collect())];
        let err = add_sql_filters(sql, &nested, meta.clone(), session.clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("At most"),
            "unexpected error: {}",
            err
        );

        // A group whose `and` is present but null is a plain `or` group, and
        // its members still count
        let null_and = vec![or_group(vec![serde_json::json!({
            "and": null,
            "or": (0..MAX_FILTERS + 1).map(leaf).collect::<Vec<_>>(),
        })])];
        let err = add_sql_filters(sql, &null_and, meta.clone(), session.clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("At most"),
            "unexpected error: {}",
            err
        );

        // Nesting a level deeper doesn't help either
        let deeply_nested = vec![or_group(vec![serde_json::json!({
            "and": (0..MAX_FILTERS + 1).map(leaf).collect::<Vec<_>>(),
        })])];
        let err = add_sql_filters(sql, &deeply_nested, meta.clone(), session.clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("At most"),
            "unexpected error: {}",
            err
        );

        // The bound applies to every operation that takes filters
        let flat = (0..MAX_FILTERS + 1)
            .map(|i| V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec![format!("v{}", i)]),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        for result in [
            set_sql_filters(sql, &flat, meta.clone(), session.clone()).await,
            delete_sql_filters(sql, &flat, meta.clone(), session.clone()).await,
            replace_sql_filters(sql, &flat, &[], meta.clone(), session.clone()).await,
            replace_sql_filters(sql, &flat[..1], &flat, meta.clone(), session.clone()).await,
        ] {
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("At most"),
                "unexpected error: {}",
                err
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_boolean_member() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT has_subscription FROM KibanaSampleDataEcommerce GROUP BY 1";
        let filter = |value: &str| V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.has_subscription".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        };

        // A boolean member is filtered with a boolean literal, which is what
        // the filter rewrite rules read
        for value in ["true", "false"] {
            let filters = vec![filter(value)];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert_eq!(
                result.sql,
                format!(
                    "SELECT has_subscription FROM KibanaSampleDataEcommerce \
                     WHERE KibanaSampleDataEcommerce.\"has_subscription\" = {} \
                     GROUP BY 1",
                    value
                )
            );
            assert!(
                result
                    .filters
                    .iter()
                    .any(|extracted| filter_key(extracted) == filter_key(&filters[0])),
                "{} filter did not round trip, got {:?}",
                value,
                result.filters
            );
        }

        // A value that is not a boolean is rejected rather than passed through
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(sql, &ModifyAction::Add(filter("notabool")), &ctx).unwrap_err();
        assert!(
            err.to_string().contains("must be a boolean"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_alias_quoting_is_preserved() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let action = || {
            ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["x".to_string()]),
                ..Default::default()
            })
        };

        // Quoting decides whether an identifier folds, so the alias is emitted
        // exactly as the projection wrote it - unquoted here,
        let sql = "\
            SELECT t.Gender FROM (\
                SELECT customer_gender AS Gender FROM KibanaSampleDataEcommerce\
            ) AS t \
            GROUP BY 1\
        ";
        let (modified_sql, applied) = modify_sql_ast(sql, &action(), &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT t.Gender FROM (\
                SELECT customer_gender AS Gender FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.Gender = 'x' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // and quoted there
        let sql = "\
            SELECT t.\"Gender\" FROM (\
                SELECT customer_gender AS \"Gender\" FROM KibanaSampleDataEcommerce\
            ) AS t \
            GROUP BY 1\
        ";
        let (modified_sql, applied) = modify_sql_ast(sql, &action(), &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT t.\"Gender\" FROM (\
                SELECT customer_gender AS \"Gender\" FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.\"Gender\" = 'x' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_cte_chain() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let gender = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        };

        // A member is followed through a chain of relations, each of which may
        // rename it, and the predicate names it as the outermost one does
        let cases = vec![
            (
                "WITH t0 AS (SELECT customer_gender FROM KibanaSampleDataEcommerce), \
                 t1 AS (SELECT customer_gender FROM t0) \
                 SELECT customer_gender FROM t1 GROUP BY 1",
                "t1.\"customer_gender\" = 'test'",
            ),
            (
                "WITH t0 AS (SELECT customer_gender FROM KibanaSampleDataEcommerce), \
                 t1 AS (SELECT customer_gender FROM t0), \
                 t2 AS (SELECT customer_gender FROM t1) \
                 SELECT customer_gender FROM t2 GROUP BY 1",
                "t2.\"customer_gender\" = 'test'",
            ),
            (
                "WITH t0 AS (SELECT customer_gender AS g FROM KibanaSampleDataEcommerce), \
                 t1 AS (SELECT g AS gg FROM t0) \
                 SELECT gg FROM t1 GROUP BY 1",
                "t1.gg = 'test'",
            ),
            (
                "SELECT t.customer_gender FROM (\
                     SELECT u.customer_gender FROM (\
                         SELECT customer_gender FROM KibanaSampleDataEcommerce\
                     ) AS u\
                 ) AS t GROUP BY 1",
                "t.customer_gender = 'test'",
            ),
        ];

        for (sql, predicate) in cases {
            let result =
                add_sql_filters(sql, slice::from_ref(&gender), meta.clone(), session.clone())
                    .await?;
            assert!(
                result.sql.contains(predicate),
                "expected {} in {}",
                predicate,
                result.sql
            );
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&gender)),
                "filter did not round trip through {}, got {:?}",
                sql,
                result.filters
            );

            // and deleting it puts the query back as it was
            let deleted = delete_sql_filters(
                &result.sql,
                slice::from_ref(&gender),
                meta.clone(),
                session.clone(),
            )
            .await?;
            assert!(
                !deleted.sql.contains(predicate),
                "predicate survived deletion in {}",
                deleted.sql
            );
        }

        // A measure aggregated at the bottom of a chain is forwarded the same way
        let max_price = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["10".to_string()]),
            ..Default::default()
        };
        let sql = "WITH t0 AS (\
                       SELECT customer_gender, MAX(maxPrice) AS mp \
                       FROM KibanaSampleDataEcommerce GROUP BY 1\
                   ), t1 AS (SELECT customer_gender, mp FROM t0) \
                   SELECT customer_gender, mp FROM t1";
        let result = add_sql_filters(
            sql,
            slice::from_ref(&max_price),
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert!(
            result.sql.contains("WHERE t1.mp > 10"),
            "unexpected SQL: {}",
            result.sql
        );

        // A column computed anywhere along the chain is still not a member
        let sql = "WITH t0 AS (\
                       SELECT LOWER(customer_gender) AS customer_gender \
                       FROM KibanaSampleDataEcommerce\
                   ), t1 AS (SELECT customer_gender FROM t0) \
                   SELECT customer_gender FROM t1 GROUP BY 1";
        let err = add_sql_filters(sql, slice::from_ref(&gender), meta, session)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    /// The native layer runs these on a multi-threaded runtime, so their
    /// futures have to stay `Send`. Nothing in the rewriting path may be held
    /// across an await - a `!Sync` value such as the relation-expansion
    /// budget would take `Send` away and this would stop compiling.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sql_filters_futures_are_send() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1".to_string();
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        }];

        let spawned = tokio::spawn({
            let (meta, session, sql, filters) =
                (meta.clone(), session.clone(), sql.clone(), filters.clone());
            async move {
                let added = add_sql_filters(&sql, &filters, meta.clone(), session.clone()).await?;
                let filters = get_sql_filters(&added.sql, meta.clone(), session.clone()).await?;
                let set =
                    set_sql_filters(&added.sql, &filters, meta.clone(), session.clone()).await?;
                let replaced = replace_sql_filters(
                    &set.sql,
                    &filters,
                    &filters,
                    meta.clone(),
                    session.clone(),
                )
                .await?;
                delete_sql_filters(&replaced.sql, &filters, meta, session).await
            }
        });

        let result = spawned
            .await
            .map_err(|e| CubeError::internal(format!("join error: {}", e)))??;
        assert_eq!(result.sql, sql);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_large_clause() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let filter = |i: usize| V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec![format!("v{}", i)]),
            ..Default::default()
        };

        // A clause holds a predicate per filter, so a batch the size of the
        // filter limit builds one longer than a recursive walk could carry
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let actions = (0..MAX_FILTERS)
            .map(|i| ModifyAction::Add(filter(i)))
            .collect::<Vec<_>>();
        let (built, applied) = modify_sql_ast_many(sql, &actions, &ctx, &ReportedFilters::none())?;
        assert!(applied.iter().all(|applied| *applied));
        assert_eq!(built.matches(" AND ").count(), MAX_FILTERS - 1);

        // and that clause can be walked again, to find and remove one filter,
        // and to remove every one of them as `set` does
        let (removed, applied) = modify_sql_ast(&built, &ModifyAction::Remove(filter(0)), &ctx)?;
        assert!(applied);
        assert_eq!(removed.matches(" AND ").count(), MAX_FILTERS - 2);
        let all = (0..MAX_FILTERS).map(filter).collect::<Vec<_>>();
        let (cleared, _) = modify_sql_ast_many(
            &built,
            &set_actions(&all, &[]),
            &ctx,
            &ReportedFilters::of(&all),
        )?;
        assert_eq!(
            cleared,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );

        // Past the clause bound the request is refused rather than attempted
        let mut clause = String::from("1 = 1");
        for i in 0..MAX_CLAUSE_PREDICATES {
            clause.push_str(&format!(" AND customer_gender = 'v{}'", i));
        }
        let sql = format!(
            "SELECT customer_gender FROM KibanaSampleDataEcommerce WHERE {} GROUP BY 1",
            clause
        );
        for result in [
            modify_sql_ast(&sql, &ModifyAction::Remove(filter(0)), &ctx).map(|(sql, _)| sql),
            modify_sql_ast(&sql, &ModifyAction::Add(filter(0)), &ctx).map(|(sql, _)| sql),
        ] {
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("more than the"),
                "unexpected error: {}",
                err
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_sibling_relation() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // The cube is in the FROM, but the projection reads the member from a
        // sibling relation, so finding the cube can't end the search
        let sql = "\
            SELECT x.g FROM (\
                SELECT t.g FROM KibanaSampleDataEcommerce k \
                JOIN (SELECT customer_gender AS g FROM KibanaSampleDataEcommerce) t ON true\
            ) x \
            GROUP BY 1\
        ";
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert!(
            result.sql.contains("WHERE x.g = 'test'"),
            "unexpected SQL: {}",
            result.sql
        );
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter) == filter_key(&filters[0])),
            "filter did not round trip, got {:?}",
            result.filters
        );

        // and what `add` can write, `set` may drop
        let cleared = set_sql_filters(&result.sql, &[], meta, session).await?;
        assert!(
            !cleared.sql.contains("WHERE"),
            "predicate survived set: {}",
            cleared.sql
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_group_as_whole_clause() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let group = and_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ]);
        let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // A group added to a query with no WHERE becomes the whole clause,
        // parentheses and all
        let (with_group, applied) = modify_sql_ast(base, &ModifyAction::Add(group.clone()), &ctx)?;
        assert_eq!(
            with_group,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
                AND KibanaSampleDataEcommerce.\"notes\" = 'y') \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // and is still matched as that group: adding it again is a no-op,
        let (again, applied) =
            modify_sql_ast(&with_group, &ModifyAction::Add(group.clone()), &ctx)?;
        assert_eq!(again, with_group);
        assert!(!applied);

        // deleting it takes the whole clause,
        let (deleted, applied) =
            modify_sql_ast(&with_group, &ModifyAction::Remove(group.clone()), &ctx)?;
        assert_eq!(deleted, base);
        assert!(applied);

        // and replacing it swaps the group rather than its members
        let action = ModifyAction::Replace {
            old: group,
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["z".to_string()]),
                ..Default::default()
            },
        };
        let (replaced, applied) = modify_sql_ast(&with_group, &action, &ctx)?;
        assert_eq!(
            replaced,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // A filter inside those parentheses is still reachable on its own
        let leaf = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        };
        let (leaf_deleted, applied) =
            modify_sql_ast(&with_group, &ModifyAction::Remove(leaf), &ctx)?;
        assert_eq!(
            leaf_deleted,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'y' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_group_member_reachability() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let group = and_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ]);
        let member = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        };
        let absent = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["absent".to_string()]),
            ..Default::default()
        };
        let other = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["c".to_string()]),
            ..Default::default()
        };

        let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let (alone, _) = modify_sql_ast(base, &ModifyAction::Add(group.clone()), &ctx)?;
        let (mixed, _) = modify_sql_ast(&alone, &ModifyAction::Add(other), &ctx)?;

        // The rewrite engine flattens a top-level "and" group into sibling
        // filters, so its members are filters of their own whether the group
        // stands alone in the clause or sits next to something else
        for clause in [&alone, &mixed] {
            // A member is removable on its own, which leaves its siblings
            let (removed, applied) =
                modify_sql_ast(clause, &ModifyAction::Remove(member.clone()), &ctx)?;
            assert!(applied);
            assert!(
                !removed.contains("\"customer_gender\" = 'x'"),
                "member survived removal: {}",
                removed
            );
            assert!(
                removed.contains("\"notes\" = 'y'"),
                "sibling was removed too: {}",
                removed
            );

            // and adding it back is a no-op rather than a duplicate
            let (added, applied) =
                modify_sql_ast(clause, &ModifyAction::Add(member.clone()), &ctx)?;
            assert_eq!(&added, clause);
            assert!(!applied);

            // The group is still removable as a whole
            let (removed, applied) =
                modify_sql_ast(clause, &ModifyAction::Remove(group.clone()), &ctx)?;
            assert!(applied);
            assert!(
                !removed.contains("\"customer_gender\" = 'x'")
                    && !removed.contains("\"notes\" = 'y'"),
                "group survived removal: {}",
                removed
            );

            // and a filter that isn't there leaves the clause as it was,
            // parentheses included
            let (untouched, applied) =
                modify_sql_ast(clause, &ModifyAction::Remove(absent.clone()), &ctx)?;
            assert_eq!(&untouched, clause);
            assert!(!applied);
        }

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_group_added_after_its_members() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let filter = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
            member: Some(format!("KibanaSampleDataEcommerce.{}", member)),
            operator: Some("equals".to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        };
        let group = and_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ]);

        // The rewrite engine reports a top-level "and" group as sibling
        // filters, so a clause that holds its members separately holds the
        // group itself, however the two got there
        let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let (with_a, _) = modify_sql_ast(
            base,
            &ModifyAction::Add(filter("customer_gender", "x")),
            &ctx,
        )?;
        let (members, _) = modify_sql_ast(&with_a, &ModifyAction::Add(filter("notes", "y")), &ctx)?;

        // Adding the group they make up is a no-op, not a duplicate
        let (added, applied) = modify_sql_ast(&members, &ModifyAction::Add(group.clone()), &ctx)?;
        assert_eq!(added, members);
        assert!(!applied);

        // Deleting it takes both members
        let (deleted, applied) =
            modify_sql_ast(&members, &ModifyAction::Remove(group.clone()), &ctx)?;
        assert_eq!(deleted, base);
        assert!(applied);

        // and replacing it drops them for the new filter
        let action = ModifyAction::Replace {
            old: group.clone(),
            new: filter("notes", "z"),
        };
        let (replaced, applied) = modify_sql_ast(&members, &action, &ctx)?;
        assert_eq!(
            replaced,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // With only part of the group present it is not present at all: the
        // filter is neither half-removed nor taken for a duplicate
        let (deleted, applied) =
            modify_sql_ast(&with_a, &ModifyAction::Remove(group.clone()), &ctx)?;
        assert_eq!(deleted, with_a);
        assert!(!applied);

        let (added, applied) = modify_sql_ast(&with_a, &ModifyAction::Add(group), &ctx)?;
        assert!(added.contains("AND (KibanaSampleDataEcommerce.\"customer_gender\" = 'x'"));
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_round_trip_reported_values() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // The planner canonicalizes a date to a full timestamp whatever the
        // query wrote, so the values are reported back in the query's own
        // form and a filter handed to delete or replace still matches
        let sql = "SELECT order_date FROM KibanaSampleDataEcommerce \
                   WHERE (KibanaSampleDataEcommerce.\"order_date\" >= '2020-01-01' \
                   AND KibanaSampleDataEcommerce.\"order_date\" <= '2021-01-01') \
                   GROUP BY 1";
        let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
        assert_eq!(
            reported[0].values,
            Some(vec!["2020-01-01".to_string(), "2021-01-01".to_string()]),
            "a date is reported in the form the query wrote it"
        );

        // Replacing it with an edited range keeps the query rewritable
        let edited = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2020-01-02".to_string(), "2021-01-01".to_string()]),
            ..Default::default()
        };
        let replaced = replace_sql_filters(
            sql,
            &reported,
            slice::from_ref(&edited),
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert!(
            replaced.sql.contains("'2020-01-02'"),
            "unexpected SQL: {}",
            replaced.sql
        );

        // and deleting it takes the range it came from
        let deleted = delete_sql_filters(sql, &reported, meta.clone(), session.clone()).await?;
        assert_eq!(
            deleted.sql,
            "SELECT order_date FROM KibanaSampleDataEcommerce GROUP BY 1"
        );

        // A value carrying a real time is not a date, and is left as it is
        let sql = "SELECT order_date FROM KibanaSampleDataEcommerce \
                   WHERE (KibanaSampleDataEcommerce.\"order_date\" >= '2020-01-01T12:30:00.000' \
                   AND KibanaSampleDataEcommerce.\"order_date\" <= '2021-01-01T23:59:59.999') \
                   GROUP BY 1";
        let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
        let values = reported[0].values.clone().unwrap_or_default();
        assert!(
            values[0].starts_with("2020-01-01T12:30:00") && values[1] == "2021-01-01",
            "a timestamp was reported as something else: {:?}",
            values
        );

        // and it still matches the query it was read from
        let deleted = delete_sql_filters(sql, &reported, meta, session).await?;
        assert_eq!(
            deleted.sql,
            "SELECT order_date FROM KibanaSampleDataEcommerce GROUP BY 1"
        );

        Ok(())
    }

    /// A column can carry more than one reported filter, and then no filter
    /// owns it: matching by column would take the other one with it, which
    /// nothing in the response would say.
    #[tokio::test]
    async fn test_delete_keeps_a_second_filter_on_the_column() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT order_date FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"order_date\" \
                   >= CURRENT_DATE - INTERVAL '30 days' \
                   AND KibanaSampleDataEcommerce.\"order_date\" IS NOT NULL \
                   GROUP BY 1";
        let reported = get_sql_filters(sql, meta.clone(), session.clone()).await?;
        let operators = reported
            .iter()
            .filter_map(|filter| filter.operator.clone())
            .collect::<HashSet<_>>();
        assert_eq!(
            operators,
            HashSet::from(["afterOrOnDate".to_string(), "set".to_string()]),
            "the query should report two filters on one member: {:?}",
            reported
        );

        // The engine works the interval out when it plans, so the bound is
        // reported as a date the query does not hold and can only be matched
        // by the column it stands on
        let bound = reported
            .iter()
            .find(|filter| filter.operator.as_deref() == Some("afterOrOnDate"))
            .expect("the bound is reported")
            .clone();
        let deleted =
            delete_sql_filters(sql, slice::from_ref(&bound), meta.clone(), session.clone()).await?;
        assert!(
            deleted.sql.contains("IS NOT NULL"),
            "the filter that was not asked for was removed: {}",
            deleted.sql
        );
        assert!(
            deleted
                .filters
                .iter()
                .any(|filter| filter.operator.as_deref() == Some("set")),
            "unexpected filters: {:?}",
            deleted.filters
        );

        Ok(())
    }

    /// A date is only two ways of writing one value on a member that holds a
    /// time. On a string member the two are two values, and a filter naming
    /// one must not reach the other.
    #[tokio::test]
    async fn test_delete_keeps_a_date_shaped_string() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"customer_gender\" = '2024-01-01' \
                   GROUP BY 1";
        let canonical = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["2024-01-01T00:00:00.000Z".to_string()]),
            ..Default::default()
        };

        let deleted = delete_sql_filters(
            sql,
            slice::from_ref(&canonical),
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert!(
            deleted.sql.contains("'2024-01-01'"),
            "a string was matched by a date written another way: {}",
            deleted.sql
        );

        Ok(())
    }

    /// The filter being added by a replacement is matched under the member it
    /// stands on, not under the one it replaces: what counts as the same
    /// value differs between a time member and any other.
    #[tokio::test]
    async fn test_replace_adds_the_new_filter_under_its_own_member() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // The date is written the way the planner canonicalizes it, and the
        // group being replaced stands on string members alone
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'FEMALE' \
                   AND KibanaSampleDataEcommerce.\"notes\" = 'x' \
                   AND KibanaSampleDataEcommerce.\"order_date\" >= '2020-01-01T00:00:00.000Z' \
                   GROUP BY 1";
        let old = V1LoadRequestQueryFilterItem {
            and: Some(vec![
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.customer_gender",
                    "operator": "equals",
                    "values": ["FEMALE"],
                }),
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.notes",
                    "operator": "equals",
                    "values": ["x"],
                }),
            ]),
            ..Default::default()
        };
        let new = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("afterOrOnDate".to_string()),
            values: Some(vec!["2020-01-01".to_string()]),
            ..Default::default()
        };

        let replaced = replace_sql_filters(
            sql,
            slice::from_ref(&old),
            slice::from_ref(&new),
            meta,
            session,
        )
        .await?;
        assert_eq!(
            replaced.sql.matches("order_date").count(),
            1,
            "the date the query already holds was written a second time: {}",
            replaced.sql
        );

        Ok(())
    }

    /// A CTE is only ever named unqualified: `public.orders` is the cube even
    /// when a CTE is called `orders`, and the recognizer has to read it the
    /// way the resolver does, or `set` keeps a filter it was asked to replace.
    #[tokio::test]
    async fn test_set_reads_a_qualified_relation_as_the_cube() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "WITH KibanaSampleDataEcommerce AS (SELECT LOWER(content) AS customer_gender FROM Logs) \
                   SELECT customer_gender, MEASURE(count) FROM public.KibanaSampleDataEcommerce \
                   WHERE customer_gender = 'female' GROUP BY 1";
        let wanted = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["male".to_string()]),
            ..Default::default()
        };

        let set = set_sql_filters(sql, slice::from_ref(&wanted), meta, session).await?;
        assert_eq!(
            set.filters.iter().map(filter_key).collect::<Vec<_>>(),
            vec![filter_key(&wanted)],
            "the filter on the cube was kept as if it were the CTE's: {}",
            set.sql
        );

        Ok(())
    }

    /// A batch keys the clause once and looks each addition up in the result,
    /// so what an addition sees has to be exactly what re-keying would show:
    /// the filters the query had, the ones added before it, and the members
    /// of any group among them.
    #[test]
    fn test_add_batch_sees_earlier_additions() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let gender = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["female".to_string()]),
            ..Default::default()
        };
        let notes = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        };
        let group = V1LoadRequestQueryFilterItem {
            and: Some(vec![
                serde_json::to_value(&gender).unwrap(),
                serde_json::to_value(&notes).unwrap(),
            ]),
            ..Default::default()
        };

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' GROUP BY 1";
        let actions = vec![
            // Already there
            ModifyAction::Add(notes.clone()),
            // New
            ModifyAction::Add(gender.clone()),
            // The same one again, within the batch
            ModifyAction::Add(gender.clone()),
            // Its members are all present by now
            ModifyAction::Add(group),
        ];
        let (built, applied) = modify_sql_ast_many(sql, &actions, &ctx, &ReportedFilters::none())?;

        assert_eq!(applied, vec![false, true, false, false]);
        assert_eq!(
            built,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce \
             WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' \
             AND KibanaSampleDataEcommerce.\"customer_gender\" = 'female' GROUP BY 1"
        );

        Ok(())
    }

    /// The key sets a batch keeps are per way of keying, and an append has to
    /// reach every one of them: a group with a time member is keyed with dates
    /// normalized, while a plain leaf is not, and the leaf must still see the
    /// group's members.
    #[test]
    fn test_add_batch_sees_additions_keyed_the_other_way() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let leaf = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
            member: Some(format!("KibanaSampleDataEcommerce.{member}")),
            operator: Some("equals".to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        };
        let range = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-02-01".to_string()]),
            ..Default::default()
        };
        let group = V1LoadRequestQueryFilterItem {
            and: Some(vec![
                serde_json::to_value(&range).unwrap(),
                serde_json::to_value(leaf("notes", "y")).unwrap(),
            ]),
            ..Default::default()
        };

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' GROUP BY 1";
        let actions = vec![
            // Keyed without date normalization
            ModifyAction::Add(leaf("customer_gender", "female")),
            // Keyed with it, as a time member is among its members
            ModifyAction::Add(group),
            // Keyed without it again, and already there through the group
            ModifyAction::Add(leaf("notes", "y")),
        ];
        let (built, applied) = modify_sql_ast_many(sql, &actions, &ctx, &ReportedFilters::none())?;

        assert_eq!(applied, vec![true, true, false]);
        assert_eq!(
            built.matches("'y'").count(),
            1,
            "a filter the group brought in was appended again: {built}"
        );

        Ok(())
    }

    /// Matching by column is the only path that removes predicates the caller
    /// did not name, so its gate is pinned directly: it takes a filter the
    /// plan reports alone on its member, and nothing else.
    #[test]
    fn test_remove_by_column_only_for_the_sole_reported_filter() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let range = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-02-01".to_string()]),
            ..Default::default()
        };
        let set = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("set".to_string()),
            ..Default::default()
        };
        // The upper bound is strict, so the range as reported is not spelled
        // out by the query and can only be matched by its column
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"order_date\" >= '2024-01-01' \
                   AND KibanaSampleDataEcommerce.\"order_date\" < '2024-02-01' GROUP BY 1";
        let remove = [ModifyAction::Remove(range.clone())];

        // Reported, and alone on its member: both bounds go
        let (built, applied) = modify_sql_ast_many(
            sql,
            &remove,
            &ctx,
            &ReportedFilters::of(slice::from_ref(&range)),
        )?;
        assert_eq!(applied, vec![true]);
        assert_eq!(
            built,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );

        // Not reported: nothing is taken, though the column matches
        let (built, applied) = modify_sql_ast_many(sql, &remove, &ctx, &ReportedFilters::none())?;
        assert_eq!(applied, vec![false]);
        assert_eq!(built, sql);

        // Reported alongside a second filter on the member: nothing is taken
        let sql_with_set = format!(
            "{} AND KibanaSampleDataEcommerce.\"order_date\" IS NOT NULL",
            &sql[..sql.len() - " GROUP BY 1".len()]
        ) + " GROUP BY 1";
        let (built, applied) = modify_sql_ast_many(
            &sql_with_set,
            &remove,
            &ctx,
            &ReportedFilters::of(&[range, set]),
        )?;
        assert_eq!(applied, vec![false]);
        assert_eq!(built, sql_with_set);

        Ok(())
    }

    /// `set` removes what the plan reports and nothing else, so a predicate
    /// extraction cannot represent - a LIKE with an inner wildcard - stays,
    /// and the rewritten query never returns rows the original did not.
    #[test]
    fn test_set_leaves_a_predicate_the_plan_does_not_report() -> DFResult<()> {
        let ctx = get_test_tenant_ctx();
        let gender = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["female".to_string()]),
            ..Default::default()
        };
        let notes = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        };

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"notes\" LIKE 'a%b' \
                   AND KibanaSampleDataEcommerce.\"customer_gender\" = 'female' GROUP BY 1";
        // What the plan reports: the LIKE has no Cube filter to become
        let reported = [gender];
        let actions = set_actions(&reported, slice::from_ref(&notes));
        let (built, applied) =
            modify_sql_ast_many(sql, &actions, &ctx, &ReportedFilters::of(&reported))?;

        assert_eq!(applied, vec![true, true]);
        assert_eq!(
            built,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce \
             WHERE KibanaSampleDataEcommerce.\"notes\" LIKE 'a%b' \
             AND KibanaSampleDataEcommerce.\"notes\" = 'x' GROUP BY 1"
        );

        Ok(())
    }

    /// A filter listed twice in `old` is one filter to replace, not one found
    /// and one missing.
    #[tokio::test]
    async fn test_replace_with_a_duplicated_old_filter() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce \
                   WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'female' GROUP BY 1";
        let old = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["female".to_string()]),
            ..Default::default()
        };
        let new = V1LoadRequestQueryFilterItem {
            values: Some(vec!["male".to_string()]),
            ..old.clone()
        };

        let replaced = replace_sql_filters(
            sql,
            &[old.clone(), old],
            slice::from_ref(&new),
            meta,
            session,
        )
        .await?;
        assert!(
            replaced.sql.contains("'male'") && !replaced.sql.contains("'female'"),
            "unexpected SQL: {}",
            replaced.sql
        );

        Ok(())
    }

    /// cubesql resolves an unquoted identifier case-insensitively, so a query
    /// writing a camelCase member bare names the same column this API writes
    /// quoted, and a filter read back has to find it.
    #[test]
    fn test_remove_matches_an_identifier_whatever_its_case() -> DFResult<()> {
        use crate::compile::test::get_test_tenant_ctx_with_meta;
        use cubeclient::models::{V1CubeMeta, V1CubeMetaType};

        // The stock context has camelCase measures but no camelCase dimension
        let ctx = get_test_tenant_ctx_with_meta(vec![V1CubeMeta {
            name: "CamelCube".to_string(),
            description: None,
            title: None,
            r#type: V1CubeMetaType::Cube,
            dimensions: vec![V1CubeMetaDimension {
                name: "CamelCube.someString".to_string(),
                r#type: "string".to_string(),
                ..Default::default()
            }],
            measures: vec![V1CubeMetaMeasure {
                name: "CamelCube.maxPrice".to_string(),
                r#type: "number".to_string(),
                agg_type: Some("max".to_string()),
                ..Default::default()
            }],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        }]);

        let dimension = V1LoadRequestQueryFilterItem {
            member: Some("CamelCube.someString".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        };
        for spelling in [
            "someString",
            "SOMESTRING",
            "\"someString\"",
            "CamelCube.somestring",
        ] {
            let sql = format!("SELECT someString FROM CamelCube WHERE {spelling} = 'x' GROUP BY 1");
            let (built, applied) =
                modify_sql_ast(&sql, &ModifyAction::Remove(dimension.clone()), &ctx)?;
            assert!(applied, "not found as {}: {}", spelling, built);
            assert_eq!(built, "SELECT someString FROM CamelCube GROUP BY 1");
        }

        let measure = V1LoadRequestQueryFilterItem {
            member: Some("CamelCube.maxPrice".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["5".to_string()]),
            ..Default::default()
        };
        for spelling in ["MEASURE(maxPrice)", "MEASURE(MAXPRICE)", "MAX(maxprice)"] {
            let sql = format!("SELECT someString FROM CamelCube GROUP BY 1 HAVING {spelling} > 5");
            let (built, applied) =
                modify_sql_ast(&sql, &ModifyAction::Remove(measure.clone()), &ctx)?;
            assert!(applied, "not found as {}: {}", spelling, built);
            assert_eq!(built, "SELECT someString FROM CamelCube GROUP BY 1");
        }

        Ok(())
    }

    /// A range's upper bound is written the way the REST API reads a bare
    /// date - the whole of that day - and folds back to the date when
    /// reported, so the two paths filter the same rows and a filter read back
    /// still matches the one that was sent.
    #[test]
    fn test_date_range_upper_bound_covers_the_day() {
        assert_eq!(date_range_upper("2024-12-31"), "2024-12-31T23:59:59.999");
        // A bound already carrying a time is left as written
        assert_eq!(
            date_range_upper("2024-12-31T12:00:00"),
            "2024-12-31T12:00:00"
        );
        assert_eq!(date_range_upper("last week"), "last week");

        assert_eq!(report_date_value("2024-12-31T23:59:59.999"), "2024-12-31");
        assert_eq!(report_date_value("2024-12-31T00:00:00.000Z"), "2024-12-31");
        assert_eq!(
            report_date_value("2024-12-31T12:00:00.000"),
            "2024-12-31T12:00:00.000"
        );
        assert_eq!(
            normalize_filter_value("2024-12-31T23:59:59.999"),
            normalize_filter_value("2024-12-31")
        );
    }

    /// A statement that compiles to something other than a plan - SET, SHOW,
    /// BEGIN - is the caller's mistake, answered as such rather than as a
    /// fault of this API.
    #[tokio::test]
    async fn test_non_select_statement_is_a_caller_error() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};
        use crate::CubeErrorCauseType;

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let err = get_sql_filters("SET timezone = 'UTC'", meta, session)
            .await
            .expect_err("a SET statement has no filters to read");
        assert!(
            matches!(err.cause, CubeErrorCauseType::User(_)),
            "not a caller error: {:?}",
            err
        );
        assert!(err.message.contains("Only SELECT"), "{}", err.message);

        Ok(())
    }

    /// The filter bound is per request: `replace` counts its old and its new
    /// filters together, not each array against the bound on its own.
    #[tokio::test]
    async fn test_replace_counts_old_and_new_filters_together() -> Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
        let filters = |count: usize| {
            (0..count)
                .map(|i| V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                    operator: Some("equals".to_string()),
                    values: Some(vec![format!("v{}", i)]),
                    ..Default::default()
                })
                .collect::<Vec<_>>()
        };
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Each array alone is under the bound; together they are over it
        let half = MAX_FILTERS / 2 + 1;
        let err = replace_sql_filters(sql, &filters(half), &filters(half), meta, session)
            .await
            .expect_err("old and new are one request");
        assert!(err.message.contains("At most"), "{}", err.message);

        Ok(())
    }
}
