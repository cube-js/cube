//! Rendering a Cube filter as a `sqlparser` expression over the relation
//! that holds its member, and the clause it belongs in.

use super::{
    matching::ClauseKind,
    report::{date_range_upper, filter_description},
    resolve::{resolve_member_source, MemberSource, MetaMember},
};
use crate::transport::MetaContext;
use cubeclient::models::V1LoadRequestQueryFilterItem;
use datafusion::error::{DataFusionError, Result as DFResult};
use sqlparser::ast;

#[derive(Debug)]
pub(super) enum ModifyAction {
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

/// Same as [`resolve_filter_expr`], but a member that is not available in the
/// outermost SELECT is an error rather than `None`.
pub(super) fn require_filter_expr(
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
pub(super) fn resolve_filter_expr(
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
