use std::collections::{HashMap, HashSet};

use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::{
            Aggregate, Analyze, CrossJoin, Distinct, Explain, Filter, Join, Limit, Projection,
            Repartition, Sort, Subquery, TableUDFs, Union, Window,
        },
        Column, DFSchema, Expr, Like, LogicalPlan,
    },
    physical_plan::functions::Volatility,
};

#[cfg(test)]
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    logical_plan::LogicalPlanBuilder,
};

/// Recursively rewrites an expression using the provided rewrite map. If the expression is explicitly
/// marked as non-rewrittable (maps to `None`), returns `None`, otherwise returns the expression.
/// If the provided rewrite map lacks the key for an expression, returns an error.
pub fn rewrite(expr: &Expr, map: &HashMap<Column, Option<Expr>>) -> Result<Option<Expr>> {
    Ok(match expr {
        Expr::Alias(expr, name) => {
            rewrite(expr, map)?.map(|expr| Expr::Alias(Box::new(expr), name.clone()))
        }
        // Outer columns may be missing from the rewrite map, so no rewrite is assumed
        Expr::OuterColumn(_, _) => None,
        Expr::Column(column) => map
            .get(column)
            .ok_or(DataFusionError::Internal(format!(
                "Unable to optimize expression: {:?} missing in rewrite map",
                column,
            )))
            .cloned()?,
        expr @ Expr::ScalarVariable(_, _) => Some(expr.clone()),
        expr @ Expr::Literal(_) => Some(expr.clone()),
        Expr::BinaryExpr { left, op, right } => {
            let rewrites = match (rewrite(left, map)?, rewrite(right, map)?) {
                (Some(left), Some(right)) => Some((left, right)),
                _ => None,
            };
            rewrites.map(|(left, right)| Expr::BinaryExpr {
                left: Box::new(left),
                op: op.clone(),
                right: Box::new(right),
            })
        }
        Expr::AnyExpr { left, op, right } => {
            let rewrites = match (rewrite(left, map)?, rewrite(right, map)?) {
                (Some(left), Some(right)) => Some((left, right)),
                _ => None,
            };
            rewrites.map(|(left, right)| Expr::AnyExpr {
                left: Box::new(left),
                op: op.clone(),
                right: Box::new(right),
            })
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let rewrites = match (rewrite(expr, map)?, rewrite(pattern, map)?) {
                (Some(expr), Some(pattern)) => Some((expr, pattern)),
                _ => None,
            };
            rewrites.map(|(expr, pattern)| {
                Expr::Like(Like {
                    negated: negated.clone(),
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    escape_char: escape_char.clone(),
                })
            })
        }
        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let rewrites = match (rewrite(expr, map)?, rewrite(pattern, map)?) {
                (Some(expr), Some(pattern)) => Some((expr, pattern)),
                _ => None,
            };
            rewrites.map(|(expr, pattern)| {
                Expr::ILike(Like {
                    negated: negated.clone(),
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    escape_char: escape_char.clone(),
                })
            })
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let rewrites = match (rewrite(expr, map)?, rewrite(pattern, map)?) {
                (Some(expr), Some(pattern)) => Some((expr, pattern)),
                _ => None,
            };
            rewrites.map(|(expr, pattern)| {
                Expr::SimilarTo(Like {
                    negated: negated.clone(),
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    escape_char: escape_char.clone(),
                })
            })
        }
        Expr::Not(expr) => rewrite(expr, map)?.map(|expr| Expr::Not(Box::new(expr))),
        Expr::IsNotNull(expr) => rewrite(expr, map)?.map(|expr| Expr::IsNotNull(Box::new(expr))),
        Expr::IsNull(expr) => rewrite(expr, map)?.map(|expr| Expr::IsNull(Box::new(expr))),
        Expr::Negative(expr) => rewrite(expr, map)?.map(|expr| Expr::Negative(Box::new(expr))),
        Expr::GetIndexedField { expr, key } => {
            let rewrites = match (rewrite(expr, map)?, rewrite(key, map)?) {
                (Some(expr), Some(key)) => Some((expr, key)),
                _ => None,
            };
            rewrites.map(|(expr, key)| Expr::GetIndexedField {
                expr: Box::new(expr),
                key: Box::new(key),
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let rewrites = match (rewrite(expr, map)?, rewrite(low, map)?, rewrite(high, map)?) {
                (Some(expr), Some(low), Some(high)) => Some((expr, low, high)),
                _ => None,
            };
            rewrites.map(|(expr, low, high)| Expr::Between {
                expr: Box::new(expr),
                negated: negated.clone(),
                low: Box::new(low),
                high: Box::new(high),
            })
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let expr = match expr {
                Some(expr) => match rewrite(expr, map)? {
                    Some(expr) => Some(expr),
                    _ => return Ok(None),
                },
                _ => None,
            };
            let when_then_expr = when_then_expr
                .iter()
                .map(
                    |(when, then)| match (rewrite(when, map), rewrite(then, map)) {
                        (Err(err), _) | (Ok(_), Err(err)) => Err(err),
                        (Ok(when), Ok(then)) => Ok(match (when, then) {
                            (Some(when), Some(then)) => Some((when, then)),
                            _ => None,
                        }),
                    },
                )
                .collect::<Result<Option<Vec<_>>>>()?;
            if when_then_expr.is_none() {
                return Ok(None);
            }
            let when_then_expr = when_then_expr.unwrap();
            let else_expr = match else_expr {
                Some(else_expr) => match rewrite(else_expr, map)? {
                    Some(else_expr) => Some(else_expr),
                    _ => return Ok(None),
                },
                _ => None,
            };
            Some(Expr::Case {
                expr: expr.map(|expr| Box::new(expr)),
                when_then_expr: when_then_expr
                    .iter()
                    .map(|(when, then)| (Box::new(when.clone()), Box::new(then.clone())))
                    .collect(),
                else_expr: else_expr.map(|else_expr| Box::new(else_expr)),
            })
        }
        Expr::Cast { expr, data_type } => rewrite(expr, map)?.map(|expr| Expr::Cast {
            expr: Box::new(expr),
            data_type: data_type.clone(),
        }),
        Expr::TryCast { expr, data_type } => rewrite(expr, map)?.map(|expr| Expr::TryCast {
            expr: Box::new(expr),
            data_type: data_type.clone(),
        }),
        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => rewrite(expr, map)?.map(|expr| Expr::Sort {
            expr: Box::new(expr),
            asc: asc.clone(),
            nulls_first: nulls_first.clone(),
        }),
        Expr::ScalarFunction { fun, args } => args
            .iter()
            .map(|arg| rewrite(arg, map))
            .collect::<Result<Option<Vec<_>>>>()?
            .map(|args| Expr::ScalarFunction {
                fun: fun.clone(),
                args,
            }),
        Expr::ScalarUDF { fun, args } => args
            .iter()
            .map(|arg| rewrite(arg, map))
            .collect::<Result<Option<Vec<_>>>>()?
            .map(|args| Expr::ScalarUDF {
                fun: fun.clone(),
                args,
            }),
        Expr::TableUDF { fun, args } => args
            .iter()
            .map(|arg| rewrite(arg, map))
            .collect::<Result<Option<Vec<_>>>>()?
            .map(|args| Expr::TableUDF {
                fun: fun.clone(),
                args,
            }),
        Expr::AggregateFunction {
            fun,
            args,
            distinct,
        } => args
            .iter()
            .map(|arg| rewrite(arg, map))
            .collect::<Result<Option<Vec<_>>>>()?
            .map(|args| Expr::AggregateFunction {
                fun: fun.clone(),
                args,
                distinct: distinct.clone(),
            }),
        Expr::WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        } => {
            let args = args
                .iter()
                .map(|arg| rewrite(arg, map))
                .collect::<Result<Option<Vec<_>>>>()?;
            if args.is_none() {
                return Ok(None);
            }
            let args = args.unwrap();
            let partition_by = partition_by
                .iter()
                .map(|partition_by| rewrite(partition_by, map))
                .collect::<Result<Option<Vec<_>>>>()?;
            if partition_by.is_none() {
                return Ok(None);
            }
            let partition_by = partition_by.unwrap();
            order_by
                .iter()
                .map(|order_by| rewrite(order_by, map))
                .collect::<Result<Option<Vec<_>>>>()?
                .map(|order_by| Expr::WindowFunction {
                    fun: fun.clone(),
                    args,
                    partition_by,
                    order_by,
                    window_frame: window_frame.clone(),
                })
        }
        Expr::AggregateUDF { fun, args } => args
            .iter()
            .map(|arg| rewrite(arg, map))
            .collect::<Result<Option<Vec<_>>>>()?
            .map(|args| Expr::AggregateUDF {
                fun: fun.clone(),
                args,
            }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = rewrite(expr, map)?;
            if expr.is_none() {
                return Ok(None);
            }
            let expr = expr.unwrap();
            list.iter()
                .map(|item| rewrite(item, map))
                .collect::<Result<Option<Vec<_>>>>()?
                .map(|list| Expr::InList {
                    expr: Box::new(expr),
                    list,
                    negated: negated.clone(),
                })
        }
        // As rewrites are used to push things down or up the plan, wildcards
        // might change the selection and should be marked as non-rewrittable
        Expr::Wildcard | Expr::QualifiedWildcard { .. } => None,
    })
}

/// Recursively rewrites an expression's column names using the provided rewrite map.
/// If any rewrites were applied, returns the new expression wrapped in Some.
/// Otherwise, returns None.
pub fn rewrite_columns(expr: &Expr, map: &HashMap<String, String>) -> Option<Expr> {
    match expr {
        Expr::Alias(expr, name) => {
            rewrite_columns(expr, map).map(|expr| Expr::Alias(Box::new(expr), name.clone()))
        }
        // Outer columns are not considered
        Expr::OuterColumn(_, _) => None,
        Expr::Column(column) => {
            if column.relation.is_some() {
                None
            } else {
                map.get(&column.name).map(|name| {
                    Expr::Column(Column {
                        relation: None,
                        name: name.clone(),
                    })
                })
            }
        }
        Expr::ScalarVariable(_, _) => None,
        Expr::Literal(_) => None,
        Expr::BinaryExpr { left, op, right } => {
            let rewrites = match (rewrite_columns(left, map), rewrite_columns(right, map)) {
                (None, None) => None,
                (m_left, m_right) => Some((
                    m_left
                        .map(|left| Box::new(left))
                        .unwrap_or_else(|| left.clone()),
                    m_right
                        .map(|right| Box::new(right))
                        .unwrap_or_else(|| right.clone()),
                )),
            };
            rewrites.map(|(left, right)| Expr::BinaryExpr {
                left,
                op: op.clone(),
                right,
            })
        }
        Expr::AnyExpr { left, op, right } => {
            let rewrites = match (rewrite_columns(left, map), rewrite_columns(right, map)) {
                (None, None) => None,
                (m_left, m_right) => Some((
                    m_left
                        .map(|left| Box::new(left))
                        .unwrap_or_else(|| left.clone()),
                    m_right
                        .map(|right| Box::new(right))
                        .unwrap_or_else(|| right.clone()),
                )),
            };
            rewrites.map(|(left, right)| Expr::AnyExpr {
                left,
                op: op.clone(),
                right,
            })
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let rewrites = match (rewrite_columns(expr, map), rewrite_columns(pattern, map)) {
                (None, None) => None,
                (m_expr, m_pattern) => Some((
                    m_expr
                        .map(|expr| Box::new(expr))
                        .unwrap_or_else(|| expr.clone()),
                    m_pattern
                        .map(|pattern| Box::new(pattern))
                        .unwrap_or_else(|| pattern.clone()),
                )),
            };
            rewrites.map(|(expr, pattern)| {
                Expr::Like(Like {
                    negated: negated.clone(),
                    expr,
                    pattern,
                    escape_char: escape_char.clone(),
                })
            })
        }
        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let rewrites = match (rewrite_columns(expr, map), rewrite_columns(pattern, map)) {
                (None, None) => None,
                (m_expr, m_pattern) => Some((
                    m_expr
                        .map(|expr| Box::new(expr))
                        .unwrap_or_else(|| expr.clone()),
                    m_pattern
                        .map(|pattern| Box::new(pattern))
                        .unwrap_or_else(|| pattern.clone()),
                )),
            };
            rewrites.map(|(expr, pattern)| {
                Expr::ILike(Like {
                    negated: negated.clone(),
                    expr,
                    pattern,
                    escape_char: escape_char.clone(),
                })
            })
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let rewrites = match (rewrite_columns(expr, map), rewrite_columns(pattern, map)) {
                (None, None) => None,
                (m_expr, m_pattern) => Some((
                    m_expr
                        .map(|expr| Box::new(expr))
                        .unwrap_or_else(|| expr.clone()),
                    m_pattern
                        .map(|pattern| Box::new(pattern))
                        .unwrap_or_else(|| pattern.clone()),
                )),
            };
            rewrites.map(|(expr, pattern)| {
                Expr::SimilarTo(Like {
                    negated: negated.clone(),
                    expr,
                    pattern,
                    escape_char: escape_char.clone(),
                })
            })
        }
        Expr::Not(expr) => rewrite_columns(expr, map).map(|expr| Expr::Not(Box::new(expr))),
        Expr::IsNotNull(expr) => {
            rewrite_columns(expr, map).map(|expr| Expr::IsNotNull(Box::new(expr)))
        }
        Expr::IsNull(expr) => rewrite_columns(expr, map).map(|expr| Expr::IsNull(Box::new(expr))),
        Expr::Negative(expr) => {
            rewrite_columns(expr, map).map(|expr| Expr::Negative(Box::new(expr)))
        }
        Expr::GetIndexedField { expr, key } => {
            let rewrites = match (rewrite_columns(expr, map), rewrite_columns(key, map)) {
                (None, None) => None,
                (m_expr, m_key) => Some((
                    m_expr
                        .map(|expr| Box::new(expr))
                        .unwrap_or_else(|| expr.clone()),
                    m_key
                        .map(|key| Box::new(key))
                        .unwrap_or_else(|| key.clone()),
                )),
            };
            rewrites.map(|(expr, key)| Expr::GetIndexedField { expr, key })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let rewrites = match (
                rewrite_columns(expr, map),
                rewrite_columns(low, map),
                rewrite_columns(high, map),
            ) {
                (None, None, None) => None,
                (m_expr, m_low, m_high) => Some((
                    m_expr
                        .map(|expr| Box::new(expr))
                        .unwrap_or_else(|| expr.clone()),
                    m_low
                        .map(|low| Box::new(low))
                        .unwrap_or_else(|| low.clone()),
                    m_high
                        .map(|high| Box::new(high))
                        .unwrap_or_else(|| high.clone()),
                )),
            };
            rewrites.map(|(expr, low, high)| Expr::Between {
                expr,
                negated: negated.clone(),
                low,
                high,
            })
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let mut did_rewrite_exprs = false;
            let expr = match expr {
                Some(expr) => match rewrite_columns(expr, map) {
                    Some(expr) => {
                        did_rewrite_exprs = true;
                        Some(Box::new(expr))
                    }
                    _ => Some(expr.clone()),
                },
                _ => None,
            };
            let when_then_expr = when_then_expr
                .iter()
                .map(|(when, then)| {
                    match (rewrite_columns(when, map), rewrite_columns(then, map)) {
                        (None, None) => (when.clone(), then.clone()),
                        (m_when, m_then) => {
                            did_rewrite_exprs = true;
                            (
                                m_when
                                    .map(|when| Box::new(when))
                                    .unwrap_or_else(|| when.clone()),
                                m_then
                                    .map(|then| Box::new(then))
                                    .unwrap_or_else(|| then.clone()),
                            )
                        }
                    }
                })
                .collect::<Vec<_>>();
            let else_expr = match else_expr {
                Some(else_expr) => match rewrite_columns(else_expr, map) {
                    Some(else_expr) => {
                        did_rewrite_exprs = true;
                        Some(Box::new(else_expr))
                    }
                    _ => Some(else_expr.clone()),
                },
                _ => None,
            };
            if did_rewrite_exprs {
                Some(Expr::Case {
                    expr,
                    when_then_expr,
                    else_expr,
                })
            } else {
                None
            }
        }
        Expr::Cast { expr, data_type } => rewrite_columns(expr, map).map(|expr| Expr::Cast {
            expr: Box::new(expr),
            data_type: data_type.clone(),
        }),
        Expr::TryCast { expr, data_type } => rewrite_columns(expr, map).map(|expr| Expr::TryCast {
            expr: Box::new(expr),
            data_type: data_type.clone(),
        }),
        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => rewrite_columns(expr, map).map(|expr| Expr::Sort {
            expr: Box::new(expr),
            asc: asc.clone(),
            nulls_first: nulls_first.clone(),
        }),
        Expr::ScalarFunction { fun, args } => {
            let mut did_rewrite_exprs = false;
            let args = args
                .iter()
                .map(|arg| {
                    rewrite_columns(arg, map)
                        .map(|arg| {
                            did_rewrite_exprs = true;
                            arg
                        })
                        .unwrap_or_else(|| arg.clone())
                })
                .collect::<Vec<_>>();
            if did_rewrite_exprs {
                Some(Expr::ScalarFunction {
                    fun: fun.clone(),
                    args,
                })
            } else {
                None
            }
        }
        Expr::ScalarUDF { fun, args } => {
            let mut did_rewrite_exprs = false;
            let args = args
                .iter()
                .map(|arg| {
                    rewrite_columns(arg, map)
                        .map(|arg| {
                            did_rewrite_exprs = true;
                            arg
                        })
                        .unwrap_or_else(|| arg.clone())
                })
                .collect::<Vec<_>>();
            if did_rewrite_exprs {
                Some(Expr::ScalarUDF {
                    fun: fun.clone(),
                    args,
                })
            } else {
                None
            }
        }
        Expr::TableUDF { fun, args } => {
            let mut did_rewrite_exprs = false;
            let args = args
                .iter()
                .map(|arg| {
                    rewrite_columns(arg, map)
                        .map(|arg| {
                            did_rewrite_exprs = true;
                            arg
                        })
                        .unwrap_or_else(|| arg.clone())
                })
                .collect::<Vec<_>>();
            if did_rewrite_exprs {
                Some(Expr::TableUDF {
                    fun: fun.clone(),
                    args,
                })
            } else {
                None
            }
        }
        Expr::AggregateFunction {
            fun,
            args,
            distinct,
        } => {
            let mut did_rewrite_exprs = false;
            let args = args
                .iter()
                .map(|arg| {
                    rewrite_columns(arg, map)
                        .map(|arg| {
                            did_rewrite_exprs = true;
                            arg
                        })
                        .unwrap_or_else(|| arg.clone())
                })
                .collect::<Vec<_>>();
            if did_rewrite_exprs {
                Some(Expr::AggregateFunction {
                    fun: fun.clone(),
                    args,
                    distinct: *distinct,
                })
            } else {
                None
            }
        }
        Expr::WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        } => {
            let mut did_rewrite_exprs = false;
            let args = args
                .iter()
                .map(|arg| {
                    rewrite_columns(arg, map)
                        .map(|arg| {
                            did_rewrite_exprs = true;
                            arg
                        })
                        .unwrap_or_else(|| arg.clone())
                })
                .collect::<Vec<_>>();
            let partition_by = partition_by
                .iter()
                .map(|expr| {
                    rewrite_columns(expr, map)
                        .map(|expr| {
                            did_rewrite_exprs = true;
                            expr
                        })
                        .unwrap_or_else(|| expr.clone())
                })
                .collect::<Vec<_>>();
            let order_by = order_by
                .iter()
                .map(|expr| {
                    rewrite_columns(expr, map)
                        .map(|expr| {
                            did_rewrite_exprs = true;
                            expr
                        })
                        .unwrap_or_else(|| expr.clone())
                })
                .collect::<Vec<_>>();
            if did_rewrite_exprs {
                Some(Expr::WindowFunction {
                    fun: fun.clone(),
                    args,
                    partition_by,
                    order_by,
                    window_frame: window_frame.clone(),
                })
            } else {
                None
            }
        }
        Expr::AggregateUDF { fun, args } => {
            let mut did_rewrite_exprs = false;
            let args = args
                .iter()
                .map(|arg| {
                    rewrite_columns(arg, map)
                        .map(|arg| {
                            did_rewrite_exprs = true;
                            arg
                        })
                        .unwrap_or_else(|| arg.clone())
                })
                .collect::<Vec<_>>();
            if did_rewrite_exprs {
                Some(Expr::AggregateUDF {
                    fun: fun.clone(),
                    args,
                })
            } else {
                None
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut did_rewrite_exprs = false;
            let expr = rewrite_columns(expr, map)
                .map(|expr| {
                    did_rewrite_exprs = true;
                    Box::new(expr)
                })
                .unwrap_or_else(|| expr.clone());
            let list = list
                .iter()
                .map(|expr| {
                    rewrite_columns(expr, map)
                        .map(|expr| {
                            did_rewrite_exprs = true;
                            expr
                        })
                        .unwrap_or_else(|| expr.clone())
                })
                .collect::<Vec<_>>();
            if did_rewrite_exprs {
                Some(Expr::InList {
                    expr,
                    list,
                    negated: *negated,
                })
            } else {
                None
            }
        }
        Expr::Wildcard | Expr::QualifiedWildcard { .. } => None,
    }
}

/// Recursively checks if the passed expr is a constant (always evaluates to the same result).
pub fn is_const_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(expr, _)
        | Expr::Not(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsNull(expr)
        | Expr::Negative(expr)
        | Expr::Cast { expr, .. }
        | Expr::TryCast { expr, .. }
        | Expr::Sort { expr, .. } => is_const_expr(expr),
        Expr::Literal(_) => true,
        Expr::BinaryExpr { left, right, .. } | Expr::AnyExpr { left, right, .. } => {
            is_const_expr(left) && is_const_expr(right)
        }
        Expr::Like(Like { expr, pattern, .. })
        | Expr::ILike(Like { expr, pattern, .. })
        | Expr::SimilarTo(Like { expr, pattern, .. }) => {
            is_const_expr(expr) && is_const_expr(pattern)
        }
        Expr::GetIndexedField { expr, key } => is_const_expr(expr) && is_const_expr(key),
        Expr::Between {
            expr, low, high, ..
        } => is_const_expr(expr) && is_const_expr(low) && is_const_expr(high),
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => expr
            .iter()
            .map(|expr| is_const_expr(expr))
            .chain(
                when_then_expr
                    .iter()
                    .map(|(when, then)| is_const_expr(when) && is_const_expr(then)),
            )
            .chain(else_expr.iter().map(|else_expr| is_const_expr(else_expr)))
            .all(|is_const| is_const),
        Expr::ScalarFunction { fun, args } => match fun.volatility() {
            Volatility::Immutable | Volatility::Stable => args.iter().all(|arg| is_const_expr(arg)),
            _ => false,
        },
        Expr::ScalarUDF { fun, args } => match fun.signature.volatility {
            Volatility::Immutable | Volatility::Stable => args.iter().all(|arg| is_const_expr(arg)),
            _ => false,
        },
        Expr::TableUDF { fun, args } => match fun.signature.volatility {
            Volatility::Immutable | Volatility::Stable => args.iter().all(|arg| is_const_expr(arg)),
            _ => false,
        },
        Expr::InList { expr, list, .. } => {
            is_const_expr(expr) && list.iter().map(|item| is_const_expr(item)).all(|item| item)
        }
        _ => false,
    }
}

/// Checks if the passed expr is a column.
pub fn is_column_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        _ => false,
    }
}

/// Recursively extracts `Column`s from an expr.
pub fn get_expr_columns(expr: &Expr) -> Vec<Column> {
    match expr {
        Expr::Alias(expr, _)
        | Expr::Not(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsNull(expr)
        | Expr::Negative(expr)
        | Expr::Cast { expr, .. }
        | Expr::TryCast { expr, .. }
        | Expr::Sort { expr, .. } => get_expr_columns(expr),
        Expr::Column(column) => vec![column.clone()],
        Expr::BinaryExpr { left, right, .. } | Expr::AnyExpr { left, right, .. } => {
            get_expr_columns(left)
                .into_iter()
                .chain(get_expr_columns(right).into_iter())
                .collect()
        }
        Expr::Like(Like { expr, pattern, .. })
        | Expr::ILike(Like { expr, pattern, .. })
        | Expr::SimilarTo(Like { expr, pattern, .. }) => get_expr_columns(expr)
            .into_iter()
            .chain(get_expr_columns(pattern).into_iter())
            .collect(),
        Expr::GetIndexedField { expr, key } => get_expr_columns(expr)
            .into_iter()
            .chain(get_expr_columns(key).into_iter())
            .collect(),
        Expr::Between {
            expr, low, high, ..
        } => get_expr_columns(expr)
            .into_iter()
            .chain(get_expr_columns(low).into_iter())
            .chain(get_expr_columns(high).into_iter())
            .collect(),
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => expr
            .as_ref()
            .map(|expr| get_expr_columns(expr))
            .unwrap_or(vec![])
            .into_iter()
            .chain(when_then_expr.iter().flat_map(|(when, then)| {
                get_expr_columns(when)
                    .into_iter()
                    .chain(get_expr_columns(then).into_iter())
                    .collect::<Vec<_>>()
            }))
            .chain(
                else_expr
                    .as_ref()
                    .map(|else_expr| get_expr_columns(else_expr))
                    .unwrap_or(vec![])
                    .into_iter(),
            )
            .collect(),
        Expr::ScalarFunction { args, .. }
        | Expr::ScalarUDF { args, .. }
        | Expr::TableUDF { args, .. }
        | Expr::AggregateFunction { args, .. }
        | Expr::AggregateUDF { args, .. } => {
            args.iter().flat_map(|arg| get_expr_columns(arg)).collect()
        }
        Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => args
            .iter()
            .flat_map(|arg| get_expr_columns(arg))
            .chain(
                partition_by
                    .iter()
                    .flat_map(|partition_by| get_expr_columns(partition_by)),
            )
            .chain(
                order_by
                    .iter()
                    .flat_map(|order_by| get_expr_columns(order_by)),
            )
            .collect(),
        Expr::InList { expr, list, .. } => get_expr_columns(expr)
            .into_iter()
            .chain(list.iter().flat_map(|item| get_expr_columns(item)))
            .collect(),
        _ => vec![],
    }
}

/// Provides a list of `Column`s the schema has, both qualified and unqualified.
pub fn get_schema_columns(schema: &DFSchema) -> HashSet<Column> {
    schema
        .fields()
        .iter()
        .flat_map(|field| vec![field.qualified_column(), field.unqualified_column()])
        .collect()
}

/// Recursively determines whether the plan yields exactly one row.
pub fn is_plan_yielding_one_row(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(Projection { input, .. })
        | LogicalPlan::Sort(Sort { input, .. })
        | LogicalPlan::Distinct(Distinct { input }) => is_plan_yielding_one_row(input),
        LogicalPlan::Aggregate(Aggregate {
            group_expr,
            aggr_expr,
            ..
        }) => group_expr.is_empty() && !aggr_expr.is_empty(),
        LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
            is_plan_yielding_one_row(left) && is_plan_yielding_one_row(right)
        }
        _ => false,
    }
}

/// Recursively determines whether the plan has Projections. This is useful for determining
/// whether or not the Projection in question is closest to TableScan.
/// If there are several inputs, returns `true` when all of the inputs have projections.
pub fn plan_has_projections(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(_) => true,
        LogicalPlan::Filter(Filter { input, .. })
        | LogicalPlan::Window(Window { input, .. })
        | LogicalPlan::Aggregate(Aggregate { input, .. })
        | LogicalPlan::Sort(Sort { input, .. })
        | LogicalPlan::Repartition(Repartition { input, .. })
        | LogicalPlan::Limit(Limit { input, .. })
        | LogicalPlan::Explain(Explain { plan: input, .. })
        | LogicalPlan::Analyze(Analyze { input, .. })
        | LogicalPlan::TableUDFs(TableUDFs { input, .. })
        | LogicalPlan::Distinct(Distinct { input }) => plan_has_projections(input),
        LogicalPlan::Join(Join { left, right, .. })
        | LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
            plan_has_projections(left) && plan_has_projections(right)
        }
        LogicalPlan::Union(Union { inputs, .. }) => {
            inputs.iter().all(|input| plan_has_projections(input))
        }
        LogicalPlan::Subquery(Subquery {
            subqueries, input, ..
        }) => {
            subqueries.iter().all(|input| plan_has_projections(input))
                && plan_has_projections(input)
        }
        _ => false,
    }
}

#[cfg(test)]
pub fn make_sample_table(name: &str, fields: Vec<&str>) -> Result<LogicalPlan> {
    let schema = Schema::new(
        fields
            .into_iter()
            .map(|field| Field::new(field, DataType::Int32, true))
            .collect(),
    );
    LogicalPlanBuilder::scan_empty(Some(name), &schema, None)?.build()
}

#[cfg(test)]
pub fn sample_table() -> Result<LogicalPlan> {
    make_sample_table("t1", vec!["c1", "c2", "c3"])
}
