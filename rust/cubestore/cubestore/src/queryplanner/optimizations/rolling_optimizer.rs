use crate::queryplanner::rolling::RollingWindowAggregate;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{
    Column, DFSchema, DataFusionError, JoinType, ScalarValue, TableReference,
};
use datafusion::functions::datetime::date_part::DatePartFunc;
use datafusion::functions::datetime::date_trunc::DateTruncFunc;
use datafusion::logical_expr::expr::{
    AggregateFunction, AggregateFunctionParams, Alias, ScalarFunction,
};
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Cast, ColumnarValue, Expr, Extension, Join, LogicalPlan, Operator,
    Projection, ScalarFunctionArgs, ScalarUDFImpl, SubqueryAlias, Union, Unnest,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use itertools::Itertools;
use std::sync::Arc;

/// Rewrites following logical plan:
/// ```plan
/// Projection
///   Aggregate, aggs: [AggregateFunction(AggregateFunction { func: AggregateUDF { inner: Sum { signature: Signature { type_signature: UserDefined, volatility: Immutable } } }, args: [Column(Column { relation: Some(Bare { table: "orders_rolling_number_cumulative__base" }), name: "orders__rolling_number" })], distinct: false, filter: None, order_by: None, null_treatment: None })]
///      Projection, [orders.created_at_series.date_from:date_from, orders_rolling_number_cumulative__base.orders__rolling_number:orders__rolling_number]
///        Join on: []
///          SubqueryAlias
///            Projection, [series.date_from:date_from, date_to]
///              SubqueryAlias
///                Projection, [date_from]
///                  Unnest
///                    Projection, [UNNEST(generate_series(Int64(1),Int64(5),Int64(1)))]
///                      Empty
///          SubqueryAlias
///            Projection, [orders__created_at_day, orders__rolling_number]
///              Aggregate, aggs: [AggregateFunction(AggregateFunction { func: AggregateUDF { inner: Sum { signature: Signature { type_signature: UserDefined, volatility: Immutable } } }, args: [Column(Column { relation: Some(Partial { schema: "s", table: "data" }), name: "n" })], distinct: false, filter: None, order_by: None, null_treatment: None })]
///                Scan s.data, source: CubeTableLogical, fields: [day, n]
/// ```
/// into:
/// ```plan
/// RollingWindowAggregate
/// ```
#[derive(Debug)]
pub struct RollingOptimizerRule {}

impl RollingOptimizerRule {
    pub fn new() -> Self {
        Self {}
    }

    pub fn extract_rolling_window_projection(
        node: &LogicalPlan,
    ) -> Option<RollingWindowProjectionExtractorResult> {
        // TODO upgrade DF: Use alias relation?
        match node {
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                let RollingWindowAggregateExtractorResult {
                    input,
                    dimension,
                    from_col,
                    from,
                    to_col,
                    to,
                    every,
                    partition_by,
                    rolling_aggs,
                    group_by_dimension,
                    aggs,
                    lower_bound,
                    upper_bound,
                    offset_to_end,
                } = Self::extract_rolling_window_aggregate(input)?;
                Some(RollingWindowProjectionExtractorResult {
                    input,
                    dimension,
                    dimension_alias: expr.iter().find_map(|e| match e {
                        Expr::Alias(Alias {
                            expr,
                            relation: _,
                            name,
                        }) => match expr.as_ref() {
                            Expr::Column(col)
                                if &col.name == &from_col.name || &col.name == &to_col.name =>
                            {
                                Some(name.clone())
                            }
                            _ => None,
                        },
                        _ => None,
                    })?,
                    from,
                    to,
                    every,
                    // The schema pairs these names with `rolling_aggs` positionally, so resolve each
                    // one from the aggregate it belongs to rather than from the order the
                    // projection happens to list them in. An aggregate the projection does not
                    // read under its own name has no name to output, so decline the rewrite
                    // instead of building a node that cannot describe its own output.
                    rolling_aggs_alias: rolling_aggs
                        .iter()
                        .map(|agg| {
                            let agg_name = agg.schema_name().to_string();
                            expr.iter().find_map(|e| match e {
                                Expr::Alias(Alias {
                                    expr,
                                    relation: _,
                                    name,
                                }) => match expr.as_ref() {
                                    Expr::Column(col) if col.name == agg_name => Some(name.clone()),
                                    _ => None,
                                },
                                _ => None,
                            })
                        })
                        .collect::<Option<Vec<_>>>()?,
                    partition_by,
                    rolling_aggs,
                    group_by_dimension,
                    aggs,
                    lower_bound,
                    upper_bound,
                    offset_to_end,
                })
            }
            // TODO it might be we better handle Aggregate but it conflicts with extract_rolling_window_aggregate extraction due to apply order
            // LogicalPlan::Aggregate(_) => {
            //     let RollingWindowAggregateExtractorResult {
            //         input,
            //         dimension,
            //         from_col,
            //         from,
            //         to_col,
            //         to,
            //         every,
            //         partition_by,
            //         rolling_aggs,
            //         group_by_dimension,
            //         aggs,
            //         lower_bound,
            //         upper_bound,
            //         offset_to_end,
            //     } = Self::extract_rolling_window_aggregate(node)?;
            //     Some(RollingWindowProjectionExtractorResult {
            //         input,
            //         dimension_alias: if offset_to_end {
            //             to_col.name.clone()
            //         } else {
            //             from_col.name.clone()
            //         },
            //         dimension,
            //         from,
            //         to,
            //         every,
            //         partition_by,
            //         rolling_aggs_alias: rolling_aggs
            //             .iter()
            //             .map(|e| e.name_for_alias().ok())
            //             .collect::<Option<Vec<_>>>()?,
            //         rolling_aggs,
            //         group_by_dimension,
            //         aggs,
            //         lower_bound,
            //         upper_bound,
            //         offset_to_end,
            //     })
            // }
            _ => None,
        }
    }

    fn output_columns(schema: &DFSchema) -> Vec<Column> {
        schema
            .iter()
            .map(|(relation, field)| Column::new(relation.cloned(), field.name()))
            .collect()
    }

    pub fn extract_rolling_window_aggregate(
        node: &LogicalPlan,
    ) -> Option<RollingWindowAggregateExtractorResult> {
        match node {
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            }) => {
                let rolling_aggs = aggr_expr
                    .iter()
                    .map(|e| match e {
                        Expr::AggregateFunction(AggregateFunction { func, params }) => {
                            let AggregateFunctionParams {
                                args,
                                distinct,
                                filter,
                                order_by,
                                null_treatment,
                            } = params;
                            // The rolling executor evaluates the aggregate over its own window and
                            // honours none of these, so rebuilding without them would silently
                            // answer a different question.
                            if *distinct
                                || filter.is_some()
                                || order_by.is_some()
                                || null_treatment.is_some()
                            {
                                return None;
                            }
                            Some(Expr::AggregateFunction(AggregateFunction {
                                func: func.clone(),
                                params: AggregateFunctionParams {
                                    args: args.clone(),
                                    distinct: false,
                                    filter: None,
                                    order_by: None,
                                    null_treatment: None,
                                },
                            }))
                        }
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;

                // A grouped aggregate with no aggregate expressions is a key generator
                // (e.g. `SELECT DISTINCT dim` over the time series, used to assemble the
                // dimension keys of a multi-measure rolling query), not a rolling window.
                // Converting it to a RollingWindowAggregate yields an empty `rolling_aggs`,
                // whose executor leaves the per-bucket `had_values` mask empty and then
                // indexes out of bounds. Leave it as a plain aggregate/range-join, which
                // produces the same dimension keys.
                if rolling_aggs.is_empty() {
                    return None;
                }

                // The executor advances one shared group counter per (aggregate, dimension point),
                // while each aggregate accumulates into its own group space. With more than one
                // aggregate they interleave, so every aggregate past the first reads the slots the
                // others left empty and comes back null. Serve these as a plain aggregate until
                // that accounting is fixed.
                if rolling_aggs.len() > 1 {
                    return None;
                }

                let RollingWindowJoinExtractorResult {
                    input,
                    dimension,
                    from,
                    from_col,
                    to,
                    to_col,
                    every,
                    group_by_dimension,
                    aggs,
                    lower_bound,
                    upper_bound,
                    offset_to_end,
                } = Self::extract_rolling_window_join(input)?;

                let partition_by = group_expr
                    .iter()
                    .map(|e| match e {
                        Expr::Column(col)
                            if &col.name != &from_col.name && &col.name != &to_col.name =>
                        {
                            Some(vec![col.clone()])
                        }
                        Expr::Column(_) => Some(Vec::new()),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect();

                Some(RollingWindowAggregateExtractorResult {
                    input,
                    dimension,
                    from_col,
                    from,
                    to_col,
                    to,
                    every,
                    rolling_aggs,
                    group_by_dimension,
                    aggs,
                    lower_bound,
                    upper_bound,
                    offset_to_end,
                    partition_by,
                })
            }
            _ => None,
        }
    }

    pub fn extract_rolling_window_join(
        node: &LogicalPlan,
    ) -> Option<RollingWindowJoinExtractorResult> {
        match node {
            LogicalPlan::Join(Join {
                left,
                right,
                // TODO
                on: _,
                join_type: JoinType::Left,
                filter,
                ..
            }) => {
                let left_series = Self::extract_series_projection(left)
                    .or_else(|| Self::extract_series_union(left))?;

                let RollingWindowBoundsExtractorResult {
                    lower_bound,
                    upper_bound,
                    dimension,
                    offset_to_end,
                } = Self::extract_dimension_and_bounds(
                    filter.as_ref()?,
                    &left_series.from_col,
                    &left_series.to_col,
                )?;

                Some(RollingWindowJoinExtractorResult {
                    input: right.clone(),
                    dimension: dimension?,
                    from: left_series.from,
                    from_col: left_series.from_col,
                    to: left_series.to,
                    to_col: left_series.to_col,
                    every: left_series.every,
                    group_by_dimension: None,
                    aggs: vec![],
                    lower_bound,
                    upper_bound,
                    offset_to_end,
                })
            }
            LogicalPlan::Projection(Projection { expr: _, input, .. }) => {
                Self::extract_rolling_window_join(input)
            }
            _ => None,
        }
    }

    pub fn extract_dimension_and_bounds(
        expr: &Expr,
        from_col: &Column,
        to_col: &Column,
    ) -> Option<RollingWindowBoundsExtractorResult> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => {
                    let left_bounds = Self::extract_dimension_and_bounds(left, from_col, to_col)?;
                    let right_bounds = Self::extract_dimension_and_bounds(right, from_col, to_col)?;
                    if left_bounds.dimension != right_bounds.dimension {
                        return None;
                    }
                    if left_bounds.offset_to_end != right_bounds.offset_to_end {
                        return None;
                    }
                    Some(RollingWindowBoundsExtractorResult {
                        lower_bound: left_bounds.lower_bound.or(right_bounds.lower_bound),
                        upper_bound: left_bounds.upper_bound.or(right_bounds.upper_bound),
                        dimension: left_bounds.dimension.or(right_bounds.dimension),
                        offset_to_end: left_bounds.offset_to_end || right_bounds.offset_to_end,
                    })
                }
                Operator::Gt | Operator::GtEq => {
                    let (dimension, bound, is_left_dimension, offset_to_end) =
                        Self::extract_bound_and_dimension(left, right, from_col, to_col)?;
                    Some(RollingWindowBoundsExtractorResult {
                        lower_bound: if is_left_dimension {
                            Some(bound.clone())
                        } else {
                            None
                        },
                        upper_bound: if is_left_dimension { None } else { Some(bound) },
                        dimension: Some(dimension.clone()),
                        offset_to_end,
                    })
                }
                Operator::Lt | Operator::LtEq => {
                    let (dimension, bound, is_left_dimension, offset_to_end) =
                        Self::extract_bound_and_dimension(left, right, from_col, to_col)?;
                    Some(RollingWindowBoundsExtractorResult {
                        lower_bound: if is_left_dimension {
                            None
                        } else {
                            Some(bound.clone())
                        },
                        upper_bound: if is_left_dimension { Some(bound) } else { None },
                        dimension: Some(dimension.clone()),
                        offset_to_end,
                    })
                }
                _ => None,
            },
            _ => None,
        }
    }

    pub fn extract_bound_and_dimension<'a>(
        left: &'a Expr,
        right: &'a Expr,
        from_col: &'a Column,
        to_col: &'a Column,
    ) -> Option<(&'a Column, Expr, bool, bool)> {
        if let Some(dimension) = match left {
            Expr::Column(col) if col != from_col && col != to_col => Some(col),
            _ => None,
        } {
            let (bound, offset_to_end) =
                Self::extract_bound_scalar_and_offset_to_end(right, from_col, to_col)?;
            Some((dimension, bound, true, offset_to_end))
        } else if let Some(dimension) = match right {
            Expr::Column(col) if col != from_col && col != to_col => Some(col),
            _ => None,
        } {
            let (bound, offset_to_end) =
                Self::extract_bound_scalar_and_offset_to_end(left, from_col, to_col)?;
            Some((dimension, bound, false, offset_to_end))
        } else {
            None
        }
    }

    pub fn extract_bound_scalar_and_offset_to_end<'a>(
        expr: &'a Expr,
        from_col: &'a Column,
        to_col: &'a Column,
    ) -> Option<(Expr, bool)> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::Plus => {
                    match left.as_ref() {
                        Expr::Column(col)
                            if col.name == from_col.name || col.name == to_col.name =>
                        {
                            return Some((right.as_ref().clone(), col.name == to_col.name));
                        }
                        _ => {}
                    }
                    match right.as_ref() {
                        Expr::Column(col)
                            if col.name == from_col.name || col.name == to_col.name =>
                        {
                            return Some((left.as_ref().clone(), col.name == to_col.name));
                        }
                        _ => {}
                    }
                    None
                }
                Operator::Minus => {
                    match left.as_ref() {
                        Expr::Column(col)
                            if col.name == from_col.name || col.name == to_col.name =>
                        {
                            match right.as_ref() {
                                Expr::Literal(value) => {
                                    return Some((
                                        Expr::Literal(value.arithmetic_negate().ok()?),
                                        col.name == to_col.name,
                                    ));
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                    None
                }
                _ => None,
            },
            Expr::Cast(Cast { expr, .. }) => {
                Self::extract_bound_scalar_and_offset_to_end(expr, from_col, to_col)
            }
            Expr::Column(col) => Some((Expr::Literal(ScalarValue::Null), col.name == to_col.name)),
            _ => None,
        }
    }

    pub fn extract_series_union(node: &LogicalPlan) -> Option<RollingWindowSeriesExtractorResult> {
        match node {
            LogicalPlan::Union(Union { inputs, .. }) => {
                let series = inputs
                    .iter()
                    .map(|input| Self::extract_series_union_projection(input))
                    .collect::<Option<Vec<_>>>()?;
                let first_series = series.iter().next()?;
                let second_series = series.iter().nth(1)?;
                let last_series = series.iter().nth(series.len() - 1)?;
                Some(RollingWindowSeriesExtractorResult {
                    from: Expr::Literal(first_series.from.clone()),
                    to: Expr::Literal(last_series.from.clone()),
                    every: Expr::Literal(month_aware_sub(&first_series.from, &second_series.from)?),
                    from_col: first_series.from_col.clone(),
                    to_col: first_series.to_col.clone(),
                })
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let series = Self::extract_series_union(input)?;
                let from_col = Self::subquery_alias_rename(alias, series.from_col);
                let to_col = Self::subquery_alias_rename(alias, series.to_col);
                Some(RollingWindowSeriesExtractorResult {
                    from: series.from,
                    to: series.to,
                    every: series.every,
                    from_col,
                    to_col,
                })
            }
            _ => None,
        }
    }

    pub fn extract_series_union_projection(
        node: &LogicalPlan,
    ) -> Option<RollingWindowSeriesProjectionResult> {
        match node {
            LogicalPlan::Projection(Projection { expr, input: _, .. }) => {
                if expr.len() != 2 && expr.len() != 1 {
                    return None;
                }
                let from_to = expr
                    .iter()
                    .map(|e| match e {
                        Expr::Alias(Alias {
                            expr,
                            relation,
                            name,
                        }) => match expr.as_ref() {
                            Expr::Literal(v) => Some((Column::new(relation.clone(), name), v)),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                let from_index = from_to
                    .iter()
                    .find_position(|(c, _)| c.name == "date_from")
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                let to_index = from_to
                    .iter()
                    .find_position(|(c, _)| c.name == "date_to")
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                Some(RollingWindowSeriesProjectionResult {
                    from: from_to[from_index].1.clone(),
                    to: from_to[to_index].1.clone(),
                    from_col: from_to[from_index].0.clone(),
                    to_col: from_to[to_index].0.clone(),
                })
            }
            _ => None,
        }
    }

    pub fn extract_series_projection(
        node: &LogicalPlan,
    ) -> Option<RollingWindowSeriesExtractorResult> {
        match node {
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                let series = Self::extract_series(input)?;
                let to_col = expr
                    .iter()
                    .find_map(|e| match e {
                        Expr::Alias(Alias {
                            expr,
                            relation,
                            name,
                        }) => match expr.as_ref() {
                            Expr::BinaryExpr(BinaryExpr { left, op, right: _ }) => {
                                if op == &Operator::Plus {
                                    match left.as_ref() {
                                        Expr::Column(col) if &col.name == &series.from_col.name => {
                                            Some(Column::new(relation.clone(), name.clone()))
                                        }
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        },
                        _ => None,
                    })
                    // It means to column isn't used and was optimized out
                    .unwrap_or(series.to_col);
                let from_col = Self::projection_rename(expr, series.from_col);

                // let to_col = Self::projection_rename(expr, series.to_col);
                Some(RollingWindowSeriesExtractorResult {
                    from: series.from,
                    to: series.to,
                    every: series.every,
                    from_col,
                    to_col,
                })
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let series = Self::extract_series_projection(input)?;
                let from_col = Self::subquery_alias_rename(alias, series.from_col);
                let to_col = Self::subquery_alias_rename(alias, series.to_col);
                Some(RollingWindowSeriesExtractorResult {
                    from: series.from,
                    to: series.to,
                    every: series.every,
                    from_col,
                    to_col,
                })
            }
            _ => None,
        }
    }

    pub fn extract_series(node: &LogicalPlan) -> Option<RollingWindowSeriesExtractorResult> {
        match node {
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                let series = Self::extract_series(input)?;
                let from_col = Self::projection_rename(expr, series.from_col);
                let to_col = Self::projection_rename(expr, series.to_col);
                Some(RollingWindowSeriesExtractorResult {
                    from: series.from,
                    to: series.to,
                    every: series.every,
                    from_col,
                    to_col,
                })
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let series = Self::extract_series(input)?;
                let from_col = Self::subquery_alias_rename(alias, series.from_col);
                let to_col = Self::subquery_alias_rename(alias, series.to_col);
                Some(RollingWindowSeriesExtractorResult {
                    from: series.from,
                    to: series.to,
                    every: series.every,
                    from_col,
                    to_col,
                })
            }
            LogicalPlan::Unnest(Unnest {
                input,
                exec_columns,
                schema,
                ..
            }) => {
                let series_column = exec_columns.iter().next().cloned()?;
                let series = Self::extract_series_from_unnest(input, series_column);
                let col = schema.field(0).name();
                series.map(|mut series| {
                    series.from_col = Column::from_name(col);
                    series.to_col = series.from_col.clone();
                    series
                })
            }
            _ => None,
        }
    }

    pub fn extract_series_from_unnest(
        node: &LogicalPlan,
        series_column: Column,
    ) -> Option<RollingWindowSeriesExtractorResult> {
        match node {
            LogicalPlan::Projection(Projection { expr, input: _, .. }) => {
                for e in expr.iter() {
                    // TODO upgrade DF: Presumably, use `relation`.
                    match e {
                        Expr::Alias(Alias {
                            expr,
                            relation: _,
                            name,
                        }) if name == &series_column.name => match expr.as_ref() {
                            Expr::ScalarFunction(ScalarFunction { func, args })
                                if func.name() == "generate_series" =>
                            {
                                let from = args.iter().next().cloned()?;
                                let to = args.iter().nth(1).cloned()?;
                                let every = args.iter().nth(2).cloned()?;
                                return Some(RollingWindowSeriesExtractorResult {
                                    from,
                                    to,
                                    every,
                                    from_col: series_column.clone(),
                                    to_col: series_column,
                                });
                            }
                            Expr::Literal(ScalarValue::List(list)) => {
                                // TODO why does first element holds the array? Is it always the case?
                                let array = list.iter().next().as_ref().cloned()??;
                                let from = ScalarValue::try_from_array(&array, 0).ok()?;
                                let to =
                                    ScalarValue::try_from_array(&array, array.len() - 1).ok()?;

                                let index_1 = ScalarValue::try_from_array(&array, 1).ok()?;
                                let every = month_aware_sub(&from, &index_1)?;

                                return Some(RollingWindowSeriesExtractorResult {
                                    from: Expr::Literal(from),
                                    to: Expr::Literal(to),
                                    every: Expr::Literal(every),
                                    from_col: series_column.clone(),
                                    to_col: series_column,
                                });
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn projection_rename(expr: &Vec<Expr>, column: Column) -> Column {
        expr.iter()
            .filter_map(|e| match e {
                Expr::Alias(Alias {
                    expr,
                    relation,
                    name,
                }) => match expr.as_ref() {
                    Expr::Column(col) if col == &column => {
                        Some(Column::new(relation.clone(), name))
                    }
                    _ => None,
                },
                Expr::Column(col) if col == &column => Some(column.clone()),
                _ => None,
            })
            .next()
            .unwrap_or(column)
    }

    fn subquery_alias_rename(alias: &TableReference, column: Column) -> Column {
        Column::new(Some(alias.table()), column.name)
    }
}

pub fn month_aware_sub(from: &ScalarValue, to: &ScalarValue) -> Option<ScalarValue> {
    match (from, to) {
        (
            ScalarValue::TimestampSecond(_, None)
            | ScalarValue::TimestampMillisecond(_, None)
            | ScalarValue::TimestampMicrosecond(_, None)
            | ScalarValue::TimestampNanosecond(_, None),
            ScalarValue::TimestampSecond(_, None)
            | ScalarValue::TimestampMillisecond(_, None)
            | ScalarValue::TimestampMicrosecond(_, None)
            | ScalarValue::TimestampNanosecond(_, None),
        ) => {
            let from_type = from.data_type();
            let to_type = to.data_type();
            // TODO lookup from registry?
            let date_trunc = DateTruncFunc::new();
            let from_trunc = date_trunc
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some("month".to_string()))),
                        ColumnarValue::Scalar(from.clone()),
                    ],
                    number_rows: 1,
                    return_type: &from_type,
                })
                .ok()?;
            let to_trunc = date_trunc
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some("month".to_string()))),
                        ColumnarValue::Scalar(to.clone()),
                    ],
                    number_rows: 1,
                    return_type: &to_type,
                })
                .ok()?;
            match (from_trunc, to_trunc) {
                (ColumnarValue::Scalar(from_trunc), ColumnarValue::Scalar(to_trunc)) => {
                    // TODO as with date_trunc above, lookup from registry?
                    let date_part = DatePartFunc::new();

                    if from.sub(from_trunc.clone()).ok() == to.sub(to_trunc.clone()).ok() {
                        let from_month = date_part
                            .invoke_with_args(ScalarFunctionArgs {
                                args: vec![
                                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                        "month".to_string(),
                                    ))),
                                    ColumnarValue::Scalar(from_trunc.clone()),
                                ],
                                number_rows: 1,
                                return_type: &DataType::Int32,
                            })
                            .ok()?;
                        let from_year = date_part
                            .invoke_with_args(ScalarFunctionArgs {
                                args: vec![
                                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                        "year".to_string(),
                                    ))),
                                    ColumnarValue::Scalar(from_trunc.clone()),
                                ],
                                number_rows: 1,
                                return_type: &DataType::Int32,
                            })
                            .ok()?;
                        let to_month = date_part
                            .invoke_with_args(ScalarFunctionArgs {
                                args: vec![
                                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                        "month".to_string(),
                                    ))),
                                    ColumnarValue::Scalar(to_trunc.clone()),
                                ],
                                number_rows: 1,
                                return_type: &DataType::Int32,
                            })
                            .ok()?;
                        let to_year = date_part
                            .invoke_with_args(ScalarFunctionArgs {
                                args: vec![
                                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                        "year".to_string(),
                                    ))),
                                    ColumnarValue::Scalar(to_trunc.clone()),
                                ],
                                number_rows: 1,
                                return_type: &DataType::Int32,
                            })
                            .ok()?;

                        match (from_month, from_year, to_month, to_year) {
                            (
                                ColumnarValue::Scalar(ScalarValue::Int32(Some(from_month))),
                                ColumnarValue::Scalar(ScalarValue::Int32(Some(from_year))),
                                ColumnarValue::Scalar(ScalarValue::Int32(Some(to_month))),
                                ColumnarValue::Scalar(ScalarValue::Int32(Some(to_year))),
                            ) => {
                                return Some(ScalarValue::IntervalYearMonth(Some(
                                    (to_year - from_year) * 12 + (to_month - from_month),
                                )))
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
            to.sub(from).ok()
        }
        (_, _) => to.sub(from).ok(),
    }
}

impl OptimizerRule for RollingOptimizerRule {
    fn name(&self) -> &str {
        "rolling_optimizer"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>, DataFusionError> {
        if let Some(rolling) = Self::extract_rolling_window_projection(&plan) {
            let projection_output = Self::output_columns(plan.schema());
            let schema = match RollingWindowAggregate::schema_from(
                &rolling.input,
                &rolling.dimension,
                &rolling.partition_by,
                &rolling.rolling_aggs,
                &rolling.dimension_alias,
                &rolling.rolling_aggs_alias,
                &rolling.from,
            ) {
                Ok(schema) => schema,
                Err(e) => {
                    // The rolling node cannot describe its own output for this shape (colliding
                    // aliases, say). Run the query as a plain aggregate/range-join rather than
                    // failing to plan it.
                    log::debug!("Not rewriting a rolling window, no schema for it: {}", e);
                    return Ok(Transformed::no(plan));
                }
            };
            let rolling_window = RollingWindowAggregate {
                schema,
                input: rolling.input,
                dimension: rolling.dimension,
                dimension_alias: rolling.dimension_alias,
                from: rolling.from,
                to: rolling.to,
                every: rolling.every,
                partition_by: rolling.partition_by,
                rolling_aggs: rolling.rolling_aggs,
                rolling_aggs_alias: rolling.rolling_aggs_alias,
                group_by_dimension: rolling.group_by_dimension,
                aggs: rolling.aggs,
                lower_bound: rolling.lower_bound,
                upper_bound: rolling.upper_bound,
                offset_to_end: rolling.offset_to_end,
            };
            let rolling_output = Self::output_columns(&rolling_window.schema);
            let rolling_plan = LogicalPlan::Extension(Extension {
                node: Arc::new(rolling_window),
            });

            if rolling_output == projection_output {
                return Ok(Transformed::yes(rolling_plan));
            }

            // The rolling node always emits dimension + partition_by + rolling aggregates, so a
            // GROUP BY column the replaced projection did not select reappears in the output.
            // A rewrite must not change the schema of the node it replaces: ancestors keep
            // resolving their columns by position against the old one. Restore the original
            // output with a projection on top, and leave the plan alone when it cannot be
            // expressed from the rolling node's output.
            let rolling_schema = rolling_plan.schema();
            if let Some(missing) = projection_output
                .iter()
                .find(|c| rolling_schema.maybe_index_of_column(c).is_none())
            {
                log::debug!(
                    "Not rewriting a rolling window, it does not output {}",
                    missing
                );
                return Ok(Transformed::no(plan));
            }
            let exprs = projection_output
                .into_iter()
                .map(Expr::Column)
                .collect_vec();
            Ok(Transformed::yes(LogicalPlan::Projection(
                Projection::try_new(exprs, Arc::new(rolling_plan))?,
            )))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

pub struct RollingWindowProjectionExtractorResult {
    pub input: Arc<LogicalPlan>,
    pub dimension: Column,
    pub dimension_alias: String,
    pub from: Expr,
    pub to: Expr,
    pub every: Expr,
    pub partition_by: Vec<Column>,
    pub rolling_aggs: Vec<Expr>,
    pub rolling_aggs_alias: Vec<String>,
    pub group_by_dimension: Option<Expr>,
    pub aggs: Vec<Expr>,
    pub lower_bound: Option<Expr>,
    pub upper_bound: Option<Expr>,
    pub offset_to_end: bool,
}

pub struct RollingWindowAggregateExtractorResult {
    pub input: Arc<LogicalPlan>,
    pub dimension: Column,
    pub from_col: Column,
    pub from: Expr,
    pub to_col: Column,
    pub to: Expr,
    pub every: Expr,
    pub partition_by: Vec<Column>,
    pub rolling_aggs: Vec<Expr>,
    pub group_by_dimension: Option<Expr>,
    pub aggs: Vec<Expr>,
    pub lower_bound: Option<Expr>,
    pub upper_bound: Option<Expr>,
    pub offset_to_end: bool,
}

pub struct RollingWindowJoinExtractorResult {
    pub input: Arc<LogicalPlan>,
    pub dimension: Column,
    pub from_col: Column,
    pub from: Expr,
    pub to_col: Column,
    pub to: Expr,
    pub every: Expr,
    pub group_by_dimension: Option<Expr>,
    pub aggs: Vec<Expr>,
    pub lower_bound: Option<Expr>,
    pub upper_bound: Option<Expr>,
    pub offset_to_end: bool,
}

pub struct RollingWindowBoundsExtractorResult {
    pub lower_bound: Option<Expr>,
    pub upper_bound: Option<Expr>,
    pub dimension: Option<Column>,
    pub offset_to_end: bool,
}

#[derive(Debug)]
pub struct RollingWindowSeriesExtractorResult {
    pub from: Expr,
    pub to: Expr,
    pub every: Expr,
    pub from_col: Column,
    pub to_col: Column,
}

pub struct RollingWindowSeriesProjectionResult {
    pub from: ScalarValue,
    pub to: ScalarValue,
    pub from_col: Column,
    pub to_col: Column,
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::DFSchema;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::expr::AggregateFunctionParams;
    use datafusion::logical_expr::{build_join_schema, col, lit, EmptyRelation, JoinConstraint};
    use datafusion::optimizer::OptimizerContext;
    use std::collections::HashMap;

    /// A stand-in for the rolling source: only its schema matters to the rule.
    fn source() -> LogicalPlan {
        let schema = DFSchema::new_with_metadata(
            ["d", "win", "n"]
                .iter()
                .map(|name| {
                    (
                        Some(TableReference::bare("rs")),
                        Arc::new(Field::new(*name, DataType::Int64, true)),
                    )
                })
                .collect(),
            HashMap::new(),
        )
        .unwrap();
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema),
        })
    }

    /// `UNION ALL` of literal `date_from`/`date_to` pairs, the shape the series extractor accepts.
    fn series() -> LogicalPlan {
        let point = |from: i64| {
            let exprs = vec![lit(from).alias("date_from"), lit(from + 1).alias("date_to")];
            Arc::new(LogicalPlan::Projection(
                Projection::try_new(
                    exprs,
                    Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(DFSchema::empty()),
                    })),
                )
                .unwrap(),
            ))
        };
        let inputs = vec![point(1), point(2)];
        let schema = Arc::clone(inputs[0].schema());
        let union = LogicalPlan::Union(Union { inputs, schema });
        LogicalPlan::SubqueryAlias(
            SubqueryAlias::try_new(Arc::new(union), TableReference::bare("s0")).unwrap(),
        )
    }

    fn sum_of(column: &str, distinct: bool, filter: Option<Expr>) -> Expr {
        Expr::AggregateFunction(AggregateFunction {
            func: sum_udaf(),
            params: AggregateFunctionParams {
                args: vec![col(Column::new(Some(TableReference::bare("rs")), column))],
                distinct,
                filter: filter.map(Box::new),
                order_by: None,
                null_treatment: None,
            },
        })
    }

    /// `Projection(projection) -> Aggregate(group_by, [agg]) -> series LEFT JOIN source`, the shape
    /// `RollingOptimizerRule` is meant to rewrite.
    fn rolling_plan(agg: Expr, group_by: Vec<Expr>, projection: Vec<Expr>) -> LogicalPlan {
        rolling_plan_with_aggs(vec![agg], group_by, projection)
    }

    fn rolling_plan_with_aggs(
        aggs: Vec<Expr>,
        group_by: Vec<Expr>,
        projection: Vec<Expr>,
    ) -> LogicalPlan {
        let (left, right) = (series(), source());
        let join_schema =
            build_join_schema(left.schema(), right.schema(), &JoinType::Left).unwrap();
        let dim = col(Column::new(Some(TableReference::bare("rs")), "d"));
        let date_to = col(Column::new(Some(TableReference::bare("s0")), "date_to"));
        let join = LogicalPlan::Join(datafusion::logical_expr::Join {
            left: Arc::new(left),
            right: Arc::new(right),
            on: vec![],
            filter: Some(
                dim.clone()
                    .gt(date_to.clone() - lit(1))
                    .and(dim.lt_eq(date_to)),
            ),
            join_type: JoinType::Left,
            join_constraint: JoinConstraint::On,
            schema: Arc::new(join_schema),
            null_equals_null: false,
        });
        let aggregate =
            Aggregate::try_new(Arc::new(join), group_by, aggs).map(LogicalPlan::Aggregate);
        LogicalPlan::Projection(
            Projection::try_new(projection, Arc::new(aggregate.unwrap())).unwrap(),
        )
    }

    /// The column an aggregate is read back as. Derived rather than written out, because a
    /// hardcoded name that does not match makes every "declines" case pass for the wrong reason.
    fn agg_col(agg: &Expr) -> Expr {
        col(agg.schema_name().to_string())
    }

    fn date_from() -> Expr {
        col(Column::new(Some(TableReference::bare("s0")), "date_from"))
    }

    fn win() -> Expr {
        col(Column::new(Some(TableReference::bare("rs")), "win"))
    }

    fn rewrite(plan: LogicalPlan) -> Transformed<LogicalPlan> {
        RollingOptimizerRule {}
            .rewrite(plan, &OptimizerContext::new())
            .unwrap()
    }

    fn is_rolling(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Extension(e) if e.node.name() == "RollingWindowAggregate")
    }

    /// Whatever the rule does, the node it produces has to answer to the same column names as the
    /// projection it replaced -- ancestors keep resolving against their own cached schema.
    fn assert_output_preserved(before: &LogicalPlan, after: &LogicalPlan) {
        let names = |plan: &LogicalPlan| {
            plan.schema()
                .iter()
                .map(|(q, f)| format!("{:?}.{}", q.map(|q| q.to_string()), f.name()))
                .collect::<Vec<_>>()
        };
        assert_eq!(names(before), names(after));
    }

    #[test]
    fn rewrites_when_the_projection_keeps_every_group_by_column() {
        let agg = sum_of("n", false, None);
        let plan = rolling_plan(
            agg.clone(),
            vec![date_from(), win()],
            vec![date_from().alias("mon"), win(), agg_col(&agg).alias("num")],
        );
        let rewritten = rewrite(plan.clone());
        assert!(rewritten.transformed);
        assert!(
            is_rolling(&rewritten.data),
            "expected a bare rolling node, got {}",
            rewritten.data.display_indent()
        );
        assert_output_preserved(&plan, &rewritten.data);
    }

    #[test]
    fn restores_the_output_when_a_group_by_column_is_not_projected() {
        let agg = sum_of("n", false, None);
        let plan = rolling_plan(
            agg.clone(),
            vec![date_from(), win()],
            vec![date_from().alias("mon"), agg_col(&agg).alias("num")],
        );
        let rewritten = rewrite(plan.clone());
        assert!(rewritten.transformed);
        // `win` is grouped by, so the rolling node emits it; a projection has to take it back out.
        match &rewritten.data {
            LogicalPlan::Projection(p) => assert!(
                is_rolling(&p.input),
                "expected the projection to sit on the rolling node, got {}",
                p.input.display_indent()
            ),
            other => panic!("expected a projection, got {}", other.display_indent()),
        }
        assert_output_preserved(&plan, &rewritten.data);
    }

    // Two independent things decline the next two shapes: the explicit modifier guard, and the
    // fact that stripping a modifier changes the aggregate's name, so the projection no longer
    // reads it under the name the rolling node would emit. The tests pin the behaviour, not which
    // of the two gets there first.

    #[test]
    fn declines_a_distinct_aggregate() {
        // The executor evaluates the aggregate over its own window and cannot honour DISTINCT.
        let agg = sum_of("n", true, None);
        let plan = rolling_plan(
            agg.clone(),
            vec![date_from(), win()],
            vec![date_from().alias("mon"), win(), agg_col(&agg).alias("num")],
        );
        assert!(!rewrite(plan).transformed);
    }

    #[test]
    fn declines_a_filtered_aggregate() {
        let agg = sum_of("n", false, Some(win().gt(lit(0))));
        let plan = rolling_plan(
            agg.clone(),
            vec![date_from(), win()],
            vec![date_from().alias("mon"), win(), agg_col(&agg).alias("num")],
        );
        assert!(!rewrite(plan).transformed);
    }

    #[test]
    fn declines_more_than_one_rolling_aggregate() {
        // The executor shares one group counter across the aggregates, so the second one reads
        // empty slots and answers null. Two aggregates also make the shape where positional and
        // name-based alias pairing could disagree, so declining keeps that question unobservable.
        let n_total = sum_of("n", false, None);
        let d_total = sum_of("d", false, None);
        let plan = rolling_plan_with_aggs(
            vec![n_total.clone(), d_total.clone()],
            vec![date_from(), win()],
            vec![
                date_from().alias("mon"),
                win(),
                agg_col(&n_total).alias("n_total"),
                agg_col(&d_total).alias("d_total"),
            ],
        );
        assert!(!rewrite(plan).transformed);
    }

    #[test]
    fn declines_when_an_aggregate_has_no_name_in_the_projection() {
        // Nothing reads the aggregate under its own name, so the rolling node would have no name
        // to give the column and would silently drop it from its schema.
        let plan = rolling_plan(
            sum_of("n", false, None),
            vec![date_from(), win()],
            vec![date_from().alias("mon"), win()],
        );
        assert!(!rewrite(plan).transformed);
    }
}
