use crate::queryplanner::planning::{ClusterSendNode, CubeExtensionPlanner};
use crate::queryplanner::topk::execute::AggregateTopKExec;
use crate::queryplanner::topk::{ClusterAggregateTopK, SortColumn};
use arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionContextState;
use datafusion::logical_plan::{DFField, DFSchema, DFSchemaRef, Expr, LogicalPlan};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::planner::{compute_aggregation_strategy, DefaultPhysicalPlanner};
use datafusion::physical_plan::sort::{SortExec, SortOptions};
use datafusion::physical_plan::ExecutionPlan;
use itertools::Itertools;
use std::sync::Arc;

/// Replaces `Limit(Sort(Aggregate(ClusterSend)))` with [ClusterAggregateTopK] when possible.
pub fn materialize_topk(p: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    match &p {
        LogicalPlan::Limit {
            n: limit,
            input: sort,
        } => match sort.as_ref() {
            LogicalPlan::Sort {
                expr: sort_expr,
                input: sort_input,
            } => {
                let projection = extract_column_projection(&sort_input);
                let aggregate = projection.as_ref().map(|p| p.input).unwrap_or(sort_input);
                match aggregate.as_ref() {
                    LogicalPlan::Aggregate {
                        input: cluster_send,
                        group_expr,
                        aggr_expr,
                        schema: aggregate_schema,
                    } => {
                        assert_eq!(
                            aggregate_schema.fields().len(),
                            group_expr.len() + aggr_expr.len()
                        );
                        if group_expr.len() == 0
                            || aggr_expr.len() == 0
                            || !aggr_exprs_allow_topk(aggr_expr)
                            || !aggr_schema_allows_topk(aggregate_schema.as_ref(), group_expr.len())
                        {
                            return Ok(p);
                        }
                        let sort_columns;
                        if let Some(sc) = extract_sort_columns(
                            group_expr.len(),
                            &sort_expr,
                            sort_input.schema(),
                            projection.as_ref().map(|c| c.input_columns.as_slice()),
                        ) {
                            sort_columns = sc;
                        } else {
                            return Ok(p);
                        }
                        match cluster_send.as_ref() {
                            LogicalPlan::Extension { node } => {
                                let cs;
                                if let Some(c) = node.as_any().downcast_ref::<ClusterSendNode>() {
                                    cs = c;
                                } else {
                                    return Ok(p);
                                }
                                let topk = LogicalPlan::Extension {
                                    node: Arc::new(ClusterAggregateTopK {
                                        limit: *limit,
                                        input: cs.input.clone(),
                                        group_expr: group_expr.clone(),
                                        aggregate_expr: aggr_expr.clone(),
                                        order_by: sort_columns,
                                        schema: aggregate_schema.clone(),
                                        snapshots: cs.snapshots.clone(),
                                    }),
                                };
                                if let Some(p) = projection {
                                    let in_schema = topk.schema();
                                    let out_schema = p.schema;
                                    let mut expr = Vec::with_capacity(p.input_columns.len());
                                    for out_i in 0..p.input_columns.len() {
                                        let in_field = in_schema.field(p.input_columns[out_i]);
                                        let out_name = out_schema.field(out_i).name();

                                        let mut e = field_reference(in_field);
                                        if out_name != in_field.name() {
                                            e = Expr::Alias(Box::new(e), out_name.clone())
                                        }
                                        expr.push(e);
                                    }
                                    return Ok(LogicalPlan::Projection {
                                        expr,
                                        input: Arc::new(topk),
                                        schema: p.schema.clone(),
                                    });
                                } else {
                                    return Ok(topk);
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        },
        _ => {}
    }

    Ok(p)
}

fn aggr_exprs_allow_topk(agg_exprs: &[Expr]) -> bool {
    for a in agg_exprs {
        match a {
            Expr::AggregateFunction { fun, distinct, .. } => {
                if *distinct || !fun_allows_topk(fun.clone()) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    return true;
}

fn aggr_schema_allows_topk(schema: &DFSchema, group_expr_len: usize) -> bool {
    for agg_field in &schema.fields()[group_expr_len..] {
        match agg_field.data_type() {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Int64Decimal(_) => {} // ok, continue.
            _ => return false,
        }
    }
    return true;
}

fn fun_allows_topk(f: AggregateFunction) -> bool {
    // Only monotone functions are allowed in principle.
    // Implementation also requires accumulator state and final value to be the same.
    // TODO: lift the restriction and add support for Avg.
    match f {
        AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max => true,
        AggregateFunction::Count | AggregateFunction::Avg => false,
    }
}

fn extract_aggregate_fun(e: &Expr) -> Option<AggregateFunction> {
    match e {
        Expr::AggregateFunction { fun, .. } => Some(fun.clone()),
        _ => None,
    }
}

struct ColumnProjection<'a> {
    input_columns: Vec<usize>,
    input: &'a Arc<LogicalPlan>,
    schema: &'a DFSchemaRef,
}

fn extract_column_projection(p: &LogicalPlan) -> Option<ColumnProjection> {
    match p {
        LogicalPlan::Projection {
            expr,
            input,
            schema,
        } => {
            let in_schema = input.schema();
            let mut input_columns = Vec::with_capacity(expr.len());
            for e in expr {
                match e {
                    Expr::Alias(box Expr::Column(n, q), _) | Expr::Column(n, q) => {
                        input_columns.push(field_index(in_schema, q.as_deref(), n)?)
                    }
                    _ => return None,
                }
            }
            Some(ColumnProjection {
                input_columns,
                input,
                schema,
            })
        }
        _ => None,
    }
}

fn extract_sort_columns(
    group_key_len: usize,
    sort_expr: &[Expr],
    schema: &DFSchema,
    projection: Option<&[usize]>,
) -> Option<Vec<SortColumn>> {
    let mut sort_columns = Vec::with_capacity(sort_expr.len());
    for e in sort_expr {
        match e {
            Expr::Sort {
                expr: box Expr::Column(n, q),
                asc,
                nulls_first,
            } => {
                let mut index = field_index(schema, q.as_deref(), n)?;
                if let Some(p) = projection {
                    index = p[index];
                }
                if index < group_key_len {
                    return None;
                }
                sort_columns.push(SortColumn {
                    agg_index: index - group_key_len,
                    asc: *asc,
                    nulls_first: *nulls_first,
                })
            }
            _ => return None,
        }
    }
    Some(sort_columns)
}

fn field_index(schema: &DFSchema, qualifier: Option<&str>, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|f| f.qualifier().map(|s| s.as_str()) == qualifier && f.name() == name)
}

pub fn plan_topk(
    ext_planner: &CubeExtensionPlanner,
    node: &ClusterAggregateTopK,
    input: Arc<dyn ExecutionPlan>,
    ctx: &ExecutionContextState,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let planner = DefaultPhysicalPlanner::default();

    // Partial aggregate on workers. Mimics corresponding planning code from DataFusion.
    let physical_input_schema = input.schema();
    let logical_input_schema = node.input.schema();
    let group_expr = node
        .group_expr
        .iter()
        .map(|e| {
            Ok((
                planner.create_physical_expr(e, &physical_input_schema, ctx)?,
                e.name(&logical_input_schema)?,
            ))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    let group_expr_len = group_expr.len();
    let initial_aggregate_expr = node
        .aggregate_expr
        .iter()
        .map(|e| {
            planner.create_aggregate_expr(e, &logical_input_schema, &physical_input_schema, ctx)
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    let strategy = compute_aggregation_strategy(input.as_ref(), &group_expr);
    let aggregate = Arc::new(HashAggregateExec::try_new(
        strategy,
        AggregateMode::Full,
        group_expr,
        initial_aggregate_expr.clone(),
        input,
    )?);

    let aggregate_schema = aggregate.as_ref().schema();

    // Sort on workers.
    let sort_expr = node
        .order_by
        .iter()
        .map(|c| {
            planner.create_physical_sort_expr(
                &field_reference(aggregate_schema.field(group_expr_len + c.agg_index)),
                &aggregate_schema,
                SortOptions {
                    descending: !c.asc,
                    nulls_first: c.nulls_first,
                },
                ctx,
            )
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    let sort = Arc::new(SortExec::try_new(
        sort_expr,
        aggregate,
        ctx.config.concurrency,
    )?);
    let sort_schema = sort.schema();

    // Send results to router.
    let schema = sort_schema.clone();
    let cluster = ext_planner.plan_cluster_send(sort, &node.snapshots, schema.clone())?;
    let agg_fun = node
        .aggregate_expr
        .iter()
        .map(|e| extract_aggregate_fun(e).unwrap())
        .collect_vec();
    Ok(Arc::new(AggregateTopKExec::new(
        node.limit,
        group_expr_len,
        initial_aggregate_expr,
        &agg_fun,
        node.order_by.clone(),
        cluster,
        schema,
    )))
}

fn field_reference(f: &DFField) -> Expr {
    Expr::Column(f.name().clone(), f.qualifier().cloned())
}
