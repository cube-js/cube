use crate::queryplanner::planning::{ClusterSendNode, CubeExtensionPlanner};
use crate::queryplanner::topk::execute::{AggregateTopKExec, TopKAggregateFunction};
use crate::queryplanner::topk::{
    ClusterAggregateTopKLower, ClusterAggregateTopKUpper, SortColumn, MIN_TOPK_STREAM_ROWS,
};
use crate::queryplanner::udfs::{scalar_udf_by_kind, CubeScalarUDFKind};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr::physical_name;
use datafusion::logical_expr::expr::{AggregateFunction, Alias, ScalarFunction};
use datafusion::physical_expr::PhysicalSortRequirement;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::udf::create_physical_expr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{
    Aggregate, Extension, Filter, Limit, LogicalPlan, Projection, SortExpr,
};
use datafusion::physical_planner::{create_aggregate_expr_and_maybe_filter, PhysicalPlanner};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use itertools::Itertools;
use std::cmp::max;
use std::fmt;
use std::sync::Arc;

/// Replaces `Limit(Sort(Aggregate(ClusterSend)))` with [ClusterAggregateTopK] when possible.
pub fn materialize_topk(p: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    match &p {
        LogicalPlan::Limit(Limit {
            skip,
            fetch: Some(limit),
            input: sort,
        }) => match sort.as_ref() {
            LogicalPlan::Sort(datafusion::logical_expr::Sort {
                expr: sort_expr,
                input: sort_input,
                fetch: sort_fetch,
            }) => {
                let skip_limit = *skip + *limit;
                let fetch = sort_fetch.unwrap_or(skip_limit).min(skip_limit);
                match materialize_topk_under_limit_sort(fetch, sort_expr, sort_input)? {
                    Some(topk_plan) => {
                        return Ok(if *skip == 0 {
                            topk_plan
                        } else {
                            LogicalPlan::Limit(Limit {
                                skip: *skip,
                                fetch: Some(fetch.saturating_sub(*skip)),
                                input: Arc::new(topk_plan),
                            })
                        })
                    }
                    None => {}
                }
            }
            _ => {}
        },
        LogicalPlan::Sort(datafusion::logical_expr::Sort {
            expr: sort_expr,
            input: sort_input,
            fetch: Some(limit),
        }) => match materialize_topk_under_limit_sort(*limit, sort_expr, sort_input)? {
            Some(plan) => return Ok(plan),
            None => {}
        },
        _ => {}
    }

    Ok(p)
}

/// Returns Ok(None) when materialization failed (without error) and the original plan should be returned.
fn materialize_topk_under_limit_sort(
    fetch: usize,
    sort_expr: &Vec<SortExpr>,
    sort_input: &Arc<LogicalPlan>,
) -> Result<Option<LogicalPlan>, DataFusionError> {
    let projection = extract_projections_and_havings(&sort_input)?;
    let Some(projection) = projection else {
        return Ok(None);
    };

    let aggregate: &Arc<LogicalPlan> = projection.input;
    match aggregate.as_ref() {
        LogicalPlan::Aggregate(Aggregate {
            input: cluster_send,
            group_expr,
            aggr_expr,
            schema: aggregate_schema,
            ..
        }) => {
            assert_eq!(
                aggregate_schema.fields().len(),
                group_expr.len() + aggr_expr.len()
            );
            if group_expr.len() == 0
                || aggr_expr.len() == 0
                || !aggr_exprs_allow_topk(aggr_expr)
                || !aggr_schema_allows_topk(aggregate_schema.as_ref(), group_expr.len())
            {
                return Ok(None);
            }
            let sort_columns;
            if let Some(sc) = extract_sort_columns(
                group_expr.len(),
                &sort_expr,
                sort_input.schema(),
                projection.input_columns.as_slice(),
            )? {
                sort_columns = sc;
            } else {
                return Ok(None);
            }
            match cluster_send.as_ref() {
                LogicalPlan::Extension(Extension { node }) => {
                    let cs;
                    if let Some(c) = node.as_any().downcast_ref::<ClusterSendNode>() {
                        cs = c;
                    } else {
                        return Ok(None);
                    }
                    let topk = LogicalPlan::Extension(Extension {
                        node: Arc::new(ClusterAggregateTopKUpper {
                            input: Arc::new(LogicalPlan::Extension(Extension {
                                node: Arc::new(ClusterAggregateTopKLower {
                                    input: cs.input.clone(),
                                    group_expr: group_expr.clone(),
                                    aggregate_expr: aggr_expr.clone(),
                                    schema: aggregate_schema.clone(),
                                    snapshots: cs.snapshots.clone(),
                                }),
                            })),
                            limit: fetch,
                            order_by: sort_columns,
                            having_expr: projection.having_expr.clone(),
                        }),
                    });
                    if projection.has_projection {
                        let p = projection;
                        let out_schema = p.schema;
                        let mut expr = Vec::with_capacity(p.input_columns.len());
                        for out_i in 0..p.input_columns.len() {
                            let (out_tr, out_field) = out_schema.qualified_field(out_i);

                            let mut e = p.post_projection[p.input_columns[out_i]].clone();
                            let (e_tr, e_name) = e.qualified_name();

                            if out_tr != e_tr.as_ref() || out_field.name() != &e_name {
                                e = Expr::Alias(Alias {
                                    expr: Box::new(e),
                                    relation: out_tr.cloned(),
                                    name: out_field.name().clone(),
                                });
                            }
                            expr.push(e);
                        }
                        return Ok(Some(LogicalPlan::Projection(
                            Projection::try_new_with_schema(
                                expr,
                                Arc::new(topk),
                                p.schema.clone(),
                            )?,
                        )));
                    } else {
                        return Ok(Some(topk));
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }

    Ok(None)
}

fn aggr_exprs_allow_topk(agg_exprs: &[Expr]) -> bool {
    for a in agg_exprs {
        match a {
            // TODO: Maybe topk could support filter
            Expr::AggregateFunction(AggregateFunction {
                func,
                args: _,
                distinct: false,
                filter: None,
                order_by: None,
                null_treatment: _,
                ..
            }) => {
                if !fun_allows_topk(func.as_ref()) {
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
            | DataType::Binary
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {} // ok, continue.
            _ => return false,
        }
    }
    return true;
}

fn fun_allows_topk(f: &datafusion::logical_expr::AggregateUDF) -> bool {
    // Only monotone functions are allowed in principle.
    // Implementation also requires accumulator state and final value to be the same.

    // TODO: lift the restriction and add support for Avg.

    fun_topk_type(f).is_some()
}

fn fun_topk_type(f: &datafusion::logical_expr::AggregateUDF) -> Option<TopKAggregateFunction> {
    // Using as_any() is "smarter" than using ".name()" and string-comparing but I'm not sure it's better.
    let f_any = f.inner().as_any();
    if f_any
        .downcast_ref::<datafusion::functions_aggregate::sum::Sum>()
        .is_some()
    {
        Some(TopKAggregateFunction::Sum)
    } else if f_any
        .downcast_ref::<datafusion::functions_aggregate::min_max::Min>()
        .is_some()
    {
        Some(TopKAggregateFunction::Min)
    } else if f_any
        .downcast_ref::<datafusion::functions_aggregate::min_max::Max>()
        .is_some()
    {
        Some(TopKAggregateFunction::Max)
    } else if f_any
        .downcast_ref::<crate::queryplanner::udfs::HllMergeUDF>()
        .is_some()
    {
        Some(TopKAggregateFunction::Merge)
    } else {
        None
    }
}

fn extract_aggregate_fun(e: &Expr) -> Option<(TopKAggregateFunction, &Vec<Expr>)> {
    match e {
        Expr::AggregateFunction(AggregateFunction {
            func,
            distinct: false,
            args,
            filter: _,
            order_by: _,
            null_treatment: _,
            ..
        }) => fun_topk_type(func).map(|t: TopKAggregateFunction| (t, args)),
        _ => None,
    }
}

#[derive(Debug)]
struct ColumnProjection<'a> {
    // The (sole) column indexes within `input.schema()` that the post_projection expr uses.
    input_columns: Vec<usize>,
    input: &'a Arc<LogicalPlan>,
    // Output schema (after applying `having_expr` and then `post_projection` and then aliases).  In
    // other words, this saves the top level projection's aliases.
    schema: &'a DFSchemaRef,
    // Defined on `input` schema.  Excludes Expr::Aliases necessary to produce the output schema, `schema`.
    post_projection: Vec<Expr>,
    // Defined on `input` schema
    having_expr: Option<Expr>,
    // True if there is some sort of projection seen.
    has_projection: bool,
}

fn extract_projections_and_havings(
    p: &Arc<LogicalPlan>,
) -> Result<Option<ColumnProjection>, DataFusionError> {
    // Goal:  Deal with arbitrary series of Projection and Filter, where the Projections are column
    // projections (or cardinality(column)), on top of an underlying node.
    //
    // Real world example:  p = Projection > Filter > Projection > Aggregation
    //
    // Because the Sort node above p is defined in terms of the projection outputs, it needs those
    // outputs remapped to projection inputs.

    match p.as_ref() {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            ..
        }) => {
            let in_schema = input.schema();
            let mut input_columns: Vec<usize> = Vec::with_capacity(expr.len());

            // Check that this projection is a column (or cardinality(column)) projection first.
            for e in expr {
                match e {
                    Expr::Alias(Alias {
                        expr: box Expr::Column(c),
                        relation: _,
                        name: _,
                    })
                    | Expr::Column(c) => {
                        let fi = field_index(in_schema, c.relation.as_ref(), &c.name)?;
                        input_columns.push(fi);
                    }
                    Expr::Alias(Alias {
                        expr: box Expr::ScalarFunction(ScalarFunction { func, args }),
                        relation: _,
                        name: _,
                    })
                    | Expr::ScalarFunction(ScalarFunction { func, args }) => {
                        if let Some(_) =
                            func.inner()
                                .as_any()
                                .downcast_ref::<crate::queryplanner::udfs::HllCardinality>()
                        {
                            match &args[0] {
                                Expr::Column(c) => {
                                    let fi = field_index(in_schema, c.relation.as_ref(), &c.name)?;
                                    input_columns.push(fi);
                                }
                                _ => return Ok(None),
                            }
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                };
            }

            // Now recurse.
            let inner_column_projection = extract_projections_and_havings(input)?;
            let Some(inner_column_projection) = inner_column_projection else {
                return Ok(None);
            };

            // Now apply our projection on top of the recursion

            // input_columns[i] is the (sole) column number of `input.schema()` used by expr[i].
            // inner_column_projection[j] is the (sole) column number of the presumed underlying `aggregate.schema()` used by inner expr j.
            // So inner_column_projection[input_columns[i]] is the column number of the presumed underlying `aggregate.schema()` used by expr[i].

            let mut deep_input_columns = Vec::with_capacity(expr.len());
            for i in 0..expr.len() {
                let j = input_columns[i];
                deep_input_columns.push(inner_column_projection.input_columns[j]);
            }

            let mut new_post_projection = Vec::with_capacity(expr.len());

            // And our projection's Column expressions need to be replaced with the inner post_projection expressions.
            for (i, e) in expr.iter().enumerate() {
                let new_e = e.clone().transform_up(|node| {
                    node.unalias_nested().transform_data(|node| match node {
                        Expr::Column(_) => {
                            let replacement: Expr =
                                inner_column_projection.post_projection[input_columns[i]].clone();
                            // Transformed::yes/no doesn't matter here.
                            // let unequal = &replacement != &node;
                            Ok(Transformed::yes(replacement))
                        }
                        _ => Ok(Transformed::no(node)),
                    })
                })?;
                new_post_projection.push(new_e.data);
            }

            let column_projection = ColumnProjection {
                input_columns: deep_input_columns,
                input: inner_column_projection.input,
                schema,
                post_projection: new_post_projection,
                having_expr: inner_column_projection.having_expr,
                has_projection: true,
            };

            return Ok(Some(column_projection));
        }
        LogicalPlan::Filter(Filter {
            predicate,
            input,
            having: _,
            ..
        }) => {
            // Filter's "having" flag is not relevant to us.  It is used by DF to get the proper wildcard
            // expansion behavior in the analysis pass (before LogicalPlan optimizations, and before we
            // materialize the topk node here).

            // First, recurse.
            let inner_column_projection = extract_projections_and_havings(input)?;
            let Some(inner_column_projection) = inner_column_projection else {
                return Ok(None);
            };

            let in_schema = input.schema();

            // Our filter's columns, defined in terms of in_schema, need to be mapped to inner_column_projection.input.schema().
            let transformed_predicate = predicate
                .clone()
                .transform_up(|node| {
                    node.unalias_nested().transform_data(|node| match node {
                        Expr::Column(c) => {
                            let fi = field_index(in_schema, c.relation.as_ref(), &c.name)?;
                            let replacement = inner_column_projection.post_projection[fi].clone();
                            // Transformed::yes/no doesn't matter here.
                            // let unequal = &replacement != &node;
                            Ok(Transformed::yes(replacement))
                        }
                        _ => Ok(Transformed::no(node)),
                    })
                })?
                .data;

            let column_projection = ColumnProjection {
                input_columns: inner_column_projection.input_columns,
                input: inner_column_projection.input,
                schema: inner_column_projection.schema,
                post_projection: inner_column_projection.post_projection,
                having_expr: Some(
                    if let Some(previous_predicate) = inner_column_projection.having_expr {
                        previous_predicate.and(transformed_predicate)
                    } else {
                        transformed_predicate
                    },
                ),
                has_projection: inner_column_projection.has_projection,
            };

            return Ok(Some(column_projection));
        }
        _ => {
            let in_schema = p.schema();
            let post_projection: Vec<Expr> = in_schema
                .iter()
                .map(|(in_field_qualifier, in_field)| {
                    Expr::Column(datafusion::common::Column {
                        relation: in_field_qualifier.cloned(),
                        name: in_field.name().clone(),
                    })
                })
                .collect();
            let column_projection = ColumnProjection {
                input_columns: (0..post_projection.len()).collect(),
                input: p,
                schema: in_schema,
                post_projection,
                having_expr: None,
                has_projection: false,
            };
            return Ok(Some(column_projection));
        }
    }
}

fn extract_sort_columns(
    group_key_len: usize,
    sort_expr: &[SortExpr],
    schema: &DFSchema,
    projection: &[usize],
) -> Result<Option<Vec<SortColumn>>, DataFusionError> {
    let mut sort_columns = Vec::with_capacity(sort_expr.len());
    for e in sort_expr {
        let SortExpr {
            expr,
            asc,
            nulls_first,
        } = e;
        match expr {
            Expr::Column(c) => {
                let mut index = field_index(schema, c.relation.as_ref(), &c.name)?;
                index = projection[index];
                if index < group_key_len {
                    return Ok(None);
                }
                sort_columns.push(SortColumn {
                    agg_index: index - group_key_len,
                    asc: *asc,
                    nulls_first: *nulls_first,
                })
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(sort_columns))
}

// It is actually an error if expressions are nonsense expressions that don't evaluate on the given
// schema.  So we return Result (instead of Option<_>) now.
fn field_index(
    schema: &DFSchema,
    qualifier: Option<&TableReference>,
    name: &str,
) -> Result<usize, DataFusionError> {
    // Calling field_not_found is exactly `schema.index_of_column(col: &Column)` behavior.
    schema
        .index_of_column_by_name(qualifier, name)
        .ok_or_else(|| datafusion::common::field_not_found(qualifier.cloned(), name, schema))
}

pub fn plan_topk(
    planner: &dyn PhysicalPlanner,
    ext_planner: &CubeExtensionPlanner,
    upper_node: &ClusterAggregateTopKUpper,
    lower_node: &ClusterAggregateTopKLower,
    input: Arc<dyn ExecutionPlan>,
    ctx: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // Partial aggregate on workers. Mimics corresponding planning code from DataFusion.
    let physical_input_schema = input.schema();
    let logical_input_schema = lower_node.input.schema();
    let group_expr = lower_node
        .group_expr
        .iter()
        .map(|e| {
            Ok((
                planner.create_physical_expr(e, &logical_input_schema, ctx)?,
                physical_name(e)?,
            ))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    let group_expr_len = group_expr.len();
    let groups = PhysicalGroupBy::new_single(group_expr);
    let initial_agg_filter: Vec<(
        datafusion::physical_plan::udaf::AggregateFunctionExpr,
        Option<Arc<dyn PhysicalExpr>>,
        Option<Vec<PhysicalSortExpr>>,
    )> = lower_node
        .aggregate_expr
        .iter()
        .map(|e| {
            create_aggregate_expr_and_maybe_filter(
                e,
                logical_input_schema,
                &physical_input_schema,
                ctx.execution_props(),
            )
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    let (initial_aggregate_expr, initial_filters, _order_bys): (Vec<_>, Vec<_>, Vec<_>) =
        itertools::multiunzip(initial_agg_filter);

    let aggregate = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        groups.clone(),
        initial_aggregate_expr.clone(),
        initial_filters.clone(),
        input,
        physical_input_schema.clone(),
    )?);

    let aggregate_schema = aggregate.schema();
    // This is only used in make_sort_expr with HllCardinality, which doesn't use the schema in
    // create_physical_expr.  So this value is unused.  Which means that creating a DFSchema that is
    // missing qualifiers and other info is okay.
    let aggregate_dfschema = Arc::new(DFSchema::try_from(aggregate_schema.clone())?);

    let agg_fun = lower_node
        .aggregate_expr
        .iter()
        .map(|e| extract_aggregate_fun(e).unwrap())
        .collect_vec();

    // Sort on workers.
    let sort_expr = upper_node
        .order_by
        .iter()
        .map(|c| {
            let i = group_expr_len + c.agg_index;
            PhysicalSortExpr {
                expr: make_sort_expr(
                    &aggregate_schema,
                    &agg_fun[c.agg_index].0,
                    Arc::new(Column::new(aggregate_schema.field(i).name(), i)),
                    agg_fun[c.agg_index].1,
                    &aggregate_dfschema,
                ),
                options: SortOptions {
                    descending: !c.asc,
                    nulls_first: c.nulls_first,
                },
            }
        })
        .collect_vec();
    let sort_requirement = sort_expr
        .iter()
        .map(|e| PhysicalSortRequirement::from(e.clone()))
        .collect::<Vec<_>>();
    let sort = Arc::new(SortExec::new(sort_expr, aggregate));
    let sort_schema = sort.schema();

    // Send results to router.
    let schema = sort_schema.clone();
    let cluster = ext_planner.plan_cluster_send(
        sort,
        &lower_node.snapshots,
        /*use_streaming*/ true,
        /*max_batch_rows*/ max(2 * upper_node.limit, MIN_TOPK_STREAM_ROWS),
        None,
        None,
        Some(sort_requirement.clone()),
    )?;

    let having = if let Some(predicate) = &upper_node.having_expr {
        Some(planner.create_physical_expr(predicate, &lower_node.schema, ctx)?)
    } else {
        None
    };

    let topk_exec: Arc<AggregateTopKExec> = Arc::new(AggregateTopKExec::new(
        upper_node.limit,
        group_expr_len,
        initial_aggregate_expr,
        &agg_fun
            .into_iter()
            .map(|(tkaf, _)| tkaf)
            .collect::<Vec<_>>(),
        upper_node.order_by.clone(),
        having,
        cluster,
        schema,
        sort_requirement,
    ));
    Ok(topk_exec)
}

pub fn make_sort_expr(
    schema: &Arc<Schema>,
    fun: &TopKAggregateFunction,
    col: Arc<dyn PhysicalExpr>,
    args: &[Expr],
    logical_schema: &DFSchema,
) -> Arc<dyn PhysicalExpr> {
    // Note that logical_schema is computed by our caller from schema, may lack qualifiers or other
    // info, and this works OK because HllCardinality's trait implementation functions don't use the
    // schema in create_physical_expr.
    match fun {
        TopKAggregateFunction::Merge => create_physical_expr(
            &scalar_udf_by_kind(CubeScalarUDFKind::HllCardinality),
            &[col],
            schema,
            args,
            logical_schema,
        )
        .unwrap(),
        _ => col,
    }
}

/// Temporarily used to bamboozle DF while constructing the initial plan -- so that we pass its
/// assertions about the output schema.  Hypothetically, we instead might actually place down a
/// legitimate AggregateExec node, and then have the ClusterAggregateTopKUpper node replace that
/// child.
#[derive(Debug)]
pub struct DummyTopKLowerExec {
    pub schema: Arc<Schema>,
    pub input: Arc<dyn ExecutionPlan>,
}

impl datafusion::physical_plan::DisplayAs for DummyTopKLowerExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(f, "DummyTopKLowerExec")
    }
}

impl ExecutionPlan for DummyTopKLowerExec {
    fn name(&self) -> &str {
        "DummyTopKLowerExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        panic!("DataFusion invoked DummyTopKLowerExec::properties");
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        panic!("DataFusion invoked DummyTopKLowerExec::with_new_children");
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        panic!("DataFusion invoked DummyTopKLowerExec::execute");
    }
}
