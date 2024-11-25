use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    Aggregate, Explain, Extension, Filter, Join, Limit, LogicalPlan, Projection, Repartition, Sort,
    Union,
};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Recursively applies a transformation on each node and rewrites the plan. The plan is traversed
/// bottom-up, top-down information can be propagated via context, see [PlanRewriter] for details.
pub fn rewrite_plan<'a, R: crate::queryplanner::optimizations::rewrite_plan::PlanRewriter>(
    p: LogicalPlan,
    ctx: &'a R::Context,
    f: &'a mut R,
) -> Result<LogicalPlan, DataFusionError> {
    Ok(rewrite_plan_impl(p, ctx, f)?.data)
}

pub fn rewrite_plan_impl<'a, R: PlanRewriter>(
    p: LogicalPlan,
    ctx: &'a R::Context,
    f: &'a mut R,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    let updated_ctx = f.enter_node(&p, ctx);
    let ctx = updated_ctx.as_ref().unwrap_or(ctx);

    p.map_children(|c| rewrite_plan_impl(c, ctx, f))?
        .transform_parent(|n| f.rewrite(n, ctx).map(|new| Transformed::yes(new)))

    // // First, update children.
    // let updated = match p {
    //     LogicalPlan::Projection(Projection {
    //         expr,
    //         input,
    //         schema,
    //         ..
    //     }) => LogicalPlan::Projection(Projection::try_new_with_schema(
    //         expr.clone(),
    //         Arc::new(rewrite_plan(input.as_ref(), ctx, f)?),
    //         schema.clone(),
    //     )?),
    //     LogicalPlan::Filter (Filter { predicate, input, having, .. }) => LogicalPlan::Filter(Filter {
    //         predicate: predicate.clone(),
    //         input: Arc::new(rewrite_plan(input.as_ref(), ctx, f)?),
    //         having: *having,
    //     }),
    //     LogicalPlan::Aggregate(Aggregate {
    //         input,
    //         group_expr,
    //         aggr_expr,
    //         schema,
    //     }) => LogicalPlan::Aggregate( Aggregate {
    //         input: Arc::new(rewrite_plan(input.as_ref(), ctx, f)?),
    //         group_expr: group_expr.clone(),
    //         aggr_expr: aggr_expr.clone(),
    //         schema: schema.clone(),
    //     }),
    //     LogicalPlan::Sort(Sort { expr, input, fetch }) => LogicalPlan::Sort(Sort {
    //         expr: expr.clone(),
    //         input: Arc::new(rewrite_plan(input.as_ref(), ctx, f)?),
    //         fetch: fetch.clone(),
    //     }),
    //     LogicalPlan::Union(Union {
    //         inputs,
    //         schema,
    //     }) => LogicalPlan::Union(Union {
    //         inputs: {
    //             let mut new_inputs = Vec::new();
    //             for i in inputs.iter() {
    //                 new_inputs.push(Arc::new(rewrite_plan(i, ctx, f)?))
    //             }
    //             new_inputs
    //         },
    //         schema: schema.clone(),
    //     }),
    //     LogicalPlan::Join (Join {
    //         left,
    //         right,
    //         on,
    //         filter, join_type,
    //         join_constraint,
    //         schema, null_equals_null,
    //                        }) => LogicalPlan::Join (Join {
    //         left: Arc::new(rewrite_plan(
    //             left.as_ref(),
    //             f.enter_join_left(p, ctx).as_ref().unwrap_or(ctx),
    //             f,
    //         )?),
    //         right: Arc::new(rewrite_plan(
    //             right.as_ref(),
    //             f.enter_join_right(p, ctx).as_ref().unwrap_or(ctx),
    //             f,
    //         )?),
    //         on: on.clone(),
    //         filter: filter.clone(),
    //         join_type: *join_type,
    //         join_constraint: *join_constraint,
    //         schema: schema.clone(),
    //
    //         null_equals_null: false,
    //     }),
    //     LogicalPlan::Repartition(Repartition {
    //         input,
    //         partitioning_scheme,
    //     }) => LogicalPlan::Repartition( Repartition {
    //         input: Arc::new(rewrite_plan(input, ctx, f)?),
    //         partitioning_scheme: partitioning_scheme.clone(),
    //     }),
    //     p @ LogicalPlan::TableScan { .. } => p.clone(),
    //     p @ LogicalPlan::EmptyRelation { .. } => p.clone(),
    //     LogicalPlan::Limit(Limit { skip, fetch, input }) => LogicalPlan::Limit(Limit {
    //         skip: skip.clone(),
    //         fetch: fetch.clone(),
    //         input: Arc::new(rewrite_plan(input, ctx, f)?),
    //     }),
    //     LogicalPlan::Explain(Explain {
    //         verbose,
    //         plan,
    //         stringified_plans,
    //         schema,
    //                              logical_optimization_succeeded,
    //      }) => LogicalPlan::Explain(Explain {
    //         verbose: *verbose,
    //         plan: Arc::new(rewrite_plan(plan, ctx, f)?),
    //         stringified_plans: stringified_plans.clone(),
    //         schema: schema.clone(),
    //         logical_optimization_succeeded: *logical_optimization_succeeded,
    //     }),
    //     LogicalPlan::Extension(Extension { node }) => LogicalPlan::Extension (Extension {
    //         node: node.from_template(
    //             &node.expressions(),
    //             &node
    //                 .inputs()
    //                 .into_iter()
    //                 .map(|p| rewrite_plan(p, ctx, f))
    //                 .collect::<Result<Vec<_>, _>>()?,
    //         ),
    //     }),
    //     LogicalPlan::Window { .. } => {
    //         return Err(DataFusionError::Internal(
    //             "unsupported operation".to_string(),
    //         ))
    //     }
    // };
    //
    // struct PlanRewriterTreeNodeRewriteAdapter {
    //     p: &'a LogicalPlan,
    //     ctx: &'a R::Context,
    //     f: &'a mut R,
    // }
    //
    // impl TreeNodeRewriter for PlanRewriterTreeNodeRewriteAdapter {
    //     type Node = LogicalPlan;
    //
    //     fn f_down(&mut self, node: Self::Node) -> datafusion::common::Result<Transformed<Self::Node>> {
    //         todo!()
    //     }
    //
    //
    //     fn f_up(&mut self, node: Self::Node) -> datafusion::common::Result<Transformed<Self::Node>> {
    //         todo!()
    //     }
    // }
    //
    // // Update the resulting plan.
    // f.rewrite(updated, ctx)
}

pub trait PlanRewriter {
    /// Use this to propagate data in top-down direction, update with [enter_node] and similar.
    type Context;

    fn rewrite(
        &mut self,
        n: LogicalPlan,
        c: &Self::Context,
    ) -> Result<LogicalPlan, DataFusionError>;

    /// Calls to `rewrite` and `enter_*` on [n] and its descendants will see the context returned
    /// by this function. Returning `None` leaves the parent context unchanged.
    fn enter_node(&mut self, _n: &LogicalPlan, _c: &Self::Context) -> Option<Self::Context> {
        None
    }
    fn enter_join_left(
        &mut self,
        _join: &LogicalPlan,
        _c: &Self::Context,
    ) -> Option<Self::Context> {
        None
    }
    fn enter_join_right(
        &mut self,
        _join: &LogicalPlan,
        _c: &Self::Context,
    ) -> Option<Self::Context> {
        None
    }
}

pub fn rewrite_physical_plan<F>(
    p: Arc<dyn ExecutionPlan>,
    rewriter: &mut F,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>
where
    F: FnMut(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>,
{
    let new_children = p
        .children()
        .into_iter()
        .map(|c| rewrite_physical_plan(c.clone(), rewriter))
        .collect::<Result<_, DataFusionError>>()?;
    let new_plan = p.with_new_children(new_children)?;
    rewriter(new_plan)
}
