use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;

/// Recursively applies a transformation on each node and rewrites the plan. The plan is traversed
/// bottom-up, top-down information can be propagated via context, see [PlanRewriter] for details.
pub fn rewrite_plan<R: PlanRewriter>(
    p: &'a LogicalPlan,
    ctx: &'a R::Context,
    f: &'a mut R,
) -> Result<LogicalPlan, DataFusionError> {
    let updated_ctx = f.enter_node(p, ctx);
    let ctx = updated_ctx.as_ref().unwrap_or(ctx);

    // First, update children.
    let updated = match p {
        LogicalPlan::Projection {
            expr,
            input,
            schema,
        } => LogicalPlan::Projection {
            expr: expr.clone(),
            input: Arc::new(rewrite_plan(input.as_ref(), ctx, f)?),
            schema: schema.clone(),
        },
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate: predicate.clone(),
            input: Arc::new(rewrite_plan(input.as_ref(), ctx, f)?),
        },
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        } => LogicalPlan::Aggregate {
            input: Arc::new(rewrite_plan(input.as_ref(), ctx, f)?),
            group_expr: group_expr.clone(),
            aggr_expr: aggr_expr.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Sort { expr, input } => LogicalPlan::Sort {
            expr: expr.clone(),
            input: Arc::new(rewrite_plan(input.as_ref(), ctx, f)?),
        },
        LogicalPlan::Union {
            inputs,
            schema,
            alias,
        } => LogicalPlan::Union {
            inputs: {
                let mut new_inputs = Vec::new();
                for i in inputs.iter() {
                    new_inputs.push(Arc::new(rewrite_plan(i, ctx, f)?))
                }
                new_inputs
            },
            schema: schema.clone(),
            alias: alias.clone(),
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            schema,
        } => LogicalPlan::Join {
            left: Arc::new(rewrite_plan(
                left.as_ref(),
                f.enter_join_left(p, ctx).as_ref().unwrap_or(ctx),
                f,
            )?),
            right: Arc::new(rewrite_plan(
                right.as_ref(),
                f.enter_join_right(p, ctx).as_ref().unwrap_or(ctx),
                f,
            )?),
            on: on.clone(),
            join_type: *join_type,
            schema: schema.clone(),
        },
        LogicalPlan::Repartition {
            input,
            partitioning_scheme,
        } => LogicalPlan::Repartition {
            input: Arc::new(rewrite_plan(input, ctx, f)?),
            partitioning_scheme: partitioning_scheme.clone(),
        },
        p @ LogicalPlan::TableScan { .. } => p.clone(),
        p @ LogicalPlan::EmptyRelation { .. } => p.clone(),
        LogicalPlan::Limit { n, input } => LogicalPlan::Limit {
            n: *n,
            input: Arc::new(rewrite_plan(input, ctx, f)?),
        },
        p @ LogicalPlan::CreateExternalTable { .. } => p.clone(),
        LogicalPlan::Explain {
            verbose,
            plan,
            stringified_plans,
            schema,
        } => LogicalPlan::Explain {
            verbose: *verbose,
            plan: Arc::new(rewrite_plan(plan, ctx, f)?),
            stringified_plans: stringified_plans.clone(),
            schema: schema.clone(),
        },
        p @ LogicalPlan::Extension { .. } => p.clone(),
    };

    // Update the resulting plan.
    f.rewrite(updated, ctx)
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
    p: &dyn ExecutionPlan,
    rewriter: &mut F,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>
where
    F: FnMut(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>,
{
    let new_children = p
        .children()
        .into_iter()
        .map(|c| rewrite_physical_plan(c.as_ref(), rewriter))
        .collect::<Result<_, DataFusionError>>()?;
    let new_plan = p.with_new_children(new_children)?;
    rewriter(new_plan)
}
