use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Join, LogicalPlan};
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

    let join_context = match &p {
        LogicalPlan::Join(Join { left, right, .. }) => vec![
            (left.clone(), f.enter_join_left(&p, ctx)),
            (right.clone(), f.enter_join_right(&p, ctx)),
        ],
        _ => Vec::new(),
    };

    // TODO upgrade DF: Check callers to see if we want to handle subquery expressions.

    p.map_children(|c| {
        let next_ctx = join_context
            .iter()
            .find(|(n, _)| n.as_ref() == &c)
            .and_then(|(_, join_ctx)| join_ctx.as_ref())
            .unwrap_or(ctx);
        rewrite_plan_impl(c, next_ctx, f)
    })?
    .transform_parent(|n| f.rewrite(n, ctx).map(|new| Transformed::yes(new)))
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
