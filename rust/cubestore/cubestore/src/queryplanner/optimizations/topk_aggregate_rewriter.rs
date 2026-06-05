use crate::queryplanner::planning::WorkerExec;
use crate::queryplanner::query_executor::ClusterSendExec;
use crate::queryplanner::topk_aggregate::TopKHashAggregateExec;
use datafusion::arrow::compute::SortOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, InputOrderMode};
use std::sync::Arc;

/// Trim the worker-side partial hash aggregate to the top-k groups when the plan is
/// `LIMIT k` over `ORDER BY <subset of group-by columns>` over a distributed hash aggregate.
///
/// Correctness requires a TOTAL order over groups (`T = ORDER BY ++ remaining group-by columns`,
/// in group-by order) applied in TWO places that must agree:
///   - the worker cut: each worker keeps its local top-k by `T`;
///   - the router select: the global Sort + Limit must also order by `T`.
/// Under `T` the router's top-k equals the global top-k by `T`, and every worker that holds a
/// partial state for such a group keeps it (its local rank can only be smaller), so every needed
/// partial state reaches the router. Ordering the router by `T` instead of the bare `ORDER BY` does
/// not change the query contract: `ORDER BY` is a prefix of `T`, so the output stays validly
/// ordered and the previously-unspecified tie order just becomes deterministic.
///
/// We only rewrite when the plan matches exactly `Sort(/Limit) -> [passthrough] -> Final aggregate
/// -> [passthrough/cluster boundary] -> Partial hash aggregate`; anything else on the path (a
/// HAVING filter, a nested aggregate, a computed projection) makes us bail, so we never trim a plan
/// where the limit does not directly govern this aggregate.
///
/// `factor` gates trimming at runtime (only when local groups exceed `factor * k`); `0` disables.
pub fn replace_with_topk_aggregate(
    plan: Arc<dyn ExecutionPlan>,
    factor: usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if factor == 0 {
        return Ok(plan);
    }
    let Some(target) = analyze(&plan) else {
        return Ok(plan);
    };
    apply(plan, &target, factor)
}

struct Target {
    /// The router `SortExec` whose ordering must be extended to the total order.
    sort: Arc<dyn ExecutionPlan>,
    /// The worker-side partial hash `AggregateExec` to replace with a trimming exec.
    partial: Arc<dyn ExecutionPlan>,
    /// Tail of the total order to append to the router sort (over the sort's input schema).
    router_tail: Vec<PhysicalSortExpr>,
    /// Full total order over the partial output schema for the worker cut.
    trim_order: Vec<(usize, SortOptions)>,
    /// `k = limit + offset`.
    k: usize,
}

fn analyze(root: &Arc<dyn ExecutionPlan>) -> Option<Target> {
    // Peel an optional top GlobalLimit (carries the offset), then require a SortExec.
    let (skip, extra_fetch, sort_node) =
        if let Some(gl) = root.as_any().downcast_ref::<GlobalLimitExec>() {
            (gl.skip(), gl.fetch(), child(root)?)
        } else {
            (0, None, root.clone())
        };
    let sort = sort_node.as_any().downcast_ref::<SortExec>()?;
    let order: Vec<PhysicalSortExpr> = sort.expr().iter().cloned().collect();
    if order.is_empty() {
        return None;
    }
    // The worker must keep enough groups to cover `limit + offset`. When a top GlobalLimit carries
    // the offset, DataFusion already folds `skip + limit` into the sort's fetch, so prefer it;
    // otherwise fall back to the GlobalLimit's own `skip + fetch`.
    let k = sort
        .fetch()
        .or_else(|| extra_fetch.map(|fetch| skip + fetch))?;

    // Sort -> [passthrough] -> Final aggregate.
    let final_agg_node = descend_to_final_aggregate(sort.input().clone())?;
    let final_agg = final_agg_node.as_any().downcast_ref::<AggregateExec>()?;

    // Final aggregate -> [passthrough/boundary] -> Partial hash aggregate.
    let partial_node = descend_to_worker_partial(final_agg.input().clone())?;
    let partial = partial_node.as_any().downcast_ref::<AggregateExec>()?;
    if !partial.group_expr().is_single()
        || matches!(partial.input_order_mode(), InputOrderMode::Sorted)
    {
        return None;
    }

    let num_group_cols = partial.group_expr().output_exprs().len();
    if num_group_cols == 0 {
        return None;
    }
    let partial_schema = partial.schema();
    let group_names: Vec<String> = partial_schema
        .fields()
        .iter()
        .take(num_group_cols)
        .map(|f| f.name().clone())
        .collect();

    // Map ORDER BY columns onto group-by columns (by name; robust to projections).
    let mut used = vec![false; num_group_cols];
    let mut trim_order: Vec<(usize, SortOptions)> = Vec::with_capacity(num_group_cols);
    for e in &order {
        let column = e.expr.as_any().downcast_ref::<Column>()?;
        let idx = group_names.iter().position(|n| n == column.name())?;
        if used[idx] {
            continue;
        }
        used[idx] = true;
        trim_order.push((idx, e.options));
    }
    if trim_order.is_empty() {
        return None;
    }

    // Totalize: append the remaining group-by columns in group-by order. Build the matching tail
    // for the router sort over its own (Final-output) schema, resolved by name.
    let sort_input_schema = sort.input().schema();
    let mut router_tail: Vec<PhysicalSortExpr> = Vec::new();
    for (idx, is_used) in used.into_iter().enumerate() {
        if is_used {
            continue;
        }
        let name = &group_names[idx];
        let options = SortOptions::default();
        let sort_col_idx = sort_input_schema.index_of(name).ok()?;
        router_tail.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(name, sort_col_idx)),
            options,
        });
        trim_order.push((idx, options));
    }

    Some(Target {
        sort: sort_node,
        partial: partial_node,
        router_tail,
        trim_order,
        k,
    })
}

fn apply(
    node: Arc<dyn ExecutionPlan>,
    target: &Target,
    factor: usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let is_sort = Arc::ptr_eq(&node, &target.sort);
    let is_partial = Arc::ptr_eq(&node, &target.partial);

    let new_children = node
        .children()
        .into_iter()
        .map(|c| apply(c.clone(), target, factor))
        .collect::<Result<Vec<_>, _>>()?;
    let node = node.with_new_children(new_children)?;

    if is_partial {
        if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
            if let Some(exec) = TopKHashAggregateExec::try_new_from_partial(
                agg,
                target.k,
                factor,
                target.trim_order.clone(),
            ) {
                return Ok(Arc::new(exec));
            }
        }
        // Leaving the full aggregate in place stays correct; the router still sorts by the total
        // order, it just receives every group instead of the trimmed top-k.
        return Ok(node);
    }

    if is_sort {
        if let Some(sort) = node.as_any().downcast_ref::<SortExec>() {
            let mut exprs: Vec<PhysicalSortExpr> = sort.expr().iter().cloned().collect();
            exprs.extend(target.router_tail.iter().cloned());
            let new_sort = SortExec::new(LexOrdering::new(exprs), sort.input().clone())
                .with_preserve_partitioning(sort.preserve_partitioning())
                .with_fetch(sort.fetch());
            return Ok(Arc::new(new_sort));
        }
    }

    Ok(node)
}

/// Walk down single-child passthrough nodes (which preserve rows and grouping) until the first
/// `Final`/`FinalPartitioned` `AggregateExec`. Returns `None` if a non-passthrough node is hit
/// first (e.g. a filter or a computed projection).
fn descend_to_final_aggregate(mut node: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    loop {
        if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
            return matches!(
                agg.mode(),
                AggregateMode::Final | AggregateMode::FinalPartitioned
            )
            .then_some(node.clone());
        }
        if is_row_passthrough(&node) {
            node = child(&node)?;
        } else {
            return None;
        }
    }
}

/// Walk down passthrough nodes from a `Final` aggregate's input to the worker-side `Partial`
/// aggregate, requiring that exactly one `ClusterSend`/`Worker` boundary is crossed. Returns `None`
/// if anything unexpected (a second aggregate, a filter, ...) is on the path.
fn descend_to_worker_partial(mut node: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let mut crossed_boundary = false;
    loop {
        if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
            return (crossed_boundary && *agg.mode() == AggregateMode::Partial)
                .then_some(node.clone());
        }
        if node.as_any().is::<ClusterSendExec>() || node.as_any().is::<WorkerExec>() {
            crossed_boundary = true;
            node = child(&node)?;
        } else if is_row_passthrough(&node) {
            node = child(&node)?;
        } else {
            return None;
        }
    }
}

/// Single-child nodes that pass rows through unchanged (preserving grouping), so a limit/sort above
/// them governs the aggregate below them.
fn is_row_passthrough(node: &Arc<dyn ExecutionPlan>) -> bool {
    let any = node.as_any();
    any.is::<CoalescePartitionsExec>()
        || any.is::<SortPreservingMergeExec>()
        || any.is::<ClusterSendExec>()
        || any.is::<WorkerExec>()
}

fn child(node: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let children = node.children();
    if children.len() != 1 {
        return None;
    }
    Some(children[0].clone())
}
