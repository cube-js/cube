use crate::cluster::WorkerPlanningParams;
use crate::queryplanner::planning::WorkerExec;
use crate::queryplanner::query_executor::ClusterSendExec;
use crate::queryplanner::tail_limit::TailLimitExec;
use crate::queryplanner::topk::AggregateTopKExec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{internal_datafusion_err, HashMap};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalSortRequirement};
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::PhysicalOptimizerRule as _;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PhysicalExpr};
use itertools::Itertools as _;
use std::collections::HashSet;
use std::sync::Arc;

/// Transforms from:
///     AggregateFinal
///     `- AggregatePartial
///        `- ClusterSend
/// to:
///     AggregateFinal
///     `- ClusterSend
///        `- AggregatePartial
///
/// The latter gives results in more parallelism and less network.
pub fn push_aggregate_to_workers(
    p_final: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p_final_agg: &AggregateExec;
    let p_partial: &Arc<dyn ExecutionPlan>;
    if let Some(a) = p_final.as_any().downcast_ref::<AggregateExec>() {
        if matches!(
            a.mode(),
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ) {
            p_final_agg = a;
            p_partial = a.input();
        } else {
            return Ok(p_final);
        }
    } else {
        return Ok(p_final);
    }

    let agg;
    if let Some(a) = p_partial.as_any().downcast_ref::<AggregateExec>() {
        agg = a;
    } else {
        return Ok(p_final);
    }
    if *agg.mode() != AggregateMode::Partial {
        return Ok(p_final);
    }

    let p_final_input: Arc<dyn ExecutionPlan> =
        if let Some(cs) = agg.input().as_any().downcast_ref::<ClusterSendExec>() {
            let clustersend_input = p_partial
                .clone()
                .with_new_children(vec![cs.input_for_optimizations.clone()])?;

            // Note that required_input_ordering is applicable when p_final_agg has a Sorted input mode.

            // Router plan, replace partial aggregate with cluster send.
            Arc::new(
                cs.with_changed_schema(
                    clustersend_input,
                    p_final_agg
                        .required_input_ordering()
                        .into_iter()
                        .next()
                        .unwrap(),
                ),
            )
        } else if let Some(w) = agg.input().as_any().downcast_ref::<WorkerExec>() {
            let worker_input = p_partial.clone().with_new_children(vec![w.input.clone()])?;

            // Worker plan, execute partial aggregate inside the worker.
            Arc::new(WorkerExec::new(
                worker_input,
                w.max_batch_rows,
                // TODO upgrade DF: WorkerExec limit_and_reverse must be wrong here.  Should be
                // None.  Same applies to cs.with_changed_schema.
                w.limit_and_reverse.clone(),
                p_final_agg
                    .required_input_ordering()
                    .into_iter()
                    .next()
                    .unwrap(),
                WorkerPlanningParams {
                    worker_partition_count: w.properties().output_partitioning().partition_count(),
                },
            ))
        } else {
            return Ok(p_final);
        };

    // We change AggregateMode::FinalPartitioned to AggregateMode::Final, because the ClusterSend
    // node ends up creating an incompatible partitioning for FinalPartitioned.  Some other ideas,
    // like adding a RepartitionExec node, would just be redundant with the behavior of
    // AggregateExec::Final, and also, tricky to set up with the ideal number of partitions in the
    // middle of optimization passes.  Having ClusterSend be able to pass through hash partitions in
    // some form is another option.
    let p_final_input_schema = p_final_input.schema();
    Ok(Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        p_final_agg.group_expr().clone(),
        p_final_agg.aggr_expr().to_vec(),
        p_final_agg.filter_expr().to_vec(),
        p_final_input,
        p_final_input_schema,
    )?))
}

pub fn ensure_partition_merge_helper(
    p: Arc<dyn ExecutionPlan>,
    new_child: &mut bool,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if p.as_any().is::<ClusterSendExec>()
        || p.as_any().is::<WorkerExec>()
        || p.as_any().is::<UnionExec>()
    {
        let rewritten: Arc<dyn ExecutionPlan> = if let Some(ordering) = p.output_ordering() {
            let ordering = ordering.to_vec();
            let merged_children = p
                .children()
                .into_iter()
                .map(|c| -> Arc<dyn ExecutionPlan> {
                    Arc::new(SortPreservingMergeExec::new(
                        LexOrdering::new(ordering.clone()),
                        c.clone(),
                    ))
                })
                .collect();
            let new_plan = p.clone().with_new_children(merged_children)?;
            Arc::new(SortPreservingMergeExec::new(
                LexOrdering::new(ordering),
                new_plan,
            ))
        } else {
            let merged_children = p
                .children()
                .into_iter()
                .map(|c| -> Arc<dyn ExecutionPlan> {
                    Arc::new(CoalescePartitionsExec::new(c.clone()))
                })
                .collect();
            let new_plan = p.clone().with_new_children(merged_children)?;
            Arc::new(CoalescePartitionsExec::new(new_plan))
        };
        *new_child = true;
        Ok(rewritten)
    } else {
        Ok(p)
    }
}

pub fn ensure_partition_merge(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let mut new_child = false;
    ensure_partition_merge_helper(p, &mut new_child)
}

// TODO upgrade DF: this one was handled by something else but most likely only in sorted scenario
pub fn ensure_partition_merge_with_acceptable_parent(
    parent: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // TODO upgrade DF: Figure out the right clean way to handle this function in general --
    // possibly involving uncommenting EnforceDistribution, and having this
    // SortPreservingMergeExec/CoalescePartitionsExec wrapping the ClusterSendExec node as we
    // construct the query.

    // Special case, don't do this inside AggregateTopKExec-ClusterSendExec-Aggregate because we
    // need the partitioning: (This is gross.)
    if parent.as_any().is::<AggregateTopKExec>() {
        return Ok(parent);
    }

    let mut any_new_children = false;
    let mut new_children = Vec::new();

    for p in parent.children() {
        new_children.push(ensure_partition_merge_helper(
            p.clone(),
            &mut any_new_children,
        )?);
    }
    if any_new_children {
        parent.with_new_children(new_children)
    } else {
        Ok(parent)
    }
}

/// Add `GlobalLimitExec` behind worker node if this node has `limit` property set and applies DF
/// `LimitPushdown` optimizer. Should be executed after all optimizations which can move `Worker`
/// node or change its input.  `config` is ignored -- we pass it to DF's `LimitPushdown` optimizer,
/// which also ignores it (as of DF 46.0.1).
pub fn add_limit_to_workers(
    p: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let limit_and_reverse;
    let input;
    if let Some(w) = p.as_any().downcast_ref::<WorkerExec>() {
        limit_and_reverse = w.limit_and_reverse;
        input = &w.input;
    } else if let Some(cs) = p.as_any().downcast_ref::<ClusterSendExec>() {
        limit_and_reverse = cs.limit_and_reverse;
        input = &cs.input_for_optimizations;
    } else {
        return Ok(p);
    }

    let Some((limit, reverse)) = limit_and_reverse else {
        return Ok(p);
    };
    if reverse {
        let limit = Arc::new(TailLimitExec::new(input.clone(), limit));
        p.with_new_children(vec![limit])
    } else {
        let limit = Arc::new(GlobalLimitExec::new(input.clone(), 0, Some(limit)));
        let limit_optimized = LimitPushdown::new().optimize(limit, config)?;
        p.with_new_children(vec![limit_optimized])
    }
}

/// Because we disable `EnforceDistribution`, and because we add `SortPreservingMergeExec` in
/// `ensure_partition_merge_with_acceptable_parent` so that Sorted ("inplace") aggregates work
/// properly (which reduces memory usage), we in some cases have unnecessary
/// `SortPreservingMergeExec` nodes underneath a `Sort` node with a different ordering.  Or,
/// perhaps, we added a `GlobalLimitExec` by `add_limit_to_workers` and we can push down the limit
/// into a _matching_ `SortPreservingMergeExec` node.
///
/// A minor complication: There may be projection nodes in between that rename things.
pub fn replace_suboptimal_merge_sorts(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if let Some(sort) = p.as_any().downcast_ref::<SortExec>() {
        if sort.preserve_partitioning() {
            // Let's not handle this.
            return Ok(p);
        }
        let required_ordering = p
            .output_ordering()
            .cloned()
            .map(LexRequirement::from)
            .unwrap_or_default();
        let new_input =
            replace_suboptimal_merge_sorts_helper(&required_ordering, sort.fetch(), sort.input())?;
        p.with_new_children(vec![new_input])
    } else {
        Ok(p)
    }
}

/// Replaces SortPreservingMergeExec in the subtree with either a CoalescePartitions (if it doesn't
/// match the ordering) or, if it does match the sort ordering, pushes down fetch information if
/// appropriate.
fn replace_suboptimal_merge_sorts_helper(
    required_ordering: &LexRequirement,
    fetch: Option<usize>,
    node: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let node_any = node.as_any();
    if let Some(spm) = node_any.downcast_ref::<SortPreservingMergeExec>() {
        // A SortPreservingMergeExec that sort_exprs is a prefix of, is an acceptable ordering.  But
        // if there is no sort_exprs at all, we just use CoalescePartitions.
        if !required_ordering.is_empty() {
            let spm_req = LexRequirement::from(
                spm.properties()
                    .output_ordering()
                    .cloned()
                    .unwrap_or(LexOrdering::default()),
            );
            if !required_ordering.is_empty()
                && spm
                    .properties()
                    .eq_properties
                    .requirements_compatible(required_ordering, &spm_req)
            {
                // Okay, we have a matching SortPreservingMergeExec node!

                let mut new_fetch: Option<usize> = fetch;
                let new_spm = if let Some(fetch) = fetch {
                    if let Some(spm_fetch) = spm.fetch() {
                        if fetch < spm_fetch {
                            Arc::new(spm.clone().with_fetch(Some(fetch)))
                        } else {
                            // spm fetch is tighter.
                            new_fetch = Some(spm_fetch);
                            node.clone()
                        }
                    } else {
                        Arc::new(spm.clone().with_fetch(Some(fetch)))
                    }
                } else {
                    node.clone()
                };

                // Pass down spm's ordering, not sort_exprs, because we didn't touch spm besides the fetch..

                let new_input = replace_suboptimal_merge_sorts_helper(
                    &spm_req,
                    new_fetch,
                    new_spm
                        .children()
                        .first()
                        .ok_or(internal_datafusion_err!("no child"))?,
                )?;

                return new_spm.with_new_children(vec![new_input]);
            }
        }
        // sort_exprs is _not_ a prefix of spm.expr()
        // Aside: if spm.expr() is a prefix of sort_exprs, maybe SortExec could take advantage.

        // So it's not an acceptable ordering.  Create a CoalescePartitions, and remove other nested SortPreservingMergeExecs.
        let new_input = replace_suboptimal_merge_sorts_helper(
            &LexRequirement::new(vec![]),
            fetch,
            spm.input(),
        )?;

        return Ok(Arc::new(CoalescePartitionsExec::new(new_input)));
    } else if let Some(proj) = node_any.downcast_ref::<ProjectionExec>() {
        // TODO: Note that ProjectionExec has a TODO comment in DF's EnforceSorting optimizer (in sort_pushdown.rs).
        if let Some(new_sort_exprs) =
            sort_exprs_underneath_projection(required_ordering, proj.expr())?
        {
            let new_input =
                replace_suboptimal_merge_sorts_helper(&new_sort_exprs, fetch, proj.input())?;
            node.clone().with_new_children(vec![new_input])
        } else {
            Ok(node.clone())
        }
    } else if let Some(u) = node_any.downcast_ref::<UnionExec>() {
        let new_children: Result<Vec<_>, DataFusionError> = u
            .inputs()
            .iter()
            .map(|child| replace_suboptimal_merge_sorts_helper(required_ordering, fetch, child))
            .collect::<Result<Vec<_>, DataFusionError>>();
        let new_children = new_children?;
        Ok(Arc::new(UnionExec::new(new_children)))
    } else {
        Ok(node.clone())
    }
}

fn sort_exprs_underneath_projection(
    sort_exprs: &LexRequirement,
    proj_expr: &[(Arc<dyn PhysicalExpr>, String)],
) -> Result<Option<LexRequirement>, DataFusionError> {
    let mut sort_expr_columns = HashSet::<usize>::new();
    for expr in sort_exprs.iter() {
        record_columns_used(&mut sort_expr_columns, expr.expr.as_ref());
    }

    // sorted() just for determinism
    let sort_expr_columns: Vec<usize> = sort_expr_columns.into_iter().sorted().collect();
    let mut replacement_map =
        HashMap::<usize, datafusion::physical_plan::expressions::Column>::with_capacity(
            sort_expr_columns.len(),
        );

    for index in sort_expr_columns {
        let proj_lookup = proj_expr.get(index).ok_or_else(|| {
            DataFusionError::Internal(
                "proj_expr lookup in sort_exprs_underneath_projection failed".to_owned(),
            )
        })?;
        let Some(column_expr) = proj_lookup
            .0
            .as_any()
            .downcast_ref::<datafusion::physical_plan::expressions::Column>()
        else {
            return Ok(None);
        };
        replacement_map.insert(index, column_expr.clone());
    }

    // Now replace the columns in the sort_exprs with our different ones.
    let mut new_sort_exprs = Vec::with_capacity(sort_exprs.len());
    for e in sort_exprs.iter() {
        let transformed = replace_columns(&replacement_map, &e.expr)?;
        new_sort_exprs.push(PhysicalSortRequirement {
            expr: transformed,
            options: e.options,
        });
    }

    Ok(Some(LexRequirement::new(new_sort_exprs)))
}

fn record_columns_used(set: &mut HashSet<usize>, expr: &dyn PhysicalExpr) {
    if let Some(column) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_plan::expressions::Column>()
    {
        set.insert(column.index());
    } else {
        for child in expr.children() {
            record_columns_used(set, child.as_ref());
        }
    }
}

fn replace_columns(
    replacement_map: &HashMap<usize, datafusion::physical_plan::expressions::Column>,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    Ok(
        TreeNode::transform(expr.clone(), |node: Arc<dyn PhysicalExpr>| {
            if let Some(column) = node
                .as_any()
                .downcast_ref::<datafusion::physical_plan::expressions::Column>()
            {
                let replacement = replacement_map.get(&column.index()).ok_or_else(|| {
                    DataFusionError::Internal("replace_columns has bad replacement_map".to_owned())
                })?;
                Ok(Transformed::yes(Arc::new(replacement.clone())))
            } else {
                Ok(Transformed::no(node))
            }
        })?
        .data,
    )
}
