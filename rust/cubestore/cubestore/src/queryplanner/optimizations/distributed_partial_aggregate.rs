use crate::cluster::WorkerPlanningParams;
use crate::queryplanner::inline_aggregate::{InlineAggregateExec, InlineAggregateMode};
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
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, InputOrderMode, PhysicalExpr,
};
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

/// Transforms from:
///     AggregatePartial, Sorted
///     `- SortPreservingMerge
///        `- source(N partitions)
/// to:
///     SortPreservingMerge
///     `- AggregatePartial, Sorted (executed per partition)
///        `- source(N partitions)
///
/// The merge then carries one row per group per partition instead of all raw rows. Duplicate
/// group keys from different partitions are adjacent in the merged stream and get combined by
/// the Final aggregate, the same way partial states from different workers are.
///
/// Only sorted (streaming) partial aggregates are pushed: they hold O(1) accumulators per
/// partition, while a hash aggregate holds O(num_groups) and would multiply its memory usage by
/// the partition count.
pub fn push_sorted_partial_aggregate_below_merge(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let Some(agg) = p.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(p);
    };
    if *agg.mode() != AggregateMode::Partial
        || !matches!(agg.input_order_mode(), InputOrderMode::Sorted)
        // Restrict to aggregates convertible to InlineAggregateExec: `add_limit_to_workers`
        // relies on every merge-above-partial-aggregate pair being an InlineAggregateExec to
        // apply row limits without truncating duplicate group keys.
        || !agg.group_expr().is_single()
    {
        return Ok(p);
    }
    let Some(merge) = agg
        .input()
        .as_any()
        .downcast_ref::<SortPreservingMergeExec>()
    else {
        return Ok(p);
    };
    if merge.fetch().is_some() {
        return Ok(p);
    }
    let merge_input = merge.input();
    if merge_input.output_partitioning().partition_count() <= 1 {
        return Ok(p);
    }

    let new_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        agg.group_expr().clone(),
        agg.aggr_expr().to_vec(),
        agg.filter_expr().to_vec(),
        merge_input.clone(),
        agg.input_schema(),
    )?;
    // Per-partition input must still be sorted on the group keys, otherwise the aggregate
    // becomes hash-based and must stay above the merge.
    if !matches!(new_agg.input_order_mode(), InputOrderMode::Sorted) {
        return Ok(p);
    }
    let Some(ordering) = new_agg.properties().output_ordering().cloned() else {
        return Ok(p);
    };
    Ok(Arc::new(SortPreservingMergeExec::new(
        ordering,
        Arc::new(new_agg),
    )))
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

    // The merged per-partition partial aggregate stream may contain duplicate group keys from
    // different partitions, and a row limit above the merge could cut off part of some group's
    // partial states, silently corrupting that group's total. Apply the limit per partition
    // instead, below the merge: within a partition the aggregate emits unique group keys, so
    // `limit` rows there are `limit` complete groups, and the union of per-partition first
    // (last, for reverse) `limit` groups covers the global first (last) `limit` groups. Groups
    // beyond that may arrive with partial totals, but the router orders by the group key, not
    // by the totals, and its own limit drops them.
    if let Some(merge) = input.as_any().downcast_ref::<SortPreservingMergeExec>() {
        if let Some(agg) = merge.input().as_any().downcast_ref::<InlineAggregateExec>() {
            if *agg.mode() == InlineAggregateMode::Partial {
                let new_input: Arc<dyn ExecutionPlan> = if reverse {
                    // The last groups are unknown until a partition is exhausted, so the
                    // aggregate can't stop early; a per-partition tail keeps the merge input
                    // and the tail window at `limit` rows per partition.
                    let tail = Arc::new(TailLimitExec::new(merge.input().clone(), limit));
                    Arc::new(SortPreservingMergeExec::new(merge.expr().clone(), tail))
                } else {
                    let partitions = agg.properties().output_partitioning().partition_count();
                    let agg_limit = agg.limit().map_or(limit, |l| l.min(limit));
                    let new_agg = Arc::new(agg.with_limit(Some(agg_limit)));
                    Arc::new(
                        SortPreservingMergeExec::new(merge.expr().clone(), new_agg)
                            .with_fetch(Some(limit.saturating_mul(partitions))),
                    )
                };
                return p.with_new_children(vec![new_input]);
            }
        }
    }

    // A single-partition sorted partial aggregate emits one row per group, so a row limit is
    // exact; pass it into the aggregate so it can stop reading its input early.
    if !reverse {
        if let Some(agg) = input.as_any().downcast_ref::<InlineAggregateExec>() {
            if *agg.mode() == InlineAggregateMode::Partial
                && agg.properties().output_partitioning().partition_count() == 1
            {
                let agg_limit = agg.limit().map_or(limit, |l| l.min(limit));
                let new_agg = Arc::new(agg.with_limit(Some(agg_limit)));
                let limit_node = Arc::new(GlobalLimitExec::new(new_agg, 0, Some(limit)));
                return p.with_new_children(vec![limit_node]);
            }
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use std::collections::BTreeMap;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn make_batch(schema: &SchemaRef, rows: &[(i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            ],
        )
        .unwrap()
    }

    /// Memory source with each partition sorted by `k`.
    fn sorted_source(
        schema: &SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Arc<dyn ExecutionPlan> {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("k", schema).unwrap(),
        )]);
        let source = MemorySourceConfig::try_new(&partitions, schema.clone(), None)
            .unwrap()
            .try_with_sort_information(vec![ordering])
            .unwrap();
        Arc::new(DataSourceExec::new(Arc::new(source)))
    }

    fn merge_by_k(input: Arc<dyn ExecutionPlan>) -> Arc<SortPreservingMergeExec> {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("k", &input.schema()).unwrap(),
        )]);
        Arc::new(SortPreservingMergeExec::new(ordering, input))
    }

    fn sum_aggregate(
        mode: AggregateMode,
        group_col: &str,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let group_by = PhysicalGroupBy::new_single(vec![(
            col(group_col, &schema).unwrap(),
            group_col.to_string(),
        )]);
        let sum = AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema).unwrap()])
            .schema(schema.clone())
            .alias("sum_v")
            .build()
            .unwrap();
        Arc::new(
            AggregateExec::try_new(
                mode,
                group_by,
                vec![Arc::new(sum)],
                vec![None],
                input,
                schema,
            )
            .unwrap(),
        )
    }

    /// Collects plan output into per-key sums, combining duplicate keys the way a Final
    /// aggregate would.
    async fn collect_summed(plan: Arc<dyn ExecutionPlan>) -> BTreeMap<i64, i64> {
        let session = SessionContext::new();
        let batches = collect(plan, session.task_ctx()).await.unwrap();
        let mut result = BTreeMap::new();
        for batch in batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for (k, s) in keys.iter().zip(sums.iter()) {
                *result.entry(k.unwrap()).or_insert(0) += s.unwrap();
            }
        }
        result
    }

    fn two_partition_source(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        // Duplicate keys 2 and 3 across partitions
        sorted_source(
            schema,
            vec![
                vec![make_batch(schema, &[(1, 10), (2, 20), (3, 30)])],
                vec![make_batch(schema, &[(2, 21), (3, 31), (4, 40)])],
            ],
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pushes_sorted_partial_aggregate_below_merge() {
        let schema = test_schema();
        let source = two_partition_source(&schema);
        let original = sum_aggregate(AggregateMode::Partial, "k", merge_by_k(source));

        let rewritten = push_sorted_partial_aggregate_below_merge(original.clone()).unwrap();

        let merge = rewritten
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .expect("merge must become the root");
        let agg = merge
            .input()
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("partial aggregate must move below the merge");
        assert_eq!(*agg.mode(), AggregateMode::Partial);
        assert!(matches!(agg.input_order_mode(), InputOrderMode::Sorted));
        assert_eq!(
            agg.properties().output_partitioning().partition_count(),
            2,
            "aggregate must run per partition"
        );
        assert_eq!(rewritten.schema(), original.schema());

        // Cross-partition duplicate keys combine to the same totals as in the original plan
        assert_eq!(
            collect_summed(rewritten).await,
            collect_summed(original).await
        );
    }

    #[test]
    fn does_not_push_hash_aggregate() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("g", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![5, 4])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let source = sorted_source(&schema, vec![vec![batch.clone()], vec![batch]]);
        // Grouping by `g` while the input is sorted by `k` makes the aggregate hash-based
        let original = sum_aggregate(AggregateMode::Partial, "g", merge_by_k(source));
        assert!(matches!(
            original
                .as_any()
                .downcast_ref::<AggregateExec>()
                .unwrap()
                .input_order_mode(),
            InputOrderMode::Linear
        ));

        let rewritten = push_sorted_partial_aggregate_below_merge(original.clone()).unwrap();
        assert!(Arc::ptr_eq(&rewritten, &original));
    }

    #[test]
    fn does_not_push_non_partial_aggregate() {
        let schema = test_schema();
        let source = two_partition_source(&schema);
        let original = sum_aggregate(AggregateMode::Single, "k", merge_by_k(source));

        let rewritten = push_sorted_partial_aggregate_below_merge(original.clone()).unwrap();
        assert!(Arc::ptr_eq(&rewritten, &original));
    }

    #[test]
    fn does_not_push_below_merge_with_fetch() {
        let schema = test_schema();
        let source = two_partition_source(&schema);
        let merge = Arc::new(merge_by_k(source).as_ref().clone().with_fetch(Some(3)));
        let original = sum_aggregate(AggregateMode::Partial, "k", merge);

        let rewritten = push_sorted_partial_aggregate_below_merge(original.clone()).unwrap();
        assert!(Arc::ptr_eq(&rewritten, &original));
    }

    #[test]
    fn does_not_push_below_single_partition_merge() {
        let schema = test_schema();
        let source = sorted_source(
            &schema,
            vec![vec![make_batch(&schema, &[(1, 10), (2, 20)])]],
        );
        let original = sum_aggregate(AggregateMode::Partial, "k", merge_by_k(source));

        let rewritten = push_sorted_partial_aggregate_below_merge(original.clone()).unwrap();
        assert!(Arc::ptr_eq(&rewritten, &original));
    }

    fn inline(plan: Arc<dyn ExecutionPlan>) -> Arc<InlineAggregateExec> {
        let agg = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
        Arc::new(InlineAggregateExec::try_new_from_aggregate(agg).unwrap())
    }

    fn worker(
        input: Arc<dyn ExecutionPlan>,
        limit_and_reverse: Option<(usize, bool)>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(WorkerExec::new(
            input,
            4096,
            limit_and_reverse,
            None,
            WorkerPlanningParams {
                worker_partition_count: 1,
            },
        ))
    }

    /// Worker plan with the partial aggregate below the merge: merge of per-partition partial
    /// states can contain duplicate group keys, so the row limit must be limit * partitions
    /// while the aggregates take the group limit.
    #[test]
    fn worker_limit_above_merged_partial_aggregate_limits_groups_per_partition() {
        let schema = test_schema();
        let source = two_partition_source(&schema);
        let agg = inline(sum_aggregate(AggregateMode::Partial, "k", source));
        let merged = push_sorted_partial_aggregate_below_merge_shape(agg);
        let p = worker(merged, Some((3, false)));

        let rewritten = add_limit_to_workers(p, &ConfigOptions::default()).unwrap();

        let worker = rewritten.as_any().downcast_ref::<WorkerExec>().unwrap();
        let merge = worker
            .input
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .expect("merge must stay on top of per-partition aggregates");
        assert_eq!(
            merge.fetch(),
            Some(6),
            "row budget must be limit * partitions"
        );
        let agg = merge
            .input()
            .as_any()
            .downcast_ref::<InlineAggregateExec>()
            .unwrap();
        assert_eq!(agg.limit(), Some(3));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn worker_reverse_limit_above_merged_partial_aggregate_tails_each_partition() {
        let schema = test_schema();
        // Keys 4..=6 are present in both partitions; per-partition tails of 3 groups are
        // {4, 5, 6} and {7, 8, 9}.
        let source = sorted_source(
            &schema,
            vec![
                vec![make_batch(
                    &schema,
                    &(1..=6).map(|k| (k, k * 10)).collect::<Vec<_>>(),
                )],
                vec![make_batch(
                    &schema,
                    &(4..=9).map(|k| (k, k * 100 + 1)).collect::<Vec<_>>(),
                )],
            ],
        );
        let baseline = collect_summed(sum_aggregate(
            AggregateMode::Partial,
            "k",
            merge_by_k(source.clone()),
        ))
        .await;

        let agg = inline(sum_aggregate(AggregateMode::Partial, "k", source));
        let merged = push_sorted_partial_aggregate_below_merge_shape(agg);
        let p = worker(merged, Some((3, true)));

        let rewritten = add_limit_to_workers(p, &ConfigOptions::default()).unwrap();

        let worker = rewritten.as_any().downcast_ref::<WorkerExec>().unwrap();
        let merge = worker
            .input
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .expect("merge must stay on top of per-partition tails");
        assert_eq!(merge.fetch(), None);
        let tail = merge
            .input()
            .as_any()
            .downcast_ref::<TailLimitExec>()
            .expect("reverse limit must become a per-partition tail below the merge");
        assert_eq!(tail.limit, 3);
        let agg = tail
            .input
            .as_any()
            .downcast_ref::<InlineAggregateExec>()
            .unwrap();
        assert_eq!(
            agg.limit(),
            None,
            "tail limit can not stop the aggregate early"
        );

        // The merged stream must carry complete totals for the last 3 group keys; earlier keys
        // may arrive partial and are dropped by the router's own limit.
        let summed = collect_summed(worker.input.clone()).await;
        for key in [7, 8, 9] {
            assert_eq!(summed[&key], baseline[&key], "complete total for key {key}");
        }
    }

    /// Single partition partial aggregate emits unique group keys, the row limit stays exact
    /// and also lets the aggregate stop early.
    #[test]
    fn worker_limit_above_single_partition_partial_aggregate_sets_aggregate_limit() {
        let schema = test_schema();
        let source = sorted_source(
            &schema,
            vec![vec![make_batch(&schema, &[(1, 10), (2, 20)])]],
        );
        let agg = inline(sum_aggregate(AggregateMode::Partial, "k", source));
        let p = worker(agg, Some((3, false)));

        let rewritten = add_limit_to_workers(p, &ConfigOptions::default()).unwrap();

        let worker = rewritten.as_any().downcast_ref::<WorkerExec>().unwrap();
        let limit = worker
            .input
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .unwrap();
        assert_eq!(limit.fetch(), Some(3));
        let agg = limit
            .input()
            .as_any()
            .downcast_ref::<InlineAggregateExec>()
            .unwrap();
        assert_eq!(agg.limit(), Some(3));
    }

    /// Builds the post-rewrite shape merge-above-aggregate for an already converted
    /// InlineAggregateExec.
    fn push_sorted_partial_aggregate_below_merge_shape(
        agg: Arc<InlineAggregateExec>,
    ) -> Arc<dyn ExecutionPlan> {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("k", &agg.schema()).unwrap(),
        )]);
        Arc::new(SortPreservingMergeExec::new(ordering, agg))
    }
}
