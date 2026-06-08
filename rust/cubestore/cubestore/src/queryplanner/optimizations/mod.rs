mod check_memory;
mod distributed_partial_aggregate;
mod inline_aggregate_rewriter;
pub mod is_not_distinct_from_join_keys;
pub mod rewrite_plan;
pub mod rolling_optimizer;
mod trace_data_loaded;

use super::serialized_plan::PreSerializedPlan;
use crate::cluster::{Cluster, WorkerPlanningParams};
use crate::queryplanner::optimizations::distributed_partial_aggregate::{
    add_limit_to_workers, ensure_partition_merge, push_aggregate_to_workers,
    replace_suboptimal_merge_sorts,
};
use crate::queryplanner::optimizations::inline_aggregate_rewriter::replace_with_inline_aggregate;
use crate::queryplanner::check_memory::CheckMemoryExec;
use crate::queryplanner::inline_aggregate::{InlineAggregateExec, InlineAggregateMode};
use crate::queryplanner::planning::CubeExtensionPlanner;
use crate::queryplanner::pretty_printers::{pp_phys_plan_ext, PPOptions};
use crate::queryplanner::query_executor::ClusterSendExec;
use crate::queryplanner::rolling::RollingWindowPlanner;
use crate::queryplanner::trace_data_loaded::DataLoadedSize;
use crate::util::memory::MemoryHandler;
use async_trait::async_trait;
use check_memory::add_check_memory_exec;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use distributed_partial_aggregate::ensure_partition_merge_with_acceptable_parent;
use rewrite_plan::rewrite_physical_plan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use trace_data_loaded::add_trace_data_loaded_exec;

pub struct CubeQueryPlanner {
    /// Set on the router
    cluster: Option<Arc<dyn Cluster>>,
    /// Set on the worker
    worker_partition_count: Option<WorkerPlanningParams>,
    serialized_plan: Arc<PreSerializedPlan>,
    memory_handler: Arc<dyn MemoryHandler>,
    data_loaded_size: Option<Arc<DataLoadedSize>>,
}

impl CubeQueryPlanner {
    pub fn new_on_router(
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<PreSerializedPlan>,
        memory_handler: Arc<dyn MemoryHandler>,
    ) -> CubeQueryPlanner {
        CubeQueryPlanner {
            cluster: Some(cluster),
            worker_partition_count: None,
            serialized_plan,
            memory_handler,
            data_loaded_size: None,
        }
    }

    pub fn new_on_worker(
        serialized_plan: Arc<PreSerializedPlan>,
        worker_planning_params: WorkerPlanningParams,
        memory_handler: Arc<dyn MemoryHandler>,
        data_loaded_size: Option<Arc<DataLoadedSize>>,
    ) -> CubeQueryPlanner {
        CubeQueryPlanner {
            serialized_plan,
            cluster: None,
            worker_partition_count: Some(worker_planning_params),
            memory_handler,
            data_loaded_size,
        }
    }
}

impl Debug for CubeQueryPlanner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CubeQueryPlanner")
    }
}

#[async_trait]
impl QueryPlanner for CubeQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let p = DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(CubeExtensionPlanner {
                cluster: self.cluster.clone(),
                worker_planning_params: self.worker_partition_count,
                serialized_plan: self.serialized_plan.clone(),
            }),
            Arc::new(RollingWindowPlanner {}),
        ])
        .create_physical_plan(logical_plan, ctx_state)
        .await?;
        let result = finalize_physical_plan(
            p,
            self.memory_handler.clone(),
            self.data_loaded_size.clone(),
            ctx_state.config().options(),
        );
        result
    }
}

#[derive(Debug)]
pub struct PreOptimizeRule {}

impl PreOptimizeRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for PreOptimizeRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        pre_optimize_physical_plan(plan)
    }

    fn name(&self) -> &str {
        "PreOptimizeRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn pre_optimize_physical_plan(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p = rewrite_physical_plan(p, &mut |p| push_aggregate_to_workers(p))?;

    // Handles non-root-node cases
    let p = rewrite_physical_plan(p, &mut |p| ensure_partition_merge_with_acceptable_parent(p))?;
    // Handles the root node case
    let p = ensure_partition_merge(p)?;

    // Replace sorted AggregateExec with InlineAggregateExec for better performance
    let p = rewrite_physical_plan(p, &mut |p| replace_with_inline_aggregate(p))?;

    // Apply worker_sort_and_limit AFTER aggregate restructuring, so the SortExec
    // wraps the aggregate output (not the raw scan input).
    let p = rewrite_physical_plan(p, &mut |p| apply_worker_sort_and_limit(p))?;

    Ok(p)
}

// These really could just be physical plan optimizers appended to the DF list.
fn finalize_physical_plan(
    p: Arc<dyn ExecutionPlan>,
    memory_handler: Arc<dyn MemoryHandler>,
    data_loaded_size: Option<Arc<DataLoadedSize>>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let p = rewrite_physical_plan(p, &mut |p| add_check_memory_exec(p, memory_handler.clone()))?;
    log::trace!(
        "Rewrote physical plan by add_check_memory_exec:\n{}",
        pp_phys_plan_ext(p.as_ref(), &PPOptions::show_nonmeta())
    );
    let p = if let Some(data_loaded_size) = data_loaded_size {
        rewrite_physical_plan(p, &mut |p| add_trace_data_loaded_exec(p, &data_loaded_size))?
    } else {
        p
    };
    log::trace!(
        "Rewrote physical plan by add_trace_data_loaded_exec:\n{}",
        pp_phys_plan_ext(p.as_ref(), &PPOptions::show_nonmeta())
    );
    let p = rewrite_physical_plan(p, &mut |p| add_limit_to_workers(p, config))?;
    log::trace!(
        "Rewrote physical plan by add_limit_to_workers:\n{}",
        pp_phys_plan_ext(p.as_ref(), &PPOptions::show_nonmeta())
    );
    let p = push_sort_to_workers(p)?;
    log::trace!(
        "Rewrote physical plan by push_sort_to_workers:\n{}",
        pp_phys_plan_ext(p.as_ref(), &PPOptions::show_nonmeta())
    );
    let p = rewrite_physical_plan(p, &mut |p| replace_suboptimal_merge_sorts(p))?;
    log::trace!(
        "Rewrote physical plan by replace_suboptimal_merge_sorts:\n{}",
        pp_phys_plan_ext(p.as_ref(), &PPOptions::show_nonmeta())
    );
    Ok(p)
}

/// When the router plan has `SortExec(fetch=N)` sorting by GROUP BY columns,
/// and the worker uses `InlineAggregateExec` (streaming aggregate where groups don't overlap),
/// push a matching `SortExec(fetch=N)` to the worker. DataFusion's SortExec with fetch uses
/// a bounded heap, so this limits worker output to N rows with O(N) memory.
fn push_sort_to_workers(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let Some(sort) = p.as_any().downcast_ref::<SortExec>() else {
        return Ok(p);
    };
    let Some(fetch) = sort.fetch() else {
        return Ok(p);
    };

    let sort_exprs = sort.expr().clone();

    // Walk down through Projection → InlineAggregate(Final) → ClusterSend
    let sort_input = sort.input();
    let (below_proj, col_mapping) = if let Some(proj) = sort_input.as_any().downcast_ref::<ProjectionExec>() {
        // Map sort column indices through the projection
        let mapping: Vec<Option<usize>> = proj.expr().iter().map(|(expr, _name)| {
            expr.as_any()
                .downcast_ref::<datafusion::physical_plan::expressions::Column>()
                .map(|c| c.index())
        }).collect();
        (proj.input().clone(), Some(mapping))
    } else {
        (sort_input.clone(), None)
    };

    let Some(inline_final) = below_proj.as_any().downcast_ref::<InlineAggregateExec>() else {
        return Ok(p);
    };
    if *inline_final.mode() != InlineAggregateMode::Final {
        return Ok(p);
    }
    let group_key_len = inline_final.group_expr().expr().len();

    // Translate sort expressions to pre-projection column indices
    let translated_sort_exprs: Vec<_> = sort_exprs.iter().map(|se| {
        if let Some(col) = se.expr.as_any().downcast_ref::<datafusion::physical_plan::expressions::Column>() {
            let actual_idx = if let Some(ref mapping) = col_mapping {
                mapping.get(col.index()).copied().flatten()
            } else {
                Some(col.index())
            };
            actual_idx.filter(|&idx| idx < group_key_len)
        } else {
            None
        }
    }).collect();

    // All sort columns must be GROUP BY columns
    if translated_sort_exprs.iter().any(|x| x.is_none()) {
        return Ok(p);
    }

    // Find ClusterSendExec below InlineAggregate(Final), possibly through CheckMemoryExec
    let final_input = inline_final.input();
    let (cluster_send, through_check_memory) =
        if let Some(cs) = final_input.as_any().downcast_ref::<ClusterSendExec>() {
            (cs, false)
        } else if let Some(cm) = final_input.as_any().downcast_ref::<CheckMemoryExec>() {
            if let Some(cs) = cm.input.as_any().downcast_ref::<ClusterSendExec>() {
                (cs, true)
            } else {
                return Ok(p);
            }
        } else {
            return Ok(p);
        };

    // Don't override if limit_and_reverse is already set
    if cluster_send.limit_and_reverse.is_some() {
        return Ok(p);
    }

    let worker_input = &cluster_send.input_for_optimizations;

    // Verify the worker has InlineAggregateExec(Partial) - confirms groups don't overlap
    let has_inline_partial = worker_input
        .as_any()
        .downcast_ref::<InlineAggregateExec>()
        .map_or(false, |ia| *ia.mode() == InlineAggregateMode::Partial);
    if !has_inline_partial {
        return Ok(p);
    }

    // Build sort expressions for the worker (same column indices, same options)
    let worker_schema = worker_input.schema();
    let worker_sort_exprs: Vec<_> = sort_exprs.iter().zip(translated_sort_exprs.iter()).map(|(se, &mapped_idx)| {
        let idx = mapped_idx.unwrap();
        datafusion::physical_expr::PhysicalSortExpr {
            expr: Arc::new(datafusion::physical_plan::expressions::Column::new(
                worker_schema.field(idx).name(),
                idx,
            )),
            options: se.options,
        }
    }).collect();

    // Wrap the worker plan: SortExec(fetch=N) → InlinePartialAggregate
    let new_worker_input: Arc<dyn ExecutionPlan> = Arc::new(
        SortExec::new(LexOrdering::new(worker_sort_exprs), worker_input.clone())
            .with_fetch(Some(fetch)),
    );

    // Rebuild ClusterSendExec with the new worker input
    let new_cluster_send: Arc<dyn ExecutionPlan> = Arc::new(
        cluster_send.with_changed_schema(new_worker_input, cluster_send.required_input_ordering.clone()),
    );

    // Re-wrap with CheckMemoryExec if it was present
    let new_final_child: Arc<dyn ExecutionPlan> = if through_check_memory {
        final_input.clone().with_new_children(vec![new_cluster_send])?
    } else {
        new_cluster_send
    };

    // Rebuild InlineAggregate(Final) with new child
    let new_inline_final: Arc<dyn ExecutionPlan> =
        Arc::clone(&below_proj).with_new_children(vec![new_final_child])?;

    // Rebuild Projection if present
    let new_sort_input = if sort_input.as_any().downcast_ref::<ProjectionExec>().is_some() {
        sort_input.clone().with_new_children(vec![new_inline_final])?
    } else {
        new_inline_final
    };

    // Rebuild SortExec with the new subtree
    p.with_new_children(vec![new_sort_input])
}

/// Apply worker_sort_and_limit on a WorkerExec by wrapping its child
/// (the partial aggregate) with SortExec(fetch=N). Must run AFTER
/// push_aggregate_to_workers and replace_with_inline_aggregate.
fn apply_worker_sort_and_limit(
    p: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    use crate::queryplanner::planning::WorkerExec;

    let Some(w) = p.as_any().downcast_ref::<WorkerExec>() else {
        return Ok(p);
    };
    let Some((sort_cols, limit)) = w.worker_sort_and_limit.as_ref() else {
        return Ok(p);
    };

    let input = &w.input;
    let schema = input.schema();
    let sort_exprs: Vec<_> = sort_cols
        .iter()
        .map(|(col_idx, asc, nulls_first)| {
            datafusion::physical_expr::PhysicalSortExpr {
                expr: Arc::new(
                    datafusion::physical_plan::expressions::Column::new(
                        schema.field(*col_idx).name(),
                        *col_idx,
                    ),
                ),
                options: datafusion::arrow::compute::SortOptions {
                    descending: !asc,
                    nulls_first: *nulls_first,
                },
            }
        })
        .collect();
    let sort_exec: Arc<dyn ExecutionPlan> = Arc::new(
        SortExec::new(LexOrdering::new(sort_exprs), input.clone())
            .with_fetch(Some(*limit)),
    );
    p.with_new_children(vec![sort_exec])
}
