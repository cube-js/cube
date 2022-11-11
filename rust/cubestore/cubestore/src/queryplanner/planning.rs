//! Query planning goes through the following stages:
//!   1. Logical plan produced by DataFusion. Contains table scans of [CubeTableLogical], does not
//!      know which physical nodes it has to query.
//!   2. [choose_index] transformation will replace the index and particular partitions to query.
//!      It will also place [ClusterSendNode] into the correct place.
//!      At this point, the logical plan is finalized, it only scans [CubeTable]s and contains
//!      enough information to distribute the plan into workers.
//!   3. We serialize the resulting logical plan into [SerializedPlan] and send it to workers.
//!   4. [CubeQueryPlanner] is used on both the router and the workers to produce a physical plan.
//!      Note that workers and the router produce different plans:
//!          - Router produces a physical plan that handles the "top" part of the logical plan, above
//!            the cluster send.
//!          - Workers take only the "bottom" part part of the logical plan, below the cluster send.
//!            In addition, workers will replace all table scans of data they do not have with empty
//!            results.
//!
//!       At this point we also optimize the physical plan to ensure we do as much work as possible
//!       on the workers, see [CubeQueryPlanner] for details.
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{Field, SchemaRef};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionContextState;
use datafusion::logical_plan::{DFSchemaRef, Expr, LogicalPlan, Operator, UserDefinedLogicalNode};
use datafusion::physical_plan::aggregates::AggregateFunction as FusionAggregateFunction;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{
    ExecutionPlan, OptimizerHints, Partitioning, PhysicalPlanner, SendableRecordBatchStream,
};
use flatbuffers::bitflags::_core::any::Any;
use flatbuffers::bitflags::_core::fmt::Formatter;
use itertools::{Itertools, EitherOrBoth};

use crate::cluster::Cluster;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::table::{Table, TablePath};
use crate::metastore::{
    AggregateFunction, Chunk, Column, IdRow, Index, IndexType, MetaStore, Partition, Schema,
};
use crate::queryplanner::optimizations::rewrite_plan::{rewrite_plan, PlanRewriter};
use crate::queryplanner::panic::{plan_panic_worker, PanicWorkerNode};
use crate::queryplanner::partition_filter::PartitionFilter;
use crate::queryplanner::query_executor::{ClusterSendExec, CubeTable, InlineTableProvider};
use crate::queryplanner::serialized_plan::{
    IndexSnapshot, InlineSnapshot, PartitionSnapshot, SerializedPlan,
};
use crate::queryplanner::topk::{materialize_topk, plan_topk, ClusterAggregateTopK};
use crate::queryplanner::CubeTableLogical;
use crate::CubeError;
use crate::table::{Row, cmp_same_types};
use datafusion::logical_plan;
use datafusion::optimizer::utils::expr_to_columns;
use datafusion::physical_plan::parquet::NoopParquetMetadataCache;
use serde::{Deserialize as SerdeDeser, Deserializer, Serialize as SerdeSer, Serializer};
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::iter::FromIterator;
use std::cmp::Ordering;

#[cfg(test)]
pub async fn choose_index(
    p: &LogicalPlan,
    metastore: &dyn PlanIndexStore,
) -> Result<(LogicalPlan, PlanningMeta), DataFusionError> {
    choose_index_ext(p, metastore, true).await
}

/// Information required to distribute the logical plan into multiple workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanningMeta {
    pub indices: Vec<IndexSnapshot>,
    /// Non-empty only if indices point to multi-partitions.
    /// Custom serde handlers as flatbuffers can't handle hash maps with integer keys.
    #[serde(deserialize_with = "de_vec_as_map")]
    #[serde(serialize_with = "se_vec_as_map")]
    pub multi_part_subtree: HashMap<u64, MultiPartition>,
}

fn se_vec_as_map<S: Serializer>(m: &HashMap<u64, MultiPartition>, s: S) -> Result<S::Ok, S::Error> {
    m.iter().collect_vec().serialize(s)
}

fn de_vec_as_map<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<HashMap<u64, MultiPartition>, D::Error> {
    Vec::<(u64, MultiPartition)>::deserialize(d).map(HashMap::from_iter)
}

pub async fn choose_index_ext(
    p: &LogicalPlan,
    metastore: &dyn PlanIndexStore,
    enable_topk: bool,
) -> Result<(LogicalPlan, PlanningMeta), DataFusionError> {
    // Prepare information to choose the index.
    let mut collector = CollectConstraints::default();
    rewrite_plan(p, &ConstraintsContext::default(), &mut collector)?;

    // Consult metastore to choose the index.
    // TODO should be single snapshot read to ensure read consistency here
    let tables = metastore
        .get_tables_with_indexes(
            collector
                .constraints
                .iter()
                .map(|c| {
                    // TODO: use ids.
                    let schema = c.table.schema.get_row().get_name().clone();
                    let table = c.table.table.get_row().get_table_name().clone();
                    (schema, table)
                })
                .collect_vec(),
        )
        .await?;
    assert_eq!(tables.len(), collector.constraints.len());
    let mut candidates = Vec::new();
    for (c, inputs) in collector.constraints.iter().zip(tables) {
        candidates.push(pick_index(c, inputs.0, inputs.1, inputs.2).await?)
    }

    // We pick partitioned index only when all tables request the same one.
    let mut indices: Vec<_> = match all_have_same_partitioned_index(&candidates) {
        true => candidates
            .into_iter()
            .map(|c| c.partitioned_index.unwrap())
            .collect(),
        // We sometimes propagate 'index for join not found' error here.
        false => candidates
            .into_iter()
            .map(|c| c.ordinary_index)
            .collect::<Result<_, DataFusionError>>()?,
    };

    // TODO should be single snapshot read to ensure read consistency here
    let partitions = metastore
        .get_active_partitions_and_chunks_by_index_id_for_select(
            indices.iter().map(|i| i.index.get_id()).collect_vec(),
        )
        .await?;

    assert_eq!(partitions.len(), indices.len());
    for ((i, c), ps) in indices
        .iter_mut()
        .zip(collector.constraints.iter())
        .zip(partitions)
    {
        i.partitions = pick_partitions(i, c, ps)?;
    }
    
    let pushdown_limit = can_pushdown_limit(&indices);

    // We have enough information to finalize the logical plan.
    let mut r = ChooseIndex {
        chosen_indices: &indices,
        next_index: 0,
        enable_topk,
        pushdown_limit,
        limit: None
    };
    let plan = rewrite_plan(p, &(), &mut r)?;
    assert_eq!(r.next_index, indices.len());

    let plan = if pushdown_limit {

        let mut r = PushDownLimit{};
        rewrite_plan(&plan, &None, &mut r)?
    } else {
        plan
    };

    let mut multi_parts = Vec::new();
    for i in &indices {
        for p in &i.partitions {
            if let Some(m) = p.partition.get_row().multi_partition_id() {
                multi_parts.push(m);
            }
        }
    }

    // TODO should be single snapshot read to ensure read consistency here
    let multi_part_subtree = metastore.get_multi_partition_subtree(multi_parts).await?;
    Ok((
        plan,
        PlanningMeta {
            indices,
            multi_part_subtree,
        },
    ))
}

fn can_pushdown_limit(indices: &Vec<IndexSnapshot>) -> bool {
    if indices.is_empty() {
        return false;
    }
    if indices[0].sort_on().is_none() || indices[0].sort_on().unwrap().is_empty() {
        return false;
    }

    let sort_on = indices[0].sort_on().unwrap();
    
    if indices.iter().any(|i| i.sort_on().map_or(true, |s| s != sort_on)) {
        return false;
    }

    let sort_on_len = sort_on.len();
    
    let mut inds_min_max = indices.iter().map(|i| index_min_max(i)).collect::<Vec<_>>();

    inds_min_max.sort_by(|a, b| cmp_row(a.0, b.0, sort_on_len, true));

    for item in inds_min_max.iter().zip_longest(inds_min_max.iter().skip(1)) {

        match item {

            EitherOrBoth::Both(left_part, right_part) => {
                match cmp_row(left_part.1, right_part.0, sort_on_len, true) {
                    Ordering::Greater => return false,
                    _ => {}
                }
            }
            _ => {}
        }
    }
    true

}



fn index_min_max(ind: &IndexSnapshot) -> (Option<&Row>, Option<&Row>) {
    let sort_on_len = ind.sort_on().unwrap().len();

    let mut partitions = ind.partitions.iter().filter(|p| p.partition().get_row().is_active()).collect::<Vec<_>>();

    if partitions.is_empty() {
        return (None, None);
    }

    partitions.sort_by(|a, b| {
        let a_row = a.partition().get_row();
        let b_row = b.partition().get_row();
        let a_val = a_row.get_min_or_lower_bound();
        let b_val = b_row.get_max_or_upper_bound();
        cmp_row(a_val, b_val, sort_on_len, true)
    });

    let min_candidates = partitions.first().unwrap()
        .chunks
        .iter()
        .map(|c| c.get_row().min().as_ref());
    let part_min = partitions.first().unwrap().partition().get_row().get_min_or_lower_bound();

    let min = if part_min.is_some() {
        min_candidates.chain(std::iter::once(part_min)).min_by(|a, b| cmp_row(*a, *b, sort_on_len, true))
    } else {
        min_candidates.min_by(|a, b| cmp_row(*a, *b, sort_on_len, true))
    };

    let max_candidates = partitions.last().unwrap()
        .chunks
        .iter()
        .map(|c| c.get_row().max().as_ref());

    let part_max = partitions.first().unwrap().partition().get_row().get_max_or_upper_bound();

    let max = if part_max.is_some() {
        max_candidates.chain(std::iter::once(part_max)).max_by(|a, b| cmp_row(*a, *b, sort_on_len, false))
    } else {
        max_candidates.max_by(|a, b| cmp_row(*a, *b, sort_on_len, false))
    };

    if min.is_none() || max.is_none() {
        return (None, None);
    }

    (min.unwrap(), max.unwrap())
}

fn cmp_row(l:Option<&Row>, r:Option<&Row>, len:usize, none_first: bool) -> Ordering {

    match (l, r) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => if none_first { Ordering::Less } else { Ordering::Greater },
        (Some(_), None) => if !none_first { Ordering::Greater } else { Ordering::Less },
        (Some(l), Some(r)) => l.values()
            .iter()
            .take(len)
            .zip(r.values().iter().take(len))
            .find_map(|(a, b)| {
                match cmp_same_types(a, b) {
                    Ordering::Less => Some(Ordering::Less),
                    Ordering::Greater => Some(Ordering::Greater),
                    Ordering::Equal => None
                }
            }
            ).unwrap_or(Ordering::Equal)
    }
}


fn all_have_same_partitioned_index(cs: &[IndexCandidate]) -> bool {
    if cs.is_empty() {
        return true;
    }
    let multi_index_id = |c: &IndexCandidate| {
        c.partitioned_index
            .as_ref()
            .and_then(|i| i.index.get_row().multi_index_id())
    };
    let id = match multi_index_id(&cs[0]) {
        Some(id) => id,
        None => return false,
    };
    for c in &cs[1..] {
        if multi_index_id(c) != Some(id) {
            return false;
        }
    }
    return true;
}

#[async_trait]
pub trait PlanIndexStore: Send + Sync {
    async fn get_tables_with_indexes(
        &self,
        inputs: Vec<(String, String)>,
    ) -> Result<Vec<(IdRow<Schema>, IdRow<Table>, Vec<IdRow<Index>>)>, CubeError>;
    async fn get_active_partitions_and_chunks_by_index_id_for_select(
        &self,
        index_id: Vec<u64>,
    ) -> Result<Vec<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>>, CubeError>;
    async fn get_multi_partition_subtree(
        &self,
        multi_part_ids: Vec<u64>,
    ) -> Result<HashMap<u64, MultiPartition>, CubeError>;
}

#[async_trait]
impl<'a> PlanIndexStore for &'a dyn MetaStore {
    async fn get_tables_with_indexes(
        &self,
        inputs: Vec<(String, String)>,
    ) -> Result<Vec<(IdRow<Schema>, IdRow<Table>, Vec<IdRow<Index>>)>, CubeError> {
        MetaStore::get_tables_with_indexes(*self, inputs).await
    }

    async fn get_active_partitions_and_chunks_by_index_id_for_select(
        &self,
        index_id: Vec<u64>,
    ) -> Result<Vec<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>>, CubeError> {
        MetaStore::get_active_partitions_and_chunks_by_index_id_for_select(*self, index_id).await
    }

    async fn get_multi_partition_subtree(
        &self,
        multi_part_ids: Vec<u64>,
    ) -> Result<HashMap<u64, MultiPartition>, CubeError> {
        MetaStore::get_multi_partition_subtree(*self, multi_part_ids).await
    }
}

#[derive(Clone)]
struct SortColumns {
    sort_on: Vec<String>,
    required: bool,
}

struct IndexConstraints {
    sort_on: Option<SortColumns>,
    table: TablePath,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    aggregates: Vec<Expr>,
}

#[derive(Default)]
struct CollectConstraints {
    constraints: Vec<IndexConstraints>,
}

#[derive(Default, Clone)]
struct ConstraintsContext {
    sort_on: Option<SortColumns>,
    aggregates: Vec<Expr>,
}

impl ConstraintsContext {
    pub fn update_sort_on(&self, sort_on: Option<SortColumns>) -> Self {
        Self {
            sort_on,
            aggregates: self.aggregates.clone(),
        }
    }
}

impl PlanRewriter for CollectConstraints {
    type Context = ConstraintsContext;

    fn rewrite(
        &mut self,
        n: LogicalPlan,
        c: &Self::Context,
    ) -> Result<LogicalPlan, DataFusionError> {
        match &n {
            LogicalPlan::TableScan {
                projection,
                filters,
                source,
                ..
            } => {
                if let Some(table) = source.as_any().downcast_ref::<CubeTableLogical>() {
                    self.constraints.push(IndexConstraints {
                        sort_on: c.sort_on.clone(),
                        table: table.table.clone(),
                        projection: projection.clone(),
                        filters: filters.clone(),
                        aggregates: c.aggregates.clone(),
                    })
                };
            }
            _ => {}
        }
        Ok(n)
    }

    fn enter_node(
        &mut self,
        n: &LogicalPlan,
        current_context: &Self::Context,
    ) -> Option<Self::Context> {
        fn column_name(expr: &Expr) -> Option<String> {
            match expr {
                Expr::Alias(e, _) => column_name(e),
                Expr::Column(col) => Some(col.name.clone()), // TODO use alias
                _ => None,
            }
        }
        match n {
            LogicalPlan::Aggregate {
                group_expr,
                aggr_expr,
                ..
            } => {
                let sort_on = group_expr.iter().map(column_name).collect::<Vec<_>>();
                let sort_on = if !sort_on.is_empty() && sort_on.iter().all(|c| c.is_some()) {
                    Some(SortColumns {
                        sort_on: sort_on.into_iter().map(|c| c.unwrap()).collect(),
                        required: false,
                    })
                } else {
                    None
                };
                Some(ConstraintsContext {
                    sort_on,
                    aggregates: aggr_expr.to_vec(),
                })
            }
            LogicalPlan::Filter { predicate, .. } => {
                let mut sort_on = Vec::new();
                if single_value_filter_columns(predicate, &mut sort_on) {
                    if !sort_on.is_empty() {
                        let sort_on = Some(SortColumns {
                            sort_on: sort_on
                                .into_iter()
                                .map(|c| c.name.to_string())
                                .chain(
                                    current_context
                                        .sort_on
                                        .as_ref()
                                        .map(|c| c.sort_on.clone())
                                        .unwrap_or_else(|| Vec::new())
                                        .into_iter(),
                                )
                                .unique()
                                .collect(),
                            required: false,
                        });
                        Some(current_context.update_sort_on(sort_on))
                    } else {
                        Some(current_context.clone())
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn enter_join_left(&mut self, join: &LogicalPlan, _: &Self::Context) -> Option<Self::Context> {
        let join_on;
        if let LogicalPlan::Join { on, .. } = join {
            join_on = on;
        } else {
            panic!("expected join node");
        }
        Some(ConstraintsContext {
            sort_on: Some(SortColumns {
                sort_on: join_on.iter().map(|(l, _)| l.name.clone()).collect(),
                required: true,
            }),
            aggregates: Vec::new(),
        })
    }

    fn enter_join_right(
        &mut self,
        join: &LogicalPlan,
        _c: &Self::Context,
    ) -> Option<Self::Context> {
        let join_on;
        if let LogicalPlan::Join { on, .. } = join {
            join_on = on;
        } else {
            panic!("expected join node");
        }
        Some(ConstraintsContext {
            sort_on: Some(SortColumns {
                sort_on: join_on.iter().map(|(_, r)| r.name.clone()).collect(),
                required: true,
            }),
            aggregates: Vec::new(),
        })
    }
}

fn single_value_filter_columns<'a>(
    expr: &'a Expr,
    columns: &mut Vec<&'a logical_plan::Column>,
) -> bool {
    match expr {
        Expr::Column(c) => {
            columns.push(c);
            true
        }
        Expr::Literal(_) => true,
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => {
                single_value_filter_columns(left, columns)
                    && single_value_filter_columns(right, columns)
            }
            Operator::And => {
                let mut l_part = Vec::new();
                let l_res = single_value_filter_columns(left, &mut l_part);

                if l_res {
                    columns.append(&mut l_part);
                }

                let mut r_part = Vec::new();
                let r_res = single_value_filter_columns(right, &mut r_part);

                if r_res {
                    columns.append(&mut r_part);
                }
                l_res || r_res
            }
            _ => false,
        },
        _ => false,
    }
}

struct ChooseIndex<'a> {
    next_index: usize,
    chosen_indices: &'a [IndexSnapshot],
    enable_topk: bool,
    pushdown_limit: bool,
    limit: Option<usize>
}

impl PlanRewriter for ChooseIndex<'_> {
    type Context = ();

    fn rewrite(
        &mut self,
        n: LogicalPlan,
        _: &Self::Context,
    ) -> Result<LogicalPlan, DataFusionError> {
        let p = self.choose_table_index(n)?;
        let mut p = pull_up_cluster_send(p)?;
        if self.enable_topk {
            p = materialize_topk(p)?;
        }
        Ok(p)
    }
}


fn try_extract_cluster_send(p: &LogicalPlan) -> Option<&ClusterSendNode> {
    if let LogicalPlan::Extension { node } = p {
        return node.as_any().downcast_ref::<ClusterSendNode>();
    }
    return None;
}

impl ChooseIndex<'_> {
    fn choose_table_index(&mut self, mut p: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        match &mut p {
            LogicalPlan::TableScan { source, .. } => {
                if let Some(table) = source.as_any().downcast_ref::<CubeTableLogical>() {
                    assert!(
                        self.next_index < self.chosen_indices.len(),
                        "inconsistent state"
                    );

                    assert_eq!(
                        table.table.table.get_id(),
                        self.chosen_indices[self.next_index]
                            .table_path
                            .table
                            .get_id()
                    );

                    let snapshot = self.chosen_indices[self.next_index].clone();
                    self.next_index += 1;

                    let table_schema = source.schema();
                    *source = Arc::new(CubeTable::try_new(
                        snapshot.clone(),
                        // Filled by workers
                        HashMap::new(),
                        Vec::new(),
                        NoopParquetMetadataCache::new(),
                    )?);

                    let index_schema = source.schema();
                    assert_eq!(table_schema, index_schema);
                    let input = match self.limit {
                        Some(n) => LogicalPlan::Limit { n, input: Arc::new(p) },
                        None => p
                    };
                    return Ok(ClusterSendNode::new(
                        Arc::new(input),
                        vec![vec![Snapshot::Index(snapshot)]],
                    )
                    .into_plan());
                } else if let Some(table) = source.as_any().downcast_ref::<InlineTableProvider>() {
                    let id = table.get_id();
                    return Ok(ClusterSendNode::new(
                        Arc::new(p),
                        vec![vec![Snapshot::Inline(InlineSnapshot { id })]],
                    )
                    .into_plan());
                } else {
                    panic!("Unexpected table source")
                }
            },
            LogicalPlan::Limit { n, .. } => {
                if self.pushdown_limit {
                    self.limit = Some(n.to_owned());
                }
                return Ok(p)
            }
            _ => return Ok(p),
        }
    }
}

struct PushDownLimit {
}

impl PlanRewriter for PushDownLimit {
    type Context = Option<usize>;

    fn enter_node(
        &mut self,
        n: &LogicalPlan,
        _: &Self::Context,
    ) -> Option<Self::Context> {
        match n {
            LogicalPlan::Limit { n, .. } => Some(Some(*n)),
            _ => None
        } 
    }
    fn rewrite(
        &mut self,
        n: LogicalPlan,
        limit: &Self::Context,
    ) -> Result<LogicalPlan, DataFusionError> {
        match &n {
            LogicalPlan::Extension { node } => {
                if let Some(limit) = limit {
                    if let Some(cs) = node.as_any().downcast_ref::<ClusterSendNode>() {
                        let res = ClusterSendNode::new(
                            Arc::new(
                                LogicalPlan::Limit { n: *limit, input: cs.input.clone() }
                                ),
                            cs.snapshots.clone()
                            ).into_plan();
                        Ok(res)
                    } else {
                        Ok(n)
                    }
                } else {
                    Ok(n)
                }
            }
            _ => Ok(n)
        }
    }
}

struct IndexCandidate {
    /// May contain for unmatched index.
    pub ordinary_index: Result<IndexSnapshot, DataFusionError>,
    pub partitioned_index: Option<IndexSnapshot>,
}

fn check_aggregates_expr(table: &IdRow<Table>, aggregates: &Vec<Expr>) -> bool {
    let table_aggregates = table.get_row().aggregate_columns();

    for aggr in aggregates.iter() {
        match aggr {
            Expr::AggregateFunction { fun, args, .. } => {
                if args.len() != 1 {
                    return false;
                }

                let aggr_fun = match fun {
                    FusionAggregateFunction::Sum => Some(AggregateFunction::SUM),
                    FusionAggregateFunction::Max => Some(AggregateFunction::MAX),
                    FusionAggregateFunction::Min => Some(AggregateFunction::MIN),
                    _ => None,
                };

                if aggr_fun.is_none() {
                    return false;
                }

                let aggr_fun = aggr_fun.unwrap();

                let col_match = match &args[0] {
                    Expr::Column(col) => table_aggregates.iter().any(|ta| {
                        ta.function() == &aggr_fun && ta.column().get_name() == &col.name
                    }),
                    _ => false,
                };

                if !col_match {
                    return false;
                }
            }
            Expr::AggregateUDF { fun, args } => {
                if args.len() != 1 {
                    return false;
                }

                let aggr_fun = match fun.name.to_uppercase().as_str() {
                    "MERGE" => Some(AggregateFunction::MERGE),
                    _ => None,
                };

                if aggr_fun.is_none() {
                    return false;
                }

                let aggr_fun = aggr_fun.unwrap();

                let col_match = match &args[0] {
                    Expr::Column(col) => table_aggregates.iter().any(|ta| {
                        ta.function() == &aggr_fun && ta.column().get_name() == &col.name
                    }),
                    _ => false,
                };

                if !col_match {
                    return false;
                }
            }
            _ => {
                return false;
            }
        };
    }
    true
}

// Picks the index, but not partitions snapshots.
async fn pick_index(
    c: &IndexConstraints,
    schema: IdRow<Schema>,
    table: IdRow<Table>,
    indices: Vec<IdRow<Index>>,
) -> Result<IndexCandidate, DataFusionError> {
    let sort_on = c.sort_on.as_ref().map(|sc| (&sc.sort_on, sc.required));

    let aggr_index_allowed = check_aggregates_expr(&table, &c.aggregates);

    let default_index = indices.iter().next().expect("no default index");
    let (index, mut partitioned_index, sort_on) = if let Some(projection_column_indices) =
        &c.projection
    {
        let projection_columns = CubeTable::project_to_table(&table, &projection_column_indices);

        let mut filter_columns = HashSet::new();
        for f in c.filters.iter() {
            expr_to_columns(f, &mut filter_columns)?;
        }

        // Skipping default index
        let filtered_by_sort_on = indices.iter().skip(1).filter(|i| {
            if let Some((join_on_columns, required)) = sort_on.as_ref() {
                if i.get_row().sort_key_size() < (join_on_columns.len() as u64) {
                    return false;
                }
                let all_columns_in_index = match i.get_row().get_type() {
                    IndexType::Aggregate => {
                        if aggr_index_allowed {
                            let projection_check = projection_columns.iter().all(|c| {
                                i.get_row()
                                    .get_columns()
                                    .iter()
                                    .find(|ic| ic.get_name() == c.get_name())
                                    .is_some()
                            });
                            let filter_check = filter_columns.iter().all(|c| {
                                i.get_row()
                                    .get_columns()
                                    .iter()
                                    .find(|ic| ic.get_name() == &c.name)
                                    .is_some()
                            });

                            projection_check && filter_check
                        } else {
                            false
                        }
                    }
                    _ => true,
                };

                if !all_columns_in_index {
                    return false;
                }
                let join_columns_in_index = join_on_columns
                    .iter()
                    .map(|c| {
                        i.get_row()
                            .get_columns()
                            .iter()
                            .find(|ic| ic.get_name().as_str() == c.as_str())
                            .cloned()
                    })
                    .collect::<Option<Vec<_>>>();
                let join_columns_in_index = match join_columns_in_index {
                    None => return false,
                    Some(c) => c,
                };
                let mut join_columns_indices = CubeTable::project_to_index_positions(
                    &join_columns_in_index
                        .iter()
                        .map(|c| c.get_name().to_string())
                        .collect(),
                    &i,
                );

                //TODO We are not touching indexes for join yet, because they should be the same sorted for different tables.
                if !required {
                    join_columns_indices.sort();
                }

                let matches = join_columns_indices
                    .iter()
                    .enumerate()
                    .all(|(i, col_i)| Some(i) == *col_i);
                matches
            } else {
                true
            }
        });
        let optimal_with_partitioned_index = optimal_index_by_score(
            filtered_by_sort_on
                .clone()
                .filter(|i| i.get_row().multi_index_id().is_some()),
            &projection_columns,
            &filter_columns,
        );
        let optimal =
            optimal_index_by_score(filtered_by_sort_on, &projection_columns, &filter_columns);
        if let Some(index) = optimal_with_partitioned_index.or(optimal) {
            (
                Ok(index),
                index.get_row().multi_index_id().map(|_| index),
                sort_on,
            )
        } else {
            if let Some((join_on_columns, true)) = sort_on.as_ref() {
                let table_name = c.table.table_name();
                let err = Err(DataFusionError::Plan(format!(
                    "Can't find index to join table {} on {}. Consider creating index: CREATE INDEX {}_{} ON {} ({})",
                    table_name,
                    join_on_columns.join(", "),
                    table.get_row().get_table_name(),
                    join_on_columns.join("_"),
                    table_name,
                    join_on_columns.join(", ")
                )));
                (err, None, sort_on)
            } else {
                let optimal = optimal_index_by_score(
                    // Skipping default index
                    indices.iter().skip(1),
                    &projection_columns,
                    &filter_columns,
                );

                let index = optimal.unwrap_or(default_index);
                (
                    Ok(index),
                    index.get_row().multi_index_id().map(|_| index),
                    None,
                )
            }
        }
    } else {
        if let Some((join_on_columns, _)) = sort_on {
            return Err(DataFusionError::Plan(format!(
                "Can't find index to join table {} on {} and projection push down optimization has been disabled. Invalid state.",
                c.table.table_name(),
                join_on_columns.join(", ")
            )));
        }
        (Ok(default_index), None, None)
    };

    // Only use partitioned index for joins. Joins are indicated by the required flag.
    if !sort_on
        .as_ref()
        .map(|(_, required)| *required)
        .unwrap_or(false)
    {
        partitioned_index = None;
    }

    let schema = Arc::new(schema);
    let create_snapshot = |index: &IdRow<Index>| {
        let index_sort_on = sort_on.map(|sc| {
            index
                .get_row()
                .columns()
                .iter()
                .take(sc.0.len())
                .map(|c| c.get_name().clone())
                .collect::<Vec<_>>()
        });
        IndexSnapshot {
            index: index.clone(),
            partitions: Vec::new(), // filled with results of `pick_partitions` later.
            table_path: TablePath {
                table: table.clone(),
                schema: schema.clone(),
            },
            sort_on: index_sort_on,
        }
    };
    Ok(IndexCandidate {
        ordinary_index: index.map(create_snapshot),
        partitioned_index: partitioned_index.map(create_snapshot),
    })
}

fn optimal_index_by_score<'a, T: Iterator<Item = &'a IdRow<Index>>>(
    indexes: T,
    projection_columns: &Vec<Column>,
    filter_columns: &HashSet<logical_plan::Column>,
) -> Option<&'a IdRow<Index>> {
    #[derive(PartialEq, Eq, Clone)]
    struct Score {
        index_type: IndexType,
        index_size: u64,
        filter_score: usize,
        projection_score: usize,
    }
    impl PartialOrd for Score {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for Score {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            let res = match self.index_type {
                IndexType::Regular => match other.index_type {
                    IndexType::Regular => core::cmp::Ordering::Equal,
                    IndexType::Aggregate => core::cmp::Ordering::Greater,
                },
                IndexType::Aggregate => match other.index_type {
                    IndexType::Regular => core::cmp::Ordering::Less,
                    IndexType::Aggregate => self.index_size.cmp(&other.index_size),
                },
            };
            match res {
                core::cmp::Ordering::Equal => {}
                ord => return ord,
            }
            match self.filter_score.cmp(&other.filter_score) {
                core::cmp::Ordering::Equal => {}
                ord => return ord,
            }
            self.projection_score.cmp(&other.projection_score)
        }
    }

    indexes
        .filter_map(|i| {
            let index_size = i.get_row().sort_key_size();

            let filter_score = CubeTable::project_to_index_positions(
                &filter_columns.iter().map(|c| c.name.to_string()).collect(),
                &i,
            )
            .into_iter()
            .fold_options(0, |a, b| a + b);

            let projection_score = CubeTable::project_to_index_positions(
                &projection_columns
                    .iter()
                    .map(|c| c.get_name().to_string())
                    .collect(),
                &i,
            )
            .into_iter()
            .fold_options(0, |a, b| a + b);

            let index_score = if filter_score.is_some() && projection_score.is_some() {
                Some(Score {
                    index_type: i.get_row().get_type(),
                    index_size,
                    filter_score: filter_score.unwrap(),
                    projection_score: projection_score.unwrap(),
                })
            } else {
                None
            };

            let res = Some(i).zip(index_score);
            res
        })
        .min_by_key(|(_, score)| score.clone())
        .map(|(index, _)| index)
}

fn pick_partitions(
    i: &IndexSnapshot,
    c: &IndexConstraints,
    partitions: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
) -> Result<Vec<PartitionSnapshot>, DataFusionError> {
    let partition_filter = PartitionFilter::extract(&partition_filter_schema(&i.index), &c.filters);
    log::trace!("Extracted partition filter is {:?}", partition_filter);
    let candidate_partitions = partitions.len();
    let mut pruned_partitions = 0;

    let mut partition_snapshots = Vec::new();
    for (partition, chunks) in partitions.into_iter() {
        let min_row = partition
            .get_row()
            .get_min_val()
            .as_ref()
            .map(|r| r.values().as_slice());
        let max_row = partition
            .get_row()
            .get_max_val()
            .as_ref()
            .map(|r| r.values().as_slice());

        if !partition_filter.can_match(min_row, max_row) {
            pruned_partitions += 1;
            continue;
        }

        partition_snapshots.push(PartitionSnapshot { chunks, partition });
    }
    log::trace!(
        "Pruned {} of {} partitions",
        pruned_partitions,
        candidate_partitions
    );

    Ok(partition_snapshots)
}

fn partition_filter_schema(index: &IdRow<Index>) -> arrow::datatypes::Schema {
    let schema_fields: Vec<Field>;
    schema_fields = index
        .get_row()
        .columns()
        .iter()
        .map(|c| c.clone().into())
        .take(index.get_row().sort_key_size() as usize)
        .collect();
    arrow::datatypes::Schema::new(schema_fields)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Snapshot {
    Index(IndexSnapshot),
    Inline(InlineSnapshot),
}

pub type Snapshots = Vec<Snapshot>;

#[derive(Debug, Clone)]
pub struct ClusterSendNode {
    pub input: Arc<LogicalPlan>,
    pub snapshots: Vec<Snapshots>,
}

impl ClusterSendNode {
    pub fn new(input: Arc<LogicalPlan>, snapshots: Vec<Snapshots>) -> Self {
        ClusterSendNode { input, snapshots }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension {
            node: Arc::new(self),
        }
    }
}

impl UserDefinedLogicalNode for ClusterSendNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String, RandomState> {
        HashSet::new()
    }

    fn fmt_for_explain<'a>(&self, f: &mut Formatter<'a>) -> std::fmt::Result {
        write!(f, "ClusterSend")
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert!(exprs.is_empty());
        assert_eq!(inputs.len(), 1);

        Arc::new(ClusterSendNode {
            input: Arc::new(inputs[0].clone()),
            snapshots: self.snapshots.clone(),
        })
    }
}

fn pull_up_cluster_send(mut p: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    let snapshots;
    match &mut p {
        // These nodes have no children, return unchanged.
        LogicalPlan::TableScan { .. }
        | LogicalPlan::EmptyRelation { .. }
        | LogicalPlan::CreateExternalTable { .. }
        | LogicalPlan::Explain { .. } => return Ok(p),
        // The ClusterSend itself, return unchanged.
        LogicalPlan::Extension { .. } => return Ok(p),
        // These nodes collect results from multiple partitions, return unchanged.
        LogicalPlan::Aggregate { .. }
        | LogicalPlan::Sort { .. }
        | LogicalPlan::Limit { .. }
        | LogicalPlan::Skip { .. }
        | LogicalPlan::Repartition { .. } => return Ok(p),
        // We can always pull cluster send for these nodes.
        LogicalPlan::Projection { input, .. } | LogicalPlan::Filter { input, .. } => {
            let send;
            if let Some(s) = try_extract_cluster_send(input) {
                send = s;
            } else {
                return Ok(p);
            }
            snapshots = send.snapshots.clone();
            *input = send.input.clone();
            return Ok(ClusterSendNode::new(Arc::new(p), snapshots).into_plan());
        }
        LogicalPlan::Union { inputs, .. } => {
            // Handle UNION over constants, e.g. inline data series.
            if inputs.iter().all(|p| try_extract_cluster_send(p).is_none()) {
                return Ok(p);
            }
            let mut union_snapshots = Vec::new();
            for i in inputs {
                let send;
                if let Some(s) = try_extract_cluster_send(i) {
                    send = s;
                } else {
                    return Err(DataFusionError::Plan(
                        "UNION argument not supported".to_string(),
                    ));
                }
                union_snapshots.extend(send.snapshots.concat());
                *i = send.input.as_ref().clone();
            }
            snapshots = vec![union_snapshots];
            return Ok(ClusterSendNode::new(Arc::new(p), snapshots).into_plan());
        }
        LogicalPlan::Join { left, right, .. } => {
            let lsend;
            let rsend;
            if let (Some(l), Some(r)) = (
                try_extract_cluster_send(left),
                try_extract_cluster_send(right),
            ) {
                lsend = l;
                rsend = r;
            } else {
                return Err(DataFusionError::Plan(
                    "JOIN argument not supported".to_string(),
                ));
            }
            snapshots = lsend
                .snapshots
                .iter()
                .chain(rsend.snapshots.iter())
                .cloned()
                .collect();
            *left = lsend.input.clone();
            *right = rsend.input.clone();
            return Ok(ClusterSendNode::new(Arc::new(p), snapshots).into_plan());
        }
        LogicalPlan::Window { .. } | LogicalPlan::CrossJoin { .. } => {
            return Err(DataFusionError::Internal(
                "unsupported operation".to_string(),
            ))
        }
    }
}

pub struct CubeExtensionPlanner {
    pub cluster: Option<Arc<dyn Cluster>>,
    pub serialized_plan: Arc<SerializedPlan>,
}

impl ExtensionPlanner for CubeExtensionPlanner {
    fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let inputs = physical_inputs;
        if let Some(cs) = node.as_any().downcast_ref::<ClusterSendNode>() {
            assert_eq!(inputs.len(), 1);
            let input = inputs.into_iter().next().unwrap();
            Ok(Some(self.plan_cluster_send(
                input.clone(),
                &cs.snapshots,
                input.schema(),
                false,
                usize::MAX,
            )?))
        } else if let Some(topk) = node.as_any().downcast_ref::<ClusterAggregateTopK>() {
            assert_eq!(inputs.len(), 1);
            let input = inputs.into_iter().next().unwrap();
            Ok(Some(plan_topk(planner, self, topk, input.clone(), state)?))
        } else if let Some(_) = node.as_any().downcast_ref::<PanicWorkerNode>() {
            assert_eq!(inputs.len(), 0);
            Ok(Some(plan_panic_worker()?))
        } else {
            Ok(None)
        }
    }
}

impl CubeExtensionPlanner {
    pub fn plan_cluster_send(
        &self,
        input: Arc<dyn ExecutionPlan>,
        snapshots: &Vec<Snapshots>,
        schema: SchemaRef,
        use_streaming: bool,
        max_batch_rows: usize,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if snapshots.is_empty() {
            return Ok(Arc::new(EmptyExec::new(false, schema)));
        }
        // Note that MergeExecs are added automatically when needed.
        if let Some(c) = self.cluster.as_ref() {
            Ok(Arc::new(ClusterSendExec::new(
                schema,
                c.clone(),
                self.serialized_plan.clone(),
                snapshots,
                input,
                use_streaming,
            )?))
        } else {
            Ok(Arc::new(WorkerExec {
                input,
                schema,
                max_batch_rows,
            }))
        }
    }
}

/// Produced on the worker, marks the subplan that the worker must execute. Anything above is the
/// router part of the plan and must be ignored.
#[derive(Debug)]
pub struct WorkerExec {
    pub input: Arc<dyn ExecutionPlan>,
    // TODO: remove and use `self.input.schema()`
    //       This is a hacky workaround for wrong schema of joins after projection pushdown.
    pub schema: SchemaRef,
    pub max_batch_rows: usize,
}

#[async_trait]
impl ExecutionPlan for WorkerExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(WorkerExec {
            input: children.into_iter().next().unwrap(),
            schema: self.schema.clone(),
            max_batch_rows: self.max_batch_rows,
        }))
    }

    fn output_hints(&self) -> OptimizerHints {
        self.input.output_hints()
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.input.execute(partition).await
    }
}

/// Use this to pick the part of the plan that the worker must execute.
pub fn get_worker_plan(
    p: &Arc<dyn ExecutionPlan>,
) -> Option<(Arc<dyn ExecutionPlan>, /*max_batch_rows*/ usize)> {
    if let Some(p) = p.as_any().downcast_ref::<WorkerExec>() {
        return Some((p.input.clone(), p.max_batch_rows));
    } else {
        let children = p.children();
        // We currently do not split inside joins or leaf nodes.
        if children.len() != 1 {
            return None;
        } else {
            return get_worker_plan(&children[0]);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Schema as ArrowSchema;
    use async_trait::async_trait;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::context::ExecutionContext;
    use datafusion::logical_plan::LogicalPlan;
    use datafusion::physical_plan::udaf::AggregateUDF;
    use datafusion::physical_plan::udf::ScalarUDF;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion::sql::planner::{ContextProvider, SqlToRel};
    use itertools::Itertools;
    use pretty_assertions::assert_eq;

    use crate::config::Config;
    use crate::metastore::multi_index::MultiPartition;
    use crate::metastore::table::{Table, TablePath};
    use crate::metastore::{Chunk, Column, ColumnType, IdRow, Index, Partition, Schema};
    use crate::queryplanner::planning::{choose_index, try_extract_cluster_send, PlanIndexStore};
    use crate::queryplanner::pretty_printers::PPOptions;
    use crate::queryplanner::query_executor::ClusterSendExec;
    use crate::queryplanner::serialized_plan::RowRange;
    use crate::queryplanner::{pretty_printers, CubeTableLogical};
    use crate::sql::parser::{CubeStoreParser, Statement};
    use crate::table::{Row, TableValue};
    use crate::CubeError;
    use datafusion::catalog::TableReference;
    use std::collections::HashMap;
    use std::iter::FromIterator;

    #[tokio::test]
    pub async fn test_choose_index() {
        let indices = default_indices();
        let plan = initial_plan("SELECT * FROM s.Customers WHERE customer_id = 1", &indices);
        assert_eq!(
            pretty_printers::pp_plan(&plan),
            "Projection, [s.Customers.customer_id, s.Customers.customer_name, s.Customers.customer_city, s.Customers.customer_registered_date]\
           \n  Filter\
           \n    Scan s.Customers, source: CubeTableLogical, fields: *"
        );

        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(
            pretty_printers::pp_plan(&plan),
            "ClusterSend, indices: [[0]]\
           \n  Projection, [s.Customers.customer_id, s.Customers.customer_name, s.Customers.customer_city, s.Customers.customer_registered_date]\
           \n    Filter\
           \n      Scan s.Customers, source: CubeTable(index: default:0:[]:sort_on[customer_id]), fields: *"
        );

        let plan = initial_plan(
            "SELECT order_customer, order_id \
             FROM s.Orders \
             GROUP BY order_customer, order_id
             ",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        let expected ="Projection, [s.Orders.order_customer, s.Orders.order_id]\
                       \n  Aggregate\
                       \n    ClusterSend, indices: [[2]]\
                       \n      Scan s.Orders, source: CubeTable(index: default:2:[]:sort_on[order_id, order_customer]), fields: [order_id, order_customer]";
        assert_eq!(pretty_printers::pp_plan(&plan), expected);
        let plan = initial_plan(
            "SELECT order_customer, order_id \
             FROM s.Orders \
             GROUP BY order_id, order_customer
             ",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(pretty_printers::pp_plan(&plan), expected);

        let plan = initial_plan(
            "SELECT order_customer, order_id \
             FROM s.Orders \
             WHERE order_customer = 'ffff'
             GROUP BY order_customer, order_id
             ",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        let expected ="Projection, [s.Orders.order_customer, s.Orders.order_id]\
                       \n  Aggregate\
                       \n    ClusterSend, indices: [[3]]\
                       \n      Filter\
                       \n        Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer, order_id]), fields: [order_id, order_customer]";

        assert_eq!(pretty_printers::pp_plan(&plan), expected);

        let plan = initial_plan(
            "SELECT order_customer, order_id \
             FROM s.Orders \
             WHERE order_customer = 'ffff'
             GROUP BY order_id, order_customer
             ",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(pretty_printers::pp_plan(&plan), expected);

        let plan = initial_plan(
            "SELECT order_customer, order_id \
             FROM s.Orders \
             WHERE order_customer = 'ffff'
             GROUP BY order_id, order_customer, order_product
             ",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;

        let expected ="Projection, [s.Orders.order_customer, s.Orders.order_id]\
                       \n  Aggregate\
                       \n    ClusterSend, indices: [[2]]\
                       \n      Filter\
                       \n        Scan s.Orders, source: CubeTable(index: default:2:[]:sort_on[order_id, order_customer, order_product]), fields: [order_id, order_customer, order_product]";

        assert_eq!(pretty_printers::pp_plan(&plan), expected);

        //Should prefer a non-default index for joins.
        let plan = initial_plan(
            "SELECT order_id, order_amount, customer_name \
             FROM s.Orders \
             JOIN s.Customers ON order_customer = customer_id",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(pretty_printers::pp_plan(&plan), "ClusterSend, indices: [[3], [0]]\
                                  \n  Projection, [s.Orders.order_id, s.Orders.order_amount, s.Customers.customer_name]\
                                  \n    Join on: [#s.Orders.order_customer = #s.Customers.customer_id]\
                                  \n      Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer]), fields: [order_id, order_customer, order_amount]\
                                  \n      Scan s.Customers, source: CubeTable(index: default:0:[]:sort_on[customer_id]), fields: [customer_id, customer_name]");

        let plan = initial_plan(
            "SELECT order_id, customer_name, product_name \
             FROM s.Orders \
             JOIN s.Customers on order_customer = customer_id \
             JOIN s.Products ON order_product = product_id",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(pretty_printers::pp_plan(&plan), "ClusterSend, indices: [[3], [0], [5]]\
        \n  Projection, [s.Orders.order_id, s.Customers.customer_name, s.Products.product_name]\
        \n    Join on: [#s.Orders.order_product = #s.Products.product_id]\
        \n      Join on: [#s.Orders.order_customer = #s.Customers.customer_id]\
        \n        Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer]), fields: [order_id, order_customer, order_product]\
        \n        Scan s.Customers, source: CubeTable(index: default:0:[]:sort_on[customer_id]), fields: [customer_id, customer_name]\
        \n      Scan s.Products, source: CubeTable(index: default:5:[]:sort_on[product_id]), fields: *");

        let plan = initial_plan(
            "SELECT c2.customer_name \
             FROM s.Orders \
             JOIN s.Customers c1 on order_customer = c1.customer_id \
             JOIN s.Customers c2 ON order_city = c2.customer_city \
             WHERE c1.customer_name = 'Customer 1'",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(pretty_printers::pp_plan(&plan), "ClusterSend, indices: [[3], [0], [1]]\
                                  \n  Projection, [c2.customer_name]\
                                  \n    Join on: [#s.Orders.order_city = #c2.customer_city]\
                                  \n      Join on: [#s.Orders.order_customer = #c1.customer_id]\
                                  \n        Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer]), fields: [order_customer, order_city]\
                                  \n        Filter\
                                  \n          Scan c1, source: CubeTable(index: default:0:[]:sort_on[customer_id, customer_name]), fields: [customer_id, customer_name]\
                                  \n      Scan c2, source: CubeTable(index: by_city:1:[]:sort_on[customer_city]), fields: [customer_name, customer_city]");
    }

    #[tokio::test]
    pub async fn test_materialize_topk() {
        let indices = default_indices();
        let plan = initial_plan(
            "SELECT order_customer, SUM(order_amount) FROM s.Orders \
             GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(
            pretty_printers::pp_plan(&plan),
            "Projection, [s.Orders.order_customer, SUM(s.Orders.order_amount)]\
           \n  ClusterAggregateTopK, limit: 10\
           \n    Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer]), fields: [order_customer, order_amount]"
        );

        // Projections should be handled properly.
        let plan = initial_plan(
            "SELECT order_customer `customer`, SUM(order_amount) `amount` FROM s.Orders \
             GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(
            pretty_printers::pp_plan(&plan),
            "Projection, [customer, amount]\
           \n  ClusterAggregateTopK, limit: 10\
           \n    Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer]), fields: [order_customer, order_amount]"
        );

        let plan = initial_plan(
            "SELECT SUM(order_amount) `amount`, order_customer `customer` FROM s.Orders \
             GROUP BY 2 ORDER BY 1 DESC LIMIT 10",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        let mut with_sort_by = PPOptions::default();
        with_sort_by.show_sort_by = true;
        assert_eq!(
            pretty_printers::pp_plan_ext(&plan, &with_sort_by),
            "Projection, [amount, customer]\
           \n  ClusterAggregateTopK, limit: 10, sortBy: [2 desc null last]\
           \n    Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer]), fields: [order_customer, order_amount]"
        );

        // Ascending order is also ok.
        let plan = initial_plan(
            "SELECT order_customer `customer`, SUM(order_amount) `amount` FROM s.Orders \
             GROUP BY 1 ORDER BY 2 ASC LIMIT 10",
            &indices,
        );
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(
            pretty_printers::pp_plan_ext(&plan, &with_sort_by),
            "Projection, [customer, amount]\
           \n  ClusterAggregateTopK, limit: 10, sortBy: [2 null last]\
           \n    Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer]), fields: [order_customer, order_amount]"
        );

        // MAX and MIN are ok, as well as multiple aggregation.
        let plan = initial_plan(
            "SELECT order_customer `customer`, SUM(order_amount) `amount`, \
                    MIN(order_amount) `min_amount`, MAX(order_amount) `max_amount` \
             FROM s.Orders \
             GROUP BY 1 ORDER BY 3 DESC, 2 ASC LIMIT 10",
            &indices,
        );
        let mut verbose = with_sort_by;
        verbose.show_aggregations = true;
        let plan = choose_index(&plan, &indices).await.unwrap().0;
        assert_eq!(
            pretty_printers::pp_plan_ext(&plan, &verbose),
            "Projection, [customer, amount, min_amount, max_amount]\
           \n  ClusterAggregateTopK, limit: 10, aggs: [SUM(#s.Orders.order_amount), MIN(#s.Orders.order_amount), MAX(#s.Orders.order_amount)], sortBy: [3 desc null last, 2 null last]\
           \n    Scan s.Orders, source: CubeTable(index: by_customer:3:[]:sort_on[order_customer]), fields: [order_customer, order_amount]"
        );

        // Should not introduce TopK by mistake in unsupported cases.
        // No 'order by'.
        let plan = initial_plan(
            "SELECT order_customer `customer`, SUM(order_amount) `amount` FROM s.Orders \
             GROUP BY 1 LIMIT 10",
            &indices,
        );
        let pp = pretty_printers::pp_plan(&choose_index(&plan, &indices).await.unwrap().0);
        assert!(!pp.contains("TopK"), "plan contained topk:\n{}", pp);

        // No limit.
        let plan = initial_plan(
            "SELECT order_customer `customer`, SUM(order_amount) `amount` FROM s.Orders \
             GROUP BY 1 ORDER BY 2 DESC",
            &indices,
        );
        let pp = pretty_printers::pp_plan(&choose_index(&plan, &indices).await.unwrap().0);
        assert!(!pp.contains("TopK"), "plan contained topk:\n{}", pp);

        // Sort by group key, not the aggregation result.
        let plan = initial_plan(
            "SELECT order_customer `customer`, SUM(order_amount) `amount` FROM s.Orders \
             GROUP BY 1 ORDER BY 1 DESC LIMIT 10",
            &indices,
        );
        let pp = pretty_printers::pp_plan(&choose_index(&plan, &indices).await.unwrap().0);
        assert!(!pp.contains("TopK"), "plan contained topk:\n{}", pp);

        // Unsupported aggregation function.
        let plan = initial_plan(
            "SELECT order_customer `customer`, AVG(order_amount) `amount` FROM s.Orders \
             GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            &indices,
        );
        let pp = pretty_printers::pp_plan(&choose_index(&plan, &indices).await.unwrap().0);
        assert!(!pp.contains("TopK"), "plan contained topk:\n{}", pp);
        let plan = initial_plan(
            "SELECT order_customer `customer`, COUNT(order_amount) `amount` FROM s.Orders \
             GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            &indices,
        );
        let pp = pretty_printers::pp_plan(&choose_index(&plan, &indices).await.unwrap().0);
        assert!(!pp.contains("TopK"), "plan contained topk:\n{}", pp);

        // Distinct aggregations.
        let plan = initial_plan(
            "SELECT order_customer `customer`, SUM(DISTINCT order_amount) `amount` FROM s.Orders \
             GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            &indices,
        );
        let pp = pretty_printers::pp_plan(&choose_index(&plan, &indices).await.unwrap().0);
        assert!(!pp.contains("TopK"), "plan contained topk:\n{}", pp);

        // Complicated sort expressions.
        let plan = initial_plan(
            "SELECT order_customer `customer`, SUM(order_amount) `amount` FROM s.Orders \
             GROUP BY 1 ORDER BY amount * amount  DESC LIMIT 10",
            &indices,
        );
        let pp = pretty_printers::pp_plan(&choose_index(&plan, &indices).await.unwrap().0);
        assert!(!pp.contains("TopK"), "plan contained topk:\n{}", pp);
    }

    #[tokio::test]
    pub async fn test_partitioned_index_join() {
        let mut indices = indices_with_partitioned_index();
        let plan = initial_plan(
            "SELECT customer_name, order_city FROM s.Orders JOIN s.Customers \
               ON order_customer = customer_id",
            &indices,
        );

        let pp = pretty_printers::pp_plan(&choose_index(&plan, &indices).await.unwrap().0);
        assert_eq!(pp, "ClusterSend, indices: [[6], [2]]\
                      \n  Projection, [s.Customers.customer_name, s.Orders.order_city]\
                      \n    Join on: [#s.Orders.order_customer = #s.Customers.customer_id]\
                      \n      Scan s.Orders, source: CubeTable(index: #mi0:6:[]:sort_on[order_customer]), fields: [order_customer, order_city]\
                      \n      Scan s.Customers, source: CubeTable(index: #mi0:2:[]:sort_on[customer_id]), fields: [customer_id, customer_name]");

        // Add some multi-partitions and validate how it runs.
        indices
            .multi_partitions
            .push(MultiPartition::new_root(0).set_active(false));
        indices.multi_partitions.push(
            MultiPartition::new_child(
                &indices.get_multi_partition(0),
                None,
                Some(Row::new(vec![TableValue::Int(100)])),
            )
            .set_active(false),
        );
        indices.multi_partitions.push(
            MultiPartition::new_child(
                &indices.get_multi_partition(0),
                Some(Row::new(vec![TableValue::Int(100)])),
                None,
            )
            .set_active(true),
        );
        indices.multi_partitions.push(
            MultiPartition::new_child(
                &indices.get_multi_partition(1),
                None,
                Some(Row::new(vec![TableValue::Int(25)])),
            )
            .set_active(true),
        );
        indices.multi_partitions.push(
            MultiPartition::new_child(
                &indices.get_multi_partition(1),
                Some(Row::new(vec![TableValue::Int(25)])),
                Some(Row::new(vec![TableValue::Int(100)])),
            )
            .set_active(true),
        );
        for i in 0..indices.indices.len() {
            // This name marks indices for multi-index.
            if indices.indices[i].get_name() == "#mi0" {
                add_multipart_partitions(
                    i as u64,
                    &indices.multi_partitions,
                    &mut indices.partitions,
                );
            }
        }
        for p in 0..indices.partitions.len() {
            indices
                .chunks
                .push(Chunk::new(p as u64, 123, None, None, false).set_uploaded(true));
        }

        // Plan again.
        let (with_index, meta) = choose_index(&plan, &indices).await.unwrap();
        let pp = pretty_printers::pp_plan(&with_index);
        assert_eq!(pp, "ClusterSend, indices: [[6], [2]]\
                      \n  Projection, [s.Customers.customer_name, s.Orders.order_city]\
                      \n    Join on: [#s.Orders.order_customer = #s.Customers.customer_id]\
                      \n      Scan s.Orders, source: CubeTable(index: #mi0:6:[5, 6, 7, 8, 9]:sort_on[order_customer]), fields: [order_customer, order_city]\
                      \n      Scan s.Customers, source: CubeTable(index: #mi0:2:[0, 1, 2, 3, 4]:sort_on[customer_id]), fields: [customer_id, customer_name]");

        let c = Config::test("partitioned_index_join").update_config(|mut c| {
            c.server_name = "router".to_string();
            c.select_workers = vec!["worker1".to_string(), "worker2".to_string()];
            c
        });
        let cs = &try_extract_cluster_send(&with_index).unwrap().snapshots;
        let assigned = ClusterSendExec::distribute_to_workers(
            c.config_obj().as_ref(),
            &cs,
            &meta.multi_part_subtree,
        )
        .unwrap();

        let part = |id: u64, start: Option<i64>, end: Option<i64>| {
            let start = start.map(|i| Row::new(vec![TableValue::Int(i)]));
            let end = end.map(|i| Row::new(vec![TableValue::Int(i)]));
            (id, RowRange { start, end })
        };
        assert_eq!(
            assigned,
            vec![
                (
                    "worker1".to_string(),
                    (
                        vec![
                            part(2, None, None),
                            part(7, None, None),
                            part(0, Some(100), None),
                            part(5, Some(100), None),
                            part(3, None, None),
                            part(8, None, None),
                            part(1, None, Some(25)),
                            part(6, None, Some(25)),
                            part(0, None, Some(25)),
                            part(5, None, Some(25)),
                        ],
                        vec![]
                    )
                ),
                (
                    "worker2".to_string(),
                    (
                        vec![
                            part(4, None, None),
                            part(9, None, None),
                            part(1, Some(25), Some(100)),
                            part(6, Some(25), Some(100)),
                            part(0, Some(25), Some(100)),
                            part(5, Some(25), Some(100)),
                        ],
                        vec![]
                    )
                )
            ]
        );
    }

    fn default_indices() -> TestIndices {
        make_test_indices(false)
    }

    fn indices_with_partitioned_index() -> TestIndices {
        make_test_indices(true)
    }

    fn add_multipart_partitions(
        index_id: u64,
        multi_parts: &[MultiPartition],
        partitions: &mut Vec<Partition>,
    ) {
        let first_part_i = partitions.len() as u64;
        for i in 0..multi_parts.len() {
            let mp = &multi_parts[i];
            let mut p = Partition::new(
                index_id,
                Some(i as u64),
                mp.min_row().cloned(),
                mp.max_row().cloned(),
            );
            if let Some(parent) = mp.parent_multi_partition_id() {
                assert!(parent <= multi_parts.len() as u64);
                p = p.update_parent_partition_id(Some(first_part_i + parent));
            }
            if mp.active() {
                p = p.to_warmed_up().to_active(true);
            }

            partitions.push(p);
        }
    }

    /// Most tests in this module use this schema.
    fn make_test_indices(add_multi_indices: bool) -> TestIndices {
        const SCHEMA: u64 = 0;
        const PARTITIONED_INDEX: u64 = 0; // Only 1 partitioned index for now.
        let mut i = TestIndices::default();

        let customers_cols = int_columns(&[
            "customer_id",
            "customer_name",
            "customer_city",
            "customer_registered_date",
        ]);
        let customers = i.add_table(Table::new(
            "Customers".to_string(),
            SCHEMA,
            customers_cols.clone(),
            None,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
            None,
        ));
        i.indices.push(
            Index::try_new(
                "by_city".to_string(),
                customers,
                put_first("customer_city", &customers_cols),
                1,
                None,
                None,
                Index::index_type_default(),
            )
            .unwrap(),
        );
        if add_multi_indices {
            i.indices.push(
                Index::try_new(
                    "#mi0".to_string(),
                    customers,
                    put_first("customer_id", &customers_cols),
                    1,
                    None,
                    Some(PARTITIONED_INDEX),
                    Index::index_type_default(),
                )
                .unwrap(),
            );
        }

        let orders_cols = int_columns(&[
            "order_id",
            "order_customer",
            "order_product",
            "order_amount",
            "order_city",
        ]);
        let orders = i.add_table(Table::new(
            "Orders".to_string(),
            SCHEMA,
            orders_cols.clone(),
            None,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
            None,
        ));

        i.indices.push(
            Index::try_new(
                "by_customer".to_string(),
                orders,
                put_first("order_customer", &orders_cols),
                2,
                None,
                None,
                Index::index_type_default(),
            )
            .unwrap(),
        );
        i.indices.push(
            Index::try_new(
                "by_city".to_string(),
                customers,
                put_first("order_city", &orders_cols),
                2,
                None,
                None,
                Index::index_type_default(),
            )
            .unwrap(),
        );
        if add_multi_indices {
            i.indices.push(
                Index::try_new(
                    "#mi0".to_string(),
                    orders,
                    put_first("order_customer", &orders_cols),
                    1,
                    None,
                    Some(PARTITIONED_INDEX),
                    Index::index_type_default(),
                )
                .unwrap(),
            );
        }

        i.add_table(Table::new(
            "Products".to_string(),
            SCHEMA,
            int_columns(&["product_id", "product_name"]),
            None,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
            None,
        ));

        i
    }

    fn put_first(c: &str, cols: &[Column]) -> Vec<Column> {
        let mut cols = cols.iter().cloned().collect_vec();
        let pos = cols.iter().position(|col| col.get_name() == c).unwrap();
        cols.swap(0, pos);
        cols
    }

    fn int_columns(names: &[&str]) -> Vec<Column> {
        names
            .iter()
            .enumerate()
            .map(|(i, n)| Column::new(n.to_string(), ColumnType::Int, i))
            .collect()
    }

    fn initial_plan(s: &str, i: &TestIndices) -> LogicalPlan {
        let statement;
        if let Statement::Statement(s) = CubeStoreParser::new(s).unwrap().parse_statement().unwrap()
        {
            statement = s;
        } else {
            panic!("not a statement")
        }
        let plan = SqlToRel::new(i)
            .statement_to_plan(&DFStatement::Statement(statement))
            .unwrap();
        ExecutionContext::new().optimize(&plan).unwrap()
    }

    #[derive(Debug, Default)]
    pub struct TestIndices {
        tables: Vec<Table>,
        indices: Vec<Index>,
        partitions: Vec<Partition>,
        chunks: Vec<Chunk>,
        multi_partitions: Vec<MultiPartition>,
    }

    impl TestIndices {
        pub fn add_table(&mut self, t: Table) -> u64 {
            assert_eq!(t.get_schema_id(), 0);
            let table_id = self.tables.len() as u64;
            self.indices.push(
                Index::try_new(
                    "default".to_string(),
                    table_id,
                    t.get_columns().clone(),
                    t.get_columns().len() as u64,
                    None,
                    None,
                    Index::index_type_default(),
                )
                .unwrap(),
            );
            self.tables.push(t);
            table_id
        }

        pub fn get_multi_partition(&self, id: u64) -> IdRow<MultiPartition> {
            IdRow::new(id, self.multi_partitions[id as usize].clone())
        }

        pub fn chunks_for_partition(&self, partition_id: u64) -> Vec<IdRow<Chunk>> {
            let mut r = Vec::new();
            for i in 0..self.chunks.len() {
                if self.chunks[i].get_partition_id() != partition_id {
                    continue;
                }
                r.push(IdRow::new(i as u64, self.chunks[i].clone()));
            }
            r
        }

        pub fn schema(&self) -> IdRow<Schema> {
            IdRow::new(0, Schema::new("s".to_string()))
        }
    }

    impl ContextProvider for TestIndices {
        fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
            let name = match name {
                TableReference::Partial { schema, table } => {
                    if schema != "s" {
                        return None;
                    }
                    table
                }
                TableReference::Bare { .. } | TableReference::Full { .. } => return None,
            };
            self.tables
                .iter()
                .find_position(|t| t.get_table_name() == name)
                .map(|(id, t)| -> Arc<dyn TableProvider> {
                    let schema = Arc::new(ArrowSchema::new(
                        t.get_columns()
                            .iter()
                            .map(|c| c.clone().into())
                            .collect::<Vec<_>>(),
                    ));
                    Arc::new(CubeTableLogical {
                        table: TablePath {
                            table: IdRow::new(id as u64, t.clone()),
                            schema: Arc::new(self.schema()),
                        },
                        schema,
                    })
                })
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            // Note that this is missing HLL functions.
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            // Note that this is missing HLL functions.
            None
        }
    }

    #[async_trait]
    impl PlanIndexStore for TestIndices {
        async fn get_tables_with_indexes(
            &self,
            inputs: Vec<(String, String)>,
        ) -> Result<Vec<(IdRow<Schema>, IdRow<Table>, Vec<IdRow<Index>>)>, CubeError> {
            let mut r = Vec::with_capacity(inputs.len());
            for (schema, table) in inputs {
                let table = self.get_table(schema, table).await?;
                let schema = self
                    .get_schema_by_id(table.get_row().get_schema_id())
                    .await?;

                let mut indexes;
                indexes = self.get_table_indexes(table.get_id()).await?;
                indexes.insert(0, self.get_default_index(table.get_id()).await?);

                r.push((schema, table, indexes))
            }
            Ok(r)
        }

        async fn get_active_partitions_and_chunks_by_index_id_for_select(
            &self,
            index_id: Vec<u64>,
        ) -> Result<Vec<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>>, CubeError> {
            Ok(index_id
                .iter()
                .map(|index_id| {
                    self.partitions
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| p.get_index_id() == *index_id)
                        .map(|(id, p)| {
                            (
                                IdRow::new(id as u64, p.clone()),
                                self.chunks_for_partition(id as u64),
                            )
                        })
                        .filter(|(p, chunks)| p.get_row().is_active() || !chunks.is_empty())
                        .collect()
                })
                .collect())
        }

        async fn get_multi_partition_subtree(
            &self,
            _multi_part_ids: Vec<u64>,
        ) -> Result<HashMap<u64, MultiPartition>, CubeError> {
            Ok(HashMap::from_iter(
                self.multi_partitions
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (i as u64, p.clone())),
            ))
        }
    }

    impl TestIndices {
        async fn get_table(
            &self,
            schema_name: String,
            table_name: String,
        ) -> Result<IdRow<Table>, CubeError> {
            if schema_name != "s" {
                return Err(CubeError::internal(
                    "only 's' schema defined in tests".to_string(),
                ));
            }
            let (pos, table) = self
                .tables
                .iter()
                .find_position(|t| t.get_table_name() == &table_name)
                .ok_or_else(|| CubeError::internal(format!("table {} not found", table_name)))?;
            Ok(IdRow::new(pos as u64, table.clone()))
        }

        async fn get_schema_by_id(&self, schema_id: u64) -> Result<IdRow<Schema>, CubeError> {
            if schema_id != 0 {
                return Err(CubeError::internal(
                    "only 's' schema with id = 0 defined in tests".to_string(),
                ));
            }
            return Ok(self.schema());
        }

        async fn get_default_index(&self, table_id: u64) -> Result<IdRow<Index>, CubeError> {
            let (pos, index) = self
                .indices
                .iter()
                .find_position(|i| i.table_id() == table_id)
                .ok_or_else(|| {
                    CubeError::internal(format!("index for table {} not found", table_id))
                })?;
            Ok(IdRow::new(pos as u64, index.clone()))
        }

        async fn get_table_indexes(&self, table_id: u64) -> Result<Vec<IdRow<Index>>, CubeError> {
            Ok(self
                .indices
                .iter()
                .enumerate()
                .filter(|(_, i)| i.table_id() == table_id)
                .map(|(pos, index)| IdRow::new(pos as u64, index.clone()))
                .collect())
        }
    }
}
