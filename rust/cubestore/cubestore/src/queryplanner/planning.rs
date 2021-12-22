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
use datafusion::logical_plan::{DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{
    ExecutionPlan, OptimizerHints, Partitioning, PhysicalPlanner, SendableRecordBatchStream,
};
use flatbuffers::bitflags::_core::any::Any;
use flatbuffers::bitflags::_core::fmt::Formatter;
use itertools::Itertools;

use crate::cluster::Cluster;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::table::{Table, TablePath};
use crate::metastore::{Chunk, IdRow, Index, MetaStore, Partition, Schema};
use crate::queryplanner::optimizations::rewrite_plan::{rewrite_plan, PlanRewriter};
use crate::queryplanner::partition_filter::PartitionFilter;
use crate::queryplanner::query_executor::{ClusterSendExec, CubeTable};
use crate::queryplanner::serialized_plan::{IndexSnapshot, PartitionSnapshot, SerializedPlan};
use crate::queryplanner::topk::{materialize_topk, plan_topk, ClusterAggregateTopK};
use crate::queryplanner::CubeTableLogical;
use crate::CubeError;
use serde::{Deserialize as SerdeDeser, Deserializer, Serialize as SerdeSer, Serializer};
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::iter::FromIterator;

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
    rewrite_plan(p, &None, &mut collector)?;

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
        i.partitions = pick_partitions(i, c, ps)?
    }

    // We have enough information to finalize the logical plan.
    let mut r = ChooseIndex {
        chosen_indices: &indices,
        next_index: 0,
        enable_topk,
    };
    let plan = rewrite_plan(p, &(), &mut r)?;
    assert_eq!(r.next_index, indices.len());

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
}

#[derive(Default)]
struct CollectConstraints {
    constraints: Vec<IndexConstraints>,
}

impl PlanRewriter for CollectConstraints {
    type Context = Option<SortColumns>;

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
                let table = source.as_any().downcast_ref::<CubeTableLogical>().unwrap();
                self.constraints.push(IndexConstraints {
                    sort_on: c.clone(),
                    table: table.table.clone(),
                    projection: projection.clone(),
                    filters: filters.clone(),
                })
            }
            _ => {}
        }
        Ok(n)
    }

    fn enter_node(
        &mut self,
        n: &LogicalPlan,
        _: &Option<SortColumns>,
    ) -> Option<Option<SortColumns>> {
        fn column_name(expr: &Expr) -> Option<String> {
            match expr {
                Expr::Alias(e, _) => column_name(e),
                Expr::Column(col) => Some(col.name.clone()), // TODO use alias
                _ => None,
            }
        }
        match n {
            LogicalPlan::Aggregate { group_expr, .. } => {
                let sort_on = group_expr.iter().map(column_name).collect::<Vec<_>>();
                if !sort_on.is_empty() && sort_on.iter().all(|c| c.is_some()) {
                    Some(Some(SortColumns {
                        sort_on: sort_on.into_iter().map(|c| c.unwrap()).collect(),
                        required: false,
                    }))
                } else {
                    Some(None)
                }
            }
            _ => None,
        }
    }

    fn enter_join_left(
        &mut self,
        join: &LogicalPlan,
        _: &Option<SortColumns>,
    ) -> Option<Option<SortColumns>> {
        let join_on;
        if let LogicalPlan::Join { on, .. } = join {
            join_on = on;
        } else {
            panic!("expected join node");
        }
        Some(Some(SortColumns {
            sort_on: join_on.iter().map(|(l, _)| l.name.clone()).collect(),
            required: true,
        }))
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
        Some(Some(SortColumns {
            sort_on: join_on.iter().map(|(_, r)| r.name.clone()).collect(),
            required: true,
        }))
    }
}

struct ChooseIndex<'a> {
    next_index: usize,
    chosen_indices: &'a [IndexSnapshot],
    enable_topk: bool,
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
                assert!(
                    self.next_index < self.chosen_indices.len(),
                    "inconsistent state"
                );
                let table = &source
                    .as_any()
                    .downcast_ref::<CubeTableLogical>()
                    .unwrap()
                    .table;
                assert_eq!(
                    table.table.get_id(),
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
                )?);

                let index_schema = source.schema();
                assert_eq!(table_schema, index_schema);

                return Ok(ClusterSendNode {
                    input: Arc::new(p),
                    snapshots: vec![vec![snapshot]],
                }
                .into_plan());
            }
            _ => return Ok(p),
        }
    }
}

struct IndexCandidate {
    /// May contain for unmatched index.
    pub ordinary_index: Result<IndexSnapshot, DataFusionError>,
    pub partitioned_index: Option<IndexSnapshot>,
}

// Picks the index, but not partitions snapshots.
async fn pick_index(
    c: &IndexConstraints,
    schema: IdRow<Schema>,
    table: IdRow<Table>,
    indices: Vec<IdRow<Index>>,
) -> Result<IndexCandidate, DataFusionError> {
    let sort_on = c.sort_on.as_ref().map(|sc| (&sc.sort_on, sc.required));

    let mut indices = indices.into_iter();
    let default_index = indices.next().expect("no default index");
    let (index, mut partitioned_index, sort_on) = if let Some(projection_column_indices) =
        &c.projection
    {
        let projection_columns = CubeTable::project_to_table(&table, &projection_column_indices);
        let mut partitioned_index = None;
        let mut ordinary_index = None;
        let mut ordinary_score = usize::MAX;
        for i in indices {
            if let Some((join_on_columns, _)) = sort_on.as_ref() {
                // TODO: join_on_columns may be larger than sort_key_size of the index.
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
                    None => continue,
                    Some(c) => c,
                };
                let join_columns_indices =
                    CubeTable::project_to_index_positions(&join_columns_in_index, &i);

                let matches = join_columns_indices
                    .iter()
                    .enumerate()
                    .all(|(i, col_i)| Some(i) == *col_i);
                if !matches {
                    continue;
                }
            }
            let projected_index_positions =
                CubeTable::project_to_index_positions(&projection_columns, &i);
            let score = projected_index_positions
                .into_iter()
                .fold_options(0, |a, b| a + b);
            if let Some(score) = score {
                if i.get_row().multi_index_id().is_some() {
                    debug_assert!(partitioned_index.is_none());
                    partitioned_index = Some(i);
                    continue;
                }
                if score < ordinary_score {
                    ordinary_index = Some(i);
                    ordinary_score = score;
                }
            }
        }
        if let Some(index) = ordinary_index {
            (Ok(index), partitioned_index, sort_on)
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
                (err, partitioned_index, sort_on)
            } else {
                (Ok(default_index), partitioned_index, None)
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
    let create_snapshot = |index| {
        IndexSnapshot {
            index,
            partitions: Vec::new(), // filled with results of `pick_partitions` later.
            table_path: TablePath {
                table: table.clone(),
                schema: schema.clone(),
            },
            sort_on: sort_on.as_ref().map(|(cols, _)| (*cols).clone()),
        }
    };
    Ok(IndexCandidate {
        ordinary_index: index.map(create_snapshot),
        partitioned_index: partitioned_index.map(create_snapshot),
    })
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

#[derive(Debug, Clone)]
pub struct ClusterSendNode {
    pub input: Arc<LogicalPlan>,
    pub snapshots: Vec<Vec<IndexSnapshot>>,
}

impl ClusterSendNode {
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

    fn fmt_for_explain(&self, f: &mut Formatter<'a>) -> std::fmt::Result {
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
            // Code after 'match' will wrap `p` in ClusterSend.
            *input = send.input.clone();
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
                // Code after 'match' will wrap `p` in ClusterSend.
                *i = send.input.as_ref().clone();
            }
            snapshots = vec![union_snapshots];
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
            // Code after 'match' will wrap `p` in ClusterSend.
            *left = lsend.input.clone();
            *right = rsend.input.clone();
        }
        LogicalPlan::Window { .. } | LogicalPlan::CrossJoin { .. } => {
            return Err(DataFusionError::Internal(
                "unsupported operation".to_string(),
            ))
        }
    }

    Ok(ClusterSendNode {
        input: Arc::new(p),
        snapshots,
    }
    .into_plan())
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
        } else {
            Ok(None)
        }
    }
}

impl CubeExtensionPlanner {
    pub fn plan_cluster_send(
        &self,
        input: Arc<dyn ExecutionPlan>,
        snapshots: &Vec<Vec<IndexSnapshot>>,
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
            )))
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
           \n      Scan s.Customers, source: CubeTable(index: default:0:[]), fields: *"
        );

        // Should prefer a non-default index for joins.
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
                                  \n          Scan c1, source: CubeTable(index: default:0:[]:sort_on[customer_id]), fields: [customer_id, customer_name]\
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
                .push(Chunk::new(p as u64, 123, false).set_uploaded(true));
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
        );

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
                    ]
                ),
                (
                    "worker2".to_string(),
                    vec![
                        part(4, None, None),
                        part(9, None, None),
                        part(1, Some(25), Some(100)),
                        part(6, Some(25), Some(100)),
                        part(0, Some(25), Some(100)),
                        part(5, Some(25), Some(100)),
                    ]
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
        ));
        i.indices.push(
            Index::try_new(
                "by_city".to_string(),
                customers,
                put_first("customer_city", &customers_cols),
                1,
                None,
                None,
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
        ));
        i.indices.push(
            Index::try_new(
                "by_customer".to_string(),
                orders,
                put_first("order_customer", &orders_cols),
                2,
                None,
                None,
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
