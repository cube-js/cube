use crate::queryplanner::topk::util::{append_value, create_builder};
use crate::queryplanner::topk::SortColumn;
use crate::queryplanner::try_make_memory_data_source;
use crate::queryplanner::udfs::read_sketch;
use datafusion::arrow::array::{ArrayBuilder, ArrayRef, StringBuilder};
use datafusion::arrow::compute::{concat_batches, SortOptions};
use datafusion::arrow::datatypes::{i256, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::cube_ext;
use datafusion::error::DataFusionError;

use datafusion::execution::TaskContext;
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::{EquivalenceProperties, LexRequirement};
use datafusion::physical_plan::aggregates::{create_accumulators, AccumulatorItem, AggregateMode};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PhysicalExpr, PlanProperties, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use smallvec::smallvec;
use smallvec::SmallVec;
use std::any::Any;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopKAggregateFunction {
    Sum,
    Min,
    Max,
    Merge,
}

/// Selects which router-side merge implementation [`AggregateTopKExec`] runs. Both keep the same
/// streaming early-termination semantics (so router memory stays bounded); `V2` is a vectorized,
/// allocation-light rewrite of `V1`'s per-row machinery, selected by
/// `CUBESTORE_TOPK_STRATEGY=vectorized`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopKMergeVersion {
    V1,
    V2,
}

#[derive(Debug, Clone)]
pub struct AggregateTopKExec {
    pub limit: usize,
    pub key_len: usize,
    pub agg_expr: Vec<Arc<AggregateFunctionExpr>>,
    pub agg_descr: Vec<AggDescr>,
    pub order_by: Vec<SortColumn>,
    pub having: Option<Arc<dyn PhysicalExpr>>,
    /// Always an instance of ClusterSendExec or WorkerExec.
    pub cluster: Arc<dyn ExecutionPlan>,
    pub schema: SchemaRef,
    pub cache: PlanProperties,
    pub sort_requirement: LexRequirement,
    pub merge_version: TopKMergeVersion,
}

/// Third item is the neutral value for the corresponding aggregate function.
type AggDescr = (TopKAggregateFunction, SortOptions, ScalarValue);

impl AggregateTopKExec {
    pub fn new(
        limit: usize,
        key_len: usize,
        agg_expr: Vec<Arc<AggregateFunctionExpr>>,
        agg_fun: &[TopKAggregateFunction],
        order_by: Vec<SortColumn>,
        having: Option<Arc<dyn PhysicalExpr>>,
        cluster: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        // sort_requirement is passed in by topk_plan mostly for the sake of code deduplication
        sort_requirement: LexRequirement,
        merge_version: TopKMergeVersion,
    ) -> AggregateTopKExec {
        assert_eq!(schema.fields().len(), agg_expr.len() + key_len);
        assert_eq!(agg_fun.len(), agg_expr.len());
        let agg_descr = Self::compute_descr(&agg_expr, agg_fun, &order_by);

        // TODO upgrade DF: Ought to have real equivalence properties.  Though, pre-upgrade didn't.
        // Pre-upgrade output_hints comment:  This is a top-level plan, so ordering properties probably don't matter.
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        AggregateTopKExec {
            limit,
            key_len,
            agg_expr,
            agg_descr,
            order_by,
            having,
            cluster,
            schema,
            cache,
            sort_requirement,
            merge_version,
        }
    }

    fn compute_descr(
        agg_expr: &[Arc<AggregateFunctionExpr>],
        agg_fun: &[TopKAggregateFunction],
        order_by: &[SortColumn],
    ) -> Vec<AggDescr> {
        let mut agg_descr = Vec::with_capacity(agg_expr.len());
        for i in 0..agg_expr.len() {
            agg_descr.push((
                agg_fun[i].clone(),
                SortOptions::default(),
                ScalarValue::Int64(None),
            ));
        }
        for o in order_by {
            agg_descr[o.agg_index].1 = o.sort_options();
        }
        agg_descr
    }

    #[cfg(test)]
    fn change_order(&mut self, order_by: Vec<SortColumn>) {
        self.agg_descr = Self::compute_descr(
            &self.agg_expr,
            &self
                .agg_descr
                .iter()
                .map(|(f, _, _)| f.clone())
                .collect_vec(),
            &order_by,
        );
        self.order_by = order_by;
    }
}

impl DisplayAs for AggregateTopKExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateTopKExec")
    }
}

impl ExecutionPlan for AggregateTopKExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::static_name()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.cluster]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 1);
        let cluster = children.into_iter().next().unwrap();
        Ok(Arc::new(AggregateTopKExec {
            limit: self.limit,
            key_len: self.key_len,
            agg_expr: self.agg_expr.clone(),
            agg_descr: self.agg_descr.clone(),
            order_by: self.order_by.clone(),
            having: self.having.clone(),
            cluster,
            schema: self.schema.clone(),
            cache: self.cache.clone(),
            sort_requirement: self.sort_requirement.clone(),
            merge_version: self.merge_version,
        }))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    // TODO upgrade DF: Probably should include output ordering in the PlanProperties.

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        vec![Some(self.sort_requirement.clone())]
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        assert_eq!(partition, 0);
        let plan: AggregateTopKExec = self.clone();
        let schema = plan.schema();

        let fut = async move {
            let nodes = plan.cluster.output_partitioning().partition_count();
            let mut tasks = Vec::with_capacity(nodes);
            for p in 0..nodes {
                let cluster = plan.cluster.clone();
                let context = context.clone();
                tasks.push(cube_ext::spawn(async move {
                    // fuse the streams to simplify further code.
                    cluster.execute(p, context).map(|s| (s.schema(), s.fuse()))
                }));
            }
            let mut streams = Vec::with_capacity(nodes);
            for t in tasks {
                streams.push(t.await.map_err(|_| {
                    DataFusionError::Internal("could not join threads".to_string())
                })??);
            }

            match plan.merge_version {
                TopKMergeVersion::V1 => {
                    let mut buffer = TopKBuffer::default();
                    let state = TopKState::new(
                        plan.limit,
                        nodes,
                        plan.key_len,
                        &plan.order_by,
                        &plan.having,
                        &plan.agg_expr,
                        &plan.agg_descr,
                        &mut buffer,
                        &context,
                        plan.schema(),
                    )?;
                    run_merge_loop(state, &mut streams, nodes).await
                }
                TopKMergeVersion::V2 => {
                    let state = TopKStateV2::new(
                        plan.limit,
                        nodes,
                        plan.key_len,
                        &plan.order_by,
                        &plan.having,
                        &plan.agg_descr,
                        &context,
                        plan.schema(),
                    )?;
                    run_merge_loop(state, &mut streams, nodes).await
                }
            }
        };

        let stream = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

// Mutex is to provide interior mutability inside async function, no actual waiting ever happens.
// TODO: remove mutex with careful use of unsafe.
type TopKBuffer = std::sync::Mutex<Vec<Group>>;

// TODO upgrade DF: This was a SmallVec<[AccumulatorItem; 2]>.
type AccumulatorSet = Vec<AccumulatorItem>;
// TODO upgrade DF: Drop the GroupByScalar nomenclature.
type GroupByScalar = ScalarValue;

struct TopKState<'a> {
    limit: usize,
    buffer: &'a TopKBuffer,
    key_len: usize,
    order_by: &'a [SortColumn],
    having: &'a Option<Arc<dyn PhysicalExpr>>,
    agg_expr: &'a Vec<Arc<AggregateFunctionExpr>>,
    agg_descr: &'a [AggDescr],
    context: &'a Arc<TaskContext>,
    /// Holds the maximum value seen in each node, used to estimate unseen scores.
    node_estimates: Vec<AccumulatorSet>,
    finished_nodes: Vec<bool>,
    sorted: BTreeSet<SortKey<'a>>,
    groups: HashSet<GroupKey<'a>>,
    /// Final output.
    top: Vec<usize>,
    schema: SchemaRef,
    /// Result Batch
    result: RecordBatch,
}

struct Group {
    pub group_key: SmallVec<[GroupByScalar; 2]>,
    /// The real value based on all nodes seen so far.
    pub accumulators: AccumulatorSet,
    /// The estimated value. Provides correct answer after the group was visited in all nodes.
    pub estimates: AccumulatorSet,
    /// Tracks nodes that have already reported this group.
    pub nodes: Vec<bool>,
}

impl Group {
    fn estimate(&self) -> Result<SmallVec<[ScalarValue; 1]>, DataFusionError> {
        self.estimates.iter().map(|e| e.peek_evaluate()).collect()
    }

    fn estimate_correct(&self) -> bool {
        self.nodes.iter().all(|b| *b)
    }
}

struct SortKey<'a> {
    order_by: &'a [SortColumn],
    estimate: SmallVec<[ScalarValue; 1]>,
    index: usize,
    /// Informative, not used in the [cmp] implementation.
    estimate_correct: bool,
}

impl PartialEq for SortKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for SortKey<'_> {}
impl PartialOrd for SortKey<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKey<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.index == other.index {
            return Ordering::Equal;
        }
        for sc in self.order_by {
            // Assuming `self` and `other` point to the same data.
            let o = cmp_same_types(
                &self.estimate[sc.agg_index],
                &other.estimate[sc.agg_index],
                sc.nulls_first,
                sc.asc,
            );
            if o != Ordering::Equal {
                return o;
            }
        }
        // Distinguish items with the same scores for removals/updates.
        self.index.cmp(&other.index)
    }
}

struct GroupKey<'a> {
    data: &'a TopKBuffer,
    index: usize,
}

impl PartialEq for GroupKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        let data = self.data.lock().unwrap();
        data[self.index].group_key == data[other.index].group_key
    }
}
impl Eq for GroupKey<'_> {}
impl Hash for GroupKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.lock().unwrap()[self.index].group_key.hash(state)
    }
}

impl TopKState<'_> {
    pub fn new<'a>(
        limit: usize,
        num_nodes: usize,
        key_len: usize,
        order_by: &'a [SortColumn],
        having: &'a Option<Arc<dyn PhysicalExpr>>,
        agg_expr: &'a Vec<Arc<AggregateFunctionExpr>>,
        agg_descr: &'a [AggDescr],
        buffer: &'a mut TopKBuffer,
        context: &'a Arc<TaskContext>,
        schema: SchemaRef,
    ) -> Result<TopKState<'a>, DataFusionError> {
        Ok(TopKState {
            limit,
            buffer,
            key_len,
            order_by,
            having,
            agg_expr,
            agg_descr,
            context,
            finished_nodes: vec![false; num_nodes],
            // initialized with the first record batches, see [update].
            node_estimates: Vec::with_capacity(num_nodes),
            sorted: BTreeSet::new(),
            groups: HashSet::new(),
            top: Vec::new(),
            schema: schema.clone(),
            result: RecordBatch::new_empty(schema),
        })
    }

    /// Sets `wanted_nodes[i]` iff we need to scan the node `i` to make progress on top candidate.
    pub fn populate_wanted_nodes(&self, wanted_nodes: &mut Vec<bool>) {
        let candidate = self.sorted.first();
        if candidate.is_none() {
            for i in 0..wanted_nodes.len() {
                wanted_nodes[i] = true;
            }
            return;
        }

        let candidate = candidate.unwrap();
        let buf = self.buffer.lock().unwrap();
        let candidate_nodes = &buf[candidate.index].nodes;
        assert_eq!(candidate_nodes.len(), wanted_nodes.len());
        for i in 0..wanted_nodes.len() {
            wanted_nodes[i] = !candidate_nodes[i];
        }
    }

    pub async fn update(
        &mut self,
        batches: &mut [Option<RecordBatch>],
    ) -> Result<bool, DataFusionError> {
        let num_nodes = batches.len();
        assert_eq!(num_nodes, self.finished_nodes.len());

        // We need correct estimates for further processing.
        if self.node_estimates.is_empty() {
            for node in 0..num_nodes {
                let mut estimates = create_accumulators(self.agg_expr)?;
                if let Some(batch) = &batches[node] {
                    assert_ne!(batch.num_rows(), 0, "empty batch passed to `update`");
                    Self::update_node_estimates(
                        self.key_len,
                        self.agg_descr,
                        &mut estimates,
                        batch.columns(),
                        0,
                    )?;
                }
                self.node_estimates.push(estimates);
            }
        }

        for node in 0..num_nodes {
            if batches[node].is_none() && !self.finished_nodes[node] {
                self.finished_nodes[node] = true;
            }
        }

        let mut num_rows = batches
            .iter()
            .map(|b| b.as_ref().map(|b| b.num_rows()).unwrap_or(0))
            .collect_vec();
        num_rows.sort_unstable();

        let mut row_i = 0;
        let mut pop_top_counter = self.limit;
        for row_limit in num_rows {
            while row_i < row_limit {
                // row_i updated at the end of the loop.
                for node in 0..num_nodes {
                    let batch;
                    if let Some(b) = &batches[node] {
                        batch = b;
                    } else {
                        continue;
                    }

                    let mut key = smallvec![GroupByScalar::Int8(Some(0)); self.key_len];
                    create_group_by_values(&batch.columns()[0..self.key_len], row_i, &mut key)?;
                    let temp_index = self.buffer.lock().unwrap().len();
                    self.buffer.lock().unwrap().push(Group {
                        group_key: key,
                        accumulators: AccumulatorSet::new(),
                        estimates: AccumulatorSet::new(),
                        nodes: Vec::new(),
                    });

                    let existing = self
                        .groups
                        .get_or_insert(GroupKey {
                            data: self.buffer,
                            index: temp_index,
                        })
                        .index;
                    if existing != temp_index {
                        // Found existing, remove the temporary value from the buffer.
                        let mut data = self.buffer.lock().unwrap();
                        data.pop();

                        // Prepare to update the estimates, will re-add when done.
                        let estimate = data[existing].estimate()?;
                        self.sorted.remove(&SortKey {
                            order_by: self.order_by,
                            estimate,
                            index: existing,
                            // Does not affect comparison.
                            estimate_correct: false,
                        });
                    } else {
                        let mut data = self.buffer.lock().unwrap();
                        let g = &mut data[temp_index];
                        g.accumulators = create_accumulators(self.agg_expr).unwrap();
                        g.estimates = create_accumulators(self.agg_expr).unwrap();
                        g.nodes = self.finished_nodes.clone();
                    }

                    // Update the group.
                    let key;
                    {
                        let mut data = self.buffer.lock().unwrap();
                        let group = &mut data[existing];
                        group.nodes[node] = true;
                        for i in 0..group.accumulators.len() {
                            group.accumulators[i].update_batch(&vec![batch
                                .column(self.key_len + i)
                                .slice(row_i, 1)])?;
                        }
                        self.update_group_estimates(group)?;
                        key = SortKey {
                            order_by: self.order_by,
                            estimate: group.estimate()?,
                            estimate_correct: group.estimate_correct(),
                            index: existing,
                        }
                    }
                    let inserted = self.sorted.insert(key);
                    assert!(inserted);

                    Self::update_node_estimates(
                        self.key_len,
                        self.agg_descr,
                        &mut self.node_estimates[node],
                        batch.columns(),
                        row_i,
                    )?;
                }

                row_i += 1;

                pop_top_counter -= 1;
                if pop_top_counter == 0 {
                    if self.pop_top_elements().await? {
                        return Ok(true);
                    }
                    pop_top_counter = self.limit;
                }
            }

            for node in 0..num_nodes {
                if let Some(b) = &batches[node] {
                    if b.num_rows() == row_limit {
                        batches[node] = None;
                    }
                }
            }
        }

        self.pop_top_elements().await
    }

    /// Moves groups with known top scores into the [top].
    /// Returns true iff [top] contains the correct answer to the top-k query.
    async fn pop_top_elements(&mut self) -> Result<bool, DataFusionError> {
        while self.result.num_rows() < self.limit && !self.sorted.is_empty() {
            let mut candidate = self.sorted.pop_first().unwrap();
            while !candidate.estimate_correct {
                // The estimate might be stale. Update and re-insert.
                let updated;
                {
                    let mut data = self.buffer.lock().unwrap();
                    self.update_group_estimates(&mut data[candidate.index])?;
                    updated = SortKey {
                        order_by: self.order_by,
                        estimate: data[candidate.index].estimate()?,
                        estimate_correct: data[candidate.index].estimate_correct(),
                        index: candidate.index,
                    };
                }
                self.sorted.insert(updated);

                let next_candidate = self.sorted.first().unwrap();
                if candidate.index == next_candidate.index && !next_candidate.estimate_correct {
                    // Same group with top estimate, need to wait until we see it on all nodes.
                    return Ok(false);
                } else {
                    candidate = self.sorted.pop_first().unwrap();
                }
            }
            self.top.push(candidate.index);
            if self.top.len() == self.limit {
                self.push_top_to_result().await?;
            }
        }

        return Ok(self.result.num_rows() == self.limit || self.finished_nodes.iter().all(|f| *f));
    }

    ///Push groups from [top] into [result] butch, applying having filter if required and clears
    ///[top] vector
    async fn push_top_to_result(&mut self) -> Result<(), DataFusionError> {
        if self.top.is_empty() {
            return Ok(());
        }

        let mut key_columns = Vec::with_capacity(self.key_len);
        let mut value_columns = Vec::with_capacity(self.agg_expr.len());

        let columns = {
            let mut data = self.buffer.lock().unwrap();
            for group in self.top.iter() {
                let g = &mut data[*group];
                write_group_result_row(
                    AggregateMode::Final,
                    &g.group_key,
                    &mut g.accumulators,
                    &self.schema.fields()[..self.key_len],
                    &mut key_columns,
                    &mut value_columns,
                )?
            }

            key_columns
                .into_iter()
                .chain(value_columns)
                .map(|mut c| c.finish())
                .collect_vec()
        };
        if !columns.is_empty() {
            let new_batch = RecordBatch::try_new(self.schema.clone(), columns)?;
            let new_batch = if let Some(having) = self.having {
                let schema = new_batch.schema();
                let filter_exec = Arc::new(FilterExec::try_new(
                    having.clone(),
                    try_make_memory_data_source(&vec![vec![new_batch]], schema.clone(), None)?,
                )?);
                let batches_stream =
                    GlobalLimitExec::new(filter_exec, 0, Some(self.limit - self.result.num_rows()))
                        .execute(0, self.context.clone())?;

                let batches = collect(batches_stream).await?;
                concat_batches(&schema, &batches)?
            } else {
                new_batch
            };
            let mut tmp = RecordBatch::new_empty(self.schema.clone());
            std::mem::swap(&mut self.result, &mut tmp);
            self.result = concat_batches(&self.schema, &vec![tmp, new_batch])?;
        }
        self.top.clear();
        Ok(())
    }

    async fn finish(mut self) -> Result<RecordBatch, DataFusionError> {
        log::trace!(
            "aggregate top-k processed {} groups to return {} rows",
            self.result.num_rows() + self.top.len() + self.sorted.len(),
            self.limit
        );
        self.push_top_to_result().await?;

        Ok(self.result)
    }

    fn merge_single_state(
        acc: &mut dyn Accumulator,
        state: Vec<ScalarValue>,
    ) -> Result<(), DataFusionError> {
        // TODO upgrade DF: This allocates and produces a lot of fluff here.
        let single_row_columns = state
            .into_iter()
            .map(|scalar| scalar.to_array())
            .collect::<Result<Vec<_>, _>>()?;
        acc.merge_batch(single_row_columns.as_slice())
    }

    /// Returns true iff the estimate matches the correct score.
    fn update_group_estimates(&self, group: &mut Group) -> Result<(), DataFusionError> {
        for i in 0..group.estimates.len() {
            group.estimates[i].reset()?;
            Self::merge_single_state(
                group.estimates[i].as_mut(),
                group.accumulators[i].peek_state()?,
            )?;
            // Node estimate might contain a neutral value (e.g. '0' for sum), but we must avoid
            // giving invalid estimates for NULL values.
            let use_node_estimates =
                !self.agg_descr[i].1.nulls_first || !group.estimates[i].peek_evaluate()?.is_null();
            for node in 0..group.nodes.len() {
                if !group.nodes[node] {
                    if self.finished_nodes[node] {
                        group.nodes[node] = true;
                        continue;
                    }
                    if use_node_estimates {
                        Self::merge_single_state(
                            group.estimates[i].as_mut(),
                            self.node_estimates[node][i].peek_state()?,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn update_node_estimates(
        key_len: usize,
        agg_descr: &[AggDescr],
        estimates: &mut AccumulatorSet,
        columns: &[ArrayRef],
        row_i: usize,
    ) -> Result<(), DataFusionError> {
        for (i, acc) in estimates.iter_mut().enumerate() {
            acc.reset()?;

            // evaluate() gives us a scalar value of the required type.
            let mut neutral = acc.peek_evaluate()?;
            to_neutral_value(&mut neutral, &agg_descr[i].0);

            acc.update_batch(&vec![columns[key_len + i].slice(row_i, 1)])?;

            // Neutral value (i.e. missing on the node) might be the right estimate.
            // E.g. `0` is better than `-10` on `SUM(x) ORDER BY SUM(x) DESC`.
            // We have to provide correct estimates.
            let o = cmp_same_types(
                &neutral,
                &acc.peek_evaluate()?,
                agg_descr[i].1.nulls_first,
                !agg_descr[i].1.descending,
            );
            if o < Ordering::Equal {
                acc.reset()?;
            }
        }
        Ok(())
    }
}

fn cmp_same_types(l: &ScalarValue, r: &ScalarValue, nulls_first: bool, asc: bool) -> Ordering {
    match (l.is_null(), r.is_null()) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            return if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {} // fallthrough.
    }

    let o = match (l, r) {
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => l.cmp(r),
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => l.total_cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l.total_cmp(r),
        (
            ScalarValue::Decimal128(Some(l), lprecision, lscale),
            ScalarValue::Decimal128(Some(r), rprecision, rscale),
        ) => {
            assert_eq!(lprecision, rprecision);
            assert_eq!(lscale, rscale);
            l.cmp(r)
        }
        (
            ScalarValue::Decimal256(Some(l), lprecision, lscale),
            ScalarValue::Decimal256(Some(r), rprecision, rscale),
        ) => {
            assert_eq!(lprecision, rprecision);
            assert_eq!(lscale, rscale);
            l.cmp(r)
        }
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l.cmp(r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(r))) => l.cmp(r),
        (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(r))) => l.cmp(r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => l.cmp(r),
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => l.cmp(r),
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => l.cmp(r),
        (ScalarValue::LargeUtf8(Some(l)), ScalarValue::LargeUtf8(Some(r))) => l.cmp(r),
        (ScalarValue::Binary(Some(l)), ScalarValue::Binary(Some(r))) => {
            let l_card = if l.len() == 0 {
                0
            } else {
                read_sketch(l).unwrap().cardinality()
            };
            let r_card = if r.len() == 0 {
                0
            } else {
                read_sketch(r).unwrap().cardinality()
            };
            l_card.cmp(&r_card)
        }
        (ScalarValue::LargeBinary(Some(l)), ScalarValue::LargeBinary(Some(r))) => l.cmp(r),
        (ScalarValue::Date32(Some(l)), ScalarValue::Date32(Some(r))) => l.cmp(r),
        (ScalarValue::Date64(Some(l)), ScalarValue::Date64(Some(r))) => l.cmp(r),
        (
            ScalarValue::TimestampSecond(Some(l), ltz),
            ScalarValue::TimestampSecond(Some(r), rtz),
        ) => {
            assert_eq!(ltz, rtz);
            l.cmp(r)
        }
        (
            ScalarValue::TimestampMillisecond(Some(l), ltz),
            ScalarValue::TimestampMillisecond(Some(r), rtz),
        ) => {
            assert_eq!(ltz, rtz);
            l.cmp(r)
        }
        (
            ScalarValue::TimestampMicrosecond(Some(l), ltz),
            ScalarValue::TimestampMicrosecond(Some(r), rtz),
        ) => {
            assert_eq!(ltz, rtz);
            l.cmp(r)
        }
        (
            ScalarValue::TimestampNanosecond(Some(l), ltz),
            ScalarValue::TimestampNanosecond(Some(r), rtz),
        ) => {
            assert_eq!(ltz, rtz);
            l.cmp(r)
        }
        (ScalarValue::IntervalYearMonth(Some(l)), ScalarValue::IntervalYearMonth(Some(r))) => {
            l.cmp(r)
        }
        (ScalarValue::IntervalDayTime(Some(l)), ScalarValue::IntervalDayTime(Some(r))) => l.cmp(r),
        (ScalarValue::List(_), ScalarValue::List(_)) => {
            panic!("list as accumulator result is not supported")
        }
        (l, r) => panic!(
            "unhandled types in comparison: {} and {}",
            l.data_type(),
            r.data_type()
        ),
    };
    if asc {
        o
    } else {
        o.reverse()
    }
}

fn to_neutral_value(s: &mut ScalarValue, f: &TopKAggregateFunction) {
    match f {
        TopKAggregateFunction::Sum => to_zero(s),
        TopKAggregateFunction::Min => to_max_value(s),
        TopKAggregateFunction::Max => to_min_value(s),
        TopKAggregateFunction::Merge => to_empty_sketch(s),
    }
}

fn to_zero(s: &mut ScalarValue) {
    match s {
        ScalarValue::Boolean(v) => *v = Some(false),
        // Note that -0.0, not 0.0, is the neutral value for floats, at least in IEEE 754.
        ScalarValue::Float32(v) => *v = Some(-0.0),
        ScalarValue::Float64(v) => *v = Some(-0.0),
        ScalarValue::Decimal128(v, _, _) => *v = Some(0),
        ScalarValue::Decimal256(v, _, _) => *v = Some(i256::ZERO),
        ScalarValue::Int8(v) => *v = Some(0),
        ScalarValue::Int16(v) => *v = Some(0),
        ScalarValue::Int32(v) => *v = Some(0),
        ScalarValue::Int64(v) => *v = Some(0),
        ScalarValue::UInt8(v) => *v = Some(0),
        ScalarValue::UInt16(v) => *v = Some(0),
        ScalarValue::UInt32(v) => *v = Some(0),
        ScalarValue::UInt64(v) => *v = Some(0),
        // TODO: dates and times?
        _ => panic!("unsupported data type"),
    }
}

fn to_max_value(s: &mut ScalarValue) {
    match s {
        ScalarValue::Boolean(v) => *v = Some(true),
        ScalarValue::Float32(v) => *v = Some(f32::INFINITY),
        ScalarValue::Float64(v) => *v = Some(f64::INFINITY),
        // TODO upgrade DF: This is possibly wrong, maybe carries over an Int64Decimal bug.
        ScalarValue::Decimal128(v, _, _) => *v = Some(i128::MAX),
        ScalarValue::Decimal256(v, _, _) => *v = Some(i256::MAX),
        ScalarValue::Int8(v) => *v = Some(i8::MAX),
        ScalarValue::Int16(v) => *v = Some(i16::MAX),
        ScalarValue::Int32(v) => *v = Some(i32::MAX),
        ScalarValue::Int64(v) => *v = Some(i64::MAX),
        ScalarValue::UInt8(v) => *v = Some(u8::MAX),
        ScalarValue::UInt16(v) => *v = Some(u16::MAX),
        ScalarValue::UInt32(v) => *v = Some(u32::MAX),
        ScalarValue::UInt64(v) => *v = Some(u64::MAX),
        // TODO: dates and times?
        _ => panic!("unsupported data type"),
    }
}

fn to_min_value(s: &mut ScalarValue) {
    match s {
        ScalarValue::Boolean(v) => *v = Some(false),
        ScalarValue::Float32(v) => *v = Some(f32::NEG_INFINITY),
        ScalarValue::Float64(v) => *v = Some(f64::NEG_INFINITY),
        // TODO upgrade DF: This is possibly wrong, maybe carries over an Int64Decimal bug.
        ScalarValue::Decimal128(v, _, _) => *v = Some(i128::MIN),
        ScalarValue::Decimal256(v, _, _) => *v = Some(i256::MIN),
        ScalarValue::Int8(v) => *v = Some(i8::MIN),
        ScalarValue::Int16(v) => *v = Some(i16::MIN),
        ScalarValue::Int32(v) => *v = Some(i32::MIN),
        ScalarValue::Int64(v) => *v = Some(i64::MIN),
        ScalarValue::UInt8(v) => *v = Some(u8::MIN),
        ScalarValue::UInt16(v) => *v = Some(u16::MIN),
        ScalarValue::UInt32(v) => *v = Some(u32::MIN),
        ScalarValue::UInt64(v) => *v = Some(u64::MIN),
        // TODO: dates and times?
        _ => panic!("unsupported data type"),
    }
}

fn to_empty_sketch(s: &mut ScalarValue) {
    match s {
        ScalarValue::Binary(v) => *v = Some(Vec::new()),
        _ => panic!("unsupported data type"),
    }
}

fn create_group_by_value(col: &ArrayRef, row: usize) -> Result<GroupByScalar, DataFusionError> {
    ScalarValue::try_from_array(col, row)
}

fn create_group_by_values(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut SmallVec<[GroupByScalar; 2]>,
) -> Result<(), DataFusionError> {
    for (i, col) in group_by_keys.iter().enumerate() {
        vec[i] = create_group_by_value(col, row)?;
    }
    Ok(())
}

fn write_group_result_row(
    mode: AggregateMode,
    group_by_values: &[GroupByScalar],
    accumulator_set: &mut AccumulatorSet,
    _key_fields: &[Arc<Field>],
    key_columns: &mut Vec<Box<dyn ArrayBuilder>>,
    value_columns: &mut Vec<Box<dyn ArrayBuilder>>,
) -> Result<(), DataFusionError> {
    let add_key_columns = key_columns.is_empty();
    for i in 0..group_by_values.len() {
        match &group_by_values[i] {
            // Optimization to avoid allocation on conversion to ScalarValue.
            GroupByScalar::Utf8(Some(str)) => {
                // TODO: Note StringArrayBuilder exists in DF; it might be faster.
                if add_key_columns {
                    key_columns.push(Box::new(StringBuilder::with_capacity(0, 0)));
                }
                key_columns[i]
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_value(str);
            }
            v => {
                let scalar = v;
                if add_key_columns {
                    key_columns.push(create_builder(scalar));
                }
                append_value(&mut *key_columns[i], &scalar)?;
            }
        }
    }
    finalize_aggregation_into(accumulator_set, &mode, value_columns)
}

/// adds aggregation results into columns, creating the required builders when necessary.
/// final value (mode = Final) or states (mode = Partial)
fn finalize_aggregation_into(
    accumulators: &mut AccumulatorSet,
    mode: &AggregateMode,
    columns: &mut Vec<Box<dyn ArrayBuilder>>,
) -> Result<(), DataFusionError> {
    let add_columns = columns.is_empty();
    match mode {
        AggregateMode::Partial => {
            let mut col_i = 0;
            for a in accumulators {
                // build the vector of states
                for v in a.peek_state()? {
                    if add_columns {
                        columns.push(create_builder(&v));
                        assert_eq!(col_i + 1, columns.len());
                    }
                    append_value(&mut *columns[col_i], &v)?;
                    col_i += 1;
                }
            }
        }
        AggregateMode::Final
        | AggregateMode::FinalPartitioned
        | AggregateMode::Single
        | AggregateMode::SinglePartitioned => {
            for i in 0..accumulators.len() {
                // merge the state to the final value
                let v = accumulators[i].peek_evaluate()?;
                if add_columns {
                    columns.push(create_builder(&v));
                    assert_eq!(i + 1, columns.len());
                }
                append_value(&mut *columns[i], &v)?;
            }
        }
    }
    Ok(())
}

/// Shared streaming driver for both merge implementations. Pulls one batch per "wanted" node each
/// round, feeds them to the merge state, and stops as soon as the state reports the top-k is final.
/// Keeping this loop common guarantees V1 and V2 share the exact early-termination cadence.
trait TopKMerge {
    async fn merge_update(
        &mut self,
        batches: &mut [Option<RecordBatch>],
    ) -> Result<bool, DataFusionError>;
    fn merge_wanted_nodes(&self, wanted_nodes: &mut Vec<bool>);
    async fn merge_finish(self) -> Result<RecordBatch, DataFusionError>;
}

async fn run_merge_loop<S: TopKMerge>(
    mut state: S,
    streams: &mut [(SchemaRef, futures::stream::Fuse<SendableRecordBatchStream>)],
    nodes: usize,
) -> Result<RecordBatch, DataFusionError> {
    let mut wanted_nodes = vec![true; nodes];
    let mut batches = Vec::with_capacity(nodes);
    loop {
        assert!(batches.is_empty());
        for i in 0..nodes {
            let (schema, s) = &mut streams[i];
            let batch = if wanted_nodes[i] {
                next_non_empty(s).await?
            } else {
                Some(RecordBatch::new_empty(schema.clone()))
            };
            batches.push(batch);
        }

        if state.merge_update(&mut batches).await? {
            batches.clear();
            break;
        }
        state.merge_wanted_nodes(&mut wanted_nodes);
        batches.clear();
    }
    state.merge_finish().await
}

impl TopKMerge for TopKState<'_> {
    async fn merge_update(
        &mut self,
        batches: &mut [Option<RecordBatch>],
    ) -> Result<bool, DataFusionError> {
        self.update(batches).await
    }
    fn merge_wanted_nodes(&self, wanted_nodes: &mut Vec<bool>) {
        self.populate_wanted_nodes(wanted_nodes)
    }
    async fn merge_finish(self) -> Result<RecordBatch, DataFusionError> {
        self.finish().await
    }
}

impl TopKMerge for TopKStateV2<'_> {
    async fn merge_update(
        &mut self,
        batches: &mut [Option<RecordBatch>],
    ) -> Result<bool, DataFusionError> {
        self.update(batches).await
    }
    fn merge_wanted_nodes(&self, wanted_nodes: &mut Vec<bool>) {
        self.populate_wanted_nodes(wanted_nodes)
    }
    async fn merge_finish(self) -> Result<RecordBatch, DataFusionError> {
        self.finish().await
    }
}

/// Vectorized router-side top-k merge (`CUBESTORE_TOPK_STRATEGY=vectorized`). Same NRA early-termination as
/// [`TopKState`] -- sorted worker streams plus per-node frontier upper bounds decide how far to pull
/// and when to stop, so router memory stays bounded. The difference is purely the hot path: group
/// keys go through an arrow [`RowConverter`] (interned per batch, no per-row `ScalarValue` key
/// vectors), per-group aggregate state is plain `ScalarValue` arithmetic (no `Mutex`, no per-row
/// 1-element `update_batch`/`to_array`), and estimates are typed combines instead of accumulator
/// state round-trips. Only the monotone primitive aggregates (Sum/Min/Max) are handled here; HLL
/// `Merge` falls back to V1 at plan time.
struct TopKStateV2<'a> {
    limit: usize,
    key_len: usize,
    num_nodes: usize,
    order_by: &'a [SortColumn],
    having: &'a Option<Arc<dyn PhysicalExpr>>,
    agg_descr: &'a [AggDescr],
    context: &'a Arc<TaskContext>,
    schema: SchemaRef,

    row_converter: RowConverter,
    /// Distinct group keys; group `gid` is `key_store.row(gid)`.
    key_store: Rows,
    /// Maps a key's hash to the group ids carrying it. Storing hash -> gids (rather than
    /// `OwnedRow -> gid`) keeps lookups allocation-free: the probe row is borrowed from
    /// `scratch_rows` and equality is checked against `key_store.row(gid)`.
    group_index: HashMap<u64, SmallVec<[usize; 1]>>,
    /// Reused per-batch buffer for the incoming keys.
    scratch_rows: Rows,

    /// `acc[gid][i]` -- accumulated value of aggregate `i` across the nodes seen so far.
    acc: Vec<SmallVec<[ScalarValue; 1]>>,
    /// `cur_estimate[gid]` mirrors the estimate currently stored for `gid` in `sorted` (so it can be
    /// removed before being updated).
    cur_estimate: Vec<SmallVec<[ScalarValue; 1]>>,
    /// `nodes_seen[gid][node]` -- whether `node` has reported this group.
    nodes_seen: Vec<Vec<bool>>,
    /// `node_frontier[node][i]` -- upper bound (clamped to the neutral value) on aggregate `i` for any
    /// group not yet seen on `node`. `None` only before the node's first batch (then the node is
    /// either about to be pulled or already finished).
    node_frontier: Vec<Option<SmallVec<[ScalarValue; 1]>>>,
    finished_nodes: Vec<bool>,

    sorted: BTreeSet<SortKey<'a>>,
    top: Vec<usize>,
    result: RecordBatch,
}

impl<'a> TopKStateV2<'a> {
    fn new(
        limit: usize,
        num_nodes: usize,
        key_len: usize,
        order_by: &'a [SortColumn],
        having: &'a Option<Arc<dyn PhysicalExpr>>,
        agg_descr: &'a [AggDescr],
        context: &'a Arc<TaskContext>,
        schema: SchemaRef,
    ) -> Result<TopKStateV2<'a>, DataFusionError> {
        let key_fields = schema.fields()[0..key_len]
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        let row_converter = RowConverter::new(key_fields)?;
        let key_store = row_converter.empty_rows(0, 0);
        let scratch_rows = row_converter.empty_rows(0, 0);
        Ok(TopKStateV2 {
            limit,
            key_len,
            num_nodes,
            order_by,
            having,
            agg_descr,
            context,
            schema: schema.clone(),
            row_converter,
            key_store,
            group_index: HashMap::new(),
            scratch_rows,
            acc: Vec::new(),
            cur_estimate: Vec::new(),
            nodes_seen: Vec::new(),
            node_frontier: vec![None; num_nodes],
            finished_nodes: vec![false; num_nodes],
            sorted: BTreeSet::new(),
            top: Vec::new(),
            result: RecordBatch::new_empty(schema),
        })
    }

    async fn update(
        &mut self,
        batches: &mut [Option<RecordBatch>],
    ) -> Result<bool, DataFusionError> {
        assert_eq!(batches.len(), self.num_nodes);
        // Refresh frontiers (and finished flags) for the whole round *before* merging any rows, so
        // every group estimate computed this round sees an up-to-date upper bound for each node --
        // including nodes processed later in the loop. Without this, a group seen only on an
        // already-processed node would be under-estimated against a node whose frontier was still
        // unset, and could be dropped from the top-k.
        for node in 0..self.num_nodes {
            match &batches[node] {
                None => self.finished_nodes[node] = true,
                Some(b) if b.num_rows() == 0 => {} // not pulled this round: keep previous frontier
                Some(b) => self.update_frontier(node, b)?,
            }
        }
        for node in 0..self.num_nodes {
            if let Some(b) = &batches[node] {
                if b.num_rows() > 0 {
                    self.process_batch_rows(node, b)?;
                }
            }
        }
        self.pop_top_elements().await
    }

    /// Sets `node`'s per-aggregate frontier from the last (smallest, since streams are sorted by the
    /// order-by aggregate) row of its current batch -- the upper bound on any group not yet seen on
    /// this node.
    fn update_frontier(&mut self, node: usize, b: &RecordBatch) -> Result<(), DataFusionError> {
        let n_agg = self.agg_descr.len();
        let value_cols = &b.columns()[self.key_len..];
        let last = b.num_rows() - 1;
        let mut frontier = SmallVec::with_capacity(n_agg);
        for i in 0..n_agg {
            let mut v = ScalarValue::try_from_array(&value_cols[i], last)?;
            clamp_to_neutral_bound(&mut v, &self.agg_descr[i]);
            frontier.push(v);
        }
        self.node_frontier[node] = Some(frontier);
        Ok(())
    }

    fn process_batch_rows(&mut self, node: usize, b: &RecordBatch) -> Result<(), DataFusionError> {
        let n_agg = self.agg_descr.len();
        let n = b.num_rows();
        let value_cols = &b.columns()[self.key_len..];

        self.scratch_rows.clear();
        self.row_converter
            .append(&mut self.scratch_rows, &b.columns()[0..self.key_len])?;

        for r in 0..n {
            let probe = self.scratch_rows.row(r);
            let hash = {
                let mut h = DefaultHasher::new();
                probe.hash(&mut h);
                h.finish()
            };
            let existing = self.group_index.get(&hash).and_then(|bucket| {
                bucket
                    .iter()
                    .copied()
                    .find(|&g| self.key_store.row(g) == probe)
            });
            let (gid, is_new) = match existing {
                Some(g) => {
                    let old = self.cur_estimate[g].clone();
                    self.sorted.remove(&SortKey {
                        order_by: self.order_by,
                        estimate: old,
                        index: g,
                        estimate_correct: false,
                    });
                    (g, false)
                }
                None => {
                    let g = self.key_store.num_rows();
                    self.key_store.push(probe);
                    self.group_index.entry(hash).or_default().push(g);
                    let mut a = SmallVec::with_capacity(n_agg);
                    for i in 0..n_agg {
                        a.push(ScalarValue::try_from_array(&value_cols[i], r)?);
                    }
                    self.acc.push(a);
                    self.cur_estimate.push(SmallVec::new());
                    self.nodes_seen.push(self.finished_nodes.clone());
                    (g, true)
                }
            };

            if !is_new {
                for i in 0..n_agg {
                    let v = ScalarValue::try_from_array(&value_cols[i], r)?;
                    scalar_combine(&mut self.acc[gid][i], &v, &self.agg_descr[i].0)?;
                }
            }
            self.nodes_seen[gid][node] = true;

            let est = self.group_estimate(gid)?;
            let correct = self.estimate_correct(gid);
            self.cur_estimate[gid] = est.clone();
            self.sorted.insert(SortKey {
                order_by: self.order_by,
                estimate: est,
                index: gid,
                estimate_correct: correct,
            });
        }
        Ok(())
    }

    /// Upper bound on `gid`'s final per-aggregate values: its accumulated value combined with each
    /// not-yet-seen node's frontier. Exact once the group has been seen (or the node finished) on
    /// every node.
    fn group_estimate(&self, gid: usize) -> Result<SmallVec<[ScalarValue; 1]>, DataFusionError> {
        let mut est = self.acc[gid].clone();
        for i in 0..est.len() {
            // Mirror V1: never turn a NULL estimate into a non-NULL one via node bounds.
            let use_node = !self.agg_descr[i].1.nulls_first || !est[i].is_null();
            if !use_node {
                continue;
            }
            for node in 0..self.num_nodes {
                if self.nodes_seen[gid][node] || self.finished_nodes[node] {
                    continue;
                }
                if let Some(frontier) = &self.node_frontier[node] {
                    scalar_combine(&mut est[i], &frontier[i], &self.agg_descr[i].0)?;
                }
            }
        }
        Ok(est)
    }

    fn estimate_correct(&self, gid: usize) -> bool {
        (0..self.num_nodes).all(|n| self.nodes_seen[gid][n] || self.finished_nodes[n])
    }

    fn populate_wanted_nodes(&self, wanted_nodes: &mut Vec<bool>) {
        match self.sorted.first() {
            None => {
                for w in wanted_nodes.iter_mut() {
                    *w = true;
                }
            }
            Some(candidate) => {
                let seen = &self.nodes_seen[candidate.index];
                assert_eq!(seen.len(), wanted_nodes.len());
                for i in 0..wanted_nodes.len() {
                    wanted_nodes[i] = !seen[i];
                }
            }
        }
    }

    async fn pop_top_elements(&mut self) -> Result<bool, DataFusionError> {
        while self.result.num_rows() < self.limit && !self.sorted.is_empty() {
            let mut candidate = self.sorted.pop_first().unwrap();
            while !candidate.estimate_correct {
                let est = self.group_estimate(candidate.index)?;
                let correct = self.estimate_correct(candidate.index);
                self.cur_estimate[candidate.index] = est.clone();
                self.sorted.insert(SortKey {
                    order_by: self.order_by,
                    estimate: est,
                    index: candidate.index,
                    estimate_correct: correct,
                });

                let next_candidate = self.sorted.first().unwrap();
                if candidate.index == next_candidate.index && !next_candidate.estimate_correct {
                    return Ok(false);
                } else {
                    candidate = self.sorted.pop_first().unwrap();
                }
            }
            self.top.push(candidate.index);
            if self.top.len() == self.limit {
                self.push_top_to_result().await?;
            }
        }
        Ok(self.result.num_rows() == self.limit || self.finished_nodes.iter().all(|f| *f))
    }

    async fn push_top_to_result(&mut self) -> Result<(), DataFusionError> {
        if self.top.is_empty() {
            return Ok(());
        }
        let n_agg = self.agg_descr.len();
        let mut columns = self
            .row_converter
            .convert_rows(self.top.iter().map(|&g| self.key_store.row(g)))?;
        for i in 0..n_agg {
            let mut builder = create_builder(&self.acc[self.top[0]][i]);
            for &g in &self.top {
                append_value(&mut *builder, &self.acc[g][i])?;
            }
            columns.push(builder.finish());
        }

        let new_batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        let new_batch = if let Some(having) = self.having {
            let schema = new_batch.schema();
            let filter_exec = Arc::new(FilterExec::try_new(
                having.clone(),
                try_make_memory_data_source(&vec![vec![new_batch]], schema.clone(), None)?,
            )?);
            let batches_stream =
                GlobalLimitExec::new(filter_exec, 0, Some(self.limit - self.result.num_rows()))
                    .execute(0, self.context.clone())?;
            let batches = collect(batches_stream).await?;
            concat_batches(&schema, &batches)?
        } else {
            new_batch
        };

        let mut tmp = RecordBatch::new_empty(self.schema.clone());
        std::mem::swap(&mut self.result, &mut tmp);
        self.result = concat_batches(&self.schema, &vec![tmp, new_batch])?;
        self.top.clear();
        Ok(())
    }

    async fn finish(mut self) -> Result<RecordBatch, DataFusionError> {
        self.push_top_to_result().await?;
        Ok(self.result)
    }
}

/// Merges `src` into `dst` the way the aggregate's distributed final step would: Sum adds, Min keeps
/// the smaller, Max the larger. NULLs are skipped (a NULL contributes nothing). `Merge` (HLL) is
/// gated out at plan time, so it is unreachable here.
///
/// This is the V2-only combine. Float SUM is not associative, and V2 accumulates in a different
/// order than V1 (per node, whole batches) -- so a float SUM top-k can differ from V1 in the last
/// ULPs, and on a near-tie at the k-th place even the *boundary group* can flip. This is inherent to
/// reordered distributed summation (the full-merge strategy reorders too, via DataFusion's own sum
/// kernel) and is not gated. MIN/MAX, by contrast, are exact: NaN is skipped to match arrow's
/// Min/Max accumulators.
fn scalar_combine(
    dst: &mut ScalarValue,
    src: &ScalarValue,
    fun: &TopKAggregateFunction,
) -> Result<(), DataFusionError> {
    if src.is_null() {
        return Ok(());
    }
    if dst.is_null() {
        *dst = src.clone();
        return Ok(());
    }
    match fun {
        // SUM keeps float semantics (a NaN input poisons the sum, matching arrow's sum).
        TopKAggregateFunction::Sum => scalar_add_assign(dst, src)?,
        // MIN/MAX skip NaN, matching arrow's Min/MaxAccumulator (`natural_cmp`/`total_cmp` would
        // otherwise rank NaN above every finite value and let it stick).
        TopKAggregateFunction::Min => {
            if !is_nan(src) && (is_nan(dst) || natural_cmp(src, dst) == Ordering::Less) {
                *dst = src.clone();
            }
        }
        TopKAggregateFunction::Max => {
            if !is_nan(src) && (is_nan(dst) || natural_cmp(src, dst) == Ordering::Greater) {
                *dst = src.clone();
            }
        }
        TopKAggregateFunction::Merge => {
            return Err(DataFusionError::Internal(
                "topk v2 does not support HLL merge".to_string(),
            ))
        }
    }
    Ok(())
}

/// Natural ascending comparison of two non-NULL numeric scalars (no sort-direction flip).
fn natural_cmp(l: &ScalarValue, r: &ScalarValue) -> Ordering {
    cmp_same_types(l, r, /*nulls_first*/ true, /*asc*/ true)
}

fn is_nan(s: &ScalarValue) -> bool {
    match s {
        ScalarValue::Float32(Some(v)) => v.is_nan(),
        ScalarValue::Float64(Some(v)) => v.is_nan(),
        _ => false,
    }
}

fn decimal_scale_mismatch() -> DataFusionError {
    DataFusionError::Internal("topk v2 decimal precision/scale mismatch in sum".to_string())
}

/// Integer and decimal SUM wrap on overflow (`wrapping_add`), matching DataFusion's `SumAccumulator`
/// (which uses `add_wrapping`); so V2 stays consistent with the default merge rather than erroring.
fn scalar_add_assign(dst: &mut ScalarValue, src: &ScalarValue) -> Result<(), DataFusionError> {
    macro_rules! add {
        ($d:expr, $s:expr) => {{
            *$d = Some($d.unwrap_or(0).wrapping_add($s.unwrap_or(0)));
        }};
    }
    match (dst, src) {
        (ScalarValue::Int8(d), ScalarValue::Int8(s)) => add!(d, s),
        (ScalarValue::Int16(d), ScalarValue::Int16(s)) => add!(d, s),
        (ScalarValue::Int32(d), ScalarValue::Int32(s)) => add!(d, s),
        (ScalarValue::Int64(d), ScalarValue::Int64(s)) => add!(d, s),
        (ScalarValue::UInt8(d), ScalarValue::UInt8(s)) => add!(d, s),
        (ScalarValue::UInt16(d), ScalarValue::UInt16(s)) => add!(d, s),
        (ScalarValue::UInt32(d), ScalarValue::UInt32(s)) => add!(d, s),
        (ScalarValue::UInt64(d), ScalarValue::UInt64(s)) => add!(d, s),
        (ScalarValue::Float32(d), ScalarValue::Float32(s)) => {
            *d = Some(d.unwrap_or(0.0) + s.unwrap_or(0.0))
        }
        (ScalarValue::Float64(d), ScalarValue::Float64(s)) => {
            *d = Some(d.unwrap_or(0.0) + s.unwrap_or(0.0))
        }
        (ScalarValue::Decimal128(d, dp, ds), ScalarValue::Decimal128(s, sp, ss)) => {
            if dp != sp || ds != ss {
                return Err(decimal_scale_mismatch());
            }
            *d = Some(d.unwrap_or(0).wrapping_add(s.unwrap_or(0)));
        }
        (ScalarValue::Decimal256(d, dp, ds), ScalarValue::Decimal256(s, sp, ss)) => {
            if dp != sp || ds != ss {
                return Err(decimal_scale_mismatch());
            }
            *d = Some(
                d.unwrap_or(i256::ZERO)
                    .wrapping_add(s.unwrap_or(i256::ZERO)),
            );
        }
        (d, s) => {
            return Err(DataFusionError::Internal(format!(
                "topk v2 unsupported sum types: {} and {}",
                d.data_type(),
                s.data_type()
            )))
        }
    }
    Ok(())
}

/// Replaces a frontier value with the aggregate's neutral value when the neutral value is a tighter
/// (or required) upper bound in the sort direction -- e.g. for `SUM(x) ... DESC` a missing group
/// contributes `0`, which beats a negative frontier. Mirrors V1's `update_node_estimates`.
fn clamp_to_neutral_bound(v: &mut ScalarValue, descr: &AggDescr) {
    if v.is_null() {
        return;
    }
    let mut neutral = v.clone();
    to_neutral_value(&mut neutral, &descr.0);
    let o = cmp_same_types(
        &neutral,
        v,
        descr.1.nulls_first,
        /*asc*/ !descr.1.descending,
    );
    if o < Ordering::Equal {
        *v = neutral;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queryplanner::topk::plan::make_sort_expr;
    use crate::queryplanner::topk::{AggregateTopKExec, SortColumn};
    use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{Column, DFSchema};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams};
    use datafusion::logical_expr::AggregateUDF;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortRequirement};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_planner::create_aggregate_expr_and_maybe_filter;
    use datafusion::prelude::Expr;
    use futures::StreamExt;
    use itertools::Itertools;

    use std::collections::HashMap;
    use std::iter::FromIterator;
    use std::sync::Arc;

    #[tokio::test]
    async fn topk_simple() {
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let context: Arc<TaskContext> = session_state.task_ctx();

        // Test sum with descending sort order.
        let proto = mock_topk(
            2,
            &[DataType::Int64],
            &[TopKAggregateFunction::Sum],
            vec![SortColumn {
                agg_index: 0,
                asc: false,
                nulls_first: true,
            }],
        )
        .unwrap();
        let bs = proto.cluster.schema();

        let r = run_topk(
            &proto,
            vec![
                vec![make_batch(&bs, &[&[1, 100], &[0, 50], &[8, 11], &[6, 10]])],
                vec![make_batch(&bs, &[&[6, 40], &[1, 20], &[0, 15], &[8, 9]])],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, 120], vec![0, 65]]);

        // empty batches.
        let r = run_topk(
            &proto,
            vec![
                vec![
                    make_batch(&bs, &[&[1, 100], &[0, 50], &[8, 11], &[6, 10]]),
                    make_batch(&bs, &[]),
                ],
                vec![
                    make_batch(&bs, &[]),
                    make_batch(&bs, &[&[6, 40], &[1, 20], &[0, 15], &[8, 9]]),
                ],
                vec![
                    make_batch(&bs, &[]),
                    make_batch(&bs, &[]),
                    make_batch(&bs, &[]),
                ],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, 120], vec![0, 65]]);

        // batches of different sizes.
        let r = run_topk(
            &proto,
            vec![
                vec![
                    make_batch(&bs, &[&[1, 100]]),
                    make_batch(&bs, &[&[0, 50], &[8, 11]]),
                    make_batch(&bs, &[&[6, 10]]),
                ],
                vec![make_batch(&bs, &[&[6, 40], &[1, 20], &[0, 15], &[8, 9]])],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, 120], vec![0, 65]]);

        // missing groups on some nodes.
        let r = run_topk(
            &proto,
            vec![
                vec![
                    make_batch(&bs, &[&[1, 100], &[8, 11]]),
                    make_batch(&bs, &[&[6, 9]]),
                ],
                vec![make_batch(&bs, &[&[6, 40], &[0, 15], &[8, 9]])],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, 100], vec![6, 49]]);

        // sort order might be affected by values that are far away in the input.
        let r = run_topk(
            &proto,
            vec![
                vec![make_batch(
                    &bs,
                    &[&[1, 1000], &[2, 500], &[3, 500], &[4, 500]],
                )],
                vec![
                    make_batch(&bs, &[&[2, 600], &[3, 599]]),
                    make_batch(&bs, &[&[4, 598], &[5, 500]]),
                    make_batch(&bs, &[&[6, 500], &[7, 500]]),
                    make_batch(&bs, &[&[8, 500], &[9, 500]]),
                    make_batch(&bs, &[&[1, 101]]),
                ],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, 1101], vec![2, 1100]]);
    }

    #[tokio::test]
    async fn topk_missing_elements() {
        let session_state: SessionState =
            SessionStateBuilder::new().with_default_features().build();
        let context: Arc<TaskContext> = session_state.task_ctx();

        // Start with sum, descending order.
        let mut proto = mock_topk(
            2,
            &[DataType::Int64],
            &[TopKAggregateFunction::Sum],
            vec![SortColumn {
                agg_index: 0,
                asc: false,
                nulls_first: true,
            }],
        )
        .unwrap();
        let bs = proto.cluster.schema();

        // negative numbers must not confuse the estimates.
        let r = run_topk(
            &proto,
            vec![
                vec![make_batch(&bs, &[&[1, 100], &[2, 50]])],
                vec![make_batch(
                    &bs,
                    &[&[3, 90], &[4, 80], &[5, -100], &[6, -500]],
                )],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, 100], vec![3, 90]]);

        // same with positive numbers in ascending order.
        proto.change_order(vec![SortColumn {
            agg_index: 0,
            asc: true,
            nulls_first: true,
        }]);
        let r = run_topk(
            &proto,
            vec![
                vec![make_batch(&bs, &[&[1, -100], &[2, -50]])],
                vec![make_batch(
                    &bs,
                    &[&[3, -90], &[4, -80], &[5, 100], &[6, 500]],
                )],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, -100], vec![3, -90]]);

        // nulls should be taken into account in the estimates.
        proto.change_order(vec![SortColumn {
            agg_index: 0,
            asc: false,
            nulls_first: true,
        }]);
        let r = run_topk_opt(
            &proto,
            vec![
                vec![make_batch_opt(&bs, &[&[Some(1), None], &[Some(2), None]])],
                vec![make_batch_opt(
                    &bs,
                    &[&[Some(10), Some(1000)], &[Some(1), Some(900)]],
                )],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![Some(2), None], vec![Some(10), Some(1000)]]);
    }

    #[tokio::test]
    async fn topk_sort_orders() {
        let session_state: SessionState =
            SessionStateBuilder::new().with_default_features().build();
        let context: Arc<TaskContext> = session_state.task_ctx();

        let mut proto = mock_topk(
            1,
            &[DataType::Int64],
            &[TopKAggregateFunction::Sum],
            vec![SortColumn {
                agg_index: 0,
                asc: true,
                nulls_first: true,
            }],
        )
        .unwrap();
        let bs = proto.cluster.schema();

        // Ascending.
        let r = run_topk(
            &proto,
            vec![
                vec![make_batch(&bs, &[&[1, 0], &[0, 100]])],
                vec![make_batch(&bs, &[&[0, -100], &[1, -5]])],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, -5]]);

        // Descending.
        proto.change_order(vec![SortColumn {
            agg_index: 0,
            asc: false,
            nulls_first: true,
        }]);
        let r = run_topk(
            &proto,
            vec![
                vec![make_batch(&bs, &[&[0, 100], &[1, 0]])],
                vec![make_batch(&bs, &[&[1, -5], &[0, -100]])],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![0, 0]]);

        // Ascending, nulls first.
        proto.change_order(vec![SortColumn {
            agg_index: 0,
            asc: true,
            nulls_first: true,
        }]);
        let r = run_topk_opt(
            &proto,
            vec![
                vec![make_batch_opt(&bs, &[&[Some(3), None]])],
                vec![make_batch_opt(
                    &bs,
                    &[&[Some(2), None], &[Some(3), Some(1)]],
                )],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![Some(2), None]]);

        // Ascending, nulls last.
        proto.change_order(vec![SortColumn {
            agg_index: 0,
            asc: true,
            nulls_first: false,
        }]);
        let r = run_topk_opt(
            &proto,
            vec![
                vec![make_batch_opt(
                    &bs,
                    &[&[Some(4), Some(10)], &[Some(3), None]],
                )],
                vec![make_batch_opt(
                    &bs,
                    &[&[Some(3), Some(1)], &[Some(2), None], &[Some(4), None]],
                )],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![Some(3), Some(1)]]);
    }

    #[tokio::test]
    async fn topk_multi_column_sort() {
        let session_state: SessionState =
            SessionStateBuilder::new().with_default_features().build();
        let context: Arc<TaskContext> = session_state.task_ctx();

        let proto = mock_topk(
            10,
            &[DataType::Int64],
            &[TopKAggregateFunction::Sum, TopKAggregateFunction::Min],
            vec![
                SortColumn {
                    agg_index: 0,
                    asc: true,
                    nulls_first: true,
                },
                SortColumn {
                    agg_index: 1,
                    asc: false,
                    nulls_first: true,
                },
            ],
        )
        .unwrap();
        let bs = proto.cluster.schema();

        let r = run_topk(
            &proto,
            vec![
                vec![make_batch(
                    &bs,
                    &[&[2, 50, 20], &[3, 100, 20], &[1, 100, 10]],
                )],
                vec![make_batch(&bs, &[&[1, 0, 10], &[3, 50, 5], &[2, 50, 5]])],
            ],
            &context,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, 100, 10], vec![2, 100, 5], vec![3, 150, 5]]);
    }

    fn make_batch(schema: &SchemaRef, rows: &[&[i64]]) -> RecordBatch {
        if rows.is_empty() {
            return RecordBatch::new_empty(schema.clone());
        }
        for r in rows {
            assert_eq!(r.len(), schema.fields().len());
        }
        let mut columns: Vec<ArrayRef> = Vec::new();
        for col_i in 0..rows[0].len() {
            let column_data = (0..rows.len()).map(|row_i| rows[row_i][col_i]);
            columns.push(Arc::new(Int64Array::from_iter_values(column_data)))
        }
        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }

    fn make_batch_opt(schema: &SchemaRef, rows: &[&[Option<i64>]]) -> RecordBatch {
        if rows.is_empty() {
            return RecordBatch::new_empty(schema.clone());
        }
        for r in rows {
            assert_eq!(r.len(), schema.fields().len());
        }
        let mut columns: Vec<ArrayRef> = Vec::new();
        for col_i in 0..rows[0].len() {
            let column_data = (0..rows.len()).map(|row_i| rows[row_i][col_i]);
            columns.push(Arc::new(Int64Array::from_iter(column_data)))
        }
        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }

    fn topk_fun_to_fusion_type(
        ctx: &SessionState,
        topk_fun: &TopKAggregateFunction,
    ) -> Option<Arc<AggregateUDF>> {
        let name = match topk_fun {
            TopKAggregateFunction::Sum => "sum",
            TopKAggregateFunction::Max => "max",
            TopKAggregateFunction::Min => "min",
            _ => return None,
        };
        ctx.aggregate_functions().get(name).cloned()
    }
    fn mock_topk(
        limit: usize,
        group_by: &[DataType],
        aggs: &[TopKAggregateFunction],
        order_by: Vec<SortColumn>,
    ) -> Result<AggregateTopKExec, DataFusionError> {
        let key_fields: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = group_by
            .iter()
            .enumerate()
            .map(|(i, t)| {
                (
                    None,
                    Arc::new(Field::new(&format!("key{}", i + 1), t.clone(), false)),
                )
            })
            .collect_vec();
        let key_len = key_fields.len();

        let input_agg_fields: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = (0
            ..aggs.len())
            .map(|i| {
                (
                    None,
                    Arc::new(Field::new(&format!("agg{}", i + 1), DataType::Int64, true)),
                )
            })
            .collect_vec();
        let input_schema = DFSchema::new_with_metadata(
            key_fields.iter().cloned().chain(input_agg_fields).collect(),
            HashMap::new(),
        )?;

        let ctx = SessionStateBuilder::new().with_default_features().build();

        let agg_functions = aggs
            .iter()
            .enumerate()
            .map(|(i, f)| AggregateFunction {
                func: topk_fun_to_fusion_type(&ctx, f).unwrap(),
                params: AggregateFunctionParams {
                    args: vec![Expr::Column(Column::from_name(format!("agg{}", i + 1)))],
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                },
            })
            .collect::<Vec<_>>();
        let agg_exprs = agg_functions
            .iter()
            .map(|agg_fn| Expr::AggregateFunction(agg_fn.clone()));
        let physical_agg_exprs: Vec<(
            Arc<AggregateFunctionExpr>,
            Option<Arc<dyn PhysicalExpr>>,
            Option<LexOrdering>,
        )> = agg_exprs
            .map(|e| {
                Ok(create_aggregate_expr_and_maybe_filter(
                    &e,
                    &input_schema,
                    input_schema.inner(),
                    ctx.execution_props(),
                )?)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        let (agg_fn_exprs, _agg_phys_exprs, _order_by): (Vec<_>, Vec<_>, Vec<_>) =
            itertools::multiunzip(physical_agg_exprs);

        let output_agg_fields = agg_fn_exprs
            .iter()
            .map(|agg| agg.field())
            .collect::<Vec<_>>();
        let output_schema = Arc::new(Schema::new(
            key_fields
                .into_iter()
                .map(|(_, k)| Field::new(k.name(), k.data_type().clone(), k.is_nullable()))
                .chain(output_agg_fields)
                .collect::<Vec<_>>(),
        ));

        let sort_requirement = order_by
            .iter()
            .map(|c| {
                let i = key_len + c.agg_index;
                PhysicalSortRequirement {
                    expr: make_sort_expr(
                        &aggs[c.agg_index],
                        Arc::new(datafusion::physical_expr::expressions::Column::new(
                            input_schema.field(i).name(),
                            i,
                        )),
                    ),
                    options: Some(SortOptions {
                        descending: !c.asc,
                        nulls_first: c.nulls_first,
                    }),
                }
            })
            .collect();

        Ok(AggregateTopKExec::new(
            limit,
            key_len,
            agg_fn_exprs,
            aggs,
            order_by,
            None,
            Arc::new(EmptyExec::new(input_schema.inner().clone())),
            output_schema,
            sort_requirement,
            TopKMergeVersion::V1,
        ))
    }

    async fn run_one_version(
        proto: &AggregateTopKExec,
        version: TopKMergeVersion,
        input: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut plan = proto.clone();
        plan.merge_version = version;
        let results = Arc::new(plan)
            .with_new_children(vec![input])?
            .execute(0, context)?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        assert_eq!(results.len(), 1);
        Ok(results.into_iter().next().unwrap())
    }

    /// Runs both merge implementations on the same input and asserts they agree, so every existing
    /// test doubles as a V1-vs-V2 equivalence check. Returns the (shared) result.
    async fn run_topk_as_batch(
        proto: Arc<AggregateTopKExec>,
        inputs: Vec<Vec<RecordBatch>>,
        context: Arc<TaskContext>,
    ) -> Result<RecordBatch, DataFusionError> {
        let input = try_make_memory_data_source(&inputs, proto.cluster.schema(), None)?;
        let r1 =
            run_one_version(&proto, TopKMergeVersion::V1, input.clone(), context.clone()).await?;
        let r2 = run_one_version(&proto, TopKMergeVersion::V2, input, context).await?;
        assert_batches_eq(&r1, &r2);
        Ok(r1)
    }

    // Exact, and used only with Int64 fixtures on purpose: float SUM is order-dependent, so a float
    // V1-vs-V2 case would need a tolerant (epsilon/ULP) comparison instead. Typed float/decimal
    // arithmetic is covered directly in `scalar_arithmetic` below.
    fn assert_batches_eq(a: &RecordBatch, b: &RecordBatch) {
        assert_eq!(a.schema(), b.schema(), "schema mismatch v1 vs v2");
        assert_eq!(a.num_rows(), b.num_rows(), "row count mismatch v1 vs v2");
        for col in 0..a.num_columns() {
            for row in 0..a.num_rows() {
                let va = ScalarValue::try_from_array(a.column(col), row).unwrap();
                let vb = ScalarValue::try_from_array(b.column(col), row).unwrap();
                assert_eq!(va, vb, "v1 vs v2 differ at col {col} row {row}");
            }
        }
    }

    #[test]
    fn scalar_arithmetic() {
        use TopKAggregateFunction::*;
        let i = |v: i64| ScalarValue::Int64(Some(v));
        let f = |v: f64| ScalarValue::Float64(Some(v));

        // SUM: add; NULL src skipped; NULL dst adopts src.
        let mut d = i(5);
        scalar_combine(&mut d, &i(3), &Sum).unwrap();
        assert_eq!(d, i(8));
        let mut d = i(5);
        scalar_combine(&mut d, &ScalarValue::Int64(None), &Sum).unwrap();
        assert_eq!(d, i(5));
        let mut d = ScalarValue::Int64(None);
        scalar_combine(&mut d, &i(7), &Sum).unwrap();
        assert_eq!(d, i(7));

        // MIN/MAX pick the extreme.
        let mut d = i(5);
        scalar_combine(&mut d, &i(3), &Min).unwrap();
        assert_eq!(d, i(3));
        let mut d = i(5);
        scalar_combine(&mut d, &i(9), &Max).unwrap();
        assert_eq!(d, i(9));

        // MIN/MAX skip NaN from either side (matches arrow's Min/Max accumulators).
        let mut d = f(5.0);
        scalar_combine(&mut d, &f(f64::NAN), &Max).unwrap();
        assert_eq!(d, f(5.0));
        let mut d = f(f64::NAN);
        scalar_combine(&mut d, &f(3.0), &Max).unwrap();
        assert_eq!(d, f(3.0));
        let mut d = f(5.0);
        scalar_combine(&mut d, &f(f64::NAN), &Min).unwrap();
        assert_eq!(d, f(5.0));
        let mut d = f(f64::NAN);
        scalar_combine(&mut d, &f(3.0), &Min).unwrap();
        assert_eq!(d, f(3.0));

        // SUM does NOT skip NaN (a NaN input poisons the sum, matching arrow sum).
        let mut d = f(5.0);
        scalar_combine(&mut d, &f(f64::NAN), &Sum).unwrap();
        assert!(matches!(d, ScalarValue::Float64(Some(v)) if v.is_nan()));

        // Decimal scale mismatch is a hard error, not a silent raw-mantissa add.
        let mut d = ScalarValue::Decimal128(Some(100), 10, 2);
        assert!(scalar_combine(&mut d, &ScalarValue::Decimal128(Some(100), 10, 3), &Sum).is_err());
        let mut d = ScalarValue::Decimal128(Some(100), 10, 2);
        scalar_combine(&mut d, &ScalarValue::Decimal128(Some(50), 10, 2), &Sum).unwrap();
        assert_eq!(d, ScalarValue::Decimal128(Some(150), 10, 2));
    }

    async fn run_topk(
        proto: &AggregateTopKExec,
        inputs: Vec<Vec<RecordBatch>>,
        context: &Arc<TaskContext>,
    ) -> Result<Vec<Vec<i64>>, DataFusionError> {
        return Ok(to_vec(
            &run_topk_as_batch(Arc::new(proto.clone()), inputs, context.clone()).await?,
        ));
    }

    async fn run_topk_opt(
        proto: &AggregateTopKExec,
        inputs: Vec<Vec<RecordBatch>>,
        context: &Arc<TaskContext>,
    ) -> Result<Vec<Vec<Option<i64>>>, DataFusionError> {
        return Ok(to_opt_vec(
            &run_topk_as_batch(Arc::new(proto.clone()), inputs, context.clone()).await?,
        ));
    }

    fn to_opt_vec(b: &RecordBatch) -> Vec<Vec<Option<i64>>> {
        let mut rows = vec![vec![None; b.num_columns()]; b.num_rows()];
        for col_i in 0..b.num_columns() {
            let col = b
                .column(col_i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row_i in 0..b.num_rows() {
                if col.is_null(row_i) {
                    continue;
                }
                rows[row_i][col_i] = Some(col.value(row_i));
            }
        }
        rows
    }

    fn to_vec(b: &RecordBatch) -> Vec<Vec<i64>> {
        let mut rows = vec![vec![0; b.num_columns()]; b.num_rows()];
        for col_i in 0..b.num_columns() {
            let col = b
                .column(col_i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(col.null_count(), 0);
            let col = col.values();
            for row_i in 0..b.num_rows() {
                rows[row_i][col_i] = col[row_i]
            }
        }
        rows
    }
}

async fn next_non_empty<S>(s: &mut S) -> Result<Option<RecordBatch>, DataFusionError>
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    loop {
        if let Some(b) = s.next().await {
            let b = b?;
            if b.num_rows() == 0 {
                continue;
            }
            return Ok(Some(b));
        } else {
            return Ok(None);
        }
    }
}
