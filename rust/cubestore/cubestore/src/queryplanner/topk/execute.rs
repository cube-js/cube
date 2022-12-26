use crate::queryplanner::topk::SortColumn;
use crate::queryplanner::udfs::read_sketch;
use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::cube_ext;
use datafusion::error::DataFusionError;

use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::group_scalar::GroupByScalar;
use datafusion::physical_plan::hash_aggregate::{
    create_accumulators, create_group_by_values, write_group_result_row, AccumulatorSet,
    AggregateMode,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{
    AggregateExpr, ExecutionPlan, OptimizerHints, Partitioning, PhysicalExpr,
    SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use flatbuffers::bitflags::_core::cmp::Ordering;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use smallvec::smallvec;
use smallvec::SmallVec;
use std::any::Any;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopKAggregateFunction {
    Sum,
    Min,
    Max,
    Merge,
}

#[derive(Debug)]
pub struct AggregateTopKExec {
    pub limit: usize,
    pub key_len: usize,
    pub agg_expr: Vec<Arc<dyn AggregateExpr>>,
    pub agg_descr: Vec<AggDescr>,
    pub order_by: Vec<SortColumn>,
    pub having: Option<Arc<dyn PhysicalExpr>>,
    /// Always an instance of ClusterSendExec or WorkerExec.
    pub cluster: Arc<dyn ExecutionPlan>,
    pub schema: SchemaRef,
}

/// Third item is the neutral value for the corresponding aggregate function.
type AggDescr = (TopKAggregateFunction, SortOptions, ScalarValue);

impl AggregateTopKExec {
    pub fn new(
        limit: usize,
        key_len: usize,
        agg_expr: Vec<Arc<dyn AggregateExpr>>,
        agg_fun: &[TopKAggregateFunction],
        order_by: Vec<SortColumn>,
        having: Option<Arc<dyn PhysicalExpr>>,
        cluster: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> AggregateTopKExec {
        assert_eq!(schema.fields().len(), agg_expr.len() + key_len);
        assert_eq!(agg_fun.len(), agg_expr.len());
        let agg_descr = Self::compute_descr(&agg_expr, agg_fun, &order_by);

        AggregateTopKExec {
            limit,
            key_len,
            agg_expr,
            agg_descr,
            order_by,
            having,
            cluster,
            schema,
        }
    }

    fn compute_descr(
        agg_expr: &[Arc<dyn AggregateExpr>],
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

#[async_trait]
impl ExecutionPlan for AggregateTopKExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.cluster.clone()]
    }

    fn with_new_children(
        &self,
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
        }))
    }

    fn output_hints(&self) -> OptimizerHints {
        // It's a top-level plan most of the time, so the results should not matter.
        OptimizerHints::default()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        assert_eq!(partition, 0);
        let nodes = self.cluster.output_partitioning().partition_count();
        let mut tasks = Vec::with_capacity(nodes);
        for p in 0..nodes {
            let cluster = self.cluster.clone();
            tasks.push(cube_ext::spawn(async move {
                // fuse the streams to simplify further code.
                cluster.execute(p).await.map(|s| (s.schema(), s.fuse()))
            }));
        }
        let mut streams = Vec::with_capacity(nodes);
        for t in tasks {
            streams.push(
                t.await.map_err(|_| {
                    DataFusionError::Internal("could not join threads".to_string())
                })??,
            );
        }

        let mut buffer = TopKBuffer::default();
        let mut state = TopKState::new(
            self.limit,
            nodes,
            self.key_len,
            &self.order_by,
            &self.having,
            &self.agg_expr,
            &self.agg_descr,
            &mut buffer,
            self.schema(),
        )?;
        let mut wanted_nodes = vec![true; nodes];
        let mut batches = Vec::with_capacity(nodes);
        'processing: loop {
            assert!(batches.is_empty());
            for i in 0..nodes {
                let (schema, s) = &mut streams[i];
                let batch;
                if wanted_nodes[i] {
                    batch = next_non_empty(s).await?;
                } else {
                    batch = Some(RecordBatch::new_empty(schema.clone()))
                }
                batches.push(batch);
            }

            if state.update(&mut batches).await? {
                batches.clear();
                break 'processing;
            }
            state.populate_wanted_nodes(&mut wanted_nodes);
            batches.clear();
        }

        let batch = state.finish().await?;
        let schema = batch.schema();
        // TODO: don't clone batch.
        MemoryExec::try_new(&vec![vec![batch]], schema, None)?
            .execute(0)
            .await
    }
}

// Mutex is to provide interior mutability inside async function, no actual waiting ever happens.
// TODO: remove mutex with careful use of unsafe.
type TopKBuffer = std::sync::Mutex<Vec<Group>>;

struct TopKState<'a> {
    limit: usize,
    buffer: &'a TopKBuffer,
    key_len: usize,
    order_by: &'a [SortColumn],
    having: &'a Option<Arc<dyn PhysicalExpr>>,
    agg_expr: &'a Vec<Arc<dyn AggregateExpr>>,
    agg_descr: &'a [AggDescr],
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
        self.estimates.iter().map(|e| e.evaluate()).collect()
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
        agg_expr: &'a Vec<Arc<dyn AggregateExpr>>,
        agg_descr: &'a [AggDescr],
        buffer: &'a mut TopKBuffer,
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

                    let mut key = smallvec![GroupByScalar::Int8(0); self.key_len];
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
                    &g.accumulators,
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
                    Arc::new(MemoryExec::try_new(
                        &vec![vec![new_batch]],
                        schema.clone(),
                        None,
                    )?),
                )?);
                let batches_stream =
                    GlobalLimitExec::new(filter_exec, self.limit - self.result.num_rows())
                        .execute(0)
                        .await?;

                let batches = collect(batches_stream).await?;
                RecordBatch::concat(&schema, &batches)?
            } else {
                new_batch
            };
            let mut tmp = RecordBatch::new_empty(self.schema.clone());
            std::mem::swap(&mut self.result, &mut tmp);
            self.result = RecordBatch::concat(&self.schema, &vec![tmp, new_batch])?;
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

    /// Returns true iff the estimate matches the correct score.
    fn update_group_estimates(&self, group: &mut Group) -> Result<(), DataFusionError> {
        for i in 0..group.estimates.len() {
            group.estimates[i].reset();
            group.estimates[i].merge(&group.accumulators[i].state()?)?;
            // Node estimate might contain a neutral value (e.g. '0' for sum), but we must avoid
            // giving invalid estimates for NULL values.
            let use_node_estimates =
                !self.agg_descr[i].1.nulls_first || !group.estimates[i].evaluate()?.is_null();
            for node in 0..group.nodes.len() {
                if !group.nodes[node] {
                    if self.finished_nodes[node] {
                        group.nodes[node] = true;
                        continue;
                    }
                    if use_node_estimates {
                        group.estimates[i].merge(&self.node_estimates[node][i].state()?)?;
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
            acc.reset();

            // evaluate() gives us a scalar value of the required type.
            let mut neutral = acc.evaluate()?;
            to_neutral_value(&mut neutral, &agg_descr[i].0);

            acc.update_batch(&vec![columns[key_len + i].slice(row_i, 1)])?;

            // Neutral value (i.e. missing on the node) might be the right estimate.
            // E.g. `0` is better than `-10` on `SUM(x) ORDER BY SUM(x) DESC`.
            // We have to provide correct estimates.
            let o = cmp_same_types(
                &neutral,
                &acc.evaluate()?,
                agg_descr[i].1.nulls_first,
                !agg_descr[i].1.descending,
            );
            if o < Ordering::Equal {
                acc.reset();
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
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l.cmp(r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (
            ScalarValue::Int64Decimal(Some(l), lscale),
            ScalarValue::Int64Decimal(Some(r), rscale),
        ) => {
            assert_eq!(lscale, rscale);
            l.cmp(r)
        }
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
        (ScalarValue::TimestampSecond(Some(l)), ScalarValue::TimestampSecond(Some(r))) => l.cmp(r),
        (
            ScalarValue::TimestampMillisecond(Some(l)),
            ScalarValue::TimestampMillisecond(Some(r)),
        ) => l.cmp(r),
        (
            ScalarValue::TimestampMicrosecond(Some(l)),
            ScalarValue::TimestampMicrosecond(Some(r)),
        ) => l.cmp(r),
        (ScalarValue::TimestampNanosecond(Some(l)), ScalarValue::TimestampNanosecond(Some(r))) => {
            l.cmp(r)
        }
        (ScalarValue::IntervalYearMonth(Some(l)), ScalarValue::IntervalYearMonth(Some(r))) => {
            l.cmp(r)
        }
        (ScalarValue::IntervalDayTime(Some(l)), ScalarValue::IntervalDayTime(Some(r))) => l.cmp(r),
        (ScalarValue::List(_, _), ScalarValue::List(_, _)) => {
            panic!("list as accumulator result is not supported")
        }
        (l, r) => panic!(
            "unhandled types in comparison: {} and {}",
            l.get_datatype(),
            r.get_datatype()
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
        ScalarValue::Int8(v) => *v = Some(0),
        ScalarValue::Int16(v) => *v = Some(0),
        ScalarValue::Int32(v) => *v = Some(0),
        ScalarValue::Int64(v) => *v = Some(0),
        ScalarValue::Int64Decimal(v, _) => *v = Some(0),
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
        ScalarValue::Int8(v) => *v = Some(i8::MAX),
        ScalarValue::Int16(v) => *v = Some(i16::MAX),
        ScalarValue::Int32(v) => *v = Some(i32::MAX),
        ScalarValue::Int64(v) => *v = Some(i64::MAX),
        ScalarValue::Int64Decimal(v, _) => *v = Some(i64::MAX),
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
        ScalarValue::Int8(v) => *v = Some(i8::MIN),
        ScalarValue::Int16(v) => *v = Some(i16::MIN),
        ScalarValue::Int32(v) => *v = Some(i32::MIN),
        ScalarValue::Int64(v) => *v = Some(i64::MIN),
        ScalarValue::Int64Decimal(v, _) => *v = Some(i64::MIN),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queryplanner::topk::{AggregateTopKExec, SortColumn};
    use arrow::array::{Array, ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::error::ArrowError;
    use arrow::record_batch::RecordBatch;
    use datafusion::catalog::catalog::MemoryCatalogList;
    use datafusion::error::DataFusionError;
    use datafusion::execution::context::{ExecutionConfig, ExecutionContextState, ExecutionProps};
    use datafusion::logical_plan::{Column, DFField, DFSchema, Expr};
    use datafusion::physical_plan::aggregates::AggregateFunction;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::StreamExt;
    use itertools::Itertools;

    use std::iter::FromIterator;
    use std::sync::Arc;

    #[tokio::test]
    async fn topk_simple() {
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
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![1, 1101], vec![2, 1100]]);
    }

    #[tokio::test]
    async fn topk_missing_elements() {
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
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![Some(2), None], vec![Some(10), Some(1000)]]);
    }

    #[tokio::test]
    async fn topk_sort_orders() {
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
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![0, 0]]);

        // Ascending, null first.
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
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![Some(2), None]]);

        // Ascending, null last.
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
        )
        .await
        .unwrap();
        assert_eq!(r, vec![vec![Some(3), Some(1)]]);
    }

    #[tokio::test]
    async fn topk_multi_column_sort() {
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

    fn topk_fun_to_fusion_type(topk_fun: &TopKAggregateFunction) -> Option<AggregateFunction> {
        match topk_fun {
            TopKAggregateFunction::Sum => Some(AggregateFunction::Sum),
            TopKAggregateFunction::Max => Some(AggregateFunction::Max),
            TopKAggregateFunction::Min => Some(AggregateFunction::Min),
            _ => None,
        }
    }
    fn mock_topk(
        limit: usize,
        group_by: &[DataType],
        aggs: &[TopKAggregateFunction],
        order_by: Vec<SortColumn>,
    ) -> Result<AggregateTopKExec, DataFusionError> {
        let key_fields = group_by
            .iter()
            .enumerate()
            .map(|(i, t)| DFField::new(None, &format!("key{}", i + 1), t.clone(), false))
            .collect_vec();
        let key_len = key_fields.len();

        let input_agg_fields = (0..aggs.len())
            .map(|i| DFField::new(None, &format!("agg{}", i + 1), DataType::Int64, true))
            .collect_vec();
        let input_schema =
            DFSchema::new(key_fields.iter().cloned().chain(input_agg_fields).collect())?;

        let ctx = ExecutionContextState {
            catalog_list: Arc::new(MemoryCatalogList::new()),
            scalar_functions: Default::default(),
            var_provider: Default::default(),
            aggregate_functions: Default::default(),
            config: ExecutionConfig::new(),
            execution_props: ExecutionProps::new(),
        };
        let agg_exprs = aggs
            .iter()
            .enumerate()
            .map(|(i, f)| Expr::AggregateFunction {
                fun: topk_fun_to_fusion_type(f).unwrap(),
                args: vec![Expr::Column(Column::from_name(format!("agg{}", i + 1)))],
                distinct: false,
            });
        let physical_agg_exprs = agg_exprs
            .map(|e| {
                Ok(DefaultPhysicalPlanner::default().create_aggregate_expr(
                    &e,
                    &input_schema,
                    &input_schema.to_schema_ref(),
                    &ctx,
                )?)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        let output_agg_fields = physical_agg_exprs
            .iter()
            .map(|agg| agg.field())
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        let output_schema = Arc::new(Schema::new(
            key_fields
                .into_iter()
                .map(|k| Field::new(k.name().as_ref(), k.data_type().clone(), k.is_nullable()))
                .chain(output_agg_fields)
                .collect(),
        ));

        Ok(AggregateTopKExec::new(
            limit,
            key_len,
            physical_agg_exprs,
            aggs,
            order_by,
            None,
            Arc::new(EmptyExec::new(false, input_schema.to_schema_ref())),
            output_schema,
        ))
    }

    async fn run_topk_as_batch(
        proto: &AggregateTopKExec,
        inputs: Vec<Vec<RecordBatch>>,
    ) -> Result<RecordBatch, DataFusionError> {
        let input = Arc::new(MemoryExec::try_new(&inputs, proto.cluster.schema(), None)?);
        let results = proto
            .with_new_children(vec![input])?
            .execute(0)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, ArrowError>>()?;
        assert_eq!(results.len(), 1);
        Ok(results.into_iter().next().unwrap())
    }

    async fn run_topk(
        proto: &AggregateTopKExec,
        inputs: Vec<Vec<RecordBatch>>,
    ) -> Result<Vec<Vec<i64>>, DataFusionError> {
        return Ok(to_vec(&run_topk_as_batch(proto, inputs).await?));
    }

    async fn run_topk_opt(
        proto: &AggregateTopKExec,
        inputs: Vec<Vec<RecordBatch>>,
    ) -> Result<Vec<Vec<Option<i64>>>, DataFusionError> {
        return Ok(to_opt_vec(&run_topk_as_batch(proto, inputs).await?));
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

async fn next_non_empty<S>(s: &mut S) -> Result<Option<RecordBatch>, ArrowError>
where
    S: Stream<Item = Result<RecordBatch, ArrowError>> + Unpin,
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
