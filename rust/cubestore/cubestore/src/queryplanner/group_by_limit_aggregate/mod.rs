mod group_by_limit_aggregate_stream;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{Distribution, LexRequirement};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{aggregates::*, InputOrderMode};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PhysicalExpr,
    PlanProperties, SendableRecordBatchStream,
};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Worker-side partial hash aggregate that trims its output to the top-k groups by a total order,
/// so far fewer partial-state rows cross the network to the router's Final aggregate.
///
/// This is a custom copy of DataFusion's partial hash aggregate (it reuses DF's `GroupValues` and
/// `GroupsAccumulator` building blocks but owns the consume/emit loop), so the only change required
/// in the DataFusion fork is making `new_group_values` public. The aggregation builds the whole
/// group table and, at the single final emit, keeps only the `k` smallest groups by `order` when
/// the number of groups exceeds `factor * k`; otherwise it emits all groups unchanged.
///
/// `order` is a TOTAL order over groups (ORDER BY columns followed by the remaining group-by
/// columns), expressed as `(partial-output column index, sort options)`. A total order is required
/// for correctness: the same group key can live on multiple workers, and a consistent cut across
/// workers guarantees every partial state the router selects reaches it.
#[derive(Debug, Clone)]
pub struct GroupByLimitAggregateExec {
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
    pub input: Arc<dyn ExecutionPlan>,
    /// Partial-aggregate output schema (group columns followed by accumulator state columns).
    schema: SchemaRef,
    input_schema: SchemaRef,
    cache: PlanProperties,
    /// Fetch count, `k = limit + offset`.
    k: usize,
    /// Only trim when the number of local groups exceeds `factor * k`.
    factor: usize,
    /// Total order over the partial output columns.
    order: Vec<(usize, SortOptions)>,
}

impl GroupByLimitAggregateExec {
    /// Build a `GroupByLimitAggregateExec` from a partial hash `AggregateExec`, or `None` if it is not a
    /// single-group-by partial aggregate (grouping sets and non-partial modes are not supported).
    pub fn try_new_from_partial(
        aggregate: &AggregateExec,
        k: usize,
        factor: usize,
        order: Vec<(usize, SortOptions)>,
    ) -> Option<Self> {
        if *aggregate.mode() != AggregateMode::Partial {
            return None;
        }
        // Sorted-prefix aggregates are handled by InlineAggregateExec; this targets the hash path.
        if matches!(aggregate.input_order_mode(), InputOrderMode::Sorted) {
            return None;
        }
        let group_by = aggregate.group_expr().clone();
        if !group_by.is_single() {
            return None;
        }
        // A global aggregate (no GROUP BY) has zero group columns. `GroupValues` can't be built over
        // an empty schema -- `intern` indexes column 0 and panics -- and such aggregates need no
        // trimming, so leave them to DataFusion.
        if group_by.expr().is_empty() {
            return None;
        }
        let input = aggregate.input().clone();
        // A partial aggregate preserves its input's partitioning (it runs once per input partition).
        // Derive the output partitioning from the input rather than copying the wrapped aggregate's
        // cached value, which can be stale: a later pass may swap our input for one with a different
        // partition count via `with_new_children` without the cache following, and a too-low count
        // makes the parent coalesce read only some partitions and silently drop the rest.
        let cache = aggregate
            .cache()
            .clone()
            .with_partitioning(input.output_partitioning().clone());
        Some(Self {
            group_by,
            aggr_expr: aggregate.aggr_expr().to_vec(),
            filter_expr: aggregate.filter_expr().to_vec(),
            input,
            schema: aggregate.schema().clone(),
            input_schema: aggregate.input_schema().clone(),
            cache,
            k,
            factor,
            order,
        })
    }

    pub fn k(&self) -> usize {
        self.k
    }

    pub fn factor(&self) -> usize {
        self.factor
    }

    pub fn order(&self) -> &[(usize, SortOptions)] {
        &self.order
    }

    pub fn aggr_expr(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggr_expr
    }

    pub fn filter_expr(&self) -> &[Option<Arc<dyn PhysicalExpr>>] {
        &self.filter_expr
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn group_expr(&self) -> &PhysicalGroupBy {
        &self.group_by
    }
}

impl DisplayAs for GroupByLimitAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GroupByLimitAggregateExec: k={}, factor={}, order={:?}",
                    self.k, self.factor, self.order
                )?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for GroupByLimitAggregateExec {
    fn name(&self) -> &'static str {
        "GroupByLimitAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        vec![None]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children[0].clone();
        // Track the (possibly changed) input's partitioning; a partial aggregate preserves it.
        let cache = self
            .cache
            .clone()
            .with_partitioning(input.output_partitioning().clone());
        Ok(Arc::new(Self {
            group_by: self.group_by.clone(),
            aggr_expr: self.aggr_expr.clone(),
            filter_expr: self.filter_expr.clone(),
            input,
            schema: self.schema.clone(),
            input_schema: self.input_schema.clone(),
            cache,
            k: self.k,
            factor: self.factor,
            order: self.order.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream = group_by_limit_aggregate_stream::GroupByLimitAggregateStream::new(
            self, context, partition,
        )?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> DFResult<Statistics> {
        // The trim keeps at most `factor * k` groups per output partition, so the output is bounded
        // by that and by the input row count. Report it (inexact) instead of Absent, which makes
        // downstream planners bail. `factor` is always > 0 here (the rewriter only builds this exec
        // when trimming is enabled), but guard anyway.
        let input_rows = self.input.statistics()?.num_rows;
        let num_rows = if self.factor == 0 {
            input_rows
        } else {
            let parts = self.cache.output_partitioning().partition_count().max(1);
            let cap = self.factor.saturating_mul(self.k).saturating_mul(parts);
            match input_rows {
                Precision::Exact(n) | Precision::Inexact(n) => Precision::Inexact(n.min(cap)),
                Precision::Absent => Precision::Inexact(cap),
            }
        };
        Ok(Statistics {
            num_rows,
            column_statistics: Statistics::unknown_column(&self.schema),
            total_byte_size: Precision::Absent,
        })
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::{collect, ExecutionPlanProperties};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use std::collections::HashSet;

    fn input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("g0", DataType::Utf8, true),
            Field::new("g1", DataType::Int64, true),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    /// `n` distinct (g0, g1) combos, each emitted twice in two different batches; every 7th combo
    /// uses a NULL g1 to exercise null grouping. `v = 1` for every row, so the correct result is `n`
    /// groups with `sum(v) = 2` each.
    fn make_batches(n: usize, batch_rows: usize) -> Vec<RecordBatch> {
        let schema = input_schema();
        let mut g0: Vec<Option<String>> = Vec::new();
        let mut g1: Vec<Option<i64>> = Vec::new();
        for pass in 0..2 {
            // Reverse one pass so the same combo lands in a different batch than the other pass.
            let iter: Box<dyn Iterator<Item = usize>> = if pass == 0 {
                Box::new(0..n)
            } else {
                Box::new((0..n).rev())
            };
            for i in iter {
                // g0 is unique per combo, so each (g0, g1) pair is distinct regardless of g1; g1
                // carries NULLs to exercise null grouping in the multi-column GroupValues path.
                g0.push(Some(format!("a{}", i)));
                g1.push(if i % 7 == 0 {
                    None
                } else {
                    Some((i % 100) as i64)
                });
            }
        }
        let total = g0.len();
        let mut batches = Vec::new();
        let mut start = 0;
        while start < total {
            let end = (start + batch_rows).min(total);
            let s = StringArray::from(g0[start..end].to_vec());
            let l = Int64Array::from(g1[start..end].to_vec());
            let v = Int64Array::from(vec![1i64; end - start]);
            batches.push(
                RecordBatch::try_new(schema.clone(), vec![Arc::new(s), Arc::new(l), Arc::new(v)])
                    .unwrap(),
            );
            start = end;
        }
        batches
    }

    fn partial_aggregate(input: Arc<dyn ExecutionPlan>) -> AggregateExec {
        let schema = input.schema();
        let group_by = PhysicalGroupBy::new_single(vec![
            (col("g0", &schema).unwrap(), "g0".to_string()),
            (col("g1", &schema).unwrap(), "g1".to_string()),
        ]);
        let sum = AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema).unwrap()])
            .schema(schema.clone())
            .alias("sum_v")
            .build()
            .unwrap();
        AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![Arc::new(sum)],
            vec![None],
            input,
            schema,
        )
        .unwrap()
    }

    fn distinct_group_rows(batches: &[RecordBatch]) -> usize {
        let mut seen: HashSet<(Option<String>, Option<i64>)> = HashSet::new();
        for b in batches {
            let g0 = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let g1 = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for r in 0..b.num_rows() {
                let k0 = if g0.is_null(r) {
                    None
                } else {
                    Some(g0.value(r).to_string())
                };
                let k1 = if g1.is_null(r) {
                    None
                } else {
                    Some(g1.value(r))
                };
                seen.insert((k0, k1));
            }
        }
        seen.len()
    }

    /// The no-trim (`k = 0`) partial path must group exactly like DataFusion's stock partial: every
    /// distinct group present in the input must appear exactly once in the partial output, across
    /// many input batches and an output larger than `batch_size`. Reproduces the multi-batch
    /// undercount seen when routing the full hash aggregate through this exec.
    #[tokio::test]
    async fn no_trim_partial_emits_every_group() {
        let n = 20_000;
        let batches = make_batches(n, 4096);
        let expected_groups = distinct_group_rows(&batches);
        assert_eq!(expected_groups, n, "test setup: combos must be distinct");

        let source = MemorySourceConfig::try_new(&vec![batches], input_schema(), None).unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
        let partial = partial_aggregate(input);

        let exec = GroupByLimitAggregateExec::try_new_from_partial(&partial, 0, 0, Vec::new())
            .expect("partial hash aggregate should be wrappable");

        let ctx = Arc::new(TaskContext::default());
        let out = collect(Arc::new(exec), ctx).await.unwrap();

        let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
        let distinct = distinct_group_rows(&out);
        assert_eq!(
            distinct, n,
            "exec emitted {distinct} distinct groups, expected {n}"
        );
        assert_eq!(
            total_rows, n,
            "exec emitted {total_rows} partial rows for {n} distinct groups (duplicate group keys in partial output)"
        );
    }

    /// Our partial output fed into DataFusion's `Final` aggregate (as the router does) must still
    /// yield every group. Reproduces the distributed undercount end-to-end without the cluster.
    #[tokio::test]
    async fn no_trim_partial_then_final_emits_every_group() {
        let n = 20_000;
        let batches = make_batches(n, 4096);

        let source = MemorySourceConfig::try_new(&vec![batches], input_schema(), None).unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
        let partial = partial_aggregate(input);
        let aggr_expr = partial.aggr_expr().to_vec();

        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            GroupByLimitAggregateExec::try_new_from_partial(&partial, 0, 0, Vec::new()).unwrap(),
        );

        let partial_schema = exec.schema();
        let final_group_by = PhysicalGroupBy::new_single(vec![
            (col("g0", &partial_schema).unwrap(), "g0".to_string()),
            (col("g1", &partial_schema).unwrap(), "g1".to_string()),
        ]);
        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            final_group_by,
            aggr_expr,
            vec![None],
            exec,
            partial_schema,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let out = collect(Arc::new(final_agg), ctx).await.unwrap();

        let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
        let distinct = distinct_group_rows(&out);
        assert_eq!(
            distinct, n,
            "final emitted {distinct} distinct groups, expected {n}"
        );
        assert_eq!(
            total_rows, n,
            "final emitted {total_rows} rows, expected {n}"
        );
    }

    /// A partial aggregate preserves its input's partitioning. If our exec keeps a stale
    /// single-partition `cache` after `with_new_children` swaps in a multi-partition input, the
    /// parent `CoalescePartitions` executes only partition 0 and silently drops the rest -- the
    /// distributed undercount we hit on real data. Build the exec over a 1-partition input (so its
    /// cache says 1), re-child it onto a 3-partition input, and require both the reported
    /// partitioning and the aggregated rows to reflect all 3 partitions.
    #[tokio::test]
    async fn output_partitioning_follows_rechilded_input() {
        use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

        let n = 5_000;
        let one_part =
            MemorySourceConfig::try_new(&vec![make_batches(n, 4096)], input_schema(), None)
                .unwrap();
        let one_input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(one_part)));
        let partial = partial_aggregate(one_input);
        let aggr_expr = partial.aggr_expr().to_vec();
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            GroupByLimitAggregateExec::try_new_from_partial(&partial, 0, 0, Vec::new()).unwrap(),
        );
        assert_eq!(exec.output_partitioning().partition_count(), 1);

        // Re-child onto a 3-partition input (same schema), as a later physical-plan pass would.
        let three_parts: Vec<Vec<RecordBatch>> = (0..3).map(|_| make_batches(n, 4096)).collect();
        let three = MemorySourceConfig::try_new(&three_parts, input_schema(), None).unwrap();
        let three_input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(three)));
        let exec3 = exec.with_new_children(vec![three_input]).unwrap();
        assert_eq!(
            exec3.output_partitioning().partition_count(),
            3,
            "exec must report its re-childed input's partition count, not a stale 1"
        );

        // End-to-end: coalesce + final must see every partition's rows.
        let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(exec3));
        let pschema = coalesced.schema();
        let final_gb = PhysicalGroupBy::new_single(vec![
            (col("g0", &pschema).unwrap(), "g0".to_string()),
            (col("g1", &pschema).unwrap(), "g1".to_string()),
        ]);
        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            final_gb,
            aggr_expr,
            vec![None],
            coalesced,
            pschema,
        )
        .unwrap();
        let out = collect(Arc::new(final_agg), Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let distinct = distinct_group_rows(&out);
        let total_v: i64 = out
            .iter()
            .map(|b| {
                let s = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..s.len()).map(|i| s.value(i)).sum::<i64>()
            })
            .sum();
        assert_eq!(
            distinct, n,
            "all {n} groups must survive across 3 partitions"
        );
        // make_batches emits each combo twice; 3 partitions -> 6 rows per group.
        assert_eq!(
            total_v as usize,
            6 * n,
            "all rows from all 3 partitions must be aggregated"
        );
    }

    /// Real-data shape: a high-cardinality `Float64` group column (like `contrib`) at ~400k groups,
    /// fed partial -> final. Floats route through the same multi-column `GroupValues` as strings, but
    /// at this scale across many input batches; reproduces (or rules out) the on-cluster undercount.
    #[tokio::test]
    async fn no_trim_float_high_cardinality_partial_then_final() {
        use datafusion::arrow::array::Float64Array;

        let n: usize = 410_000;
        let batch_rows = 8192;
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float64, true),
            Field::new("g", DataType::Utf8, true),
            Field::new("v", DataType::Int64, false),
        ]));

        // n distinct (f, g) combos, each twice (forward + reversed pass) so duplicates land in
        // different batches; every 9th combo carries NULL f.
        let mut f: Vec<Option<f64>> = Vec::new();
        let mut g: Vec<Option<String>> = Vec::new();
        for pass in 0..2 {
            let iter: Box<dyn Iterator<Item = usize>> = if pass == 0 {
                Box::new(0..n)
            } else {
                Box::new((0..n).rev())
            };
            for i in iter {
                f.push(if i % 9 == 0 {
                    None
                } else {
                    Some(i as f64 * 0.01)
                });
                g.push(Some(format!("g{}", i)));
            }
        }
        let total = f.len();
        let mut batches = Vec::new();
        let mut start = 0;
        while start < total {
            let end = (start + batch_rows).min(total);
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Float64Array::from(f[start..end].to_vec())),
                        Arc::new(StringArray::from(g[start..end].to_vec())),
                        Arc::new(Int64Array::from(vec![1i64; end - start])),
                    ],
                )
                .unwrap(),
            );
            start = end;
        }

        let source = MemorySourceConfig::try_new(&vec![batches], schema.clone(), None).unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
        let group_by = PhysicalGroupBy::new_single(vec![
            (col("f", &schema).unwrap(), "f".to_string()),
            (col("g", &schema).unwrap(), "g".to_string()),
        ]);
        let sum = AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema).unwrap()])
            .schema(schema.clone())
            .alias("sum_v")
            .build()
            .unwrap();
        let partial = AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![Arc::new(sum)],
            vec![None],
            input,
            schema.clone(),
        )
        .unwrap();
        let aggr_expr = partial.aggr_expr().to_vec();

        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            GroupByLimitAggregateExec::try_new_from_partial(&partial, 0, 0, Vec::new()).unwrap(),
        );
        let partial_schema = exec.schema();
        let final_group_by = PhysicalGroupBy::new_single(vec![
            (col("f", &partial_schema).unwrap(), "f".to_string()),
            (col("g", &partial_schema).unwrap(), "g".to_string()),
        ]);
        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            final_group_by,
            aggr_expr,
            vec![None],
            exec,
            partial_schema,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let out = collect(Arc::new(final_agg), ctx).await.unwrap();
        let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, n,
            "final emitted {total_rows} rows, expected {n}"
        );
    }

    /// The worker serializes each partial-output batch to the router with Arrow IPC
    /// (`StreamWriter`). Our exec emits slices of one big `emit(All)` batch (offset > 0), so the IPC
    /// roundtrip must preserve every group of every sliced batch. Reproduces the distributed
    /// undercount, which the in-process tests miss because they never serialize.
    #[tokio::test]
    async fn ipc_roundtrip_of_sliced_output_preserves_every_group() {
        use datafusion::arrow::ipc::reader::StreamReader;
        use datafusion::arrow::ipc::writer::StreamWriter;
        use std::io::Cursor;

        let n = 20_000;
        let batches = make_batches(n, 4096);
        let source = MemorySourceConfig::try_new(&vec![batches], input_schema(), None).unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
        let partial = partial_aggregate(input);
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            GroupByLimitAggregateExec::try_new_from_partial(&partial, 0, 0, Vec::new()).unwrap(),
        );
        let schema = exec.schema();

        let ctx = Arc::new(TaskContext::default());
        let out = collect(Arc::clone(&exec), ctx).await.unwrap();
        assert!(
            out.len() > 1,
            "test must exercise multiple sliced output batches"
        );

        let mut roundtripped = Vec::new();
        for batch in &out {
            let mut buf = Vec::new();
            {
                let mut w = StreamWriter::try_new(Cursor::new(&mut buf), &schema).unwrap();
                w.write(batch).unwrap();
                w.finish().unwrap();
            }
            let mut r = StreamReader::try_new(Cursor::new(buf), None).unwrap();
            roundtripped.push(r.next().unwrap().unwrap());
        }

        let distinct = distinct_group_rows(&roundtripped);
        assert_eq!(
            distinct, n,
            "after IPC roundtrip of sliced batches, {distinct} distinct groups survived, expected {n}"
        );
    }

    /// A global aggregate (no GROUP BY) has zero group columns; wrapping it would build a
    /// `GroupValues` over an empty schema and panic on `intern`. `try_new_from_partial` must decline
    /// it so the full hash routing leaves global aggregates to DataFusion.
    #[tokio::test]
    async fn global_aggregate_is_not_wrapped() {
        let batches = make_batches(100, 64);
        let source = MemorySourceConfig::try_new(&vec![batches], input_schema(), None).unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
        let schema = input.schema();

        let sum = AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema).unwrap()])
            .schema(schema.clone())
            .alias("sum_v")
            .build()
            .unwrap();
        let global = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![]),
            vec![Arc::new(sum)],
            vec![None],
            input,
            schema,
        )
        .unwrap();

        assert!(
            GroupByLimitAggregateExec::try_new_from_partial(&global, 0, 0, Vec::new()).is_none(),
            "global aggregate (no group columns) must not be wrapped"
        );
    }

    /// Mirror the distributed worker shape: a multi-partition scan, our exec running once per
    /// partition, then `CoalescePartitions` and the `Final` aggregate. Every group must survive even
    /// though the same key appears in several partitions' partial outputs.
    #[tokio::test]
    async fn no_trim_multi_partition_then_final_emits_every_group() {
        use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

        let n = 20_000;
        // Same n combos in each of 4 partitions -> heavy cross-partition group overlap.
        let partitions: Vec<Vec<RecordBatch>> = (0..4).map(|_| make_batches(n, 4096)).collect();

        let source = MemorySourceConfig::try_new(&partitions, input_schema(), None).unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
        let partial = partial_aggregate(input);
        let aggr_expr = partial.aggr_expr().to_vec();

        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            GroupByLimitAggregateExec::try_new_from_partial(&partial, 0, 0, Vec::new()).unwrap(),
        );
        assert_eq!(
            exec.output_partitioning().partition_count(),
            4,
            "exec should preserve the input's partition count"
        );
        let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(exec));

        let partial_schema = coalesced.schema();
        let final_group_by = PhysicalGroupBy::new_single(vec![
            (col("g0", &partial_schema).unwrap(), "g0".to_string()),
            (col("g1", &partial_schema).unwrap(), "g1".to_string()),
        ]);
        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            final_group_by,
            aggr_expr,
            vec![None],
            coalesced,
            partial_schema,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let out = collect(Arc::new(final_agg), ctx).await.unwrap();

        let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
        let distinct = distinct_group_rows(&out);
        assert_eq!(
            distinct, n,
            "final emitted {distinct} distinct groups, expected {n}"
        );
        assert_eq!(
            total_rows, n,
            "final emitted {total_rows} rows, expected {n}"
        );
    }
}
