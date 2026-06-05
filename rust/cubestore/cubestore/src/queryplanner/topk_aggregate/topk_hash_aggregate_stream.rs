use datafusion::arrow::array::{ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::dfschema::internal_err;
use datafusion::error::Result as DFResult;
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};
use datafusion::physical_expr::GroupsAccumulatorAdapter;
use datafusion::physical_plan::aggregates::group_values::{new_group_values, GroupValues};
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, SendableRecordBatchStream};
use futures::ready;
use futures::stream::{Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::TopKHashAggregateExec;

enum ExecutionState {
    ReadingInput,
    ProducingOutput(RecordBatch),
    Done,
}

pub(crate) struct TopKHashAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    group_by: PhysicalGroupBy,
    batch_size: usize,
    exec_state: ExecutionState,
    input_done: bool,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    group_values: Box<dyn GroupValues>,
    current_group_indices: Vec<usize>,
    k: usize,
    factor: usize,
    order: Vec<(usize, SortOptions)>,
}

impl TopKHashAggregateStream {
    pub fn new(
        agg: &TopKHashAggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> DFResult<Self> {
        let agg_schema = Arc::clone(&agg.schema());
        let agg_group_by = agg.group_expr().clone();
        let agg_filter_expr = agg.filter_expr().to_vec();

        let batch_size = context.session_config().batch_size();
        let input = agg.input().execute(partition, Arc::clone(&context))?;

        let aggregate_arguments =
            aggregate_expressions(agg.aggr_expr(), agg_group_by.num_group_exprs())?;

        let accumulators: Vec<_> = agg
            .aggr_expr()
            .iter()
            .map(create_group_accumulator)
            .collect::<DFResult<_>>()?;

        let group_schema = agg_group_by.group_schema(&agg.input().schema())?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;

        Ok(TopKHashAggregateStream {
            schema: agg_schema,
            input,
            aggregate_arguments,
            filter_expressions: agg_filter_expr,
            group_by: agg_group_by,
            batch_size,
            exec_state: ExecutionState::ReadingInput,
            input_done: false,
            accumulators,
            group_values,
            current_group_indices: Vec::with_capacity(batch_size),
            k: agg.k(),
            factor: agg.factor(),
            order: agg.order().to_vec(),
        })
    }
}

impl Stream for TopKHashAggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match &self.exec_state {
                ExecutionState::ReadingInput => match ready!(self.input.poll_next_unpin(cx)) {
                    Some(Ok(batch)) => {
                        if let Err(e) = self.group_aggregate_batch(batch) {
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    // Input exhausted: emit the whole group table at once, then trim to top-k.
                    None => {
                        self.input_done = true;
                        match self.emit_all_trimmed() {
                            Ok(Some(batch)) => {
                                self.exec_state = ExecutionState::ProducingOutput(batch)
                            }
                            Ok(None) => self.exec_state = ExecutionState::Done,
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                },

                ExecutionState::ProducingOutput(batch) => {
                    let batch = batch.clone();
                    let size = self.batch_size;
                    let (next_state, output) = if batch.num_rows() <= size {
                        (ExecutionState::Done, batch)
                    } else {
                        let remaining = batch.slice(size, batch.num_rows() - size);
                        let output = batch.slice(0, size);
                        (ExecutionState::ProducingOutput(remaining), output)
                    };
                    self.exec_state = next_state;
                    return Poll::Ready(Some(Ok(output)));
                }

                ExecutionState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for TopKHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl TopKHashAggregateStream {
    fn group_aggregate_batch(&mut self, batch: RecordBatch) -> DFResult<()> {
        let group_by_values = evaluate_group_by(&self.group_by, &batch)?;
        let input_values = evaluate_many(&self.aggregate_arguments, &batch)?;
        let filter_values = evaluate_optional(&self.filter_expressions, &batch)?;

        assert_eq!(group_by_values.len(), 1, "Exactly 1 group value required");
        self.group_values
            .intern(&group_by_values[0], &mut self.current_group_indices)?;
        let group_indices = &self.current_group_indices;
        let total_num_groups = self.group_values.len();

        for ((acc, values), opt_filter) in self
            .accumulators
            .iter_mut()
            .zip(input_values.iter())
            .zip(filter_values.iter())
        {
            let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());
            acc.update_batch(values, group_indices, opt_filter, total_num_groups)?;
        }
        Ok(())
    }

    /// Build the partial-state batch for all groups, then keep only the `k` smallest by the total
    /// order when the number of groups exceeds `factor * k`.
    fn emit_all_trimmed(&mut self) -> DFResult<Option<RecordBatch>> {
        if self.group_values.is_empty() {
            return Ok(None);
        }
        let mut columns = self.group_values.emit(EmitTo::All)?;
        for acc in &mut self.accumulators {
            columns.extend(acc.state(EmitTo::All)?);
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        Ok(Some(self.trim_top_k(batch)?))
    }

    fn trim_top_k(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let g = batch.num_rows();
        if self.k == 0 || g <= self.factor.saturating_mul(self.k) {
            return Ok(batch);
        }
        let sort_columns: Vec<SortColumn> = self
            .order
            .iter()
            .map(|(idx, options)| SortColumn {
                values: Arc::clone(batch.column(*idx)),
                options: Some(*options),
            })
            .collect();
        let indices = lexsort_to_indices(&sort_columns, Some(self.k))?;
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }
}

/// Partial-aggregate argument expressions, one vec per aggregate. Mirrors DataFusion's private
/// `aggregate_expressions` for `AggregateMode::Partial`.
fn aggregate_expressions(
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    _col_idx_base: usize,
) -> DFResult<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    Ok(aggr_expr
        .iter()
        .map(|agg| {
            let mut result = agg.expressions();
            if let Some(ordering_req) = agg.order_bys() {
                result.extend(ordering_req.iter().map(|item| Arc::clone(&item.expr)));
            }
            result
        })
        .collect())
}

fn create_group_accumulator(
    agg_expr: &Arc<AggregateFunctionExpr>,
) -> DFResult<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator()
    } else {
        let agg_expr_captured = Arc::clone(agg_expr);
        let factory = move || agg_expr_captured.create_accumulator();
        Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
    }
}

fn evaluate(expr: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> DFResult<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| {
            expr.evaluate(batch)
                .and_then(|v| v.into_array(batch.num_rows()))
        })
        .collect()
}

fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> DFResult<Vec<Vec<ArrayRef>>> {
    expr.iter().map(|expr| evaluate(expr, batch)).collect()
}

fn evaluate_optional(
    expr: &[Option<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> DFResult<Vec<Option<ArrayRef>>> {
    expr.iter()
        .map(|expr| {
            expr.as_ref()
                .map(|expr| {
                    expr.evaluate(batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .transpose()
        })
        .collect()
}

fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> DFResult<Vec<Vec<ArrayRef>>> {
    let exprs: Vec<ArrayRef> = group_by
        .expr()
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            value.into_array(batch.num_rows())
        })
        .collect::<DFResult<Vec<_>>>()?;

    if !group_by.is_single() {
        return internal_err!("TopKHashAggregate does not support grouping sets");
    }

    Ok(vec![exprs])
}
