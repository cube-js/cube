use async_trait::async_trait;
use datafusion::arrow::array::{make_array, Array, ArrayRef, MutableArrayData};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::cube_ext;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::stream::Stream;
use futures::{Future, StreamExt};
use pin_project_lite::pin_project;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Returns the last `limit` rows of each input partition.
#[derive(Debug)]
pub struct TailLimitExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub limit: usize,
}

impl TailLimitExec {
    /// Create a new MergeExec
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: usize) -> Self {
        TailLimitExec { input, limit }
    }
}

impl DisplayAs for TailLimitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TailLimitExec")
    }
}

#[async_trait]
impl ExecutionPlan for TailLimitExec {
    fn name(&self) -> &str {
        "TailLimitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(TailLimitExec {
            input: children.into_iter().next().unwrap(),
            limit: self.limit,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(TailLimitStream::new(input, self.limit)))
    }
}

pin_project! {
/// A Reverse stream reverse rows order
    struct TailLimitStream {
        schema: SchemaRef,
        #[pin]
        output: futures::channel::oneshot::Receiver<Result<RecordBatch, DataFusionError>>,
        loaded_input: Option<Vec<RecordBatch>>,
        finished: bool
    }
}

impl TailLimitStream {
    fn new(input: SendableRecordBatchStream, n: usize) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel();
        let schema = input.schema();
        let task = async move {
            let schema = input.schema();
            let data = collect_tail_window(input, n).await?;
            batches_tail(data, n, schema.clone())
        };
        cube_ext::spawn_oneshot_with_catch_unwind(task, tx);

        Self {
            schema,
            output: rx,
            loaded_input: None,
            finished: false,
        }
    }
}

/// Collects a sliding tail window of the input: keeps only the trailing batches needed to cover
/// `limit` rows, newer rows displace older ones. Every stored batch is cut to at most `limit`
/// rows on arrival and the eviction keeps the minimal covering suffix, so the window never holds
/// more than 2 * `limit` rows. The front batch may overshoot the window, it is sliced later by
/// [batches_tail].
async fn collect_tail_window(
    mut input: SendableRecordBatchStream,
    limit: usize,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut window = VecDeque::new();
    let mut total_rows = 0;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        let rows = batch.num_rows();
        if rows >= limit {
            // The batch alone covers the whole window
            window.clear();
            total_rows = limit;
            window.push_back(if rows > limit {
                skip_first_rows(&batch, rows - limit)
            } else {
                batch
            });
            continue;
        }
        total_rows += rows;
        window.push_back(batch);
        while window.len() > 1 && total_rows - window.front().unwrap().num_rows() >= limit {
            total_rows -= window.pop_front().unwrap().num_rows();
        }
    }
    Ok(window.into())
}

fn batches_tail(
    mut batches: Vec<RecordBatch>,
    limit: usize,
    schema: SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let mut rest = limit;
    let mut merge_from = 0;
    for (i, batch) in batches.iter_mut().enumerate().rev() {
        if rest > batch.num_rows() {
            rest -= batch.num_rows();
        } else {
            if rest < batch.num_rows() {
                let offset = batch.num_rows() - rest;
                *batch = skip_first_rows(&batch, offset)
            }
            merge_from = i;
            break;
        }
    }
    let result = concat_batches(&schema, &batches[merge_from..batches.len()])?;
    Ok(result)
}

pub fn skip_first_rows(batch: &RecordBatch, n: usize) -> RecordBatch {
    let sliced_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| {
            // We only do the copy to make sure IPC serialization does not mess up later.
            // Currently, after a roundtrip through IPC, arrays always start at offset 0.
            // TODO: fix IPC serialization and use c.slice().
            let d = c.to_data();
            let mut data = MutableArrayData::new(vec![&d], false, c.len() - n);
            data.extend(0, n, c.len());
            make_array(data.freeze())
        })
        .collect();

    RecordBatch::try_new(batch.schema(), sliced_columns).unwrap()
}

impl Stream for TailLimitStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // is the output ready?
        let this = self.project();
        let output_poll = this.output.poll(cx);

        match output_poll {
            Poll::Ready(result) => {
                *this.finished = true;

                // check for error in receiving channel and unwrap actual result
                let result = match result {
                    Err(e) => Some(Err(DataFusionError::Execution(format!(
                        "Error receiving tail limit: {}",
                        e
                    )))), // error receiving
                    Ok(result) => Some(result), // TODO upgrade DF: .transpose(),
                };

                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TailLimitStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::queryplanner::try_make_memory_data_source;

    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::collect as result_collect;
    use itertools::Itertools;

    fn ints_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
    }

    fn ints(d: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(ints_schema(), vec![Arc::new(Int64Array::from(d))]).unwrap()
    }

    fn to_ints(rs: Vec<RecordBatch>) -> Vec<Vec<i64>> {
        rs.into_iter()
            .map(|r| {
                r.columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn one_batch() {
        let input = vec![ints(vec![1, 2, 3, 4])];

        let schema = ints_schema();
        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 3)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![2, 3, 4],
        );

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 4)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![1, 2, 3, 4],
        );

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 8)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![1, 2, 3, 4],
        );

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 1)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(to_ints(r).into_iter().flatten().collect_vec(), vec![4],);

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 0)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert!(to_ints(r).into_iter().flatten().collect_vec().is_empty());
    }

    #[tokio::test]
    async fn batches_larger_than_limit() {
        // 20-row batch followed by a 3-row batch, limit 5: last 2 rows of the big batch + 3
        let big: Vec<i64> = (0..20).collect();
        let input = vec![ints(big), ints(vec![100, 101, 102])];
        let inp = try_make_memory_data_source(&vec![input], ints_schema(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 5)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![18, 19, 100, 101, 102],
        );
    }

    /// The window must stay bounded by the limit, not by the largest input batch.
    #[tokio::test]
    async fn window_stays_bounded() {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        // A large batch first, then a trickle of small ones that never covers the limit
        let mut batches = vec![ints((0..1000).collect())];
        batches.extend((0..10).map(|i| ints(vec![i])));

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            ints_schema(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        let window = collect_tail_window(stream, 20).await.unwrap();

        let window_rows: usize = window.iter().map(|b| b.num_rows()).sum();
        assert!(
            window_rows <= 2 * 20,
            "window holds {} rows, must stay within 2 * limit",
            window_rows
        );
        let result = batches_tail(window, 20, ints_schema()).unwrap();
        let last_20: Vec<i64> = (990..1000).chain(0..10).collect();
        assert_eq!(
            to_ints(vec![result]).into_iter().flatten().collect_vec(),
            last_20
        );
    }

    #[tokio::test]
    async fn empty_partition() {
        let partitions = vec![vec![], vec![ints(vec![1, 2])]];
        let inp = try_make_memory_data_source(&partitions, ints_schema(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 2)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(to_ints(r).into_iter().flatten().collect_vec(), vec![1, 2]);
    }

    #[tokio::test]
    async fn multiple_partitions() {
        let partitions = vec![
            vec![ints(vec![1, 2, 3]), ints(vec![4, 5])],
            vec![ints(vec![10, 20])],
        ];
        let inp = try_make_memory_data_source(&partitions, ints_schema(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 2)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        // The last 2 rows of each partition
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![4, 5, 10, 20],
        );
    }

    #[tokio::test]
    async fn several_batches() {
        let input = vec![
            ints(vec![1, 2, 3, 4]),
            ints(vec![5, 6]),
            ints(vec![]),
            ints(vec![7]),
            ints(vec![8, 9, 10]),
        ];

        let schema = ints_schema();
        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 2)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(to_ints(r).into_iter().flatten().collect_vec(), vec![9, 10],);

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 3)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![8, 9, 10],
        );

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 4)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![7, 8, 9, 10],
        );

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 5)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![6, 7, 8, 9, 10],
        );

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 10)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        );

        let inp = try_make_memory_data_source(&vec![input.clone()], schema.clone(), None).unwrap();
        let r = result_collect(
            Arc::new(TailLimitExec::new(inp, 100)),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        );
    }
}
