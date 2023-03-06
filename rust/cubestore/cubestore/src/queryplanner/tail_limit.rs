use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::cube_ext;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::common::{collect, combine_batches};
use datafusion::physical_plan::skip::skip_first_rows;
use datafusion::physical_plan::{
    ExecutionPlan, OptimizerHints, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use flatbuffers::bitflags::_core::any::Any;
use futures::stream::Stream;
use futures::Future;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

///Return n last rows in input
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

#[async_trait]
impl ExecutionPlan for TailLimitExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
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
        Ok(Arc::new(TailLimitExec {
            input: children.into_iter().next().unwrap(),
            limit: self.limit,
        }))
    }

    fn output_hints(&self) -> OptimizerHints {
        self.input.output_hints()
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "TailLimitExec invalid partition {}",
                partition
            )));
        }

        if 1 != self.input.output_partitioning().partition_count() {
            return Err(DataFusionError::Internal(
                "TailLimitExec requires a single input partition".to_owned(),
            ));
        }

        let input = self.input.execute(partition).await?;
        Ok(Box::pin(TailLimitStream::new(input, self.limit)))
    }
}

pin_project! {
/// A Reverse stream reverse rows order
    struct TailLimitStream {
        schema: SchemaRef,
        #[pin]
        output: futures::channel::oneshot::Receiver<ArrowResult<Option<RecordBatch>>>,
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
            let data = collect(input)
                .await
                .map_err(DataFusionError::into_arrow_external_error)?;
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

fn batches_tail(
    mut batches: Vec<RecordBatch>,
    limit: usize,
    schema: SchemaRef,
) -> ArrowResult<Option<RecordBatch>> {
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
    let result = combine_batches(&batches[merge_from..batches.len()], schema.clone())?;
    Ok(result)
}

impl Stream for TailLimitStream {
    type Item = ArrowResult<RecordBatch>;

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
                    Err(e) => Some(Err(ArrowError::ExternalError(Box::new(e)))), // error receiving
                    Ok(result) => result.transpose(),
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
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::collect as result_collect;
    use datafusion::physical_plan::memory::MemoryExec;
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
        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 3)))
            .await
            .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![2, 3, 4],
        );

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 4)))
            .await
            .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![1, 2, 3, 4],
        );

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 8)))
            .await
            .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![1, 2, 3, 4],
        );

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 1)))
            .await
            .unwrap();
        assert_eq!(to_ints(r).into_iter().flatten().collect_vec(), vec![4],);

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 0)))
            .await
            .unwrap();
        assert!(to_ints(r).into_iter().flatten().collect_vec().is_empty());
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
        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 2)))
            .await
            .unwrap();
        assert_eq!(to_ints(r).into_iter().flatten().collect_vec(), vec![9, 10],);

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 3)))
            .await
            .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![8, 9, 10],
        );

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 4)))
            .await
            .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![7, 8, 9, 10],
        );

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 5)))
            .await
            .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![6, 7, 8, 9, 10],
        );

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 10)))
            .await
            .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        );

        let inp =
            Arc::new(MemoryExec::try_new(&vec![input.clone()], schema.clone(), None).unwrap());
        let r = result_collect(Arc::new(TailLimitExec::new(inp, 100)))
            .await
            .unwrap();
        assert_eq!(
            to_ints(r).into_iter().flatten().collect_vec(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        );
    }
}
