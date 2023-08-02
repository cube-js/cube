use crate::util::batch_memory::record_batch_buffer_size;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{
    ExecutionPlan, OptimizerHints, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use flatbuffers::bitflags::_core::any::Any;
use futures::stream::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct DataLoadedSize {
    size: AtomicUsize,
}

impl DataLoadedSize {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            size: AtomicUsize::new(0),
        })
    }

    pub fn add(&self, size: usize) {
        self.size.fetch_add(size, Ordering::SeqCst);
    }

    pub fn get(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct TraceDataLoadedExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub data_loaded_size: Arc<DataLoadedSize>,
}

impl TraceDataLoadedExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, data_loaded_size: Arc<DataLoadedSize>) -> Self {
        Self {
            input,
            data_loaded_size,
        }
    }
}

#[async_trait]
impl ExecutionPlan for TraceDataLoadedExec {
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
        Ok(Arc::new(Self {
            input: children.into_iter().next().unwrap(),
            data_loaded_size: self.data_loaded_size.clone(),
        }))
    }

    fn output_hints(&self) -> OptimizerHints {
        self.input.output_hints()
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition >= self.input.output_partitioning().partition_count() {
            return Err(DataFusionError::Internal(format!(
                "ExecutionPlanExec invalid partition {}",
                partition
            )));
        }

        let input = self.input.execute(partition).await?;
        Ok(Box::pin(TraceDataLoadedStream {
            schema: self.schema(),
            data_loaded_size: self.data_loaded_size.clone(),
            input,
        }))
    }
}

struct TraceDataLoadedStream {
    schema: SchemaRef,
    data_loaded_size: Arc<DataLoadedSize>,
    input: SendableRecordBatchStream,
}

impl Stream for TraceDataLoadedStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                self.data_loaded_size.add(record_batch_buffer_size(&batch));
                Some(Ok(batch))
            }
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for TraceDataLoadedStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
