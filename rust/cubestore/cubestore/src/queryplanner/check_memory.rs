use crate::util::memory::MemoryHandler;
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
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct CheckMemoryExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub memory_handler: Arc<dyn MemoryHandler>,
}

impl CheckMemoryExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, memory_handler: Arc<dyn MemoryHandler>) -> Self {
        Self {
            input,
            memory_handler,
        }
    }
}

#[async_trait]
impl ExecutionPlan for CheckMemoryExec {
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
        Ok(Arc::new(CheckMemoryExec {
            input: children.into_iter().next().unwrap(),
            memory_handler: self.memory_handler.clone(),
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
        Ok(Box::pin(CheckMemoryStream {
            schema: self.schema(),
            memory_handler: self.memory_handler.clone(),
            input,
        }))
    }
}

struct CheckMemoryStream {
    schema: SchemaRef,
    memory_handler: Arc<dyn MemoryHandler>,
    input: SendableRecordBatchStream,
}

impl Stream for CheckMemoryStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                let r = self
                    .memory_handler
                    .check_memory()
                    .map(|_| batch)
                    .map_err(|e| e.into());
                Some(r)
            }
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for CheckMemoryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
