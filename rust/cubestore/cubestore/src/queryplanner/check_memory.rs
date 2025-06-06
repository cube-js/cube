use crate::util::memory::MemoryHandler;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use flatbuffers::bitflags::_core::any::Any;
use futures::stream::Stream;
use futures::StreamExt;
use std::fmt::Formatter;
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

impl DisplayAs for CheckMemoryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CheckMemoryExec")
    }
}

#[async_trait]
impl ExecutionPlan for CheckMemoryExec {
    fn name(&self) -> &str {
        "CheckMemoryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(CheckMemoryExec {
            input: children.into_iter().next().unwrap(),
            memory_handler: self.memory_handler.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition >= self.input.properties().partitioning.partition_count() {
            return Err(DataFusionError::Internal(format!(
                "ExecutionPlanExec invalid partition {}",
                partition
            )));
        }

        let input = self.input.execute(partition, context)?;
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
    type Item = Result<RecordBatch, DataFusionError>;

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
