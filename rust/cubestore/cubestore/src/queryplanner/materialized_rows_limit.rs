use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::stream::Stream;
use futures::StreamExt;
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

/// Errors out when the wrapped stream produces more than `limit` rows in total across all
/// partitions. Placed at points of the plan where rows get materialized in memory.
#[derive(Debug)]
pub struct MaterializedRowsLimitExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub limit: usize,
    /// Human-readable description of the materialization point, used in the error message.
    pub stage: &'static str,
    rows: Arc<AtomicUsize>,
}

impl MaterializedRowsLimitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: usize, stage: &'static str) -> Self {
        Self {
            input,
            limit,
            stage,
            rows: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl DisplayAs for MaterializedRowsLimitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MaterializedRowsLimitExec, limit: {}, stage: {}",
            self.limit, self.stage
        )
    }
}

#[async_trait]
impl ExecutionPlan for MaterializedRowsLimitExec {
    fn name(&self) -> &str {
        "MaterializedRowsLimitExec"
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
        Ok(Arc::new(MaterializedRowsLimitExec {
            input: children.into_iter().next().unwrap(),
            limit: self.limit,
            stage: self.stage,
            rows: self.rows.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition >= self.input.properties().partitioning.partition_count() {
            return Err(DataFusionError::Internal(format!(
                "MaterializedRowsLimitExec invalid partition {}",
                partition
            )));
        }

        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(MaterializedRowsLimitStream {
            schema: self.schema(),
            limit: self.limit,
            stage: self.stage,
            rows: self.rows.clone(),
            input,
        }))
    }
}

struct MaterializedRowsLimitStream {
    schema: SchemaRef,
    limit: usize,
    stage: &'static str,
    rows: Arc<AtomicUsize>,
    input: SendableRecordBatchStream,
}

impl Stream for MaterializedRowsLimitStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                let total =
                    self.rows.fetch_add(batch.num_rows(), Ordering::Relaxed) + batch.num_rows();
                if total > self.limit {
                    Some(Err(CubeError::user(format!(
                        "Query execution stage '{}' materialized more than {} rows. \
                         Consider creating a pre-aggregation that performs this stage ahead of time.",
                        self.stage, self.limit
                    ))
                    .into()))
                } else {
                    Some(Ok(batch))
                }
            }
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for MaterializedRowsLimitStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::collect;
    use datafusion_datasource::memory::MemorySourceConfig;

    fn batches(sizes: &[usize]) -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batches = sizes
            .iter()
            .map(|size| {
                let array = Int64Array::from((0..*size as i64).collect::<Vec<_>>());
                RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
            })
            .collect();
        (schema, batches)
    }

    async fn run_with_limit(
        sizes: &[usize],
        limit: usize,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let (schema, batches) = batches(sizes);
        let input = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let limited = Arc::new(MaterializedRowsLimitExec::new(input, limit, "test stage"));
        collect(limited, Arc::new(TaskContext::default())).await
    }

    #[tokio::test]
    async fn passes_under_limit() {
        let r = run_with_limit(&[3, 4], 7).await.unwrap();
        assert_eq!(r.iter().map(|b| b.num_rows()).sum::<usize>(), 7);
    }

    #[tokio::test]
    async fn errors_over_limit() {
        let err = run_with_limit(&[3, 4], 6).await.unwrap_err();
        let message = err.to_string();
        assert!(message.contains("'test stage'"), "{}", message);
        assert!(message.contains("pre-aggregation"), "{}", message);
    }
}
