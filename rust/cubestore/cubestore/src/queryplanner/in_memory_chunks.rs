use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// Scans the in-memory chunks of one partition as a single stream sorted by the index key.
/// Each chunk is sorted on its own; they are merged at execution, so a scan over many chunks
/// plans one node instead of one input with its own operators per chunk.
pub struct InMemoryChunksExec {
    chunks: Vec<Vec<RecordBatch>>,
    chunk_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    sort_order: LexOrdering,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl InMemoryChunksExec {
    /// `sort_order` refers to the projected schema.
    pub fn try_new(
        chunks: Vec<Vec<RecordBatch>>,
        chunk_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        sort_order: LexOrdering,
    ) -> Result<Self, DataFusionError> {
        let schema = match &projection {
            Some(p) => Arc::new(chunk_schema.project(p)?),
            None => chunk_schema.clone(),
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, &[sort_order.clone()]),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            chunks,
            chunk_schema,
            projection,
            sort_order,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

// Not derived: the chunks' data would end up in plan dumps.
impl std::fmt::Debug for InMemoryChunksExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryChunksExec")
            .field("chunks", &self.chunks.len())
            .field("projection", &self.projection)
            .field("sort_order", &self.sort_order)
            .finish()
    }
}

impl DisplayAs for InMemoryChunksExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InMemoryChunksExec: chunks={}", self.chunks.len())
    }
}

#[async_trait]
impl ExecutionPlan for InMemoryChunksExec {
    fn name(&self) -> &str {
        "InMemoryChunksExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "InMemoryChunksExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "InMemoryChunksExec has one partition, got {}",
                partition
            )));
        }
        let mut streams = self
            .chunks
            .iter()
            .map(
                |batches| -> Result<SendableRecordBatchStream, DataFusionError> {
                    Ok(Box::pin(MemoryStream::try_new(
                        batches.clone(),
                        self.chunk_schema.clone(),
                        self.projection.clone(),
                    )?))
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        if streams.len() == 1 {
            return Ok(streams.remove(0));
        }
        let reservation = MemoryConsumer::new("InMemoryChunksExec").register(context.memory_pool());
        StreamingMergeBuilder::new()
            .with_streams(streams)
            .with_schema(self.schema())
            .with_expressions(&self.sort_order)
            .with_metrics(BaselineMetrics::new(&self.metrics, partition))
            .with_batch_size(context.session_config().batch_size())
            .with_reservation(reservation)
            // Deterministic order for rows with equal keys: they come in chunk order.
            .with_round_robin_tie_breaker(false)
            .build()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Session config extension: when set to true, table scans read the in-memory chunks of a
/// partition through one [InMemoryChunksExec].
#[derive(Debug)]
pub struct MergeInMemoryChunks(pub bool);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::collect;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]))
    }

    fn batch(rows: &[(i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            ],
        )
        .unwrap()
    }

    fn column(batches: &[RecordBatch], i: usize) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                let a = b.column(i).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..a.len()).map(|r| a.value(r)).collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn merges_chunks_keeping_chunk_order_on_ties() {
        let by_key = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("key", 0)),
            SortOptions::default(),
        )]);
        let exec = InMemoryChunksExec::try_new(
            vec![
                vec![batch(&[(1, 10), (3, 11)]), batch(&[(5, 12)])],
                vec![batch(&[(2, 20), (3, 21), (4, 22)])],
                vec![batch(&[(3, 30)])],
            ],
            schema(),
            None,
            by_key,
        )
        .unwrap();
        let r = collect(Arc::new(exec), Arc::new(TaskContext::default()))
            .await
            .unwrap();
        assert_eq!(column(&r, 0), vec![1, 2, 3, 3, 3, 4, 5]);
        // Equal keys come in chunk order.
        assert_eq!(column(&r, 1), vec![10, 20, 11, 21, 30, 22, 12]);
    }

    #[tokio::test]
    async fn projects_chunk_columns() {
        let by_val = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("val", 0)),
            SortOptions::default(),
        )]);
        let exec = InMemoryChunksExec::try_new(
            vec![vec![batch(&[(1, 10)])], vec![batch(&[(2, 5)])]],
            schema(),
            Some(vec![1]),
            by_val,
        )
        .unwrap();
        assert_eq!(exec.schema().fields().len(), 1);
        let r = collect(Arc::new(exec), Arc::new(TaskContext::default()))
            .await
            .unwrap();
        assert_eq!(column(&r, 0), vec![5, 10]);
    }
}
