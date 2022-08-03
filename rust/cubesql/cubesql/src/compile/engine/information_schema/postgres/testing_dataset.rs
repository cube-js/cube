use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, Int32Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

pub struct InfoSchemaTestingDatasetProviderBuilder {
    start: usize,
    capacity: usize,
    id: Int32Builder,
    random_str: StringBuilder,
}

impl InfoSchemaTestingDatasetProviderBuilder {
    pub fn new(start: usize, capacity: usize) -> Self {
        Self {
            start,
            capacity,
            id: Int32Builder::new(capacity),
            random_str: StringBuilder::new(capacity),
        }
    }

    pub fn finish(mut self) -> Vec<Arc<dyn Array>> {
        for i in self.start..(self.start + self.capacity) {
            self.id.append_value(i as i32).unwrap();
            self.random_str.append_value("test".to_string()).unwrap();
        }

        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.id.finish()));
        columns.push(Arc::new(self.random_str.finish()));

        columns
    }
}

pub struct InfoSchemaTestingDatasetProvider {
    batches: usize,
    rows_per_batch: usize,
}

impl InfoSchemaTestingDatasetProvider {
    pub fn new(batches: usize, rows_per_batch: usize) -> Self {
        Self {
            batches,
            rows_per_batch,
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaTestingDatasetProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("random_str", DataType::Utf8, false),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut batches: Vec<RecordBatch> = vec![];

        for i in 0..self.batches {
            let builder = InfoSchemaTestingDatasetProviderBuilder::new(
                i * self.rows_per_batch,
                self.rows_per_batch,
            );

            batches.push(RecordBatch::try_new(
                self.schema(),
                builder.finish().to_vec(),
            )?);
        }

        Ok(Arc::new(MemoryExec::try_new(
            &[batches],
            self.schema(),
            projection.clone(),
        )?))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
