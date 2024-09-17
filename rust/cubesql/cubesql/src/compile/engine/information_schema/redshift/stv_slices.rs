use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int32Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct RedshiftStvSlicesBuilder {
    node: Int32Builder,
    slice: Int32Builder,
    localslice: Int32Builder,
    r#type: StringBuilder,
}

impl RedshiftStvSlicesBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            node: Int32Builder::new(capacity),
            slice: Int32Builder::new(capacity),
            localslice: Int32Builder::new(capacity),
            r#type: StringBuilder::new(capacity),
        }
    }

    fn add_table(&mut self, node: i32, slice: i32, localslice: i32, r#type: impl AsRef<str>) {
        self.node.append_value(node).unwrap();
        self.slice.append_value(slice).unwrap();
        self.localslice.append_value(localslice).unwrap();
        self.r#type.append_value(r#type).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.node.finish()),
            Arc::new(self.slice.finish()),
            Arc::new(self.localslice.finish()),
            Arc::new(self.r#type.finish()),
        ];

        columns
    }
}

pub struct RedshiftStvSlicesProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl RedshiftStvSlicesProvider {
    pub fn new() -> Self {
        let mut builder = RedshiftStvSlicesBuilder::new(1);

        builder.add_table(0, 0, 0, "D");

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for RedshiftStvSlicesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("node", DataType::Int32, false),
            Field::new("slice", DataType::Int32, false),
            Field::new("localslice", DataType::Int32, false),
            Field::new("type", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
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
