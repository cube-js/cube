use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Float32Builder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogEnumBuilder {
    oid: UInt32Builder,
    enumtypid: UInt32Builder,
    enumsortorder: Float32Builder,
    enumlabel: StringBuilder,
}

impl PgCatalogEnumBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::new(capacity),
            enumtypid: UInt32Builder::new(capacity),
            enumsortorder: Float32Builder::new(capacity),
            enumlabel: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.enumtypid.finish()));
        columns.push(Arc::new(self.enumsortorder.finish()));
        columns.push(Arc::new(self.enumlabel.finish()));

        columns
    }
}

pub struct PgCatalogEnumProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogEnumProvider {
    pub fn new() -> Self {
        let builder = PgCatalogEnumBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogEnumProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("enumtypid", DataType::UInt32, false),
            Field::new("enumsortorder", DataType::Float32, false),
            Field::new("enumlabel", DataType::Utf8, false),
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
