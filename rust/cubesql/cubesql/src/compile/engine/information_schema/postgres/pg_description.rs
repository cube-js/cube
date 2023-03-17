use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int32Builder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogDescriptionBuilder {
    objoid: UInt32Builder,
    classoid: UInt32Builder,
    objsubid: Int32Builder,
    description: StringBuilder,
}

impl PgCatalogDescriptionBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            objoid: UInt32Builder::new(capacity),
            classoid: UInt32Builder::new(capacity),
            objsubid: Int32Builder::new(capacity),
            description: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.objoid.finish()));
        columns.push(Arc::new(self.classoid.finish()));
        columns.push(Arc::new(self.objsubid.finish()));
        columns.push(Arc::new(self.description.finish()));

        columns
    }
}

pub struct PgCatalogDescriptionProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogDescriptionProvider {
    pub fn new() -> Self {
        let builder = PgCatalogDescriptionBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogDescriptionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("objoid", DataType::UInt32, false),
            Field::new("classoid", DataType::UInt32, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("description", DataType::Utf8, false),
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
