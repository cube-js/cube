use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, ListBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogForeignDataWrapperBuilder {
    oid: UInt32Builder,
    fdwname: StringBuilder,
    fdwowner: UInt32Builder,
    fdwhandler: UInt32Builder,
    fdwvalidator: UInt32Builder,
    fdwacl: ListBuilder<StringBuilder>,
    fdwoptions: ListBuilder<StringBuilder>,
    xmin: UInt32Builder,
}

impl PgCatalogForeignDataWrapperBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            fdwname: StringBuilder::with_capacity(capacity, capacity),
            fdwowner: UInt32Builder::with_capacity(capacity),
            fdwhandler: UInt32Builder::with_capacity(capacity),
            fdwvalidator: UInt32Builder::with_capacity(capacity),
            fdwacl: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
            fdwoptions: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
            xmin: UInt32Builder::with_capacity(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.fdwname.finish()),
            Arc::new(self.fdwowner.finish()),
            Arc::new(self.fdwhandler.finish()),
            Arc::new(self.fdwvalidator.finish()),
            Arc::new(self.fdwacl.finish()),
            Arc::new(self.fdwoptions.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogForeignDataWrapperProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-foreign-data-wrapper.html
impl PgCatalogForeignDataWrapperProvider {
    pub fn new() -> Self {
        let builder = PgCatalogForeignDataWrapperBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogForeignDataWrapperProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("fdwname", DataType::Utf8, false),
            Field::new("fdwowner", DataType::UInt32, false),
            Field::new("fdwhandler", DataType::UInt32, false),
            Field::new("fdwvalidator", DataType::UInt32, false),
            Field::new(
                "fdwacl",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "fdwoptions",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("xmin", DataType::UInt32, false),
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
