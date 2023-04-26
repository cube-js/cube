use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, ListBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogExtensionBuilder {
    oid: UInt32Builder,
    extname: StringBuilder,
    extowner: UInt32Builder,
    extnamespace: UInt32Builder,
    extrelocatable: BooleanBuilder,
    extversion: StringBuilder,
    extconfig: ListBuilder<UInt32Builder>,
    extcondition: ListBuilder<StringBuilder>,
}

impl PgCatalogExtensionBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::new(capacity),
            extname: StringBuilder::new(capacity),
            extowner: UInt32Builder::new(capacity),
            extnamespace: UInt32Builder::new(capacity),
            extrelocatable: BooleanBuilder::new(capacity),
            extversion: StringBuilder::new(capacity),
            extconfig: ListBuilder::new(UInt32Builder::new(capacity)),
            extcondition: ListBuilder::new(StringBuilder::new(capacity)),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.extname.finish()));
        columns.push(Arc::new(self.extowner.finish()));
        columns.push(Arc::new(self.extnamespace.finish()));
        columns.push(Arc::new(self.extrelocatable.finish()));
        columns.push(Arc::new(self.extversion.finish()));
        columns.push(Arc::new(self.extconfig.finish()));
        columns.push(Arc::new(self.extcondition.finish()));

        columns
    }
}

pub struct PgCatalogExtensionProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogExtensionProvider {
    pub fn new() -> Self {
        let builder = PgCatalogExtensionBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogExtensionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("extname", DataType::Utf8, false),
            Field::new("extowner", DataType::UInt32, false),
            Field::new("extnamespace", DataType::UInt32, false),
            Field::new("extrelocatable", DataType::Boolean, false),
            Field::new("extversion", DataType::Utf8, false),
            Field::new(
                "extconfig",
                DataType::List(Box::new(Field::new("item", DataType::UInt32, true))),
                true,
            ),
            Field::new(
                "extcondition",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
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
