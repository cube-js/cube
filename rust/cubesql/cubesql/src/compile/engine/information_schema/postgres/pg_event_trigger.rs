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

struct PgCatalogEventTriggerBuilder {
    oid: UInt32Builder,
    evtname: StringBuilder,
    evtevent: StringBuilder,
    evtowner: UInt32Builder,
    evtfoid: UInt32Builder,
    evtenabled: StringBuilder,
    evttags: ListBuilder<StringBuilder>,
    xmin: UInt32Builder,
}

impl PgCatalogEventTriggerBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::new(capacity),
            evtname: StringBuilder::new(capacity),
            evtevent: StringBuilder::new(capacity),
            evtowner: UInt32Builder::new(capacity),
            evtfoid: UInt32Builder::new(capacity),
            evtenabled: StringBuilder::new(capacity),
            evttags: ListBuilder::new(StringBuilder::new(capacity)),
            xmin: UInt32Builder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.evtname.finish()),
            Arc::new(self.evtevent.finish()),
            Arc::new(self.evtowner.finish()),
            Arc::new(self.evtfoid.finish()),
            Arc::new(self.evtenabled.finish()),
            Arc::new(self.evttags.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogEventTriggerProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-event-trigger.html
impl PgCatalogEventTriggerProvider {
    pub fn new() -> Self {
        let builder = PgCatalogEventTriggerBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogEventTriggerProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("evtname", DataType::Utf8, false),
            Field::new("evtevent", DataType::Utf8, false),
            Field::new("evtowner", DataType::UInt32, false),
            Field::new("evtfoid", DataType::UInt32, false),
            Field::new("evtenabled", DataType::Utf8, false),
            Field::new(
                "evttags",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
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
