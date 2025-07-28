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

struct PgCatalogForeignTableBuilder {
    ftrelid: UInt32Builder,
    ftserver: UInt32Builder,
    ftoptions: ListBuilder<StringBuilder>,
}

impl PgCatalogForeignTableBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            ftrelid: UInt32Builder::new(capacity),
            ftserver: UInt32Builder::new(capacity),
            ftoptions: ListBuilder::new(StringBuilder::new(capacity)),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.ftrelid.finish()),
            Arc::new(self.ftserver.finish()),
            Arc::new(self.ftoptions.finish()),
        ];

        columns
    }
}

pub struct PgCatalogForeignTableProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-foreign-table.html
impl PgCatalogForeignTableProvider {
    pub fn new() -> Self {
        let builder = PgCatalogForeignTableBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogForeignTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ftrelid", DataType::UInt32, false),
            Field::new("ftserver", DataType::UInt32, false),
            Field::new(
                "ftoptions",
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
