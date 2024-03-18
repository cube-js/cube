use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, Int32Builder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogInheritsBuilder {
    inhrelid: UInt32Builder,
    inhparent: UInt32Builder,
    inhseqno: Int32Builder,
    inhdetachpending: BooleanBuilder,
}

impl PgCatalogInheritsBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            inhrelid: UInt32Builder::new(capacity),
            inhparent: UInt32Builder::new(capacity),
            inhseqno: Int32Builder::new(capacity),
            inhdetachpending: BooleanBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.inhrelid.finish()));
        columns.push(Arc::new(self.inhparent.finish()));
        columns.push(Arc::new(self.inhseqno.finish()));
        columns.push(Arc::new(self.inhdetachpending.finish()));

        columns
    }
}

pub struct PgCatalogInheritsProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-inherits.html
impl PgCatalogInheritsProvider {
    pub fn new() -> Self {
        let builder = PgCatalogInheritsBuilder::new(0);

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogInheritsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("inhrelid", DataType::UInt32, true),
            Field::new("inhparent", DataType::UInt32, false),
            Field::new("inhseqno", DataType::Int32, true),
            Field::new("inhdetachpending", DataType::Boolean, false),
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
