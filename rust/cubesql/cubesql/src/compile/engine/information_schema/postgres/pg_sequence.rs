use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, Int64Builder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogSequenceBuilder {
    seqrelid: UInt32Builder,
    seqtypid: UInt32Builder,
    seqstart: Int64Builder,
    seqincrement: Int64Builder,
    seqmax: Int64Builder,
    seqmin: Int64Builder,
    seqcache: Int64Builder,
    seqcycle: BooleanBuilder,
}

impl PgCatalogSequenceBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            seqrelid: UInt32Builder::new(capacity),
            seqtypid: UInt32Builder::new(capacity),
            seqstart: Int64Builder::new(capacity),
            seqincrement: Int64Builder::new(capacity),
            seqmax: Int64Builder::new(capacity),
            seqmin: Int64Builder::new(capacity),
            seqcache: Int64Builder::new(capacity),
            seqcycle: BooleanBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.seqrelid.finish()));
        columns.push(Arc::new(self.seqtypid.finish()));
        columns.push(Arc::new(self.seqstart.finish()));
        columns.push(Arc::new(self.seqincrement.finish()));
        columns.push(Arc::new(self.seqmax.finish()));
        columns.push(Arc::new(self.seqmin.finish()));
        columns.push(Arc::new(self.seqcache.finish()));
        columns.push(Arc::new(self.seqcycle.finish()));

        columns
    }
}

pub struct PgCatalogSequenceProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-sequence.html
impl PgCatalogSequenceProvider {
    pub fn new() -> Self {
        let builder = PgCatalogSequenceBuilder::new(0);

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogSequenceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("seqrelid", DataType::UInt32, false),
            Field::new("seqtypid", DataType::UInt32, false),
            Field::new("seqstart", DataType::Int64, false),
            Field::new("seqincrement", DataType::Int64, false),
            Field::new("seqmax", DataType::Int64, false),
            Field::new("seqmin", DataType::Int64, false),
            Field::new("seqcache", DataType::Int64, false),
            Field::new("seqcycle", DataType::Boolean, false),
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
