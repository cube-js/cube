use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogCastBuilder {
    oid: UInt32Builder,
    castsource: UInt32Builder,
    casttarget: UInt32Builder,
    castfunc: UInt32Builder,
    castcontext: StringBuilder,
    castmethod: StringBuilder,
    xmin: UInt32Builder,
}

impl PgCatalogCastBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::new(capacity),
            castsource: UInt32Builder::new(capacity),
            casttarget: UInt32Builder::new(capacity),
            castfunc: UInt32Builder::new(capacity),
            castcontext: StringBuilder::new(capacity),
            castmethod: StringBuilder::new(capacity),
            xmin: UInt32Builder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.castsource.finish()),
            Arc::new(self.casttarget.finish()),
            Arc::new(self.castfunc.finish()),
            Arc::new(self.castcontext.finish()),
            Arc::new(self.castmethod.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogCastProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-cast.html
impl PgCatalogCastProvider {
    pub fn new() -> Self {
        let builder = PgCatalogCastBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogCastProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("castsource", DataType::UInt32, false),
            Field::new("casttarget", DataType::UInt32, false),
            Field::new("castfunc", DataType::UInt32, false),
            Field::new("castcontext", DataType::Utf8, false),
            Field::new("castmethod", DataType::Utf8, false),
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
