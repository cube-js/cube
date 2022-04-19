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

struct PgCatalogDependBuilder {
    classid: UInt32Builder,
    objid: UInt32Builder,
    objsubid: Int32Builder,
    refclassid: UInt32Builder,
    refobjid: UInt32Builder,
    refobjsubid: Int32Builder,
    deptype: StringBuilder,
}

impl PgCatalogDependBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            classid: UInt32Builder::new(capacity),
            objid: UInt32Builder::new(capacity),
            objsubid: Int32Builder::new(capacity),
            refclassid: UInt32Builder::new(capacity),
            refobjid: UInt32Builder::new(capacity),
            refobjsubid: Int32Builder::new(capacity),
            deptype: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.classid.finish()));
        columns.push(Arc::new(self.objid.finish()));
        columns.push(Arc::new(self.objsubid.finish()));
        columns.push(Arc::new(self.refclassid.finish()));
        columns.push(Arc::new(self.refobjid.finish()));
        columns.push(Arc::new(self.refobjsubid.finish()));
        columns.push(Arc::new(self.deptype.finish()));

        columns
    }
}

pub struct PgCatalogDependProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogDependProvider {
    pub fn new() -> Self {
        let builder = PgCatalogDependBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogDependProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("classid", DataType::UInt32, false),
            Field::new("objid", DataType::UInt32, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("refclassid", DataType::UInt32, false),
            Field::new("refobjid", DataType::UInt32, false),
            Field::new("refobjsubid", DataType::Int32, false),
            Field::new("deptype", DataType::Utf8, false),
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
