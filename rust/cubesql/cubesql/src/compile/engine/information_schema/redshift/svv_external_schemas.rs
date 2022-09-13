use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, Int16Builder, Int32Builder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct RedshiftSvvExternalSchemasBuilder {
    esoid: UInt32Builder,
    eskind: Int16Builder,
    schemaname: StringBuilder,
    esowner: Int32Builder,
    databasename: StringBuilder,
    esoptions: StringBuilder,
}

impl RedshiftSvvExternalSchemasBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            esoid: UInt32Builder::new(capacity),
            eskind: Int16Builder::new(capacity),
            schemaname: StringBuilder::new(capacity),
            esowner: Int32Builder::new(capacity),
            databasename: StringBuilder::new(capacity),
            esoptions: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.esoid.finish()));
        columns.push(Arc::new(self.eskind.finish()));
        columns.push(Arc::new(self.schemaname.finish()));
        columns.push(Arc::new(self.esowner.finish()));
        columns.push(Arc::new(self.databasename.finish()));
        columns.push(Arc::new(self.esoptions.finish()));

        columns
    }
}

pub struct RedshiftSvvExternalSchemasTableProvider {}

impl RedshiftSvvExternalSchemasTableProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProvider for RedshiftSvvExternalSchemasTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("esoid", DataType::UInt32, false),
            Field::new("eskind", DataType::Int16, false),
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("esowner", DataType::Int32, false),
            Field::new("databasename", DataType::Utf8, false),
            Field::new("esoptions", DataType::Utf8, true),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let builder = RedshiftSvvExternalSchemasBuilder::new(0);
        let batch = RecordBatch::try_new(self.schema(), builder.finish())?;

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
