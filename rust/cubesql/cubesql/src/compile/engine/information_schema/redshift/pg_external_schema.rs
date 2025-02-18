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

struct RedshiftPgExternalSchemaBuilder {
    esoid: UInt32Builder,
    eskind: Int32Builder,
    esdbname: StringBuilder,
    esoptions: StringBuilder,
}

impl RedshiftPgExternalSchemaBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            esoid: UInt32Builder::new(capacity),
            eskind: Int32Builder::new(capacity),
            esdbname: StringBuilder::new(capacity),
            esoptions: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.esoid.finish()),
            Arc::new(self.eskind.finish()),
            Arc::new(self.esdbname.finish()),
            Arc::new(self.esoptions.finish()),
        ];

        columns
    }
}

pub struct RedshiftPgExternalSchemaProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl RedshiftPgExternalSchemaProvider {
    pub fn new() -> Self {
        let builder = RedshiftPgExternalSchemaBuilder::new(0);

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for RedshiftPgExternalSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("esoid", DataType::UInt32, false),
            Field::new("eskind", DataType::Int32, false),
            Field::new("esdbname", DataType::Utf8, false),
            Field::new("esoptions", DataType::Utf8, false),
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
