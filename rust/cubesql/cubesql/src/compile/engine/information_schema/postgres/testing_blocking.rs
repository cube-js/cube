use std::{any::Any, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::ExecutionPlan,
};

pub struct InfoSchemaTestingBlockingProvider {}

impl InfoSchemaTestingBlockingProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProvider for InfoSchemaTestingBlockingProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        tokio::time::sleep(Duration::from_secs(60 * 60 * 3600)).await;

        panic!("timeout reached on testing_blocking");
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
