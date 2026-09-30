use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::ExecutionPlan,
};

/// Panics as soon as a query plans a scan of it, which happens inside the portal's
/// `execute_stream`. Lets tests check that a panic reaches the client as an error.
pub struct InfoSchemaTestingPanicProvider {}

impl InfoSchemaTestingPanicProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProvider for InfoSchemaTestingPanicProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]))
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
        panic!("testing_panic");
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
