use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use super::utils::{ExtDataType, OidBuilder};

struct PgCatalogAmBuilder {
    oid: OidBuilder,
    amname: StringBuilder,
    // TODO: type regproc?
    amhandler: StringBuilder,
    amtype: StringBuilder,
}

impl PgCatalogAmBuilder {
    fn new() -> Self {
        let capacity = 0;

        Self {
            oid: OidBuilder::new(capacity),
            amname: StringBuilder::new(capacity),
            amhandler: StringBuilder::new(capacity),
            amtype: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.amname.finish()));
        columns.push(Arc::new(self.amhandler.finish()));
        columns.push(Arc::new(self.amtype.finish()));

        columns
    }
}

pub struct PgCatalogAmProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogAmProvider {
    pub fn new() -> Self {
        let builder = PgCatalogAmBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogAmProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", ExtDataType::Oid.into(), false),
            Field::new("amname", DataType::Utf8, false),
            Field::new("amhandler", DataType::Utf8, false),
            Field::new("amtype", DataType::Utf8, false),
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
