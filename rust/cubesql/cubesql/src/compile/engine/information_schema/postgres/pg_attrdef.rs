use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int16Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use super::utils::{ExtDataType, OidBuilder};

struct PgCatalogAttrdefBuilder {
    oid: OidBuilder,
    adrelid: OidBuilder,
    adnum: Int16Builder,
    // TODO: type pg_node_tree?
    adbin: StringBuilder,
}

impl PgCatalogAttrdefBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: OidBuilder::new(capacity),
            adrelid: OidBuilder::new(capacity),
            adnum: Int16Builder::new(capacity),
            adbin: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.adrelid.finish()));
        columns.push(Arc::new(self.adnum.finish()));
        columns.push(Arc::new(self.adbin.finish()));

        columns
    }
}

pub struct PgCatalogAttrdefProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogAttrdefProvider {
    pub fn new() -> Self {
        let builder = PgCatalogAttrdefBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogAttrdefProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", ExtDataType::Oid.into(), false),
            Field::new("adrelid", ExtDataType::Oid.into(), false),
            Field::new("adnum", DataType::Int16, false),
            Field::new("adbin", DataType::Utf8, false),
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
