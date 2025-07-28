use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, ListBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogForeignServerBuilder {
    oid: UInt32Builder,
    srvname: StringBuilder,
    srvowner: UInt32Builder,
    srvfdw: UInt32Builder,
    srvtype: StringBuilder,
    srvversion: StringBuilder,
    srvacl: ListBuilder<StringBuilder>,
    srvoptions: ListBuilder<StringBuilder>,
    xmin: UInt32Builder,
}

impl PgCatalogForeignServerBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::new(capacity),
            srvname: StringBuilder::new(capacity),
            srvowner: UInt32Builder::new(capacity),
            srvfdw: UInt32Builder::new(capacity),
            srvtype: StringBuilder::new(capacity),
            srvversion: StringBuilder::new(capacity),
            srvacl: ListBuilder::new(StringBuilder::new(capacity)),
            srvoptions: ListBuilder::new(StringBuilder::new(capacity)),
            xmin: UInt32Builder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.srvname.finish()),
            Arc::new(self.srvowner.finish()),
            Arc::new(self.srvfdw.finish()),
            Arc::new(self.srvtype.finish()),
            Arc::new(self.srvversion.finish()),
            Arc::new(self.srvacl.finish()),
            Arc::new(self.srvoptions.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogForeignServerProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-foreign-server.html
impl PgCatalogForeignServerProvider {
    pub fn new() -> Self {
        let builder = PgCatalogForeignServerBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogForeignServerProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("srvname", DataType::Utf8, false),
            Field::new("srvowner", DataType::UInt32, false),
            Field::new("srvfdw", DataType::UInt32, false),
            Field::new("srvtype", DataType::Utf8, true),
            Field::new("srvversion", DataType::Utf8, true),
            Field::new(
                "srvacl",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "srvoptions",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
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
