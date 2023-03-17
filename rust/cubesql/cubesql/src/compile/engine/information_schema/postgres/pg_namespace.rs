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

struct PgNamespace {
    oid: u32,
    nspname: &'static str,
    nspowner: u32,
    nspacl: &'static str,
}

struct PgCatalogNamespaceBuilder {
    oid: UInt32Builder,
    nspname: StringBuilder,
    nspowner: UInt32Builder,
    nspacl: StringBuilder,
}

impl PgCatalogNamespaceBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::new(capacity),
            nspname: StringBuilder::new(capacity),
            nspowner: UInt32Builder::new(capacity),
            nspacl: StringBuilder::new(capacity),
        }
    }

    fn add_namespace(&mut self, ns: &PgNamespace) {
        self.oid.append_value(ns.oid).unwrap();
        self.nspname.append_value(ns.nspname).unwrap();
        self.nspowner.append_value(ns.nspowner).unwrap();
        self.nspacl.append_value(ns.nspacl).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.nspname.finish()));
        columns.push(Arc::new(self.nspowner.finish()));
        columns.push(Arc::new(self.nspacl.finish()));

        columns
    }
}

pub struct PgCatalogNamespaceProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogNamespaceProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogNamespaceBuilder::new();
        builder.add_namespace(&PgNamespace {
            oid: 11,
            nspname: "pg_catalog",
            nspowner: 10,
            nspacl: "{test=UC/test,=U/test}",
        });
        builder.add_namespace(&PgNamespace {
            oid: 2200,
            nspname: "public",
            nspowner: 10,
            nspacl: "{test=UC/test,=U/test}",
        });
        builder.add_namespace(&PgNamespace {
            oid: 13000,
            nspname: "information_schema",
            nspowner: 10,
            nspacl: "{test=UC/test,=U/test}",
        });

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogNamespaceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("nspname", DataType::Utf8, false),
            Field::new("nspowner", DataType::UInt32, false),
            Field::new("nspacl", DataType::Utf8, true),
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
