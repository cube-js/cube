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

struct PgTablespace {
    oid: u32,
    spcname: &'static str,
    spcowner: u32,
}

struct PgCatalogTablespaceBuilder {
    oid: UInt32Builder,
    spcname: StringBuilder,
    spcowner: UInt32Builder,
    spcacl: ListBuilder<StringBuilder>,
    spcoptions: ListBuilder<StringBuilder>,
    xmin: UInt32Builder,
}

impl PgCatalogTablespaceBuilder {
    fn new() -> Self {
        let capacity = 2;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            spcname: StringBuilder::with_capacity(capacity, capacity),
            spcowner: UInt32Builder::with_capacity(capacity),
            spcacl: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
            spcoptions: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
            xmin: UInt32Builder::with_capacity(capacity),
        }
    }

    fn add_tablespace(&mut self, ts: &PgTablespace) {
        self.oid.append_value(ts.oid);
        self.spcname.append_value(ts.spcname);
        self.spcowner.append_value(ts.spcowner);
        self.spcacl.append(false);
        self.spcoptions.append(false);
        self.xmin.append_value(1);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.spcname.finish()),
            Arc::new(self.spcowner.finish()),
            Arc::new(self.spcacl.finish()),
            Arc::new(self.spcoptions.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogTablespaceProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-tablespace.html
impl PgCatalogTablespaceProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogTablespaceBuilder::new();
        builder.add_tablespace(&PgTablespace {
            oid: 1663,
            spcname: "pg_default",
            spcowner: 10,
        });
        builder.add_tablespace(&PgTablespace {
            oid: 1664,
            spcname: "pg_global",
            spcowner: 10,
        });

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogTablespaceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("spcname", DataType::Utf8, false),
            Field::new("spcowner", DataType::UInt32, false),
            Field::new(
                "spcacl",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "spcoptions",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
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
