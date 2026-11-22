use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogOpclassBuilder {
    oid: UInt32Builder,
    opcmethod: UInt32Builder,
    opcname: StringBuilder,
    opcnamespace: UInt32Builder,
    opcowner: UInt32Builder,
    opcfamily: UInt32Builder,
    opcintype: UInt32Builder,
    opcdefault: BooleanBuilder,
    opckeytype: UInt32Builder,
    xmin: UInt32Builder,
}

impl PgCatalogOpclassBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            opcmethod: UInt32Builder::with_capacity(capacity),
            opcname: StringBuilder::with_capacity(capacity, capacity),
            opcnamespace: UInt32Builder::with_capacity(capacity),
            opcowner: UInt32Builder::with_capacity(capacity),
            opcfamily: UInt32Builder::with_capacity(capacity),
            opcintype: UInt32Builder::with_capacity(capacity),
            opcdefault: BooleanBuilder::with_capacity(capacity),
            opckeytype: UInt32Builder::with_capacity(capacity),
            xmin: UInt32Builder::with_capacity(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.opcmethod.finish()),
            Arc::new(self.opcname.finish()),
            Arc::new(self.opcnamespace.finish()),
            Arc::new(self.opcowner.finish()),
            Arc::new(self.opcfamily.finish()),
            Arc::new(self.opcintype.finish()),
            Arc::new(self.opcdefault.finish()),
            Arc::new(self.opckeytype.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogOpclassProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-opclass.html
impl PgCatalogOpclassProvider {
    pub fn new() -> Self {
        let builder = PgCatalogOpclassBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogOpclassProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("opcmethod", DataType::UInt32, false),
            Field::new("opcname", DataType::Utf8, false),
            Field::new("opcnamespace", DataType::UInt32, false),
            Field::new("opcowner", DataType::UInt32, false),
            Field::new("opcfamily", DataType::UInt32, false),
            Field::new("opcintype", DataType::UInt32, false),
            Field::new("opcdefault", DataType::Boolean, false),
            Field::new("opckeytype", DataType::UInt32, false),
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
