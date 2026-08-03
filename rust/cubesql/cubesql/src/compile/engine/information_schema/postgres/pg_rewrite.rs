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

struct PgCatalogRewriteBuilder {
    oid: UInt32Builder,
    rulename: StringBuilder,
    ev_class: UInt32Builder,
    ev_type: StringBuilder,
    ev_enabled: StringBuilder,
    is_instead: BooleanBuilder,
    ev_qual: StringBuilder,   // FIXME: actual type "pg_node_tree"
    ev_action: StringBuilder, // FIXME: actual type "pg_node_tree"
    xmin: UInt32Builder,
}

impl PgCatalogRewriteBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::new(capacity),
            rulename: StringBuilder::new(capacity),
            ev_class: UInt32Builder::new(capacity),
            ev_type: StringBuilder::new(capacity),
            ev_enabled: StringBuilder::new(capacity),
            is_instead: BooleanBuilder::new(capacity),
            ev_qual: StringBuilder::new(capacity),
            ev_action: StringBuilder::new(capacity),
            xmin: UInt32Builder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.rulename.finish()),
            Arc::new(self.ev_class.finish()),
            Arc::new(self.ev_type.finish()),
            Arc::new(self.ev_enabled.finish()),
            Arc::new(self.is_instead.finish()),
            Arc::new(self.ev_qual.finish()),
            Arc::new(self.ev_action.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogRewriteProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-rewrite.html
impl PgCatalogRewriteProvider {
    pub fn new() -> Self {
        let builder = PgCatalogRewriteBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogRewriteProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("rulename", DataType::Utf8, false),
            Field::new("ev_class", DataType::UInt32, false),
            Field::new("ev_type", DataType::Utf8, false),
            Field::new("ev_enabled", DataType::Utf8, false),
            Field::new("is_instead", DataType::Boolean, false),
            Field::new("ev_qual", DataType::Utf8, false),
            Field::new("ev_action", DataType::Utf8, false),
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
