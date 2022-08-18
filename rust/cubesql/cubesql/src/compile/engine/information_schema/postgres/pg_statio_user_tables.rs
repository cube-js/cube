use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int64Builder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use crate::compile::CubeMetaTable;

struct PgCatalogStatioUserTablesBuilder {
    relid: UInt32Builder,
    schemaname: StringBuilder,
    relname: StringBuilder,
    heap_blks_read: Int64Builder,
    heap_blks_hit: Int64Builder,
    idx_blks_read: Int64Builder,
    idx_blks_hit: Int64Builder,
    toast_blks_read: Int64Builder,
    toast_blks_hit: Int64Builder,
    tidx_blks_read: Int64Builder,
    tidx_blks_hit: Int64Builder,
}

impl PgCatalogStatioUserTablesBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            relid: UInt32Builder::new(capacity),
            schemaname: StringBuilder::new(capacity),
            relname: StringBuilder::new(capacity),
            heap_blks_read: Int64Builder::new(capacity),
            heap_blks_hit: Int64Builder::new(capacity),
            idx_blks_read: Int64Builder::new(capacity),
            idx_blks_hit: Int64Builder::new(capacity),
            toast_blks_read: Int64Builder::new(capacity),
            toast_blks_hit: Int64Builder::new(capacity),
            tidx_blks_read: Int64Builder::new(capacity),
            tidx_blks_hit: Int64Builder::new(capacity),
        }
    }

    fn add_table(&mut self, relid: u32, schemaname: impl AsRef<str>, relname: impl AsRef<str>) {
        self.relid.append_value(relid).unwrap();
        self.schemaname.append_value(schemaname).unwrap();
        self.relname.append_value(relname).unwrap();
        self.heap_blks_read.append_value(0).unwrap();
        self.heap_blks_hit.append_value(0).unwrap();
        self.idx_blks_read.append_value(0).unwrap();
        self.idx_blks_hit.append_value(0).unwrap();
        self.toast_blks_read.append_null().unwrap();
        self.toast_blks_hit.append_null().unwrap();
        self.tidx_blks_read.append_null().unwrap();
        self.tidx_blks_hit.append_null().unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.relid.finish()));
        columns.push(Arc::new(self.schemaname.finish()));
        columns.push(Arc::new(self.relname.finish()));
        columns.push(Arc::new(self.heap_blks_read.finish()));
        columns.push(Arc::new(self.heap_blks_hit.finish()));
        columns.push(Arc::new(self.idx_blks_read.finish()));
        columns.push(Arc::new(self.idx_blks_hit.finish()));
        columns.push(Arc::new(self.toast_blks_read.finish()));
        columns.push(Arc::new(self.toast_blks_hit.finish()));
        columns.push(Arc::new(self.tidx_blks_read.finish()));
        columns.push(Arc::new(self.tidx_blks_hit.finish()));

        columns
    }
}

pub struct PgCatalogStatioUserTablesProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogStatioUserTablesProvider {
    pub fn new(cube_tables: &Vec<CubeMetaTable>) -> Self {
        let mut builder = PgCatalogStatioUserTablesBuilder::new(cube_tables.len());

        for table in cube_tables.iter() {
            builder.add_table(table.oid, "public", &table.name);
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogStatioUserTablesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("relid", DataType::UInt32, false),
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("heap_blks_read", DataType::Int64, false),
            Field::new("heap_blks_hit", DataType::Int64, false),
            Field::new("idx_blks_read", DataType::Int64, false),
            Field::new("idx_blks_hit", DataType::Int64, false),
            Field::new("toast_blks_read", DataType::Int64, true),
            Field::new("toast_blks_hit", DataType::Int64, true),
            Field::new("tidx_blks_read", DataType::Int64, true),
            Field::new("tidx_blks_hit", DataType::Int64, true),
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
