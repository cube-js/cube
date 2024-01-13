use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int16Builder, ListBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogPartitionedTableBuilder {
    partrelid: UInt32Builder,
    partstrat: StringBuilder,
    partnatts: Int16Builder,
    partdefid: UInt32Builder,
    partattrs: ListBuilder<Int16Builder>,
    partclass: ListBuilder<UInt32Builder>,
    partcollation: ListBuilder<UInt32Builder>,
    partexprs: StringBuilder, // FIXME: actual type "pg_node_tree"
}

impl PgCatalogPartitionedTableBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            partrelid: UInt32Builder::new(capacity),
            partstrat: StringBuilder::new(capacity),
            partnatts: Int16Builder::new(capacity),
            partdefid: UInt32Builder::new(capacity),
            partattrs: ListBuilder::new(Int16Builder::new(capacity)),
            partclass: ListBuilder::new(UInt32Builder::new(capacity)),
            partcollation: ListBuilder::new(UInt32Builder::new(capacity)),
            partexprs: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.partrelid.finish()));
        columns.push(Arc::new(self.partstrat.finish()));
        columns.push(Arc::new(self.partnatts.finish()));
        columns.push(Arc::new(self.partdefid.finish()));
        columns.push(Arc::new(self.partattrs.finish()));
        columns.push(Arc::new(self.partclass.finish()));
        columns.push(Arc::new(self.partcollation.finish()));
        columns.push(Arc::new(self.partexprs.finish()));

        columns
    }
}

pub struct PgCatalogPartitionedTableProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-partitioned-table.html
impl PgCatalogPartitionedTableProvider {
    pub fn new() -> Self {
        let builder = PgCatalogPartitionedTableBuilder::new(0);

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogPartitionedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("partrelid", DataType::UInt32, false),
            Field::new("partstrat", DataType::Utf8, false),
            Field::new("partnatts", DataType::Int16, false),
            Field::new("partdefid", DataType::UInt32, false),
            Field::new(
                "partattrs",
                DataType::List(Box::new(Field::new("item", DataType::Int16, true))),
                false,
            ),
            Field::new(
                "partclass",
                DataType::List(Box::new(Field::new("item", DataType::UInt32, true))),
                false,
            ),
            Field::new(
                "partcollation",
                DataType::List(Box::new(Field::new("item", DataType::UInt32, true))),
                false,
            ),
            Field::new("partexprs", DataType::Utf8, true),
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
