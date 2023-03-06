use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, Int64Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct RedshiftLateBindingViewUnpackedBuilder {
    schemaname: StringBuilder,
    tablename: StringBuilder,
    columnname: StringBuilder,
    columntype: StringBuilder,
    columnnum: Int64Builder,
}

impl RedshiftLateBindingViewUnpackedBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            schemaname: StringBuilder::new(capacity),
            tablename: StringBuilder::new(capacity),
            columnname: StringBuilder::new(capacity),
            columntype: StringBuilder::new(capacity),
            columnnum: Int64Builder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.schemaname.finish()));
        columns.push(Arc::new(self.tablename.finish()));
        columns.push(Arc::new(self.columnname.finish()));
        columns.push(Arc::new(self.columntype.finish()));
        columns.push(Arc::new(self.columnnum.finish()));

        columns
    }
}

// Table to replace unpack pg_get_late_binding_view_cols() lbv_cols(schemaname name, tablename name, columnname name, columntype text, columnnum int))
pub struct RedshiftLateBindingViewUnpackedTableProvider {}

impl RedshiftLateBindingViewUnpackedTableProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProvider for RedshiftLateBindingViewUnpackedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("tablename", DataType::Utf8, false),
            Field::new("columnname", DataType::Utf8, false),
            Field::new("columntype", DataType::Utf8, false),
            Field::new("columnnum", DataType::Int64, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let builder = RedshiftLateBindingViewUnpackedBuilder::new(1);
        let batch = RecordBatch::try_new(self.schema(), builder.finish())?;

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
