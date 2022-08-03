use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int32Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct InfoSchemaKeyColumnUsageBuilder {
    constraint_catalog: StringBuilder,
    constraint_schema: StringBuilder,
    constraint_name: StringBuilder,
    table_catalog: StringBuilder,
    table_schema: StringBuilder,
    table_name: StringBuilder,
    column_name: StringBuilder,
    ordinal_position: Int32Builder,
    position_in_unique_constraint: Int32Builder,
}

impl InfoSchemaKeyColumnUsageBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            constraint_catalog: StringBuilder::new(capacity),
            constraint_schema: StringBuilder::new(capacity),
            constraint_name: StringBuilder::new(capacity),
            table_catalog: StringBuilder::new(capacity),
            table_schema: StringBuilder::new(capacity),
            table_name: StringBuilder::new(capacity),
            column_name: StringBuilder::new(capacity),
            ordinal_position: Int32Builder::new(capacity),
            position_in_unique_constraint: Int32Builder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.constraint_catalog.finish()));
        columns.push(Arc::new(self.constraint_schema.finish()));
        columns.push(Arc::new(self.constraint_name.finish()));
        columns.push(Arc::new(self.table_catalog.finish()));
        columns.push(Arc::new(self.table_schema.finish()));
        columns.push(Arc::new(self.table_name.finish()));
        columns.push(Arc::new(self.column_name.finish()));
        columns.push(Arc::new(self.ordinal_position.finish()));
        columns.push(Arc::new(self.position_in_unique_constraint.finish()));

        columns
    }
}

pub struct InfoSchemaKeyColumnUsageProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaKeyColumnUsageProvider {
    pub fn new() -> Self {
        let builder = InfoSchemaKeyColumnUsageBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaKeyColumnUsageProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("constraint_catalog", DataType::Utf8, false),
            Field::new("constraint_schema", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("position_in_unique_constraint", DataType::Int32, true),
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
