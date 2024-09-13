use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct InfoSchemaTableConstraintsBuilder {
    constraint_catalog: StringBuilder,
    constraint_schema: StringBuilder,
    constraint_name: StringBuilder,
    table_catalog: StringBuilder,
    table_schema: StringBuilder,
    table_name: StringBuilder,
    constraint_type: StringBuilder,
    is_deferrable: StringBuilder,
    initially_deferred: StringBuilder,
    enforced: StringBuilder,
}

impl InfoSchemaTableConstraintsBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            constraint_catalog: StringBuilder::new(capacity),
            constraint_schema: StringBuilder::new(capacity),
            constraint_name: StringBuilder::new(capacity),
            table_catalog: StringBuilder::new(capacity),
            table_schema: StringBuilder::new(capacity),
            table_name: StringBuilder::new(capacity),
            constraint_type: StringBuilder::new(capacity),
            is_deferrable: StringBuilder::new(capacity),
            initially_deferred: StringBuilder::new(capacity),
            enforced: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.constraint_catalog.finish()),
            Arc::new(self.constraint_schema.finish()),
            Arc::new(self.constraint_name.finish()),
            Arc::new(self.table_catalog.finish()),
            Arc::new(self.table_schema.finish()),
            Arc::new(self.table_name.finish()),
            Arc::new(self.constraint_type.finish()),
            Arc::new(self.is_deferrable.finish()),
            Arc::new(self.initially_deferred.finish()),
            Arc::new(self.enforced.finish()),
        ];

        columns
    }
}

pub struct InfoSchemaTableConstraintsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaTableConstraintsProvider {
    pub fn new() -> Self {
        let builder = InfoSchemaTableConstraintsBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaTableConstraintsProvider {
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
            Field::new("constraint_type", DataType::Utf8, false),
            Field::new("is_deferrable", DataType::Utf8, false),
            Field::new("initially_deferred", DataType::Utf8, false),
            Field::new("enforced", DataType::Utf8, false),
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
