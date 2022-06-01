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

struct InformationSchemaViewsBuilder {
    table_catalog: StringBuilder,
    table_schema: StringBuilder,
    table_name: StringBuilder,
    view_definition: StringBuilder,
    check_option: StringBuilder,
    is_updatable: StringBuilder,
    is_insertable_into: StringBuilder,
    is_trigger_updatable: StringBuilder,
    is_trigger_deletable: StringBuilder,
    is_trigger_insertable_into: StringBuilder,
}

impl InformationSchemaViewsBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            table_catalog: StringBuilder::new(capacity),
            table_schema: StringBuilder::new(capacity),
            table_name: StringBuilder::new(capacity),
            view_definition: StringBuilder::new(capacity),
            check_option: StringBuilder::new(capacity),
            is_updatable: StringBuilder::new(capacity),
            is_insertable_into: StringBuilder::new(capacity),
            is_trigger_updatable: StringBuilder::new(capacity),
            is_trigger_deletable: StringBuilder::new(capacity),
            is_trigger_insertable_into: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.table_catalog.finish()));
        columns.push(Arc::new(self.table_schema.finish()));
        columns.push(Arc::new(self.table_name.finish()));
        columns.push(Arc::new(self.view_definition.finish()));
        columns.push(Arc::new(self.check_option.finish()));
        columns.push(Arc::new(self.is_updatable.finish()));
        columns.push(Arc::new(self.is_insertable_into.finish()));
        columns.push(Arc::new(self.is_trigger_updatable.finish()));
        columns.push(Arc::new(self.is_trigger_deletable.finish()));
        columns.push(Arc::new(self.is_trigger_insertable_into.finish()));

        columns
    }
}

pub struct InfoSchemaViewsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaViewsProvider {
    pub fn new() -> Self {
        let builder = InformationSchemaViewsBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaViewsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("view_definition", DataType::Utf8, false),
            Field::new("check_option", DataType::Utf8, false),
            Field::new("is_updatable", DataType::Utf8, false),
            Field::new("is_insertable_into", DataType::Utf8, false),
            Field::new("is_trigger_updatable", DataType::Utf8, false),
            Field::new("is_trigger_deletable", DataType::Utf8, false),
            Field::new("is_trigger_insertable_into", DataType::Utf8, false),
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
