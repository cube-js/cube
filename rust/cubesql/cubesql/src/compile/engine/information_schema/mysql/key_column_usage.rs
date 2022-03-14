use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use super::utils::{
    new_boolean_array_with_placeholder, new_string_array_with_placeholder,
    new_uint32_array_with_placeholder,
};

pub struct InfoSchemaKeyColumnUsageProvider {}

impl InfoSchemaKeyColumnUsageProvider {
    pub fn new() -> Self {
        Self {}
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
            Field::new("CONSTRAINT_CATALOG", DataType::Utf8, false),
            Field::new("CONSTRAINT_SCHEMA", DataType::Utf8, false),
            Field::new("CONSTRAINT_NAME", DataType::Utf8, false),
            Field::new("TABLE_CATALOG", DataType::Utf8, false),
            Field::new("TABLE_SCHEMA", DataType::Utf8, false),
            Field::new("TABLE_NAME", DataType::Utf8, false),
            Field::new("COLUMN_NAME", DataType::Utf8, false),
            Field::new("ORDINAL_POSITION", DataType::UInt32, false),
            Field::new("POSITION_IN_UNIQUE_CONSTRAINT", DataType::Boolean, true),
            Field::new("REFERENCED_TABLE_SCHEMA", DataType::Utf8, false),
            Field::new("REFERENCED_TABLE_NAME", DataType::Utf8, false),
            Field::new("REFERENCED_COLUMN_NAME", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut data: Vec<Arc<dyn Array>> = vec![];
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        // ORDINAL_POSITION
        data.push(Arc::new(new_uint32_array_with_placeholder(0, 0)));
        // POSITION_IN_UNIQUE_CONSTRAINT
        data.push(Arc::new(new_boolean_array_with_placeholder(0, false)));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));
        data.push(Arc::new(new_string_array_with_placeholder(
            0,
            Some("".to_string()),
        )));

        let batch = RecordBatch::try_new(self.schema(), data)?;

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
