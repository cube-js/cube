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

use super::utils::{new_string_array_with_placeholder, new_uint32_array_with_placeholder};
use crate::compile::engine::provider::TableName;

pub struct InfoSchemaKeyColumnUsageProvider {}

impl InfoSchemaKeyColumnUsageProvider {
    pub fn new() -> Self {
        Self {}
    }
}

impl TableName for InfoSchemaKeyColumnUsageProvider {
    fn table_name(&self) -> &str {
        "information_schema.key_column_usage"
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
            Field::new("CONSTRAINT_NAME", DataType::Utf8, true),
            Field::new("TABLE_CATALOG", DataType::Utf8, false),
            Field::new("TABLE_SCHEMA", DataType::Utf8, false),
            Field::new("TABLE_NAME", DataType::Utf8, false),
            Field::new("COLUMN_NAME", DataType::Utf8, true),
            Field::new("ORDINAL_POSITION", DataType::UInt32, false),
            Field::new("POSITION_IN_UNIQUE_CONSTRAINT", DataType::UInt32, true),
            Field::new("REFERENCED_TABLE_SCHEMA", DataType::Utf8, true),
            Field::new("REFERENCED_TABLE_NAME", DataType::Utf8, true),
            Field::new("REFERENCED_COLUMN_NAME", DataType::Utf8, true),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut data: Vec<Arc<dyn Array>> = vec![];
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        // ORDINAL_POSITION
        data.push(Arc::new(new_uint32_array_with_placeholder(0, Some(0))));
        // POSITION_IN_UNIQUE_CONSTRAINT
        data.push(Arc::new(new_uint32_array_with_placeholder(0, None)));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));
        data.push(Arc::new(new_string_array_with_placeholder(0, Some(""))));

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
