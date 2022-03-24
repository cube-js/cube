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

use super::utils::new_string_array_with_placeholder;

pub struct InfoSchemaReferentialConstraintsProvider {}

impl InfoSchemaReferentialConstraintsProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProvider for InfoSchemaReferentialConstraintsProvider {
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
            Field::new("UNIQUE_CONSTRAINT_CATALOG", DataType::Utf8, false),
            Field::new("UNIQUE_CONSTRAINT_SCHEMA", DataType::Utf8, false),
            Field::new("UNIQUE_CONSTRAINT_NAME", DataType::Utf8, false),
            Field::new("MATCH_OPTION", DataType::Utf8, false),
            Field::new("UPDATE_RULE", DataType::Utf8, false),
            Field::new("DELETE_RULE", DataType::Utf8, false),
            Field::new("TABLE_NAME", DataType::Utf8, false),
            Field::new("REFERENCED_TABLE_NAME", DataType::Utf8, false),
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
