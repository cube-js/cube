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

use super::utils::{new_string_array_with_placeholder, new_uint32_array_with_placeholder};

struct InformationSchemaStatisticsBuilder {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
}

impl InformationSchemaStatisticsBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            catalog_names: StringBuilder::new(capacity),
            schema_names: StringBuilder::new(capacity),
            table_names: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        let catalog_names = self.catalog_names.finish();
        let total = catalog_names.len();
        columns.push(Arc::new(catalog_names));
        columns.push(Arc::new(self.schema_names.finish()));
        columns.push(Arc::new(self.table_names.finish()));

        // NON_UNIQUE
        columns.push(Arc::new(new_uint32_array_with_placeholder(total, 0)));
        // INDEX_SCHEMA
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        // INDEX_NAME
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        // SEQ_IN_INDEX
        columns.push(Arc::new(new_uint32_array_with_placeholder(total, 0)));
        // COLUMN_NAME
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        // COLLATION
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        // EXPRESSION
        columns.push(Arc::new(new_uint32_array_with_placeholder(total, 0)));

        columns
    }
}

pub struct InfoSchemaStatisticsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaStatisticsProvider {
    pub fn new() -> Self {
        let builder = InformationSchemaStatisticsBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaStatisticsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("TABLE_CATALOG", DataType::Utf8, false),
            Field::new("TABLE_SCHEMA", DataType::Utf8, false),
            Field::new("TABLE_NAME", DataType::Utf8, false),
            Field::new("NON_UNIQUE", DataType::UInt32, false),
            Field::new("INDEX_SCHEMA", DataType::Utf8, false),
            Field::new("INDEX_NAME", DataType::Utf8, false),
            Field::new("SEQ_IN_INDEX", DataType::UInt32, false),
            Field::new("COLUMN_NAME", DataType::Utf8, false),
            Field::new("COLLATION", DataType::Utf8, true),
            Field::new("CARDINALITY", DataType::UInt32, true),
            // @todo
            // SUB_PART
            // PACKED
            // NULLABLE
            // INDEX_TYPE
            // COMMENT
            // INDEX_COMMENT
            // IS_VISIBLE
            // EXPRESSION
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
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
