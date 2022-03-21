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

use super::utils::new_string_array_with_placeholder;

struct InformationSchemaSchemataBuilder {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    default_character_set_names: StringBuilder,
    default_collation_names: StringBuilder,
}

impl InformationSchemaSchemataBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            catalog_names: StringBuilder::new(capacity),
            schema_names: StringBuilder::new(capacity),
            default_character_set_names: StringBuilder::new(capacity),
            default_collation_names: StringBuilder::new(capacity),
        }
    }

    fn add_schema(
        &mut self,
        schema_name: impl AsRef<str>,
        default_character_set_name: impl AsRef<str>,
        default_collation_name: impl AsRef<str>,
    ) {
        self.catalog_names.append_value("def").unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.default_character_set_names
            .append_value(default_character_set_name.as_ref())
            .unwrap();
        self.default_collation_names
            .append_value(default_collation_name.as_ref())
            .unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        let catalog_names = self.catalog_names.finish();
        let total = catalog_names.len();

        columns.push(Arc::new(catalog_names));
        columns.push(Arc::new(self.schema_names.finish()));
        columns.push(Arc::new(self.default_character_set_names.finish()));
        columns.push(Arc::new(self.default_collation_names.finish()));

        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("NO".to_string()),
        )));

        columns
    }
}

pub struct InfoSchemaSchemataProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaSchemataProvider {
    pub fn new() -> Self {
        let mut builder = InformationSchemaSchemataBuilder::new();
        // information_schema
        builder.add_schema("information_schema", "utf8", "utf8_general_ci");
        builder.add_schema("mysql", "utf8mb4", "utf8mb4_0900_ai_ci");
        builder.add_schema("performance_schema", "utf8mb4", "utf8mb4_0900_ai_ci");
        builder.add_schema("sys", "utf8mb4", "utf8mb4_0900_ai_ci");
        builder.add_schema("test", "utf8mb4", "utf8mb4_0900_ai_ci");

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaSchemataProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("CATALOG_NAME", DataType::Utf8, false),
            Field::new("SCHEMA_NAME", DataType::Utf8, false),
            Field::new("DEFAULT_CHARACTER_SET_NAME", DataType::Utf8, false),
            Field::new("DEFAULT_COLLATION_NAME", DataType::Utf8, false),
            Field::new("SQL_PATH", DataType::Utf8, true),
            Field::new("DEFAULT_ENCRYPTION", DataType::Utf8, false),
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
