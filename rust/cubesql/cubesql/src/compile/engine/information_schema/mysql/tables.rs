use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::V1CubeMeta;
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

use super::utils::{new_int64_array_with_placeholder, new_string_array_with_placeholder};

struct InformationSchemaTablesBuilder {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
}

impl InformationSchemaTablesBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            catalog_names: StringBuilder::new(capacity),
            schema_names: StringBuilder::new(capacity),
            table_names: StringBuilder::new(capacity),
            table_types: StringBuilder::new(capacity),
        }
    }

    fn add_table(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
    ) {
        self.catalog_names
            .append_value(catalog_name.as_ref())
            .unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.table_names.append_value(table_name.as_ref()).unwrap();
        self.table_types
            .append_value("BASE TABLE".to_string())
            .unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.catalog_names.finish()));
        columns.push(Arc::new(self.schema_names.finish()));
        columns.push(Arc::new(self.table_names.finish()));

        let tables_types = self.table_types.finish();
        let total = tables_types.len();
        columns.push(Arc::new(tables_types));

        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("InnoDB".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("10".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("Dynamic".to_string()),
        )));
        columns.push(Arc::new(new_int64_array_with_placeholder(total, 0)));
        columns.push(Arc::new(new_int64_array_with_placeholder(total, 0)));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("16384".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        columns
    }
}

pub struct InfoSchemaTableProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaTableProvider {
    pub fn new(cubes: &Vec<V1CubeMeta>) -> Self {
        let mut builder = InformationSchemaTablesBuilder::new();
        // information_schema
        builder.add_table("def", "information_schema", "tables");
        builder.add_table("def", "information_schema", "columns");
        builder.add_table("def", "information_schema", "key_column_usage");
        builder.add_table("def", "information_schema", "referential_constraints");
        //  performance_schema
        builder.add_table("def", "performance_schema", "session_variables");
        builder.add_table("def", "performance_schema", "global_variables");

        for cube in cubes {
            builder.add_table("def", "db", cube.name.clone());
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaTableProvider {
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
            Field::new("TABLE_TYPE", DataType::Utf8, false),
            Field::new("ENGINE", DataType::Utf8, false),
            Field::new("VERSION", DataType::Utf8, false),
            Field::new("ROW_FORMAT", DataType::Utf8, false),
            Field::new("TABLES_ROWS", DataType::Int64, true),
            Field::new("AVG_ROW_LENGTH", DataType::Int64, false),
            Field::new("DATA_LENGTH", DataType::Utf8, false),
            Field::new("MAX_DATA_LENGTH", DataType::Utf8, false),
            Field::new("INDEX_LENGTH", DataType::Utf8, false),
            Field::new("DATA_FREE", DataType::Utf8, false),
            Field::new("AUTO_INCREMENT", DataType::Utf8, true),
            Field::new("CREATE_TIME", DataType::Utf8, false),
            Field::new("UPDATE_TIME", DataType::Utf8, true),
            Field::new("CHECK_TIME", DataType::Utf8, true),
            Field::new("TABLE_COLLATION", DataType::Utf8, false),
            Field::new("CHECKSUM", DataType::Utf8, true),
            Field::new("CREATE_OPTIONS", DataType::Utf8, false),
            Field::new("TABLE_COMMENT", DataType::Utf8, false),
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
