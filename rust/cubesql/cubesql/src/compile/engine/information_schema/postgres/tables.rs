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

use super::utils::new_string_array_with_placeholder;

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
        table_type: impl AsRef<str>,
    ) {
        self.catalog_names
            .append_value(catalog_name.as_ref())
            .unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.table_names.append_value(table_name.as_ref()).unwrap();
        self.table_types.append_value(table_type.as_ref()).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.catalog_names.finish()));
        columns.push(Arc::new(self.schema_names.finish()));
        columns.push(Arc::new(self.table_names.finish()));

        let tables_types = self.table_types.finish();
        let total = tables_types.len();
        columns.push(Arc::new(tables_types));

        // self_referencing_column_name
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // reference_generation
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // user_defined_type_catalog
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // user_defined_type_schema
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // user_defined_type_name
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // is_insertable_into
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("NO".to_string()),
        )));

        // is_typed
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("NO".to_string()),
        )));

        // commit_action
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
        builder.add_table("db", "information_schema", "tables", "VIEW");
        builder.add_table("db", "information_schema", "columns", "VIEW");
        builder.add_table("db", "information_schema", "pg_tables", "VIEW");

        for cube in cubes {
            builder.add_table("db", "public", &cube.name, "BASE TABLE");
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
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("self_referencing_column_name", DataType::Utf8, false),
            Field::new("reference_generation", DataType::Utf8, false),
            Field::new("user_defined_type_catalog", DataType::Utf8, false),
            Field::new("user_defined_type_schema", DataType::Utf8, false),
            Field::new("user_defined_type_name", DataType::Utf8, false),
            Field::new("is_insertable_into", DataType::Utf8, false),
            Field::new("is_typed", DataType::Utf8, false),
            Field::new("commit_action", DataType::Utf8, false),
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
