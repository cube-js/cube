use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::V1CubeMeta;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use crate::transport::{CubeColumn, V1CubeMetaExt};

use super::utils::new_string_array_with_placeholder;

struct InformationSchemaColumnsBuilder {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    column_names: StringBuilder,
    ordinal_positions: UInt32Builder,
    column_default: StringBuilder,
    is_nullable: StringBuilder,
    data_type: StringBuilder,
    char_max_length: UInt32Builder,
    char_octet_length: UInt32Builder,
    column_type: StringBuilder,
    numeric_scale: UInt32Builder,
    numeric_precision: UInt32Builder,
    datetime_precision: UInt32Builder,
}

impl InformationSchemaColumnsBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            catalog_names: StringBuilder::new(capacity),
            schema_names: StringBuilder::new(capacity),
            table_names: StringBuilder::new(capacity),
            column_names: StringBuilder::new(capacity),
            ordinal_positions: UInt32Builder::new(capacity),
            column_default: StringBuilder::new(capacity),
            is_nullable: StringBuilder::new(capacity),
            data_type: StringBuilder::new(capacity),
            char_max_length: UInt32Builder::new(capacity),
            char_octet_length: UInt32Builder::new(capacity),
            column_type: StringBuilder::new(capacity),
            numeric_precision: UInt32Builder::new(capacity),
            numeric_scale: UInt32Builder::new(capacity),
            datetime_precision: UInt32Builder::new(capacity),
        }
    }

    fn add_column(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        column: &CubeColumn,
        ordinal_position: u32,
    ) {
        self.catalog_names
            .append_value(catalog_name.as_ref())
            .unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.table_names.append_value(table_name.as_ref()).unwrap();
        self.column_names.append_value(column.get_name()).unwrap();
        self.ordinal_positions
            .append_value(ordinal_position)
            .unwrap();
        self.column_default.append_value("").unwrap();
        self.is_nullable
            .append_value(if column.sql_can_be_null() {
                "YES"
            } else {
                "NO"
            })
            .unwrap();

        self.data_type.append_value(column.get_data_type()).unwrap();
        self.column_type
            .append_value(column.get_column_type())
            .unwrap();

        self.char_max_length.append_null().unwrap();
        self.char_octet_length.append_null().unwrap();
        self.numeric_precision.append_null().unwrap();
        self.numeric_scale.append_null().unwrap();
        self.datetime_precision.append_null().unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        let catalog_names = self.catalog_names.finish();
        let total = catalog_names.len();
        columns.push(Arc::new(catalog_names));
        columns.push(Arc::new(self.schema_names.finish()));
        columns.push(Arc::new(self.table_names.finish()));
        columns.push(Arc::new(self.column_names.finish()));
        columns.push(Arc::new(self.ordinal_positions.finish()));
        columns.push(Arc::new(self.column_default.finish()));
        columns.push(Arc::new(self.is_nullable.finish()));
        columns.push(Arc::new(self.data_type.finish()));
        columns.push(Arc::new(self.char_max_length.finish()));
        columns.push(Arc::new(self.char_octet_length.finish()));
        columns.push(Arc::new(self.column_type.finish()));
        columns.push(Arc::new(self.numeric_precision.finish()));
        columns.push(Arc::new(self.numeric_scale.finish()));
        columns.push(Arc::new(self.datetime_precision.finish()));

        // COLUMN_KEY
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        // EXTRA
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        // COLUMN_COMMENT
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        // GENERATION_EXPRESSION
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));
        // SRS_ID
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        columns
    }
}

pub struct InfoSchemaColumnsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaColumnsProvider {
    pub fn new(cubes: &Vec<V1CubeMeta>) -> Self {
        let mut builder = InformationSchemaColumnsBuilder::new();

        for cube in cubes {
            let position = 0;

            for column in cube.get_columns() {
                builder.add_column("def", "db", cube.name.clone(), &column, position)
            }
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaColumnsProvider {
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
            Field::new("COLUMN_NAME", DataType::Utf8, false),
            Field::new("ORDINAL_POSITION", DataType::UInt32, false),
            Field::new("COLUMN_DEFAULT", DataType::Utf8, true),
            Field::new("IS_NULLABLE", DataType::Utf8, false),
            Field::new("DATA_TYPE", DataType::Utf8, false),
            Field::new("CHARACTER_MAXIMUM_LENGTH", DataType::UInt32, true),
            Field::new("CHARACTER_OCTET_LENGTH", DataType::UInt32, true),
            Field::new("COLUMN_TYPE", DataType::Utf8, false),
            Field::new("NUMERIC_PRECISION", DataType::UInt32, true),
            Field::new("NUMERIC_SCALE", DataType::UInt32, true),
            Field::new("DATETIME_PRECISION", DataType::UInt32, true),
            Field::new("COLUMN_KEY", DataType::Utf8, false),
            Field::new("EXTRA", DataType::Utf8, false),
            Field::new("COLUMN_COMMENT", DataType::Utf8, false),
            Field::new("GENERATION_EXPRESSION", DataType::Utf8, false),
            Field::new("SRS_ID", DataType::Utf8, true),
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
