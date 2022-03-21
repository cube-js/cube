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

use super::utils::{new_string_array_with_placeholder, new_uint32_array_with_placeholder};

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
    numeric_precision: UInt32Builder,
    numeric_precision_radix: UInt32Builder,
    numeric_scale: UInt32Builder,
    datetime_precision: UInt32Builder,
    domain_catalog: StringBuilder,
    domain_schema: StringBuilder,
    domain_name: StringBuilder,
    udt_catalog: StringBuilder,
    udt_schema: StringBuilder,
    udt_name: StringBuilder,
    dtd_identifier: StringBuilder,
    is_updatable: StringBuilder,
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
            numeric_precision: UInt32Builder::new(capacity),
            numeric_precision_radix: UInt32Builder::new(capacity),
            numeric_scale: UInt32Builder::new(capacity),
            datetime_precision: UInt32Builder::new(capacity),
            domain_catalog: StringBuilder::new(capacity),
            domain_schema: StringBuilder::new(capacity),
            domain_name: StringBuilder::new(capacity),
            udt_catalog: StringBuilder::new(capacity),
            udt_schema: StringBuilder::new(capacity),
            udt_name: StringBuilder::new(capacity),
            dtd_identifier: StringBuilder::new(capacity),
            is_updatable: StringBuilder::new(capacity),
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
                //TODO: rename mysql_can_be_null
                "YES"
            } else {
                "NO"
            })
            .unwrap();

        self.data_type.append_value(column.get_data_type()).unwrap();

        self.char_max_length.append_null().unwrap();
        self.char_octet_length.append_null().unwrap();
        self.numeric_precision.append_null().unwrap();
        self.numeric_precision_radix.append_null().unwrap();
        self.numeric_scale.append_null().unwrap();
        self.datetime_precision.append_null().unwrap();
        self.domain_catalog.append_null().unwrap();
        self.domain_schema.append_null().unwrap();
        self.domain_name.append_null().unwrap();
        self.udt_catalog.append_null().unwrap();
        self.udt_schema.append_null().unwrap();
        self.udt_name.append_null().unwrap();
        self.dtd_identifier.append_null().unwrap();
        self.is_updatable.append_null().unwrap();
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
        columns.push(Arc::new(self.numeric_precision.finish()));
        columns.push(Arc::new(self.numeric_precision_radix.finish()));
        columns.push(Arc::new(self.numeric_scale.finish()));
        columns.push(Arc::new(self.datetime_precision.finish()));

        // interval_type
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // interval_precision
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // character_set_catalog
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // character_set_schema
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // character_set_name
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // collation_catalog
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // collation_schema
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // collation_name
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        columns.push(Arc::new(self.domain_catalog.finish()));
        columns.push(Arc::new(self.domain_schema.finish()));
        columns.push(Arc::new(self.domain_name.finish()));
        columns.push(Arc::new(self.udt_catalog.finish()));
        columns.push(Arc::new(self.udt_schema.finish()));
        columns.push(Arc::new(self.udt_name.finish()));

        // scope_catalog
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // scope_schema
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // scope_name
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // maximum_cardinality
        columns.push(Arc::new(new_uint32_array_with_placeholder(total, 0)));
        columns.push(Arc::new(self.dtd_identifier.finish()));

        // is_self_referencing
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("NO".to_string()),
        )));

        // is_identity
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("NO".to_string()),
        )));

        // identity_generation
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // identity_start
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // identity_increment
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // identity_maximum
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // identity_minimum
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // identity_cycle
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("NO".to_string()),
        )));

        // is_generated
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("NEVER".to_string()),
        )));

        // generation_expression
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        columns.push(Arc::new(self.is_updatable.finish()));

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
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::UInt32, false),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            // TODO: to fill
            // ----------------------------------- start
            Field::new("character_maximum_length", DataType::UInt32, true),
            Field::new("character_octet_length", DataType::UInt32, true),
            Field::new("numeric_precision", DataType::UInt32, true),
            Field::new("numeric_precision_radix", DataType::UInt32, true),
            Field::new("numeric_scale", DataType::UInt32, true),
            Field::new("datetime_precision", DataType::UInt32, true),
            // ----------------------------------- end
            Field::new("interval_type", DataType::Utf8, false),
            Field::new("interval_precision", DataType::Utf8, true),
            Field::new("character_set_catalog", DataType::Utf8, true),
            Field::new("character_set_schema", DataType::Utf8, true),
            Field::new("character_set_name", DataType::Utf8, true),
            // TODO: to fill
            // ----------------------------------- start
            Field::new("collation_catalog", DataType::Utf8, true),
            Field::new("collation_schema", DataType::Utf8, true),
            Field::new("collation_name", DataType::Utf8, true),
            Field::new("domain_catalog", DataType::Utf8, true),
            Field::new("domain_schema", DataType::Utf8, true),
            Field::new("domain_name", DataType::Utf8, true),
            Field::new("udt_catalog", DataType::Utf8, true),
            Field::new("udt_schema", DataType::Utf8, true),
            Field::new("udt_name", DataType::Utf8, true),
            // ----------------------------------- end
            Field::new("scope_catalog", DataType::Utf8, true),
            Field::new("scope_schema", DataType::Utf8, true),
            Field::new("scope_name", DataType::Utf8, true),
            Field::new("maximum_cardinality", DataType::UInt32, true),
            // TODO: to fill
            // ----------------------------------- start
            Field::new("dtd_identifier", DataType::Utf8, true),
            // ----------------------------------- end
            Field::new("is_self_referencing", DataType::Utf8, true),
            // TODO: to fill
            // ----------------------------------- start
            Field::new("is_identity", DataType::Utf8, true),
            Field::new("identity_generation", DataType::Utf8, true),
            Field::new("identity_start", DataType::Utf8, true),
            Field::new("identity_increment", DataType::Utf8, true),
            Field::new("identity_maximum", DataType::Utf8, true),
            Field::new("identity_minimum", DataType::Utf8, true),
            // ----------------------------------- end
            Field::new("identity_cycle", DataType::Utf8, true),
            Field::new("is_generated", DataType::Utf8, true),
            Field::new("generation_expression", DataType::Utf8, false),
            // TODO: to fill
            // ----------------------------------- start
            Field::new("is_updatable", DataType::Utf8, true),
            // ----------------------------------- end
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
