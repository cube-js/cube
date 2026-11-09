use std::{any::Any, sync::Arc};

use async_trait::async_trait;
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

use crate::transport::{CubeColumn, CubeMeta, V1CubeMetaExt};

use super::{
    ext::CubeColumnPostgresExt,
    utils::{new_string_array_with_placeholder, new_uint32_array_with_placeholder},
};

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
            catalog_names: StringBuilder::with_capacity(capacity, capacity),
            schema_names: StringBuilder::with_capacity(capacity, capacity),
            table_names: StringBuilder::with_capacity(capacity, capacity),
            column_names: StringBuilder::with_capacity(capacity, capacity),
            ordinal_positions: UInt32Builder::with_capacity(capacity),
            column_default: StringBuilder::with_capacity(capacity, capacity),
            is_nullable: StringBuilder::with_capacity(capacity, capacity),
            data_type: StringBuilder::with_capacity(capacity, capacity),
            char_max_length: UInt32Builder::with_capacity(capacity),
            char_octet_length: UInt32Builder::with_capacity(capacity),
            numeric_precision: UInt32Builder::with_capacity(capacity),
            numeric_precision_radix: UInt32Builder::with_capacity(capacity),
            numeric_scale: UInt32Builder::with_capacity(capacity),
            datetime_precision: UInt32Builder::with_capacity(capacity),
            domain_catalog: StringBuilder::with_capacity(capacity, capacity),
            domain_schema: StringBuilder::with_capacity(capacity, capacity),
            domain_name: StringBuilder::with_capacity(capacity, capacity),
            udt_catalog: StringBuilder::with_capacity(capacity, capacity),
            udt_schema: StringBuilder::with_capacity(capacity, capacity),
            udt_name: StringBuilder::with_capacity(capacity, capacity),
            dtd_identifier: StringBuilder::with_capacity(capacity, capacity),
            is_updatable: StringBuilder::with_capacity(capacity, capacity),
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
        self.catalog_names.append_value(catalog_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.column_names.append_value(column.get_name());
        self.ordinal_positions.append_value(ordinal_position);
        self.column_default.append_value("");
        self.is_nullable.append_value(column.is_nullable());

        self.data_type.append_value(column.get_data_type());

        self.char_max_length.append_null();

        match column.char_octet_length() {
            Some(value) => self.char_octet_length.append_value(value),
            _ => self.char_octet_length.append_null(),
        }

        match column.get_numeric_precision() {
            Some(value) => self.numeric_precision.append_value(value),
            _ => self.numeric_precision.append_null(),
        }

        match column.numeric_precision_radix() {
            Some(value) => self.numeric_precision_radix.append_value(value),
            _ => self.numeric_precision_radix.append_null(),
        }

        match column.numeric_scale() {
            Some(value) => self.numeric_scale.append_value(value),
            _ => self.numeric_scale.append_null(),
        }

        match column.datetime_precision() {
            Some(value) => self.datetime_precision.append_value(value),
            _ => self.datetime_precision.append_null(),
        }

        self.domain_catalog.append_null();
        self.domain_schema.append_null();
        self.domain_name.append_null();

        self.udt_catalog.append_value(catalog_name.as_ref());
        self.udt_schema.append_value(column.udt_schema());
        self.udt_name.append_value(column.get_udt_name());

        // unsupported
        self.dtd_identifier.append_value("0");

        // always YES for basic tables
        self.is_updatable.append_value("YES");
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
    pub fn new(db_name: &str, cubes: &Vec<CubeMeta>) -> Self {
        let mut builder = InformationSchemaColumnsBuilder::new();

        for cube in cubes {
            for (position, column) in (1..).zip(cube.get_columns()) {
                builder.add_column(db_name, "public", cube.name.clone(), &column, position);
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
            Field::new("character_maximum_length", DataType::UInt32, true),
            Field::new("character_octet_length", DataType::UInt32, true),
            Field::new("numeric_precision", DataType::UInt32, true),
            Field::new("numeric_precision_radix", DataType::UInt32, true),
            Field::new("numeric_scale", DataType::UInt32, true),
            Field::new("datetime_precision", DataType::UInt32, true),
            Field::new("interval_type", DataType::Utf8, false),
            Field::new("interval_precision", DataType::Utf8, true),
            Field::new("character_set_catalog", DataType::Utf8, true),
            Field::new("character_set_schema", DataType::Utf8, true),
            Field::new("character_set_name", DataType::Utf8, true),
            Field::new("collation_catalog", DataType::Utf8, true),
            Field::new("collation_schema", DataType::Utf8, true),
            Field::new("collation_name", DataType::Utf8, true),
            Field::new("domain_catalog", DataType::Utf8, true),
            Field::new("domain_schema", DataType::Utf8, true),
            Field::new("domain_name", DataType::Utf8, true),
            Field::new("udt_catalog", DataType::Utf8, true),
            Field::new("udt_schema", DataType::Utf8, true),
            Field::new("udt_name", DataType::Utf8, true),
            Field::new("scope_catalog", DataType::Utf8, true),
            Field::new("scope_schema", DataType::Utf8, true),
            Field::new("scope_name", DataType::Utf8, true),
            Field::new("maximum_cardinality", DataType::UInt32, true),
            Field::new("dtd_identifier", DataType::Utf8, true),
            Field::new("is_self_referencing", DataType::Utf8, true),
            // TODO: is_identity is not supported yet
            Field::new("is_identity", DataType::Utf8, true),
            Field::new("identity_generation", DataType::Utf8, true),
            Field::new("identity_start", DataType::Utf8, true),
            Field::new("identity_increment", DataType::Utf8, true),
            Field::new("identity_maximum", DataType::Utf8, true),
            Field::new("identity_minimum", DataType::Utf8, true),
            Field::new("identity_cycle", DataType::Utf8, true),
            Field::new("is_generated", DataType::Utf8, true),
            Field::new("generation_expression", DataType::Utf8, false),
            Field::new("is_updatable", DataType::Utf8, true),
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
