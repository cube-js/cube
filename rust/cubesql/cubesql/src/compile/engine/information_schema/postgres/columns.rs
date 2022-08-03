use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::V1CubeMeta;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int32Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use crate::transport::{CubeColumn, V1CubeMetaExt};

use super::{
    ext::CubeColumnPostgresExt,
    utils::{
        new_int32_array_with_placeholder, new_string_array_with_placeholder,
        new_yes_no_array_with_placeholder, yes_no, ExtDataType, YesNoBuilder,
    },
};

struct InformationSchemaColumnsBuilder {
    table_catalog: StringBuilder,
    table_schema: StringBuilder,
    table_name: StringBuilder,
    column_name: StringBuilder,
    ordinal_position: Int32Builder,
    column_default: StringBuilder,
    is_nullable: YesNoBuilder,
    data_type: StringBuilder,
    character_maximum_length: Int32Builder,
    character_octet_length: Int32Builder,
    numeric_precision: Int32Builder,
    numeric_precision_radix: Int32Builder,
    numeric_scale: Int32Builder,
    datetime_precision: Int32Builder,
    domain_catalog: StringBuilder,
    domain_schema: StringBuilder,
    domain_name: StringBuilder,
    udt_catalog: StringBuilder,
    udt_schema: StringBuilder,
    udt_name: StringBuilder,
    dtd_identifier: StringBuilder,
    is_updatable: YesNoBuilder,
}

impl InformationSchemaColumnsBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            table_catalog: StringBuilder::new(capacity),
            table_schema: StringBuilder::new(capacity),
            table_name: StringBuilder::new(capacity),
            column_name: StringBuilder::new(capacity),
            ordinal_position: Int32Builder::new(capacity),
            column_default: StringBuilder::new(capacity),
            is_nullable: YesNoBuilder::new(capacity),
            data_type: StringBuilder::new(capacity),
            character_maximum_length: Int32Builder::new(capacity),
            character_octet_length: Int32Builder::new(capacity),
            numeric_precision: Int32Builder::new(capacity),
            numeric_precision_radix: Int32Builder::new(capacity),
            numeric_scale: Int32Builder::new(capacity),
            datetime_precision: Int32Builder::new(capacity),
            domain_catalog: StringBuilder::new(capacity),
            domain_schema: StringBuilder::new(capacity),
            domain_name: StringBuilder::new(capacity),
            udt_catalog: StringBuilder::new(capacity),
            udt_schema: StringBuilder::new(capacity),
            udt_name: StringBuilder::new(capacity),
            dtd_identifier: StringBuilder::new(capacity),
            is_updatable: YesNoBuilder::new(capacity),
        }
    }

    fn add_column(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        column: &CubeColumn,
        ordinal_position: i32,
    ) {
        self.table_catalog.append_value(&catalog_name).unwrap();
        self.table_schema.append_value(schema_name).unwrap();
        self.table_name.append_value(table_name).unwrap();
        self.column_name.append_value(column.get_name()).unwrap();
        self.ordinal_position
            .append_value(ordinal_position)
            .unwrap();
        self.column_default.append_null().unwrap();
        self.is_nullable
            .append_value(yes_no(column.sql_can_be_null()))
            .unwrap();
        self.data_type.append_value(column.get_data_type()).unwrap();
        self.character_maximum_length.append_null().unwrap();
        self.character_octet_length
            .append_option(column.get_char_octet_length())
            .unwrap();
        self.numeric_precision
            .append_option(column.get_numeric_precision())
            .unwrap();
        self.numeric_precision_radix
            .append_option(column.get_numeric_precision_radix())
            .unwrap();
        self.numeric_scale
            .append_option(column.get_numeric_scale())
            .unwrap();
        self.datetime_precision
            .append_option(column.get_datetime_precision())
            .unwrap();
        self.domain_catalog.append_null().unwrap();
        self.domain_schema.append_null().unwrap();
        self.domain_name.append_null().unwrap();
        self.udt_catalog.append_value(catalog_name).unwrap();
        self.udt_schema
            .append_value(column.get_udt_schema())
            .unwrap();
        self.udt_name.append_value(column.get_udt_name()).unwrap();

        // unsupported
        self.dtd_identifier.append_value("0").unwrap();

        // always YES for basic tables
        self.is_updatable.append_value(yes_no(true)).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        let table_catalog = self.table_catalog.finish();
        let total = table_catalog.len();
        columns.push(Arc::new(table_catalog));
        columns.push(Arc::new(self.table_schema.finish()));
        columns.push(Arc::new(self.table_name.finish()));
        columns.push(Arc::new(self.column_name.finish()));
        columns.push(Arc::new(self.ordinal_position.finish()));
        columns.push(Arc::new(self.column_default.finish()));
        columns.push(Arc::new(self.is_nullable.finish()));
        columns.push(Arc::new(self.data_type.finish()));
        columns.push(Arc::new(self.character_maximum_length.finish()));
        columns.push(Arc::new(self.character_octet_length.finish()));
        columns.push(Arc::new(self.numeric_precision.finish()));
        columns.push(Arc::new(self.numeric_precision_radix.finish()));
        columns.push(Arc::new(self.numeric_scale.finish()));
        columns.push(Arc::new(self.datetime_precision.finish()));

        // interval_type
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // interval_precision
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // character_set_catalog
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // character_set_schema
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // character_set_name
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // collation_catalog
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // collation_schema
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // collation_name
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        columns.push(Arc::new(self.domain_catalog.finish()));
        columns.push(Arc::new(self.domain_schema.finish()));
        columns.push(Arc::new(self.domain_name.finish()));
        columns.push(Arc::new(self.udt_catalog.finish()));
        columns.push(Arc::new(self.udt_schema.finish()));
        columns.push(Arc::new(self.udt_name.finish()));

        // scope_catalog
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // scope_schema
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // scope_name
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // maximum_cardinality
        columns.push(Arc::new(new_int32_array_with_placeholder(total, None)));

        columns.push(Arc::new(self.dtd_identifier.finish()));

        // is_self_referencing
        columns.push(Arc::new(new_yes_no_array_with_placeholder(
            total,
            Some(false),
        )));

        // is_identity
        columns.push(Arc::new(new_yes_no_array_with_placeholder(
            total,
            Some(false),
        )));

        // identity_generation
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // identity_start
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // identity_increment
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // identity_maximum
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // identity_minimum
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // identity_cycle
        columns.push(Arc::new(new_yes_no_array_with_placeholder(
            total,
            Some(false),
        )));

        // is_generated
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("NEVER"),
        )));

        // generation_expression
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        columns.push(Arc::new(self.is_updatable.finish()));

        columns
    }
}

pub struct InfoSchemaColumnsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaColumnsProvider {
    pub fn new(db_name: &str, cubes: &Vec<V1CubeMeta>) -> Self {
        let mut builder = InformationSchemaColumnsBuilder::new();

        for cube in cubes {
            let mut position_iter = 1..;

            for column in cube.get_columns() {
                builder.add_column(
                    db_name,
                    "public",
                    cube.name.clone(),
                    &column,
                    position_iter.next().unwrap_or(0),
                );
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
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("is_nullable", ExtDataType::YesNo.into(), false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("character_maximum_length", DataType::Int32, true),
            Field::new("character_octet_length", DataType::Int32, true),
            Field::new("numeric_precision", DataType::Int32, true),
            Field::new("numeric_precision_radix", DataType::Int32, true),
            Field::new("numeric_scale", DataType::Int32, true),
            Field::new("datetime_precision", DataType::Int32, true),
            Field::new("interval_type", DataType::Utf8, true),
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
            Field::new("udt_catalog", DataType::Utf8, false),
            Field::new("udt_schema", DataType::Utf8, false),
            Field::new("udt_name", DataType::Utf8, false),
            Field::new("scope_catalog", DataType::Utf8, true),
            Field::new("scope_schema", DataType::Utf8, true),
            Field::new("scope_name", DataType::Utf8, true),
            Field::new("maximum_cardinality", DataType::Int32, true),
            Field::new("dtd_identifier", DataType::Utf8, false),
            Field::new("is_self_referencing", ExtDataType::YesNo.into(), false),
            // TODO: is_identity is not supported yet
            Field::new("is_identity", ExtDataType::YesNo.into(), false),
            Field::new("identity_generation", DataType::Utf8, true),
            Field::new("identity_start", DataType::Utf8, true),
            Field::new("identity_increment", DataType::Utf8, true),
            Field::new("identity_maximum", DataType::Utf8, true),
            Field::new("identity_minimum", DataType::Utf8, true),
            Field::new("identity_cycle", ExtDataType::YesNo.into(), false),
            Field::new("is_generated", DataType::Utf8, false),
            Field::new("generation_expression", DataType::Utf8, true),
            Field::new("is_updatable", ExtDataType::YesNo.into(), false),
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
