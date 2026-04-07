use crate::metastore::table::TablePath;
use crate::queryplanner::info_schema::timestamp_nanos_or_panic;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, StringBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

pub struct SystemTablesTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemTablesTableDef {
    type T = TablePath;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(Arc::unwrap_or_clone(
            ctx.meta_store.get_tables_with_path(true).await?,
        ))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("schema_id", DataType::UInt64, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("columns", DataType::Utf8, false),
            Field::new("locations", DataType::Utf8, false),
            Field::new("import_format", DataType::Utf8, false),
            Field::new("has_data", DataType::Boolean, true),
            Field::new("is_ready", DataType::Boolean, true),
            Field::new("unique_key_column_indices", DataType::Utf8, true),
            Field::new("aggregate_columns", DataType::Utf8, true),
            Field::new("seq_column_index", DataType::Utf8, true),
            Field::new("partition_split_threshold", DataType::UInt64, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "build_range_end",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "seal_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("sealed", DataType::Boolean, false),
            Field::new("select_statement", DataType::Utf8, true),
            Field::new("extension", DataType::Utf8, true),
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = UInt64Builder::with_capacity(num_rows);
        let mut schema_id_builder = UInt64Builder::with_capacity(num_rows);
        let mut schema_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut name_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut columns_builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
        let mut locations_builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
        let mut format_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut has_data_builder = BooleanBuilder::with_capacity(num_rows);
        let mut is_ready_builder = BooleanBuilder::with_capacity(num_rows);
        let mut unique_key_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut agg_columns_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut seq_column_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut split_threshold_builder = UInt64Builder::with_capacity(num_rows);
        let mut created_at_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut range_end_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut seal_at_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut sealed_builder = BooleanBuilder::with_capacity(num_rows);
        let mut select_builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
        let mut extension_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);

        for row in rows.into_iter() {
            id_builder.append_value(row.table.get_id());
            let table = row.table.get_row();
            schema_id_builder.append_value(table.get_schema_id());
            schema_builder.append_value(row.schema.get_row().get_name());
            name_builder.append_value(table.get_table_name());
            columns_builder.append_value(format!("{:?}", table.get_columns()));
            locations_builder.append_value(format!("{:?}", table.locations()));
            format_builder.append_value(format!("{:?}", table.import_format()));
            has_data_builder.append_value(*table.has_data());
            is_ready_builder.append_value(table.is_ready());
            unique_key_builder.append_option(
                table
                    .unique_key_columns()
                    .as_ref()
                    .map(|v| format!("{:?}", v)),
            );
            agg_columns_builder.append_value(format!("{:?}", table.aggregate_columns()));
            seq_column_builder
                .append_option(table.seq_column().as_ref().map(|v| format!("{:?}", v)));
            split_threshold_builder.append_option(*table.partition_split_threshold());
            created_at_builder
                .append_option(table.created_at().as_ref().map(timestamp_nanos_or_panic));
            range_end_builder.append_option(
                table
                    .build_range_end()
                    .as_ref()
                    .map(timestamp_nanos_or_panic),
            );
            seal_at_builder.append_option(table.seal_at().as_ref().map(timestamp_nanos_or_panic));
            sealed_builder.append_value(table.sealed());
            select_builder.append_option(table.select_statement().as_ref().map(|s| s.as_str()));
            extension_builder.append_option(table.extension().as_ref().map(|e| e.as_str()));
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(schema_id_builder.finish()),
            Arc::new(schema_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(columns_builder.finish()),
            Arc::new(locations_builder.finish()),
            Arc::new(format_builder.finish()),
            Arc::new(has_data_builder.finish()),
            Arc::new(is_ready_builder.finish()),
            Arc::new(unique_key_builder.finish()),
            Arc::new(agg_columns_builder.finish()),
            Arc::new(seq_column_builder.finish()),
            Arc::new(split_threshold_builder.finish()),
            Arc::new(created_at_builder.finish()),
            Arc::new(range_end_builder.finish()),
            Arc::new(seal_at_builder.finish()),
            Arc::new(sealed_builder.finish()),
            Arc::new(select_builder.finish()),
            Arc::new(extension_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemTablesTableDef);
