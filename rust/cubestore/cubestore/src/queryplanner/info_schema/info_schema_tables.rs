use crate::metastore::table::TablePath;
use crate::queryplanner::info_schema::timestamp_nanos_or_panic;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder, TimestampNanosecondBuilder};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

pub struct TablesInfoSchemaTableDef;

#[async_trait]
impl InfoSchemaTableDef for TablesInfoSchemaTableDef {
    type T = TablePath;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Vec<TablePath>, CubeError> {
        Ok(Arc::unwrap_or_clone(
            ctx.meta_store.get_tables_with_path(false).await?,
        ))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
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
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut schema_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut name_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut range_end_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut seal_builder = TimestampNanosecondBuilder::with_capacity(num_rows);

        for row in rows.into_iter() {
            schema_builder.append_value(row.schema.get_row().get_name());
            let table = row.table.get_row();
            name_builder.append_value(table.get_table_name());
            range_end_builder.append_option(
                table
                    .build_range_end()
                    .as_ref()
                    .map(timestamp_nanos_or_panic),
            );
            seal_builder.append_option(table.seal_at().as_ref().map(timestamp_nanos_or_panic));
        }

        vec![
            Arc::new(schema_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(range_end_builder.finish()),
            Arc::new(seal_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(TablesInfoSchemaTableDef);
