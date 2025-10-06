use crate::metastore::table::TablePath;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
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
    ) -> Result<Arc<Vec<TablePath>>, CubeError> {
        ctx.meta_store.get_tables_with_path(false).await
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new(
                "build_range_end",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "seal_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(
                    tables.iter().map(|row| row.schema.get_row().get_name()),
                ))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(
                    tables
                        .iter()
                        .map(|row| row.table.get_row().get_table_name()),
                ))
            }),
            Box::new(|tables| {
                Arc::new(TimestampNanosecondArray::from_iter(tables.iter().map(
                    |row| {
                        row.table
                            .get_row()
                            .build_range_end()
                            .as_ref()
                            .map(|t| t.timestamp_nanos())
                    },
                )))
            }),
            Box::new(|tables| {
                Arc::new(TimestampNanosecondArray::from_iter(tables.iter().map(
                    |row| {
                        row.table
                            .get_row()
                            .seal_at()
                            .as_ref()
                            .map(|t| t.timestamp_nanos())
                    },
                )))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(TablesInfoSchemaTableDef);
