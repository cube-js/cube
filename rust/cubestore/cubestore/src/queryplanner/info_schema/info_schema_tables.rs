use crate::metastore::table::TablePath;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct TablesInfoSchemaTableDef;

#[async_trait]
impl InfoSchemaTableDef for TablesInfoSchemaTableDef {
    type T = TablePath;

    async fn rows(&self, ctx: InfoSchemaTableDefContext) -> Result<Arc<Vec<TablePath>>, CubeError> {
        ctx.meta_store.get_tables_with_path(false).await
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<TablePath>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("table_schema", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|row| row.schema.get_row().get_name().as_str())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("table_name", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|row| row.table.get_row().get_table_name().as_str())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new(
                    "build_range_end",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|tables| {
                    Arc::new(TimestampNanosecondArray::from(
                        tables
                            .iter()
                            .map(|row| {
                                row.table
                                    .get_row()
                                    .build_range_end()
                                    .as_ref()
                                    .map(|t| t.timestamp_nanos())
                            })
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new(
                    "seal_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|tables| {
                    Arc::new(TimestampNanosecondArray::from(
                        tables
                            .iter()
                            .map(|row| {
                                row.table
                                    .get_row()
                                    .seal_at()
                                    .as_ref()
                                    .map(|t| t.timestamp_nanos())
                            })
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(TablesInfoSchemaTableDef);
