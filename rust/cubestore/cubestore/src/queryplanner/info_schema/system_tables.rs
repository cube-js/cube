use crate::metastore::table::TablePath;
use crate::metastore::MetaStore;
use crate::queryplanner::InfoSchemaTableDef;
use crate::CubeError;
use arrow::array::{ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemTablesTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemTablesTableDef {
    type T = TablePath;

    async fn rows(&self, meta_store: Arc<dyn MetaStore>) -> Result<Arc<Vec<Self::T>>, CubeError> {
        meta_store.get_tables_with_path(true).await
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::UInt64, false),
                Box::new(|tables| {
                    Arc::new(UInt64Array::from(
                        tables
                            .iter()
                            .map(|row| row.table.get_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("schema_id", DataType::UInt64, false),
                Box::new(|tables| {
                    Arc::new(UInt64Array::from(
                        tables
                            .iter()
                            .map(|row| row.table.get_row().get_schema_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
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
                Field::new("columns", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|row| format!("{:?}", row.table.get_row().get_columns()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("locations", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|row| format!("{:?}", row.table.get_row().locations()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("import_format", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|row| format!("{:?}", row.table.get_row().import_format()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("has_data", DataType::Boolean, true),
                Box::new(|tables| {
                    Arc::new(BooleanArray::from(
                        tables
                            .iter()
                            .map(|row| *row.table.get_row().has_data())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("is_ready", DataType::Boolean, true),
                Box::new(|tables| {
                    Arc::new(BooleanArray::from(
                        tables
                            .iter()
                            .map(|row| row.table.get_row().is_ready())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("unique_key_column_indices", DataType::Utf8, true),
                Box::new(|tables| {
                    let unique_key_columns = tables
                        .iter()
                        .map(|row| {
                            row.table
                                .get_row()
                                .unique_key_columns()
                                .as_ref()
                                .map(|v| format!("{:?}", v))
                        })
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        unique_key_columns
                            .iter()
                            .map(|v| v.as_ref().map(|v| v.as_str()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("aggregate_columns", DataType::Utf8, true),
                Box::new(|tables| {
                    let aggregates = tables
                        .iter()
                        .map(|row| format!("{:?}", row.table.get_row().aggregate_columns()))
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        aggregates.iter().map(|v| v.as_str()).collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("seq_column_index", DataType::Utf8, true),
                Box::new(|tables| {
                    let seq_columns = tables
                        .iter()
                        .map(|row| {
                            row.table
                                .get_row()
                                .seq_column()
                                .as_ref()
                                .map(|v| format!("{:?}", v))
                        })
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        seq_columns
                            .iter()
                            .map(|v| v.as_ref().map(|v| v.as_str()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("partition_split_threshold", DataType::UInt64, true),
                Box::new(|tables| {
                    let array = tables
                        .iter()
                        .map(|row| row.table.get_row().partition_split_threshold().clone())
                        .collect::<Vec<_>>();
                    Arc::new(UInt64Array::from(array))
                }),
            ),
            (
                Field::new(
                    "created_at",
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
                                    .created_at()
                                    .as_ref()
                                    .map(|t| t.timestamp_nanos())
                            })
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
        ]
    }
}

crate::base_info_schema_table_def!(SystemTablesTableDef);
