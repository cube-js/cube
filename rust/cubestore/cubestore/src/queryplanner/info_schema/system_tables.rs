use crate::metastore::table::TablePath;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray, UInt64Array,
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
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        ctx.meta_store.get_tables_with_path(true).await
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
                false,
            ),
            Field::new(
                "seal_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("sealed", DataType::Boolean, false),
            Field::new("select_statement", DataType::Utf8, false),
            Field::new("extension", DataType::Utf8, true),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|tables| {
                Arc::new(UInt64Array::from_iter_values(
                    tables.iter().map(|row| row.table.get_id()),
                ))
            }),
            Box::new(|tables| {
                Arc::new(UInt64Array::from_iter_values(
                    tables.iter().map(|row| row.table.get_row().get_schema_id()),
                ))
            }),
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
                Arc::new(StringArray::from_iter_values(
                    tables
                        .iter()
                        .map(|row| format!("{:?}", row.table.get_row().get_columns())),
                ))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(
                    tables
                        .iter()
                        .map(|row| format!("{:?}", row.table.get_row().locations())),
                ))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(
                    tables
                        .iter()
                        .map(|row| format!("{:?}", row.table.get_row().import_format())),
                ))
            }),
            Box::new(|tables| {
                Arc::new(BooleanArray::from_iter(
                    tables
                        .iter()
                        .map(|row| Some(*row.table.get_row().has_data())),
                ))
            }),
            Box::new(|tables| {
                Arc::new(BooleanArray::from_iter(
                    tables
                        .iter()
                        .map(|row| Some(row.table.get_row().is_ready())),
                ))
            }),
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
                Arc::new(StringArray::from_iter(
                    unique_key_columns.iter().map(|v| v.as_deref()),
                ))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(tables.iter().map(|row| {
                    format!("{:?}", row.table.get_row().aggregate_columns())
                })))
            }),
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
                Arc::new(StringArray::from_iter(
                    seq_columns.iter().map(|v| v.as_deref()),
                ))
            }),
            Box::new(|tables| {
                Arc::new(UInt64Array::from_iter(tables.iter().map(|row| {
                    row.table.get_row().partition_split_threshold().clone()
                })))
            }),
            Box::new(|tables| {
                Arc::new(TimestampNanosecondArray::from_iter(tables.iter().map(
                    |row| {
                        row.table
                            .get_row()
                            .created_at()
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
            Box::new(|tables| {
                Arc::new(BooleanArray::from_iter(
                    tables.iter().map(|row| Some(row.table.get_row().sealed())),
                ))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter(tables.iter().map(|row| {
                    row.table
                        .get_row()
                        .select_statement()
                        .as_ref()
                        .map(|t| t.as_str())
                })))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter(
                    tables
                        .iter()
                        .map(|row| row.table.get_row().extension().as_deref()),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemTablesTableDef);
