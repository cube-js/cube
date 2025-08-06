use crate::metastore::chunks::chunk_file_name;
use crate::metastore::{Chunk, IdRow, MetaStoreTable};
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

pub struct SystemChunksTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemChunksTableDef {
    type T = IdRow<Chunk>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.meta_store.chunks_table().all_rows().await?))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("file_name", DataType::Utf8, false),
            Field::new("partition_id", DataType::UInt64, false),
            Field::new("replay_handle_id", DataType::UInt64, false),
            Field::new("row_count", DataType::UInt64, true),
            Field::new("uploaded", DataType::Boolean, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("in_memory", DataType::Boolean, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "oldest_insert_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "deactivated_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("file_size", DataType::UInt64, true),
            Field::new("min_row", DataType::Utf8, true),
            Field::new("max_row", DataType::Utf8, true),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|chunks| {
                Arc::new(UInt64Array::from_iter_values(
                    chunks.iter().map(|row| row.get_id()),
                ))
            }),
            Box::new(|chunks| {
                Arc::new(StringArray::from_iter_values(chunks.iter().map(|row| {
                    chunk_file_name(row.get_id(), row.get_row().suffix())
                })))
            }),
            Box::new(|chunks| {
                Arc::new(UInt64Array::from_iter_values(
                    chunks.iter().map(|row| row.get_row().get_partition_id()),
                ))
            }),
            Box::new(|chunks| {
                Arc::new(UInt64Array::from_iter(
                    chunks
                        .iter()
                        .map(|row| row.get_row().replay_handle_id().clone()),
                ))
            }),
            Box::new(|chunks| {
                Arc::new(UInt64Array::from_iter_values(
                    chunks.iter().map(|row| row.get_row().get_row_count()),
                ))
            }),
            Box::new(|chunks| {
                Arc::new(BooleanArray::from_iter(
                    chunks.iter().map(|row| Some(row.get_row().uploaded())),
                ))
            }),
            Box::new(|chunks| {
                Arc::new(BooleanArray::from_iter(
                    chunks.iter().map(|row| Some(row.get_row().active())),
                ))
            }),
            Box::new(|chunks| {
                Arc::new(BooleanArray::from_iter(
                    chunks.iter().map(|row| Some(row.get_row().in_memory())),
                ))
            }),
            Box::new(|chunks| {
                Arc::new(TimestampNanosecondArray::from_iter(chunks.iter().map(
                    |row| {
                        row.get_row()
                            .created_at()
                            .as_ref()
                            .map(|t| t.timestamp_nanos())
                    },
                )))
            }),
            Box::new(|chunks| {
                Arc::new(TimestampNanosecondArray::from_iter(chunks.iter().map(
                    |row| {
                        row.get_row()
                            .oldest_insert_at()
                            .as_ref()
                            .map(|t| t.timestamp_nanos())
                    },
                )))
            }),
            Box::new(|chunks| {
                Arc::new(TimestampNanosecondArray::from_iter(chunks.iter().map(
                    |row| {
                        row.get_row()
                            .deactivated_at()
                            .as_ref()
                            .map(|t| t.timestamp_nanos())
                    },
                )))
            }),
            Box::new(|chunks| {
                Arc::new(UInt64Array::from_iter(
                    chunks.iter().map(|row| row.get_row().file_size()),
                ))
            }),
            Box::new(|chunks| {
                let min_array = chunks
                    .iter()
                    .map(|row| row.get_row().min().as_ref().map(|x| format!("{:?}", x)))
                    .collect::<Vec<_>>();
                Arc::new(StringArray::from_iter(
                    min_array.iter().map(|v| v.as_deref()),
                ))
            }),
            Box::new(|chunks| {
                let max_array = chunks
                    .iter()
                    .map(|row| row.get_row().max().as_ref().map(|x| format!("{:?}", x)))
                    .collect::<Vec<_>>();
                Arc::new(StringArray::from_iter(
                    max_array.iter().map(|v| v.as_deref()),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemChunksTableDef);
