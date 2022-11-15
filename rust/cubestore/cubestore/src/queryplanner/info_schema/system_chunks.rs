use crate::metastore::chunks::chunk_file_name;
use crate::metastore::{Chunk, IdRow, MetaStore, MetaStoreTable};
use crate::queryplanner::InfoSchemaTableDef;
use crate::CubeError;
use arrow::array::{ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemChunksTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemChunksTableDef {
    type T = IdRow<Chunk>;

    async fn rows(&self, meta_store: Arc<dyn MetaStore>) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(meta_store.chunks_table().all_rows().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::UInt64, false),
                Box::new(|chunks| {
                    Arc::new(UInt64Array::from(
                        chunks.iter().map(|row| row.get_id()).collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("file_name", DataType::Utf8, false),
                Box::new(|chunks| {
                    Arc::new(StringArray::from(
                        chunks
                            .iter()
                            .map(|row| chunk_file_name(row.get_id(), row.get_row().suffix()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("partition_id", DataType::UInt64, false),
                Box::new(|chunks| {
                    Arc::new(UInt64Array::from(
                        chunks
                            .iter()
                            .map(|row| row.get_row().get_partition_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("replay_handle_id", DataType::UInt64, false),
                Box::new(|chunks| {
                    Arc::new(UInt64Array::from(
                        chunks
                            .iter()
                            .map(|row| row.get_row().replay_handle_id().clone())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("row_count", DataType::UInt64, true),
                Box::new(|chunks| {
                    Arc::new(UInt64Array::from(
                        chunks
                            .iter()
                            .map(|row| row.get_row().get_row_count())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("uploaded", DataType::Boolean, true),
                Box::new(|chunks| {
                    Arc::new(BooleanArray::from(
                        chunks
                            .iter()
                            .map(|row| row.get_row().uploaded())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("active", DataType::Boolean, true),
                Box::new(|chunks| {
                    Arc::new(BooleanArray::from(
                        chunks
                            .iter()
                            .map(|row| row.get_row().active())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("in_memory", DataType::Boolean, true),
                Box::new(|chunks| {
                    Arc::new(BooleanArray::from(
                        chunks
                            .iter()
                            .map(|row| row.get_row().in_memory())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|chunks| {
                    Arc::new(TimestampNanosecondArray::from(
                        chunks
                            .iter()
                            .map(|row| {
                                row.get_row()
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
                    "oldest_insert_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|chunks| {
                    Arc::new(TimestampNanosecondArray::from(
                        chunks
                            .iter()
                            .map(|row| {
                                row.get_row()
                                    .oldest_insert_at()
                                    .as_ref()
                                    .map(|t| t.timestamp_nanos())
                            })
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new(
                    "deactivated_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|chunks| {
                    Arc::new(TimestampNanosecondArray::from(
                        chunks
                            .iter()
                            .map(|row| {
                                row.get_row()
                                    .deactivated_at()
                                    .as_ref()
                                    .map(|t| t.timestamp_nanos())
                            })
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("file_size", DataType::UInt64, true),
                Box::new(|chunks| {
                    Arc::new(UInt64Array::from(
                        chunks
                            .iter()
                            .map(|row| row.get_row().file_size())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("min_row", DataType::Utf8, true),
                Box::new(|chunks| {
                    let min_array = chunks
                        .iter()
                        .map(|row| row.get_row().min().as_ref().map(|x| format!("{:?}", x)))
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        min_array
                            .iter()
                            .map(|v| v.as_ref().map(|v| v.as_str()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("max_row", DataType::Utf8, true),
                Box::new(|chunks| {
                    let max_array = chunks
                        .iter()
                        .map(|row| row.get_row().max().as_ref().map(|x| format!("{:?}", x)))
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        max_array
                            .iter()
                            .map(|v| v.as_ref().map(|v| v.as_str()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(SystemChunksTableDef);
