use crate::metastore::{Chunk, IdRow, MetaStore, MetaStoreTable};
use crate::queryplanner::InfoSchemaTableDef;
use crate::CubeError;
use arrow::array::{ArrayRef, BooleanArray, TimestampNanosecondArray, UInt64Array};
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
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("partition_id", DataType::UInt64, false),
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().get_partition_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("row_count", DataType::UInt64, true),
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().get_row_count())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("uploaded", DataType::Boolean, true),
                Box::new(|partitions| {
                    Arc::new(BooleanArray::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().uploaded())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("active", DataType::Boolean, true),
                Box::new(|partitions| {
                    Arc::new(BooleanArray::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().active())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("in_memory", DataType::Boolean, true),
                Box::new(|partitions| {
                    Arc::new(BooleanArray::from(
                        partitions
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
                Box::new(|partitions| {
                    Arc::new(TimestampNanosecondArray::from(
                        partitions
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
        ]
    }
}

crate::base_info_schema_table_def!(SystemChunksTableDef);
