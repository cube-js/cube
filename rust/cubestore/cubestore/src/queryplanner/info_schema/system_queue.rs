use crate::cachestore::QueueItem;
use crate::metastore::IdRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemQueueTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemQueueTableDef {
    type T = IdRow<QueueItem>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.cache_store.queue_all(limit).await?))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("prefix", DataType::Utf8, true),
            Field::new(
                "created",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("status", DataType::Utf8, false),
            Field::new("priority", DataType::Int64, false),
            Field::new(
                "heartbeat",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "orphaned",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("value", DataType::Utf8, false),
            Field::new("extra", DataType::Utf8, true),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items.iter().map(|row| Some(row.get_row().get_key())),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items.iter().map(|row| row.get_row().get_prefix().clone()),
                ))
            }),
            Box::new(|items| {
                Arc::new(TimestampNanosecondArray::from(
                    items
                        .iter()
                        .map(|row| row.get_row().get_created().timestamp_nanos())
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from(
                    items
                        .iter()
                        .map(|row| format!("{:?}", row.get_row().get_status()))
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|items| {
                Arc::new(Int64Array::from(
                    items
                        .iter()
                        .map(|row| row.get_row().get_priority().clone())
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|items| {
                Arc::new(TimestampNanosecondArray::from(
                    items
                        .iter()
                        .map(|row| {
                            row.get_row()
                                .get_heartbeat()
                                .as_ref()
                                .map(|v| v.timestamp_nanos())
                        })
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|items| {
                Arc::new(TimestampNanosecondArray::from(
                    items
                        .iter()
                        .map(|row| {
                            row.get_row()
                                .get_orphaned()
                                .as_ref()
                                .map(|v| v.timestamp_nanos())
                        })
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items.iter().map(|row| Some(row.get_row().get_value())),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items.iter().map(|row| row.get_row().get_extra().clone()),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemQueueTableDef);
