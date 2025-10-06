use crate::cachestore::QueueAllItem;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

pub struct SystemQueueTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemQueueTableDef {
    type T = QueueAllItem;

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
                    items.iter().map(|row| Some(row.item.get_row().get_key())),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items
                        .iter()
                        .map(|row| row.item.get_row().get_prefix().clone()),
                ))
            }),
            Box::new(|items| {
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    items
                        .iter()
                        .map(|row| row.item.get_row().get_created().timestamp_nanos()),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter_values(
                    items
                        .iter()
                        .map(|row| format!("{:?}", row.item.get_row().get_status())),
                ))
            }),
            Box::new(|items| {
                Arc::new(Int64Array::from_iter_values(
                    items
                        .iter()
                        .map(|row| row.item.get_row().get_priority().clone()),
                ))
            }),
            Box::new(|items| {
                Arc::new(TimestampNanosecondArray::from_iter(items.iter().map(
                    |row| {
                        row.item
                            .get_row()
                            .get_heartbeat()
                            .as_ref()
                            .map(|v| v.timestamp_nanos())
                    },
                )))
            }),
            Box::new(|items| {
                Arc::new(TimestampNanosecondArray::from_iter(items.iter().map(
                    |row| {
                        row.item
                            .get_row()
                            .get_orphaned()
                            .as_ref()
                            .map(|v| v.timestamp_nanos())
                    },
                )))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items.iter().map(|row| row.payload.as_ref()),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items
                        .iter()
                        .map(|row| row.item.get_row().get_extra().clone()),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemQueueTableDef);
