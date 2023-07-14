use crate::cachestore::QueueResult;
use crate::metastore::IdRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemQueueResultsTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemQueueResultsTableDef {
    type T = IdRow<QueueResult>;

    async fn rows(&self, ctx: InfoSchemaTableDefContext) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.cache_store.queue_results_all().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::UInt64, false),
                Box::new(|items| {
                    Arc::new(UInt64Array::from_iter(
                        items.iter().map(|row| Some(row.get_id())),
                    ))
                }),
            ),
            (
                Field::new("path", DataType::Utf8, false),
                Box::new(|items| {
                    Arc::new(StringArray::from_iter(
                        items
                            .iter()
                            .map(|row| Some(row.get_row().get_path().clone())),
                    ))
                }),
            ),
            (
                Field::new(
                    "expire",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|items| {
                    Arc::new(TimestampNanosecondArray::from(
                        items
                            .iter()
                            .map(|row| row.get_row().get_expire().timestamp_nanos())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("deleted", DataType::Boolean, false),
                Box::new(|items| {
                    Arc::new(BooleanArray::from_iter(
                        items.iter().map(|row| Some(row.get_row().is_deleted())),
                    ))
                }),
            ),
            (
                Field::new("value", DataType::Utf8, false),
                Box::new(|items| {
                    Arc::new(StringArray::from(
                        items
                            .iter()
                            .map(|row| format!("{:?}", row.get_row().get_value()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(SystemQueueResultsTableDef);
