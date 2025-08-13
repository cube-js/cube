use crate::cachestore::QueueResult;
use crate::metastore::IdRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

pub struct SystemQueueResultsTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemQueueResultsTableDef {
    type T = IdRow<QueueResult>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.cache_store.queue_results_all(limit).await?))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new(
                "expire",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("deleted", DataType::Boolean, false),
            Field::new("value", DataType::Utf8, false),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|items| {
                Arc::new(UInt64Array::from_iter(
                    items.iter().map(|row| Some(row.get_id())),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items
                        .iter()
                        .map(|row| Some(row.get_row().get_path().clone())),
                ))
            }),
            Box::new(|items| {
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    items
                        .iter()
                        .map(|row| row.get_row().get_expire().timestamp_nanos()),
                ))
            }),
            Box::new(|items| {
                Arc::new(BooleanArray::from_iter(
                    items.iter().map(|row| Some(row.get_row().is_deleted())),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter_values(
                    items.iter().map(|row| row.get_row().get_value().clone()),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemQueueResultsTableDef);
