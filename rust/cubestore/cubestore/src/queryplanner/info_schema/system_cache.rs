use crate::cachestore::CacheItem;
use crate::metastore::IdRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

pub struct SystemCacheTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemCacheTableDef {
    type T = IdRow<CacheItem>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.cache_store.cache_all(limit).await?))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("prefix", DataType::Utf8, true),
            Field::new(
                "expire",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("value", DataType::Utf8, false),
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
                Arc::new(TimestampNanosecondArray::from_iter(items.iter().map(
                    |row| {
                        row.get_row()
                            .get_expire()
                            .as_ref()
                            .map(|t| t.timestamp_nanos())
                    },
                )))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items.iter().map(|row| Some(row.get_row().get_value())),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemCacheTableDef);
