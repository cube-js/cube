use crate::cachestore::CacheItem;
use crate::metastore::IdRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemCacheTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemCacheTableDef {
    type T = IdRow<CacheItem>;

    async fn rows(&self, ctx: InfoSchemaTableDefContext) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.cache_store.cache_all().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::Utf8, false),
                Box::new(|items| {
                    Arc::new(StringArray::from_iter(
                        items.iter().map(|row| Some(row.get_row().get_key())),
                    ))
                }),
            ),
            (
                Field::new("prefix", DataType::Utf8, false),
                Box::new(|items| {
                    Arc::new(StringArray::from_iter(
                        items.iter().map(|row| row.get_row().get_prefix().clone()),
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
                            .map(|row| {
                                row.get_row()
                                    .get_expire()
                                    .as_ref()
                                    .map(|t| t.timestamp_nanos())
                            })
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("value", DataType::Utf8, false),
                Box::new(|items| {
                    Arc::new(StringArray::from_iter(
                        items.iter().map(|row| Some(row.get_row().get_value())),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(SystemCacheTableDef);
