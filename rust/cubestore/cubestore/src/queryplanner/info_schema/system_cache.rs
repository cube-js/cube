use crate::cachestore::CacheItem;
use crate::metastore::IdRow;
use crate::queryplanner::info_schema::timestamp_nanos_or_panic;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder, TimestampNanosecondBuilder};
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
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.cache_store.cache_all(limit).await?)
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

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut key_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut prefix_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut expire_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut value_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);

        for row in rows.into_iter() {
            let item = row.get_row();
            key_builder.append_value(item.get_key());
            prefix_builder.append_option(item.get_prefix().as_deref());
            expire_builder.append_option(item.get_expire().as_ref().map(timestamp_nanos_or_panic));
            value_builder.append_value(item.get_value());
        }

        vec![
            Arc::new(key_builder.finish()),
            Arc::new(prefix_builder.finish()),
            Arc::new(expire_builder.finish()),
            Arc::new(value_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemCacheTableDef);
