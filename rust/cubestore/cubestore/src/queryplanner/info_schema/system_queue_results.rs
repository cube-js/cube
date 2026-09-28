use crate::cachestore::QueueResult;
use crate::metastore::IdRow;
use crate::queryplanner::info_schema::timestamp_nanos_or_panic;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, StringBuilder, TimestampNanosecondBuilder, UInt64Builder,
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
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.cache_store.queue_results_all(limit).await?)
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
            Field::new("external_id", DataType::Utf8, true),
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = UInt64Builder::with_capacity(num_rows);
        let mut path_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut expire_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut deleted_builder = BooleanBuilder::with_capacity(num_rows);
        let mut value_builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
        let mut external_id_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);

        for row in rows.into_iter() {
            id_builder.append_value(row.get_id());
            let result = row.get_row();
            path_builder.append_value(result.get_path());
            expire_builder.append_value(timestamp_nanos_or_panic(result.get_expire()));
            deleted_builder.append_value(result.is_deleted());
            value_builder.append_value(result.get_value());
            external_id_builder.append_option(result.get_external_id().as_deref());
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(path_builder.finish()),
            Arc::new(expire_builder.finish()),
            Arc::new(deleted_builder.finish()),
            Arc::new(value_builder.finish()),
            Arc::new(external_id_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemQueueResultsTableDef);
