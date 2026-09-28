use crate::cachestore::QueueAllItem;
use crate::queryplanner::info_schema::timestamp_nanos_or_panic;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
};
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
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.cache_store.queue_all(limit).await?)
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
            Field::new("process_id", DataType::Utf8, true),
            Field::new("exclusive", DataType::Boolean, false),
            Field::new("external_id", DataType::Utf8, true),
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut prefix_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut created_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut status_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut priority_builder = Int64Builder::with_capacity(num_rows);
        let mut heartbeat_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut orphaned_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut value_builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
        let mut extra_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut process_id_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut exclusive_builder = BooleanBuilder::with_capacity(num_rows);
        let mut external_id_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);

        for row in rows.into_iter() {
            let item = row.item.get_row();
            id_builder.append_value(item.get_key());
            prefix_builder.append_option(item.get_prefix().as_deref());
            created_builder.append_value(timestamp_nanos_or_panic(item.get_created()));
            status_builder.append_value(format!("{:?}", item.get_status()));
            priority_builder.append_value(*item.get_priority());
            heartbeat_builder
                .append_option(item.get_heartbeat().as_ref().map(timestamp_nanos_or_panic));
            orphaned_builder
                .append_option(item.get_orphaned().as_ref().map(timestamp_nanos_or_panic));
            value_builder.append_option(row.payload.as_deref());
            extra_builder.append_option(item.get_extra().as_deref());
            process_id_builder.append_option(item.get_process_id().as_deref());
            exclusive_builder.append_value(item.get_exclusive());
            external_id_builder.append_option(item.get_external_id().as_deref());
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(prefix_builder.finish()),
            Arc::new(created_builder.finish()),
            Arc::new(status_builder.finish()),
            Arc::new(priority_builder.finish()),
            Arc::new(heartbeat_builder.finish()),
            Arc::new(orphaned_builder.finish()),
            Arc::new(value_builder.finish()),
            Arc::new(extra_builder.finish()),
            Arc::new(process_id_builder.finish()),
            Arc::new(exclusive_builder.finish()),
            Arc::new(external_id_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemQueueTableDef);
