use crate::metastore::chunks::chunk_file_name;
use crate::metastore::{Chunk, IdRow, MetaStoreTable};
use crate::queryplanner::info_schema::timestamp_nanos_or_panic;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, StringBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

pub struct SystemChunksTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemChunksTableDef {
    type T = IdRow<Chunk>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.meta_store.chunks_table().all_rows().await?)
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("file_name", DataType::Utf8, false),
            Field::new("partition_id", DataType::UInt64, false),
            Field::new("replay_handle_id", DataType::UInt64, true),
            Field::new("row_count", DataType::UInt64, true),
            Field::new("uploaded", DataType::Boolean, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("in_memory", DataType::Boolean, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "oldest_insert_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "deactivated_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("file_size", DataType::UInt64, true),
            Field::new("min_row", DataType::Utf8, true),
            Field::new("max_row", DataType::Utf8, true),
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = UInt64Builder::with_capacity(num_rows);
        let mut file_name_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut partition_id_builder = UInt64Builder::with_capacity(num_rows);
        let mut replay_handle_id_builder = UInt64Builder::with_capacity(num_rows);
        let mut row_count_builder = UInt64Builder::with_capacity(num_rows);
        let mut uploaded_builder = BooleanBuilder::with_capacity(num_rows);
        let mut active_builder = BooleanBuilder::with_capacity(num_rows);
        let mut in_memory_builder = BooleanBuilder::with_capacity(num_rows);
        let mut created_at_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut oldest_insert_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut deactivated_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut file_size_builder = UInt64Builder::with_capacity(num_rows);
        let mut min_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut max_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);

        for row in rows.into_iter() {
            let id = row.get_id();
            let chunk = row.get_row();
            id_builder.append_value(id);
            file_name_builder.append_value(chunk_file_name(id, chunk.suffix()));
            partition_id_builder.append_value(chunk.get_partition_id());
            replay_handle_id_builder.append_option(*chunk.replay_handle_id());
            row_count_builder.append_value(chunk.get_row_count());
            uploaded_builder.append_value(chunk.uploaded());
            active_builder.append_value(chunk.active());
            in_memory_builder.append_value(chunk.in_memory());
            created_at_builder
                .append_option(chunk.created_at().as_ref().map(timestamp_nanos_or_panic));
            oldest_insert_builder.append_option(
                chunk
                    .oldest_insert_at()
                    .as_ref()
                    .map(timestamp_nanos_or_panic),
            );
            deactivated_builder.append_option(
                chunk
                    .deactivated_at()
                    .as_ref()
                    .map(timestamp_nanos_or_panic),
            );
            file_size_builder.append_option(chunk.file_size());
            min_builder.append_option(chunk.min().as_ref().map(|v| format!("{:?}", v)));
            max_builder.append_option(chunk.max().as_ref().map(|v| format!("{:?}", v)));
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(file_name_builder.finish()),
            Arc::new(partition_id_builder.finish()),
            Arc::new(replay_handle_id_builder.finish()),
            Arc::new(row_count_builder.finish()),
            Arc::new(uploaded_builder.finish()),
            Arc::new(active_builder.finish()),
            Arc::new(in_memory_builder.finish()),
            Arc::new(created_at_builder.finish()),
            Arc::new(oldest_insert_builder.finish()),
            Arc::new(deactivated_builder.finish()),
            Arc::new(file_size_builder.finish()),
            Arc::new(min_builder.finish()),
            Arc::new(max_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemChunksTableDef);
