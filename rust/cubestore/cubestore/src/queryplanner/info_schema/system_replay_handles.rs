use crate::metastore::replay_handle::{ReplayHandle, SeqPointerForLocation};
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

pub struct SystemReplayHandlesTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemReplayHandlesTableDef {
    type T = IdRow<ReplayHandle>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.meta_store.all_replay_handles().await?)
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("table_id", DataType::UInt64, false),
            Field::new("has_failed_to_persist_chunks", DataType::Boolean, true),
            Field::new("seq_pointers_by_location", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = UInt64Builder::with_capacity(num_rows);
        let mut table_id_builder = UInt64Builder::with_capacity(num_rows);
        let mut failed_builder = BooleanBuilder::with_capacity(num_rows);
        let mut seq_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut created_builder = TimestampNanosecondBuilder::with_capacity(num_rows);

        for row in rows.into_iter() {
            id_builder.append_value(row.get_id());
            let handle = row.get_row();
            table_id_builder.append_value(handle.table_id());
            failed_builder.append_value(handle.has_failed_to_persist_chunks());
            seq_builder.append_value(format!("{:?}", handle.seq_pointers_by_location()));
            created_builder.append_value(timestamp_nanos_or_panic(handle.created_at()));
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(table_id_builder.finish()),
            Arc::new(failed_builder.finish()),
            Arc::new(seq_builder.finish()),
            Arc::new(created_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemReplayHandlesTableDef);
