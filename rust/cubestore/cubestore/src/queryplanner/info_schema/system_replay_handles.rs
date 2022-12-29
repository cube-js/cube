use crate::metastore::replay_handle::{ReplayHandle, SeqPointerForLocation};
use crate::metastore::IdRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemReplayHandlesTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemReplayHandlesTableDef {
    type T = IdRow<ReplayHandle>;

    async fn rows(&self, ctx: InfoSchemaTableDefContext) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.meta_store.all_replay_handles().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::UInt64, false),
                Box::new(|handles| {
                    Arc::new(UInt64Array::from(
                        handles.iter().map(|row| row.get_id()).collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("table_id", DataType::UInt64, false),
                Box::new(|handles| {
                    Arc::new(UInt64Array::from(
                        handles
                            .iter()
                            .map(|row| row.get_row().table_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("has_failed_to_persist_chunks", DataType::Boolean, true),
                Box::new(|handles| {
                    Arc::new(BooleanArray::from(
                        handles
                            .iter()
                            .map(|row| row.get_row().has_failed_to_persist_chunks())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("seq_pointers_by_location", DataType::Utf8, false),
                Box::new(|jobs| {
                    Arc::new(StringArray::from(
                        jobs.iter()
                            .map(|row| format!("{:?}", row.get_row().seq_pointers_by_location()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|handles| {
                    Arc::new(TimestampNanosecondArray::from(
                        handles
                            .iter()
                            .map(|row| row.get_row().created_at().timestamp_nanos())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(SystemReplayHandlesTableDef);
