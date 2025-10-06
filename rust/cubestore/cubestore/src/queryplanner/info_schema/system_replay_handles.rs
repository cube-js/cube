use crate::metastore::replay_handle::{ReplayHandle, SeqPointerForLocation};
use crate::metastore::IdRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray, UInt64Array,
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
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.meta_store.all_replay_handles().await?))
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

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|handles| {
                Arc::new(UInt64Array::from_iter_values(
                    handles.iter().map(|row| row.get_id()),
                ))
            }),
            Box::new(|handles| {
                Arc::new(UInt64Array::from_iter_values(
                    handles.iter().map(|row| row.get_row().table_id()),
                ))
            }),
            Box::new(|handles| {
                Arc::new(BooleanArray::from_iter(
                    handles
                        .iter()
                        .map(|row| Some(row.get_row().has_failed_to_persist_chunks())),
                ))
            }),
            Box::new(|jobs| {
                Arc::new(StringArray::from_iter_values(jobs.iter().map(|row| {
                    format!("{:?}", row.get_row().seq_pointers_by_location())
                })))
            }),
            Box::new(|handles| {
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    handles
                        .iter()
                        .map(|row| row.get_row().created_at().timestamp_nanos()),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemReplayHandlesTableDef);
