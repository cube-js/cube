use crate::metastore::snapshot_info::SnapshotInfo;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemSnapshotsTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemSnapshotsTableDef {
    type T = SnapshotInfo;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.meta_store.get_snapshots_list().await?))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "created (Utc)",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("current", DataType::Boolean, true),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|snapshots| {
                Arc::new(StringArray::from(
                    snapshots
                        .iter()
                        .map(|row| format!("{}", row.id))
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|snapshots| {
                Arc::new(TimestampNanosecondArray::from(
                    snapshots
                        .iter()
                        .map(|row| (row.id * 1000000) as i64)
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|snapshots| {
                Arc::new(BooleanArray::from(
                    snapshots.iter().map(|row| row.current).collect::<Vec<_>>(),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemSnapshotsTableDef);
