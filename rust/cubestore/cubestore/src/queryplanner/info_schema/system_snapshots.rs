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

    async fn rows(&self, ctx: InfoSchemaTableDefContext) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.meta_store.get_snapshots_list().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::Utf8, false),
                Box::new(|snapshots| {
                    Arc::new(StringArray::from(
                        snapshots
                            .iter()
                            .map(|row| format!("{}", row.id))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new(
                    "created (Utc)",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|snapshots| {
                    Arc::new(TimestampNanosecondArray::from(
                        snapshots
                            .iter()
                            .map(|row| (row.id * 1000000) as i64)
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("current", DataType::Boolean, true),
                Box::new(|snapshots| {
                    Arc::new(BooleanArray::from(
                        snapshots.iter().map(|row| row.current).collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(SystemSnapshotsTableDef);
