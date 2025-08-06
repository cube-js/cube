use crate::metastore::snapshot_info::SnapshotInfo;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
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
                Arc::new(StringArray::from_iter_values(
                    snapshots.iter().map(|row| format!("{}", row.id)),
                ))
            }),
            Box::new(|snapshots| {
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    snapshots.iter().map(|row| (row.id * 1000000) as i64),
                ))
            }),
            Box::new(|snapshots| {
                Arc::new(BooleanArray::from_iter(
                    snapshots.iter().map(|row| Some(row.current)),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemSnapshotsTableDef);
