use crate::metastore::snapshot_info::SnapshotInfo;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, StringBuilder, TimestampNanosecondBuilder,
};
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
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.meta_store.get_snapshots_list().await?)
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

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut created_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        let mut current_builder = BooleanBuilder::with_capacity(num_rows);

        for row in rows.into_iter() {
            id_builder.append_value(format!("{}", row.id));
            created_builder.append_value((row.id * 1000000) as i64);
            current_builder.append_value(row.current);
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(created_builder.finish()),
            Arc::new(current_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemSnapshotsTableDef);
