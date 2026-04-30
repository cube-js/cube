use crate::metastore::job::Job;
use crate::metastore::IdRow;
use crate::queryplanner::info_schema::timestamp_nanos_or_panic;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, StringBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

pub struct SystemJobsTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemJobsTableDef {
    type T = IdRow<Job>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.meta_store.all_jobs().await?)
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("row_reference", DataType::Utf8, false),
            Field::new("job_type", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new(
                "last_heart_beat",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = UInt64Builder::with_capacity(num_rows);
        let mut row_ref_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut job_type_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut status_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut heartbeat_builder = TimestampNanosecondBuilder::with_capacity(num_rows);

        for row in rows.into_iter() {
            id_builder.append_value(row.get_id());
            let job = row.get_row();
            row_ref_builder.append_value(format!("{:?}", job.row_reference()));
            job_type_builder.append_value(format!("{:?}", job.job_type()));
            status_builder.append_value(format!("{:?}", job.status()));
            heartbeat_builder.append_value(timestamp_nanos_or_panic(job.last_heart_beat()));
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(row_ref_builder.finish()),
            Arc::new(job_type_builder.finish()),
            Arc::new(status_builder.finish()),
            Arc::new(heartbeat_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemJobsTableDef);
