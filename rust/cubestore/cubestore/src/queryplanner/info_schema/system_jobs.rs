use crate::metastore::job::Job;
use crate::metastore::IdRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemJobsTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemJobsTableDef {
    type T = IdRow<Job>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.meta_store.all_jobs().await?))
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

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|jobs| {
                Arc::new(UInt64Array::from(
                    jobs.iter().map(|row| row.get_id()).collect::<Vec<_>>(),
                ))
            }),
            Box::new(|jobs| {
                Arc::new(StringArray::from(
                    jobs.iter()
                        .map(|row| format!("{:?}", row.get_row().row_reference()))
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|jobs| {
                Arc::new(StringArray::from(
                    jobs.iter()
                        .map(|row| format!("{:?}", row.get_row().job_type()))
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|jobs| {
                Arc::new(StringArray::from(
                    jobs.iter()
                        .map(|row| format!("{:?}", row.get_row().status()))
                        .collect::<Vec<_>>(),
                ))
            }),
            Box::new(|jobs| {
                Arc::new(TimestampNanosecondArray::from(
                    jobs.iter()
                        .map(|row| row.get_row().last_heart_beat().timestamp_nanos())
                        .collect::<Vec<_>>(),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemJobsTableDef);
