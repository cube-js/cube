use crate::metastore::job::Job;
use crate::metastore::{IdRow, MetaStore};
use crate::queryplanner::InfoSchemaTableDef;
use crate::CubeError;
use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemJobsTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemJobsTableDef {
    type T = IdRow<Job>;

    async fn rows(&self, meta_store: Arc<dyn MetaStore>) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(meta_store.all_jobs().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::UInt64, false),
                Box::new(|jobs| {
                    Arc::new(UInt64Array::from(
                        jobs.iter().map(|row| row.get_id()).collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("row_reference", DataType::Utf8, false),
                Box::new(|jobs| {
                    Arc::new(StringArray::from(
                        jobs.iter()
                            .map(|row| format!("{:?}", row.get_row().row_reference()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("job_type", DataType::Utf8, false),
                Box::new(|jobs| {
                    Arc::new(StringArray::from(
                        jobs.iter()
                            .map(|row| format!("{:?}", row.get_row().job_type()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("status", DataType::Utf8, false),
                Box::new(|jobs| {
                    Arc::new(StringArray::from(
                        jobs.iter()
                            .map(|row| format!("{:?}", row.get_row().status()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new(
                    "last_heart_beat",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Box::new(|jobs| {
                    Arc::new(TimestampNanosecondArray::from(
                        jobs.iter()
                            .map(|row| row.get_row().last_heart_beat().timestamp_nanos())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(SystemJobsTableDef);
