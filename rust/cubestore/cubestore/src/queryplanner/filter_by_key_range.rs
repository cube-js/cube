use crate::cube_ext::stream::StreamWithSchema;
use crate::queryplanner::serialized_plan::{RowFilter, RowRange};
use crate::table::data::cmp_partition_key;
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use itertools::Itertools;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct FilterByKeyRangeExec {
    input: Arc<dyn ExecutionPlan>,
    key_len: usize,
    filter: Arc<RowFilter>,
}

impl FilterByKeyRangeExec {
    /// Input must be sorted by row key. Filter and input schema must match.
    pub fn issue_filters(
        input: Arc<dyn ExecutionPlan>,
        filter: Arc<RowFilter>,
        key_len: usize,
    ) -> Arc<dyn ExecutionPlan> {
        if filter.matches_all_rows() {
            return input;
        }
        Arc::new(FilterByKeyRangeExec {
            input,
            filter,
            key_len,
        })
    }
}

impl DisplayAs for FilterByKeyRangeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FilterByKeyRangeExec")
    }
}

#[async_trait]
impl ExecutionPlan for FilterByKeyRangeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(FilterByKeyRangeExec {
            input: children.remove(0),
            filter: self.filter.clone(),
            key_len: self.key_len,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let i = self.input.execute(partition, context)?;
        let s = i.schema();
        let f = self.filter.clone();
        let key_len = self.key_len;
        Ok(Box::pin(StreamWithSchema::wrap(
            s,
            i.flat_map(move |b| {
                let r;
                match b {
                    Ok(b) => r = apply_row_filter(b, key_len, &f),
                    err => r = vec![err],
                }
                futures::stream::iter(r)
            }),
        )))
    }

    fn name(&self) -> &str {
        "FilterByKeyRangeExec"
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }
}

fn apply_row_filter(
    b: RecordBatch,
    key_len: usize,
    f: &RowFilter,
) -> Vec<Result<RecordBatch, DataFusionError>> {
    let num_rows = b.num_rows();
    if num_rows == 0 {
        return vec![Ok(b)];
    }

    let mut intervals = Vec::new();
    let key_cols = &b.columns()[0..key_len];
    for r in &f.or_filters {
        if !has_matches(key_cols, r) {
            continue;
        }
        let mut start = 0;
        if r.start.is_some() {
            let s = r.start.as_ref().unwrap().values();
            while start < num_rows
                && cmp_partition_key(key_len, s, key_cols, start) > Ordering::Equal
            {
                start += 1
            }
        }
        let mut end = num_rows;
        if r.end.is_some() {
            let e = r.end.as_ref().unwrap().values();
            while 0 < end && cmp_partition_key(key_len, e, key_cols, end - 1) <= Ordering::Equal {
                end -= 1
            }
        }
        assert!(start <= end, "{} <= {}", start, end);
        intervals.push((start, end));
    }

    // Merge intersecting intervals together.
    intervals.sort_unstable();
    for i in 1..intervals.len() {
        if intervals[i - 1].1 <= intervals[i].0 {
            intervals[i - 1].1 = intervals[i].1;
            intervals[i].0 = intervals[i - 1].0;
        }
    }
    intervals.dedup();

    intervals
        .into_iter()
        .map(move |(start, end)| Ok(b.slice(start, end - start)))
        .collect_vec()
}

fn has_matches(cols: &[ArrayRef], r: &RowRange) -> bool {
    assert_ne!(cols.len(), 0);
    assert_ne!(cols[0].len(), 0);
    let key_len = cols.len();
    let num_rows = cols[0].len();
    if r.start.is_some()
        && cmp_partition_key(
            key_len,
            r.start.as_ref().unwrap().values().as_slice(),
            cols,
            num_rows - 1,
        ) > Ordering::Equal
    {
        return false;
    }
    if r.end.is_some()
        && cmp_partition_key(
            key_len,
            r.end.as_ref().unwrap().values().as_slice(),
            cols,
            0,
        ) <= Ordering::Equal
    {
        return false;
    }
    return true;
}
