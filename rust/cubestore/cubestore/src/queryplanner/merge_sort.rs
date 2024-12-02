use async_trait::async_trait;
use datafusion::arrow::array::{
    build_compare, make_comparator, ArrayRef, BooleanArray, DynComparator, RecordBatch,
};
use datafusion::arrow::compute::{filter_record_batch, SortOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::Stream;
use futures_util::StreamExt;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Filter out all but last row by unique key execution plan
#[derive(Debug)]
pub struct LastRowByUniqueKeyExec {
    input: Arc<dyn ExecutionPlan>,
    /// Columns to sort on
    pub unique_key: Vec<Column>,
    properties: PlanProperties,
}

impl LastRowByUniqueKeyExec {
    /// Create a new execution plan
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        unique_key: Vec<Column>,
    ) -> Result<Self, DataFusionError> {
        if unique_key.is_empty() {
            return Err(DataFusionError::Internal(
                "Empty unique_key passed for LastRowByUniqueKeyExec".to_string(),
            ));
        }
        let properties = input.properties().clone();
        Ok(Self {
            input,
            unique_key,
            properties,
        })
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for LastRowByUniqueKeyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LastRowByUniqueKeyExec")
    }
}

#[async_trait]
impl ExecutionPlan for LastRowByUniqueKeyExec {
    fn name(&self) -> &str {
        "LastRowByUniqueKeyExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(LastRowByUniqueKeyExec::try_new(
            children[0].clone(),
            self.unique_key.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "LastRowByUniqueKeyExec invalid partition {}",
                partition
            )));
        }

        if self.input.properties().partitioning.partition_count() != 1 {
            return Err(DataFusionError::Internal(format!(
                "LastRowByUniqueKeyExec expects only one partition but got {}",
                self.input.properties().partitioning.partition_count()
            )));
        }
        let input_stream = self.input.execute(0, context)?;

        Ok(Box::pin(LastRowByUniqueKeyExecStream {
            schema: self.input.schema(),
            input: input_stream,
            unique_key: self.unique_key.clone(),
            current_record_batch: None,
        }))
    }
}

/// Filter out all but last row by unique key stream
struct LastRowByUniqueKeyExecStream {
    /// Output schema, which is the same as the input schema for this operator
    schema: SchemaRef,
    /// The input stream to filter.
    input: SendableRecordBatchStream,
    /// Key columns
    unique_key: Vec<Column>,
    /// Current Record Batch
    current_record_batch: Option<RecordBatch>,
}

impl LastRowByUniqueKeyExecStream {
    fn row_equals(comparators: &Vec<DynComparator>, a: usize, b: usize) -> bool {
        for comparator in comparators.iter().rev() {
            if comparator(a, b) != Ordering::Equal {
                return false;
            }
        }
        true
    }

    #[tracing::instrument(level = "trace", skip(self, next_batch))]
    fn keep_only_last_rows_by_key(
        &mut self,
        next_batch: Option<RecordBatch>,
    ) -> Result<RecordBatch, DataFusionError> {
        let batch = self.current_record_batch.take().unwrap();
        let num_rows = batch.num_rows();
        let mut builder = BooleanArray::builder(num_rows);
        let key_columns = self
            .unique_key
            .iter()
            .map(|k| batch.column(k.index()).clone())
            .collect::<Vec<ArrayRef>>();
        let mut requires_filtering = false;
        let self_column_comparators = key_columns
            .iter()
            .map(|c| make_comparator(c.as_ref(), c.as_ref(), SortOptions::default()))
            .collect::<Result<Vec<_>, _>>()?;
        for i in 0..num_rows {
            let filter_value = if i == num_rows - 1 && next_batch.is_none() {
                true
            } else if i == num_rows - 1 {
                let next_key_columns = self
                    .unique_key
                    .iter()
                    .map(|k| next_batch.as_ref().unwrap().column(k.index()).clone())
                    .collect::<Vec<ArrayRef>>();
                let next_column_comparators = key_columns
                    .iter()
                    .zip(next_key_columns.iter())
                    .map(|(c, n)| make_comparator(c.as_ref(), n.as_ref(), SortOptions::default()))
                    .collect::<Result<Vec<_>, _>>()?;
                !Self::row_equals(&next_column_comparators, i, 0)
            } else {
                !Self::row_equals(&self_column_comparators, i, i + 1)
            };
            if !filter_value {
                requires_filtering = true;
            }
            builder.append_value(filter_value);
        }
        self.current_record_batch = next_batch;
        if requires_filtering {
            let filter_array = builder.finish();
            Ok(filter_record_batch(&batch, &filter_array)?)
        } else {
            Ok(batch)
        }
    }
}

impl Stream for LastRowByUniqueKeyExecStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| {
            match x {
                Some(Ok(batch)) => {
                    if self.current_record_batch.is_none() {
                        let schema = batch.schema();
                        self.current_record_batch = Some(batch);
                        // TODO get rid of empty batch. Returning Poll::Pending here results in stuck stream.
                        Some(Ok(RecordBatch::new_empty(schema)))
                    } else {
                        Some(self.keep_only_last_rows_by_key(Some(batch)))
                    }
                }
                None => {
                    if self.current_record_batch.is_some() {
                        Some(self.keep_only_last_rows_by_key(None))
                    } else {
                        None
                    }
                }
                other => other,
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.input.size_hint();
        (lower, upper.map(|u| u + 1))
    }
}

impl RecordBatchStream for LastRowByUniqueKeyExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
