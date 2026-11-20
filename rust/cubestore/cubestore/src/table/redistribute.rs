use crate::table::data::concat_record_batches;
use crate::CubeError;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use std::collections::VecDeque;
use std::future::Future;

/// Utf8 and Binary arrays address their values with i32 offsets.
const MAX_COLUMN_VALUE_BYTES: usize = i32::MAX as usize;

/// Redistributes outputs of [s] in chunks of [output_batch_size] rows. [process] can return a batch
/// to prepend into the next chunk. This is useful if chunks occasionally need to be cut in pieces
/// of different sizes. A chunk is cut short when its variable-length values would not fit into a
/// single Arrow array.
pub async fn redistribute<F>(
    s: SendableRecordBatchStream,
    output_batch_size: usize,
    process: impl FnMut(RecordBatch) -> F,
) -> Result<(), CubeError>
where
    F: Future<Output = Result<Option<RecordBatch>, CubeError>>,
{
    redistribute_impl(
        s,
        BatchBuffer::new(output_batch_size, MAX_COLUMN_VALUE_BYTES),
        process,
    )
    .await
}

async fn redistribute_impl<F>(
    s: SendableRecordBatchStream,
    mut buf: BatchBuffer,
    mut process: impl FnMut(RecordBatch) -> F,
) -> Result<(), CubeError>
where
    F: Future<Output = Result<Option<RecordBatch>, CubeError>>,
{
    let mut s = s.fuse();
    loop {
        while !buf.has_group() {
            match s.next().await.transpose()? {
                None => {
                    while let Some(b) = buf.take_rest() {
                        if let Some(b) = process(b).await? {
                            buf.prepend(b);
                        }
                    }
                    return Ok(());
                }
                Some(b) => buf.append(b),
            }
        }

        if let Some(b) = process(buf.take_group()).await? {
            buf.prepend(b)
        }
    }
}

struct BatchBuffer {
    pending: VecDeque<RecordBatch>,
    num_pending_rows: usize,
    rows_per_group: usize,
    max_column_value_bytes: usize,
}

impl BatchBuffer {
    fn new(rows_per_group: usize, max_column_value_bytes: usize) -> BatchBuffer {
        assert!(0 < rows_per_group);
        BatchBuffer {
            pending: VecDeque::new(),
            num_pending_rows: 0,
            rows_per_group,
            max_column_value_bytes,
        }
    }

    fn append(&mut self, r: RecordBatch) {
        self.num_pending_rows += r.num_rows();
        self.pending.push_back(r);
    }

    fn prepend(&mut self, r: RecordBatch) {
        self.num_pending_rows += r.num_rows();
        self.pending.push_front(r);
    }

    fn has_group(&self) -> bool {
        self.rows_per_group <= self.num_pending_rows
    }

    /// Takes up to `rows_per_group` rows; fewer if more would overflow a variable-length column.
    fn take_group(&mut self) -> RecordBatch {
        assert!(self.has_group());
        self.take_rows(self.rows_per_group)
    }

    fn take_rest(&mut self) -> Option<RecordBatch> {
        if self.num_pending_rows == 0 {
            return None;
        }
        Some(self.take_rows(self.num_pending_rows))
    }

    fn take_rows(&mut self, max_rows: usize) -> RecordBatch {
        let mut group = Vec::new();
        let mut group_rows = 0;
        let mut group_bytes: Vec<usize> = Vec::new();
        while group_rows < max_rows {
            let Some(next) = self.pending.front() else {
                break;
            };
            let take = next.num_rows().min(max_rows - group_rows);
            let part = next.slice(0, take);
            let part_bytes = column_value_bytes(&part);
            // The first batch always fits: its arrays already exist.
            if !group.is_empty()
                && group_bytes
                    .iter()
                    .zip(&part_bytes)
                    .any(|(g, p)| g + p > self.max_column_value_bytes)
            {
                break;
            }
            if group_bytes.is_empty() {
                group_bytes = part_bytes;
            } else {
                group_bytes
                    .iter_mut()
                    .zip(&part_bytes)
                    .for_each(|(g, p)| *g += p);
            }

            let next = self.pending.pop_front().unwrap();
            if take < next.num_rows() {
                self.pending
                    .push_front(next.slice(take, next.num_rows() - take));
            }
            group.push(part);
            group_rows += take;
        }
        self.num_pending_rows -= group_rows;
        concat_record_batches(&group)
    }
}

/// Bytes of values referenced by each column; 0 for columns without i32 offsets.
fn column_value_bytes(b: &RecordBatch) -> Vec<usize> {
    b.columns()
        .iter()
        .map(|c| {
            let offsets = match c.data_type() {
                DataType::Utf8 => c.as_string::<i32>().value_offsets(),
                DataType::Binary => c.as_binary::<i32>().value_offsets(),
                _ => return 0,
            };
            (offsets[offsets.len() - 1] - offsets[0]) as usize
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::buffer::{Buffer, OffsetBuffer};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]))
    }

    fn batch(ids: Vec<i64>, s: StringArray) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids)), Arc::new(s)];
        RecordBatch::try_new(schema(), columns).unwrap()
    }

    fn stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    async fn collect_groups(batches: Vec<RecordBatch>, buf: BatchBuffer) -> Vec<(Vec<i64>, usize)> {
        let mut groups = Vec::new();
        redistribute_impl(stream(batches), buf, |b| {
            let ids = b
                .column(0)
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            groups.push((ids.values().to_vec(), column_value_bytes(&b)[1]));
            async { Ok(None) }
        })
        .await
        .unwrap();
        groups
    }

    const SMALL_BATCH_VALUE_BYTES: usize = 5 * 10;

    /// Four batches of five rows with a 10-byte string each.
    fn small_batches() -> Vec<RecordBatch> {
        (0..4)
            .map(|b| {
                let ids = (b * 5..b * 5 + 5).collect::<Vec<i64>>();
                let s = StringArray::from(ids.iter().map(|_| "x".repeat(10)).collect::<Vec<_>>());
                batch(ids, s)
            })
            .collect()
    }

    async fn group_sizes(rows_per_group: usize, max_column_value_bytes: usize) -> Vec<usize> {
        let groups = collect_groups(
            small_batches(),
            BatchBuffer::new(rows_per_group, max_column_value_bytes),
        )
        .await;
        let all_ids = groups
            .iter()
            .flat_map(|(ids, _)| ids.clone())
            .collect::<Vec<_>>();
        assert_eq!(all_ids, (0..20).collect::<Vec<i64>>());
        assert!(groups
            .iter()
            .all(|(_, bytes)| *bytes <= max_column_value_bytes.max(SMALL_BATCH_VALUE_BYTES)));
        groups.iter().map(|(ids, _)| ids.len()).collect()
    }

    #[tokio::test]
    async fn groups_are_cut_by_rows_and_value_bytes() {
        assert_eq!(group_sizes(8, 120).await, vec![8, 8, 4]);
        assert_eq!(group_sizes(8, 60).await, vec![5, 5, 5, 5]);
        assert_eq!(group_sizes(8, 5).await, vec![5, 5, 5, 5]);
    }

    /// Concatenating these two batches would need Utf8 offsets past i32::MAX ("offset overflow").
    #[tokio::test]
    async fn string_values_over_i32_max_are_not_concatenated() {
        const ROWS: usize = 8;
        const VALUE_LEN: usize = 160 * 1024 * 1024;
        let wide_batch = |first_id: i64| {
            // Zeroed allocations stay unbacked until touched, so this costs little real memory.
            let values = Buffer::from_vec(vec![0u8; ROWS * VALUE_LEN]);
            let offsets =
                OffsetBuffer::<i32>::from_lengths(std::iter::repeat(VALUE_LEN).take(ROWS));
            let s = StringArray::new(offsets, values, None);
            batch((first_id..first_id + ROWS as i64).collect(), s)
        };
        let batches = vec![wide_batch(0), wide_batch(ROWS as i64)];

        let mut groups = Vec::new();
        redistribute(stream(batches), 2 * ROWS, |b| {
            groups.push((b.num_rows(), column_value_bytes(&b)[1]));
            async { Ok(None) }
        })
        .await
        .unwrap();

        assert_eq!(
            groups,
            vec![(ROWS, ROWS * VALUE_LEN), (ROWS, ROWS * VALUE_LEN)]
        );
    }
}
