use crate::table::data::concat_record_batches;
use crate::CubeError;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use itertools::Itertools;
use std::future::Future;

/// Redistributes outputs of [s] in chunks of [output_batch_size] rows. [process] can return a batch
/// to prepend into the next chunk. This is useful if chunks occasionally need to be cut in pieces
/// of different sizes.
pub async fn redistribute<F>(
    s: SendableRecordBatchStream,
    output_batch_size: usize,
    mut process: impl FnMut(RecordBatch) -> F,
) -> Result<(), CubeError>
where
    F: Future<Output = Result<Option<RecordBatch>, CubeError>>,
{
    let mut s = s.fuse();
    let mut buf = BatchBuffer::new(output_batch_size);
    loop {
        while !buf.has_group() {
            match s.next().await.transpose()? {
                None => {
                    if let Some(b) = buf.take_rest() {
                        if let Some(b) = process(b).await? {
                            buf.prepend(b);
                            continue;
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
    pending: Vec<RecordBatch>,
    num_pending_rows: usize,
    rows_per_group: usize,
}

impl BatchBuffer {
    pub fn new(rows_per_group: usize) -> BatchBuffer {
        assert!(0 < rows_per_group);
        BatchBuffer {
            pending: Vec::new(),
            num_pending_rows: 0,
            rows_per_group,
        }
    }

    pub fn append(&mut self, r: RecordBatch) {
        self.num_pending_rows += r.num_rows();
        self.pending.push(r);
    }

    pub fn prepend(&mut self, r: RecordBatch) {
        self.num_pending_rows += r.num_rows();
        self.pending.insert(0, r);
    }

    pub fn has_group(&self) -> bool {
        self.rows_per_group <= self.num_pending_rows
    }

    pub fn take_group(&mut self) -> RecordBatch {
        assert!(self.has_group());

        // Take enough batches.
        let mut last_batch_i = 0;
        let mut num_rows = self.pending[0].num_rows();
        while num_rows < self.rows_per_group {
            last_batch_i += 1;
            num_rows += self.pending[last_batch_i].num_rows();
        }

        // Last batch might have extra rows.
        let last_batch = &self.pending[last_batch_i];
        let last_batch_rows = last_batch.num_rows();
        let extra_rows = num_rows - self.rows_per_group;
        let extra_batch = last_batch.slice(last_batch_rows - extra_rows, extra_rows);

        // Update the state of our buffer.
        let mut batches;
        if extra_rows == 0 {
            batches = self.pending.drain(0..=last_batch_i).collect_vec();
        } else {
            batches = self
                .pending
                .splice(0..=last_batch_i, [extra_batch])
                .collect_vec();
        }
        self.num_pending_rows -= self.rows_per_group;

        // The last batch still has some extra rows.
        *batches.last_mut().unwrap() = batches
            .last()
            .unwrap()
            .slice(0, last_batch_rows - extra_rows);

        let r = concat_record_batches(&batches);
        debug_assert_eq!(r.num_rows(), self.rows_per_group);
        r
    }

    pub fn take_rest(&mut self) -> Option<RecordBatch> {
        if self.num_pending_rows == 0 {
            return None;
        }
        let r = Some(concat_record_batches(&self.pending));

        self.pending.clear();
        self.num_pending_rows = 0;

        r
    }
}
