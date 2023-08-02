use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

pub fn record_batch_buffer_size(batch: &RecordBatch) -> usize {
    columns_vec_buffer_size(batch.columns())
}
pub fn columns_vec_buffer_size(columns: &[ArrayRef]) -> usize {
    columns
        .iter()
        .fold(0, |size, col| size + col.get_buffer_memory_size())
}
