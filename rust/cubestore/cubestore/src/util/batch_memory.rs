use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

pub fn record_batch_buffer_size(batch: &RecordBatch) -> usize {
    columns_vec_buffer_size(batch.columns())
}
pub fn columns_vec_buffer_size(columns: &[ArrayRef]) -> usize {
    let mut sum = 0;
    for col in columns {
        let buffer_memory_size = col.get_buffer_memory_size();

        // Add a minimum batch size for the column for primitive types.  For simplicity (to avoid
        // needing a parallel implementation of Array::get_buffer_memory_size for every type of
        // Array) and due to lack of necessity, we don't recursively handle complex column types (such as
        // structs).
        let old_batch_size = 4096;
        let data_type = col.data_type();
        let min_credited_buffer_size = if data_type == &DataType::Boolean {
            old_batch_size / 8
        } else {
            data_type.primitive_width().unwrap_or(0) * old_batch_size
        };

        sum += min_credited_buffer_size.max(buffer_memory_size);
    }
    sum
}
