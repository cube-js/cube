#![allow(dead_code)]

use cubeorchestrator::query_result_transform::{ColumnarArray, DBResponsePrimitive};
use cubeorchestrator::transport::JsRawColumnarData;

pub const ROW_COUNTS: &[usize] = &[1_000, 10_000, 50_000, 100_000];
pub const COLUMN_COUNTS: &[usize] = &[8, 16, 32, 64];

#[derive(Clone)]
pub struct TimeColumn {
    pub member: String,
    pub alias: String,
}

/// Split a target column count into ~60% dimensions and ~40% measures.
pub fn split_dim_measure(col_count: usize) -> (usize, usize) {
    let dim_count = (col_count * 6) / 10;
    let measure_count = col_count - dim_count;
    (dim_count, measure_count)
}

pub fn make_member_aliases(prefix: &str, count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|i| {
            (
                format!("Sales.{}{}", prefix, i),
                format!("sales__{}{}", prefix, i),
            )
        })
        .collect()
}

pub fn build_dataset(
    row_count: usize,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
    time_dims: &[TimeColumn],
) -> JsRawColumnarData {
    let total_cols = dimensions.len() + measures.len() + time_dims.len();
    let mut members = Vec::with_capacity(total_cols);
    let mut columns: Vec<ColumnarArray> = Vec::with_capacity(total_cols);

    for (j, (_, alias)) in dimensions.iter().enumerate() {
        members.push(alias.clone());
        let mut col = ColumnarArray::with_capacity(row_count);
        for i in 0..row_count {
            col.push(DBResponsePrimitive::String(format!(
                "dim_{}_{}",
                j,
                i % 1000
            )));
        }
        columns.push(col);
    }
    for (j, (_, alias)) in measures.iter().enumerate() {
        members.push(alias.clone());
        let mut col = ColumnarArray::with_capacity(row_count);
        for i in 0..row_count {
            col.push(DBResponsePrimitive::Float64(((i * (j + 1)) as f64) * 0.5));
        }
        columns.push(col);
    }
    for (j, td) in time_dims.iter().enumerate() {
        members.push(td.alias.clone());
        let mut col = ColumnarArray::with_capacity(row_count);
        for i in 0..row_count {
            // Format mirrors typical CubeStore output: ISO-8601 with millisecond
            // fractional and no timezone.
            let month = ((i + j) % 12) + 1;
            let day = ((i / 12) % 28) + 1;
            col.push(DBResponsePrimitive::String(format!(
                "2024-{:02}-{:02}T00:00:00.000",
                month, day
            )));
        }
        columns.push(col);
    }

    JsRawColumnarData { members, columns }
}

/// Build an Arrow IPC **stream** payload with the same logical data shape as
/// [`build_dataset`]: dimensions as Utf8, measures as Float64, time dimensions
/// as Timestamp(Millisecond). Used to compare Arrow parse throughput against the
/// JSON path.
pub fn build_arrow_ipc(
    row_count: usize,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
    time_dims: &[TimeColumn],
) -> Vec<u8> {
    use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let total_cols = dimensions.len() + measures.len() + time_dims.len();
    let mut fields = Vec::with_capacity(total_cols);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(total_cols);

    for (j, (_, alias)) in dimensions.iter().enumerate() {
        fields.push(Field::new(alias.clone(), DataType::Utf8, false));
        let values: Vec<String> = (0..row_count)
            .map(|i| format!("dim_{}_{}", j, i % 1000))
            .collect();
        columns.push(Arc::new(StringArray::from(values)));
    }
    for (j, (_, alias)) in measures.iter().enumerate() {
        fields.push(Field::new(alias.clone(), DataType::Float64, false));
        let values: Vec<f64> = (0..row_count)
            .map(|i| ((i * (j + 1)) as f64) * 0.5)
            .collect();
        columns.push(Arc::new(Float64Array::from(values)));
    }
    for (j, td) in time_dims.iter().enumerate() {
        fields.push(Field::new(
            td.alias.clone(),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        // One day apart, offset per column — arbitrary but realistic spread.
        let values: Vec<i64> = (0..row_count)
            .map(|i| ((i + j) as i64) * 86_400_000)
            .collect();
        columns.push(Arc::new(TimestampMillisecondArray::from(values)));
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).expect("arrow record batch");

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref()).expect("arrow writer");
        writer.write(&batch).expect("write arrow batch");
        writer.finish().expect("finish arrow stream");
    }
    buf
}
