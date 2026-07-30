#![allow(dead_code)]

use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::query_result_transform::{ColumnarArray, DBResponsePrimitive};
use cubeorchestrator::transport::JsRawColumnarData;
use cubeshared::codegen::{
    HttpCommand, HttpMessage, HttpMessageArgs, HttpQueryResult, HttpQueryResultArgs,
    HttpQueryResultArrow, HttpQueryResultArrowArgs, HttpQueryResultData,
};
use cubeshared::flatbuffers::FlatBufferBuilder;

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

/// How measure columns are typed in an Arrow fixture. CubeStore answers `SUM`
/// with `Decimal128`, so that is the shape most measure cells really have; the
/// `Float64` variant is kept because it is the cheaper baseline to compare against.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum MeasureKind {
    Float64,
    Decimal128,
}

impl MeasureKind {
    pub fn label(self) -> &'static str {
        match self {
            MeasureKind::Float64 => "arrow",
            MeasureKind::Decimal128 => "arrow_dec",
        }
    }
}

/// Build an Arrow IPC **stream** payload with the same logical data shape as
/// [`build_dataset`]: dimensions as Utf8, measures per `measure_kind`, time
/// dimensions as Timestamp(Millisecond). Used to compare Arrow parse throughput
/// against the JSON path.
pub fn build_arrow_ipc(
    row_count: usize,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
    time_dims: &[TimeColumn],
    measure_kind: MeasureKind,
) -> Vec<u8> {
    use arrow::array::{
        ArrayRef, Decimal128Array, Float64Array, StringArray, TimestampMillisecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let total_cols = dimensions.len() + measures.len() + time_dims.len();
    let mut fields = Vec::with_capacity(total_cols);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(total_cols);

    // Every column is built with `from_iter_values`, which fills the Arrow buffer
    // straight from the iterator. Collecting into a `Vec` first would hold a
    // second copy of the whole column — and for the string case, `row_count` live
    // `String` allocations — just to hand it over.
    for (j, (_, alias)) in dimensions.iter().enumerate() {
        fields.push(Field::new(alias.clone(), DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from_iter_values(
            (0..row_count).map(|i| format!("dim_{}_{}", j, i % 1000)),
        )));
    }
    for (j, (_, alias)) in measures.iter().enumerate() {
        match measure_kind {
            MeasureKind::Float64 => {
                fields.push(Field::new(alias.clone(), DataType::Float64, false));
                columns.push(Arc::new(Float64Array::from_iter_values(
                    (0..row_count).map(|i| ((i * (j + 1)) as f64) * 0.5),
                )));
            }
            MeasureKind::Decimal128 => {
                fields.push(Field::new(
                    alias.clone(),
                    DataType::Decimal128(38, 2),
                    false,
                ));
                // Same magnitudes as the Float64 arm, as a scale-2 mantissa.
                columns.push(Arc::new(
                    Decimal128Array::from_iter_values(
                        (0..row_count).map(|i| ((i * (j + 1)) as i128) * 50),
                    )
                    .with_precision_and_scale(38, 2)
                    .expect("decimal precision"),
                ));
            }
        }
    }
    for (j, td) in time_dims.iter().enumerate() {
        fields.push(Field::new(
            td.alias.clone(),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        // One day apart, offset per column — arbitrary but realistic spread.
        columns.push(Arc::new(TimestampMillisecondArray::from_iter_values(
            (0..row_count).map(|i| ((i + j) as i64) * 86_400_000),
        )));
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

/// Wrap raw Arrow IPC bytes in an `HttpMessage` FlatBuffer carrying
/// `HttpQueryResultArrow`, exactly as CubeStore sends it.
pub fn build_cubestore_fb_arrow_message(arrow_ipc: &[u8]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let data_vec = builder.create_vector(arrow_ipc);
    let arrow = HttpQueryResultArrow::create(
        &mut builder,
        &HttpQueryResultArrowArgs {
            data: Some(data_vec),
            is_last: true,
        },
    );
    let query_result = HttpQueryResult::create(
        &mut builder,
        &HttpQueryResultArgs {
            data_type: HttpQueryResultData::HttpQueryResultArrow,
            data: Some(arrow.as_union_value()),
        },
    );
    let connection_id = builder.create_string("bench_connection");
    let message = HttpMessage::create(
        &mut builder,
        &HttpMessageArgs {
            message_id: 1,
            command_type: HttpCommand::HttpQueryResult,
            command: Some(query_result.as_union_value()),
            connection_id: Some(connection_id),
        },
    );
    builder.finish(message, None);
    builder.finished_data().to_vec()
}

/// An Arrow-backed `QueryResult` with the same logical shape as
/// [`build_dataset`], so transform throughput can be compared column storage
/// against column storage.
///
/// Note the time dimensions differ in kind, not just encoding: the Arrow fixture
/// carries `Timestamp(Millisecond)` cells, which the `time` member type passes
/// straight through, while [`build_dataset`] carries ISO strings that
/// `transform_value` re-parses and re-formats per cell.
pub fn build_arrow_query_result(
    row_count: usize,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
    time_dims: &[TimeColumn],
    measure_kind: MeasureKind,
) -> QueryResult {
    let ipc = build_arrow_ipc(row_count, dimensions, measures, time_dims, measure_kind);
    let payload = build_cubestore_fb_arrow_message(&ipc);
    QueryResult::from_cubestore_fb(&payload).expect("arrow query result")
}
