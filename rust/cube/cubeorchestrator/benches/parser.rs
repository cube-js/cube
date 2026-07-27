use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::transport::JsRawColumnarData;
use cubeshared::codegen::{
    HttpColumnValue, HttpColumnValueArgs, HttpCommand, HttpMessage, HttpMessageArgs,
    HttpQueryResult, HttpQueryResultArgs, HttpQueryResultArrow, HttpQueryResultArrowArgs,
    HttpQueryResultData, HttpResultSet, HttpResultSetArgs, HttpRow, HttpRowArgs,
};
use cubeshared::flatbuffers::FlatBufferBuilder;

#[path = "common/mod.rs"]
mod common;
use common::{
    build_arrow_ipc, build_dataset, make_member_aliases, split_dim_measure, COLUMN_COUNTS,
    ROW_COUNTS,
};

/// Build a FlatBuffer `HttpMessage` payload mirroring CubeStore's wire format
/// for `from_cubestore_fb` to parse. Cells are 16-character strings to give
/// a realistic per-cell allocation cost.
fn build_cubestore_fb_message(num_rows: usize, num_columns: usize) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    let column_names: Vec<_> = (0..num_columns)
        .map(|i| builder.create_string(&format!("column_{:02}", i)))
        .collect();

    let mut rows_vec = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut values_vec = Vec::with_capacity(num_columns);
        for col_idx in 0..num_columns {
            let value_str = builder.create_string(&format!("r{:08}_c{:04}", row_idx, col_idx));
            let col_value = HttpColumnValue::create(
                &mut builder,
                &HttpColumnValueArgs {
                    string_value: Some(value_str),
                },
            );
            values_vec.push(col_value);
        }
        let values_vector = builder.create_vector(&values_vec);
        let row = HttpRow::create(
            &mut builder,
            &HttpRowArgs {
                values: Some(values_vector),
            },
        );
        rows_vec.push(row);
    }

    let columns_vector = builder.create_vector(&column_names);
    let rows_vector = builder.create_vector(&rows_vec);
    let result_set = HttpResultSet::create(
        &mut builder,
        &HttpResultSetArgs {
            columns: Some(columns_vector),
            rows: Some(rows_vector),
        },
    );

    let connection_id = builder.create_string("bench_connection");
    let message = HttpMessage::create(
        &mut builder,
        &HttpMessageArgs {
            message_id: 1,
            command_type: HttpCommand::HttpResultSet,
            command: Some(result_set.as_union_value()),
            connection_id: Some(connection_id),
        },
    );

    builder.finish(message, None);
    builder.finished_data().to_vec()
}

fn bench_from_cubestore_fb(c: &mut Criterion) {
    let mut group = c.benchmark_group("QueryResult::from_cubestore_fb");

    for &col_count in COLUMN_COUNTS {
        for &row_count in ROW_COUNTS {
            let msg = build_cubestore_fb_message(row_count, col_count);
            let id = format!("c{:02}_r{}", col_count, row_count);
            group.throughput(Throughput::Elements((row_count * col_count) as u64));
            group.bench_with_input(BenchmarkId::from_parameter(id), &(), |b, _| {
                b.iter(|| {
                    let result =
                        QueryResult::from_cubestore_fb(black_box(&msg)).expect("from_cubestore_fb");
                    black_box(result);
                });
            });
        }
    }

    group.finish();
}

fn bench_from_js_raw_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("QueryResult::from_js_raw_data");

    let combos: &[(usize, usize)] = &[
        (8, 1),
        (8, 10),
        (8, 10_000),
        (16, 10_000),
        (16, 100_000),
        (32, 100_000),
    ];

    for &(col_count, row_count) in combos {
        let (dim_count, measure_count) = split_dim_measure(col_count);
        let dimensions = make_member_aliases("dim", dim_count);
        let measures = make_member_aliases("measure", measure_count);

        let dataset = build_dataset(row_count, &dimensions, &measures, &[]);
        let payload = serde_json::to_vec(&dataset).expect("to_vec");
        let payload_len = payload.len();

        eprintln!(
            "from_js_raw_data: c{:02}_r{} payload_bytes={}",
            col_count, row_count, payload_len
        );

        group.throughput(Throughput::Elements((row_count * col_count) as u64));

        let id_param = format!("c{:02}_r{}", col_count, row_count);

        // Parse only: serde_json::from_slice into the wire type.
        group.bench_with_input(BenchmarkId::new("parse_only", &id_param), &(), |b, _| {
            b.iter(|| {
                let parsed: JsRawColumnarData =
                    serde_json::from_slice(black_box(&payload)).expect("from_slice");
                black_box(parsed);
            });
        });

        // End-to-end: parse + build into QueryResult — what the Neon bridge does.
        group.bench_with_input(
            BenchmarkId::new("parse_plus_build", &id_param),
            &(),
            |b, _| {
                b.iter(|| {
                    let parsed: JsRawColumnarData =
                        serde_json::from_slice(black_box(&payload)).expect("from_slice");
                    let built = QueryResult::from_js_raw_data(parsed).expect("from_js_raw_data");
                    black_box(built);
                });
            },
        );
    }

    group.finish();
}

/// Wrap raw Arrow IPC bytes in an `HttpMessage` FlatBuffer carrying
fn build_cubestore_fb_arrow_message(arrow_ipc: &[u8]) -> Vec<u8> {
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

fn bench_from_cubestore_fb_arrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("QueryResult::from_cubestore_fb_arrow");

    let combos: &[(usize, usize)] = &[
        (8, 1),
        (8, 10),
        (8, 10_000),
        (16, 10_000),
        (16, 100_000),
        (32, 100_000),
    ];

    for &(col_count, row_count) in combos {
        let (dim_count, measure_count) = split_dim_measure(col_count);
        let dimensions = make_member_aliases("dim", dim_count);
        let measures = make_member_aliases("measure", measure_count);

        let arrow_ipc = build_arrow_ipc(row_count, &dimensions, &measures, &[]);
        let payload = build_cubestore_fb_arrow_message(&arrow_ipc);
        let payload_len = payload.len();

        eprintln!(
            "from_cubestore_fb_arrow: c{:02}_r{} payload_bytes={}",
            col_count, row_count, payload_len
        );

        group.throughput(Throughput::Elements((row_count * col_count) as u64));

        let id = format!("c{:02}_r{}", col_count, row_count);
        // Arrow IPC parse always materializes the QueryResult, so this measures
        // the equivalent of from_js_raw_data's `parse_plus_build`.
        group.bench_with_input(BenchmarkId::from_parameter(id), &(), |b, _| {
            b.iter(|| {
                let built = QueryResult::from_cubestore_fb(black_box(&payload))
                    .expect("from_cubestore_fb arrow");
                black_box(built);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_from_cubestore_fb,
    bench_from_js_raw_data,
    bench_from_cubestore_fb_arrow
);
criterion_main!(benches);
