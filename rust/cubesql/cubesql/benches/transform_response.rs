use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use cubesql::compile::engine::df::scan::{
    convert_transport_response, convert_transport_response_columnar, DataType, MemberField, Schema,
    SchemaRef,
};
use cubesql::transport::{TransportLoadResponse, TransportLoadResponseColumnar};
use datafusion::arrow::datatypes::{Field, TimeUnit};
use serde_json::json;

const ROWS: &[usize] = &[1_000, 5_000, 10_000, 50_000, 100_000];
const COLS: &[usize] = &[8, 16, 32, 64];
const TIME_DIMS: &[usize] = &[0, 1, 2];

#[derive(Clone, Copy)]
enum ColKind {
    TimeDim,
    Float,
    Int,
    Str,
}

fn col_kinds(cols: usize, time_dims: usize) -> Vec<ColKind> {
    let mut out = Vec::with_capacity(cols);
    for i in 0..cols {
        if i < time_dims {
            out.push(ColKind::TimeDim);
        } else {
            // Rotate through Float/Int/Str so the row mix is realistic and
            // independent of `cols` modulo 3.
            out.push(match (i - time_dims) % 3 {
                0 => ColKind::Float,
                1 => ColKind::Int,
                _ => ColKind::Str,
            });
        }
    }
    out
}

fn member_name(idx: usize, kind: ColKind) -> String {
    match kind {
        ColKind::TimeDim => format!("Cube.t{}", idx),
        ColKind::Float => format!("Cube.f{}", idx),
        ColKind::Int => format!("Cube.i{}", idx),
        ColKind::Str => format!("Cube.s{}", idx),
    }
}

/// Field name as it appears in the Cube response (and so in `MemberField::field_name`).
/// Matches `MemberField::time_dimension` which appends `.<granularity>` to the member.
fn field_name(idx: usize, kind: ColKind) -> String {
    let base = member_name(idx, kind);
    match kind {
        ColKind::TimeDim => format!("{}.day", base),
        _ => base,
    }
}

fn build_schema(kinds: &[ColKind]) -> SchemaRef {
    let fields = kinds
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let name = field_name(i, *k);
            let dt = match k {
                ColKind::TimeDim => DataType::Timestamp(TimeUnit::Nanosecond, None),
                ColKind::Float => DataType::Float64,
                ColKind::Int => DataType::Int64,
                ColKind::Str => DataType::Utf8,
            };
            Field::new(&name, dt, true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn build_member_fields(kinds: &[ColKind]) -> Vec<MemberField> {
    kinds
        .iter()
        .enumerate()
        .map(|(i, k)| match k {
            ColKind::TimeDim => MemberField::time_dimension(member_name(i, *k), "day".to_string()),
            _ => MemberField::regular(member_name(i, *k)),
        })
        .collect()
}

/// Build one primitive cell value, in the encoding that real Cube responses use:
/// timestamps and ints arrive as JSON strings, floats as JSON numbers, strings as quoted.
fn cell_value(row: usize, col: usize, kind: ColKind) -> serde_json::Value {
    match kind {
        ColKind::TimeDim => {
            // Spread rows over a 12 x 28 grid of always-valid dates.
            let n = row % (12 * 28);
            let month = (n / 28) + 1;
            let day = (n % 28) + 1;
            // ISO-8601 with no offset, parseable by `parse_date_str`.
            json!(format!("2024-{:02}-{:02}T00:00:00.000", month, day))
        }
        // Integer-valued floats are common in Cube responses.
        ColKind::Float => json!(row as f64 + (col as f64) / 100.0),
        // Cube returns numeric measures as JSON strings.
        ColKind::Int => json!(row.wrapping_mul(31).wrapping_add(col).to_string()),
        ColKind::Str => json!(format!("row-{}-c{}", row, col)),
    }
}

fn annotation_value() -> serde_json::Value {
    json!({
        "measures": {},
        "dimensions": {},
        "segments": {},
        "timeDimensions": {},
    })
}

fn build_row_json(rows: usize, kinds: &[ColKind]) -> String {
    let names: Vec<String> = kinds
        .iter()
        .enumerate()
        .map(|(i, k)| field_name(i, *k))
        .collect();

    let data: Vec<serde_json::Value> = (0..rows)
        .map(|r| {
            let mut row_obj = serde_json::Map::with_capacity(kinds.len());
            for (c, kind) in kinds.iter().enumerate() {
                row_obj.insert(names[c].clone(), cell_value(r, c, *kind));
            }
            serde_json::Value::Object(row_obj)
        })
        .collect();

    let response = json!({
        "results": [{
            "annotation": annotation_value(),
            "data": data,
        }]
    });

    serde_json::to_string(&response).expect("serialize row json")
}

fn build_columnar_json(rows: usize, kinds: &[ColKind]) -> String {
    let names: Vec<String> = kinds
        .iter()
        .enumerate()
        .map(|(i, k)| field_name(i, *k))
        .collect();

    let columns: Vec<Vec<serde_json::Value>> = kinds
        .iter()
        .enumerate()
        .map(|(c, kind)| (0..rows).map(|r| cell_value(r, c, *kind)).collect())
        .collect();

    let response = json!({
        "results": [{
            "annotation": annotation_value(),
            "data": {
                "members": names,
                "columns": columns,
            },
        }]
    });

    serde_json::to_string(&response).expect("serialize columnar json")
}

struct Inputs {
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
    row_json: String,
    columnar_json: String,
}

fn build_inputs(rows: usize, cols: usize, time_dims: usize) -> Inputs {
    let kinds = col_kinds(cols, time_dims);
    Inputs {
        schema: build_schema(&kinds),
        member_fields: build_member_fields(&kinds),
        row_json: build_row_json(rows, &kinds),
        columnar_json: build_columnar_json(rows, &kinds),
    }
}

fn sample_size_for(rows: usize) -> usize {
    // Default Criterion sample size is 100; trim for the heavy shapes so
    // a full matrix run completes in reasonable wall time.
    if rows >= 50_000 {
        10
    } else if rows >= 10_000 {
        20
    } else {
        50
    }
}

fn bench_transform_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform_response");

    for &rows in ROWS {
        let sample = sample_size_for(rows);
        group.sample_size(sample);
        group.throughput(Throughput::Elements(rows as u64));

        for &cols in COLS {
            for &td in TIME_DIMS {
                if td > cols {
                    continue;
                }
                let inputs = build_inputs(rows, cols, td);
                let Inputs {
                    schema,
                    member_fields,
                    row_json,
                    columnar_json,
                } = &inputs;

                let row_id = format!("row/rows={}/cols={}/td={}", rows, cols, td);
                group.bench_with_input(
                    BenchmarkId::from_parameter(&row_id),
                    row_json.as_str(),
                    |b, json| {
                        b.iter(|| {
                            let value: serde_json::Value =
                                serde_json::from_str(json).expect("row from_str");
                            let response: TransportLoadResponse =
                                serde_json::from_value(value).expect("row from_value");
                            convert_transport_response(
                                response,
                                schema.clone(),
                                member_fields.clone(),
                            )
                            .expect("convert_transport_response")
                        })
                    },
                );

                let col_id = format!("columnar/rows={}/cols={}/td={}", rows, cols, td);
                group.bench_with_input(
                    BenchmarkId::from_parameter(&col_id),
                    columnar_json.as_str(),
                    |b, json| {
                        b.iter(|| {
                            let value: serde_json::Value =
                                serde_json::from_str(json).expect("columnar from_str");
                            let response: TransportLoadResponseColumnar =
                                serde_json::from_value(value).expect("columnar from_value");
                            convert_transport_response_columnar(
                                response,
                                schema.clone(),
                                member_fields.clone(),
                            )
                            .expect("convert_transport_response_columnar")
                        })
                    },
                );

                drop(inputs);
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_transform_response);
criterion_main!(benches);
