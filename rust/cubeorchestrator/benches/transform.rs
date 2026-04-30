use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::query_result_transform::{DBResponsePrimitive, TransformedData};
use cubeorchestrator::transport::{
    ConfigItem, JsRawData, MemberOrMemberExpression, NormalizedQuery, QueryType, ResultType,
    TransformDataRequest,
};
use indexmap::IndexMap;

const ROW_COUNTS: &[usize] = &[1_000, 10_000, 50_000, 100_000];
const COLUMN_COUNTS: &[usize] = &[8, 16, 32, 64];

/// Split a target column count into ~60% dimensions and ~40% measures.
fn split_dim_measure(col_count: usize) -> (usize, usize) {
    let dim_count = (col_count * 6) / 10;
    let measure_count = col_count - dim_count;
    (dim_count, measure_count)
}

fn make_member_aliases(prefix: &str, count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|i| {
            (
                format!("Sales.{}{}", prefix, i),
                format!("sales__{}{}", prefix, i),
            )
        })
        .collect()
}

fn config_item(member_type: &str) -> ConfigItem {
    ConfigItem {
        title: None,
        short_title: None,
        description: None,
        member_type: Some(member_type.to_string()),
        format: None,
        currency: None,
        meta: None,
        drill_members: None,
        drill_members_grouped: None,
        granularities: None,
        granularity: None,
    }
}

fn build_request(
    res_type: Option<ResultType>,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
) -> TransformDataRequest {
    let mut alias_to_member_name_map = HashMap::new();
    let mut annotation = HashMap::new();

    for (member, alias) in dimensions {
        alias_to_member_name_map.insert(alias.clone(), member.clone());
        annotation.insert(member.clone(), config_item("string"));
    }
    for (member, alias) in measures {
        alias_to_member_name_map.insert(alias.clone(), member.clone());
        annotation.insert(member.clone(), config_item("number"));
    }

    let dimensions_query = dimensions
        .iter()
        .map(|(m, _)| MemberOrMemberExpression::Member(m.clone()))
        .collect();
    let measures_query = measures
        .iter()
        .map(|(m, _)| MemberOrMemberExpression::Member(m.clone()))
        .collect();

    let query = NormalizedQuery {
        measures: Some(measures_query),
        dimensions: Some(dimensions_query),
        time_dimensions: None,
        segments: None,
        limit: None,
        offset: None,
        total: None,
        total_query: None,
        timezone: Some("UTC".to_string()),
        renew_query: None,
        ungrouped: None,
        response_format: None,
        filters: None,
        row_limit: None,
        order: None,
        query_type: Some(QueryType::RegularQuery),
    };

    TransformDataRequest {
        alias_to_member_name_map,
        annotation,
        query,
        query_type: Some(QueryType::RegularQuery),
        res_type,
    }
}

fn build_dataset(
    row_count: usize,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
) -> JsRawData {
    let total_cols = dimensions.len() + measures.len();
    let mut rows = Vec::with_capacity(row_count);

    for i in 0..row_count {
        let mut row = IndexMap::with_capacity(total_cols);
        for (j, (_, alias)) in dimensions.iter().enumerate() {
            row.insert(
                alias.clone(),
                DBResponsePrimitive::String(format!("dim_{}_{}", j, i % 1000)),
            );
        }
        for (j, (_, alias)) in measures.iter().enumerate() {
            row.insert(
                alias.clone(),
                DBResponsePrimitive::Number(((i * (j + 1)) as f64) * 0.5),
            );
        }
        rows.push(row);
    }

    rows
}

fn bench_transform(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransformedData::transform");

    for &col_count in COLUMN_COUNTS {
        let (dim_count, measure_count) = split_dim_measure(col_count);
        let dimensions = make_member_aliases("dim", dim_count);
        let measures = make_member_aliases("measure", measure_count);

        for &row_count in ROW_COUNTS {
            let raw =
                QueryResult::from_js_raw_data(build_dataset(row_count, &dimensions, &measures))
                    .expect("from_js_raw_data");

            // Throughput in cells/sec so numbers are comparable across widths.
            group.throughput(Throughput::Elements((row_count * col_count) as u64));

            for (label, res_type) in [
                ("compact", Some(ResultType::Compact)),
                ("columnar", Some(ResultType::Columnar)),
                ("vanilla", None),
            ] {
                let request = build_request(res_type, &dimensions, &measures);
                let id_param = format!("c{:02}_r{}", col_count, row_count);
                group.bench_with_input(BenchmarkId::new(label, id_param), &(), |b, _| {
                    b.iter(|| {
                        let result =
                            TransformedData::transform(black_box(&request), black_box(&raw))
                                .expect("transform");
                        black_box(result);
                    });
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_transform);
criterion_main!(benches);
